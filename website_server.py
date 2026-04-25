"""
Velcor3 — website backend (FastAPI).
Serves the landing page, exposes /api/info and /api/claim.

Run independently:
    python website_server.py           # default port 8000
    python website_server.py --port 80

The bot (main.py) does NOT need to run for this server to work — it only
needs DISCORD_TOKEN, DISCORD_GUILD_ID, treasury addresses, and price config
from .env.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
import hmac
import hashlib
import base64
import json
import re
import sqlite3
from pathlib import Path
from typing import Optional

import requests
import aiohttp
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

_ENV_PATH = Path(__file__).parent / ".env"
# Load the repo's .env deterministically (avoid picking up another .env).
load_dotenv(dotenv_path=str(_ENV_PATH), override=True)

# ---------------------------------------------------------------------------
# Config (mirrors config.py — import it directly so values are always in sync)
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent))
import config
import payment_database
import payment_verify
import feed_events
import database
from trackers import kolfi_tokens_client as kolfi
from app_paths import DATA_DIR, ensure_dirs

ensure_dirs()
database.init_db()  # Ensure all tables (projects, follows, etc.) exist on startup
feed_events.init_db()  # Ensure feed_events table exists

DISCORD_TOKEN: str = config.DISCORD_TOKEN or ""
GUILD_ID: int = int(config.DISCORD_GUILD_ID or 0)
MONTHLY_ROLE_ID: int = config.PREMIUM_MONTHLY_ROLE_ID or 0
LIFETIME_ROLE_ID: int = config.PREMIUM_LIFETIME_ROLE_ID or 0

DISCORD_API = "https://discord.com/api/v10"
WEBSITE_DIR = Path(__file__).parent / "website"
EARLY_ACCESS_HTML = WEBSITE_DIR / "early_access.html"

ACCESS_COOKIE = "na_access"
ACCESS_TOKEN_TTL_SECONDS = int(os.getenv("WEBSITE_ACCESS_TOKEN_TTL_SECONDS", "2592000"))  # 30 days
_ACCESS_SECRET = (os.getenv("WEBSITE_ACCESS_SECRET") or DISCORD_TOKEN or "dev-secret").encode("utf-8")
# Set DEV_PREVIEW=1 in .env or environment to bypass the early-access gate (for local testing only)
_DEV_PREVIEW = os.getenv("DEV_PREVIEW", "0").strip() == "1"

# Discord OAuth (website gate)
DISCORD_OAUTH_CLIENT_ID = (os.getenv("DISCORD_OAUTH_CLIENT_ID") or "").strip()
DISCORD_OAUTH_CLIENT_SECRET = (os.getenv("DISCORD_OAUTH_CLIENT_SECRET") or "").strip()
DISCORD_OAUTH_REDIRECT_URI = (os.getenv("DISCORD_OAUTH_REDIRECT_URI") or "").strip()
_DISCORD_STATE_COOKIE = "na_discord_state"

# ---------------------------------------------------------------------------
# Account DB (Discord profile + points + task claims)
# ---------------------------------------------------------------------------
_ACCOUNT_DB = str(DATA_DIR / "account.db")

# Supabase (PostgREST) — optional. If configured, accounts/points/claims use Supabase instead of SQLite.
_SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
_SUPABASE_SERVICE_ROLE_KEY = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()


def _sb_enabled() -> bool:
    return bool(_SUPABASE_URL and _SUPABASE_SERVICE_ROLE_KEY)


def _sb_headers(*, prefer: str = "") -> dict[str, str]:
    h = {
        "apikey": _SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {_SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }
    if prefer:
        h["Prefer"] = prefer
    return h


def _sb_url(path: str) -> str:
    p = str(path or "").strip()
    if not p.startswith("/"):
        p = "/" + p
    return _SUPABASE_URL + "/rest/v1" + p


def _acct_init() -> None:
    if _sb_enabled():
        return
    conn = sqlite3.connect(_ACCOUNT_DB)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            global_name TEXT,
            avatar_url TEXT,
            points INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS task_claims (
            user_id INTEGER NOT NULL,
            task_id TEXT NOT NULL,
            day TEXT NOT NULL,
            points INTEGER DEFAULT 0,
            ts TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (user_id, task_id, day)
        )
        """
    )
    conn.commit()
    conn.close()


def _discord_avatar_url(user_id: int, avatar_hash: str | None) -> str:
    # Discord CDN avatar; if missing, use embed avatar (default)
    if avatar_hash:
        return f"https://cdn.discordapp.com/avatars/{int(user_id)}/{avatar_hash}.png?size=128"
    # fallback (stable default icon)
    return f"https://cdn.discordapp.com/embed/avatars/{int(user_id) % 5}.png"


def _acct_upsert_user(*, user_id: int, username: str, global_name: str, avatar_url: str) -> None:
    if _sb_enabled():
        payload = {
            "user_id": int(user_id),
            "username": (username or "")[:80],
            "global_name": (global_name or "")[:80],
            "avatar_url": (avatar_url or "")[:300],
        }
        # Prefer merge-duplicates when PostgREST is configured for upserts; fall back to PATCH/POST.
        r = requests.post(
            _sb_url("/users"),
            headers=_sb_headers(prefer="resolution=merge-duplicates"),
            data=json.dumps(payload),
            timeout=12,
        )
        if r.status_code in (200, 201, 204):
            return
        if r.status_code in (400, 409):
            r2 = requests.patch(
                _sb_url(f"/users?user_id=eq.{int(user_id)}"),
                headers=_sb_headers(),
                data=json.dumps(
                    {
                        "username": payload["username"],
                        "global_name": payload["global_name"],
                        "avatar_url": payload["avatar_url"],
                    }
                ),
                timeout=12,
            )
            if r2.status_code in (200, 204):
                return
            if r2.status_code == 404:
                r3 = requests.post(_sb_url("/users"), headers=_sb_headers(), data=json.dumps(payload), timeout=12)
                if r3.status_code in (200, 201, 204):
                    return
                raise RuntimeError(f"Supabase insert user failed: HTTP {r3.status_code}: {r3.text[:240]}")
            raise RuntimeError(f"Supabase patch user failed: HTTP {r2.status_code}: {r2.text[:240]}")
        raise RuntimeError(f"Supabase upsert user failed: HTTP {r.status_code}: {r.text[:240]}")
    _acct_init()
    conn = sqlite3.connect(_ACCOUNT_DB)
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO users (user_id, username, global_name, avatar_url, points)
        VALUES (?, ?, ?, ?, 0)
        ON CONFLICT(user_id) DO UPDATE SET
            username = excluded.username,
            global_name = excluded.global_name,
            avatar_url = excluded.avatar_url,
            updated_at = datetime('now')
        """,
        (int(user_id), (username or "")[:80], (global_name or "")[:80], (avatar_url or "")[:300]),
    )
    conn.commit()
    conn.close()


def _acct_get_user(user_id: int) -> Optional[dict]:
    if _sb_enabled():
        r = requests.get(
            _sb_url(f"/users?user_id=eq.{int(user_id)}&select=user_id,username,global_name,avatar_url,points"),
            headers=_sb_headers(),
            timeout=12,
        )
        if r.status_code != 200:
            return None
        rows = r.json()
        if not isinstance(rows, list) or not rows:
            return None
        row = rows[0] if isinstance(rows[0], dict) else {}
        try:
            return {
                "user_id": int(row.get("user_id") or 0),
                "username": str(row.get("username") or ""),
                "global_name": str(row.get("global_name") or ""),
                "avatar_url": str(row.get("avatar_url") or ""),
                "points": int(row.get("points") or 0),
            }
        except Exception:
            return None
    _acct_init()
    conn = sqlite3.connect(_ACCOUNT_DB)
    c = conn.cursor()
    c.execute("SELECT user_id, username, global_name, avatar_url, points FROM users WHERE user_id = ?", (int(user_id),))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    return {"user_id": int(row[0]), "username": row[1] or "", "global_name": row[2] or "", "avatar_url": row[3] or "", "points": int(row[4] or 0)}


def _acct_add_points(user_id: int, points: int) -> None:
    if not points:
        return
    if _sb_enabled():
        u = _acct_get_user(int(user_id))
        if not u:
            # Ensure row exists, then apply points delta.
            _acct_upsert_user(user_id=int(user_id), username="", global_name="", avatar_url="")
            u = _acct_get_user(int(user_id)) or {"points": 0}
        new_pts = int(u.get("points") or 0) + int(points)
        r = requests.patch(
            _sb_url(f"/users?user_id=eq.{int(user_id)}"),
            headers=_sb_headers(),
            data=json.dumps({"points": int(new_pts)}),
            timeout=12,
        )
        if r.status_code not in (200, 204):
            raise RuntimeError(f"Supabase add points failed: HTTP {r.status_code}: {r.text[:240]}")
        return
    _acct_init()
    conn = sqlite3.connect(_ACCOUNT_DB)
    c = conn.cursor()
    c.execute(
        "UPDATE users SET points = COALESCE(points,0) + ?, updated_at = datetime('now') WHERE user_id = ?",
        (int(points), int(user_id)),
    )
    conn.commit()
    conn.close()


def _today_utc() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).date().isoformat()


_TASKS = [
    {"id": "connect_discord", "title": "Connect Discord", "points": 50, "type": "once"},
    {"id": "daily_checkin", "title": "Daily check-in", "points": 10, "type": "daily"},
    {"id": "visit_token_alerts", "title": "Visit Token Alerts", "points": 5, "type": "daily"},
]

# X reply points (format-only validation; no API verification)
X_REPLY_POINTS = int(os.getenv("X_REPLY_POINTS", "20") or 20)
X_TWEET_AUTHOR_ALLOW = (os.getenv("X_TWEET_AUTHOR_ALLOW", "") or "").strip().lower()  # optional: enforce author handle


def _looks_like_x_status(url: str) -> bool:
    s = (url or "").strip()
    if not s:
        return False
    return bool(re.search(r"https?://(www\.)?(x\.com|twitter\.com)/[^/]+/status/\d+", s))


def _extract_x_handle_and_status_id(url: str) -> tuple[Optional[str], Optional[str]]:
    try:
        m = re.search(r"https?://(www\.)?(x\.com|twitter\.com)/([^/]+)/status/(\d+)", (url or "").strip())
        if not m:
            return None, None
        handle = (m.group(3) or "").strip().lstrip("@")
        sid = (m.group(4) or "").strip()
        return (handle.lower() if handle else None), (sid if sid else None)
    except Exception:
        return None, None


def _acct_claim_task(user_id: int, task_id: str) -> tuple[bool, str, int]:
    tid = str(task_id or "").strip()
    if not tid:
        return False, "Missing task_id", 0
    task = next((t for t in _TASKS if t.get("id") == tid), None)
    if not task:
        return False, "Unknown task", 0
    day = _today_utc()
    ttype = str(task.get("type") or "once")
    scope_day = day if ttype == "daily" else "once"
    pts = int(task.get("points") or 0)
    if pts <= 0:
        return False, "Task has no points", 0
    if _sb_enabled():
        payload = {"user_id": int(user_id), "task_id": tid, "day": scope_day, "points": int(pts)}
        r = requests.post(_sb_url("/task_claims"), headers=_sb_headers(), data=json.dumps(payload), timeout=12)
        if r.status_code in (200, 201, 204):
            _acct_add_points(int(user_id), int(pts))
            return True, "Claimed", int(pts)
        txt = (r.text or "").lower()
        if r.status_code == 409 or "duplicate" in txt or "unique" in txt:
            return False, "Already claimed", 0
        return False, f"Claim failed: HTTP {r.status_code}", 0
    _acct_init()
    conn = sqlite3.connect(_ACCOUNT_DB)
    c = conn.cursor()
    try:
        c.execute(
            "INSERT INTO task_claims (user_id, task_id, day, points) VALUES (?, ?, ?, ?)",
            (int(user_id), tid, scope_day, int(pts)),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return False, "Already claimed", 0
    conn.close()
    _acct_add_points(int(user_id), int(pts))
    return True, "Claimed", int(pts)

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="Velcor3", docs_url=None, redoc_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Serve static assets from website/ if the folder exists
if WEBSITE_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(WEBSITE_DIR)), name="static")

# Serve image assets — check website/ first, fall back to project root
_ROOT_DIR = Path(__file__).parent

# ---------------------------------------------------------------------------
# Simple in-memory caching (website endpoints can be expensive)
# ---------------------------------------------------------------------------
_CACHE: dict[str, tuple[float, object]] = {}
_CACHE_LOCK = asyncio.Lock()
_THUMB_CACHE: dict[str, tuple[float, str]] = {}
_FIRST_CALLS_PATH = str(DATA_DIR / "kolfi_first_calls.json")
_FIRST_CALLS: dict[str, dict] = {}
_FIRST_CALLS_LOADED = False


def _load_first_calls() -> dict[str, dict]:
    global _FIRST_CALLS_LOADED, _FIRST_CALLS
    if _FIRST_CALLS_LOADED:
        return _FIRST_CALLS
    _FIRST_CALLS_LOADED = True
    try:
        p = Path(_FIRST_CALLS_PATH)
        if p.exists():
            raw = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(raw, dict) and isinstance(raw.get("by_mint"), dict):
                _FIRST_CALLS = dict(raw["by_mint"])
    except Exception:
        _FIRST_CALLS = {}
    return _FIRST_CALLS


def _save_first_calls(by_mint: dict[str, dict], max_mints: int = 6000) -> None:
    try:
        # trim if huge (drop entries missing ts first; else keep most recent)
        if len(by_mint) > max_mints:
            items = sorted(by_mint.items(), key=lambda kv: str((kv[1] or {}).get("messageTs") or ""))
            for k, _ in items[: max(0, len(by_mint) - max_mints)]:
                by_mint.pop(k, None)
        payload = {"version": 1, "by_mint": by_mint, "updated_at": int(time.time())}
        Path(_FIRST_CALLS_PATH).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        return


def _first_call_pick(a: Optional[dict], b: Optional[dict]) -> Optional[dict]:
    """Pick earlier of two call dicts by messageTs."""
    if not isinstance(a, dict):
        return b if isinstance(b, dict) else None
    if not isinstance(b, dict):
        return a
    try:
        ta = str(a.get("messageTs") or "")
        tb = str(b.get("messageTs") or "")
        if ta and tb:
            return a if ta <= tb else b
    except Exception:
        pass
    return a


def _persist_first_call_for_mint(mint: str, call: Optional[dict]) -> Optional[dict]:
    """
    Persist the earliest call we've ever seen for this mint.
    NOTE: this is earliest in OUR history (not necessarily global if upstream omits old calls).
    """
    mint = (mint or "").strip()
    if not mint or not isinstance(call, dict):
        return call if isinstance(call, dict) else None
    by_mint = _load_first_calls()
    prev = by_mint.get(mint)
    prev_call = prev.get("call") if isinstance(prev, dict) else None
    chosen = _first_call_pick(prev_call if isinstance(prev_call, dict) else None, call)
    if chosen is None:
        return None
    # store minimal stable fields only
    by_mint[mint] = {
        "call": {
            "messageTs": chosen.get("messageTs"),
            "callMarketCap": chosen.get("callMarketCap"),
            "kolXId": chosen.get("kolXId") or chosen.get("kol_x_id"),
            "kolUsername": chosen.get("kolUsername") or chosen.get("channelName") or chosen.get("kol_name"),
        }
    }
    _FIRST_CALLS = by_mint
    _save_first_calls(by_mint)
    return by_mint[mint]["call"]


def _get_persisted_first_call(mint: str) -> Optional[dict]:
    mint = (mint or "").strip()
    if not mint:
        return None
    by_mint = _load_first_calls()
    ent = by_mint.get(mint)
    if isinstance(ent, dict) and isinstance(ent.get("call"), dict):
        return ent["call"]
    return None


async def _cache_get_or_set(key: str, ttl_sec: float, builder):
    now = time.time()
    hit = _CACHE.get(key)
    if hit:
        ts, val = hit
        if now - ts <= float(ttl_sec):
            return val
    async with _CACHE_LOCK:
        hit = _CACHE.get(key)
        if hit:
            ts, val = hit
            if now - ts <= float(ttl_sec):
                return val
        val = await builder()
        _CACHE[key] = (now, val)
        return val


async def _thumb_url_cached(session: aiohttp.ClientSession, item: dict, mint: str, ttl_sec: float = 3600.0) -> str:
    if not mint:
        return ""
    now = time.time()
    hit = _THUMB_CACHE.get(mint)
    if hit:
        ts, url = hit
        if now - ts <= float(ttl_sec):
            return url or ""
    # Deterministic fallback (no extra HTTP): Dexscreener CDN icon for Solana mints.
    # Even if it 404s for some tokens, the browser will handle it and we avoid blocking the server.
    dex_cdn = f"https://dd.dexscreener.com/ds-data/tokens/solana/{mint}.png"
    # Prefer URL already in payload (free)
    for k in ("logo", "iconUrl", "image", "tokenImage", "icon_url", "imageUrl", "thumb_url"):
        v = (item or {}).get(k)
        if v and isinstance(v, str) and v.startswith("http"):
            _THUMB_CACHE[mint] = (now, v)
            return v
    try:
        url = await kolfi.resolve_token_thumbnail(session, item or {}, mint)
    except Exception:
        url = None
    u = str(url or "").strip()
    # Avoid caching empty forever; use a short TTL for failures.
    if not u:
        _THUMB_CACHE[mint] = (now - (float(ttl_sec) - 45.0), dex_cdn)  # expires in ~45s
        return dex_cdn
    _THUMB_CACHE[mint] = (now, u)
    return u


def _serve_image(filename: str, media_type: str):
    from fastapi.responses import FileResponse
    for candidate in (WEBSITE_DIR / filename, _ROOT_DIR / filename):
        if candidate.exists():
            return FileResponse(str(candidate), media_type=media_type)
    raise HTTPException(404, f"{filename} not found")


@app.get("/logo.png")
async def serve_logo():
    return _serve_image("logo.png", "image/png")


@app.get("/banner.png")
async def serve_banner_png():
    return _serve_image("banner.png", "image/png")


@app.get("/banner.jpg")
async def serve_banner_jpg():
    return _serve_image("banner.jpg", "image/jpeg")


@app.get("/channels.png")
async def serve_channels():
    return _serve_image("channels.png", "image/png")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _b64url(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode("ascii").rstrip("=")


def _b64url_decode(s: str) -> bytes:
    pad = "=" * ((4 - (len(s) % 4)) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("ascii"))


def _sign(data: bytes) -> str:
    return _b64url(hmac.new(_ACCESS_SECRET, data, hashlib.sha256).digest())


def _make_access_token(*, user_id: int) -> str:
    now = int(time.time())
    payload = {"uid": int(user_id), "iat": now, "exp": now + int(ACCESS_TOKEN_TTL_SECONDS)}
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return _b64url(body) + "." + _sign(body)


def _verify_access_token(token: str) -> Optional[dict]:
    try:
        parts = (token or "").split(".")
        if len(parts) != 2:
            return None
        body_b64, sig = parts
        body = _b64url_decode(body_b64)
        if not hmac.compare_digest(sig, _sign(body)):
            return None
        payload = json.loads(body.decode("utf-8"))
        exp = int(payload.get("exp", 0))
        now = int(time.time())
        if exp and now > exp:
            return None
        return payload
    except Exception:
        return None


def _has_access(req: Request) -> bool:
    tok = req.cookies.get(ACCESS_COOKIE, "")
    return _verify_access_token(tok) is not None


def _rand_state(n: int = 20) -> str:
    try:
        import secrets

        return secrets.token_urlsafe(max(8, int(n)))
    except Exception:
        return _b64url(os.urandom(18))


def _discord_redirect_uri(request: Request) -> str:
    # Prefer explicit config; else derive from request.
    if DISCORD_OAUTH_REDIRECT_URI:
        return DISCORD_OAUTH_REDIRECT_URI
    # Best-effort based on current host.
    try:
        base = str(request.base_url).rstrip("/")
    except Exception:
        base = "http://127.0.0.1:8000"
    return base + "/api/access/discord/callback"


def _is_https(request: Request) -> bool:
    """Detect HTTPS behind reverse proxies (Render sets X-Forwarded-Proto)."""
    try:
        xf = (request.headers.get("x-forwarded-proto", "") or "").split(",")[0].strip().lower()
        if xf:
            return xf == "https"
    except Exception:
        pass
    try:
        return str(getattr(request.url, "scheme", "") or "").lower() == "https"
    except Exception:
        return False


def _discord_oauth_enabled() -> bool:
    return bool(DISCORD_OAUTH_CLIENT_ID and DISCORD_OAUTH_CLIENT_SECRET)


@app.get("/api/access/discord/start")
async def api_access_discord_start(request: Request, next: str = "/projects"):
    """
    Start Discord OAuth flow for website access gate.
    Sets a short-lived state cookie and redirects to Discord authorize URL.
    """
    if not _discord_oauth_enabled():
        raise HTTPException(400, "Discord OAuth not configured (set DISCORD_OAUTH_CLIENT_ID/SECRET).")
    # sanitize next (same-origin paths only)
    nxt = (next or "/projects").strip()
    if not nxt.startswith("/"):
        nxt = "/projects"
    if nxt.startswith("//"):
        nxt = "/projects"

    state = _rand_state()
    redir = _discord_redirect_uri(request)
    params = {
        "client_id": DISCORD_OAUTH_CLIENT_ID,
        "redirect_uri": redir,
        "response_type": "code",
        "scope": "identify",
        "state": state,
    }
    from urllib.parse import urlencode

    url = "https://discord.com/oauth2/authorize?" + urlencode(params)
    res = RedirectResponse(url=url, status_code=302)
    # Store state + next in a cookie (httpOnly) so callback can validate and redirect.
    body = json.dumps({"s": state, "n": nxt}, separators=(",", ":")).encode("utf-8")
    res.set_cookie(
        key=_DISCORD_STATE_COOKIE,
        value=_b64url(body) + "." + _sign(body),
        httponly=True,
        secure=_is_https(request),
        samesite="lax",
        max_age=10 * 60,
        path="/",
    )
    return res


def _verify_state_cookie(req: Request) -> Optional[dict]:
    tok = req.cookies.get(_DISCORD_STATE_COOKIE, "")
    try:
        parts = (tok or "").split(".")
        if len(parts) != 2:
            return None
        body_b64, sig = parts
        body = _b64url_decode(body_b64)
        if not hmac.compare_digest(sig, _sign(body)):
            return None
        payload = json.loads(body.decode("utf-8"))
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


@app.get("/api/access/discord/callback")
async def api_access_discord_callback(request: Request, code: str = "", state: str = ""):
    """
    Discord OAuth callback: exchange code for token, fetch /users/@me, then set access cookie.
    """
    if not _discord_oauth_enabled():
        raise HTTPException(400, "Discord OAuth not configured.")
    st = _verify_state_cookie(request) or {}
    expected = str(st.get("s") or "")
    nxt = str(st.get("n") or "/projects")
    if not nxt.startswith("/"):
        nxt = "/projects"
    if not code or not state or not expected or state != expected:
        # Clear state cookie and send to projects with an error flag.
        res = RedirectResponse(url="/projects?gate=discord_error", status_code=302)
        res.delete_cookie(_DISCORD_STATE_COOKIE, path="/")
        return res

    redir = _discord_redirect_uri(request)
    token_url = "https://discord.com/api/oauth2/token"
    data = {
        "client_id": DISCORD_OAUTH_CLIENT_ID,
        "client_secret": DISCORD_OAUTH_CLIENT_SECRET,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redir,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    access_token = ""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(token_url, data=data, headers=headers, timeout=aiohttp.ClientTimeout(total=12)) as r:
                js = await r.json()
                access_token = str(js.get("access_token") or "")
        except Exception:
            access_token = ""
        if not access_token:
            res = RedirectResponse(url="/projects?gate=discord_error", status_code=302)
            res.delete_cookie(_DISCORD_STATE_COOKIE, path="/")
            return res
        user_id = None
        user_profile = None
        try:
            async with session.get(
                "https://discord.com/api/v10/users/@me",
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                js = await r.json()
                uid = js.get("id")
                if uid:
                    user_id = int(uid)
                    user_profile = js
        except Exception:
            user_id = None

    if not user_id:
        res = RedirectResponse(url="/projects?gate=discord_error", status_code=302)
        res.delete_cookie(_DISCORD_STATE_COOKIE, path="/")
        return res

    # Save Discord profile (best-effort)
    try:
        prof = user_profile if isinstance(user_profile, dict) else {}
        uname = str(prof.get("username") or "")
        gname = str(prof.get("global_name") or prof.get("globalName") or "")
        avh = prof.get("avatar")
        av_url = _discord_avatar_url(user_id, str(avh) if avh else None)
        _acct_upsert_user(user_id=user_id, username=uname, global_name=gname, avatar_url=av_url)
        # Auto-claim connect task once
        _acct_claim_task(user_id, "connect_discord")
    except Exception:
        pass

    tok = _make_access_token(user_id=user_id)
    res = RedirectResponse(url=nxt, status_code=302)
    res.set_cookie(
        key=ACCESS_COOKIE,
        value=tok,
        httponly=True,
        secure=_is_https(request),
        samesite="lax",
        max_age=int(ACCESS_TOKEN_TTL_SECONDS),
        path="/",
    )
    res.delete_cookie(_DISCORD_STATE_COOKIE, path="/")
    return res


@app.get("/account", response_class=HTMLResponse)
async def page_account(request: Request):
    return _serve_page(request, "account.html")


@app.get("/api/me")
async def api_me(request: Request):
    if not _DEV_PREVIEW and not _has_access(request):
        raise HTTPException(401, "Unauthorized")
    tok = request.cookies.get(ACCESS_COOKIE, "")
    payload = _verify_access_token(tok) or {}
    uid = int(payload.get("uid") or 0)
    if uid <= 0:
        raise HTTPException(401, "Unauthorized")
    u = _acct_get_user(uid) or {"user_id": uid, "username": "", "global_name": "", "avatar_url": "", "points": 0}
    roles = await _discord_fetch_member_roles(uid)
    is_premium = _member_roles_include_premium(roles)
    engage_tweet_url = (os.getenv("NA_COMMUNITY_ENGAGE_TWEET_URL") or "").strip()
    return {
        **u,
        "is_premium": bool(is_premium),
        "engage_tweet_url": engage_tweet_url,
    }


@app.get("/api/tasks")
async def api_tasks(request: Request):
    if not _DEV_PREVIEW and not _has_access(request):
        raise HTTPException(401, "Unauthorized")
    tok = request.cookies.get(ACCESS_COOKIE, "")
    payload = _verify_access_token(tok) or {}
    uid = int(payload.get("uid") or 0)
    if uid <= 0:
        raise HTTPException(401, "Unauthorized")
    # compute claimed
    day = _today_utc()
    rows = []
    if _sb_enabled():
        r = requests.get(
            _sb_url(f"/task_claims?user_id=eq.{int(uid)}&select=task_id,day"),
            headers=_sb_headers(),
            timeout=12,
        )
        if r.status_code != 200:
            raise HTTPException(502, f"Supabase task_claims read failed: HTTP {r.status_code}")
        js = r.json()
        rows = js if isinstance(js, list) else []
        claimed_once = {str((row or {}).get("task_id") or "") for row in rows if isinstance(row, dict) and str((row or {}).get("day") or "") == "once"}
        claimed_today = {str((row or {}).get("task_id") or "") for row in rows if isinstance(row, dict) and str((row or {}).get("day") or "") == day}
    else:
        _acct_init()
        conn = sqlite3.connect(_ACCOUNT_DB)
        c = conn.cursor()
        c.execute("SELECT task_id, day FROM task_claims WHERE user_id = ?", (uid,))
        rows = c.fetchall() or []
        conn.close()
        claimed_once = {r[0] for r in rows if r and str(r[1]) == "once"}
        claimed_today = {r[0] for r in rows if r and str(r[1]) == day}
    out = []
    for t in _TASKS:
        tid = str(t.get("id") or "")
        ttype = str(t.get("type") or "once")
        claimed = (tid in claimed_today) if ttype == "daily" else (tid in claimed_once)
        out.append({**t, "claimed": bool(claimed)})
    return {"tasks": out, "day": day}


class AdvertiseLeadRequest(BaseModel):
    project_name: str
    project_url: str = ""
    pitch: str = ""


@app.post("/api/advertise/lead")
async def api_advertise_lead(request: Request, body: AdvertiseLeadRequest):
    """Store a sponsored-placement inquiry (admin can follow up in Discord)."""
    if not _DEV_PREVIEW and not _has_access(request):
        raise HTTPException(401, "Unauthorized")
    tok = request.cookies.get(ACCESS_COOKIE, "")
    payload = _verify_access_token(tok) or {}
    uid = int(payload.get("uid") or 0)
    if uid <= 0:
        raise HTTPException(401, "Unauthorized")
    name = (body.project_name or "").strip()[:120]
    if not name:
        raise HTTPException(400, "project_name required")
    url_s = (body.project_url or "").strip()[:500]
    pitch = (body.pitch or "").strip()[:1200]
    try:
        feed_events.add_event(
            kind="advertise_inquiry",
            guild_id=int(GUILD_ID or 0),
            channel_id=0,
            title=f"Advertise: {name}",
            body=pitch or "(no pitch)",
            url=url_s,
            extra={"user_id": uid, "project_name": name, "project_url": url_s},
        )
    except Exception:
        raise HTTPException(500, "Could not save inquiry")
    return {"ok": True}


class TaskClaimRequest(BaseModel):
    task_id: str


@app.post("/api/tasks/claim")
async def api_tasks_claim(request: Request, body: TaskClaimRequest):
    if not _DEV_PREVIEW and not _has_access(request):
        raise HTTPException(401, "Unauthorized")
    tok = request.cookies.get(ACCESS_COOKIE, "")
    payload = _verify_access_token(tok) or {}
    uid = int(payload.get("uid") or 0)
    if uid <= 0:
        raise HTTPException(401, "Unauthorized")
    ok, msg, pts = _acct_claim_task(uid, str(body.task_id or ""))
    if not ok:
        raise HTTPException(400, msg)
    u = _acct_get_user(uid) or {"points": 0}
    return {"success": True, "message": msg, "points_awarded": pts, "total_points": int(u.get("points") or 0)}


class XReplyClaimRequest(BaseModel):
    tweet_url: str
    reply_url: str


@app.post("/api/x/claim")
async def api_x_claim(request: Request, body: XReplyClaimRequest):
    """
    Earn points by replying to one of our tweets on X.
    Validation is format-only; awards once per (user, tweet_id).
    """
    if not _DEV_PREVIEW and not _has_access(request):
        raise HTTPException(401, "Unauthorized")
    tok = request.cookies.get(ACCESS_COOKIE, "")
    payload = _verify_access_token(tok) or {}
    uid = int(payload.get("uid") or 0)
    if uid <= 0:
        raise HTTPException(401, "Unauthorized")

    tweet_url = (body.tweet_url or "").strip()
    reply_url = (body.reply_url or "").strip()
    if not _looks_like_x_status(tweet_url):
        raise HTTPException(400, "Invalid tweet_url (expected x.com/.../status/ID)")
    if not _looks_like_x_status(reply_url):
        raise HTTPException(400, "Invalid reply_url (expected x.com/.../status/ID)")

    th, tid = _extract_x_handle_and_status_id(tweet_url)
    rh, rid = _extract_x_handle_and_status_id(reply_url)
    if not tid or not rid:
        raise HTTPException(400, "Could not parse tweet/reply status IDs")
    if X_TWEET_AUTHOR_ALLOW and th and th != X_TWEET_AUTHOR_ALLOW:
        raise HTTPException(400, "tweet_url is not from the allowed author")

    # Claim once per tweet ID
    if _sb_enabled():
        payload = {"user_id": int(uid), "task_id": f"x_reply:{tid}", "day": "once", "points": int(X_REPLY_POINTS)}
        r = requests.post(_sb_url("/task_claims"), headers=_sb_headers(), data=json.dumps(payload), timeout=12)
        if r.status_code not in (200, 201, 204):
            txt = (r.text or "").lower()
            if r.status_code == 409 or "duplicate" in txt or "unique" in txt:
                raise HTTPException(400, "Already claimed for this tweet")
            raise HTTPException(502, f"Supabase claim insert failed: HTTP {r.status_code}: {r.text[:240]}")
        _acct_add_points(int(uid), int(X_REPLY_POINTS))
        u = _acct_get_user(uid) or {"points": 0}
        return {"success": True, "points_awarded": int(X_REPLY_POINTS), "total_points": int(u.get("points") or 0)}

    _acct_init()
    conn = sqlite3.connect(_ACCOUNT_DB)
    c = conn.cursor()
    try:
        c.execute(
            "INSERT INTO task_claims (user_id, task_id, day, points) VALUES (?, ?, ?, ?)",
            (int(uid), f"x_reply:{tid}", "once", int(X_REPLY_POINTS)),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        raise HTTPException(400, "Already claimed for this tweet")
    conn.close()
    _acct_add_points(int(uid), int(X_REPLY_POINTS))
    u = _acct_get_user(uid) or {"points": 0}
    return {"success": True, "points_awarded": int(X_REPLY_POINTS), "total_points": int(u.get("points") or 0)}


def _min_amount(tier: str, chain: str) -> float:
    if tier == "lifetime":
        if chain == "eth_mainnet":
            return config.PAYMENT_LIFETIME_MIN_ETH_MAINNET
        if chain == "eth_base":
            return config.PAYMENT_LIFETIME_MIN_ETH_BASE
        return config.PAYMENT_LIFETIME_MIN_SOL
    if chain == "eth_mainnet":
        return config.PAYMENT_MONTHLY_MIN_ETH_MAINNET
    if chain == "eth_base":
        return config.PAYMENT_MONTHLY_MIN_ETH_BASE
    return config.PAYMENT_MONTHLY_MIN_SOL


def _treasury(chain: str) -> Optional[str]:
    if chain == "eth_mainnet":
        return config.PAYMENT_TREASURY_ETH_MAINNET
    if chain == "eth_base":
        return config.PAYMENT_TREASURY_ETH_BASE
    return config.PAYMENT_TREASURY_SOL


async def _fetch_eth_price_usd(session: aiohttp.ClientSession) -> float:
    try:
        async with session.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=aiohttp.ClientTimeout(total=5),
        ) as r:
            d = await r.json()
            return float(d["ethereum"]["usd"])
    except Exception:
        return float(getattr(config, "PAYMENT_PRICE_FALLBACK_ETH_USD", 3500.0))


async def _fetch_sol_price_usd(session: aiohttp.ClientSession) -> float:
    try:
        async with session.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
            timeout=aiohttp.ClientTimeout(total=5),
        ) as r:
            d = await r.json()
            return float(d["solana"]["usd"])
    except Exception:
        return float(getattr(config, "PAYMENT_PRICE_FALLBACK_SOL_USD", 150.0))


async def _discord_fetch_member_roles(user_id: int) -> list[str]:
    """Guild member role IDs for premium checks (website /api/me)."""
    if not DISCORD_TOKEN or not GUILD_ID or int(user_id or 0) <= 0:
        return []
    url = f"{DISCORD_API}/guilds/{GUILD_ID}/members/{int(user_id)}"
    headers = {"Authorization": f"Bot {DISCORD_TOKEN}"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status != 200:
                    return []
                js = await r.json()
                roles = js.get("roles") if isinstance(js, dict) else None
                if not isinstance(roles, list):
                    return []
                return [str(x) for x in roles]
    except Exception:
        return []


def _member_roles_include_premium(role_ids: list[str]) -> bool:
    ids = {str(x) for x in role_ids}
    if MONTHLY_ROLE_ID and str(MONTHLY_ROLE_ID) in ids:
        return True
    if LIFETIME_ROLE_ID and str(LIFETIME_ROLE_ID) in ids:
        return True
    return False


async def _discord_grant_role(session: aiohttp.ClientSession, user_id: int, role_id: int) -> tuple[bool, str]:
    """PUT /guilds/{guild}/members/{user}/roles/{role} via bot token."""
    if not DISCORD_TOKEN or not GUILD_ID or not role_id:
        return False, "Discord credentials not configured on server."
    url = f"{DISCORD_API}/guilds/{GUILD_ID}/members/{user_id}/roles/{role_id}"
    headers = {"Authorization": f"Bot {DISCORD_TOKEN}"}
    try:
        async with session.put(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 204:
                return True, "Role assigned."
            if r.status == 403:
                return False, "Bot lacks permission to assign roles."
            if r.status == 404:
                return False, "User not found — please join the Discord server first, then submit again."
            body = await r.text()
            return False, f"Discord API error {r.status}: {body[:200]}"
    except Exception as e:
        return False, f"Discord API unreachable: {e}"


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    html_file = WEBSITE_DIR / "index.html"
    if html_file.exists():
        return HTMLResponse(content=html_file.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>Velcor3</h1><p>Place website/index.html next to website_server.py.</p>")


def _inject_gate_flag(html: str) -> str:
    """
    Mark a page as "needs access" so the frontend can show the tweet-gate modal.
    We intentionally still serve the page HTML so the UI can appear blurred behind the modal.
    """
    marker = "<script>window.NA_NEEDS_GATE=true;</script>"
    # Only skip if we've already injected the *true* flag (pages may reference NA_NEEDS_GATE in their JS).
    if "window.NA_NEEDS_GATE=true" in html or marker in html:
        return html
    if "</head>" in html:
        return html.replace("</head>", marker + "\n</head>", 1)
    if "</body>" in html:
        return html.replace("</body>", marker + "\n</body>", 1)
    return marker + "\n" + html


def _serve_page(request: Request, filename: str) -> HTMLResponse:
    needs_gate = (not _DEV_PREVIEW) and (not _has_access(request))
    html_file = WEBSITE_DIR / filename
    if html_file.exists():
        html = html_file.read_text(encoding="utf-8")
        if needs_gate:
            html = _inject_gate_flag(html)
        return HTMLResponse(content=html)
    return HTMLResponse(f"<h1>Missing website/{filename}</h1>", status_code=500)


@app.get("/projects", response_class=HTMLResponse)
async def page_projects(request: Request):
    return _serve_page(request, "projects.html")


@app.get("/kol-alerts", response_class=HTMLResponse)
async def page_kol_alerts(request: Request):
    return _serve_page(request, "kol_alerts.html")


@app.get("/telegram", response_class=HTMLResponse)
async def page_telegram(request: Request):
    return _serve_page(request, "telegram.html")


@app.get("/daily-finds", response_class=HTMLResponse)
async def page_daily_finds(request: Request):
    return _serve_page(request, "daily_finds.html")


@app.get("/early-access", response_class=HTMLResponse)
async def early_access():
    if EARLY_ACCESS_HTML.exists():
        return HTMLResponse(content=EARLY_ACCESS_HTML.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>Early Access</h1><p>Missing website/early_access.html</p>", status_code=503)


@app.get("/api/info")
async def api_info():
    """Return wallet addresses, prices, and tier amounts for the frontend."""
    async with aiohttp.ClientSession() as session:
        eth_usd, sol_usd = await asyncio.gather(
            _fetch_eth_price_usd(session),
            _fetch_sol_price_usd(session),
        )

    monthly_usd = float(getattr(config, "PAYMENT_PANEL_PRICE_USD", 30.0))
    lifetime_usd = monthly_usd * 6  # 6-month equivalent as default lifetime display

    def _usd_to_eth(usd: float) -> str:
        return f"{usd / eth_usd:.4f}" if eth_usd > 0 else "—"

    def _usd_to_sol(usd: float) -> str:
        return f"{usd / sol_usd:.3f}" if sol_usd > 0 else "—"

    return {
        "guild_id": GUILD_ID,
        "discord_invite": "https://discord.gg/MDqvrdhFDY",
        "wallets": {
            "eth_mainnet": config.PAYMENT_TREASURY_ETH_MAINNET or None,
            "eth_base": config.PAYMENT_TREASURY_ETH_BASE or None,
            "solana": config.PAYMENT_TREASURY_SOL or None,
        },
        "prices_usd": {
            "eth": round(eth_usd, 2),
            "sol": round(sol_usd, 2),
        },
        "tiers": {
            "monthly": {
                "usd": monthly_usd,
                "eth": _usd_to_eth(monthly_usd),
                "sol": _usd_to_sol(monthly_usd),
                "min_eth_mainnet": config.PAYMENT_MONTHLY_MIN_ETH_MAINNET,
                "min_eth_base": config.PAYMENT_MONTHLY_MIN_ETH_BASE,
                "min_sol": config.PAYMENT_MONTHLY_MIN_SOL,
            },
            "lifetime": {
                "usd": lifetime_usd,
                "eth": _usd_to_eth(lifetime_usd),
                "sol": _usd_to_sol(lifetime_usd),
                "min_eth_mainnet": config.PAYMENT_LIFETIME_MIN_ETH_MAINNET,
                "min_eth_base": config.PAYMENT_LIFETIME_MIN_ETH_BASE,
                "min_sol": config.PAYMENT_LIFETIME_MIN_SOL,
            },
        },
    }


@app.get("/api/debug/access")
async def api_debug_access(request: Request):
    """Debug helper: show access gate state."""
    return {
        "dev_preview": _DEV_PREVIEW,
        "has_access": _has_access(request),
        "has_cookie": bool(request.cookies.get(ACCESS_COOKIE, "")),
    }


@app.get("/api/feed/events")
async def api_feed_events(request: Request, limit: int = 120, kinds: str = ""):
    """Website dashboard feed (requires access cookie)."""
    if not _DEV_PREVIEW and not _has_access(request):
        raise HTTPException(401, "Unauthorized")
    ks = [k.strip() for k in (kinds or "").split(",") if k.strip()]
    return {"events": feed_events.list_events(limit=limit, kinds=ks or None)}


def _age_days_from_created_at(created_at: Optional[str]) -> Optional[int]:
    if not created_at:
        return None
    try:
        s = str(created_at).replace("Z", "+00:00").strip()
        # Common formats from X libs: "2026-04-22 12:34:56+00:00" or ISO
        from datetime import datetime, timezone

        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            # X/Twitter created_at often looks like: "Wed Apr 22 12:34:56 +0000 2026"
            try:
                dt = datetime.strptime(s, "%a %b %d %H:%M:%S %z %Y")
            except Exception:
                # last resort: trim trailing timezone name fragments
                dt = datetime.fromisoformat(s.split(" ", 1)[0])
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        days = int((now - dt.astimezone(timezone.utc)).total_seconds() // 86400)
        return max(0, days)
    except Exception:
        return None


def _parse_event_ts(ts: Optional[str]):
    if not ts:
        return None
    try:
        from datetime import datetime, timezone

        s = str(ts).replace("Z", "+00:00").strip()
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _recent_escalation_handles(*, hours: float = 24.0, limit: int = 400) -> set[str]:
    """
    Collect handles that appeared in recent escalation events.
    Used to avoid duplicating escalation projects in the Finds list.
    """
    hs: set[str] = set()
    try:
        evs = feed_events.list_events(limit=max(50, min(900, int(limit or 400))), kinds=["escalation"])
        from datetime import datetime, timezone, timedelta

        cutoff = datetime.now(timezone.utc) - timedelta(hours=float(hours or 24.0))
        for ev in evs or []:
            ts = _parse_event_ts((ev or {}).get("ts"))
            if ts and ts < cutoff:
                continue
            extra = (ev or {}).get("extra") or {}
            h = (extra.get("handle") or "").strip().lstrip("@").lower()
            if not h:
                title = str((ev or {}).get("title") or "")
                m = re.search(r"@([A-Za-z0-9_]{1,20})", title)
                if m:
                    h = (m.group(1) or "").strip().lower()
            if h:
                hs.add(h)
    except Exception:
        return set()
    return hs


def _latest_profile_map(*, limit: int = 400) -> dict[str, dict]:
    """
    Best-effort lookup for avatar/banner urls by handle from recent feed events.
    Uses discovery + escalation events since those include pfp/banner extras.
    """
    mp: dict[str, dict] = {}
    try:
        evs = feed_events.list_events(limit=max(50, min(800, int(limit or 400))), kinds=["discovery", "escalation"])
        for ev in evs:
            extra = (ev or {}).get("extra") or {}
            h = (extra.get("handle") or "").strip().lstrip("@").lower()
            if not h:
                continue
            if h in mp:
                continue
            mp[h] = {
                "pfp_url": str(extra.get("pfp_url") or ""),
                "banner_url": str(extra.get("banner_url") or ""),
                "age_days": extra.get("age_days"),
                "followers": extra.get("followers"),
            }
    except Exception:
        mp = {}
    return mp


@app.get("/api/projects/trending")
async def api_projects_trending(request: Request, limit: int = 5):
    if not _DEV_PREVIEW and not _has_access(request):
        raise HTTPException(401, "Unauthorized")
    limit = max(1, min(50, int(limit or 5)))
    # Trending: distinct HVAs over the last 30 days (computed from follows table).
    rows = database.get_trending_projects_30d(limit=limit)
    if not rows:
        # Fallback: some DBs don't populate first_seen_at reliably, which makes
        # database.get_trending_projects() return empty. Use alerted+smarts directly.
        try:
            import sqlite3

            conn = sqlite3.connect(database.DB_PATH)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT twitter_id, handle, name, description, created_at, last_posted_smarts
                FROM projects
                WHERE alerted_at IS NOT NULL
                  AND COALESCE(alerted_discord, 0) = 1
                ORDER BY last_posted_smarts DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = cur.fetchall()
            conn.close()
        except Exception:
            rows = []
    prof = _latest_profile_map()
    out = []
    max_age_days = int(getattr(config, "SNIPER_MAX_AGE_DAYS", 90) or 90)
    for row in rows:
        # Normalize to: (twitter_id, handle, name, desc, created_at, smarts_30d)
        if isinstance(row, (list, tuple)):
            r = list(row) + [None] * 10
            twitter_id, handle, name, desc, created_at, smarts = r[0], r[1], r[2], r[3], r[4], r[5]
        else:
            twitter_id, handle, name, desc, created_at, smarts = "", "", "", "", "", 0
        hkey = str(handle or "").strip().lstrip("@").lower()
        p = prof.get(hkey) or {}
        age_days = _age_days_from_created_at(created_at)
        if age_days is None:
            try:
                age_days = int(p.get("age_days")) if p.get("age_days") is not None else None
            except Exception:
                age_days = None
        # Enforce "only recent projects" rule for the website trending list.
        # If age is unknown, keep it (better than dropping everything); otherwise filter by max_age_days.
        if isinstance(age_days, int) and age_days > max_age_days:
            continue
        pfp_url = p.get("pfp_url") or ""
        if not pfp_url and handle:
            # Fallback avatar if we haven't seen a discovery/escalation event yet.
            pfp_url = f"https://unavatar.io/twitter/{str(handle).lstrip('@')}"
        out.append(
            {
                "twitter_id": str(twitter_id),
                "handle": str(handle or ""),
                "name": str(name or ""),
                "description": str(desc or ""),
                "created_at": str(created_at or ""),
                "age_days": age_days,
                "hva_smarts": int(smarts or 0),
                "url": f"https://x.com/{handle}" if handle else "",
                "pfp_url": pfp_url,
                "banner_url": p.get("banner_url") or "",
            }
        )
    return {"items": out[:limit]}


@app.get("/api/projects/new")
async def api_projects_new(request: Request, limit: int = 5):
    """Newest alerted projects (last 24h), newest first."""
    if not _DEV_PREVIEW and not _has_access(request):
        raise HTTPException(401, "Unauthorized")
    limit = max(1, min(10, int(limit or 5)))
    rows = database.get_projects_finds_24h(limit=limit)
    out = []
    for twitter_id, handle, name, desc, created_at, alerted_at, cat, summ, followers in rows:
        out.append(
            {
                "twitter_id": str(twitter_id),
                "handle": str(handle or ""),
                "name": str(name or ""),
                "description": str(desc or ""),
                "created_at": str(created_at or ""),
                "alerted_at": str(alerted_at or ""),
                "age_days": _age_days_from_created_at(created_at),
                "followers": int(followers or 0) if followers is not None else None,
                "category": str(cat or ""),
                "summary": str(summ or ""),
                "url": f"https://x.com/{handle}" if handle else "",
            }
        )
    return {"items": out}


@app.get("/api/projects/finds")
async def api_projects_finds(request: Request, limit: int = 50, exclude_escalations: int = 1):
    """Main finds list for the Projects page (alerted only, last 24h)."""
    if not _DEV_PREVIEW and not _has_access(request):
        raise HTTPException(401, "Unauthorized")
    limit = max(10, min(200, int(limit or 50)))
    rows = database.get_projects_finds_24h(limit=limit)
    esc_handles = _recent_escalation_handles(hours=24.0, limit=400) if int(exclude_escalations or 0) else set()
    prof = _latest_profile_map()
    out = []
    for twitter_id, handle, name, desc, created_at, alerted_at, cat, summ, followers in rows:
        hnorm = str(handle or "").strip().lstrip("@").lower()
        if esc_handles and hnorm and hnorm in esc_handles:
            continue
        hkey = str(handle or "").strip().lstrip("@").lower()
        p = prof.get(hkey) or {}
        age_days = _age_days_from_created_at(created_at)
        if age_days is None:
            try:
                age_days = int(p.get("age_days")) if p.get("age_days") is not None else None
            except Exception:
                age_days = None
        out.append(
            {
                "twitter_id": str(twitter_id),
                "handle": str(handle or ""),
                "name": str(name or ""),
                "description": str(desc or ""),
                "created_at": str(created_at or ""),
                "alerted_at": str(alerted_at or ""),
                "age_days": age_days,
                "followers": int(followers or 0) if followers is not None else None,
                "category": str(cat or ""),
                "summary": str(summ or ""),
                "url": f"https://x.com/{handle}" if handle else "",
                "pfp_url": p.get("pfp_url") or "",
                "banner_url": p.get("banner_url") or "",
            }
        )
    return {"items": out}


@app.get("/api/daily-finds")
async def api_daily_finds(request: Request, day: str = "", limit: int = 200):
    """
    Daily finds list for a chosen UTC day (YYYY-MM-DD). Requires access cookie.
    """
    if not _DEV_PREVIEW and not _has_access(request):
        raise HTTPException(401, "Unauthorized")
    day = (day or "").strip()
    limit = max(1, min(500, int(limit or 200)))
    from datetime import datetime, timedelta, timezone

    try:
        if not day:
            d0 = datetime.now(timezone.utc).date()
        else:
            d0 = datetime.strptime(day, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(400, "Invalid day (expected YYYY-MM-DD)")

    start_dt = datetime(d0.year, d0.month, d0.day, tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1)
    rows = database.get_projects_alerted_between_utc(start_dt.isoformat(), end_dt.isoformat(), limit=limit)
    prof = _latest_profile_map()
    out = []
    for twitter_id, handle, name, desc, created_at, alerted_at, cat, summ, followers in rows:
        hkey = str(handle or "").strip().lstrip("@").lower()
        p = prof.get(hkey) or {}
        age_days = _age_days_from_created_at(created_at)
        if age_days is None:
            try:
                age_days = int(p.get("age_days")) if p.get("age_days") is not None else None
            except Exception:
                age_days = None
        pfp_url = p.get("pfp_url") or ""
        if not pfp_url and handle:
            pfp_url = f"https://unavatar.io/twitter/{str(handle).lstrip('@')}"
        out.append(
            {
                "twitter_id": str(twitter_id),
                "handle": str(handle or ""),
                "name": str(name or ""),
                "description": str(desc or ""),
                "created_at": str(created_at or ""),
                "alerted_at": str(alerted_at or ""),
                "age_days": age_days,
                "followers": int(followers or 0) if followers is not None else None,
                "category": str(cat or ""),
                "summary": str(summ or ""),
                "url": f"https://x.com/{handle}" if handle else "",
                "pfp_url": pfp_url,
            }
        )
    return {"day": start_dt.date().isoformat(), "items": out}


def _kolfi_alert_watchlist_by_mint() -> dict:
    """Mint → entrée watchlist (jetons pour lesquels le bot a posté une alerte)."""
    try:
        p = getattr(kolfi, "ALERT_WATCHLIST_PATH", "")
        watch_path = Path(str(p)) if p else DATA_DIR / "kolfi_alert_watchlist.json"
        if not watch_path.exists():
            return {}
        watch = json.loads(watch_path.read_text(encoding="utf-8"))
        bm = watch.get("by_mint") if isinstance(watch, dict) else None
        return bm if isinstance(bm, dict) else {}
    except Exception:
        return {}


def _token_alert_rollup_from_db(*, days: int = 0, limit_events: int = 8000) -> dict[str, dict]:
    """
    Build mint-level rollup from persisted token_alert feed events.
    Source of truth for "our alerts" history (DB-backed, not JSON watchlist).
    """
    from datetime import datetime, timezone, timedelta

    out: dict[str, dict] = {}
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(days=max(0, int(days)))) if int(days or 0) > 0 else None
    evs = feed_events.list_events(limit=max(100, min(20000, int(limit_events or 8000))), kinds=["token_alert"])
    # list_events() returns newest first; iterate oldest->newest for stable first_alert fields.
    for ev in reversed(evs or []):
        extra = (ev or {}).get("extra") or {}
        mint = str(extra.get("mint") or "").strip()
        if not mint:
            continue
        ev_ts = str(extra.get("alert_ts") or (ev or {}).get("ts") or "").strip()
        ev_dt = _parse_iso_dt(ev_ts)
        if cutoff is not None and (ev_dt is None or ev_dt < cutoff):
            continue

        symbol = str(extra.get("symbol") or extra.get("name") or "").strip()
        alert_mc = kolfi._safe_float(extra.get("alert_mc"))  # type: ignore[attr-defined]
        if alert_mc is None or alert_mc <= 0:
            continue

        row = out.get(mint)
        if not isinstance(row, dict):
            out[mint] = {
                "mint": mint,
                "ticker": symbol or "—",
                "first_alert_ts": ev_ts,
                "last_alert_ts": ev_ts,
                "alert_count": 1,
                "first_alert_mc": alert_mc,
                "last_alert_mc": alert_mc,
            }
            continue

        row["last_alert_ts"] = ev_ts or row.get("last_alert_ts") or ""
        row["last_alert_mc"] = alert_mc
        row["alert_count"] = int(row.get("alert_count") or 0) + 1
        if symbol and (not str(row.get("ticker") or "").strip() or str(row.get("ticker")) == "—"):
            row["ticker"] = symbol
    return out


def _parse_iso_dt(s: str):
    try:
        from datetime import datetime, timezone

        if not s:
            return None
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _dashboard_bucket_thresholds() -> tuple[float, float]:
    """(low_max, mid_max) for MC@call tiers: <low_max, [low_max, mid_max), >=mid_max -> 1m column."""
    try:
        low_max = float(os.getenv("KOL_DASHBOARD_LOW_MAX_USD", "50000") or 50000)
    except Exception:
        low_max = 50000.0
    try:
        mid_max = float(os.getenv("KOL_DASHBOARD_MID_MAX_USD", "1000000") or 1000000)
    except Exception:
        mid_max = 1000000.0
    low_max = max(1000.0, low_max)
    mid_max = max(low_max + 1.0, mid_max)
    return low_max, mid_max


def _dashboard_bucket_by_call_mc(call_mc: Optional[float], low_max: float, mid_max: float) -> str:
    """Map MC@call to dashboard column keys low / 100k / 1m (names kept for API compatibility)."""
    v = kolfi._safe_float(call_mc)  # type: ignore[attr-defined]
    if v is None or v <= 0:
        return "low"
    if v < low_max:
        return "low"
    if v < mid_max:
        return "100k"
    return "1m"


@app.get("/api/kol/dashboard")
async def api_kol_dashboard(
    request: Request,
    hours: float = 24.0,
    top: int = 5,
    per_bucket: int = 5,
):
    """
    Website dashboard for KOL alerts:
    - Top (24h window): mints with a site token_alert in the window, ranked by ATHx.
    - Buckets: same window, deduped by mint, columns split by MC@call (env tier thresholds).
    """
    if not _DEV_PREVIEW and not _has_access(request):
        raise HTTPException(401, "Unauthorized")
    api_key = str(getattr(config, "KOLFI_API_KEY", "") or "").strip()
    if not api_key:
        raise HTTPException(400, "Missing KOLFI_API_KEY")
    hours = float(hours or 24.0)
    top = max(1, min(25, int(top or 5)))
    per_bucket = max(1, min(25, int(per_bucket or 5)))

    cache_key = f"kol_dashboard:v5_feed_only:{hours:.2f}:{top}:{per_bucket}"

    async def _build():
        from datetime import datetime, timezone, timedelta

        async with aiohttp.ClientSession() as session:
            items, err = await kolfi.fetch_tokens_overview(
                session,
                api_key,
                limit=150,
                include_calls=50,
                max_pages=28,
            )
            if err and not items:
                raise HTTPException(502, err)

            # Map mint -> item (latest snapshot)
            by_mint: dict[str, dict] = {}
            for it in items or []:
                if isinstance(it, dict):
                    m = kolfi._item_mint(it)  # type: ignore[attr-defined]
                    if m:
                        by_mint[m] = it

            now = datetime.now(timezone.utc)
            cutoff_top = now - timedelta(hours=hours)

            async def _row_leaderboard(
                mint: str,
                *,
                site_alert_ts: str,
                ent_opt: Optional[dict],
            ) -> Optional[dict]:
                """Leaderboard row: prefer Kolfi overview snapshot; else watchlist snapshot (mint not on current board)."""
                if not mint:
                    return None
                tick = "—"
                if isinstance(ent_opt, dict):
                    tick = str(ent_opt.get("ticker") or "—")
                snap = by_mint.get(mint) or {}
                if tick == "—" and snap:
                    tick = kolfi._item_ticker(snap)  # type: ignore[attr-defined]

                ath_mc = kolfi._safe_float((snap or {}).get("ath_market_cap"))  # type: ignore[attr-defined]
                if (ath_mc is None or ath_mc <= 0) and isinstance(ent_opt, dict):
                    ath_mc = kolfi._safe_float(ent_opt.get("last_kolfi_ath_usd"))  # type: ignore[attr-defined]
                if (ath_mc is None or ath_mc <= 0) and isinstance(ent_opt, dict):
                    ath_mc = kolfi._safe_float(ent_opt.get("baseline_ath_usd"))  # type: ignore[attr-defined]

                fc_raw = None
                if snap:
                    try:
                        fc_raw = kolfi._first_call_with_mc(snap)  # type: ignore[attr-defined]
                    except Exception:
                        fc_raw = None
                call: Optional[dict] = _persist_first_call_for_mint(mint, fc_raw) or fc_raw or _get_persisted_first_call(mint)  # type: ignore[assignment]
                if not isinstance(call, dict) and isinstance(ent_opt, dict):
                    lc = ent_opt.get("last_call")
                    if isinstance(lc, dict):
                        cm = kolfi._safe_float(lc.get("callMarketCap"))  # type: ignore[attr-defined]
                        if cm and cm > 0:
                            call = dict(lc)
                            if not call.get("kolUsername") and lc.get("who"):
                                call["kolUsername"] = str(lc.get("who"))
                if not isinstance(call, dict) and isinstance(ent_opt, dict):
                    bcm = kolfi._safe_float(ent_opt.get("baseline_mc_usd"))  # type: ignore[attr-defined]
                    if bcm and bcm > 0:
                        lc = ent_opt.get("last_call") if isinstance(ent_opt.get("last_call"), dict) else {}
                        call = {
                            "callMarketCap": bcm,
                            "kolUsername": str((lc or {}).get("who") or "caller"),
                            "kolXId": (lc or {}).get("kolXId") or (lc or {}).get("kol_x_id"),
                        }
                if not isinstance(call, dict):
                    return None

                call_mc = kolfi._safe_float(call.get("callMarketCap"))  # type: ignore[attr-defined]
                if call_mc is None or call_mc <= 0:
                    return None

                cur_mc = kolfi._safe_float((snap or {}).get("last_market_cap"))  # type: ignore[attr-defined]
                if (cur_mc is None or cur_mc <= 0) and isinstance(ent_opt, dict):
                    cur_mc = kolfi._safe_float(ent_opt.get("last_kolfi_mc_usd"))  # type: ignore[attr-defined]

                since = (cur_mc / call_mc) if (cur_mc and call_mc > 0) else None
                ath_x = (ath_mc / call_mc) if (ath_mc and call_mc > 0) else None
                if ath_x is None or ath_x <= 0:
                    return None

                caller = ""
                try:
                    caller = kolfi._call_label(call)  # type: ignore[attr-defined]
                except Exception:
                    caller = str(call.get("who") or call.get("kolUsername") or "caller")
                caller_x = str(call.get("kolXId") or call.get("kol_x_id") or "").strip()

                chart = f"https://gmgn.ai/sol/token/{mint}" if mint else ""
                dex = str((snap or {}).get("dexscreener_url") or (snap or {}).get("dexUrl") or "")
                thumb = await _thumb_url_cached(session, snap or {}, mint) if mint else ""
                alert_ts = (site_alert_ts or "").strip() or (
                    str((ent_opt or {}).get("first_alert_ts") or "").strip()
                    if isinstance(ent_opt, dict)
                    else ""
                )
                return {
                    "ticker": tick,
                    "mint": mint,
                    "caller": caller,
                    "caller_x": caller_x,
                    "call_mc": call_mc,
                    "cur_mc": cur_mc,
                    "ath_mc": ath_mc,
                    "since_x": since,
                    "ath_x": ath_x,
                    "call_ts": alert_ts,
                    "chart_url": chart,
                    "dex_url": dex,
                    "thumb_url": thumb or "",
                }

            evs_top = feed_events.list_events(limit=800, kinds=["token_alert"])
            watch_by_mint = _kolfi_alert_watchlist_by_mint()
            mint_site_ts: dict[str, str] = {}
            mint_order: list[str] = []
            seen_ev_mint: set[str] = set()
            for ev in evs_top:
                ev_dt = _parse_iso_dt(str((ev or {}).get("ts") or ""))
                if not ev_dt or ev_dt < cutoff_top:
                    continue
                extra = (ev or {}).get("extra") or {}
                mint = str(extra.get("mint") or "").strip()
                if not mint or mint in seen_ev_mint:
                    continue
                seen_ev_mint.add(mint)
                mint_order.append(mint)
                mint_site_ts[mint] = str((ev or {}).get("ts") or "").strip()

            rows_top: list = []
            for mint in mint_order:
                ent = watch_by_mint.get(mint) if isinstance(watch_by_mint.get(mint), dict) else None
                row = await _row_leaderboard(
                    mint,
                    site_alert_ts=mint_site_ts.get(mint, ""),
                    ent_opt=ent,
                )
                if row:
                    rows_top.append(row)

            rows_top.sort(key=lambda r: float(r.get("ath_x") or 0), reverse=True)
            top_rows = rows_top[:top]

            low_max, mid_max = _dashboard_bucket_thresholds()
            evs = feed_events.list_events(limit=800, kinds=["token_alert"])

            seen_bucket: set[str] = set()
            staged: dict[str, list] = {"low": [], "100k": [], "1m": []}
            for ev in evs:
                ev_dt = _parse_iso_dt(str((ev or {}).get("ts") or ""))
                if not ev_dt or ev_dt < cutoff_top:
                    continue
                extra = (ev or {}).get("extra") or {}
                mint = str(extra.get("mint") or "").strip()
                if not mint or mint in seen_bucket:
                    continue

                it = by_mint.get(mint) or {}
                ent_b = watch_by_mint.get(mint) if isinstance(watch_by_mint.get(mint), dict) else None
                fc_raw = kolfi._first_call_with_mc(it) if it else None  # type: ignore[attr-defined]
                fc = _persist_first_call_for_mint(mint, fc_raw) or fc_raw or _get_persisted_first_call(mint)
                if not isinstance(fc, dict) and isinstance(ent_b, dict):
                    lc = ent_b.get("last_call")
                    if isinstance(lc, dict):
                        cm = kolfi._safe_float(lc.get("callMarketCap"))  # type: ignore[attr-defined]
                        if cm and cm > 0:
                            fc = dict(lc)
                            if not fc.get("kolUsername") and lc.get("who"):
                                fc["kolUsername"] = str(lc.get("who"))
                if not isinstance(fc, dict) and isinstance(ent_b, dict):
                    bcm = kolfi._safe_float(ent_b.get("baseline_mc_usd"))  # type: ignore[attr-defined]
                    if bcm and bcm > 0:
                        lc = ent_b.get("last_call") if isinstance(ent_b.get("last_call"), dict) else {}
                        fc = {
                            "callMarketCap": bcm,
                            "kolUsername": str((lc or {}).get("who") or "caller"),
                            "kolXId": (lc or {}).get("kolXId") or (lc or {}).get("kol_x_id"),
                        }
                # ── OUR ALERT MC (what MC was when WE posted the alert) ──────────
                # Priority: extra.alert_mc (saved since fix) → watchlist baseline_mc_usd
                # NEVER use callMarketCap from Kolfi (= the Telegram KOL's original call,
                # which can be days/weeks old at a much lower MC than our actual alert MC).
                call_mc = kolfi._safe_float(extra.get("alert_mc"))  # type: ignore[attr-defined]
                if call_mc is None or call_mc <= 0:
                    call_mc = kolfi._safe_float((ent_b or {}).get("baseline_mc_usd"))  # type: ignore[attr-defined]
                if call_mc is None or call_mc <= 0:
                    # Absolute last resort: Kolfi KOL first-call (may be approximate)
                    call_mc = kolfi._safe_float((fc or {}).get("callMarketCap")) if isinstance(fc, dict) else None  # type: ignore[attr-defined]
                if call_mc is None or call_mc <= 0:
                    continue

                seen_bucket.add(mint)
                bucket = _dashboard_bucket_by_call_mc(call_mc, low_max, mid_max)

                cur_mc = kolfi._safe_float(it.get("last_market_cap"))  # type: ignore[attr-defined]
                if (cur_mc is None or cur_mc <= 0) and isinstance(ent_b, dict):
                    cur_mc = kolfi._safe_float(ent_b.get("last_kolfi_mc_usd"))  # type: ignore[attr-defined]
                ath_mc = kolfi._safe_float(it.get("ath_market_cap"))  # type: ignore[attr-defined]
                if (ath_mc is None or ath_mc <= 0) and isinstance(ent_b, dict):
                    ath_mc = kolfi._safe_float(ent_b.get("last_kolfi_ath_usd"))  # type: ignore[attr-defined]
                if (ath_mc is None or ath_mc <= 0) and isinstance(ent_b, dict):
                    ath_mc = kolfi._safe_float(ent_b.get("baseline_ath_usd"))  # type: ignore[attr-defined]
                since = (cur_mc / call_mc) if (call_mc and call_mc > 0 and cur_mc and cur_mc > 0) else None
                ath_x = (ath_mc / call_mc) if (call_mc and call_mc > 0 and ath_mc and ath_mc > 0) else None
                dex = str(it.get("dexscreener_url") or it.get("dexUrl") or ev.get("url") or "")
                chart = f"https://gmgn.ai/sol/token/{mint}" if mint else ""
                caller_label = kolfi._call_label(fc) if isinstance(fc, dict) else ""  # type: ignore[attr-defined]
                caller_x = str((fc or {}).get("kolXId") or (fc or {}).get("kol_x_id") or "").strip()
                thumb_url = str(extra.get("thumb_url") or "") or (await _thumb_url_cached(session, it, mint) if mint else "")
                # ── AUTHORITATIVE ALERT TIMESTAMP ────────────────────────────────
                # Use extra.alert_ts (exact moment bot fired alert) → ev.ts (feed event ts)
                call_ts = str(extra.get("alert_ts") or "").strip() or str((ev or {}).get("ts") or "").strip()
                row_b = {
                    "event_id": ev.get("id"),
                    "ts": ev.get("ts"),
                    "call_ts": call_ts,
                    "title": ev.get("title"),
                    "mint": mint,
                    "ticker": str(extra.get("symbol") or extra.get("name") or "").strip()
                    or kolfi._item_ticker(it),  # type: ignore[attr-defined]
                    "caller": caller_label or (caller_x or ""),
                    "caller_x": caller_x,
                    "call_mc": call_mc,
                    "cur_mc": cur_mc,
                    "ath_mc": ath_mc,
                    "since_x": since,
                    "ath_x": ath_x,
                    "chart_url": chart,
                    "dex_url": dex,
                    "thumb_url": thumb_url,
                }
                staged[bucket].append((ev_dt, row_b))

            buckets_out = {"low": [], "100k": [], "1m": []}
            for key in ("low", "100k", "1m"):
                arr = staged.get(key) or []
                arr.sort(key=lambda t: t[0], reverse=True)
                buckets_out[key] = [r for _, r in arr[:per_bucket]]

            return {"top": top_rows, "buckets": buckets_out}
    # Cache for 20 seconds; frontend refresh is also throttled.
    return await _cache_get_or_set(cache_key, 20.0, _build)


@app.get("/api/kol/top-performers")
async def api_kol_top_performers(request: Request, days: int = 30, top: int = 10):
    """
    Top performing coins within a rolling window (default 30d), ranked by ATH multiple
    from first call MC → ATH MC (ATHx).
    """
    if not _DEV_PREVIEW and not _has_access(request):
        raise HTTPException(401, "Unauthorized")
    api_key = str(getattr(config, "KOLFI_API_KEY", "") or "").strip()
    if not api_key:
        raise HTTPException(400, "Missing KOLFI_API_KEY")
    days = max(1, min(180, int(days or 30)))
    top = max(1, min(25, int(top or 10)))
    hours = float(days) * 24.0

    cache_key = f"kol_top_performers:{days}:{top}"

    async def _build():
        async with aiohttp.ClientSession() as session:
            items, err = await kolfi.fetch_tokens_overview(
                session,
                api_key,
                limit=100,
                include_calls=50,
                max_pages=8,
            )
            if err and not items:
                raise HTTPException(502, err)

        scored = []
        for it in items or []:
            if not isinstance(it, dict):
                continue
            row = kolfi._entry_for_leaderboard(it, max_call_age_hours=hours)  # type: ignore[attr-defined]
            if row:
                scored.append(row)

        def _athx_key(x: dict) -> float:
            try:
                ath = float(x.get("ath_usd") or 0)
                call = float(x.get("call_mc") or 0)
                if ath > 0 and call > 0:
                    return ath / call
            except Exception:
                pass
            return 0.0

        scored.sort(key=_athx_key, reverse=True)
        scored = [s for s in scored if _athx_key(s) > 0][:top]

        out = []
        for r in scored:
            it = r.get("item") or {}
            bc = r.get("best_call") or {}
            mint = str(r.get("mint") or "")
            ticker = str(r.get("ticker") or "—")
            call_mc = kolfi._safe_float(r.get("call_mc"))  # type: ignore[attr-defined]
            cur_mc = kolfi._safe_float(r.get("cur_mc"))  # type: ignore[attr-defined]
            ath_mc = kolfi._safe_float(r.get("ath_usd"))  # type: ignore[attr-defined]
            since = (cur_mc / call_mc) if (call_mc and call_mc > 0 and cur_mc and cur_mc > 0) else None
            ath_x = (ath_mc / call_mc) if (call_mc and call_mc > 0 and ath_mc and ath_mc > 0) else None
            caller = kolfi._call_label(bc) if isinstance(bc, dict) else ""  # type: ignore[attr-defined]
            caller_x = str((bc or {}).get("kolXId") or "").strip()
            chart = f"https://gmgn.ai/sol/token/{mint}" if mint else ""
            dex = str(it.get("dexscreener_url") or it.get("dexUrl") or "")
            thumb = await _thumb_url_cached(session, it, mint) if mint else ""
            out.append(
                {
                    "ticker": ticker,
                    "mint": mint,
                    "caller": caller,
                    "caller_x": caller_x,
                    "call_mc": call_mc,
                    "cur_mc": cur_mc,
                    "ath_mc": ath_mc,
                    "since_x": since,
                    "ath_x": ath_x,
                    "call_ts": (bc or {}).get("messageTs") or "",
                    "chart_url": chart,
                    "dex_url": dex,
                    "thumb_url": thumb or "",
                }
            )
        return {"days": days, "items": out}

    return await _cache_get_or_set(cache_key, 60.0, _build)


@app.get("/api/kol/alerts/top-performers")
async def api_kol_alerts_top_performers(request: Request, days: int = 30, top: int = 10):
    """
    Top performers among tokens we alerted (DB-backed token_alert events), ranked by ATHx
    from OUR first alert MC (ATH MC / first_alert_mc).
    Use days=0 for all-time (no cutoff); days>0 = first alert within rolling window.
    """
    if not _DEV_PREVIEW and not _has_access(request):
        raise HTTPException(401, "Unauthorized")
    api_key = str(getattr(config, "KOLFI_API_KEY", "") or "").strip()
    if not api_key:
        raise HTTPException(400, "Missing KOLFI_API_KEY")
    # Note: do not use `days or 30` — 0 is a valid value meaning all-time.
    days = max(0, min(3650, int(days)))
    top = max(1, min(25, int(top or 10)))

    rollup = _token_alert_rollup_from_db(days=days, limit_events=10000)
    if not rollup:
        return {"days": days, "all_time": days <= 0, "items": []}
    entries = list(rollup.values())

    cache_key = f"kol_alerts_top:{days}:{top}"

    async def _build():
        async with aiohttp.ClientSession() as session:
            items, err = await kolfi.fetch_tokens_overview(
                session,
                api_key,
                limit=100,
                include_calls=30,
                max_pages=8,
            )
            if err and not items:
                raise HTTPException(502, err)

            by_snap: dict[str, dict] = {}
            for it in items or []:
                if not isinstance(it, dict):
                    continue
                m = kolfi._item_mint(it)  # type: ignore[attr-defined]
                if m:
                    by_snap[m] = it

            rows = []
            for ent in entries:
                mint = str(ent.get("mint") or "").strip()
                if not mint:
                    continue
                tick = str(ent.get("ticker") or "—")

                snap = by_snap.get(mint) or {}
                ath_mc = kolfi._safe_float((snap or {}).get("ath_market_cap"))  # type: ignore[attr-defined]
                if ath_mc is None or ath_mc <= 0:
                    continue

                alert_mc = kolfi._safe_float(ent.get("first_alert_mc"))  # type: ignore[attr-defined]
                if alert_mc is None or alert_mc <= 0:
                    continue

                cur_mc = kolfi._safe_float((snap or {}).get("last_market_cap"))  # type: ignore[attr-defined]
                since = (cur_mc / alert_mc) if (cur_mc and cur_mc > 0) else None
                ath_x = (ath_mc / alert_mc) if (ath_mc and ath_mc > 0) else None
                if ath_x is None or ath_x <= 0:
                    continue

                call = None
                fc_raw = None
                if snap:
                    try:
                        fc_raw = kolfi._first_call_with_mc(snap)  # type: ignore[attr-defined]
                    except Exception:
                        fc_raw = None
                call = _persist_first_call_for_mint(mint, fc_raw) or fc_raw or _get_persisted_first_call(mint)
                caller = ""
                caller_x = ""
                if isinstance(call, dict):
                    try:
                        caller = kolfi._call_label(call)  # type: ignore[attr-defined]
                    except Exception:
                        caller = str(call.get("who") or call.get("kolUsername") or "caller")
                    caller_x = str(call.get("kolXId") or call.get("kol_x_id") or "").strip()

                chart = f"https://gmgn.ai/sol/token/{mint}" if mint else ""
                dex = str((snap or {}).get("dexscreener_url") or (snap or {}).get("dexUrl") or "")
                thumb = await _thumb_url_cached(session, snap or {}, mint) if mint else ""

                first_ts = str(ent.get("first_alert_ts") or "").strip()
                last_ts = str(ent.get("last_alert_ts") or "").strip()
                alert_count = int(ent.get("alert_count") or 0)

                rows.append(
                    {
                        "ticker": tick,
                        "mint": mint,
                        "caller": caller,
                        "caller_x": caller_x,
                        "call_mc": alert_mc,
                        "cur_mc": cur_mc,
                        "ath_mc": ath_mc,
                        "since_x": since,
                        "ath_x": ath_x,
                        "call_ts": first_ts,
                        "first_alert_ts": first_ts,
                        "last_alert_ts": last_ts,
                        "alert_count": alert_count,
                        "chart_url": chart,
                        "dex_url": dex,
                        "thumb_url": thumb or "",
                    }
                )

            # Primary: ATHx from OUR first alert MC. Tie-breaker: latest first alert.
            rows.sort(
                key=lambda r: (
                    float(r.get("ath_x") or 0),
                    str(r.get("first_alert_ts") or ""),
                ),
                reverse=True,
            )
            return {"days": days, "all_time": days <= 0, "items": rows[:top]}

    return await _cache_get_or_set(cache_key, 60.0, _build)


@app.get("/api/kol/alerts/history")
async def api_kol_alerts_history(request: Request, days: int = 30, limit: int = 100):
    """
    Date-ordered list of our alerted tokens with performance from first alert MC:
    includes first/last alert timestamps, alert count, since_x, and ath_x.
    """
    if not _DEV_PREVIEW and not _has_access(request):
        raise HTTPException(401, "Unauthorized")
    api_key = str(getattr(config, "KOLFI_API_KEY", "") or "").strip()
    if not api_key:
        raise HTTPException(400, "Missing KOLFI_API_KEY")
    days = max(0, min(3650, int(days)))
    limit = max(1, min(500, int(limit or 100)))

    rollup = _token_alert_rollup_from_db(days=days, limit_events=15000)
    if not rollup:
        return {"days": days, "all_time": days <= 0, "items": []}
    entries = list(rollup.values())

    cache_key = f"kol_alerts_history:{days}:{limit}"

    async def _build():
        async with aiohttp.ClientSession() as session:
            items, err = await kolfi.fetch_tokens_overview(
                session,
                api_key,
                limit=100,
                include_calls=20,
                max_pages=8,
            )
            if err and not items:
                raise HTTPException(502, err)
            by_snap: dict[str, dict] = {}
            for it in items or []:
                if not isinstance(it, dict):
                    continue
                m = kolfi._item_mint(it)  # type: ignore[attr-defined]
                if m:
                    by_snap[m] = it

            out: list[dict] = []
            for ent in entries:
                mint = str(ent.get("mint") or "").strip()
                if not mint:
                    continue
                first_alert_mc = kolfi._safe_float(ent.get("first_alert_mc"))  # type: ignore[attr-defined]
                if first_alert_mc is None or first_alert_mc <= 0:
                    continue
                snap = by_snap.get(mint) or {}
                cur_mc = kolfi._safe_float((snap or {}).get("last_market_cap"))  # type: ignore[attr-defined]
                ath_mc = kolfi._safe_float((snap or {}).get("ath_market_cap"))  # type: ignore[attr-defined]
                since_x = (cur_mc / first_alert_mc) if (cur_mc and cur_mc > 0) else None
                ath_x = (ath_mc / first_alert_mc) if (ath_mc and ath_mc > 0) else None
                out.append(
                    {
                        "mint": mint,
                        "ticker": str(ent.get("ticker") or "—"),
                        "first_alert_ts": str(ent.get("first_alert_ts") or ""),
                        "last_alert_ts": str(ent.get("last_alert_ts") or ""),
                        "alert_count": int(ent.get("alert_count") or 0),
                        "call_mc": first_alert_mc,
                        "cur_mc": cur_mc,
                        "ath_mc": ath_mc,
                        "since_x": since_x,
                        "ath_x": ath_x,
                    }
                )
            out.sort(key=lambda r: str(r.get("first_alert_ts") or ""), reverse=True)
            return {"days": days, "all_time": days <= 0, "items": out[:limit]}

    return await _cache_get_or_set(cache_key, 30.0, _build)


@app.get("/api/feed/event/{event_id}")
async def api_feed_event(request: Request, event_id: int):
    """Single event detail (requires access cookie)."""
    if not _DEV_PREVIEW and not _has_access(request):
        raise HTTPException(401, "Unauthorized")
    ev = feed_events.get_event(event_id)
    if not ev:
        raise HTTPException(404, "Not found")
    return ev


@app.get("/alert/{event_id}", response_class=HTMLResponse)
async def alert_page(request: Request, event_id: int):
    if not _DEV_PREVIEW and not _has_access(request):
        if EARLY_ACCESS_HTML.exists():
            return HTMLResponse(content=EARLY_ACCESS_HTML.read_text(encoding="utf-8"))
        return HTMLResponse("<h1>Early Access</h1>", status_code=503)
    html_file = WEBSITE_DIR / "alert.html"
    if html_file.exists():
        return HTMLResponse(content=html_file.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>Missing website/alert.html</h1>", status_code=500)


class ClaimRequest(BaseModel):
    discord_user_id: str
    tier: str          # "monthly" | "lifetime"
    chain: str         # "eth_mainnet" | "eth_base" | "solana"
    tx_hash: str


@app.post("/api/claim")
async def api_claim(req: ClaimRequest):
    tier = req.tier.strip().lower()
    chain = req.chain.strip().lower()
    tx_raw = req.tx_hash.strip()

    if tier not in ("monthly", "lifetime"):
        raise HTTPException(400, "tier must be 'monthly' or 'lifetime'")
    if chain not in ("eth_mainnet", "eth_base", "solana"):
        raise HTTPException(400, "chain must be 'eth_mainnet', 'eth_base', or 'solana'")

    # Parse Discord user ID
    try:
        discord_user_id = int(req.discord_user_id.strip())
    except ValueError:
        raise HTTPException(400, "Invalid Discord user ID (must be a numeric snowflake).")

    treasury = _treasury(chain)
    if not treasury:
        raise HTTPException(400, "Payment wallet not configured for this network.")

    min_amount = _min_amount(tier, chain)
    if min_amount <= 0:
        raise HTTPException(400, "This tier/chain combination is not enabled.")

    # Normalise tx hash
    if chain in ("eth_mainnet", "eth_base"):
        tx_store = payment_verify.normalize_evm_tx_hash(tx_raw)
        if not tx_store:
            raise HTTPException(400, "Invalid EVM transaction hash (expected 0x + 64 hex chars).")
    else:
        tx_store = payment_verify.normalize_sol_signature(tx_raw)
        if not tx_store:
            raise HTTPException(400, "Invalid Solana transaction signature (base58).")

    # Duplicate check
    payment_database.init_db()
    if payment_database.claim_exists(tx_store, chain):
        raise HTTPException(409, "This transaction has already been used for a claim.")

    if payment_database.claims_today_utc(discord_user_id) >= config.PAYMENT_MAX_CLAIMS_PER_DAY:
        raise HTTPException(429, f"Daily claim limit ({config.PAYMENT_MAX_CLAIMS_PER_DAY}) reached.")

    # Verify on-chain
    async with aiohttp.ClientSession() as session:
        if chain == "eth_mainnet":
            min_wei = payment_verify.eth_to_wei(min_amount)
            ok, msg, raw_amount = await payment_verify.verify_evm_native_payment(
                session, 1, tx_store, treasury, min_wei, config.PAYMENT_MIN_CONFIRMATIONS_ETH
            )
        elif chain == "eth_base":
            min_wei = payment_verify.eth_to_wei(min_amount)
            ok, msg, raw_amount = await payment_verify.verify_evm_native_payment(
                session, 8453, tx_store, treasury, min_wei, config.PAYMENT_MIN_CONFIRMATIONS_BASE
            )
        else:
            min_lam = payment_verify.sol_to_lamports(min_amount)
            ok, msg, raw_amount = await payment_verify.verify_solana_native_payment(
                session, tx_store, treasury, min_lam
            )

        if not ok:
            raise HTTPException(402, f"Payment not verified: {msg}")

        # Save claim
        import sqlite3
        try:
            payment_database.insert_claim(tx_store, chain, discord_user_id, GUILD_ID, tier, str(raw_amount))
        except sqlite3.IntegrityError:
            raise HTTPException(409, "Transaction already claimed (race condition).")

        # Assign role
        role_id = LIFETIME_ROLE_ID if tier == "lifetime" else MONTHLY_ROLE_ID
        granted, role_msg = await _discord_grant_role(session, discord_user_id, role_id)

        if tier == "monthly":
            try:
                payment_database.upsert_monthly_subscription(
                    discord_user_id, GUILD_ID, tx_store, chain, config.PREMIUM_MONTHLY_DAYS
                )
            except Exception:
                pass

    # Issue early-access code after verified claim (idempotent)
    try:
        access_code = payment_database.issue_access_code_for_claim(tx_hash=tx_store, chain=chain, user_id=discord_user_id)
    except Exception:
        access_code = None

    return JSONResponse({
        "success": True,
        "role_assigned": granted,
        "access_code": access_code,
        "message": (
            f"Payment verified! {'Role granted — welcome to Velcor3!' if granted else role_msg}"
        ),
    })


class RedeemRequest(BaseModel):
    code: str


@app.post("/api/access/redeem")
async def api_access_redeem(request: Request, req: RedeemRequest):
    ip = ""
    try:
        ip = request.client.host if request.client else ""
    except Exception:
        ip = ""

    ok, msg, user_id = payment_database.redeem_access_code(req.code, ip=ip)
    if not ok or not user_id:
        raise HTTPException(400, msg)

    tok = _make_access_token(user_id=user_id)
    res = JSONResponse({"success": True, "message": msg})
    # httpOnly so scripts can't steal; SameSite=Lax for simple redirects
    res.set_cookie(
        key=ACCESS_COOKIE,
        value=tok,
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=int(ACCESS_TOKEN_TTL_SECONDS),
        path="/",
    )
    return res


class TweetGateRequest(BaseModel):
    tweet_url: str
    reply_url: str


_TWEET_GATE_DB = str(DATA_DIR / "tweet_gate.db")


def _tweet_gate_init() -> None:
    conn = sqlite3.connect(_TWEET_GATE_DB)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS tweet_gate (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            tweet_url TEXT,
            reply_url TEXT,
            ip TEXT,
            ua TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def _looks_like_x_status(url: str) -> bool:
    s = (url or "").strip()
    if not s:
        return False
    return bool(re.search(r"https?://(www\.)?(x\.com|twitter\.com)/[^/]+/status/\d+", s))


@app.post("/api/access/tweet")
async def api_access_tweet(request: Request, body: TweetGateRequest):
    """
    Lightweight access gate: like/RT/reply to a tweet, then submit the reply URL.
    Note: format validation only (no X API verification).
    """
    tweet_url = (body.tweet_url or "").strip()
    reply_url = (body.reply_url or "").strip()
    if not _looks_like_x_status(tweet_url):
        raise HTTPException(400, "Invalid tweet_url (expected x.com/.../status/ID)")
    if not _looks_like_x_status(reply_url):
        raise HTTPException(400, "Invalid reply_url (expected x.com/.../status/ID)")

    try:
        _tweet_gate_init()
        conn = sqlite3.connect(_TWEET_GATE_DB)
        c = conn.cursor()
        c.execute(
            "INSERT INTO tweet_gate (ts, tweet_url, reply_url, ip, ua) VALUES (datetime('now'), ?, ?, ?, ?)",
            (
                tweet_url[:500],
                reply_url[:500],
                (request.client.host if request.client else "")[:80],
                (request.headers.get("user-agent", "") or "")[:240],
            ),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass

    uid = int.from_bytes(hashlib.sha256(reply_url.encode("utf-8")).digest()[:4], "big", signed=False)
    tok = _make_access_token(user_id=uid)
    res = JSONResponse({"success": True})
    res.set_cookie(
        key=ACCESS_COOKIE,
        value=tok,
        httponly=True,
        secure=_is_https(request),
        samesite="lax",
        max_age=int(ACCESS_TOKEN_TTL_SECONDS),
        path="/",
    )
    return res


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    # Render provides PORT; default to 8000 locally.
    try:
        _default_port = int(os.getenv("PORT", "8000") or 8000)
    except Exception:
        _default_port = 8000
    parser.add_argument("--port", type=int, default=_default_port)
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args()
    print(f"[Velcor3] Website server starting on http://{args.host}:{args.port}")
    uvicorn.run("website_server:app", host=args.host, port=args.port, reload=args.reload)
