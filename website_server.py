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

import aiohttp
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
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
    limit = max(1, min(20, int(limit or 5)))
    rows = database.get_trending_projects(hours=24, limit=limit)
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
        # database.get_trending_projects is defined twice in database.py; one version returns 6 cols,
        # another returns 10 cols. Normalize to the first 6:
        # (twitter_id, handle, name, desc, created_at, smarts)
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
    # If filtering removed many rows, try to top up by grabbing more from DB.
    if len(out) < limit and limit < 10:
        # This endpoint already caps limit<=10, so we can safely retry once with a larger pull.
        try:
            more_rows = database.get_trending_projects(hours=24, limit=10)
            for row in more_rows:
                if len(out) >= limit:
                    break
                if isinstance(row, (list, tuple)):
                    r = list(row) + [None] * 10
                    twitter_id, handle, name, desc, created_at, smarts = r[0], r[1], r[2], r[3], r[4], r[5]
                else:
                    continue
                hkey = str(handle or "").strip().lstrip("@").lower()
                if any((it.get("handle") or "").strip().lower() == str(handle or "").strip().lower() for it in out):
                    continue
                p = prof.get(hkey) or {}
                age_days = _age_days_from_created_at(created_at)
                if age_days is None:
                    try:
                        age_days = int(p.get("age_days")) if p.get("age_days") is not None else None
                    except Exception:
                        age_days = None
                if isinstance(age_days, int) and age_days > max_age_days:
                    continue
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
                        "age_days": age_days,
                        "hva_smarts": int(smarts or 0),
                        "url": f"https://x.com/{handle}" if handle else "",
                        "pfp_url": pfp_url,
                        "banner_url": p.get("banner_url") or "",
                    }
                )
        except Exception:
            pass
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
async def api_projects_finds(request: Request, limit: int = 50):
    """Main finds list for the Projects page (alerted only, last 24h)."""
    if not _DEV_PREVIEW and not _has_access(request):
        raise HTTPException(401, "Unauthorized")
    limit = max(10, min(200, int(limit or 50)))
    rows = database.get_projects_finds_24h(limit=limit)
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


@app.get("/api/kol/dashboard")
async def api_kol_dashboard(
    request: Request,
    hours: float = 24.0,
    top: int = 5,
    per_bucket: int = 5,
):
    """
    Website dashboard for KOL alerts:
    - top performing calls (ranked by ATH reached)
    - 3 columns by MC bucket at time of call (low/100k/1m) with performance since call
    """
    if not _DEV_PREVIEW and not _has_access(request):
        raise HTTPException(401, "Unauthorized")
    api_key = str(getattr(config, "KOLFI_API_KEY", "") or "").strip()
    if not api_key:
        raise HTTPException(400, "Missing KOLFI_API_KEY")
    hours = float(hours or 24.0)
    top = max(1, min(25, int(top or 5)))
    per_bucket = max(1, min(25, int(per_bucket or 5)))

    async with aiohttp.ClientSession() as session:
        items, err = await kolfi.fetch_tokens_overview(
            session,
            api_key,
            limit=100,
            include_calls=50,
            max_pages=15,
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

        # Top performers within window (ranked by ATH multiple from call -> ATH)
        scored = []
        for it in items or []:
            if not isinstance(it, dict):
                continue
            row = kolfi._entry_for_leaderboard(it, max_call_age_hours=hours)  # type: ignore[attr-defined]
            if row:
                scored.append(row)
        # Rank by ATHx (ath_usd / call_mc), not by absolute ATH dollars.
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

        top_rows = []
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
            thumb = await kolfi.resolve_token_thumbnail(session, it, mint) if mint else None
            top_rows.append(
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

        # Build 3 columns of recent alert events, enriched with current perf
        evs = feed_events.list_events(limit=350, kinds=["token_alert"])
        buckets = {"low": [], "100k": [], "1m": []}
        seen = set()
        for ev in evs:
            extra = (ev or {}).get("extra") or {}
            mint = str(extra.get("mint") or "").strip()
            if not mint or mint in seen:
                continue
            seen.add(mint)
            bucket = str(extra.get("bucket") or "").strip().lower()
            if bucket not in buckets:
                # fallback: compute from current MC if unknown
                try:
                    bucket = kolfi._mc_bucket(kolfi._safe_float((by_mint.get(mint) or {}).get("last_market_cap")))  # type: ignore[attr-defined]
                except Exception:
                    bucket = "low"

            it = by_mint.get(mint) or {}
            fc = kolfi._first_call_with_mc(it) if it else None  # type: ignore[attr-defined]
            call_mc = kolfi._safe_float((fc or {}).get("callMarketCap")) if isinstance(fc, dict) else None  # type: ignore[attr-defined]
            cur_mc = kolfi._safe_float(it.get("last_market_cap"))  # type: ignore[attr-defined]
            ath_mc = kolfi._safe_float(it.get("ath_market_cap"))  # type: ignore[attr-defined]
            since = (cur_mc / call_mc) if (call_mc and call_mc > 0 and cur_mc and cur_mc > 0) else None
            ath_x = (ath_mc / call_mc) if (call_mc and call_mc > 0 and ath_mc and ath_mc > 0) else None
            dex = str(it.get("dexscreener_url") or it.get("dexUrl") or ev.get("url") or "")
            chart = f"https://gmgn.ai/sol/token/{mint}" if mint else ""
            buckets[bucket].append(
                {
                    "event_id": ev.get("id"),
                    "ts": ev.get("ts"),
                    "title": ev.get("title"),
                    "mint": mint,
                    "ticker": str(extra.get("symbol") or extra.get("name") or "").strip() or kolfi._item_ticker(it),  # type: ignore[attr-defined]
                    "caller": str((fc or {}).get("kolXId") or "") if isinstance(fc, dict) else "",
                    "call_mc": call_mc,
                    "cur_mc": cur_mc,
                    "ath_mc": ath_mc,
                    "since_x": since,
                    "ath_x": ath_x,
                    "chart_url": chart,
                    "dex_url": dex,
                }
            )
            if len(buckets[bucket]) >= per_bucket:
                continue
            if all(len(v) >= per_bucket for v in buckets.values()):
                break

        return {
            "top": top_rows,
            "buckets": {
                "low": buckets["low"][:per_bucket],
                "100k": buckets["100k"][:per_bucket],
                "1m": buckets["1m"][:per_bucket],
            },
        }


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
        secure=False,
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
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args()
    print(f"[Velcor3] Website server starting on http://{args.host}:{args.port}")
    uvicorn.run("website_server:app", host=args.host, port=args.port, reload=args.reload)
