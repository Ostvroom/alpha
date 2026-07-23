"""Discord engagement points — ledger, caps, and award paths.

Design notes
------------
This module owns the *engagement points* economy (kept separate from $V3 and
from the Alpha/Meme caller score in `alpha_ping.py`).

Two things are deliberate:

1. **Append-only ledger first.** Every award writes a row to `engagement_events`
   BEFORE the running total moves, and each row carries a deterministic
   `event_id`. That makes awards idempotent (a retry re-derives the same id and
   is rejected by the PK), auditable, and replayable to the website. The audit
   of the existing points code found totals with no event history, which made
   awards impossible to reconcile — this avoids repeating that.

2. **Totals live where the website already reads them.** Points are applied via
   `website_server._acct_add_points()`, the single existing chokepoint for
   `users.points`, so the site's Discord Points section picks them up with no
   schema change. The ledger is additive, not a replacement.

Only the X-engage claim path is wired up so far; `award()` is generic so the
remaining earning paths (messages, alpha calls, votes) drop in without rework.
"""
from __future__ import annotations

import logging
import os
import sqlite3
import uuid
from contextlib import closing
from datetime import datetime, timezone
from typing import Optional, Tuple

from app_paths import DATA_DIR, ensure_dirs

logger = logging.getLogger(__name__)

ensure_dirs()
DB_PATH = str(DATA_DIR / "engagement.db")

# Stable namespace for deterministic event ids. Must never change: it is what
# makes a retried award resolve to the same id instead of double-paying.
_EVENT_NS = uuid.UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff")

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except ValueError:
        return default


# --- Earning rules ---------------------------------------------------------
# Honour-system X engagement: the member confirms they engaged. Kept low-value
# and hard-capped precisely because it is self-reported.
X_ENGAGE_POINTS = _env_int("ENGAGE_X_POINTS", 10)
X_ENGAGE_DAILY_CAP = _env_int("ENGAGE_X_DAILY_CAP", 3)

# Chat messages. Raw message count is the easiest signal to farm, so points are
# gated on length, a per-message cooldown, a repeat-content check and a cap.
MSG_POINTS = _env_int("ENGAGE_MSG_POINTS", 2)
MSG_REPLY_POINTS = _env_int("ENGAGE_MSG_REPLY_POINTS", 3)
MSG_DAILY_CAP = _env_int("ENGAGE_MSG_DAILY_CAP", 40)
MSG_COOLDOWN_SEC = _env_int("ENGAGE_MSG_COOLDOWN_SEC", 60)
MSG_MIN_CHARS = _env_int("ENGAGE_MSG_MIN_CHARS", 15)
ALPHA_CHANNEL_MULTIPLIER = _env_float("ENGAGE_ALPHA_CHANNEL_MULTIPLIER", 1.5)

# Alpha / meme calls and community curation.
CALL_POINTS = _env_int("ENGAGE_CALL_POINTS", 25)
CALL_DAILY_CAP = _env_int("ENGAGE_CALL_DAILY_CAP", 75)
COOK_RECEIVED_POINTS = _env_int("ENGAGE_COOK_RECEIVED_POINTS", 5)
SKIP_RECEIVED_POINTS = _env_int("ENGAGE_SKIP_RECEIVED_POINTS", -3)
COOK_RECEIVED_DAILY_CAP = _env_int("ENGAGE_COOK_RECEIVED_DAILY_CAP", 50)
VOTE_CAST_POINTS = _env_int("ENGAGE_VOTE_POINTS", 3)
VOTE_CAST_DAILY_CAP = _env_int("ENGAGE_VOTE_DAILY_CAP", 30)

# Anti-abuse: brand-new accounts / members earn nothing.
MIN_ACCOUNT_AGE_DAYS = _env_int("ENGAGE_MIN_ACCOUNT_AGE_DAYS", 30)
MIN_MEMBER_AGE_HOURS = _env_int("ENGAGE_MIN_MEMBER_AGE_HOURS", 24)

# Verified holders earn a bonus on every path — ties Discord activity to the
# staking product.
HOLDER_MULTIPLIER = _env_float("ENGAGE_HOLDER_MULTIPLIER", 1.25)

# Absolute ceiling across every earning path, so no future path can be farmed
# into the ground before its own cap is tuned.
GLOBAL_DAILY_CAP = _env_int("ENGAGE_GLOBAL_DAILY_CAP", 250)


def _points_channel_ids() -> set:
    """Channels where chat earns points. Opt-in allowlist by design: an
    opt-out list would silently pay out in every new/bot-output channel."""
    raw = (os.getenv("ENGAGE_POINTS_CHANNEL_IDS", "") or "").strip()
    out = set()
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if part.isdigit():
            out.add(int(part))
    return out


def _alpha_channel_ids() -> set:
    """Channels that earn the alpha-channel multiplier."""
    raw = (os.getenv("ENGAGE_ALPHA_CHANNEL_IDS", "") or "").strip()
    out = set()
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if part.isdigit():
            out.add(int(part))
    return out


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    # WAL + immediate transactions: the bot writes from async handlers while the
    # in-process FastAPI thread may read concurrently.
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except Exception:
        pass
    return conn


def init_db() -> None:
    with closing(_conn()) as conn, conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS engagement_events (
                event_id TEXT PRIMARY KEY,
                discord_user_id INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                points_delta INTEGER NOT NULL DEFAULT 0,
                description TEXT,
                occurred_at TEXT NOT NULL,
                synced_at TEXT
            )
            """
        )
        # Cap lookups are per-user-per-day-per-type; this index keeps them O(log n).
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_engagement_user_day "
            "ON engagement_events (discord_user_id, occurred_at)"
        )
        # Unsynced-sweep index for the future website push.
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_engagement_unsynced "
            "ON engagement_events (synced_at) WHERE synced_at IS NULL"
        )
        # Maps a posted Discord message back to the tweet it advertises, so the
        # claim button can resolve its tweet from interaction.message.id. Mirrors
        # the proven get_post_by_message() pattern in alpha_ping.py, which keeps
        # the button's custom_id static and therefore restart-safe.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tweet_engage_posts (
                message_id INTEGER PRIMARY KEY,
                tweet_id TEXT NOT NULL,
                handle TEXT,
                posted_at TEXT NOT NULL
            )
            """
        )
        # The dedup guard: one claim per member per tweet, enforced by the PK.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tweet_engage_claims (
                discord_user_id INTEGER NOT NULL,
                tweet_id TEXT NOT NULL,
                claimed_at TEXT NOT NULL,
                PRIMARY KEY (discord_user_id, tweet_id)
            )
            """
        )
        # Chat anti-spam state: last paid message time (cooldown) and a short
        # hash trail of recent content (repeat/copy-paste detection).
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS message_state (
                discord_user_id INTEGER PRIMARY KEY,
                last_award_ts REAL NOT NULL DEFAULT 0,
                recent_hashes TEXT NOT NULL DEFAULT ''
            )
            """
        )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def make_event_id(*parts: object) -> str:
    """Deterministic id from a natural key, so retries can't double-award."""
    return str(uuid.uuid5(_EVENT_NS, ":".join(str(p) for p in parts)))


def points_today(discord_user_id: int, event_type: Optional[str] = None) -> int:
    """Points already earned today (UTC), optionally for one event type."""
    day = _utc_today()
    sql = (
        "SELECT COALESCE(SUM(points_delta), 0) AS total FROM engagement_events "
        "WHERE discord_user_id = ? AND substr(occurred_at, 1, 10) = ? AND points_delta > 0"
    )
    args: list = [int(discord_user_id), day]
    if event_type:
        sql += " AND event_type = ?"
        args.append(event_type)
    with closing(_conn()) as conn:
        row = conn.execute(sql, args).fetchone()
    return int(row["total"] or 0) if row else 0


def get_total_points(discord_user_id: int) -> int:
    """Lifetime engagement points from the ledger (source of truth for audit)."""
    with closing(_conn()) as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(points_delta), 0) AS total FROM engagement_events "
            "WHERE discord_user_id = ?",
            (int(discord_user_id),),
        ).fetchone()
    return int(row["total"] or 0) if row else 0


def _apply_points_to_account(discord_user_id: int, points: int) -> None:
    """Push the delta onto users.points via the website's existing chokepoint.

    Imported lazily: website_server is heavy (FastAPI app + route registration)
    and importing it at module scope would create an import cycle when the bot
    boots. Failure here is logged, not raised — the ledger row is already
    committed and remains the recoverable record of truth.
    """
    try:
        from website_server import _acct_add_points

        _acct_add_points(int(discord_user_id), int(points))
    except Exception as e:
        logger.warning(
            "[Engagement] Ledger written but account total not updated for %s: %s",
            discord_user_id,
            e,
        )


def award(
    discord_user_id: int,
    event_type: str,
    points: int,
    *,
    event_id: str,
    description: str = "",
    daily_cap: Optional[int] = None,
) -> Tuple[bool, str, int]:
    """Record an engagement award. Returns (ok, reason, points_awarded).

    Ledger row first, then the account total — so a crash between the two
    leaves a replayable record rather than silently vanished points.
    """
    uid = int(discord_user_id)
    pts = int(points)
    if uid <= 0 or pts == 0:
        return False, "invalid", 0

    # Per-type cap, then the global ceiling.
    if daily_cap is not None and pts > 0:
        if points_today(uid, event_type) + pts > int(daily_cap):
            return False, "daily_cap", 0
    if pts > 0 and points_today(uid) + pts > GLOBAL_DAILY_CAP:
        return False, "global_cap", 0

    try:
        with closing(_conn()) as conn, conn:
            conn.execute(
                "INSERT INTO engagement_events "
                "(event_id, discord_user_id, event_type, points_delta, description, occurred_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (event_id, uid, str(event_type), pts, str(description or "")[:300], _utc_now_iso()),
            )
    except sqlite3.IntegrityError:
        # Same event_id already recorded — a retry, not a new award.
        return False, "duplicate", 0

    _apply_points_to_account(uid, pts)
    return True, "ok", pts


# --- X engage claim path ---------------------------------------------------

def register_tweet_post(message_id: int, tweet_id: str, handle: str = "") -> None:
    """Remember which tweet a posted alert message advertises."""
    if not message_id or not tweet_id:
        return
    try:
        with closing(_conn()) as conn, conn:
            conn.execute(
                "INSERT OR REPLACE INTO tweet_engage_posts "
                "(message_id, tweet_id, handle, posted_at) VALUES (?, ?, ?, ?)",
                (int(message_id), str(tweet_id), str(handle or ""), _utc_now_iso()),
            )
    except Exception as e:
        logger.warning("[Engagement] register_tweet_post failed: %s", e)


def get_tweet_for_message(message_id: int) -> Optional[dict]:
    with closing(_conn()) as conn:
        row = conn.execute(
            "SELECT message_id, tweet_id, handle FROM tweet_engage_posts WHERE message_id = ?",
            (int(message_id),),
        ).fetchone()
    return dict(row) if row else None


def get_engage_claim_count(tweet_id: str) -> int:
    """How many members claimed this tweet (drives the button's counter)."""
    with closing(_conn()) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM tweet_engage_claims WHERE tweet_id = ?",
            (str(tweet_id),),
        ).fetchone()
    return int(row["n"] or 0) if row else 0


def claim_tweet_engagement(discord_user_id: int, tweet_id: str) -> Tuple[bool, str, int]:
    """Honour-system claim for engaging with a watched tweet.

    Returns (ok, reason, points). Reasons: ok | already_claimed | daily_cap |
    global_cap | duplicate | invalid | error.
    """
    uid = int(discord_user_id)
    tid = str(tweet_id or "").strip()
    if uid <= 0 or not tid:
        return False, "invalid", 0

    # Reserve the (user, tweet) pair first. The PK is the real dedup guard and
    # makes concurrent double-clicks safe: the second INSERT loses the race.
    try:
        with closing(_conn()) as conn, conn:
            conn.execute(
                "INSERT INTO tweet_engage_claims (discord_user_id, tweet_id, claimed_at) "
                "VALUES (?, ?, ?)",
                (uid, tid, _utc_now_iso()),
            )
    except sqlite3.IntegrityError:
        return False, "already_claimed", 0
    except Exception as e:
        logger.warning("[Engagement] claim insert failed: %s", e)
        return False, "error", 0

    ok, reason, pts = award(
        uid,
        "x_engage",
        X_ENGAGE_POINTS,
        event_id=make_event_id("x_engage", uid, tid),
        description=f"Confirmed engagement with tweet {tid}",
        daily_cap=X_ENGAGE_DAILY_CAP * X_ENGAGE_POINTS,
    )
    if not ok:
        # Cap hit / duplicate: release the reservation so the member can claim
        # this same tweet tomorrow instead of it being permanently consumed.
        try:
            with closing(_conn()) as conn, conn:
                conn.execute(
                    "DELETE FROM tweet_engage_claims WHERE discord_user_id = ? AND tweet_id = ?",
                    (uid, tid),
                )
        except Exception:
            pass
    return ok, reason, pts


# --- Eligibility & multipliers ---------------------------------------------

def member_is_eligible(member) -> bool:
    """Brand-new Discord accounts and brand-new members earn nothing.

    This is the cheapest defence against throwaway alts farming the economy.
    Fails OPEN on unexpected shapes so a metadata quirk can't silently stop
    paying the whole server.
    """
    try:
        now = datetime.now(timezone.utc)
        created = getattr(getattr(member, "_user", member), "created_at", None)
        if created is not None and MIN_ACCOUNT_AGE_DAYS > 0:
            if (now - created).total_seconds() < MIN_ACCOUNT_AGE_DAYS * 86400:
                return False
        joined = getattr(member, "joined_at", None)
        if joined is not None and MIN_MEMBER_AGE_HOURS > 0:
            if (now - joined).total_seconds() < MIN_MEMBER_AGE_HOURS * 3600:
                return False
    except Exception:
        return True
    return True


def holder_multiplier(member) -> float:
    """1.25x for verified holders, 1.0x otherwise."""
    try:
        import config

        role_id = int(getattr(config, "HOLDER_VERIFIED_ROLE_ID", 0) or 0)
        if role_id and any(int(r.id) == role_id for r in getattr(member, "roles", []) or []):
            return HOLDER_MULTIPLIER
    except Exception:
        pass
    return 1.0


# --- Chat messages ---------------------------------------------------------

def _message_state(uid: int) -> Tuple[float, list]:
    with closing(_conn()) as conn:
        row = conn.execute(
            "SELECT last_award_ts, recent_hashes FROM message_state WHERE discord_user_id = ?",
            (int(uid),),
        ).fetchone()
    if not row:
        return 0.0, []
    hashes = [h for h in str(row["recent_hashes"] or "").split(",") if h]
    return float(row["last_award_ts"] or 0.0), hashes


def _save_message_state(uid: int, ts: float, hashes: list) -> None:
    with closing(_conn()) as conn, conn:
        conn.execute(
            "INSERT INTO message_state (discord_user_id, last_award_ts, recent_hashes) "
            "VALUES (?, ?, ?) ON CONFLICT(discord_user_id) DO UPDATE SET "
            "last_award_ts = excluded.last_award_ts, recent_hashes = excluded.recent_hashes",
            (int(uid), float(ts), ",".join(hashes[-3:])),
        )


def _content_hash(text: str) -> str:
    import hashlib

    normalised = " ".join((text or "").lower().split())
    return hashlib.sha256(normalised.encode("utf-8")).hexdigest()[:12]


def _meaningful_length(text: str) -> int:
    """Length ignoring mentions, links and custom emoji, so a wall of pings or
    a bare link doesn't clear the minimum-effort bar."""
    import re

    s = str(text or "")
    s = re.sub(r"<@[!&]?\d+>|<#\d+>|<a?:\w+:\d+>", "", s)   # mentions + custom emoji
    s = re.sub(r"https?://\S+", "", s)                       # links
    return len(s.strip())


def award_message(message) -> Tuple[bool, str, int]:
    """Award points for a chat message. Returns (ok, reason, points).

    Called from on_message, so every rejection path must be cheap and must
    never raise into the event handler.
    """
    try:
        author = getattr(message, "author", None)
        if author is None or getattr(author, "bot", False):
            return False, "bot", 0
        uid = int(getattr(author, "id", 0) or 0)
        if uid <= 0:
            return False, "invalid", 0

        alpha_ids = _alpha_channel_ids()
        # Alpha channels are implicitly points-eligible: listing one only in
        # ENGAGE_ALPHA_CHANNEL_IDS would otherwise silently earn nothing.
        allow = _points_channel_ids() | alpha_ids
        ch_id = int(getattr(getattr(message, "channel", None), "id", 0) or 0)
        # No allowlist configured -> pay nowhere. Opt-in is the safe default.
        if not allow or ch_id not in allow:
            return False, "channel_not_eligible", 0

        if not member_is_eligible(author):
            return False, "account_too_new", 0

        content = getattr(message, "content", "") or ""
        if _meaningful_length(content) < MSG_MIN_CHARS:
            return False, "too_short", 0

        import time as _time

        now_ts = _time.time()
        last_ts, hashes = _message_state(uid)
        if now_ts - last_ts < MSG_COOLDOWN_SEC:
            return False, "cooldown", 0

        chash = _content_hash(content)
        if chash in hashes:
            return False, "repeat_content", 0

        base = MSG_REPLY_POINTS if getattr(message, "reference", None) else MSG_POINTS
        if ch_id in alpha_ids:
            base = int(round(base * ALPHA_CHANNEL_MULTIPLIER))
        pts = max(1, int(round(base * holder_multiplier(author))))

        ok, reason, awarded = award(
            uid,
            "message_activity",
            pts,
            event_id=make_event_id("message", uid, getattr(message, "id", 0)),
            description=f"Chat activity in #{getattr(getattr(message,'channel',None),'name','?')}",
            daily_cap=MSG_DAILY_CAP,
        )
        if ok:
            # Only advance cooldown//hash trail on a real award, so a capped or
            # rejected message doesn't start the next cooldown window.
            _save_message_state(uid, now_ts, hashes + [chash])
        return ok, reason, awarded
    except Exception as e:
        logger.warning("[Engagement] award_message failed: %s", e)
        return False, "error", 0


# --- Alpha / meme calls and curation ---------------------------------------

def award_call_posted(discord_user_id: int, kind: str, message_id: int,
                      member=None) -> Tuple[bool, str, int]:
    """Points for posting an !alpha / !meme call."""
    pts = CALL_POINTS
    if member is not None:
        pts = max(1, int(round(pts * holder_multiplier(member))))
    if member is not None and not member_is_eligible(member):
        return False, "account_too_new", 0
    return award(
        discord_user_id,
        "alpha_call",
        pts,
        event_id=make_event_id("call", discord_user_id, message_id),
        description=f"Posted {kind} call",
        daily_cap=CALL_DAILY_CAP,
    )


def award_vote_cast(voter_id: int, post_id: int, vote: str) -> Tuple[bool, str, int]:
    """Points for curating — voting on someone else's call."""
    return award(
        voter_id,
        "vote_cast",
        VOTE_CAST_POINTS,
        event_id=make_event_id("vote", voter_id, post_id),
        description=f"Voted {vote} on a call",
        daily_cap=VOTE_CAST_DAILY_CAP,
    )


def award_vote_received(poster_id: int, post_id: int, voter_id: int,
                        vote: str) -> Tuple[bool, str, int]:
    """Points to the CALLER when their call is voted on.

    Cook pays, Skip deducts — this is what makes low-effort calls net-negative
    and stops call-spam being a viable farm. The event id keys on the voter so
    each voter can move the caller's score exactly once per post; a vote change
    is intentionally not re-paid.
    """
    if vote in ("cook", "good"):
        pts, cap = COOK_RECEIVED_POINTS, COOK_RECEIVED_DAILY_CAP
    else:
        pts, cap = SKIP_RECEIVED_POINTS, None      # penalties are never capped
    if pts == 0:
        return False, "no_points", 0
    return award(
        poster_id,
        "call_vote_received",
        pts,
        event_id=make_event_id("vote_recv", poster_id, post_id, voter_id),
        description=f"Received {vote} vote on a call",
        daily_cap=cap,
    )
