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

# --- Earning rules ---------------------------------------------------------
# Honour-system X engagement: the member confirms they engaged. Kept low-value
# and hard-capped precisely because it is self-reported.
X_ENGAGE_POINTS = int(os.getenv("ENGAGE_X_POINTS", "10") or 10)
X_ENGAGE_DAILY_CAP = int(os.getenv("ENGAGE_X_DAILY_CAP", "3") or 3)
# Absolute ceiling across every earning path, so no future path can be farmed
# into the ground before its own cap is tuned.
GLOBAL_DAILY_CAP = int(os.getenv("ENGAGE_GLOBAL_DAILY_CAP", "250") or 250)


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
