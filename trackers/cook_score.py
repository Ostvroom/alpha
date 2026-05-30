"""
Community "cook score" — 🔥 reactions on live mint messages; boosts to hot channel when threshold hit.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import discord

import config
from app_paths import DATA_DIR, ensure_dirs

logger = logging.getLogger(__name__)

ensure_dirs()
DB_PATH = DATA_DIR / "cook_score.db"


def _conn():
    return sqlite3.connect(str(DB_PATH))


def init_db() -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS live_alert_messages (
            message_id INTEGER PRIMARY KEY,
            channel_id INTEGER NOT NULL,
            guild_id INTEGER NOT NULL,
            contract TEXT NOT NULL,
            fire_count INTEGER DEFAULT 0,
            boosted INTEGER DEFAULT 0,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_cook_contract ON live_alert_messages(contract)"
    )
    conn.commit()
    conn.close()


def register_live_message(
    message_id: int,
    channel_id: int,
    guild_id: int,
    contract: str,
) -> None:
    if not getattr(config, "ENABLE_COOK_SCORE", True):
        return
    contract = (contract or "").lower()
    if not contract or not message_id:
        return
    init_db()
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT fire_count, boosted FROM live_alert_messages WHERE message_id = ?",
        (int(message_id),),
    )
    existing = cur.fetchone()
    fc = int(existing[0] or 0) if existing else 0
    boosted = int(existing[1] or 0) if existing else 0
    cur.execute(
        """
        INSERT OR REPLACE INTO live_alert_messages
        (message_id, channel_id, guild_id, contract, fire_count, boosted, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(message_id),
            int(channel_id),
            int(guild_id),
            contract,
            fc,
            boosted,
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    conn.commit()
    conn.close()


def get_message_row(message_id: int) -> Optional[Dict[str, Any]]:
    init_db()
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT message_id, channel_id, guild_id, contract, fire_count, boosted
        FROM live_alert_messages WHERE message_id = ?
        """,
        (int(message_id),),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "message_id": row[0],
        "channel_id": row[1],
        "guild_id": row[2],
        "contract": row[3],
        "fire_count": int(row[4] or 0),
        "boosted": bool(row[5]),
    }


def increment_fire(message_id: int) -> Optional[Dict[str, Any]]:
    init_db()
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE live_alert_messages SET fire_count = fire_count + 1 WHERE message_id = ?",
        (int(message_id),),
    )
    if cur.rowcount == 0:
        conn.close()
        return None
    conn.commit()
    conn.close()
    return get_message_row(message_id)


def mark_boosted(message_id: int) -> None:
    init_db()
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE live_alert_messages SET boosted = 1 WHERE message_id = ?",
        (int(message_id),),
    )
    conn.commit()
    conn.close()


def get_contract_fire_total(contract: str) -> int:
    init_db()
    c = (contract or "").lower()
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT COALESCE(SUM(fire_count), 0) FROM live_alert_messages WHERE contract = ?",
        (c,),
    )
    n = int(cur.fetchone()[0] or 0)
    conn.close()
    return n


def cook_fire_emoji() -> str:
    return (getattr(config, "COOK_SCORE_FIRE_EMOJI", "🔥") or "🔥").strip()


def reaction_is_fire(payload: discord.RawReactionActionEvent) -> bool:
    emoji = payload.emoji
    if not emoji:
        return False
    target = cook_fire_emoji()
    if emoji.is_unicode_emoji():
        return str(emoji) == target
    return str(emoji.name) == target.lstrip(":").rstrip(":")


async def add_fire_reactions(message: discord.Message) -> None:
    if not getattr(config, "ENABLE_COOK_SCORE", True):
        return
    try:
        await message.add_reaction(cook_fire_emoji())
    except Exception as e:
        logger.debug("cook score add reaction: %s", e)


async def try_community_hot_boost(bot: discord.Client, row: Dict[str, Any]) -> None:
    """Post a community-pick hot alert when fire threshold is reached."""
    if row.get("boosted"):
        return
    threshold = int(getattr(config, "COOK_SCORE_HOT_THRESHOLD", 5) or 5)
    if int(row.get("fire_count") or 0) < threshold:
        return

    try:
        from trackers.nftscan_live_feed import get_nftscan_live_feed

        feed = get_nftscan_live_feed()
        if feed:
            await feed.post_community_hot_pick(
                row["contract"],
                int(row["fire_count"]),
            )
            mark_boosted(int(row["message_id"]))
            return
    except Exception as e:
        logger.warning("community hot boost failed: %s", e)
