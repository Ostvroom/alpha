"""
Per-guild license keys for multi-tenant Discord alert delivery.
Owner issues a key bound to a guild_id; server admins activate, then run setup to create channels.
"""
from __future__ import annotations

import hashlib
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator, List, Optional, Tuple

import config

from app_paths import DATA_DIR, ensure_dirs

ensure_dirs()
DB_FILE = str(DATA_DIR / "guild_licenses.db")


def _conn():
    return sqlite3.connect(DB_FILE)


def _pepper() -> str:
    p = (getattr(config, "LICENSE_KEY_PEPPER", None) or "").strip()
    if not p:
        # Weak default — set LICENSE_KEY_PEPPER in .env for production
        p = (config.DISCORD_TOKEN or "velcor3")[:32]
    return p


def _hash_key(plain: str) -> str:
    return hashlib.sha256(f"{_pepper()}:{plain.strip()}".encode()).hexdigest()


def init_db() -> None:
    conn = _conn()
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS guild_subscriptions (
            guild_id INTEGER PRIMARY KEY,
            license_key_hash TEXT NOT NULL,
            issued_at TEXT NOT NULL,
            activated_at TEXT,
            channel_new_id INTEGER DEFAULT 0,
            channel_established_id INTEGER DEFAULT 0,
            channel_escalation_id INTEGER DEFAULT 0,
            channel_trending_id INTEGER DEFAULT 0
        )
        """
    )
    # Lightweight migrations (sqlite): add new columns when upgrading in-place.
    try:
        c.execute("PRAGMA table_info(guild_subscriptions)")
        cols = {str(r[1]) for r in c.fetchall()}
        if "channel_wallet_nft_id" not in cols:
            c.execute(
                "ALTER TABLE guild_subscriptions ADD COLUMN channel_wallet_nft_id INTEGER DEFAULT 0"
            )
        if "channel_daily_finds_id" not in cols:
            c.execute(
                "ALTER TABLE guild_subscriptions ADD COLUMN channel_daily_finds_id INTEGER DEFAULT 0"
            )
    except Exception:
        pass
    conn.commit()
    conn.close()


def issue_license(guild_id: int) -> Tuple[Optional[str], str]:
    """
    Generate a new key for this guild. Fails if an active subscription already exists.
    Returns (plaintext_key_or_None, message).
    """
    guild_id = int(guild_id)
    init_db()
    conn = _conn()
    c = conn.cursor()
    c.execute(
        "SELECT activated_at FROM guild_subscriptions WHERE guild_id = ?",
        (guild_id,),
    )
    row = c.fetchone()
    if row and row[0]:
        conn.close()
        return None, "This guild already has an activated license. Revoke it before issuing a new key."

    plain = "V3-" + secrets.token_urlsafe(24).replace("-", "")[:36]
    h = _hash_key(plain)
    now = datetime.now(timezone.utc).isoformat()
    c.execute(
        """
        INSERT INTO guild_subscriptions (guild_id, license_key_hash, issued_at, activated_at)
        VALUES (?, ?, ?, NULL)
        ON CONFLICT(guild_id) DO UPDATE SET
            license_key_hash = excluded.license_key_hash,
            issued_at = excluded.issued_at,
            activated_at = NULL,
            channel_new_id = 0,
            channel_established_id = 0,
            channel_escalation_id = 0,
            channel_trending_id = 0
        """,
        (guild_id, h, now),
    )
    conn.commit()
    conn.close()
    return plain, "License key issued. Give this key to the server admin — it works only for this guild."


def activate_license(guild_id: int, plain_key: str) -> Tuple[bool, str]:
    """Verify key matches this guild and mark activated."""
    guild_id = int(guild_id)
    init_db()
    h = _hash_key(plain_key.strip())
    conn = _conn()
    c = conn.cursor()
    c.execute(
        "SELECT license_key_hash, activated_at FROM guild_subscriptions WHERE guild_id = ?",
        (guild_id,),
    )
    row = c.fetchone()
    if not row:
        conn.close()
        return False, "No license issued for this server. Ask the bot owner for a key tied to this guild ID."
    if row[0] != h:
        conn.close()
        return False, "Invalid license key."
    if row[1]:
        conn.close()
        return True, "License was already activated. Run `/alerts setup` if you still need channels."
    now = datetime.now(timezone.utc).isoformat()
    c.execute(
        "UPDATE guild_subscriptions SET activated_at = ? WHERE guild_id = ?",
        (now, guild_id),
    )
    conn.commit()
    conn.close()
    return True, "License activated. Use `/alerts setup` to create channels and start receiving alerts."


def set_install_channels(
    guild_id: int,
    new_id: int,
    established_id: int,
    escalation_id: int,
    trending_id: int,
) -> None:
    guild_id = int(guild_id)
    init_db()
    conn = _conn()
    c = conn.cursor()
    c.execute(
        """
        UPDATE guild_subscriptions SET
            channel_new_id = ?,
            channel_established_id = ?,
            channel_escalation_id = ?,
            channel_trending_id = ?
        WHERE guild_id = ?
        """,
        (int(new_id), int(established_id), int(escalation_id), int(trending_id), guild_id),
    )
    conn.commit()
    conn.close()


def set_wallet_nft_channel(guild_id: int, channel_id: int) -> None:
    """Persist this guild's wallet-tracker (ETH NFTs) destination channel."""
    guild_id = int(guild_id)
    init_db()
    conn = _conn()
    c = conn.cursor()
    c.execute(
        "UPDATE guild_subscriptions SET channel_wallet_nft_id = ? WHERE guild_id = ?",
        (int(channel_id), guild_id),
    )
    conn.commit()
    conn.close()


def set_daily_finds_channel(guild_id: int, channel_id: int) -> None:
    """Persist this guild's daily-finds digest destination channel."""
    guild_id = int(guild_id)
    init_db()
    conn = _conn()
    c = conn.cursor()
    c.execute(
        "UPDATE guild_subscriptions SET channel_daily_finds_id = ? WHERE guild_id = ?",
        (int(channel_id), guild_id),
    )
    conn.commit()
    conn.close()


@dataclass
class GuildSubRow:
    guild_id: int
    channel_new_id: int
    channel_established_id: int
    channel_escalation_id: int
    channel_trending_id: int
    channel_wallet_nft_id: int
    channel_daily_finds_id: int
    activated_at: Optional[str]


def iter_active_subscriptions() -> Iterator[GuildSubRow]:
    """Subscriptions that are activated and have at least one channel configured."""
    init_db()
    conn = _conn()
    c = conn.cursor()
    c.execute(
        """
        SELECT guild_id, channel_new_id, channel_established_id, channel_escalation_id,
               channel_trending_id, channel_wallet_nft_id, channel_daily_finds_id, activated_at
        FROM guild_subscriptions
        WHERE activated_at IS NOT NULL
          AND (
            channel_new_id > 0 OR channel_established_id > 0 OR channel_escalation_id > 0 OR channel_trending_id > 0 OR channel_wallet_nft_id > 0 OR channel_daily_finds_id > 0
          )
        """
    )
    for row in c.fetchall():
        yield GuildSubRow(
            guild_id=int(row[0]),
            channel_new_id=int(row[1] or 0),
            channel_established_id=int(row[2] or 0),
            channel_escalation_id=int(row[3] or 0),
            channel_trending_id=int(row[4] or 0),
            channel_wallet_nft_id=int(row[5] or 0),
            channel_daily_finds_id=int(row[6] or 0),
            activated_at=row[7],
        )
    conn.close()


def get_subscription(guild_id: int) -> Optional[Tuple[Optional[str], Optional[str], int, int, int, int]]:
    """Returns (activated_at_iso or None, issued_at, ch_new, ch_est, ch_esc, ch_trend, ch_wallet_nft, ch_daily_finds) or None."""
    init_db()
    conn = _conn()
    c = conn.cursor()
    c.execute(
        """
        SELECT activated_at, issued_at, channel_new_id, channel_established_id,
               channel_escalation_id, channel_trending_id, channel_wallet_nft_id, channel_daily_finds_id
        FROM guild_subscriptions WHERE guild_id = ?
        """,
        (int(guild_id),),
    )
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    return (
        row[0],
        row[1],
        int(row[2] or 0),
        int(row[3] or 0),
        int(row[4] or 0),
        int(row[5] or 0),
        int(row[6] or 0),
        int(row[7] or 0),
    )


def all_wallet_nft_channel_ids() -> List[int]:
    """Extra wallet-tracker destinations from licensed guilds."""
    ids: List[int] = []
    for row in iter_active_subscriptions():
        if getattr(row, "channel_wallet_nft_id", 0):
            ids.append(int(row.channel_wallet_nft_id))
    return ids


def all_daily_finds_channel_ids() -> List[int]:
    """Extra daily-finds digest destinations from licensed guilds."""
    ids: List[int] = []
    for row in iter_active_subscriptions():
        if getattr(row, "channel_daily_finds_id", 0):
            ids.append(int(row.channel_daily_finds_id))
    return ids


def list_all_rows() -> List[Tuple[int, str, Optional[str], int, int, int, int]]:
    """Owner listing: guild_id, issued_at, activated_at, 4 channel ids."""
    init_db()
    conn = _conn()
    c = conn.cursor()
    c.execute(
        """
        SELECT guild_id, issued_at, activated_at,
               channel_new_id, channel_established_id, channel_escalation_id, channel_trending_id
        FROM guild_subscriptions ORDER BY guild_id
        """
    )
    rows = [tuple(r) for r in c.fetchall()]
    conn.close()
    return rows


def revoke_license(guild_id: int) -> bool:
    init_db()
    conn = _conn()
    c = conn.cursor()
    c.execute("DELETE FROM guild_subscriptions WHERE guild_id = ?", (int(guild_id),))
    n = c.rowcount
    conn.commit()
    conn.close()
    return n > 0


def all_trending_channel_ids() -> List[int]:
    """Extra trending report destinations from licensed guilds."""
    ids: List[int] = []
    for row in iter_active_subscriptions():
        if row.channel_trending_id:
            ids.append(row.channel_trending_id)
    return ids
