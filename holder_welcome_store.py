from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from app_paths import DATA_DIR, ensure_dirs


ensure_dirs()

DB_FILE = str(DATA_DIR / "holder_welcomes.db")
PG_DSN = (os.getenv("DATABASE_URL") or "").strip()
SQLITE_FALLBACK = (os.getenv("HOLDER_WELCOME_SQLITE_FALLBACK") or "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)


def is_configured() -> bool:
    return bool(PG_DSN or SQLITE_FALLBACK)


def _conn_pg():
    try:
        import psycopg
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "DATABASE_URL is set but psycopg is not installed. "
            "Install with: pip install psycopg[binary]"
        ) from e
    return psycopg.connect(PG_DSN)


def _conn_sqlite():
    return sqlite3.connect(DB_FILE)


def init_db() -> None:
    if not is_configured():
        return

    if PG_DSN:
        conn = _conn_pg()
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS holder_welcomes (
                guild_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                role_id BIGINT NOT NULL,
                first_welcomed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                last_seen_holder_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                welcome_message_id BIGINT,
                PRIMARY KEY (guild_id, user_id)
            )
            """
        )
        conn.commit()
        conn.close()
        return

    conn = _conn_sqlite()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS holder_welcomes (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role_id INTEGER NOT NULL,
            first_welcomed_at TEXT NOT NULL,
            last_seen_holder_at TEXT NOT NULL,
            welcome_message_id INTEGER,
            PRIMARY KEY (guild_id, user_id)
        )
        """
    )
    conn.commit()
    conn.close()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def has_welcomed_holder(guild_id: int, user_id: int) -> bool:
    init_db()
    if not is_configured():
        return False

    if PG_DSN:
        conn = _conn_pg()
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM holder_welcomes WHERE guild_id = %s AND user_id = %s",
            (int(guild_id), int(user_id)),
        )
        found = cur.fetchone() is not None
        conn.close()
        return found

    conn = _conn_sqlite()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM holder_welcomes WHERE guild_id = ? AND user_id = ?",
        (int(guild_id), int(user_id)),
    )
    found = cur.fetchone() is not None
    conn.close()
    return found


def mark_holder_seen(
    *,
    guild_id: int,
    user_id: int,
    role_id: int,
    welcome_message_id: Optional[int] = None,
) -> bool:
    init_db()
    if not is_configured():
        return False

    if PG_DSN:
        conn = _conn_pg()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO holder_welcomes (
                guild_id, user_id, role_id, welcome_message_id
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (guild_id, user_id)
            DO UPDATE SET
                role_id = EXCLUDED.role_id,
                last_seen_holder_at = now(),
                welcome_message_id = COALESCE(
                    EXCLUDED.welcome_message_id,
                    holder_welcomes.welcome_message_id
                )
            RETURNING (xmax = 0) AS inserted
            """,
            (
                int(guild_id),
                int(user_id),
                int(role_id),
                int(welcome_message_id) if welcome_message_id else None,
            ),
        )
        row = cur.fetchone()
        inserted = bool(row and row[0])
        conn.commit()
        conn.close()
        return inserted

    now = _now_iso()
    conn = _conn_sqlite()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM holder_welcomes WHERE guild_id = ? AND user_id = ?",
        (int(guild_id), int(user_id)),
    )
    existed = cur.fetchone() is not None
    cur.execute(
        """
        INSERT INTO holder_welcomes (
            guild_id, user_id, role_id, first_welcomed_at,
            last_seen_holder_at, welcome_message_id
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(guild_id, user_id)
        DO UPDATE SET
            role_id = excluded.role_id,
            last_seen_holder_at = excluded.last_seen_holder_at,
            welcome_message_id = COALESCE(
                excluded.welcome_message_id,
                holder_welcomes.welcome_message_id
            )
        """,
        (
            int(guild_id),
            int(user_id),
            int(role_id),
            now,
            now,
            int(welcome_message_id) if welcome_message_id else None,
        ),
    )
    conn.commit()
    conn.close()
    return not existed
