"""
Per-user NFT contract watchlist (SQLite). Used for DM alerts on live/hot/smart activity.
"""
from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

from app_paths import DATA_DIR, ensure_dirs

ensure_dirs()
DB_PATH = DATA_DIR / "user_watchlist.db"

_MAX_PER_USER_DEFAULT = 15
_dm_cooldown: Dict[Tuple[int, str], float] = {}


def _conn():
    return sqlite3.connect(str(DB_PATH))


def init_db() -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_watchlist (
            user_id INTEGER NOT NULL,
            contract TEXT NOT NULL,
            label TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            PRIMARY KEY (user_id, contract)
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_watchlist_contract ON user_watchlist(contract)"
    )
    conn.commit()
    conn.close()


def normalize_contract(addr: str) -> Optional[str]:
    a = (addr or "").strip().lower()
    if a.startswith("0x") and len(a) == 42:
        try:
            int(a[2:], 16)
            return a
        except ValueError:
            return None
    return None


def add_watch(user_id: int, contract: str, *, label: str = "", max_per_user: int = _MAX_PER_USER_DEFAULT) -> Tuple[bool, str]:
    init_db()
    c = normalize_contract(contract)
    if not c:
        return False, "Invalid contract address (use `0x` + 40 hex chars)."
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM user_watchlist WHERE user_id = ?", (int(user_id),))
    n = int(cur.fetchone()[0] or 0)
    if n >= max_per_user:
        conn.close()
        return False, f"You can watch at most **{max_per_user}** contracts. Remove one with `/watch remove`."
    cur.execute(
        "SELECT 1 FROM user_watchlist WHERE user_id = ? AND contract = ?",
        (int(user_id), c),
    )
    if cur.fetchone():
        conn.close()
        return False, f"Already watching `{c[:6]}...{c[-4:]}`."
    cur.execute(
        """
        INSERT INTO user_watchlist (user_id, contract, label, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (int(user_id), c, (label or "")[:120], datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    conn.close()
    return True, f"Watching **`{c}`** — you'll get a DM when this collection has mint activity."


def remove_watch(user_id: int, contract: str) -> Tuple[bool, str]:
    init_db()
    c = normalize_contract(contract)
    if not c:
        return False, "Invalid contract address."
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM user_watchlist WHERE user_id = ? AND contract = ?",
        (int(user_id), c),
    )
    conn.commit()
    deleted = cur.rowcount > 0
    conn.close()
    if not deleted:
        return False, "That contract is not on your watchlist."
    return True, f"Removed `{c[:6]}...{c[-4:]}` from your watchlist."


def list_watches(user_id: int) -> List[Dict[str, str]]:
    init_db()
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT contract, label, created_at FROM user_watchlist
        WHERE user_id = ? ORDER BY created_at DESC
        """,
        (int(user_id),),
    )
    rows = [
        {"contract": r[0], "label": r[1] or "", "created_at": r[2] or ""}
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


def watchers_for_contract(contract: str) -> List[int]:
    init_db()
    c = normalize_contract(contract)
    if not c:
        return []
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT user_id FROM user_watchlist WHERE contract = ?",
        (c,),
    )
    out = [int(r[0]) for r in cur.fetchall()]
    conn.close()
    return out


def is_watched_by(user_id: int, contract: str) -> bool:
    init_db()
    c = normalize_contract(contract)
    if not c:
        return False
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM user_watchlist WHERE user_id = ? AND contract = ?",
        (int(user_id), c),
    )
    ok = cur.fetchone() is not None
    conn.close()
    return ok


def _dm_allowed(user_id: int, contract: str, cooldown_sec: int) -> bool:
    key = (int(user_id), contract.lower())
    now = time.time()
    last = _dm_cooldown.get(key, 0.0)
    if now - last < cooldown_sec:
        return False
    _dm_cooldown[key] = now
    if len(_dm_cooldown) > 5000:
        cutoff = now - cooldown_sec * 2
        for k in list(_dm_cooldown.keys()):
            if _dm_cooldown[k] < cutoff:
                del _dm_cooldown[k]
    return True
