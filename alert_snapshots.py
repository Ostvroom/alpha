"""
Baseline metrics when the bot posts an alert — used for 24h performance recap.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from app_paths import DATA_DIR, ensure_dirs

ensure_dirs()
DB_PATH = DATA_DIR / "alert_snapshots.db"

_DEDUPE_HOURS = 12


def _conn():
    return sqlite3.connect(str(DB_PATH))


def init_db() -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS alert_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL,
            ref TEXT NOT NULL,
            ref_label TEXT DEFAULT '',
            alerted_at TEXT NOT NULL,
            guild_id INTEGER DEFAULT 0,
            channel_id INTEGER DEFAULT 0,
            message_id INTEGER DEFAULT 0,
            followers_at INTEGER,
            supply_at INTEGER,
            max_supply_at INTEGER,
            mc_at REAL,
            extra_json TEXT,
            measured_at TEXT,
            followers_now INTEGER,
            followers_delta INTEGER,
            supply_now INTEGER,
            supply_delta INTEGER,
            outcome TEXT,
            outcome_detail TEXT
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_alert_snap_at ON alert_snapshots(alerted_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_alert_snap_kind_ref ON alert_snapshots(kind, ref)"
    )
    conn.commit()
    conn.close()


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _parse_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        s = str(ts).replace("Z", "+00:00").replace(" ", "T", 1) if "T" not in str(ts) else str(ts)
        if "T" not in s and " " in s:
            s = s.replace(" ", "T", 1)
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def get_first_alert_jump(handle: str) -> Optional[str]:
    """Discord jump URL from earliest alert snapshot with channel/message ids."""
    h = str(handle or "").strip().lstrip("@").lower()
    if not h:
        return None
    init_db()
    conn = _conn()
    cur = conn.cursor()
    for kind in ("discovery", "escalation"):
        cur.execute(
            """
            SELECT guild_id, channel_id, message_id FROM alert_snapshots
            WHERE kind = ? AND ref = ?
            ORDER BY id ASC LIMIT 1
            """,
            (kind, h),
        )
        row = cur.fetchone()
        if not row:
            continue
        gid, cid, mid = int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)
        if gid and cid and mid:
            conn.close()
            return f"https://discord.com/channels/{gid}/{cid}/{mid}"
    conn.close()
    return None


def get_followers_at_alert(handle: str) -> Optional[int]:
    """Follower count stored when we first alerted this X handle (discovery/escalation)."""
    h = str(handle or "").strip().lstrip("@").lower()
    if not h:
        return None
    init_db()
    conn = _conn()
    cur = conn.cursor()
    for kind in ("discovery", "escalation"):
        cur.execute(
            """
            SELECT followers_at FROM alert_snapshots
            WHERE kind = ? AND ref = ? AND followers_at IS NOT NULL
            ORDER BY id ASC LIMIT 1
            """,
            (kind, h),
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            conn.close()
            try:
                return int(row[0])
            except (TypeError, ValueError):
                return None
    conn.close()
    return None


def _recent_duplicate(kind: str, ref: str) -> bool:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT alerted_at FROM alert_snapshots
        WHERE kind = ? AND ref = ?
        ORDER BY id DESC LIMIT 1
        """,
        (kind, ref),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return False
    at = _parse_ts(row[0])
    if not at:
        return False
    return datetime.now(timezone.utc) - at < timedelta(hours=_DEDUPE_HOURS)


def record_snapshot(
    *,
    kind: str,
    ref: str,
    ref_label: str = "",
    guild_id: int = 0,
    channel_id: int = 0,
    message_id: int = 0,
    followers_at: Optional[int] = None,
    supply_at: Optional[int] = None,
    max_supply_at: Optional[int] = None,
    mc_at: Optional[float] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """Store baseline at alert time (deduped per kind+ref within 12h)."""
    try:
        import config

        if not getattr(config, "ENABLE_PERFORMANCE_RECAP", True):
            return
    except Exception:
        pass

    kind = (kind or "").strip()[:48]
    ref = (ref or "").strip().lower()[:128]
    if not kind or not ref:
        return

    init_db()
    if _recent_duplicate(kind, ref):
        return

    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO alert_snapshots (
            kind, ref, ref_label, alerted_at, guild_id, channel_id, message_id,
            followers_at, supply_at, max_supply_at, mc_at, extra_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            kind,
            ref,
            (ref_label or "")[:200],
            _now_iso(),
            int(guild_id or 0),
            int(channel_id or 0),
            int(message_id or 0),
            int(followers_at) if followers_at is not None else None,
            int(supply_at) if supply_at is not None else None,
            int(max_supply_at) if max_supply_at is not None else None,
            float(mc_at) if mc_at is not None else None,
            json.dumps(extra or {}, separators=(",", ":"), ensure_ascii=False),
        ),
    )
    conn.commit()
    conn.close()


def record_from_feed_event(
    *,
    kind: str,
    guild_id: int = 0,
    channel_id: int = 0,
    title: str = "",
    url: str = "",
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """Map feed_events.add_event payloads into alert_snapshots."""
    extra = extra or {}
    ref = ""
    ref_label = (title or "")[:200]
    followers_at = None
    supply_at = None
    max_supply_at = None
    mc_at = None

    if kind in ("discovery", "escalation"):
        handle = (extra.get("handle") or "").strip().lstrip("@").lower()
        ref = handle or (url or "").rstrip("/").split("/")[-1].lower()
        followers_at = extra.get("followers")
        try:
            followers_at = int(followers_at) if followers_at is not None else None
        except (TypeError, ValueError):
            followers_at = None
        ref_label = f"@{handle}" if handle else ref_label

    elif kind == "wallet_nft":
        ref = (extra.get("contract") or "").lower()
        ref_label = (extra.get("wallet") or ref_label)[:200]

    elif kind == "token_alert":
        ref = (extra.get("mint") or "").strip()
        ref_label = (extra.get("symbol") or extra.get("name") or ref_label)[:200]
        try:
            mc_at = float(extra.get("alert_mc")) if extra.get("alert_mc") is not None else None
        except (TypeError, ValueError):
            mc_at = None

    elif kind == "live_mint":
        ref = (extra.get("contract") or "").lower()
        ref_label = (extra.get("name") or ref)[:200]
        supply_at = extra.get("supply_at")
        max_supply_at = extra.get("max_supply_at")
        try:
            supply_at = int(supply_at) if supply_at is not None else None
        except (TypeError, ValueError):
            supply_at = None
        try:
            max_supply_at = int(max_supply_at) if max_supply_at is not None else None
        except (TypeError, ValueError):
            max_supply_at = None

    else:
        ref = (url or title or kind)[:128]

    if not ref:
        return

    message_id = 0
    try:
        message_id = int(extra.get("message_id") or 0)
    except (TypeError, ValueError):
        message_id = 0

    record_snapshot(
        kind=kind,
        ref=ref,
        ref_label=ref_label,
        guild_id=guild_id,
        channel_id=channel_id,
        message_id=message_id,
        followers_at=followers_at,
        supply_at=supply_at,
        max_supply_at=max_supply_at,
        mc_at=mc_at,
        extra=extra,
    )


def list_pending_measurement(*, min_age_hours: float = 24.0, limit: int = 80) -> List[Dict[str, Any]]:
    """Snapshots at least min_age_hours old that have not been measured yet."""
    init_db()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=min_age_hours)
    cutoff_s = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, kind, ref, ref_label, alerted_at, followers_at, supply_at, max_supply_at,
               mc_at, extra_json
        FROM alert_snapshots
        WHERE measured_at IS NULL AND alerted_at <= ?
        ORDER BY alerted_at ASC
        LIMIT ?
        """,
        (cutoff_s, max(1, min(200, int(limit)))),
    )
    rows = cur.fetchall()
    conn.close()
    out = []
    for r in rows:
        extra = {}
        try:
            extra = json.loads(r[9] or "{}")
        except Exception:
            pass
        out.append(
            {
                "id": r[0],
                "kind": r[1],
                "ref": r[2],
                "ref_label": r[3],
                "alerted_at": r[4],
                "followers_at": r[5],
                "supply_at": r[6],
                "max_supply_at": r[7],
                "mc_at": r[8],
                "extra": extra,
            }
        )
    return out


def save_measurement(
    snap_id: int,
    *,
    followers_now: Optional[int] = None,
    followers_delta: Optional[int] = None,
    supply_now: Optional[int] = None,
    supply_delta: Optional[int] = None,
    outcome: str = "",
    outcome_detail: str = "",
) -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE alert_snapshots SET
            measured_at = ?,
            followers_now = ?,
            followers_delta = ?,
            supply_now = ?,
            supply_delta = ?,
            outcome = ?,
            outcome_detail = ?
        WHERE id = ?
        """,
        (
            _now_iso(),
            followers_now,
            followers_delta,
            supply_now,
            supply_delta,
            (outcome or "")[:32],
            (outcome_detail or "")[:500],
            int(snap_id),
        ),
    )
    conn.commit()
    conn.close()


def list_measured_since_hours(hours: float = 26.0, limit: int = 100) -> List[Dict[str, Any]]:
    """Recently measured snapshots for recap display."""
    init_db()
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    since_s = since.strftime("%Y-%m-%d %H:%M:%S")
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, kind, ref, ref_label, alerted_at, followers_at, followers_delta,
               supply_at, supply_delta, outcome, outcome_detail, mc_at, extra_json
        FROM alert_snapshots
        WHERE measured_at IS NOT NULL AND measured_at >= ?
        ORDER BY
            CASE outcome WHEN 'winner' THEN 0 WHEN 'flat' THEN 1 ELSE 2 END,
            COALESCE(followers_delta, 0) DESC,
            COALESCE(supply_delta, 0) DESC
        LIMIT ?
        """,
        (since_s, max(1, min(300, int(limit)))),
    )
    rows = cur.fetchall()
    conn.close()
    out = []
    for r in rows:
        out.append(
            {
                "id": r[0],
                "kind": r[1],
                "ref": r[2],
                "ref_label": r[3],
                "alerted_at": r[4],
                "followers_at": r[5],
                "followers_delta": r[6],
                "supply_at": r[7],
                "supply_delta": r[8],
                "outcome": r[9],
                "outcome_detail": r[10],
                "mc_at": r[11],
                "extra": json.loads(r[12] or "{}") if r[12] else {},
            }
        )
    return out


def count_snapshots_alerted_since_hours(hours: float = 24.0) -> Dict[str, int]:
    init_db()
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    since_s = since.strftime("%Y-%m-%d %H:%M:%S")
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT kind, COUNT(*) FROM alert_snapshots
        WHERE alerted_at >= ?
        GROUP BY kind
        """,
        (since_s,),
    )
    rows = cur.fetchall()
    conn.close()
    return {str(k): int(n) for k, n in rows}
