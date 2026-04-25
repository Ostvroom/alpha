"""
Velcor3 — lightweight event log for website dashboards.

Stores a compact record each time the bot posts an alert (project discovery, wallet tracker,
token alert, telegram call, daily mints, etc.).
"""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from app_paths import DATA_DIR, ensure_dirs

ensure_dirs()
DB_FILE = str(DATA_DIR / "feed_events.db")
PG_DSN = (os.getenv("DATABASE_URL") or "").strip()


def _use_pg() -> bool:
    return bool(PG_DSN)


def _conn_sqlite():
    return sqlite3.connect(DB_FILE)


def _conn_pg():
    try:
        import psycopg
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "DATABASE_URL is set but psycopg is not installed. "
            "Install with: pip install psycopg[binary]"
        ) from e
    return psycopg.connect(PG_DSN)


def init_db() -> None:
    if _use_pg():
        conn = _conn_pg()
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS feed_events (
                id BIGSERIAL PRIMARY KEY,
                ts TIMESTAMPTZ NOT NULL DEFAULT now(),
                kind TEXT NOT NULL,
                guild_id BIGINT DEFAULT 0,
                channel_id BIGINT DEFAULT 0,
                title TEXT,
                body TEXT,
                url TEXT,
                extra_json JSONB
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_feed_events_ts ON feed_events(ts DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_feed_events_kind ON feed_events(kind)")
        conn.commit()
        conn.close()
        return

    conn = _conn_sqlite()
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS feed_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            kind TEXT NOT NULL,
            guild_id INTEGER DEFAULT 0,
            channel_id INTEGER DEFAULT 0,
            title TEXT,
            body TEXT,
            url TEXT,
            extra_json TEXT
        )
        """
    )
    c.execute("CREATE INDEX IF NOT EXISTS idx_feed_events_ts ON feed_events(ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_feed_events_kind ON feed_events(kind)")
    conn.commit()
    conn.close()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        s = str(ts).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def add_event(
    *,
    kind: str,
    guild_id: int = 0,
    channel_id: int = 0,
    title: str = "",
    body: str = "",
    url: str = "",
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    kind = (kind or "").strip()[:48]
    if not kind:
        return
    init_db()
    title_s = (title or "")[:200]
    body_s = (body or "")[:1500]
    url_s = (url or "")[:500]
    extra_obj = extra or {}

    if _use_pg():
        conn = _conn_pg()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT ts::text, COALESCE(title,''), COALESCE(body,''), COALESCE(url,'')
                FROM feed_events
                WHERE kind = %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (kind,),
            )
            prev = cur.fetchone()
            if prev:
                prev_ts, prev_title, prev_body, prev_url = prev
                if prev_title == title_s and prev_body == body_s and prev_url == url_s:
                    a = _parse_ts(prev_ts)
                    b = _parse_ts(_now_iso())
                    if a and b and abs((b - a).total_seconds()) <= 120:
                        conn.close()
                        return
        except Exception:
            pass

        cur.execute(
            """
            INSERT INTO feed_events (ts, kind, guild_id, channel_id, title, body, url, extra_json)
            VALUES (now(), %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                kind,
                int(guild_id or 0),
                int(channel_id or 0),
                title_s,
                body_s,
                url_s,
                json.dumps(extra_obj, separators=(",", ":"), ensure_ascii=False),
            ),
        )
        conn.commit()
        conn.close()
        return

    conn = _conn_sqlite()
    c = conn.cursor()
    try:
        c.execute(
            """
            SELECT ts, title, body, url
            FROM feed_events
            WHERE kind = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (kind,),
        )
        prev = c.fetchone()
        if prev:
            prev_ts, prev_title, prev_body, prev_url = prev
            if (prev_title or "") == title_s and (prev_body or "") == body_s and (prev_url or "") == url_s:
                a = _parse_ts(prev_ts)
                b = _parse_ts(_now_iso())
                if a and b and abs((b - a).total_seconds()) <= 120:
                    conn.close()
                    return
    except Exception:
        pass
    c.execute(
        """
        INSERT INTO feed_events (ts, kind, guild_id, channel_id, title, body, url, extra_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _now_iso(),
            kind,
            int(guild_id or 0),
            int(channel_id or 0),
            title_s,
            body_s,
            url_s,
            json.dumps(extra_obj, separators=(",", ":"), ensure_ascii=False) if extra_obj else None,
        ),
    )
    conn.commit()
    conn.close()


def list_events(
    *,
    limit: int = 100,
    kinds: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    init_db()
    limit = max(1, min(800, int(limit or 100)))
    rows: List[tuple] = []
    if _use_pg():
        conn = _conn_pg()
        cur = conn.cursor()
        params: List[Any] = []
        where = ""
        if kinds:
            ks = [str(k).strip()[:48] for k in kinds if str(k).strip()]
            if ks:
                where = "WHERE kind = ANY(%s)"
                params.append(ks)
        q = f"""
            SELECT id, ts::text, kind, guild_id, channel_id, title, body, url, COALESCE(extra_json::text,'')
            FROM feed_events
            {where}
            ORDER BY id DESC
            LIMIT %s
        """
        params.append(limit)
        cur.execute(q, tuple(params))
        rows = list(cur.fetchall() or [])
        conn.close()
    else:
        conn = _conn_sqlite()
        c = conn.cursor()
        params2: List[Any] = []
        where2 = ""
        if kinds:
            ks = [str(k).strip()[:48] for k in kinds if str(k).strip()]
            if ks:
                where2 = "WHERE kind IN (" + ",".join(["?"] * len(ks)) + ")"
                params2.extend(ks)
        c.execute(
            f"""
            SELECT id, ts, kind, guild_id, channel_id, title, body, url, extra_json
            FROM feed_events
            {where2}
            ORDER BY id DESC
            LIMIT ?
            """,
            (*params2, limit),
        )
        rows = c.fetchall()
        conn.close()
    out: List[Dict[str, Any]] = []
    for _id, ts, kind, gid, cid, title, body, url, extra_json in rows:
        extra = None
        if extra_json:
            try:
                extra = json.loads(extra_json)
            except Exception:
                extra = None
        out.append(
            {
                "id": int(_id),
                "ts": ts,
                "kind": kind,
                "guild_id": int(gid or 0),
                "channel_id": int(cid or 0),
                "title": title or "",
                "body": body or "",
                "url": url or "",
                "extra": extra or {},
            }
        )
    # UI-friendly de-dupe: collapse adjacent identical events within ~2 minutes.
    # Helps clean up old duplicates already in the DB, especially for escalation feeds.
    deduped: List[Dict[str, Any]] = []
    last_key = None
    last_ts = None
    for ev in out:
        key = (ev.get("kind"), ev.get("title"), ev.get("body"), ev.get("url"))
        ts_dt = _parse_ts(ev.get("ts"))
        if key == last_key and ts_dt and last_ts and abs((ts_dt - last_ts).total_seconds()) <= 120:
            continue
        deduped.append(ev)
        last_key = key
        last_ts = ts_dt
    return deduped


def get_event(event_id: int) -> Optional[Dict[str, Any]]:
    """Fetch one event by id."""
    try:
        event_id = int(event_id)
    except Exception:
        return None
    if event_id <= 0:
        return None
    init_db()
    row = None
    if _use_pg():
        conn = _conn_pg()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, ts::text, kind, guild_id, channel_id, title, body, url, COALESCE(extra_json::text,'')
            FROM feed_events
            WHERE id = %s
            LIMIT 1
            """,
            (event_id,),
        )
        row = cur.fetchone()
        conn.close()
    else:
        conn = _conn_sqlite()
        c = conn.cursor()
        c.execute(
            """
            SELECT id, ts, kind, guild_id, channel_id, title, body, url, extra_json
            FROM feed_events
            WHERE id = ?
            LIMIT 1
            """,
            (event_id,),
        )
        row = c.fetchone()
        conn.close()
    if not row:
        return None
    _id, ts, kind, gid, cid, title, body, url, extra_json = row
    extra = {}
    if extra_json:
        try:
            extra = json.loads(extra_json) or {}
        except Exception:
            extra = {}
    return {
        "id": int(_id),
        "ts": ts,
        "kind": kind,
        "guild_id": int(gid or 0),
        "channel_id": int(cid or 0),
        "title": title or "",
        "body": body or "",
        "url": url or "",
        "extra": extra,
    }


def delete_events_by_kind(kind: str) -> int:
    """Delete all events of one kind; returns deleted row count."""
    k = str(kind or "").strip()[:48]
    if not k:
        return 0
    init_db()
    if _use_pg():
        conn = _conn_pg()
        cur = conn.cursor()
        cur.execute("DELETE FROM feed_events WHERE kind = %s", (k,))
        n = int(cur.rowcount or 0)
        conn.commit()
        conn.close()
        return n
    conn = _conn_sqlite()
    c = conn.cursor()
    c.execute("DELETE FROM feed_events WHERE kind = ?", (k,))
    n = int(c.rowcount or 0)
    conn.commit()
    conn.close()
    return n

