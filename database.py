import sqlite3
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from app_paths import DATA_DIR, ensure_dirs

ensure_dirs()
DB_PATH = os.path.join(DATA_DIR, "block_brain.db")

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Track accounts we've already alerted on
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS projects (
            twitter_id TEXT PRIMARY KEY,
            handle TEXT,
            name TEXT,
            description TEXT,
            created_at TEXT,
            ai_summary TEXT,
            ai_category TEXT,
            ai_alpha_score INTEGER DEFAULT 0,
            followers_count INTEGER,
            first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_posted_smarts INTEGER DEFAULT 0
        )
    """)
    
    # Track who followed/interacted (for multi-HVA alerts)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS follows (
            project_id TEXT,
            hva_id TEXT,
            interaction_type TEXT DEFAULT 'follow', -- 'follow', 'like', 'reply', 'retweet', 'mention', 'quote', 'ct_domain', 'keyword_search'
            followed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(project_id) REFERENCES projects(twitter_id),
            PRIMARY KEY(project_id, hva_id, interaction_type)
        )
    """)
    
    # Track HVA stats for priority scanning and delta detection
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hva_stats (
            hva_handle TEXT PRIMARY KEY,
            discovery_count INTEGER DEFAULT 0,
            last_scan_at TEXT,
            last_follows_count INTEGER DEFAULT 0,
            status TEXT DEFAULT 'Active',
            quality_score REAL DEFAULT 0.0,
            error_count INTEGER DEFAULT 0
        )
    """)
    
    # Migration: Add status if missing
    try: cursor.execute("ALTER TABLE hva_stats ADD COLUMN status TEXT DEFAULT 'Active'")
    except: pass
    try: cursor.execute("ALTER TABLE hva_stats ADD COLUMN last_scan_at TEXT")
    except: pass
    try: cursor.execute("ALTER TABLE hva_stats ADD COLUMN quality_score REAL DEFAULT 0.0")
    except: pass
    try: cursor.execute("ALTER TABLE hva_stats ADD COLUMN error_count INTEGER DEFAULT 0")
    except: pass
    
    # Handle old 'last_scan' column removal/replacement
    try: cursor.execute("ALTER TABLE hva_stats ADD COLUMN last_follows_count INTEGER DEFAULT 0")
    except: pass
    
    # Migration: Add alerted_at for tracking which projects were actually alerted
    try: cursor.execute("ALTER TABLE projects ADD COLUMN alerted_at TIMESTAMP")
    except: pass
    # Migration: explicit "actually posted to Discord" flag (prevents daily-finds spam)
    try:
        cursor.execute("ALTER TABLE projects ADD COLUMN alerted_discord INTEGER DEFAULT 0")
    except Exception:
        pass
    # Migrate: last_posted_smarts
    try: cursor.execute("ALTER TABLE projects ADD COLUMN last_posted_smarts INTEGER DEFAULT 0")
    except: pass
    
    # Migrate: AI columns
    try: cursor.execute("ALTER TABLE projects ADD COLUMN ai_summary TEXT")
    except: pass
    try: cursor.execute("ALTER TABLE projects ADD COLUMN ai_category TEXT")
    except: pass
    try: cursor.execute("ALTER TABLE projects ADD COLUMN ai_alpha_score INTEGER DEFAULT 0")
    except: pass
    try:
        cursor.execute("ALTER TABLE projects ADD COLUMN followers_count INTEGER")
    except Exception:
        pass
    
    # Track signal alerts for new-accs-signal feature
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS signal_alerts (
            project_id TEXT PRIMARY KEY,
            first_discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_signal_level TEXT DEFAULT 'none',
            last_alert_at TIMESTAMP,
            FOREIGN KEY(project_id) REFERENCES projects(twitter_id)
        )
    """)

    # X project-first search keyword rules (editable without code changes)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS x_project_search_keywords (
            keyword TEXT PRIMARY KEY,
            weight  INTEGER DEFAULT 1,
            enabled INTEGER DEFAULT 1,
            source  TEXT DEFAULT 'seed'
        )
        """
    )

    # Dedup for project-first search tweets (avoid reprocessing every cycle)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS x_project_search_seen (
            tweet_id TEXT PRIMARY KEY,
            seen_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    # Presale submissions
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS presale_submissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tx_hash TEXT NOT NULL,
            discord_id TEXT NOT NULL,
            discord_username TEXT DEFAULT '',
            submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'pending',
            notes TEXT DEFAULT '',
            UNIQUE(tx_hash)
        )
        """
    )
    try:
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_presale_discord ON presale_submissions(discord_id)")
    except Exception:
        pass

    # Distinct @handles observed per X user id (X does not expose full rename history via API).
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS x_handle_snapshots (
            twitter_id TEXT NOT NULL,
            handle TEXT NOT NULL,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source TEXT DEFAULT '',
            UNIQUE(twitter_id, handle)
        )
        """
    )
    try:
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_x_handle_snapshots_tid ON x_handle_snapshots(twitter_id)"
        )
    except Exception:
        pass
    
    # Optional migration: backfill alerted_at for legacy DBs.
    # Disabled by default because it can cause "daily finds" to include projects that were never posted.
    if os.getenv("MIGRATE_ALERTED_AT", "0").strip() == "1":
        try:
            cursor.execute(
                """
                UPDATE projects
                SET alerted_at = first_seen_at
                WHERE alerted_at IS NULL
                  AND first_seen_at >= datetime('now', '-7 day')
                """
            )
        except Exception:
            pass

    # Always backfill alerted_discord=1 for projects that have alerted_at set.
    # alerted_at is only written when we post to Discord, so this is always safe.
    try:
        cursor.execute(
            """
            UPDATE projects
            SET alerted_discord = 1
            WHERE alerted_at IS NOT NULL
              AND COALESCE(alerted_discord, 0) = 0
            """
        )
    except Exception:
        pass

    # Remove blocklisted HVAs from hva_stats (handles config/DB drift after edits to HVA_LIST).
    try:
        import config

        block = {
            str(h).strip().lower()
            for h in (getattr(config, "HVA_BLOCKLIST", None) or [])
            if str(h).strip()
        }
        for h in block:
            cursor.execute("DELETE FROM hva_stats WHERE hva_handle = ?", (h,))
    except Exception:
        pass

    conn.commit()
    conn.close()


def seed_x_project_search_keywords_if_empty() -> None:
    """
    Seed `x_project_search_keywords` from config.PROJECT_CATEGORIES on first run.
    Safe to call repeatedly (only fills when table is empty).
    """
    try:
        import config  # local import avoids circulars
    except Exception:
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM x_project_search_keywords")
        n = int(cursor.fetchone()[0] or 0)
    except Exception:
        conn.close()
        return
    if n > 0:
        conn.close()
        return

    kws = set()
    try:
        cats = getattr(config, "PROJECT_CATEGORIES", {}) or {}
        for _cat, words in cats.items():
            for w in (words or []):
                s = str(w).strip().lower()
                if s:
                    kws.add(s)
    except Exception:
        pass

    # Add a few strong project-first patterns
    kws.update(
        {
            "testnet",
            "mainnet",
            "whitelist",
            "waitlist",
            "airdrop",
            "mint",
            "launch",
            "token",
            "tge",
            "ca:",  # common "contract address" prefix
            "contract address",
            "docs",
            "sdk",
            "rpc",
        }
    )

    for k in sorted(kws):
        cursor.execute(
            "INSERT OR IGNORE INTO x_project_search_keywords(keyword, weight, enabled, source) VALUES (?, ?, 1, 'seed')",
            (k, 1),
        )

    conn.commit()
    conn.close()


def upsert_x_project_search_keywords(keywords: dict, source: str = "baseline") -> None:
    """
    Upsert a keyword→weight map into x_project_search_keywords.

    Safety rules:
    - Never disables existing keywords.
    - If keyword exists, keeps `enabled` as-is.
    - Weight only increases (takes max).
    """
    if not keywords:
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        for k, w in keywords.items():
            kw = str(k or "").strip().lower()
            if not kw:
                continue
            try:
                wt = int(w)
            except Exception:
                wt = 1
            wt = max(1, min(100, wt))
            cursor.execute(
                """
                INSERT INTO x_project_search_keywords(keyword, weight, enabled, source)
                VALUES (?, ?, 1, ?)
                ON CONFLICT(keyword) DO UPDATE SET
                    weight = CASE
                        WHEN excluded.weight > x_project_search_keywords.weight
                        THEN excluded.weight
                        ELSE x_project_search_keywords.weight
                    END
                """,
                (kw, wt, str(source or "baseline")),
            )
        conn.commit()
    finally:
        conn.close()


def seed_x_project_search_keywords_baseline() -> None:
    """
    Seed a larger, ecosystem-aware baseline keyword list for project-first discovery.
    This is safe to call every startup (upsert + weight only increases).
    """
    kws: dict[str, int] = {}

    def add(items, w: int):
        for it in items:
            s = str(it or "").strip().lower()
            if not s:
                continue
            kws[s] = max(kws.get(s, 0), int(w))

    # High-signal launch / early indicators
    add(
        [
            "testnet",
            "devnet",
            "mainnet",
            "public testnet",
            "incentivized testnet",
            "waitlist",
            "whitelist",
            "early access",
            "beta",
            "closed beta",
            "open beta",
            "coming soon",
            "introducing",
            "now live",
            "launching",
            "stealth",
            "stealth launch",
            "building",
            "we're building",
            "hiring",
            "founding engineer",
            "docs",
            "documentation",
            "litepaper",
            "whitepaper",
            "tokenomics",
            "airdrop",
            "airdrop checker",
            "claim",
            "points",
            "season",
            "snapshot",
            "quests",
            "task",
            "waitlist open",
            "mint live",
            "minting",
            "wl mint",
            "public mint",
        ],
        12,
    )

    # Contract / address patterns (very high-signal, noisy but valuable)
    add(["ca:", "contract address", "token address", "pair address"], 15)

    # Infra / protocol keywords
    add(
        [
            "protocol",
            "rollup",
            "zk",
            "zkp",
            "proof",
            "bridge",
            "oracle",
            "indexer",
            "rpc",
            "node",
            "validator",
            "sequencer",
            "data availability",
            "da layer",
            "l2",
            "l3",
            "modular",
            "restaking",
            "re-staking",
            "avs",
            "account abstraction",
            "smart wallet",
            "paymaster",
            "wallet sdk",
            "sdk",
            "api",
            "open source",
            "github",
        ],
        9,
    )

    # NFT / gaming signals
    add(
        [
            "nft",
            "pfp",
            "mint",
            "reveal",
            "supply",
            "allowlist",
            "art reveal",
            "collection",
            "gen art",
            "onchain",
            "on-chain",
            "game",
            "alpha test",
            "playtest",
            "demo",
            "gameplay",
            "launcher",
            "season pass",
        ],
        7,
    )

    # Ecosystem tags / chain names
    add(
        [
            "solana",
            "spl",
            "anchor",
            "sealevel",
            "evm",
            "ethereum",
            "erc20",
            "erc-20",
            "erc721",
            "erc-721",
            "erc1155",
            "erc-1155",
            "base",
            "optimism",
            "arbitrum",
            "polygon",
            "avax",
            "bsc",
            "bnb chain",
            "ton",
            "toncoin",
            "jetton",
        ],
        6,
    )

    # Solana-native venues / patterns (early memes + infra)
    add(
        [
            "pump.fun",
            "pumpfun",
            "raydium",
            "jupiter",
            "orca",
            "meteora",
            "drift",
            "marginfi",
            "kamino",
            "tensor",
            "magic eden",
            "helius",
        ],
        8,
    )

    # Base / L2 ecosystem
    add(["based", "base chain", "on base", "superchain", "op stack", "optimism stack"], 6)

    # TON ecosystem
    add(["mini app", "telegram mini app", "tma", "jetton", "ton nft", "ton defi"], 6)

    # Meme + degen terms (lower weight, but catches early stuff)
    add(
        [
            "memecoin",
            "meme",
            "fair launch",
            "presale",
            "pre-sale",
            "wl",
            "tg",
            "telegram",
            "discord",
            "community",
            "x spaces",
            "spaces",
        ],
        4,
    )

    upsert_x_project_search_keywords(kws, source="baseline")


def get_x_project_search_keywords(limit: int = 40) -> list[str]:
    """Return enabled keywords ordered by weight desc, then keyword asc."""
    limit = max(1, min(200, int(limit or 40)))
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT keyword FROM x_project_search_keywords
            WHERE enabled = 1
            ORDER BY weight DESC, keyword ASC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cursor.fetchall()
        return [r[0] for r in rows if r and r[0]]
    finally:
        conn.close()


def is_x_project_search_tweet_new(tweet_id: str) -> bool:
    if not tweet_id:
        return False
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM x_project_search_seen WHERE tweet_id = ?", (str(tweet_id),))
    res = cursor.fetchone()
    conn.close()
    return res is None


def mark_x_project_search_tweet_seen(tweet_id: str) -> None:
    if not tweet_id:
        return
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO x_project_search_seen(tweet_id) VALUES (?)",
        (str(tweet_id),),
    )
    conn.commit()
    conn.close()

def is_project_new(twitter_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM projects WHERE twitter_id = ?", (twitter_id,))
    res = cursor.fetchone()
    conn.close()
    return res is None

def save_project(
    twitter_id,
    handle,
    name,
    description,
    created_at,
    ai_summary=None,
    ai_category=None,
    ai_alpha_score=0,
    followers_count=None,
):
    """
    Upsert project metadata without wiping alerted_at / first_seen / last_posted_smarts
    (INSERT OR REPLACE was clearing alerted_at and broke strict 24h daily-finds queries).
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT handle FROM projects WHERE twitter_id = ?", (twitter_id,))
    prow = cursor.fetchone()
    prev_handle_key = str(prow[0]) if prow and prow[0] is not None else ""
    cursor.execute(
        """
        INSERT INTO projects (twitter_id, handle, name, description, created_at, ai_summary, ai_category, ai_alpha_score, followers_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(twitter_id) DO UPDATE SET
            handle = excluded.handle,
            name = excluded.name,
            description = excluded.description,
            created_at = excluded.created_at,
            ai_summary = excluded.ai_summary,
            ai_category = excluded.ai_category,
            ai_alpha_score = excluded.ai_alpha_score,
            followers_count = COALESCE(excluded.followers_count, projects.followers_count)
        """,
        (twitter_id, handle, name, description, created_at, ai_summary, ai_category, ai_alpha_score, followers_count),
    )
    conn.commit()
    conn.close()
    _record_handle_snapshot_after_project_save(twitter_id, handle, prev_handle_key)


def _normalize_handle_key(h: Optional[str]) -> str:
    if not h:
        return ""
    return str(h).strip().lstrip("@").lower()


def _record_handle_snapshot_after_project_save(
    twitter_id: str, new_handle: Optional[str], prev_handle_key: str
) -> None:
    """Log current and previous handle keys when a project row changes handle."""
    tid = str(twitter_id or "").strip()
    if not tid:
        return
    new_k = _normalize_handle_key(new_handle)
    if new_k:
        record_handle_snapshot(tid, new_k, "project_upsert")
    prev_k = _normalize_handle_key(prev_handle_key)
    if prev_k and new_k and prev_k != new_k:
        record_handle_snapshot(tid, prev_k, "before_rename")


def record_handle_snapshot(twitter_id: str, handle: str, source: str = "") -> None:
    """Upsert one (twitter_id, handle) row and refresh recorded_at (first-seen preserved via MIN elsewhere if needed)."""
    tid = str(twitter_id or "").strip()
    h = _normalize_handle_key(handle)
    if not tid or not h:
        return
    src = (source or "")[:120]
    conn = sqlite3.connect(DB_PATH)
    try:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO x_handle_snapshots (twitter_id, handle, source, recorded_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(twitter_id, handle) DO UPDATE SET
                recorded_at = excluded.recorded_at,
                source = excluded.source
            """,
            (tid, h, src),
        )
        conn.commit()
    finally:
        conn.close()


def list_remembered_handles(
    twitter_id: str, exclude_handle: Optional[str] = None, limit: int = 50
) -> list[dict]:
    """
    Handles stored for this user id, ordered by last observation (newest first).
    Excludes the current screen name when provided.
    """
    tid = str(twitter_id or "").strip()
    if not tid:
        return []
    lim = max(1, min(200, int(limit or 50)))
    ex = _normalize_handle_key(exclude_handle) if exclude_handle else ""
    conn = sqlite3.connect(DB_PATH)
    try:
        c = conn.cursor()
        c.execute(
            """
            SELECT handle, recorded_at
            FROM x_handle_snapshots
            WHERE twitter_id = ?
            ORDER BY recorded_at DESC
            LIMIT ?
            """,
            (tid, lim + 5),
        )
        rows = c.fetchall() or []
    finally:
        conn.close()
    out: list[dict] = []
    seen: set[str] = set()
    for h, ts in rows:
        key = _normalize_handle_key(str(h or ""))
        if not key or key == ex or key in seen:
            continue
        seen.add(key)
        out.append({"handle": key, "last_seen": str(ts or "")})
        if len(out) >= lim:
            break
    return out


def save_presale_submission(tx_hash: str, discord_id: str, discord_username: str = "") -> dict:
    """Insert a presale submission. Returns {'ok': True} or {'ok': False, 'error': str}."""
    th = str(tx_hash or "").strip()
    did = str(discord_id or "").strip()
    dname = str(discord_username or "")[:120]
    if not th or not did:
        return {"ok": False, "error": "tx_hash and discord_id are required"}
    conn = sqlite3.connect(DB_PATH)
    try:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO presale_submissions (tx_hash, discord_id, discord_username)
            VALUES (?, ?, ?)
            """,
            (th, did, dname),
        )
        conn.commit()
        return {"ok": True}
    except Exception as exc:
        conn.rollback()
        msg = str(exc)
        if "UNIQUE" in msg.upper():
            return {"ok": False, "error": "duplicate_tx"}
        return {"ok": False, "error": msg[:200]}
    finally:
        conn.close()


def get_project_ai_data(twitter_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT ai_summary, ai_category, ai_alpha_score FROM projects WHERE twitter_id = ?", (twitter_id,))
    res = cursor.fetchone()
    conn.close()
    if res:
        return {"summary": res[0], "category": res[1], "alpha_score": res[2]}
    return None

def save_follow(project_id, hva_id, interaction_type='follow'):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR IGNORE INTO follows (project_id, hva_id, interaction_type)
        VALUES (?, ?, ?)
    """, (project_id, hva_id, interaction_type))
    inserted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return inserted

def get_project_follows(project_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT hva_id, interaction_type FROM follows WHERE project_id = ?", (project_id,))
    res = cursor.fetchall()
    conn.close()
    return res # List of tuples [(hva_id, type), ...]


def get_project_follow_events(project_id: str, limit: int = 500):
    """
    Raw follow/interaction rows including timestamps, newest first.
    Returns: [(hva_id, interaction_type, followed_at), ...]
    """
    pid = str(project_id or "").strip()
    if not pid:
        return []
    lim = max(1, min(2000, int(limit or 500)))
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT hva_id, interaction_type, followed_at
        FROM follows
        WHERE project_id = ?
        ORDER BY datetime(followed_at) DESC
        LIMIT ?
        """,
        (pid, lim),
    )
    rows = cursor.fetchall() or []
    conn.close()
    return rows


def calculate_project_smart_followers_v2(project_id: str) -> dict:
    """
    Quality-weighted + recency-aware smart followers signal (X-first scoring).
    Returns:
      {
        "raw_score": float,
        "unique_hvas": int,
        "hvas_24h": int,
        "hvas_7d": int,
        "hvas_30d": int,
      }
    """
    pid = str(project_id or "").strip()
    if not pid:
        return {"raw_score": 0.0, "unique_hvas": 0, "hvas_24h": 0, "hvas_7d": 0, "hvas_30d": 0}

    events = get_project_follow_events(pid, limit=1200)
    if not events:
        return {"raw_score": 0.0, "unique_hvas": 0, "hvas_24h": 0, "hvas_7d": 0, "hvas_30d": 0}

    now = datetime.now(timezone.utc)
    unique_meta: dict = {}

    for row in events:
        if not row:
            continue
        hva = str(row[0] or "").strip().lower()
        it = str(row[1] or "").strip().lower()
        ts = _parse_sqlite_ts(row[2])
        if not hva:
            continue
        if hva not in unique_meta:
            unique_meta[hva] = {
                "last_ts": ts,
                "interactions": set([it] if it else []),
            }
        else:
            if ts and (unique_meta[hva]["last_ts"] is None or ts > unique_meta[hva]["last_ts"]):
                unique_meta[hva]["last_ts"] = ts
            if it:
                unique_meta[hva]["interactions"].add(it)

    if not unique_meta:
        return {"raw_score": 0.0, "unique_hvas": 0, "hvas_24h": 0, "hvas_7d": 0, "hvas_30d": 0}

    import config  # local import avoids circulars
    t1 = {str(h or "").strip().lower() for h in (getattr(config, "TIER_1_HVAs", []) or []) if str(h or "").strip()}
    tier_w = getattr(config, "HVA_TIER_WEIGHTS", {}) or {}
    t1_weight = float(tier_w.get("tier1", 3) or 3)
    t3_weight = float(tier_w.get("tier3", 1) or 1)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    raw_score = 0.0
    hvas_24h, hvas_7d, hvas_30d = 0, 0, 0

    try:
        for hva, meta in unique_meta.items():
            last_ts = meta.get("last_ts")
            age_hours = None
            if isinstance(last_ts, datetime):
                age_hours = max(0.0, (now - last_ts).total_seconds() / 3600.0)
                if age_hours <= 24:
                    hvas_24h += 1
                if age_hours <= 24 * 7:
                    hvas_7d += 1
                if age_hours <= 24 * 30:
                    hvas_30d += 1

            if age_hours is None:
                recency_mult = 0.85
            elif age_hours <= 24:
                recency_mult = 1.35
            elif age_hours <= 72:
                recency_mult = 1.20
            elif age_hours <= 24 * 7:
                recency_mult = 1.05
            elif age_hours <= 24 * 30:
                recency_mult = 0.90
            else:
                recency_mult = 0.75

            cursor.execute("SELECT quality_score FROM hva_stats WHERE hva_handle = ?", (hva,))
            res = cursor.fetchone()
            try:
                perf = float(res[0]) if (res and res[0] is not None) else 0.0
            except Exception:
                perf = 0.0
            perf_mult = max(0.5, min(2.0, perf / 50.0)) if perf > 0 else 1.0
            tier_weight = t1_weight if hva in t1 else t3_weight

            raw_score += 15.0 * tier_weight * perf_mult * recency_mult

            interactions = meta.get("interactions") or set()
            if "retweet" in interactions:
                raw_score += 4.0
            if "reply" in interactions:
                raw_score += 2.0
    finally:
        conn.close()

    if hvas_24h >= 3:
        raw_score += 18.0
    elif hvas_24h >= 2:
        raw_score += 10.0
    if hvas_7d >= 5:
        raw_score += 8.0

    return {
        "raw_score": float(raw_score),
        "unique_hvas": int(len(unique_meta)),
        "hvas_24h": int(hvas_24h),
        "hvas_7d": int(hvas_7d),
        "hvas_30d": int(hvas_30d),
    }

def update_posted_smarts(project_id, count):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("UPDATE projects SET last_posted_smarts = ? WHERE twitter_id = ?", (count, project_id))
    conn.commit()
    conn.close()

def get_posted_smarts(project_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT last_posted_smarts FROM projects WHERE twitter_id = ?", (project_id,))
    res = cursor.fetchone()
    conn.close()
    return res[0] if res else 0

def update_project_followers_count(twitter_id, followers_count) -> None:
    """Best-effort refresh of follower snapshot (existing rows only)."""
    if followers_count is None:
        return
    try:
        n = int(followers_count)
    except (TypeError, ValueError):
        return
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE projects SET followers_count = ? WHERE twitter_id = ?",
        (n, str(twitter_id)),
    )
    conn.commit()
    conn.close()


def mark_alerted(project_id):
    """
    Mark a project as alerted.

    IMPORTANT: `alerted_at` should represent the *first time* we alerted on the project.
    Escalations/momentum updates should not move a project into today's Daily Finds.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE projects
        SET
            alerted_at = COALESCE(alerted_at, CURRENT_TIMESTAMP),
            alerted_discord = 1
        WHERE twitter_id = ?
        """,
        (project_id,),
    )
    conn.commit()
    conn.close()

def get_recent_follows(project_id, hours=24):
    """Get follows within the last X hours for velocity detection."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT hva_id, interaction_type, followed_at FROM follows 
        WHERE project_id = ? AND followed_at >= datetime('now', ? || ' hours')
    """, (project_id, -hours))
    res = cursor.fetchall()
    conn.close()
    return res

def get_alerted_projects():
    """Get only projects that were actually alerted (for Trending)."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT twitter_id, handle, name, last_posted_smarts, alerted_at 
        FROM projects WHERE alerted_at IS NOT NULL AND COALESCE(alerted_discord, 0) = 1
        ORDER BY last_posted_smarts DESC
    """)
    res = cursor.fetchall()
    conn.close()
    return res

def was_project_alerted(twitter_id):
    """Check if a project was previously alerted (has alerted_at timestamp)."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT alerted_at FROM projects WHERE twitter_id = ?", (twitter_id,))
    res = cursor.fetchone()
    conn.close()
    return res is not None and res[0] is not None

def get_trending_projects_30d(limit: int = 30):
    """
    Trending projects by DISTINCT HVA count in the last 30 days.
    Returns rows:
    (twitter_id, handle, name, description, created_at, hva_30d, ai_summary, ai_category)
    """
    limit = max(1, min(200, int(limit or 30)))
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        WITH Smarts30d AS (
            SELECT f.project_id, COUNT(DISTINCT f.hva_id) AS count
            FROM follows f
            WHERE f.followed_at >= datetime('now', '-30 day')
            GROUP BY f.project_id
        )
        SELECT
            p.twitter_id, p.handle, p.name, p.description, p.created_at,
            COALESCE(s30.count, 0) AS hva_30d,
            p.ai_summary, p.ai_category
        FROM projects p
        LEFT JOIN Smarts30d s30 ON p.twitter_id = s30.project_id
        WHERE p.alerted_at IS NOT NULL
          AND COALESCE(p.alerted_discord, 0) = 1
        ORDER BY COALESCE(s30.count, 0) DESC, datetime(p.alerted_at) DESC
        LIMIT ?
        """,
        (limit,),
    )
    res = cursor.fetchall()
    conn.close()
    return res

# ═══════════════════════════════════════════════════════════════
# HVA STATS FUNCTIONS (Priority Scanning & Delta Detection)
# ═══════════════════════════════════════════════════════════════

def _hva_blocklist_lower():
    try:
        import config

        return {
            str(h).strip().lower()
            for h in (getattr(config, "HVA_BLOCKLIST", None) or [])
            if str(h).strip()
        }
    except Exception:
        return set()


def increment_hva_discovery(hva_handle):
    """Increment discovery count for an HVA (for priority ranking)."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO hva_stats (hva_handle, discovery_count) VALUES (?, 1)
        ON CONFLICT(hva_handle) DO UPDATE SET discovery_count = discovery_count + 1
    """, (hva_handle.lower(),))
    conn.commit()
    conn.close()

def get_hva_priority_list():
    """Return all active HVAs with intelligent 5-tier priority system.
    
    Priority Tiers (Highest to Lowest):
    1. TIER 1 (🔥 Discovery) - Never scanned yet (last_scan_at = NULL)
    2. TIER 2 (⭐ Elite) - Top performers (5+ discoveries)
    3. TIER 3 (✅ Active) - Active hunters (1-4 discoveries)
    4. TIER 4 (🕐 Refresh) - Inactive but stale (0 discoveries, last scan >7 days)
    5. TIER 5 (💤 Low) - Inactive and recently scanned (0 discoveries, last scan <7 days)
    
    This ensures never-scanned HVAs get priority while maintaining efficiency.
    """
    import random
    from datetime import datetime, timedelta
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get all active HVAs with scan history
    cursor.execute(
        """
        SELECT hva_handle, discovery_count, last_scan_at, status
        FROM hva_stats
        WHERE status != 'Dead'
        """
    )
    data = cursor.fetchall()

    block = _hva_blocklist_lower()
    if block:
        data = [row for row in data if row[0].lower() not in block]

    # Keep DB and config list aligned:
    # existing deployments often keep an old hva_stats set (seeded once), so newly
    # added config.HVA_LIST handles never enter priority tiers unless manually added.
    import config

    cfg_norm = []
    for h in (config.HVA_LIST or []):
        k = str(h or "").strip().lstrip("@").lower()
        if not k:
            continue
        if block and k in block:
            continue
        cfg_norm.append(k)

    by_handle = {}
    for handle, disc_count, last_scan, status in data:
        hk = str(handle or "").strip().lstrip("@").lower()
        if not hk:
            continue
        by_handle[hk] = (hk, int(disc_count or 0), last_scan, status)

    missing_cfg = [h for h in cfg_norm if h not in by_handle]
    if missing_cfg:
        for h in missing_cfg:
            cursor.execute(
                """
                INSERT INTO hva_stats (hva_handle, status, discovery_count)
                VALUES (?, 'Active', 0)
                ON CONFLICT(hva_handle) DO UPDATE SET status='Active'
                """,
                (h,),
            )
            by_handle[h] = (h, 0, None, "Active")
        conn.commit()

    conn.close()
    data = list(by_handle.values())

    if not data:
        return cfg_norm

    # Initialize tier buckets
    tier1_never_scanned = []      # Never scanned (highest priority)
    tier2_elite = []               # 5+ discoveries
    tier3_active = []              # 1-4 discoveries
    tier4_refresh = []             # 0 discoveries, >7 days old
    tier5_low = []                 # 0 discoveries, <7 days old
    
    week_ago = datetime.now() - timedelta(days=7)
    
    for handle, disc_count, last_scan, status in data:
        # TIER 1: Never scanned (highest priority - get these checked ASAP)
        if last_scan is None:
            tier1_never_scanned.append(handle)
        
        # TIER 2: Elite performers (5+ discoveries)
        elif disc_count >= 5:
            tier2_elite.append(handle)
        
        # TIER 3: Active hunters (1-4 discoveries)
        elif disc_count >= 1:
            tier3_active.append(handle)
        
        # TIER 4 & 5: Inactive (0 discoveries) - split by scan freshness
        else:
            try:
                last_scan_dt = datetime.fromisoformat(last_scan)
                
                # TIER 4: Stale inactive (>7 days since scan - give them another chance)
                if last_scan_dt < week_ago:
                    tier4_refresh.append(handle)
                
                # TIER 5: Recently scanned inactive (<7 days - lowest priority)
                else:
                    tier5_low.append(handle)
            except:
                # If parsing fails, treat as never scanned
                tier1_never_scanned.append(handle)
    
    # Shuffle within each tier for fairness
    random.shuffle(tier1_never_scanned)
    random.shuffle(tier2_elite)
    random.shuffle(tier3_active)
    random.shuffle(tier4_refresh)
    random.shuffle(tier5_low)
    
    # Build final priority list (tier 1 first, tier 5 last)
    final_list = (
        tier1_never_scanned +
        tier2_elite +
        tier3_active +
        tier4_refresh +
        tier5_low
    )
    
    # Log tier distribution for monitoring
    print(f"\n📊 HVA Priority Distribution:")
    print(f"   🔥 TIER 1 (Never Scanned): {len(tier1_never_scanned)} HVAs")
    print(f"   ⭐ TIER 2 (Elite 5+): {len(tier2_elite)} HVAs")
    print(f"   ✅ TIER 3 (Active 1-4): {len(tier3_active)} HVAs")
    print(f"   🕐 TIER 4 (Refresh >7d): {len(tier4_refresh)} HVAs")
    print(f"   💤 TIER 5 (Recent <7d): {len(tier5_low)} HVAs")
    print(f"   📋 Total Active HVAs: {len(final_list)}\n")
    
    return final_list

def add_hva(handle):
    """Add a new HVA to tracking. Returns False if handle is on HVA_BLOCKLIST."""
    h = str(handle).replace("@", "").strip().lower()
    if not h or h in _hva_blocklist_lower():
        return False
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO hva_stats (hva_handle, status) VALUES (?, 'Active')
        ON CONFLICT(hva_handle) DO UPDATE SET status = 'Active'
    """, (h,))
    conn.commit()
    conn.close()
    return True

def remove_hva(handle):
    """Optionally set status to Dead or actually delete. Let's delete for this command."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM hva_stats WHERE hva_handle = ?", (handle.lower(),))
    conn.commit()
    conn.close()

def get_all_hvas():
    """Get all tracked HVAs including dead ones."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT hva_handle, status, discovery_count FROM hva_stats")
    res = cursor.fetchall()
    conn.close()
    return res

def update_hva_follows_count(hva_handle, count):
    """Store last known follows count for delta detection."""
    from datetime import datetime
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    # Prefer the current schema column `last_scan_at`. If a user has an older DB with `last_scan`,
    # fall back to that to avoid breaking.
    try:
        cursor.execute(
            """
            INSERT INTO hva_stats (hva_handle, last_follows_count, last_scan_at) VALUES (?, ?, ?)
            ON CONFLICT(hva_handle) DO UPDATE SET last_follows_count = ?, last_scan_at = ?
            """,
            (hva_handle.lower(), count, now, count, now),
        )
    except sqlite3.OperationalError:
        cursor.execute(
            """
            INSERT INTO hva_stats (hva_handle, last_follows_count, last_scan) VALUES (?, ?, ?)
            ON CONFLICT(hva_handle) DO UPDATE SET last_follows_count = ?, last_scan = ?
            """,
            (hva_handle.lower(), count, now, count, now),
        )
    conn.commit()
    conn.close()

def get_hva_last_follows_count(hva_handle):
    """Get last known follows count for delta detection."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT last_follows_count FROM hva_stats WHERE hva_handle = ?", (hva_handle.lower(),))
    res = cursor.fetchone()
    conn.close()
    return res[0] if res else 0

# ═══════════════════════════════════════════════════════════════
# SIGNAL TRACKING FUNCTIONS (New-Accs-Signal Feature)
# ═══════════════════════════════════════════════════════════════

def init_signal_tracking(project_id):
    """Initialize signal tracking for a new project."""
    from datetime import datetime
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR IGNORE INTO signal_alerts (project_id, first_discovered_at, last_signal_level)
        VALUES (?, ?, 'none')
    """, (project_id, datetime.now().isoformat()))
    conn.commit()
    conn.close()

def get_signal_data(project_id):
    """Get signal tracking data for a project."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT first_discovered_at, last_signal_level FROM signal_alerts WHERE project_id = ?
    """, (project_id,))
    res = cursor.fetchone()
    conn.close()
    return res  # (first_discovered_at, last_signal_level) or None

def update_signal_level(project_id, new_level):
    """Update signal level and alert timestamp."""
    from datetime import datetime
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE signal_alerts SET last_signal_level = ?, last_alert_at = ? WHERE project_id = ?
    """, (new_level, datetime.now().isoformat(), project_id))
    conn.commit()
    conn.close()

def get_follows_with_timestamps(project_id):
    """Get all HVA interactions with timestamps for signal calculation."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT hva_id, interaction_type, followed_at FROM follows WHERE project_id = ?
    """, (project_id,))
    res = cursor.fetchall()
    conn.close()
    return res  # List of (hva_id, interaction_type, timestamp)

# ═══════════════════════════════════════════════════════════════
# ANALYTICAL FUNCTIONS (New Commands)
# ═══════════════════════════════════════════════════════════════

def get_top_hvas_24h():
    """Returns top HVAs by unique discovery count in last 24h."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT hva_id, COUNT(DISTINCT project_id) as count 
        FROM follows 
        WHERE followed_at >= datetime('now', '-1 day')
        GROUP BY hva_id 
        ORDER BY count DESC 
        LIMIT 10
    """)
    res = cursor.fetchall()
    conn.close()
    return res


def get_trending_report_db_snapshot():
    """
    Operator/debug counts for the Discord trending report.
    Returns: (total_projects, alerted_any, alerted_discord_on)
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
          (SELECT COUNT(*) FROM projects),
          (SELECT COUNT(*) FROM projects WHERE alerted_at IS NOT NULL),
          (SELECT COUNT(*) FROM projects WHERE alerted_at IS NOT NULL
             AND COALESCE(alerted_discord, 0) = 1)
        """
    )
    row = cursor.fetchone() or (0, 0, 0)
    conn.close()
    return int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)


def get_trending_projects(hours=24, limit=20):
    """Returns top projects ranked by total smart followers (HVAs) in the last 30 days.
    Simple, easy-to-understand ranking of the most followed projects.
    Returns: (id, handle, name, desc, created_at, smarts_24h, smarts_7d, total_smarts, ai_sum, ai_cat)
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute(f"""
        WITH Smarts24h AS (
            SELECT f.project_id, COUNT(DISTINCT f.hva_id) as count
            FROM follows f
            WHERE f.followed_at >= datetime('now', '-24 hours')
            GROUP BY f.project_id
        ),
        Smarts7d AS (
            SELECT f.project_id, COUNT(DISTINCT f.hva_id) as count
            FROM follows f
            WHERE f.followed_at >= datetime('now', '-7 day')
            GROUP BY f.project_id
        ),
        TotalSmarts AS (
            SELECT f.project_id, COUNT(DISTINCT f.hva_id) as count
            FROM follows f
            WHERE f.followed_at >= datetime('now', '-30 day')
            GROUP BY f.project_id
        )
        SELECT 
            p.twitter_id, p.handle, p.name, p.description, p.created_at, 
            COALESCE(s24.count, 0) as s24h, 
            COALESCE(s7.count, 0) as s7d,
            COALESCE(ts.count, 0) as total,
            p.ai_summary, p.ai_category
        FROM projects p
        LEFT JOIN TotalSmarts ts ON p.twitter_id = ts.project_id
        LEFT JOIN Smarts7d s7 ON p.twitter_id = s7.project_id
        LEFT JOIN Smarts24h s24 ON p.twitter_id = s24.project_id
        WHERE p.alerted_at IS NOT NULL
          AND COALESCE(p.alerted_discord, 0) = 1
        ORDER BY COALESCE(ts.count, 0) DESC,
                 COALESCE(p.last_posted_smarts, 0) DESC,
                 datetime(p.alerted_at) DESC
        LIMIT ?
    """, (limit,))
    
    res = cursor.fetchall()
    conn.close()
    return res


def get_projects_top_smarts_24h(limit=10):
    """
    Top alerted projects by distinct HVA follows in the last 24 hours (Velcor3 discovery DB).
    Returns: (twitter_id, handle, name, description, created_at, smarts_24h, ai_summary, ai_category)
    """
    limit = max(1, min(50, int(limit)))
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        WITH Smarts24h AS (
            SELECT f.project_id, COUNT(DISTINCT f.hva_id) AS count
            FROM follows f
            WHERE f.followed_at >= datetime('now', '-24 hours')
            GROUP BY f.project_id
        )
        SELECT
            p.twitter_id, p.handle, p.name, p.description, p.created_at,
            COALESCE(s24.count, 0) AS s24h,
            p.ai_summary, p.ai_category
        FROM projects p
        INNER JOIN Smarts24h s24 ON p.twitter_id = s24.project_id
        WHERE p.alerted_at IS NOT NULL
        ORDER BY s24.count DESC
        LIMIT ?
        """,
        (limit,),
    )
    res = cursor.fetchall()
    conn.close()
    return res


def _parse_sqlite_ts(val) -> Optional[datetime]:
    if val is None:
        return None
    s = str(val).strip().replace("Z", "+00:00")
    if not s:
        return None
    try:
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            return dt
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def get_projects_finds_24h(limit=200):
    """
    All projects we alerted (Discord discovery) in the rolling **last 24 hours** (UTC), newest first.
    Returns: (twitter_id, handle, name, description, created_at, alerted_at, ai_category, ai_summary, followers_count)
    """
    limit = max(1, min(500, int(limit)))
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            p.twitter_id, p.handle, p.name, p.description, p.created_at,
            p.alerted_at, p.ai_category, p.ai_summary, p.followers_count
        FROM projects p
        WHERE p.alerted_at IS NOT NULL
          AND COALESCE(p.alerted_discord, 0) = 1
          AND datetime(p.alerted_at) >= datetime('now', '-24 hours')
        ORDER BY p.alerted_at DESC
        LIMIT ?
        """,
        (limit * 2,),
    )
    raw = cursor.fetchall()
    conn.close()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    out: list = []
    for row in raw:
        at = _parse_sqlite_ts(row[5])
        if at is None or at < cutoff:
            continue
        out.append(row)
        if len(out) >= limit:
            break
    return out


def get_projects_alerted_between_utc(day_start_iso: str, day_end_iso: str, limit: int = 300):
    """
    Projects alerted (Discord discovery) between [day_start_iso, day_end_iso) in UTC.
    Returns: (twitter_id, handle, name, description, created_at, alerted_at, ai_category, ai_summary, followers_count)
    """
    limit = max(1, min(500, int(limit or 300)))
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            p.twitter_id, p.handle, p.name, p.description, p.created_at,
            p.alerted_at, p.ai_category, p.ai_summary, p.followers_count
        FROM projects p
        WHERE p.alerted_at IS NOT NULL
          AND COALESCE(p.alerted_discord, 0) = 1
          AND datetime(p.alerted_at) >= datetime(?)
          AND datetime(p.alerted_at) < datetime(?)
        ORDER BY p.alerted_at DESC
        LIMIT ?
        """,
        (str(day_start_iso), str(day_end_iso), limit * 2),
    )
    raw = cursor.fetchall()
    conn.close()
    # Final filter using robust parser (handles mixed timestamp formats).
    start_dt = _parse_sqlite_ts(day_start_iso)
    end_dt = _parse_sqlite_ts(day_end_iso)
    out: list = []
    for row in raw:
        at = _parse_sqlite_ts(row[5])
        if at is None:
            continue
        if start_dt and at < start_dt:
            continue
        if end_dt and at >= end_dt:
            continue
        out.append(row)
        if len(out) >= limit:
            break
    return out


def get_projects_alerted_since_utc(since_iso: str, limit: int = 200):
    """
    Projects alerted (Discord discovery) since datetime(since_iso) in UTC (strictly after),
    newest first.
    """
    limit = max(1, min(500, int(limit or 200)))
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            p.twitter_id, p.handle, p.name, p.description, p.created_at,
            p.alerted_at, p.ai_category, p.ai_summary, p.followers_count
        FROM projects p
        WHERE p.alerted_at IS NOT NULL
          AND COALESCE(p.alerted_discord, 0) = 1
          AND datetime(p.alerted_at) > datetime(?)
        ORDER BY p.alerted_at DESC
        LIMIT ?
        """,
        (str(since_iso), limit * 3),
    )
    raw = cursor.fetchall()
    conn.close()
    since_dt = _parse_sqlite_ts(since_iso)
    out: list = []
    for row in raw:
        at = _parse_sqlite_ts(row[5])
        if at is None:
            continue
        if since_dt and at <= since_dt:
            continue
        out.append(row)
        if len(out) >= limit:
            break
    return out


def get_project_by_handle(handle: str):
    """
    Project row by handle (case-insensitive).
    Returns: (twitter_id, handle, name, description, created_at, alerted_at, ai_category, ai_summary, followers_count, last_posted_smarts)
    """
    h = str(handle or "").strip().lstrip("@").lower()
    if not h:
        return None
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            p.twitter_id, p.handle, p.name, p.description, p.created_at,
            p.alerted_at, p.ai_category, p.ai_summary, p.followers_count, p.last_posted_smarts
        FROM projects p
        WHERE lower(p.handle) = ?
        LIMIT 1
        """,
        (h,),
    )
    row = cursor.fetchone()
    conn.close()
    return row


def get_project_smart_followers(project_id: str, limit: int = 80) -> list[str]:
    """
    Distinct HVA handles that interacted with a project, most recent first.
    """
    pid = str(project_id or "").strip()
    if not pid:
        return []
    lim = max(1, min(300, int(limit or 80)))
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT f.hva_id, MAX(f.followed_at) AS last_ts
        FROM follows f
        WHERE f.project_id = ?
        GROUP BY f.hva_id
        ORDER BY datetime(last_ts) DESC
        LIMIT ?
        """,
        (pid, lim),
    )
    rows = cursor.fetchall() or []
    conn.close()
    out: list[str] = []
    for r in rows:
        if not r:
            continue
        h = str(r[0] or "").strip()
        if h:
            out.append(h)
    return out


def get_trending_projects_24h(limit=5):
    """
    For ENABLE_X_POST_DAILY tweet: (id, handle, name, description, created_at, smarts_24h).
    """
    rows = get_projects_top_smarts_24h(limit)
    return [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows]


def get_hva_engagements_24h(hva_id):
    """List all unique projects an HVA interacted with in the last 24h."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT p.handle, f.interaction_type, f.followed_at
        FROM projects p
        JOIN follows f ON p.twitter_id = f.project_id
        WHERE f.hva_id = ? AND f.followed_at >= datetime('now', '-1 day')
        ORDER BY f.followed_at DESC
    """, (hva_id.lower(),))
    res = cursor.fetchall()
    conn.close()
    return res

def get_hva_global_stats(hva_id):
    """Get total discoveries and rank for an HVA."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Total discoveries
    cursor.execute("SELECT discovery_count FROM hva_stats WHERE hva_handle = ?", (hva_id.lower(),))
    count_row = cursor.fetchone()
    total = count_row[0] if count_row else 0
    
    # Rank
    cursor.execute("SELECT COUNT(*) + 1 FROM hva_stats WHERE discovery_count > ?", (total,))
    rank = cursor.fetchone()[0]
    
    conn.close()
    return total, rank

def get_db_stats():
    """Get global database stats including AI category breakdown."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM projects")
    total_projects = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM follows")
    total_follows = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT hva_id) FROM follows")
    active_hvas = cursor.fetchone()[0]
    
    # Category breakdown (Only for alerted projects)
    cursor.execute("""
        SELECT ai_category, COUNT(*) as count 
        FROM projects 
        WHERE ai_category IS NOT NULL AND alerted_at IS NOT NULL
        GROUP BY ai_category 
        ORDER BY count DESC
    """)
    categories = cursor.fetchall()
    
    conn.close()
    return total_projects, total_follows, active_hvas, categories

def get_hva_recent_projects(hva_id, limit=15):
    """List most recent projects an HVA interacted with (unrestricted by time)."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT p.twitter_id, p.handle, f.interaction_type, f.followed_at
        FROM projects p
        JOIN follows f ON p.twitter_id = f.project_id
        WHERE f.hva_id = ?
        ORDER BY f.followed_at DESC
        LIMIT ?
    """, (hva_id.lower(), limit))
    res = cursor.fetchall()
    conn.close()
    return res

def get_hva_health_report():
    """Returns a categorized report of all HVAs."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Active: Interaction in last 48h
    cursor.execute("""
        SELECT COUNT(DISTINCT hva_id) FROM follows 
        WHERE followed_at >= datetime('now', '-2 day')
    """)
    active_count = cursor.fetchone()[0]
    
    # Semi: Interaction in last 7 days
    cursor.execute("""
        SELECT COUNT(DISTINCT hva_id) FROM follows 
        WHERE followed_at >= datetime('now', '-7 day')
    """)
    semi_count = cursor.fetchone()[0]
    
    # Dead: Status set to 'Dead'
    cursor.execute("SELECT COUNT(*) FROM hva_stats WHERE status = 'Dead'")
    dead_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT hva_handle FROM hva_stats WHERE status = 'Dead'")
    dead_list = [row[0] for row in cursor.fetchall()]
    
    # Quality: Top 5 by quality score
    cursor.execute("""
        SELECT hva_handle, quality_score, discovery_count 
        FROM hva_stats 
        WHERE quality_score > 0 
        ORDER BY quality_score DESC, discovery_count DESC 
        LIMIT 5
    """)
    quality_top = cursor.fetchall()
    
    conn.close()
    return {
        'active': active_count,
        'semi': semi_count,
        'dead_count': dead_count,
        'dead_list': dead_list,
        'quality_top': quality_top
    }

def update_hva_quality_scores():
    """Recalculate HVA quality scores based on the performance of their discoveries."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all HVAs
    cursor.execute("SELECT hva_handle FROM hva_stats")
    hvas = [r[0] for r in cursor.fetchall()]
    
    for hva in hvas:
        # Calculate score based on:
        # 1. Avg AI Alpha Score of projects they followed
        # 2. Bonus for projects that reached High Smarts (5+)
        cursor.execute("""
            SELECT AVG(p.ai_alpha_score), COUNT(p.twitter_id)
            FROM projects p
            JOIN follows f ON p.twitter_id = f.project_id
            WHERE f.hva_id = ? AND p.ai_alpha_score > 0
        """, (hva.lower(),))
        avg_score, count = cursor.fetchone()
        
        if not avg_score: avg_score = 0
        
        # Bonus for "Winners" (Projects found by this HVA that now have 5+ hunters)
        cursor.execute("""
            SELECT COUNT(*) FROM projects p
            JOIN follows f1 ON p.twitter_id = f1.project_id
            WHERE f1.hva_id = ? AND p.last_posted_smarts >= 5
        """, (hva.lower(),))
        winners = cursor.fetchone()[0]
        
        # Final Quality Score Formula
        final_score = (avg_score * 0.7) + (winners * 10)
        
        cursor.execute("UPDATE hva_stats SET quality_score = ? WHERE hva_handle = ?", (round(final_score, 2), hva.lower()))
    
    conn.commit()
    conn.close()

def update_hva_scan_timestamp(hva_handle):
    """Update the last scan timestamp for an HVA after each scan attempt."""
    from datetime import datetime
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO hva_stats (hva_handle, last_scan_at) VALUES (?, ?)
        ON CONFLICT(hva_handle) DO UPDATE SET last_scan_at = ?
    """, (hva_handle.lower(), datetime.now().isoformat(), datetime.now().isoformat()))
    conn.commit()
    conn.close()

def get_inactive_hva_analysis():
    """Analyze why HVAs are inactive - never scanned vs scanned but found nothing.
    Returns dict with categorized lists and stats.
    """
    from datetime import datetime, timedelta
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all HVAs with 0 discoveries
    cursor.execute("""
        SELECT hva_handle, last_scan_at, discovery_count, status 
        FROM hva_stats 
        WHERE discovery_count = 0
    """)
    
    inactive_hvas = cursor.fetchall()
    
    never_scanned = []  # HVAs with last_scan_at = NULL
    scanned_no_results = []  # HVAs scanned but found nothing
    recently_scanned = []  # Scanned in last 7 days (still fresh, maybe just unlucky)
    stale_scanned = []  # Scanned 3+ times but no results
    
    week_ago = datetime.now() - timedelta(days=7)
    
    for handle, last_scan, disc_count, status in inactive_hvas:
        if last_scan is None:
            never_scanned.append(handle)
        else:
            try:
                last_scan_dt = datetime.fromisoformat(last_scan)
                
                # Check if scanned recently (within last 7 days)
                if last_scan_dt >= week_ago:
                    recently_scanned.append((handle, last_scan))
                else:
                    # Check how many times they've been scanned (estimate based on age)
                    days_since_scan = (datetime.now() - last_scan_dt).days
                    estimated_scans = max(1, days_since_scan // 7)  # Rough estimate
                    
                    if estimated_scans >= 3:
                        stale_scanned.append((handle, last_scan, days_since_scan))
                    else:
                        scanned_no_results.append((handle, last_scan))
            except:
                never_scanned.append(handle)
    
    # Get total HVAs for percentage calculation
    cursor.execute("SELECT COUNT(*) FROM hva_stats")
    total_hvas = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        'total_inactive': len(inactive_hvas),
        'total_hvas': total_hvas,
        'never_scanned': never_scanned,
        'recently_scanned': recently_scanned,  # May still find projects
        'stale_scanned': stale_scanned,  # Scanned 3+ times, found nothing - safe to remove
        'scanned_no_results': scanned_no_results
    }

if __name__ == "__main__":
    init_db()
    print("Database initialized.")
