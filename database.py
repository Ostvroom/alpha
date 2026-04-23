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

def get_trending_projects(hours=24, limit=30):
    """Get trending projects (only alerted ones) for Trending report."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT p.twitter_id, p.handle, p.name, p.description, p.created_at, p.last_posted_smarts
        FROM projects p
        WHERE p.alerted_at IS NOT NULL AND COALESCE(p.alerted_discord, 0) = 1
        AND p.first_seen_at >= datetime('now', ? || ' hours')
        ORDER BY p.last_posted_smarts DESC
        LIMIT ?
    """, (-hours, limit))
    res = cursor.fetchall()
    conn.close()
    return res


def get_trending_projects_30d(limit: int = 30):
    """
    Trending projects by DISTINCT HVA count in the last 30 days.
    Returns rows: (twitter_id, handle, name, description, created_at, hva_30d)
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
            COALESCE(s30.count, 0) AS hva_30d
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
    cursor.execute("""
        SELECT hva_handle, discovery_count, last_scan_at, status 
        FROM hva_stats 
        WHERE status != 'Dead'
    """)
    
    data = cursor.fetchall()
    conn.close()
    
    if not data:
        # Fallback to config list if DB is empty
        import config
        return config.HVA_LIST
    
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
    """Add a new HVA to tracking."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO hva_stats (hva_handle, status) VALUES (?, 'Active')
        ON CONFLICT(hva_handle) DO UPDATE SET status = 'Active'
    """, (handle.lower(),))
    conn.commit()
    conn.close()

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
        JOIN TotalSmarts ts ON p.twitter_id = ts.project_id
        LEFT JOIN Smarts7d s7 ON p.twitter_id = s7.project_id
        LEFT JOIN Smarts24h s24 ON p.twitter_id = s24.project_id
        WHERE p.alerted_at IS NOT NULL
        ORDER BY ts.count DESC
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
