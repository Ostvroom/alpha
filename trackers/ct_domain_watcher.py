"""
Certificate Transparency (CT) Domain Watcher.

Polls crt.sh for newly issued TLS certificates whose domains match
crypto / NFT / infra / gaming keyword patterns.

For each new domain we:
  1. Check it is genuinely new (SQLite dedup in ct_seen_domains table).
  2. Attempt to scrape the homepage and find an X / Twitter handle.
  3. Return a structured candidate list to the Discord task for alerting.

The discord_bot.py task (ct_domain_watcher_task) handles:
  - Posting the "New Domain Detected" embed to the CT channel.
  - Optionally routing to process_discovery() when an X handle is found.
"""

import asyncio
import re
import sqlite3
import os
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Tuple

import aiohttp

# ── paths ─────────────────────────────────────────────────────────────────────
from app_paths import DATA_DIR, ensure_dirs

ensure_dirs()
DB_PATH = os.path.join(DATA_DIR, "block_brain.db")

# ── keyword / TLD lists ────────────────────────────────────────────────────────
CRYPTO_KEYWORDS: List[str] = [
    "defi", "swap", "dex", "dao", "nft", "mint", "stake", "vault", "yield",
    "airdrop", "token", "protocol", "chain", "bridge", "rollup", "zk",
    "layer", "node", "rpc", "agent", "compute", "gaming", "metaverse",
    "launchpad", "presale", "testnet", "mainnet", "liquidity", "meme",
    "punk", "ape", "pepe", "doge", "shib", "wagmi", "pump", "launch",
    "memecoin", "staking", "restake", "eigenl", "avs", "infra",
]

TARGET_TLDS: List[str] = [
    ".xyz", ".fun", ".io", ".fi", ".wtf", ".lol", ".app", ".gg", ".pro",
]

CRT_API_URL = "https://crt.sh/?q={query}&output=json"

# Known non-account paths to skip when extracting X handles from HTML
_X_SKIP_HANDLES = frozenset({
    "home", "explore", "notifications", "messages", "i", "search",
    "intent", "share", "hashtag", "login", "signup", "settings",
    "compose", "tos", "privacy", "about",
})

_X_HANDLE_RE = re.compile(
    r'(?:https?://)?(?:www\.)?(?:x\.com|twitter\.com)/([A-Za-z0-9_]{1,15})'
    r'(?:[/?#"\'\s]|$)'
)


# ── SQLite helpers ─────────────────────────────────────────────────────────────

def init_ct_db() -> None:
    """Create the ct_seen_domains table if it doesn't exist."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS ct_seen_domains (
            domain        TEXT PRIMARY KEY,
            cert_id       INTEGER,
            first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            x_handle      TEXT,
            alerted       INTEGER DEFAULT 0
        )
        """
    )
    conn.commit()
    conn.close()


def is_domain_new(domain: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT 1 FROM ct_seen_domains WHERE domain = ?", (domain.lower(),))
    res = c.fetchone()
    conn.close()
    return res is None


def mark_domain_seen(
    domain: str,
    cert_id: int,
    x_handle: Optional[str] = None,
) -> None:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT OR IGNORE INTO ct_seen_domains (domain, cert_id, x_handle) VALUES (?, ?, ?)",
        (domain.lower(), cert_id, x_handle),
    )
    conn.commit()
    conn.close()


# ── X handle extraction from homepage ────────────────────────────────────────

async def extract_x_handle_from_site(
    session: aiohttp.ClientSession,
    domain: str,
) -> Optional[str]:
    """
    Fetch the project homepage (https then http fallback) and return the
    first x.com/<handle> link that looks like a real account.
    Returns None if the site is unreachable or no handle is found.
    """
    for scheme in ("https", "http"):
        url = f"{scheme}://{domain}"
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=8),
                allow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0 (compatible; CryptoRadar/1.0)"},
                ssl=False,
            ) as resp:
                if resp.status != 200:
                    continue
                html = await resp.text(errors="ignore")
                for m in _X_HANDLE_RE.findall(html):
                    handle = m.strip()
                    if handle.lower() not in _X_SKIP_HANDLES and 2 <= len(handle) <= 15:
                        return handle
        except Exception:
            pass
    return None


# ── crt.sh polling ─────────────────────────────────────────────────────────────

def _domain_has_crypto_keyword(domain: str) -> bool:
    d = domain.lower()
    return any(kw in d for kw in CRYPTO_KEYWORDS)


def _domain_has_target_tld(domain: str) -> bool:
    d = domain.lower()
    return any(d.endswith(tld) for tld in TARGET_TLDS)


async def _fetch_crt(
    session: aiohttp.ClientSession,
    query: str,
) -> List[dict]:
    """Fetch CT log entries from crt.sh matching the given SQL LIKE query."""
    url = CRT_API_URL.format(query=query)
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=20),
            headers={"User-Agent": "Mozilla/5.0"},
        ) as resp:
            if resp.status != 200:
                return []
            return await resp.json(content_type=None) or []
    except Exception:
        return []


async def poll_new_domains(
    lookback_minutes: int = 45,
    max_new: int = 10,
) -> List[Tuple[str, int, Optional[str]]]:
    """
    Poll crt.sh for fresh TLS certs (issued within `lookback_minutes`).

    Filters entries by:
      - entry_timestamp >= now - lookback_minutes
      - domain has a TARGET_TLD
      - domain contains at least one CRYPTO_KEYWORD
      - domain not already seen (ct_seen_domains table)

    For each passing domain, attempts to extract an X handle from the
    site homepage.

    Returns list of (domain, cert_id, x_handle_or_None), capped at max_new.
    """
    init_ct_db()

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
    candidates: List[Tuple[str, int, Optional[str]]] = []
    seen_this_cycle: set = set()

    # Query crt.sh by TLD; keyword filtering is done client-side to
    # keep the total number of API round-trips small.
    queries = ["%.xyz", "%.fun", "%.io", "%.fi", "%.wtf", "%.lol", "%.gg", "%.app"]

    async with aiohttp.ClientSession() as http:
        for query in queries:
            if len(candidates) >= max_new:
                break

            await asyncio.sleep(2.0)  # respectful rate-limit for crt.sh

            entries = await _fetch_crt(http, query)
            if not entries:
                continue

            for entry in entries:
                if len(candidates) >= max_new:
                    break

                cert_id = int(entry.get("id") or 0)
                name_value = (entry.get("name_value") or "").strip().lower()
                ts_str = entry.get("entry_timestamp") or ""

                # Parse crt.sh timestamp
                try:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                except Exception:
                    continue

                if ts < cutoff:
                    # crt.sh returns newest first; stop processing this query
                    # once we're past the lookback window.
                    break

                # The name_value field can have multiple SANs separated by '\n'
                for raw in name_value.splitlines():
                    domain = raw.strip().lstrip("*.")
                    if not domain or domain in seen_this_cycle:
                        continue
                    if not _domain_has_target_tld(domain):
                        continue
                    if not _domain_has_crypto_keyword(domain):
                        continue
                    if not is_domain_new(domain):
                        continue

                    seen_this_cycle.add(domain)

                    # Best-effort X handle extraction from homepage
                    x_handle = await extract_x_handle_from_site(http, domain)
                    mark_domain_seen(domain, cert_id, x_handle)
                    candidates.append((domain, cert_id, x_handle))
                    break  # one domain per cert entry

    return candidates
