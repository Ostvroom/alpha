"""
ETH mints overview — 1h leaderboard (ranked by on-chain mint velocity).
Data: mintscan proxy overview + per-collection detail for X / mint / marketplace links.
"""
from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from discord import Client, Color, Embed

PROXY_BASE = "https://mintscan-proxy.mike-d4a.workers.dev"
OVERVIEW_URL = f"{PROXY_BASE}/api/overview"
COLLECTION_URL = f"{PROXY_BASE}/api/collection"

_last_error: Optional[str] = None

HEAT_EMOJI = {
    "very_hot": "🔥🔥",
    "hot":      "🔥",
    "warm":     "🌡️",
    "none":     "",
}


def get_last_overview_error() -> Optional[str]:
    return _last_error


async def fetch_collection_detail(
    session: aiohttp.ClientSession, address: str
) -> Optional[Dict[str, Any]]:
    if not address:
        return None
    try:
        url = f"{COLLECTION_URL}/{address}"
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=12),
            headers={"Accept": "application/json"},
        ) as r:
            if r.status != 200:
                return None
            return await r.json()
    except Exception:
        return None


def _normalize_x_url(twitter: str) -> Optional[str]:
    """Return https X URL or None."""
    t = (twitter or "").strip()
    if not t:
        return None
    if t.startswith("http://") or t.startswith("https://"):
        return t.replace("twitter.com/", "x.com/").replace("http://", "https://")
    if t.startswith("@"):
        h = t.lstrip("@").split("/")[0]
        if h:
            return f"https://x.com/{h}"
    if re.match(r"^[A-Za-z0-9_]{1,30}$", t):
        return f"https://x.com/{t}"
    return None


def best_external_link(detail: Optional[Dict[str, Any]], address: str) -> str:
    """
    Prefer X, then project website, then OpenSea / Blur (mint entry points).
    Fallback: OpenSea contract page.
    """
    addr = (address or "").strip()
    if not addr:
        return "https://opensea.io"

    if detail:
        xu = _normalize_x_url(detail.get("twitter") or "")
        if xu:
            return xu

        web = (detail.get("website") or "").strip()
        if web.startswith("http"):
            return web

        for key in ("opensea_url", "blur_url"):
            u = detail.get(key)
            if u and isinstance(u, str) and u.startswith("http"):
                return u

    return f"https://opensea.io/assets/ethereum/{addr}"


async def enrich_collection_links(
    session: aiohttp.ClientSession,
    cols: List[Dict[str, Any]],
    *,
    concurrency: int = 4,
) -> None:
    """Attach link_url (X, mint site, or marketplace) to each col dict."""
    sem = asyncio.Semaphore(concurrency)

    async def one(col: Dict[str, Any]) -> None:
        addr = col.get("address", "")
        async with sem:
            detail = await fetch_collection_detail(session, addr)
        col["link_url"] = best_external_link(detail, addr)

    await asyncio.gather(*[one(c) for c in cols])


async def fetch_overview(session: aiohttp.ClientSession) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    global _last_error
    _last_error = None
    try:
        async with session.get(
            OVERVIEW_URL,
            timeout=aiohttp.ClientTimeout(total=15),
            headers={"Accept": "application/json"},
        ) as r:
            if r.status != 200:
                _last_error = f"Mints overview HTTP {r.status}"
                return [], _last_error
            data = await r.json()
    except Exception as e:
        _last_error = str(e)
        return [], _last_error

    collections: List[Dict[str, Any]] = data.get("collections", [])
    # Filter airdrops; sort by recent_mints desc
    filtered = [c for c in collections if not c.get("is_airdrop", False)]
    filtered.sort(key=lambda c: c.get("recent_mints", 0), reverse=True)
    return filtered, None


def _heat_bar(recent: int, top: int) -> str:
    """5-block activity bar scaled to the top collection in this snapshot."""
    if top == 0:
        return "░░░░░"
    ratio = min(recent / top, 1.0)
    filled = round(ratio * 5)
    return "█" * filled + "░" * (5 - filled)


def _tag(col: Dict[str, Any]) -> str:
    tags = []
    if col.get("is_mintable"):
        tags.append("`MINT`")
    if col.get("verified"):
        tags.append("`✓`")
    h = col.get("heat", "none")
    if h in ("hot", "very_hot"):
        tags.append(HEAT_EMOJI.get(h, ""))
    return " ".join(tags)


def build_overview_embed(
    cols: List[Dict[str, Any]],
    *,
    brand_name: str = "Velcor3",
    embed_color: int = 0x202025,
) -> Embed:
    """Build embed from a pre-sorted, pre-trimmed list (with optional link_url per row)."""
    now = datetime.now(timezone.utc)
    embed = Embed(color=Color(embed_color), timestamp=now)
    embed.set_author(name=f"⚡ {brand_name}  ·  ETH Mints  ·  1h heat")

    if not cols:
        embed.description = "_No active mints in this window — check back soon._"
        embed.set_footer(text=f"{brand_name}  ·  Mint overview  ·  DYOR")
        return embed

    top_count = cols[0].get("recent_mints", 1) or 1

    # Thumbnail = top collection logo
    for c in cols:
        img = c.get("image_url")
        if img and isinstance(img, str) and img.startswith("http") and not img.endswith(".svg"):
            embed.set_thumbnail(url=img)
            break

    rows: List[str] = []
    for rank, col in enumerate(cols, 1):
        name = col.get("name") or "Unknown"
        addr = (col.get("address") or "").strip()
        recent = col.get("recent_mints", 0)
        total = col.get("total_mints", 0)
        bar = _heat_bar(recent, top_count)
        tag = _tag(col)
        href = (col.get("link_url") or "").strip()
        if not href and addr:
            href = best_external_link(None, addr)
        link = f"[{name}]({href})" if href else name
        rank_icon = ("🥇", "🥈", "🥉")[rank - 1] if rank <= 3 else f"`{rank}.`"
        tag_str = f"  {tag}" if tag else ""
        rows.append(
            f"{rank_icon} **{link}**{tag_str}\n"
            f"   `{bar}`  **{recent}** mints (1h)  ·  {total:,} total"
        )

    embed.description = "\n\n".join(rows)

    total_mints_1h = sum(c.get("recent_mints", 0) for c in cols)
    embed.set_footer(
        text=f"{brand_name}  ·  {total_mints_1h} mints (1h)  ·  {len(cols)} collections  ·  Not financial advice"
    )
    return embed


async def run_overview_once(
    client: Client,
    session: aiohttp.ClientSession,
    channel_id: int,
    *,
    brand_name: str = "Velcor3",
    embed_color: int = 0x202025,
    top_n: int = 12,
) -> Tuple[bool, Optional[str]]:
    """
    Fetch & post a new mints overview message in channel_id (each run = new post for pings / history).
    Returns (success, error_or_None).
    """
    collections, err = await fetch_overview(session)
    if err and not collections:
        return False, err

    cols = [c for c in collections if c.get("recent_mints", 0) > 0][:top_n]
    if cols:
        await enrich_collection_links(session, cols)

    embed = build_overview_embed(
        cols,
        brand_name=brand_name,
        embed_color=embed_color,
    )

    try:
        ch = client.get_channel(channel_id) or await client.fetch_channel(channel_id)
    except Exception as e:
        return False, f"Channel {channel_id} not accessible: {e}"
    if not ch:
        return False, f"Channel {channel_id} not found"

    await ch.send(embed=embed)
    return True, None
