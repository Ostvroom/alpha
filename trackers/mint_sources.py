"""
Active Mints - Data layer (Waypoint MintScan API).
Fetches trending/active NFT mints and normalizes to ActiveMint.
"""
import os
from dataclasses import dataclass, field
from typing import Optional, List
import asyncio
import aiohttp
from dotenv import load_dotenv

load_dotenv(override=True)

WAYPOINT_BASE = "https://mintscan-proxy.mike-d4a.workers.dev"

# Last fetch error (for /mints_status)
_last_fetch_error: Optional[str] = None


@dataclass
class ActiveMint:
    """Normalized active mint for Discord embed."""
    name: str
    symbol: str
    chain: str
    contract: str
    collection_id: str
    total_supply: int
    minted_count: int
    unique_minters: Optional[int] = None
    mint_price: Optional[str] = None
    image_url: Optional[str] = None
    # Marketplace / explorer links
    etherscan_url: Optional[str] = None
    opensea_url: Optional[str] = None
    blur_url: Optional[str] = None
    nftscan_url: Optional[str] = None
    magiceden_url: Optional[str] = None
    tensor_url: Optional[str] = None
    solscan_url: Optional[str] = None
    website_url: Optional[str] = None
    waypoint_url: Optional[str] = None
    # Socials
    twitter_url: Optional[str] = None
    discord_url: Optional[str] = None
    # Velocity
    recent_mints_count: Optional[int] = None
    mints_3m: Optional[int] = None
    mints_10m: Optional[int] = None
    mints_1h: Optional[int] = None
    # Heat level from Waypoint (none, warm, hot, very_hot)
    heat: Optional[str] = None
    # High-value traders active on this mint
    tracked_minters: int = 0
    # Is it mintable?
    is_mintable: bool = False
    is_airdrop: bool = False
    # Verified on OpenSea
    verified: bool = False


async def fetch_waypoint_overview(
    session: aiohttp.ClientSession,
    limit: int = 10,
) -> List[dict]:
    """
    Fetch trending mints overview from Waypoint MintScan API.
    Returns raw collection dicts sorted by recent_mints (descending).
    """
    global _last_fetch_error
    try:
        url = f"{WAYPOINT_BASE}/api/overview"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                _last_fetch_error = f"Waypoint overview returned {resp.status}"
                return []
            data = await resp.json()
    except Exception as e:
        _last_fetch_error = f"Waypoint overview error: {e}"
        print(f"[ActiveMints] Waypoint overview error: {e}")
        return []

    collections = data.get("collections", [])
    
    # Filter out airdrops and sort by recent mints
    filtered = [c for c in collections if not c.get("is_airdrop", False)]
    filtered.sort(key=lambda c: c.get("recent_mints", 0), reverse=True)
    
    return filtered[:limit]


async def fetch_waypoint_collection(
    session: aiohttp.ClientSession,
    address: str,
) -> Optional[dict]:
    """
    Fetch detailed collection data from Waypoint MintScan API.
    """
    try:
        url = f"{WAYPOINT_BASE}/api/collection/{address}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception as e:
        print(f"[ActiveMints] Waypoint collection detail error for {address}: {e}")
        return None


def _build_mint_from_waypoint(overview: dict, detail: Optional[dict] = None) -> ActiveMint:
    """
    Build an ActiveMint from Waypoint overview data + optional detail enrichment.
    """
    address = overview.get("address", "")
    name = overview.get("name") or overview.get("full_name") or "Unknown"
    full_name = overview.get("full_name", name)
    
    # Extract symbol from full_name pattern "Name (SYMBOL)"
    symbol = "???"
    if "(" in full_name and full_name.endswith(")"):
        symbol = full_name.rsplit("(", 1)[-1].rstrip(")")
    
    image_url = overview.get("image_url")
    heat = overview.get("heat", "none")
    recent_mints = overview.get("recent_mints", 0)
    is_airdrop = overview.get("is_airdrop", False)
    is_mintable = overview.get("is_mintable", False)
    verified = overview.get("verified", False)
    
    # Defaults from overview
    current_supply = overview.get("total_mints", 0)
    max_supply = None
    unique_minters = None
    mint_price = None
    twitter = None
    discord_url = None
    website = None
    etherscan_url = f"https://etherscan.io/address/{address}"
    opensea_url = f"https://opensea.io/assets/ethereum/{address}"
    blur_url = None
    mints_3m = None
    mints_10m = None
    mints_1h = None
    
    # Enrich from detail endpoint if available
    if detail:
        current_supply = detail.get("current_supply", current_supply)
        max_supply = detail.get("max_supply")
        unique_minters = detail.get("unique_minters")
        mint_price = detail.get("mint_price", "Unknown")
        twitter = detail.get("twitter")
        discord_url = detail.get("discord_url")
        website = detail.get("website")
        image_url = detail.get("image_url") or image_url
        etherscan_url = detail.get("etherscan_url") or etherscan_url
        opensea_url = detail.get("opensea_url") or opensea_url
        blur_url = detail.get("blur_url")
        is_airdrop = detail.get("is_airdrop", is_airdrop)
        verified = detail.get("verified", verified)
        mints_3m = detail.get("mints_3m")
        mints_10m = detail.get("mints_10m")
        mints_1h = detail.get("mints_1h")
        
        # Extract symbol from detail if available
        if detail.get("symbol"):
            symbol = detail["symbol"]
        elif detail.get("name") and "(" in detail["name"] and detail["name"].endswith(")"):
            symbol = detail["name"].rsplit("(", 1)[-1].rstrip(")")
    
    total_supply = max_supply if max_supply and max_supply > 0 else current_supply
    minted_count = current_supply
    
    waypoint_url = f"https://waypoint.tools/mintscan/#{address}"
    
    return ActiveMint(
        name=name,
        symbol=symbol,
        chain="ethereum",
        contract=address,
        collection_id=address,
        total_supply=total_supply,
        minted_count=minted_count,
        unique_minters=unique_minters,
        mint_price=mint_price,
        image_url=image_url,
        etherscan_url=etherscan_url,
        opensea_url=opensea_url,
        blur_url=blur_url,
        waypoint_url=waypoint_url,
        twitter_url=twitter,
        discord_url=discord_url,
        website_url=website,
        recent_mints_count=recent_mints,
        mints_3m=mints_3m,
        mints_10m=mints_10m,
        mints_1h=mints_1h,
        heat=heat,
        tracked_minters=0,
        is_mintable=is_mintable,
        is_airdrop=is_airdrop,
        verified=verified,
    )


async def fetch_waypoint_mints(
    session: aiohttp.ClientSession,
    limit: int = 10,
    enrich: bool = True,
) -> List[ActiveMint]:
    """
    Fetch trending mints from Waypoint and optionally enrich with detailed collection data.
    """
    global _last_fetch_error
    
    overview_list = await fetch_waypoint_overview(session, limit=limit * 2)
    if not overview_list:
        return []
    
    results: List[ActiveMint] = []
    
    for col in overview_list[:limit]:
        address = col.get("address", "")
        if not address:
            continue
        
        detail = None
        if enrich:
            detail = await fetch_waypoint_collection(session, address)
            # Small delay between API calls
            await asyncio.sleep(0.3)
        
        try:
            mint = _build_mint_from_waypoint(col, detail)
            results.append(mint)
        except Exception as e:
            print(f"[ActiveMints] Skip Waypoint collection {address}: {e}")
            continue
    
    return results


async def fetch_active_mints(
    session: aiohttp.ClientSession,
    limit: int = 5,
    period: str = "24h",
    include_eth: bool = True,
    include_solana: bool = False,
) -> List[ActiveMint]:
    """
    Fetch active mints. Uses:
    1. ETH in-memory live scraper (if available)
    2. Waypoint MintScan API as fallback
    """
    global _last_fetch_error
    _last_fetch_error = None

    mints = []

    if include_eth:
        try:
            from trackers.eth_live_mints import get_top_eth_mints

            eth_mints = await get_top_eth_mints(limit=limit)
            mints.extend(eth_mints)
        except Exception as e:
            _last_fetch_error = f"ETH Memory Error: {e}"

    # Sort by recent velocity
    mints.sort(key=lambda m: m.recent_mints_count or 0, reverse=True)
    mints = mints[:limit]

    # 3. Fallback to Waypoint API if live scrapers have nothing
    if not mints:
        mints = await fetch_waypoint_mints(session, limit=limit, enrich=True)

    return mints


def get_last_fetch_error() -> Optional[str]:
    """For /mints_status: reason why last fetch returned no mints."""
    return _last_fetch_error
