"""
Free collection / NFT image resolution for wallet-tracker embeds.

Strategy (aligned with nftscan-discord-bot contract_enricher):
  1. Per-token metadata (tokenURI / IPFS) when available
  2. Alchemy getContractMetadata (OpenSea CDN image)
  3. CoinGecko NFT contract logo (no API key)
  4. NFTScan logo CDN HEAD (no API key)
  5. OpenSea v2 contract endpoint
  6. Reservoir collections API
  7. Identicon fallback (stamp.fyi)

Optional: wsrv.nl square proxy for Discord thumbnails (free).
"""
from __future__ import annotations

import json
import os
import struct
import time
import zlib
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
import discord

import config
from app_paths import ASSETS_DIR, DATA_DIR, ensure_dirs

_CACHE_FILE = DATA_DIR / "collection_image_cache.json"
_CACHE_TTL = int(os.getenv("COLLECTION_IMAGE_CACHE_TTL", str(7 * 24 * 3600)))
_MEMORY: Dict[str, tuple[str, float]] = {}

# Token-level image cache (in-memory only; tokens change less often than collections)
_TOKEN_MEMORY: Dict[str, tuple[str, float]] = {}
_TOKEN_CACHE_TTL = int(os.getenv("TOKEN_IMAGE_CACHE_TTL", str(24 * 3600)))


def _token_cache_key(contract: str, token_id: int) -> str:
    return f"{contract.lower()}:{int(token_id)}"


def _token_cache_get(contract: str, token_id: int) -> Optional[str]:
    key = _token_cache_key(contract, token_id)
    mem = _TOKEN_MEMORY.get(key)
    if mem and (time.time() - mem[1]) < _TOKEN_CACHE_TTL:
        return mem[0] or None
    return None


def _token_cache_set(contract: str, token_id: int, url: Optional[str]) -> None:
    key = _token_cache_key(contract, token_id)
    clean = normalize_nft_image_url(url) or ""
    _TOKEN_MEMORY[key] = (clean, time.time())

_BAD_IMAGE_SUBSTRINGS = (
    "nftscan.com/images/og-img",
    "og-img/home.png",
)

COINGECKO_NFT_URL = "https://api.coingecko.com/api/v3/nfts/ethereum/contract/{contract}"
NFTSCAN_LOGO_URL = "https://logo.nftscan.com/logo/{contract}.png"

# Square black thumbnail when no collection art (256×256 — same target as wsrv.nl crop)
COLLECTION_FALLBACK_FILENAME = "wt_collection_fallback_256.png"
COLLECTION_THUMB_PX = 256

IPFS_GATEWAYS = (
    "https://cf-ipfs.com/ipfs",
    "https://ipfs.io/ipfs",
    "https://cloudflare-ipfs.com/ipfs",
)


def _alchemy_key() -> str:
    return (
        (getattr(config, "ALCHEMY_NFT_API_KEY", None) or os.getenv("ALCHEMY_NFT_API_KEY") or "")
        .strip()
        or "demo"
    )


def normalize_nft_image_url(url: Optional[str]) -> Optional[str]:
    if not url or not isinstance(url, str):
        return None
    u = url.strip()
    if not u or u.startswith("data:"):
        return None
    low = u.lower()
    if any(b in low for b in _BAD_IMAGE_SUBSTRINGS):
        return None
    if u.startswith("ipfs://"):
        tail = u.replace("ipfs://", "").lstrip("/")
        return f"{IPFS_GATEWAYS[0]}/{tail}"
    if u.startswith("http://") or u.startswith("https://"):
        return u
    return None


def is_placeholder_collection_image(url: Optional[str]) -> bool:
    """True if URL is missing or only the generic identicon fallback."""
    if not url or not str(url).strip():
        return True
    low = str(url).lower()
    if "stamp.fyi/avatar/" in low:
        return True
    if low.startswith("attachment://"):
        return False
    return False


def _write_solid_png_rgb(path: Path, size: int, rgb: tuple[int, int, int] = (0, 0, 0)) -> None:
    """Write a solid-color PNG (stdlib only)."""
    r, g, b = rgb

    def _chunk(tag: bytes, data: bytes) -> bytes:
        block = tag + data
        return struct.pack(">I", len(data)) + block + struct.pack(">I", zlib.crc32(block) & 0xFFFFFFFF)

    ihdr = struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0)
    row = b"\x00" + bytes([r, g, b] * size)
    raw = row * size
    idat = zlib.compress(raw, 9)
    png = b"\x89PNG\r\n\x1a\n" + _chunk(b"IHDR", ihdr) + _chunk(b"IDAT", idat) + _chunk(b"IEND", b"")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(png)


def ensure_black_collection_fallback_path() -> Path:
    """256×256 black PNG for Discord embed thumbnail (created once under assets/)."""
    path = ASSETS_DIR / COLLECTION_FALLBACK_FILENAME
    if path.is_file() and path.stat().st_size > 200:
        return path
    ensure_dirs()
    _write_solid_png_rgb(path, COLLECTION_THUMB_PX, (0, 0, 0))
    return path


def prepare_collection_thumbnail(
    token_image_url: Optional[str],
    collection_image_url: Optional[str],
    files: List[discord.File],
) -> str:
    """
    Pick thumbnail URL for embed.set_thumbnail.
    Uses real collection/token art when available; otherwise attaches black 256×256 PNG.
    """
    for candidate in (token_image_url, collection_image_url):
        if candidate and not is_placeholder_collection_image(candidate):
            proxied = discord_embed_image_url(candidate)
            if proxied:
                return proxied
    fallback_path = ensure_black_collection_fallback_path()
    files.append(discord.File(str(fallback_path), filename=COLLECTION_FALLBACK_FILENAME))
    return f"attachment://{COLLECTION_FALLBACK_FILENAME}"


def discord_embed_image_url(url: Optional[str]) -> str:
    """Return URL safe for Discord embed thumbnail; optional wsrv.nl square crop."""
    clean = normalize_nft_image_url(url)
    if not clean:
        return ""
    if os.getenv("WALLET_TRACKER_IMAGE_PROXY", "1").strip().lower() in ("0", "false", "no", "off"):
        return clean
    from urllib.parse import quote

    return f"https://wsrv.nl/?url={quote(clean, safe='')}&w=256&h=256&fit=cover&output=png"


def _cache_get(contract: str) -> Optional[str]:
    key = contract.lower()
    now = time.time()
    mem = _MEMORY.get(key)
    if mem and (now - mem[1]) < _CACHE_TTL:
        return mem[0] or None
    try:
        if _CACHE_FILE.is_file():
            data = json.loads(_CACHE_FILE.read_text(encoding="utf-8"))
            row = data.get(key)
            if isinstance(row, dict):
                ts = float(row.get("ts", 0))
                url = normalize_nft_image_url(row.get("url"))
                if url and (now - ts) < _CACHE_TTL:
                    _MEMORY[key] = (url, ts)
                    return url
    except Exception:
        pass
    return None


def _cache_set(contract: str, url: Optional[str]) -> None:
    key = contract.lower()
    clean = normalize_nft_image_url(url) or ""
    now = time.time()
    _MEMORY[key] = (clean, now)
    try:
        ensure_dirs()
        data: Dict[str, Any] = {}
        if _CACHE_FILE.is_file():
            data = json.loads(_CACHE_FILE.read_text(encoding="utf-8"))
        data[key] = {"url": clean, "ts": now}
        _CACHE_FILE.write_text(json.dumps(data, indent=0)[:500_000], encoding="utf-8")
    except Exception:
        pass


async def _head_ok(session: aiohttp.ClientSession, url: str) -> bool:
    try:
        async with session.head(url, timeout=aiohttp.ClientTimeout(total=6), allow_redirects=True) as r:
            return r.status == 200
    except Exception:
        return False


async def _fetch_coingecko_logo(session: aiohttp.ClientSession, contract: str) -> Optional[str]:
    url = COINGECKO_NFT_URL.format(contract=contract.lower())
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return None
            data = await r.json()
            img = data.get("image") if isinstance(data.get("image"), dict) else {}
            return normalize_nft_image_url(img.get("small_2x") or img.get("small") or img.get("large"))
    except Exception:
        return None


async def _fetch_nftscan_logo(session: aiohttp.ClientSession, contract: str) -> Optional[str]:
    logo = NFTSCAN_LOGO_URL.format(contract=contract.lower())
    if await _head_ok(session, logo):
        return logo
    return None


async def _fetch_alchemy_token_image(
    session: aiohttp.ClientSession, contract: str, token_id: int
) -> Optional[str]:
    """Fetch per-token image via Alchemy getNFTMetadata (paid plan = reliable CDN URLs).

    Uses image.cachedUrl > thumbnailUrl > pngUrl > originalUrl for best Discord compatibility.
    Skips automatically when using a demo key to conserve quota.
    """
    key = _alchemy_key()
    if key == "demo":
        return None
    url = f"https://eth-mainnet.g.alchemy.com/nft/v3/{key}/getNFTMetadata"
    try:
        async with session.get(
            url,
            params={"contractAddress": contract.lower(), "tokenId": str(token_id)},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            if r.status != 200:
                return None
            data = await r.json()
            img = data.get("image") if isinstance(data.get("image"), dict) else {}
            # Prefer Alchemy CDN URLs (cached, fast, reliable for Discord)
            return normalize_nft_image_url(
                img.get("cachedUrl")
                or img.get("thumbnailUrl")
                or img.get("pngUrl")
                or img.get("originalUrl")
                or data.get("image")
                or data.get("image_url")
            )
    except Exception:
        return None


async def _fetch_alchemy_contract_image(
    session: aiohttp.ClientSession, contract: str, alchemy_data: Optional[Dict[str, Any]] = None
) -> Optional[str]:
    if alchemy_data:
        osm = alchemy_data.get("openSeaMetadata") or {}
        u = normalize_nft_image_url(
            osm.get("imageUrl")
            or osm.get("image_url")
            or alchemy_data.get("imageUrl")
            or alchemy_data.get("image_url")
        )
        if u:
            return u
    key = _alchemy_key()
    url = f"https://eth-mainnet.g.alchemy.com/nft/v3/{key}/getContractMetadata"
    try:
        async with session.get(
            url,
            params={"contractAddress": contract.lower()},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status != 200:
                return None
            data = await r.json()
            osm = data.get("openSeaMetadata") or {}
            return normalize_nft_image_url(
                osm.get("imageUrl")
                or osm.get("image_url")
                or data.get("imageUrl")
                or data.get("image_url")
            )
    except Exception:
        return None


async def _fetch_opensea_contract_image(session: aiohttp.ClientSession, contract: str) -> Optional[str]:
    api_url = f"https://api.opensea.io/api/v2/chain/ethereum/contract/{contract.lower()}"
    headers = {"Accept": "application/json"}
    okey = (os.getenv("OPENSEA_API_KEY") or "").strip()
    if okey:
        headers["X-API-KEY"] = okey
    try:
        async with session.get(api_url, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200:
                return None
            data = await r.json()
            coll = data.get("collection") if isinstance(data.get("collection"), dict) else {}
            return normalize_nft_image_url(
                data.get("image_url")
                or data.get("imageUrl")
                or coll.get("image_url")
                or coll.get("banner_image_url")
            )
    except Exception:
        return None


async def _fetch_reservoir_image(session: aiohttp.ClientSession, contract: str) -> Optional[str]:
    url = f"https://api.reservoir.tools/collections/v7?id={contract.lower()}"
    headers = {}
    rkey = (getattr(config, "RESERVOIR_API_KEY", None) or os.getenv("RESERVOIR_API_KEY") or "").strip()
    if rkey:
        headers["x-api-key"] = rkey
    try:
        async with session.get(url, headers=headers or None, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200:
                return None
            data = await r.json()
            collections = data.get("collections") or []
            if not collections:
                return None
            c0 = collections[0]
            meta = c0.get("metadata") if isinstance(c0.get("metadata"), dict) else {}
            return normalize_nft_image_url(
                c0.get("image") or c0.get("imageUrl") or meta.get("imageUrl") or meta.get("image")
            )
    except Exception:
        return None


async def fetch_token_image_enhanced(
    session: aiohttp.ClientSession,
    contract: str,
    token_id: int = 1,
) -> Optional[str]:
    """Fetch per-token image: Alchemy getNFTMetadata first (paid plan), then on-chain tokenURI fallback.

    Uses an in-memory LRU-style cache per (contract, token_id) to avoid repeat API calls.
    """
    contract_l = (contract or "").strip().lower()
    if not contract_l.startswith("0x") or len(contract_l) < 10:
        return None

    cached = _token_cache_get(contract_l, token_id)
    if cached:
        return cached

    # 1. Alchemy getNFTMetadata (reliable CDN with paid plan)
    alchemy_img = await _fetch_alchemy_token_image(session, contract_l, token_id)
    if alchemy_img:
        _token_cache_set(contract_l, token_id, alchemy_img)
        return alchemy_img

    # 2. On-chain tokenURI fallback
    try:
        from trackers.eth_live_mints import fetch_token_image

        onchain_img = await fetch_token_image(contract_l, token_id)
        if onchain_img:
            _token_cache_set(contract_l, token_id, onchain_img)
        return onchain_img
    except Exception:
        return None


async def fetch_collection_image(
    session: aiohttp.ClientSession,
    contract: str,
    token_id: int = 1,
    *,
    alchemy_contract_data: Optional[Dict[str, Any]] = None,
    token_image_url: Optional[str] = None,
) -> str:
    """
    Resolve best collection/NFT image URL for embeds (cached).
    Returns a normalized https URL or stamp.fyi identicon.
    """
    contract_l = (contract or "").strip().lower()
    if not contract_l.startswith("0x") or len(contract_l) < 10:
        return ""

    cached = _cache_get(contract_l)
    if cached and not is_placeholder_collection_image(cached):
        return cached

    from trackers.eth_live_mints import fetch_token_image

    candidates: list[Optional[str]] = []

    if token_image_url:
        candidates.append(normalize_nft_image_url(token_image_url))
    else:
        # Priority 1: Alchemy getNFTMetadata (reliable CDN with paid plan)
        alchemy_token_img = _token_cache_get(contract_l, token_id)
        if not alchemy_token_img:
            alchemy_token_img = await _fetch_alchemy_token_image(
                session, contract_l, int(token_id or 1)
            )
            if alchemy_token_img:
                _token_cache_set(contract_l, token_id, alchemy_token_img)
        if alchemy_token_img:
            candidates.append(alchemy_token_img)
        else:
            # Priority 2: On-chain tokenURI fallback
            try:
                candidates.append(await fetch_token_image(contract_l, int(token_id or 1)))
            except Exception:
                pass

    candidates.append(await _fetch_alchemy_contract_image(session, contract_l, alchemy_contract_data))
    candidates.append(await _fetch_coingecko_logo(session, contract_l))
    candidates.append(await _fetch_nftscan_logo(session, contract_l))
    candidates.append(await _fetch_opensea_contract_image(session, contract_l))
    candidates.append(await _fetch_reservoir_image(session, contract_l))

    resolved = ""
    for c in candidates:
        if c:
            resolved = c
            break

    if resolved and not is_placeholder_collection_image(resolved):
        _cache_set(contract_l, resolved)
    return resolved or ""
