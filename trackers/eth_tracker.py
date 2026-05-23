"""
Ethereum Tracker - ERC20 & NFT (ERC721/1155)
"""
import asyncio
import io
import json
import os
import re
import time as time_module
import discord
from datetime import datetime, timezone
from collections import defaultdict, deque
from typing import Dict, List, Optional, Set, Any, Tuple
from web3 import Web3
from discord import Embed, Color
from dotenv import load_dotenv
import aiohttp
import wallet_database
import config

try:
    import feed_events
except Exception:
    feed_events = None

load_dotenv(override=True)

# Velocr3 — wallet tracker embed branding (matches token / mint overview style)
_BRAND_NAME = (
    (
        os.getenv("VELOCR3_BRAND_NAME")
        or os.getenv("VELCOR3_BRAND_NAME")
        or os.getenv("NERDS_BRAND_NAME")
        or "Velocr3"
    ).strip()
    or "Velocr3"
)
_EMBED_NEUTRAL = 0x202025

# Configuration
ETHEREUM_RPC_URL = os.getenv("ETHEREUM_RPC_URL", "https://eth.llamarpc.com")
# Policy: do not use Alchemy as a general-purpose RPC here.
# Alchemy should only be used via its NFT API endpoints (key in ALCHEMY_NFT_API_KEY).
if "alchemy.com" in (ETHEREUM_RPC_URL or "").lower():
    fallback = (os.getenv("NON_ALCHEMY_ETH_RPC_URL") or "https://eth.llamarpc.com").strip()
    print("[WARN] ETHEREUM_RPC_URL points to Alchemy. Falling back to NON_ALCHEMY_ETH_RPC_URL for RPC.")
    ETHEREUM_RPC_URL = fallback

ETHSCAN_API_KEY = (config.ETHSCAN_API_KEY or "").strip()
NFTSCAN_API_KEY = os.getenv("NFTSCAN_API_KEY")

# Alchemy is allowed ONLY for wallet NFT tracking enrichment (not as a general RPC dependency).
# Use a dedicated key env var so the wallet tracker can call Alchemy's NFT endpoints without
# requiring an Alchemy RPC URL anywhere else.
ALCHEMY_API_KEY = (os.getenv("ALCHEMY_NFT_API_KEY") or "").strip() or None

# Web3 Setup (may stay disconnected until connect_web3 finds a working URL)
w3: Optional[Web3] = None


def _candidate_eth_rpc_urls() -> List[str]:
    """Ordered list of RPC URLs to try (skips Alchemy hostnames for this module)."""
    seen: Set[str] = set()
    out: List[str] = []
    raw_parts: List[str] = []
    for key in (
        "NON_ALCHEMY_ETH_RPC_URL",
        "ETHEREUM_RPC_URL",
        "ETHEREUM_MINT_RPC_URLS",
    ):
        v = (os.getenv(key) or "").strip()
        if v:
            raw_parts.extend(v.split(","))
    # Sensible public fallbacks if .env URLs are down
    raw_parts.extend(
        [
            "https://ethereum.publicnode.com",
            "https://1rpc.io/eth",
            "https://rpc.ankr.com/eth",
        ]
    )
    for part in raw_parts:
        u = (part or "").strip()
        if not u or "alchemy.com" in u.lower() or u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def connect_web3():
    """Connect to the first working ETH RPC from env + public fallbacks."""
    global w3
    for url in _candidate_eth_rpc_urls():
        try:
            temp_w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 20}))
            if temp_w3.is_connected():
                w3 = temp_w3
                safe = url if "alchemy.com" not in url.lower() else url.rsplit("/", 1)[0] + "/***"
                print(f"[OK] Connected to ETH RPC: {safe}")
                return
        except Exception:
            continue
    w3 = None
    print("[X] Failed to connect to any ETH RPC. Set NON_ALCHEMY_ETH_RPC_URL (or ETHEREUM_RPC_URL) to a working endpoint.")


# Initial Connection
connect_web3()

# Avatar fallback URL (effigy.im serves PNG identicons, Discord-friendly)
EFFIGY_AVATAR = "https://cdn.stamp.fyi/avatar/eth:{address}?s=300"

# Tracked wallets
tracked_eth_wallets: Dict[str, str] = {}
startup_time: int = int(datetime.utcnow().timestamp())

# Bounded dedup set — keeps last 50 000 tx keys to prevent unbounded RAM growth
_SEEN_TXS_MAX = 50_000
_seen_txs_deque: deque = deque(maxlen=_SEEN_TXS_MAX)
seen_txs: Set[str] = set()

def _seen_txs_add(key: str) -> bool:
    """Add key to seen set. Returns True if it was NOT already seen (i.e. new)."""
    if key in seen_txs:
        return False
    if len(seen_txs) >= _SEEN_TXS_MAX:
        oldest = _seen_txs_deque.popleft()
        seen_txs.discard(oldest)
    seen_txs.add(key)
    _seen_txs_deque.append(key)
    return True

# Cached ETH/USD price (refreshed in background)
_cached_eth_usd: float = 3000.0  # Safe default

# Cached wallet profile pictures
_wallet_avatar_cache: Dict[str, Optional[str]] = {}
_wallet_profile_cache: Dict[str, dict] = {}
_WALLET_PROFILE_TTL = 1800  # 30 minutes

# Cached NFT collection socials — (data_dict, timestamp)
_nft_socials_cache: Dict[str, tuple] = {}
_NFT_SOCIALS_TTL = 3600  # 1 hour

# Alchemy getNFTSales cache (tx_hash -> price_eth) to avoid repeat lookups
_alchemy_price_cache: Dict[str, float] = {}

# Throttle flags — print once per session, not on every call
_opensea_no_key_warned: bool = False
_reservoir_conn_warned: bool = False
_alchemy_429_last_warn: float = 0.0  # epoch seconds

# Etherscan v2: log NOTOK / HTTP errors (otherwise wallet tracker is silent)
_etherscan_warn_at: Dict[str, float] = {}
_ETHERSCAN_WARN_COOLDOWN_S = 120.0

# Etherscan v2 hard rate limits can be as low as 3 req/sec depending on key/tier.
# Use a single global scheduler so ALL Etherscan calls share the same budget.
try:
    # Safe default: ~1.8 req/sec total (below 3/sec hard cap).
    _ETHERSCAN_MIN_INTERVAL_S = float(os.getenv("ETHERSCAN_MIN_INTERVAL", "0.55") or 0.55)
except Exception:
    _ETHERSCAN_MIN_INTERVAL_S = 0.55
_ETHERSCAN_MIN_INTERVAL_S = max(0.20, _ETHERSCAN_MIN_INTERVAL_S)  # keep sane
try:
    _ETHERSCAN_MAX_RETRIES = int(os.getenv("ETHERSCAN_MAX_RETRIES", "3") or 3)
except Exception:
    _ETHERSCAN_MAX_RETRIES = 3
_ETHERSCAN_MAX_RETRIES = max(0, min(8, _ETHERSCAN_MAX_RETRIES))
_etherscan_lock = asyncio.Lock()
_etherscan_next_at = 0.0  # epoch seconds
_etherscan_cooldown_until = 0.0  # epoch seconds (when rate-limited, pause all Etherscan calls)


def _throttled_etherscan_warn(kind: str, message: str) -> None:
    now = time_module.time()
    prev = _etherscan_warn_at.get(kind, 0.0)
    if now - prev < _ETHERSCAN_WARN_COOLDOWN_S:
        return
    _etherscan_warn_at[kind] = now
    print(f"\033[91m[ETHERSCAN]\033[0m {message}")


async def _etherscan_acquire_slot() -> None:
    """Global pacing for Etherscan HTTP requests."""
    global _etherscan_next_at
    async with _etherscan_lock:
        now = time_module.time()
        cd = float(_etherscan_cooldown_until or 0.0)
        if cd > now:
            # Don't burn a slot while cooling down; just sleep outside lock.
            wait_cd = cd - now
            # schedule next slot after cooldown ends
            _etherscan_next_at = max(cd, _etherscan_next_at)
        else:
            wait_cd = 0.0
        wait_s = max(0.0, float(_etherscan_next_at) - now)
        # schedule next slot before sleeping so other waiters queue properly
        _etherscan_next_at = max(now, _etherscan_next_at) + float(_ETHERSCAN_MIN_INTERVAL_S)
    if wait_cd > 0:
        await asyncio.sleep(wait_cd)
    if wait_s > 0:
        await asyncio.sleep(wait_s)


async def _etherscan_fetch_json(session: aiohttp.ClientSession, url: str, kind: str, wallet: str) -> Tuple[Optional[dict], Optional[str]]:
    """Fetch Etherscan JSON with backoff on rate limits. Returns (payload, error)."""
    last_err: Optional[str] = None
    for attempt in range(max(1, _ETHERSCAN_MAX_RETRIES + 1)):
        await _etherscan_acquire_slot()
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=14)) as r:
                if r.status != 200:
                    # 429/5xx: backoff + retry
                    if r.status in (429, 500, 502, 503, 504):
                        backoff = min(8.0, 0.8 * (2 ** (attempt - 1)))
                        last_err = f"HTTP {r.status}"
                        await asyncio.sleep(backoff)
                        continue
                    return None, f"HTTP {r.status}"
                try:
                    payload = await r.json()
                except Exception as ex:
                    return None, f"invalid JSON: {ex}"
        except Exception as ex:
            last_err = f"request failed: {ex}"
            backoff = min(8.0, 0.8 * (2 ** (attempt - 1)))
            await asyncio.sleep(backoff)
            continue

        # Etherscan error payloads are still HTTP 200 with status != "1"
        rows, err = _etherscan_account_tx_rows(payload)
        if err:
            low = str(err).lower()
            if "max calls per sec" in low or "rate limit" in low:
                # Global cooldown so we stop scanning wallets for a bit.
                global _etherscan_cooldown_until
                _etherscan_cooldown_until = max(float(_etherscan_cooldown_until or 0.0), time_module.time() + 20.0)
                backoff = min(8.0, 1.0 * (2 ** (attempt - 1)))
                if attempt == 1:
                    _throttled_etherscan_warn(kind, f"{kind} ({wallet[:10]}…): {err}")
                await asyncio.sleep(backoff)
                last_err = err
                continue
            return payload, err
        return payload, None

    return None, last_err or "rate limited"


def _etherscan_account_tx_rows(payload: Any) -> Tuple[Optional[List[dict]], Optional[str]]:
    """Parse Etherscan account module JSON (tokentx / tokennfttx). Returns (rows, error)."""
    if not isinstance(payload, dict):
        return None, "response is not a JSON object"
    st = payload.get("status")
    if str(st) != "1":
        res = payload.get("result")
        msg = (payload.get("message") or "").strip()
        if isinstance(res, str) and res.strip():
            err = f"{msg}: {res}".strip(": ") if msg else res
            return None, err or "Etherscan status not OK"
        return None, msg or str(res) or "Etherscan status not OK"
    res = payload.get("result")
    if isinstance(res, str):
        return None, res
    if not isinstance(res, list):
        return None, f"unexpected result type {type(res).__name__}"
    return res, None


# WETH / Blur pool addresses for receipt log parsing (shared single + bulk NFT pricing)
_PAY_TOKEN_ADDRS = frozenset(
    {
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
        "0x0000000000a39bb272e79075ade125fd351887ac",  # Blur Pool (ETH)
        "0x4300000000000000000000000000000000000004",  # WETH (Blast)
    }
)
_WETH_TRANSFER_TOPIC = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def _sum_weth_eth_from_receipt(receipt: Any) -> float:
    """Sum WETH/Blur-pool Transfer amounts in a tx receipt (total trade payment). Returns ETH."""
    if not receipt:
        return 0.0
    weth_vals: List[int] = []
    receipt_logs = receipt.logs if hasattr(receipt, "logs") else receipt.get("logs", [])
    for l in receipt_logs:
        log_addr = (getattr(l, "address", "") if not isinstance(l, dict) else l.get("address", "")).lower()
        log_topics = getattr(l, "topics", []) if not isinstance(l, dict) else l.get("topics", [])
        if log_addr not in _PAY_TOKEN_ADDRS or len(log_topics) < 3:
            continue
        topic_hex = log_topics[0].hex() if hasattr(log_topics[0], "hex") else str(log_topics[0])
        topic_hex = topic_hex.replace("0x", "")
        if topic_hex != _WETH_TRANSFER_TOPIC:
            continue
        try:
            raw_data = getattr(l, "data", b"") if not isinstance(l, dict) else l.get("data", b"")
            data_hex = raw_data.hex() if hasattr(raw_data, "hex") else str(raw_data).replace("0x", "")
            if data_hex:
                weth_vals.append(int(data_hex, 16))
        except Exception:
            pass
    if not weth_vals:
        return 0.0
    return sum(weth_vals) / 1e18


def _erc721_transfer_key(tx: dict) -> str:
    return f"{tx['hash']}_{tx.get('contractAddress', '')}_{tx.get('to', '')}_{tx.get('value', '')}_{tx.get('tokenID', '')}"


def _group_erc721_transactions(results: list, wallet: str) -> List[dict]:
    """Group ERC721 rows by (tx hash, contract, direction) so bulk buys/sells/mints show once with quantity."""
    wl = wallet.lower()
    groups_map: Dict[tuple, List[dict]] = defaultdict(list)
    for tx in results:
        from_a = tx["from"].lower()
        to_a = tx["to"].lower()
        th = tx["hash"].lower()
        contract = tx["contractAddress"].lower()
        if from_a == "0x0000000000000000000000000000000000000000":
            kind = "mint"
        elif to_a == wl:
            kind = "buy"
        elif from_a == wl:
            kind = "sell"
        else:
            continue
        groups_map[(th, contract, kind)].append(tx)
    out: List[dict] = []
    for (th, contract, kind), txs in groups_map.items():
        txs.sort(key=lambda x: int(x.get("tokenID", "0")))
        out.append({"hash": th, "contract": contract, "kind": kind, "txs": txs})
    out.sort(key=lambda g: int(g["txs"][0].get("timeStamp", 0)), reverse=True)
    return out

async def fetch_nft_sale_price(session: aiohttp.ClientSession, contract: str, token_id: int, tx_hash: str, buyer: str = "", seller: str = "") -> float:
    """Query Alchemy getNFTSales v3 API for the exact sale price of an NFT trade.
    Tries contract+tokenId first, then buyer/seller address filter as fallback.
    Returns price in ETH. Returns 0 if not found or API unavailable."""
    if not ALCHEMY_API_KEY:
        return 0.0
    
    # Check cache first
    cache_key = tx_hash.lower()
    if cache_key in _alchemy_price_cache:
        return _alchemy_price_cache[cache_key]
    
    url = f"https://eth-mainnet.g.alchemy.com/nft/v3/{ALCHEMY_API_KEY}/getNFTSales"
    
    # Build query strategies: try contract+token first, then buyer/seller
    queries = [
        {"contractAddress": contract, "tokenId": str(token_id), "limit": 10, "order": "desc"},
    ]
    if buyer:
        queries.append({"buyerAddress": buyer, "contractAddress": contract, "limit": 10, "order": "desc"})
    if seller:
        queries.append({"sellerAddress": seller, "contractAddress": contract, "limit": 10, "order": "desc"})
    
    for params in queries:
        try:
            async with session.get(url, params=params, timeout=8) as r:
                if r.status != 200:
                    if r.status == 429:
                        global _alchemy_429_last_warn
                        import time as _time
                        _now = _time.time()
                        if _now - _alchemy_429_last_warn > 60:
                            _alchemy_429_last_warn = _now
                            print(f"\033[93m[ALCHEMY]\033[0m getNFTSales rate limited (429) — backing off.")
                    else:
                        print(f"\033[93m[ALCHEMY]\033[0m getNFTSales returned status {r.status}")
                    continue
                data = await r.json()
                sales = data.get("nftSales", [])
                
                for sale in sales:
                    sale_tx = sale.get("transactionHash", "").lower()
                    if sale_tx == tx_hash.lower():
                        # Sum sellerFee + protocolFee + royaltyFee = total trade price
                        total_raw = 0
                        for fee_key in ("sellerFee", "protocolFee", "royaltyFee"):
                            fee = sale.get(fee_key, {})
                            amt = int(fee.get("amount", "0") or "0")
                            total_raw += amt
                        
                        decimals = int(sale.get("sellerFee", {}).get("decimals", 18) or 18)
                        price = total_raw / (10 ** decimals)
                        
                        symbol = sale.get("sellerFee", {}).get("symbol", "ETH")
                        marketplace = sale.get("marketplace", "unknown")
                        print(f"\033[92m[ALCHEMY]\033[0m Price found: {price:.4f} {symbol} via {marketplace} for tx {tx_hash[:16]}...")
                        
                        _alchemy_price_cache[cache_key] = price
                        return price
        except Exception as e:
            print(f"\033[93m[ALCHEMY]\033[0m getNFTSales error: {e}")
            continue
    
    # No sale found in any query
    _alchemy_price_cache[cache_key] = 0.0
    return 0.0

# Floor price cache: contract_address -> (floor_eth, timestamp)
_floor_price_cache: Dict[str, tuple] = {}
FLOOR_CACHE_TTL = 3600  # 1 hour (Optimized to save NFTScan CUs)

async def fetch_collection_floor_price(session: aiohttp.ClientSession, contract: str) -> float:
    """Fetch NFT collection floor price via Alchemy getFloorPrice v3 API.
    Returns floor price in ETH. Cached for 5 minutes per collection."""
    contract_lower = contract.lower()
    
    # Check cache
    if contract_lower in _floor_price_cache:
        cached_price, cached_time = _floor_price_cache[contract_lower]
        if (time_module.time() - cached_time) < FLOOR_CACHE_TTL:
            return cached_price
    
    # Try Alchemy first
    if ALCHEMY_API_KEY:
        try:
            url = f"https://eth-mainnet.g.alchemy.com/nft/v3/{ALCHEMY_API_KEY}/getFloorPrice"
            params = {"contractAddress": contract_lower}
            async with session.get(url, params=params, timeout=8) as r:
                if r.status == 200:
                    data = await r.json()
                    # Check OpenSea floor first, then LooksRare
                    for marketplace in ("openSea", "looksRare"):
                        mp_data = data.get(marketplace, {})
                        fp = mp_data.get("floorPrice")
                        if fp and float(fp) > 0:
                            floor = float(fp)
                            print(f"\033[92m[FLOOR]\033[0m {contract_lower[:10]}... = {floor:.4f} ETH ({marketplace})")
                            _floor_price_cache[contract_lower] = (floor, time_module.time())
                            return floor
        except Exception as e:
            print(f"\033[93m[FLOOR]\033[0m Alchemy getFloorPrice error: {e}")
    
    # Fallback: NFTScan
    if NFTSCAN_API_KEY:
        try:
            nftscan_url = f"https://restapi.nftscan.com/api/v2/statistics/collection/{contract_lower}"
            headers = {"X-API-KEY": NFTSCAN_API_KEY}
            async with session.get(nftscan_url, headers=headers, timeout=5) as r:
                # Use content_type=None to handle missing headers from NFTScan
                data = await r.json(content_type=None)
                
                # Check for internal error codes (e.g. 403 Limit Reached)
                code = data.get('code')
                if code and code != 200:
                    msg = data.get('msg', 'Unknown Error')
                    print(f"[-] NFTScan Floor Result ({code}): {msg}")
                    return 0.0

                fp = data.get("data", {}).get("floor_price")
                if fp and float(fp) > 0:
                    floor = float(fp)
                    print(f"\033[92m[FLOOR]\033[0m {contract_lower[:10]}... = {floor:.4f} ETH (NFTScan)")
                    _floor_price_cache[contract_lower] = (floor, time_module.time())
                    return floor
        except Exception as e:
            print(f"\033[93m[FLOOR]\033[0m NFTScan floor lookup failed: {e}")
    
    _floor_price_cache[contract_lower] = (0.0, time_module.time())
    return 0.0

async def refresh_eth_usd_price(session: aiohttp.ClientSession):
    """Background task to keep ETH/USD price fresh."""
    global _cached_eth_usd
    try:
        price_refresh_s = max(30, int(os.getenv("ETH_USD_PRICE_INTERVAL_S", "120")))
    except Exception:
        price_refresh_s = 120
    while True:
        try:
            async with session.get(
                "https://coins.llama.fi/prices/current/ethereum:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                timeout=5
            ) as r:
                if r.status == 200:
                    d = await r.json()
                    price = d.get('coins', {}).get(
                        'ethereum:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', {}
                    ).get('price', 0)
                    if price > 0:
                        _cached_eth_usd = price
                        if getattr(config, "LOG_ETH_USD_REFRESH", False):
                            print(f"\033[90m[PRICE]\033[0m ETH/USD updated: ${_cached_eth_usd:,.2f}")
        except Exception as e:
            if getattr(config, "LOG_ETH_USD_REFRESH", False):
                print(f"\033[93m[PRICE WARN]\033[0m ETH/USD refresh failed: {e}")
        await asyncio.sleep(price_refresh_s)

# Discord only shows embed author icons for direct image URLs (PNG/JPG). SVG and some CDNs don't display.
EFFIGY_AVATAR = "https://cdn.stamp.fyi/avatar/eth:{address}?s=300"  # Fast reliable CDN mapping ENS and identicons

async def fetch_wallet_avatar(wallet: str, session: aiohttp.ClientSession):
    """Trader profile image for embed author. Local file -> DB -> OpenSea API -> PNG identicon. Returns (url, local_file_path)"""
    import os
    wallet_lower = wallet.lower()
    
    # 0. Local PFP folder override
    for ext in ['png', 'jpg', 'jpeg', 'gif']:
        local_path = f"pfps/{wallet_lower}.{ext}"
        if os.path.exists(local_path):
            return f"attachment://pfp_{wallet_lower}.{ext}", local_path
            
    if wallet_lower in _wallet_avatar_cache:
        return _wallet_avatar_cache[wallet_lower], None
    
    # 1. Database (PFP scraped/stored from OpenSea via /track_opensea)
    import wallet_database as database
    db_pfp = database.get_pfp_url(wallet_lower) or database.get_pfp_url(wallet)
    if db_pfp and isinstance(db_pfp, str) and db_pfp.startswith("http"):
        _wallet_avatar_cache[wallet_lower] = db_pfp
        return db_pfp, None
    
    # helper for auto-update
    def update_pfp_db(addr, url):
        return database.update_pfp_db(addr, url)
    
    # 2. Try OpenSea API to fetch profile picture automatically
    try:
        opensea_url = f"https://api.opensea.io/api/v2/accounts/{wallet_lower}"
        headers = {"Accept": "application/json"}
        opensea_api_key = os.getenv("OPENSEA_API_KEY")
        if opensea_api_key:
            headers["X-API-KEY"] = opensea_api_key
            
        async with session.get(opensea_url, headers=headers, timeout=5) as r:
            if r.status == 200:
                data = await r.json()
                pfp = data.get("profile_image_url") or data.get("profile_img_url") or ""
                if pfp and pfp.startswith("http") and "blank" not in pfp.lower():
                    # Cache in DB for future use
                    update_pfp_db(wallet_lower, pfp)
                    _wallet_avatar_cache[wallet_lower] = pfp
                    print(f"\033[92m[PFP]\033[0m Auto-fetched PFP from OpenSea for {wallet_lower[:10]}...")
                    return pfp, None
            elif r.status in (401, 403) and not opensea_api_key:
                global _opensea_no_key_warned
                if not _opensea_no_key_warned:
                    _opensea_no_key_warned = True
                    print(f"\033[93m[PFP]\033[0m No OPENSEA_API_KEY set — automatic PFP fetches disabled. Falling back to identicon.")
    except Exception as e:
        pass  # Silently fall through to identicon
    
    # 3. Guaranteed PNG for Discord (effigy.im identicon)
    avatar_url = EFFIGY_AVATAR.format(address=wallet)
    _wallet_avatar_cache[wallet_lower] = avatar_url
    return avatar_url, None


async def attach_wallet_pfp_for_embed(
    wallet: str,
    session: aiohttp.ClientSession,
    files: List[discord.File],
    *,
    profile_image_url: str = "",
) -> str:
    """
    Always attach a wallet PFP file when possible so Discord shows the author icon.
    Returns icon_url (attachment://…) for embed.set_author.
    """
    wallet_lower = wallet.lower()
    avatar_url, pfp_local = await fetch_wallet_avatar(wallet, session)
    prof = (profile_image_url or "").strip()
    if prof.startswith("http") and "blank" not in prof.lower() and not pfp_local:
        avatar_url = prof

    if pfp_local:
        ext = pfp_local.rsplit(".", 1)[-1].lower()
        fname = f"wt_pfp_{wallet_lower}.{ext}"
        files.append(discord.File(pfp_local, filename=fname))
        return f"attachment://{fname}"

    if avatar_url.startswith("attachment://"):
        return avatar_url

    urls_to_try: List[str] = []
    if avatar_url.startswith("http"):
        urls_to_try.append(avatar_url)
    urls_to_try.append(EFFIGY_AVATAR.format(address=wallet))

    for url in urls_to_try:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status != 200:
                    continue
                data = await r.read()
                if len(data) < 200:
                    continue
                ctype = (r.headers.get("Content-Type") or "").lower()
                ext = "png"
                if "jpeg" in ctype or "jpg" in ctype:
                    ext = "jpg"
                elif "gif" in ctype:
                    ext = "gif"
                elif "webp" in ctype:
                    ext = "webp"
                fname = f"wt_pfp_{wallet_lower}.{ext}"
                buf = io.BytesIO(data)
                buf.seek(0)
                files.append(discord.File(buf, filename=fname))
                return f"attachment://{fname}"
        except Exception:
            continue

    return EFFIGY_AVATAR.format(address=wallet)


def _embed_section_spacer(embed: discord.Embed) -> None:
    """Visual gap between embed field groups."""
    embed.add_field(name="\u200b", value="\u200b", inline=False)


async def fetch_wallet_profile(wallet: str, session: aiohttp.ClientSession) -> dict:
    """Fetch wallet account profile metadata from OpenSea for richer embeds."""
    wallet_lower = wallet.lower()
    now_ts = time_module.time()
    cached = _wallet_profile_cache.get(wallet_lower)
    if cached and (now_ts - float(cached.get("_ts", 0))) < _WALLET_PROFILE_TTL:
        return cached

    profile = {
        "name": f"{wallet[:6]}...{wallet[-4:]}",
        "bio": "",
        "joined": "",
        "profile_image_url": "",
        "banner_image_url": "",
        "is_verified": False,
        "opensea_url": f"https://opensea.io/{wallet_lower}",
        "portfolio_usd": 0.0,
        "nft_percent": 0.0,
        "token_percent": 0.0,
        "_ts": now_ts,
    }

    try:
        headers = {"Accept": "application/json"}
        opensea_api_key = os.getenv("OPENSEA_API_KEY")
        if opensea_api_key:
            headers["X-API-KEY"] = opensea_api_key
        url = f"https://api.opensea.io/api/v2/accounts/{wallet_lower}"
        async with session.get(url, headers=headers, timeout=6) as r:
            if r.status == 200:
                data = await r.json()
                profile["name"] = (
                    data.get("username")
                    or data.get("name")
                    or data.get("address")
                    or profile["name"]
                )
                profile["bio"] = (data.get("bio") or "").strip()
                profile["joined"] = (data.get("created_date") or "").strip()
                profile["profile_image_url"] = (
                    data.get("profile_image_url")
                    or data.get("profile_img_url")
                    or ""
                )
                profile["banner_image_url"] = (
                    data.get("banner_image_url")
                    or data.get("banner_img_url")
                    or ""
                )
                profile["is_verified"] = bool(
                    data.get("is_verified")
                    or data.get("verified")
                    or data.get("is_enabled")
                )
                # Portfolio figures are not guaranteed in API responses; parse defensively.
                stats = data.get("stats") or data.get("totals") or {}
                profile["portfolio_usd"] = float(stats.get("portfolio_usd", 0) or 0)
                profile["nft_percent"] = float(stats.get("nfts_percent", 0) or 0)
                profile["token_percent"] = float(stats.get("tokens_percent", 0) or 0)
    except Exception:
        pass

    _wallet_profile_cache[wallet_lower] = profile
    return profile

async def fetch_eth_nft_socials(contract: str, session: aiohttp.ClientSession) -> dict:
    """Fetch social links for an ETH NFT collection. Returns a dict of URLs.
    Priority: Alchemy -> OpenSea -> NFTScan -> Reservoir -> Waypoint."""
    contract_lower = contract.lower()
    cached = _nft_socials_cache.get(contract_lower)
    if cached and (time_module.time() - cached[1]) < _NFT_SOCIALS_TTL:
        return cached[0]
    
    socials = {}
    
    # 1. Alchemy getContractMetadata (reliable, we have API key)
    if ALCHEMY_API_KEY and not socials:
        try:
            url = f"https://eth-mainnet.g.alchemy.com/nft/v3/{ALCHEMY_API_KEY}/getContractMetadata"
            params = {"contractAddress": contract_lower}
            async with session.get(url, params=params, timeout=8) as r:
                if r.status == 200:
                    data = await r.json()
                    opensea_meta = data.get("openSeaMetadata", {})
                    twitter = opensea_meta.get("twitterUsername")
                    discord_url = opensea_meta.get("discordUrl")
                    website = opensea_meta.get("externalUrl")
                    
                    if twitter:
                        socials["twitter"] = f"https://x.com/{twitter}"
                    if discord_url:
                        socials["discord"] = discord_url
                    if website:
                        socials["website"] = website
                    if socials:
                        print(f"\033[92m[SOCIALS]\033[0m Found {len(socials)} links via Alchemy for {contract_lower[:10]}...")
        except Exception as e:
            print(f"\033[93m[SOCIALS]\033[0m Alchemy metadata failed: {e}")
    
    # 2. OpenSea API (public, no key needed for collection endpoint)
    if not socials:
        try:
            opensea_url = f"https://api.opensea.io/api/v2/chain/ethereum/contract/{contract_lower}"
            headers = {"Accept": "application/json"}
            async with session.get(opensea_url, headers=headers, timeout=5) as r:
                if r.status == 200:
                    data = await r.json()
                    twitter = data.get("collection_twitter_username") or data.get("twitter_username")
                    discord_url = data.get("collection_discord_url") or data.get("discord_url")
                    website = data.get("collection_external_url") or data.get("external_url") or data.get("project_url")
                    
                    if twitter:
                        socials["twitter"] = f"https://x.com/{twitter}"
                    if discord_url:
                        socials["discord"] = discord_url
                    if website:
                        socials["website"] = website
                    if socials:
                        print(f"\033[92m[SOCIALS]\033[0m Found {len(socials)} links via OpenSea for {contract_lower[:10]}...")
        except Exception as e:
            print(f"\033[93m[SOCIALS]\033[0m OpenSea collection failed: {e}")
    
    # 3. NFTScan API (backup — skip if already hit limit or failed)
    if not socials and NFTSCAN_API_KEY:
        try:
            nftscan_url = f"https://restapi.nftscan.com/api/v2/collections/{contract_lower}"
            headers = {"X-API-KEY": NFTSCAN_API_KEY}
            async with session.get(nftscan_url, headers=headers, timeout=5) as r:
                data = await r.json(content_type=None)
                code = data.get('code')
                if code and code != 200:
                    # Skip error message to avoid console spam during socials fallback
                    return socials

                result = data.get("data", {})
                if result:
                    twitter = result.get("twitter")
                    discord_url = result.get("discord")
                    website = result.get("website") or result.get("external_url")
                    
                    if twitter:
                        if not twitter.startswith("http"):
                            twitter = f"https://x.com/{twitter.lstrip('@')}"
                        socials["twitter"] = twitter
                    if discord_url:
                        socials["discord"] = discord_url
                    if website:
                        socials["website"] = website
        except Exception as e:
            pass
    
    # 4. Reservoir API (backup)
    if not socials:
        try:
            reservoir_url = f"https://api.reservoir.tools/collections/v7?id={contract_lower}"
            async with session.get(reservoir_url, timeout=5) as r:
                if r.status == 200:
                    data = await r.json()
                    collections = data.get("collections", [])
                    if collections:
                        col = collections[0]
                        twitter = col.get("twitterUsername")
                        discord_url = col.get("discordUrl")
                        website = col.get("externalUrl")
                        
                        if twitter:
                            socials["twitter"] = f"https://x.com/{twitter}"
                        if discord_url:
                            socials["discord"] = discord_url
                        if website:
                            socials["website"] = website
        except Exception as e:
            err_s = str(e)
            # Suppress repeated DNS/connection failures (host unreachable from this network)
            if "getaddrinfo" in err_s or "Cannot connect" in err_s or "ClientConnector" in err_s:
                global _reservoir_conn_warned
                if not _reservoir_conn_warned:
                    _reservoir_conn_warned = True
                    print(f"\033[93m[SOCIALS]\033[0m Reservoir unreachable (DNS/network) — socials skipped for ETH collections.")
            else:
                print(f"\033[93m[SOCIALS]\033[0m Reservoir fetch failed: {err_s[:120]}")
    
    _nft_socials_cache[contract_lower] = (socials, time_module.time())
    return socials


def _unique_channel_ids(primary, fallback=None) -> List[int]:
    """Parse comma-separated Discord channel snowflakes, deduped in order."""
    raw = primary if primary else fallback
    if not raw:
        return []
    out: List[int] = []
    seen: Set[int] = set()
    for part in str(raw).split(","):
        part = part.strip()
        if not part:
            continue
        try:
            cid = int(part)
        except ValueError:
            continue
        if cid not in seen:
            seen.add(cid)
            out.append(cid)
    return out


async def check_eth_block(client, token_channel_id: int, nft_channel_id: int):
    """Refined API Poller replacing Web3 getLogs"""
    global startup_time
    
    async with aiohttp.ClientSession() as session:
        enable_erc20 = os.getenv("ENABLE_ETH_ERC20", "0").strip().lower() not in ("0", "false", "no", "off")
        enable_erc721 = os.getenv("ENABLE_ETH_NFT", "1").strip().lower() not in ("0", "false", "no", "off")
        # Defaults tuned for minimal Etherscan usage while still covering all wallets (round-robin).
        # One wallet per tick × longer sleep ≈ far fewer calls/sec than batch=5 every 30s.
        # Raise ETH_WALLET_BATCH_SIZE and/or lower ETH_SCAN_INTERVAL if you need snappier alerts.
        try:
            etherscan_offset = max(1, int(os.getenv("ETHERSCAN_OFFSET", "3")))
        except Exception:
            etherscan_offset = 3
        try:
            scan_interval_s = max(10, int(os.getenv("ETH_SCAN_INTERVAL", "60")))
        except Exception:
            scan_interval_s = 60
        try:
            wallet_batch_size = max(1, int(os.getenv("ETH_WALLET_BATCH_SIZE", "1")))
        except Exception:
            wallet_batch_size = 1
        if not enable_erc20:
            print("[ETH] ERC20 tracking disabled via ENABLE_ETH_ERC20=0")
        if not enable_erc721:
            print("[ETH] NFT tracking disabled via ENABLE_ETH_NFT=0")
        print(
            f"[ETH] Etherscan offset={etherscan_offset}, scan_interval={scan_interval_s}s, "
            f"wallets_per_tick={wallet_batch_size} (~{len(tracked_eth_wallets) or '?'} tracked)"
        )
        if not ETHSCAN_API_KEY:
            print(
                "\033[93m[ETHERSCAN]\033[0m ETHSCAN_API_KEY is missing — v2 account APIs return no "
                "transactions. Create a key at https://etherscan.io/myapikey and set ETHSCAN_API_KEY in .env."
            )
        else:
            print(f"\033[92m[ETHERSCAN]\033[0m API key loaded ({len(ETHSCAN_API_KEY)} characters).")

        scan_idx = 0
        while True:
            if not tracked_eth_wallets:
                await asyncio.sleep(10)
                continue

            wallets = list(tracked_eth_wallets.items())
            if not wallets:
                await asyncio.sleep(10)
                continue
            # Scan a small batch per interval to reduce API usage.
            n = len(wallets)
            start = scan_idx % n
            end = min(start + wallet_batch_size, n)
            batch = wallets[start:end]
            # if we hit the end, wrap next cycle
            scan_idx = (end % n) if end < n else 0

            for wallet, label in batch:
                try:
                    _ekey = f"&apikey={ETHSCAN_API_KEY}" if ETHSCAN_API_KEY else ""
                    erc20_url = f"https://api.etherscan.io/v2/api?chainid=1&module=account&action=tokentx&address={wallet}&page=1&offset={etherscan_offset}&sort=desc{_ekey}"
                    erc721_url = f"https://api.etherscan.io/v2/api?chainid=1&module=account&action=tokennfttx&address={wallet}&page=1&offset={etherscan_offset}&sort=desc{_ekey}"
                    
                    # Fetch ERC20
                    if enable_erc20 and token_channel_id:
                        d, err0 = await _etherscan_fetch_json(session, erc20_url, "tokentx", wallet)
                        if err0 and not d:
                            _throttled_etherscan_warn("tokentx", f"tokentx ({wallet[:10]}…): {err0}")
                        elif d is not None:
                            rows, err = _etherscan_account_tx_rows(d)
                            if err:
                                _throttled_etherscan_warn("tokentx", f"tokentx ({wallet[:10]}…): {err}")
                            elif rows:
                                for tx in rows:
                                    await process_api_tx(
                                        tx,
                                        wallet,
                                        "ERC20",
                                        client,
                                        session,
                                        token_channel_id,
                                        nft_channel_id,
                                    )

                    # Fetch ERC721 (group by tx+contract for bulk buy/sell quantity)
                    if enable_erc721 and (nft_channel_id or token_channel_id):
                        d, err0 = await _etherscan_fetch_json(session, erc721_url, "tokennfttx", wallet)
                        if err0 and not d:
                            _throttled_etherscan_warn("tokennfttx", f"tokennfttx ({wallet[:10]}…): {err0}")
                        elif d is not None:
                            rows, err = _etherscan_account_tx_rows(d)
                            if err:
                                _throttled_etherscan_warn("tokennfttx", f"tokennfttx ({wallet[:10]}…): {err}")
                            elif rows:
                                for group in _group_erc721_transactions(rows, wallet):
                                    await process_erc721_group(
                                        group,
                                        wallet,
                                        client,
                                        session,
                                        token_channel_id,
                                        nft_channel_id,
                                    )

                except Exception as e:
                    print(f"\033[91m[ETH ERROR]\033[0m API Polling failed for \033[93m{wallet}\033[0m: {e}")
                
                # Small gap between wallets (real pacing handled by global Etherscan limiter)
                await asyncio.sleep(0.05)
                
            if getattr(config, "LOG_ETH_WALLET_HEARTBEAT", False):
                print(f"\033[94m[ETH]\033[0m Scan complete for \033[93m{len(batch)}\033[0m wallets. Waiting...")
            await asyncio.sleep(scan_interval_s)


async def process_erc721_group(
    group: dict,
    wallet: str,
    client,
    session,
    token_channel_id: int,
    nft_channel_id: int,
) -> None:
    """Process one or more ERC721 transfers sharing the same tx hash + contract + direction (bulk)."""
    txs: List[dict] = group["txs"]
    if not txs:
        return
    first = txs[0]
    tx_hash = first["hash"]
    contract = first["contractAddress"]
    keys = [_erc721_transfer_key(t) for t in txs]
    if all(k in seen_txs for k in keys):
        return
    tx_time = int(first.get("timeStamp", 0))
    if tx_time < startup_time:
        for k in keys:
            _seen_txs_add(k)
        return
    for k in keys:
        _seen_txs_add(k)

    n = len(txs)
    kind = group["kind"]
    if kind == "sell":
        action_type = "Sent"
    else:
        action_type = "Received"  # buy + mint (from zero address)

    token_id = int(first["tokenID"])
    from_addr = first["from"]
    to_addr = first["to"]
    all_token_ids = [int(t["tokenID"]) for t in txs]
    bulk_qty = n if n > 1 else None

    try:
        from trackers.eth_address import ZERO_ADDRESS, normalize_eth_address
        from trackers.mint_wallet_intel import register_wallet_tracker_mint

        if normalize_eth_address(from_addr) == ZERO_ADDRESS:
            wl = tracked_eth_wallets.get(wallet.lower(), wallet)
            register_wallet_tracker_mint(
                tx_hash,
                wallet,
                wl,
                contract=contract,
                token_id=str(token_id),
            )
        elif kind != "sell":
            from trackers.nftscan_live_feed import record_smart_wallet_buy_from_tracker

            record_smart_wallet_buy_from_tracker(
                {
                    "to": wallet,
                    "contract_address": contract,
                    "token_id": str(token_id),
                    "tx_hash": tx_hash,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
    except Exception:
        pass

    try:
        embed, content, view, files = await create_eth_nft_embed(
            action_type=action_type,
            wallet=wallet,
            contract=contract,
            session=session,
            tx_hash=tx_hash,
            token_id=token_id,
            from_addr=from_addr,
            to_addr=to_addr,
            bulk_quantity=bulk_qty,
            all_token_ids=all_token_ids if bulk_qty else None,
        )
        target_ids = _unique_channel_ids(nft_channel_id, token_channel_id)
        for chan_id in target_ids:
            ch = client.get_channel(chan_id)
            if ch is None:
                try:
                    ch = await client.fetch_channel(chan_id)
                except Exception as e:
                    print(f"\033[91m[ETH ERROR]\033[0m Channel {chan_id} not found / no access: {e}")
                    continue
            label = "bulk" if bulk_qty else "single"
            print(
                f"\033[92m[ETH ALERT]\033[0m \033[1mERC721\033[0m ({label} x{n}) {action_type} for \033[96m{wallet}\033[0m: \033[90m{tx_hash}\033[0m"
            )
            await _safe_discord_send(ch, content=content, embed=embed, view=view, files=files)
            if feed_events is not None:
                try:
                    thumb = ""
                    try:
                        thumb = str(getattr(getattr(embed, "thumbnail", None), "url", "") or "")
                    except Exception:
                        thumb = ""
                    icon = ""
                    try:
                        icon = str(getattr(getattr(embed, "author", None), "icon_url", "") or "")
                    except Exception:
                        icon = ""
                    feed_events.add_event(
                        kind="wallet_nft",
                        guild_id=int(getattr(getattr(ch, "guild", None), "id", 0) or 0),
                        channel_id=int(getattr(ch, "id", 0) or 0),
                        title=str(getattr(getattr(embed, "author", None), "name", "") or "Wallet NFT")[:200],
                        body=(str(getattr(embed, "description", "") or "")[:1500]),
                        url=f"https://etherscan.io/tx/{tx_hash}",
                        extra={"wallet": wallet, "contract": contract, "tx": tx_hash, "thumb_url": thumb, "icon_url": icon},
                    )
                except Exception:
                    pass
    except Exception as e:
        print(f"\033[91m[ETH ERROR]\033[0m Failed alerting ERC721 group TX \033[90m{tx_hash}\033[0m: {e}")


async def process_api_tx(tx, wallet, tx_type, client, session, token_channel_id, nft_channel_id):
    tx_hash = tx["hash"]
    tx_key = f"{tx_hash}_{tx.get('contractAddress')}_{tx.get('to')}_{tx.get('value')}_{tx.get('tokenID','')}"
    
    if not _seen_txs_add(tx_key):
        return
    
    tx_time = int(tx.get("timeStamp", 0))
    if tx_time < startup_time:
        return # Skip old transactions present on startup
        
    contract = tx["contractAddress"]
    from_addr = tx["from"]
    to_addr = tx["to"]
    
    try:
        if tx_type == "ERC721":
            token_id = int(tx["tokenID"])
            action_type = "Received" if to_addr.lower() == wallet.lower() else "Sent"
            
            embed, content, view, files = await create_eth_nft_embed(
                action_type=action_type, wallet=wallet, contract=contract,
                session=session, tx_hash=tx_hash, token_id=token_id,
                from_addr=from_addr, to_addr=to_addr
            )
            target_ids = _unique_channel_ids(nft_channel_id, token_channel_id)
            for chan_id in target_ids:
                ch = client.get_channel(chan_id)
                if ch is None:
                    try:
                        ch = await client.fetch_channel(chan_id)
                    except Exception as e:
                        print(f"\033[91m[ETH ERROR]\033[0m Channel {chan_id} not found / no access: {e}")
                        continue
                print(f"\033[92m[ETH ALERT]\033[0m \033[1m{tx_type}\033[0m {action_type} for \033[96m{wallet}\033[0m: \033[90m{tx_hash}\033[0m")
                await _safe_discord_send(ch, content=content, embed=embed, view=view, files=files)
                
        else: # ERC20
            value_raw = int(tx["value"])
            decimals = int(tx["tokenDecimal"])
            amount = value_raw / (10 ** decimals) if decimals > 0 else value_raw
            action_type = "Received" if to_addr.lower() == wallet.lower() else "Sent"
            
            embed, files = await create_eth_embed(
                action=f"{action_type} Token", wallet=wallet, contract=contract,
                amount=amount, session=session, tx_hash=tx_hash
            )
            target_ids = _unique_channel_ids(token_channel_id)
            for chan_id in target_ids:
                ch = client.get_channel(chan_id)
                if ch is None:
                    try:
                        ch = await client.fetch_channel(chan_id)
                    except Exception as e:
                        print(f"\033[91m[ETH ERROR]\033[0m Channel {chan_id} not found / no access: {e}")
                        continue
                print(f"\033[92m[ETH ALERT]\033[0m \033[1m{tx_type}\033[0m {action_type} for \033[96m{wallet}\033[0m: \033[90m{tx_hash}\033[0m")
                await _safe_discord_send(ch, embed=embed, files=files)
                
    except Exception as e:
        print(f"\033[91m[ETH ERROR]\033[0m Failed alerting TX \033[90m{tx_hash}\033[0m: {e}")

import time


def _fresh_discord_files(files: Optional[List[discord.File]]) -> Optional[List[discord.File]]:
    """Build new File objects for each send — discord.py closes the file handle after one message."""
    if not files:
        return None
    out: List[discord.File] = []
    for f in files:
        fn = f.filename
        fp = getattr(f, "_fp", None) or getattr(f, "fp", None)
        path = getattr(fp, "name", None) if fp is not None else None
        if isinstance(path, str) and path and not path.startswith("<"):
            try:
                out.append(discord.File(path, filename=fn))
            except OSError:
                continue
    return out or None


_discord_send_lock = asyncio.Lock()
_discord_global_pause_until = 0.0


async def _safe_discord_send(ch, *, embed=None, content=None, view=None, files=None):
    """Send with 429-aware retry and shared global pause."""
    global _discord_global_pause_until
    max_retries = 4
    for attempt in range(max_retries):
        now = time_module.time()
        if _discord_global_pause_until > now:
            await asyncio.sleep(max(0.2, _discord_global_pause_until - now))
        try:
            kwargs = {}
            if content is not None:
                kwargs["content"] = content
            if embed is not None:
                kwargs["embed"] = embed
            if view is not None:
                kwargs["view"] = view
            if files:
                kwargs["files"] = _fresh_discord_files(files)
            return await ch.send(**kwargs)
        except discord.HTTPException as e:
            if int(getattr(e, "status", 0) or 0) != 429:
                raise
            retry_after = float(getattr(e, "retry_after", 0.0) or 0.0)
            if retry_after <= 0:
                retry_after = min(30.0, 1.5 * (2 ** attempt))
            async with _discord_send_lock:
                _discord_global_pause_until = max(
                    float(_discord_global_pause_until or 0.0),
                    time_module.time() + retry_after + 0.25,
                )
            print(
                f"\033[93m[DISCORD]\033[0m 429 rate limit hit; waiting {retry_after:.2f}s "
                f"(attempt {attempt + 1}/{max_retries})"
            )
            await asyncio.sleep(retry_after + 0.25)
    raise RuntimeError("Discord send failed after repeated 429 responses")


def _normalize_nft_media_url(url: Optional[str]) -> Optional[str]:
    """Return a Discord-embed-friendly http(s) URL; normalize ipfs:// to an HTTP gateway."""
    if not url or not isinstance(url, str):
        return None
    u = url.strip()
    if not u:
        return None
    if u.startswith("ipfs://"):
        tail = u.replace("ipfs://", "").lstrip("/")
        return f"https://cf-ipfs.com/ipfs/{tail}"
    if u.startswith("http://") or u.startswith("https://"):
        return u
    return None


def _embed_box(text: str, *, max_len: int = 988) -> str:
    """Discord multiline code block for a compact 'card / box' look."""
    t = (text or "").replace("```", "`\u200b``")
    if len(t) > max_len:
        t = t[: max_len - 1] + "…"
    return f"```{t}```"


# Wallet-tracker NFT card palette (buy / sell / neutral)
_WT_COLOR_BUY = 0x3DDC97
_WT_COLOR_SELL = 0xED4245
_WT_COLOR_NEUTRAL = 0x4E5058


def _format_eth_amount(amt: float) -> str:
    if amt <= 0:
        return "0"
    if amt >= 1:
        s = f"{amt:,.4f}"
    elif amt >= 0.0001:
        s = f"{amt:.4f}"
    else:
        s = f"{amt:.6f}"
    return s.rstrip("0").rstrip(".")


def _wallet_tracker_action_label(action_word: str) -> str:
    return {
        "BOUGHT": "Buy",
        "SOLD": "Sell",
        "MINTED": "Mint",
        "CLAIMED": "Claim",
        "TRANSFERRED": "Transfer",
    }.get(action_word, action_word.title())


def _wallet_tracker_action_display(action_word: str) -> str:
    """Past-tense label for the ⚡ Action field (Minted, Bought, Sold, …)."""
    return {
        "BOUGHT": "Bought",
        "SOLD": "Sold",
        "MINTED": "Minted",
        "CLAIMED": "Claimed",
        "TRANSFERRED": "Transfer",
    }.get(action_word, action_word.title())


def _wallet_tracker_price_compact(
    display_eth: float,
    price_source: str,
    *,
    is_floor: bool,
    action_word: str,
    source: str,
    floor_eth: float,
    usd_rate: float,
) -> str:
    """Single-line price for inline row (matches legacy Value field)."""
    detail = _wallet_tracker_price_field(
        display_eth,
        price_source,
        is_floor=is_floor,
        bulk_qty=None,
        action_word=action_word,
        source=source,
        floor_eth=floor_eth,
        usd_rate=usd_rate,
    )
    if "\n" not in detail:
        return detail
    lines = detail.split("\n")
    main = lines[0]
    usd_line = next((ln for ln in lines[1:] if ln.startswith("≈")), "")
    if usd_line:
        usd_bit = usd_line.replace("≈ ", "").replace(" USD", "")
        return f"{main} ({usd_bit})"
    return main


def _wallet_tracker_price_field(
    display_eth: float,
    price_source: str,
    *,
    is_floor: bool,
    bulk_qty: Optional[int],
    action_word: str,
    source: str,
    floor_eth: float,
    usd_rate: float,
) -> str:
    if price_source == "weth_logs":
        sym = "WETH"
    else:
        sym = "ETH"

    if display_eth <= 0 and source == "Mint" and floor_eth == 0:
        return "**FREE**"
    if display_eth <= 0 and action_word == "TRANSFERRED":
        return "—"

    if display_eth <= 0 and source == "Mint" and floor_eth > 0:
        amt = _format_eth_amount(floor_eth)
        lines = [f"**FREE**", f"floor ~{amt} ETH"]
        if usd_rate > 0:
            lines.append(f"≈ ${floor_eth * usd_rate:,.0f} USD")
        return "\n".join(lines)

    if display_eth <= 0:
        return "—"

    amt = _format_eth_amount(display_eth)
    prefix = "~" if is_floor else ""
    lines = [f"{prefix}**{amt} {sym}**"]
    if usd_rate > 0:
        lines.append(f"≈ ${display_eth * usd_rate:,.0f} USD")
    if is_floor and bulk_qty:
        lines.append(f"_(floor × {bulk_qty})_")
    elif is_floor:
        lines.append("_(floor est.)_")
    return "\n".join(lines)


def _wallet_tracker_token_ids_field(
    token_id: int,
    all_token_ids: Optional[List[int]],
    bulk_qty: Optional[int],
) -> str:
    if all_token_ids:
        shown = all_token_ids[:24]
        parts = [f"**#{tid}**" for tid in shown]
        line = " · ".join(parts)
        if len(all_token_ids) > 24:
            line += f"\n+{len(all_token_ids) - 24} more"
        return line
    return f"**#{token_id}**"


# In-memory cache for direct Alchemy token-image calls (bypasses stale module cache)
_eth_tracker_token_img_cache: Dict[str, tuple[str, float]] = {}
_ETH_TRACKER_TOKEN_IMG_TTL = 24 * 3600


async def _fetch_alchemy_token_image_direct(
    session: aiohttp.ClientSession, contract: str, token_id: int
) -> Optional[str]:
    """Fetch per-token image directly from Alchemy — lives in eth_tracker.py to avoid sys.modules cache issues."""
    key = (os.getenv("ALCHEMY_NFT_API_KEY") or "").strip()
    if not key:
        return None
    cache_key = f"{contract.lower()}:{int(token_id)}"
    now = time_module.time()
    cached = _eth_tracker_token_img_cache.get(cache_key)
    if cached and (now - cached[1]) < _ETH_TRACKER_TOKEN_IMG_TTL:
        return cached[0] or None
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
            resolved = (
                img.get("cachedUrl")
                or img.get("thumbnailUrl")
                or img.get("pngUrl")
                or img.get("originalUrl")
                or data.get("image")
                or data.get("image_url")
            )
            if resolved and isinstance(resolved, str) and resolved.strip().startswith("http"):
                _eth_tracker_token_img_cache[cache_key] = (resolved.strip(), now)
                print(f"\033[92m[IMG]\033[0m Direct Alchemy token image for {contract[:10]}... #{token_id}")
                return resolved.strip()
    except Exception as e:
        print(f"\033[91m[IMG]\033[0m Direct Alchemy error for {contract[:10]}... #{token_id}: {e}")
    return None


async def create_eth_nft_embed(
    action_type,
    wallet,
    contract,
    session,
    tx_hash,
    token_id,
    from_addr,
    to_addr,
    bulk_quantity: Optional[int] = None,
    all_token_ids: Optional[List[int]] = None,
):
    from trackers.eth_live_mints import get_contract_info, fetch_token_image

    # Enrichment toggles (reduce API usage / avoid 429s)
    enable_alchemy_nft = os.getenv("ENABLE_ALCHEMY_NFT", "1").strip().lower() not in ("0", "false", "no", "off")
    enable_floor = os.getenv("ENABLE_NFT_FLOOR", "1").strip().lower() not in ("0", "false", "no", "off")
    enable_socials = os.getenv("ENABLE_NFT_SOCIALS", "1").strip().lower() not in ("0", "false", "no", "off")
    
    wallet_label = tracked_eth_wallets.get(wallet.lower(), f"{wallet[:6]}...{wallet[-4:]}")
    if wallet_label.lower() == "batch import" or not wallet_label:
        wallet_label = f"{wallet[:6]}...{wallet[-4:]}"
        
    action_word = "TRANSFERRED"
    color = Color(_WT_COLOR_NEUTRAL)
    emoji = "🔄"
    
    from trackers.eth_address import ZERO_ADDRESS, normalize_eth_address

    if normalize_eth_address(from_addr) == ZERO_ADDRESS:
        action_word = "MINTED"
        color = Color(_WT_COLOR_BUY)
        emoji = "🟢"
    elif action_type == "Sent":
        action_word = "SOLD"
        color = Color(_WT_COLOR_SELL)
        emoji = "🔴"
    elif action_type == "Received":
        action_word = "BOUGHT"
        color = Color(_WT_COLOR_BUY)
        emoji = "🟢"
        
    bulk_qty = bulk_quantity if bulk_quantity and bulk_quantity > 1 else None

    loop = asyncio.get_event_loop()
    tx_value = 0
    tx_to = ""
    receipt = None
    if w3 is not None and w3.is_connected():
        try:
            tx = await loop.run_in_executor(None, w3.eth.get_transaction, tx_hash)
            receipt = await loop.run_in_executor(None, w3.eth.get_transaction_receipt, tx_hash)
            tx_value = tx.get("value", 0)
            tx_to = (tx.get("to") or "").lower()
        except Exception:
            tx_value = 0
            tx_to = ""
            receipt = None

    # --- Marketplace Detection (expanded for 2025/2026 contracts) ---
    SEAPORT_ADDRS = {
        "0x00000000000000adc04c56bf30ac9d3c0aaf14dc",  # Seaport 1.5
        "0x0000000000000068f116a894984e2db1123eb395",  # Seaport 1.6
    }
    BLUR_ADDRS = {
        "0x000000000000ad05ccc4f1004514d1ce0838c746",  # Blur Marketplace
        "0xb2ecfe4e4d61f8790bac794022beec10d9fa859f",  # Blur v2
        "0x39da41747a83aee658334415666f3ef92ceb12a7",  # Blur Blend
        "0x29469395eaf6f95920e59f858042f0e28d98a20b",  # Blur v3 (2025+)
    }
    
    source = "Contract"
    if tx_to in SEAPORT_ADDRS: source = "Seaport"
    elif tx_to in BLUR_ADDRS: source = "Blur"
    elif tx_to == "0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b": source = "Uniswap"
    elif normalize_eth_address(from_addr) == ZERO_ADDRESS:
        source = "Mint"
    
    # =====================================================================
    # 4-LAYER PRICE RESOLUTION (most reliable first)
    # =====================================================================
    price_eth = 0.0
    price_source = "none"

    # Bulk: total WETH across the whole tx (avoid per-token Alchemy understating)
    if bulk_qty and receipt:
        weth_bulk = _sum_weth_eth_from_receipt(receipt)
        if weth_bulk > 0:
            price_eth = weth_bulk
            price_source = "weth_logs"
    
    # --- LAYER 1: Alchemy getNFTSales (single-item sales only; bulk uses receipt total above) ---
    if enable_alchemy_nft and ALCHEMY_API_KEY and not bulk_qty:
        _buyer = to_addr if action_type == "Received" else ""
        _seller = from_addr if action_type == "Sent" else ""
        alchemy_price = await fetch_nft_sale_price(session, contract, token_id, tx_hash, buyer=_buyer, seller=_seller)
        if alchemy_price > 0:
            price_eth = alchemy_price
            price_source = "alchemy"
    
    # --- LAYER 2: Native tx.value (direct ETH mints/transfers) ---
    if price_eth == 0 and tx_value > 0:
        price_eth = tx_value / 1e18
        price_source = "tx.value"
    
    # --- LAYER 3: WETH / Blur Pool Transfer log parsing ---
    if price_eth == 0 and receipt:
        weth_from_logs = _sum_weth_eth_from_receipt(receipt)
        if weth_from_logs > 0:
            price_eth = weth_from_logs
            price_source = "weth_logs"
    
    # --- LAYER 4: Etherscan internal TX API (ETH movements in mints/sales) ---
    if price_eth == 0 and ETHSCAN_API_KEY:
        try:
            _ekey = f"&apikey={ETHSCAN_API_KEY}" if ETHSCAN_API_KEY else ""
            internal_url = f"https://api.etherscan.io/v2/api?chainid=1&module=account&action=txlistinternal&txhash={tx_hash}{_ekey}"
            async with session.get(internal_url, timeout=5) as r:
                if r.status == 200:
                    d = await r.json()
                    if d.get("status") == "1" and d.get("result"):
                        total = sum(int(itx.get("value", 0)) for itx in d["result"])
                        if total > 0:
                            price_eth = total / 1e18
                            price_source = "etherscan_internal"
        except:
            pass
    
    if getattr(config, "LOG_ETH_TX_PRICE_LINES", True):
        if price_source != "none":
            print(f"\033[92m[PRICE]\033[0m {price_eth:.4f} ETH via {price_source} for tx {tx_hash[:16]}...")
        else:
            print(f"\033[93m[PRICE]\033[0m Could not resolve price for tx {tx_hash[:16]}...")
    
    # --- Smart label correction ---
    # If price is 0 and no marketplace was detected, this is likely a direct
    # transfer (not a sale/purchase). Downgrade SOLD/BOUGHT -> TRANSFERRED.
    if price_eth == 0 and source == "Contract" and action_word in ("SOLD", "BOUGHT"):
        action_word = "TRANSFERRED"
        color = Color(_WT_COLOR_NEUTRAL)
        emoji = "🔄"
    # If Alchemy confirmed a marketplace sale, upgrade source label
    if price_source == "alchemy" and source == "Contract":
        source = "Marketplace"
            
    # NOTE: Avoid per-embed HTTP price calls. We maintain _cached_eth_usd via refresh_eth_usd_price().
    info = await get_contract_info(contract)
    from trackers.collection_image import fetch_collection_image, prepare_collection_thumbnail

    # Direct Alchemy fetch bypasses stale sys.modules cache from nftscan_live_feed.py import
    image_url = await _fetch_alchemy_token_image_direct(session, contract, token_id)
    if not image_url:
        try:
            from trackers.eth_live_mints import fetch_token_image
            image_url = await fetch_token_image(contract, token_id)
        except Exception:
            pass
    col_name = info.get("name")
    
    # Fetch floor price (optional; can be API-heavy)
    floor_eth = 0.0
    if enable_floor:
        floor_eth = await fetch_collection_floor_price(session, contract)
    
    alchemy_contract_data = None
    if ALCHEMY_API_KEY:
        try:
            url = f"https://eth-mainnet.g.alchemy.com/nft/v3/{ALCHEMY_API_KEY}/getContractMetadata"
            params = {"contractAddress": contract.lower()}
            async with session.get(url, params=params, timeout=5) as r:
                if r.status == 200:
                    alchemy_contract_data = await r.json()
        except Exception:
            pass

    if alchemy_contract_data and (not col_name or col_name == "Unknown"):
        c_name = alchemy_contract_data.get("name")
        if c_name:
            col_name = c_name

    if not col_name or col_name == "Unknown":
        # 2. Try OpenSea
        try:
            opensea_url = f"https://api.opensea.io/api/v2/chain/ethereum/contract/{contract.lower()}"
            headers = {"Accept": "application/json"}
            if os.getenv("OPENSEA_API_KEY"):
                headers["X-API-KEY"] = os.getenv("OPENSEA_API_KEY")
            async with session.get(opensea_url, headers=headers, timeout=5) as r:
                if r.status == 200:
                    data = await r.json()
                    c_name = data.get("collection", {}).get("name") or data.get("name")
                    if c_name:
                        col_name = c_name
        except Exception:
            pass

        # 3. Try Reservoir
        if not col_name or col_name == "Unknown":
            try:
                reservoir_url = f"https://api.reservoir.tools/collections/v7?id={contract.lower()}"
                async with session.get(reservoir_url, timeout=5) as r:
                    if r.status == 200:
                        data = await r.json()
                        collections = data.get("collections", [])
                        if collections:
                            c_name = collections[0].get("name")
                            if c_name:
                                col_name = c_name
            except Exception:
                pass

        if not col_name or col_name == "Unknown":
            col_name = "Collection"

    # Collection image: free chain (Alchemy → CoinGecko → NFTScan logo CDN → OpenSea → Reservoir)
    project_image_url = await fetch_collection_image(
        session,
        contract,
        token_id,
        alchemy_contract_data=alchemy_contract_data,
        token_image_url=image_url,
    )

    # Smart label correction for older collections
    if action_word == "MINTED" and floor_eth > 0:
        action_word = "CLAIMED"
        
    files: List[discord.File] = []
    wallet_profile = await fetch_wallet_profile(wallet, session)
    profile_img = str(wallet_profile.get("profile_image_url") or "").strip()
    avatar_url = await attach_wallet_pfp_for_embed(
        wallet, session, files, profile_image_url=profile_img
    )

    # ── Price resolution ─────────────────────────────────────────────────────
    display_eth = price_eth
    is_floor = False
    if price_eth == 0 and floor_eth > 0:
        if bulk_qty:
            display_eth = floor_eth * bulk_qty
            is_floor = True
        else:
            display_eth = floor_eth
            is_floor = True
    profile_name = str(wallet_profile.get("name") or "").strip()
    display_name = profile_name if profile_name and profile_name.lower() != wallet_label.lower() else wallet_label
    col_display = (col_name or "Collection").strip()
    thumb_url = prepare_collection_thumbnail(image_url, project_image_url, files)

    if getattr(config, "WALLET_TRACKER_LUXURY_MODE", True):
        from trackers.wallet_tracker_embed import (
            NftWalletAlertContext,
            build_premium_nft_wallet_embed_async,
            resolve_trader_name,
        )

        os_profile_url = str(wallet_profile.get("opensea_url") or "").strip()
        trader_x = wallet_database.get_x_url(wallet) or ""
        trader_name = resolve_trader_name(wallet_label, profile_name)

        alert_ctx = NftWalletAlertContext(
            action_word=action_word,
            wallet=wallet,
            wallet_label=wallet_label,
            display_name=display_name,
            contract=contract,
            collection_name=col_display,
            token_id=token_id,
            tx_hash=tx_hash,
            display_eth=display_eth,
            price_eth=price_eth,
            price_source=price_source,
            is_floor_estimate=is_floor,
            floor_eth=floor_eth,
            marketplace_source=source,
            eth_usd=_cached_eth_usd,
            from_zero_address=normalize_eth_address(from_addr) == ZERO_ADDRESS,
            portfolio_usd=float(wallet_profile.get("portfolio_usd") or 0.0),
            is_verified=bool(wallet_profile.get("is_verified")),
            joined_iso=str(wallet_profile.get("joined") or ""),
            avatar_url=avatar_url,
            thumb_url=thumb_url or "",
            bulk_qty=bulk_qty,
            all_token_ids=all_token_ids,
            chain="Ethereum",
            trader_name=trader_name,
            x_url=trader_x,
            opensea_profile_url=os_profile_url,
        )
        embed = await build_premium_nft_wallet_embed_async(alert_ctx, session)
        return embed, None, None, files

    opensea_asset_url = f"https://opensea.io/assets/ethereum/{contract}/{token_id}"
    tx_url = f"https://etherscan.io/tx/{tx_hash}"
    wallet_url = f"https://etherscan.io/address/{wallet}"
    action_label = _wallet_tracker_action_label(action_word)
    embed = discord.Embed(url=opensea_asset_url, color=color, timestamp=datetime.now(timezone.utc))
    embed.set_author(name=action_label, icon_url=avatar_url, url=wallet_url)
    embed.add_field(name="👤 Wallet", value=f"[**{display_name}**]({wallet_url})", inline=True)
    embed.add_field(name="🖼️ Collection", value=f"[**{col_display}**]({opensea_asset_url})", inline=True)
    embed.add_field(name="⚡ Action", value=f"**{_wallet_tracker_action_display(action_word)}**", inline=True)
    _embed_section_spacer(embed)
    qty = bulk_qty if bulk_qty else 1
    price_compact = _wallet_tracker_price_compact(
        display_eth, price_source, is_floor=is_floor, action_word=action_word,
        source=source, floor_eth=floor_eth, usd_rate=_cached_eth_usd,
    )
    embed.add_field(name="🖼️ Amount", value=f"**{qty}** NFT{'s' if qty != 1 else ''}", inline=True)
    embed.add_field(name="🕒 Time", value=f"<t:{int(time.time())}:R>", inline=True)
    _embed_section_spacer(embed)
    embed.add_field(name="💰 Value", value=price_compact, inline=True)
    embed.add_field(
        name="🔢 Token IDs",
        value=_wallet_tracker_token_ids_field(token_id, all_token_ids, bulk_qty),
        inline=True,
    )
    _embed_section_spacer(embed)
    embed.add_field(
        name="🔗 Links",
        value=f"[OpenSea]({opensea_asset_url}) · [Etherscan TX]({tx_url}) · [Contract](https://etherscan.io/address/{contract})",
        inline=False,
    )
    embed.set_thumbnail(url=thumb_url)
    embed.set_footer(text=f"{_BRAND_NAME} · Wallet Tracker")
    return embed, None, None, files

async def create_eth_embed(action: str, wallet: str, contract: str, amount: float, session: aiohttp.ClientSession, tx_hash: str, is_nft: bool = False, token_id: int = None) -> Embed:
    """Create ETH Embed with Logo and Socials"""
    
    # 1. Fetch Info
    info = {
        "name": "Unknown NFT" if is_nft else "Unknown",
        "symbol": "NFT" if is_nft else "???",
        "price_usd": 0,
        "mcap": 0,
        "logo": None,
        "url": f"https://opensea.io/assets/ethereum/{contract}/{token_id}" if is_nft else f"https://etherscan.io/address/{contract}",
        "socials": ""
    }
    
    if not is_nft:
        try:
            url = f"https://api.dexscreener.com/tokens/v1/ethereum/{contract}"
            async with session.get(url, timeout=5) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data and len(data) > 0:
                        pair = data[0]
                        base = pair.get("baseToken", {})
                        info_data = pair.get("info", {})
                        
                        # Socials
                        links = []
                        for s in info_data.get("socials", []):
                            pt = s.get("type", "").lower()
                            u = s.get("url", "")
                            if "twitter" in pt: links.append(f"[🐦]({u})")
                            elif "telegram" in pt: links.append(f"[💬]({u})")
                            elif "discord" in pt: links.append(f"[👾]({u})")
                        for w in info_data.get("websites", []):
                            links.append(f"[🌐]({w.get('url', '')})")
                        
                        info.update({
                            "name": base.get("name", "Unknown"),
                            "symbol": base.get("symbol", "???"),
                            "price_usd": float(pair.get("priceUsd", 0) or 0),
                            "mcap": pair.get("marketCap", 0) or pair.get("fdv", 0) or 0,
                            "logo": info_data.get("imageUrl"),
                            "socials": " | ".join(links)
                        })
        except Exception as e:
            print(f"DexScreener ETH Error: {e}")

    # 2. Build Embed
    
    color = Color.green() if "Received" in action or "Buy" in action else Color.red()
    emoji = "🟢" if color == Color.green() else "🔴"
    
    wallet_label = tracked_eth_wallets.get(wallet.lower(), f"{wallet[:6]}...{wallet[-4:]}")
    
    embed = Embed(
        title=f"{emoji} {action} | {wallet_label}",
        color=color,
        timestamp=datetime.utcnow(),
        url=f"https://etherscan.io/tx/{tx_hash}"
    )
    
    files = []
    
    # Fetch Trader PFP for Author Icon
    avatar_url, pfp_local = await fetch_wallet_avatar(wallet, session)
    if pfp_local:
        files.append(discord.File(pfp_local, filename=f"pfp_{wallet.lower()}.{pfp_local.split('.')[-1]}"))
        
    embed.set_author(name=f"{_BRAND_NAME} · ETH", icon_url=avatar_url)
    
    if is_nft:
        embed.add_field(name="Collection", value=f"[`{contract[:6]}...`]({info['url']})", inline=True)
        embed.add_field(name="Token ID", value=f"**#{token_id}**", inline=True)
        embed.set_thumbnail(url="https://opensea.io/static/images/logos/opensea.svg") # Fallback logo
    else:
        # Pricing
        price = info["price_usd"]
        value_usd = amount * price
        
        amt_str = f"{amount:,.4f} {info['symbol']}"
        val_str = f"(${value_usd:,.2f})" if price > 0 else ""
        
        embed.add_field(name="Token", value=f"**{info['name']}**", inline=True)
        embed.add_field(name="Amount", value=f"{amt_str}\n{val_str}", inline=True)
        
        if info["mcap"] > 0:
            if info["mcap"] >= 1_000_000: mcap_str = f"${info['mcap']/1_000_000:.1f}M"
            elif info["mcap"] >= 1_000: mcap_str = f"${info['mcap']/1_000:.1f}K"
            else: mcap_str = f"${info['mcap']:.2f}"
            embed.add_field(name="Market Cap", value=f"**{mcap_str}**", inline=True)
            
    # Thumbnail with custom fallback
    local_fallback = "83bc8b88cf6bc4b4e04d153a418cde62.jpg"
    if info["logo"] and isinstance(info["logo"], str) and info["logo"].startswith("http"):
        embed.set_thumbnail(url=info["logo"])
    else:
        if os.path.exists(local_fallback):
            files.append(discord.File(local_fallback, filename="fallback.jpg"))
            embed.set_thumbnail(url="attachment://fallback.jpg")
        else:
            embed.set_thumbnail(url="https://placehold.co/400x400/202025/AAAAAA.png?text=%E2%97%86")

    embed.add_field(name="Contract", value=f"```\n{contract}\n```", inline=False)
    
    # Tracked Wallet (label + optional X)
    tw_val = f"**{wallet_label}**"
    _xw = wallet_database.get_x_url(wallet)
    if _xw:
        tw_val += f" · [X]({_xw})"
    embed.add_field(name="Tracked Wallet", value=tw_val, inline=False)
    
    # Socials in Field
    if info["socials"]:
        embed.add_field(name="Socials", value=info["socials"], inline=False)
        
    embed.set_footer(text=f"{_BRAND_NAME} · ETH · {datetime.utcnow().strftime('%H:%M:%S UTC')}")
    
    return embed, files

async def start_eth_tracking(wallet: str, label: str):
    tracked_eth_wallets[wallet.lower()] = label

