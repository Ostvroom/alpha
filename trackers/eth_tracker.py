"""
Ethereum Tracker - ERC20 & NFT (ERC721/1155)
"""
import asyncio
import json
import os
import re
import time as time_module
from datetime import datetime
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

# Velcor3 — wallet tracker embed branding (matches token / mint overview style)
_BRAND_NAME = (
    (os.getenv("VELCOR3_BRAND_NAME") or os.getenv("NERDS_BRAND_NAME") or "Velcor3").strip()
    or "Velcor3"
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


def _throttled_etherscan_warn(kind: str, message: str) -> None:
    now = time_module.time()
    prev = _etherscan_warn_at.get(kind, 0.0)
    if now - prev < _ETHERSCAN_WARN_COOLDOWN_S:
        return
    _etherscan_warn_at[kind] = now
    print(f"\033[91m[ETHERSCAN]\033[0m {message}")


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
                        print(f"\033[90m[PRICE]\033[0m ETH/USD updated: ${_cached_eth_usd:,.2f}")
        except Exception as e:
            print(f"\033[93m[PRICE WARN]\033[0m ETH/USD refresh failed: {e}")
        await asyncio.sleep(60)

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

async def check_eth_block(client, token_channel_id: int, nft_channel_id: int):
    """Refined API Poller replacing Web3 getLogs"""
    global startup_time
    
    async with aiohttp.ClientSession() as session:
        enable_erc20 = os.getenv("ENABLE_ETH_ERC20", "0").strip().lower() not in ("0", "false", "no", "off")
        enable_erc721 = os.getenv("ENABLE_ETH_NFT", "1").strip().lower() not in ("0", "false", "no", "off")
        try:
            etherscan_offset = max(1, int(os.getenv("ETHERSCAN_OFFSET", "5")))
        except Exception:
            etherscan_offset = 5
        try:
            scan_interval_s = max(10, int(os.getenv("ETH_SCAN_INTERVAL", "30")))
        except Exception:
            scan_interval_s = 30
        if not enable_erc20:
            print("[ETH] ERC20 tracking disabled via ENABLE_ETH_ERC20=0")
        if not enable_erc721:
            print("[ETH] NFT tracking disabled via ENABLE_ETH_NFT=0")
        print(f"[ETH] Etherscan offset={etherscan_offset}, scan_interval={scan_interval_s}s")
        if not ETHSCAN_API_KEY:
            print(
                "\033[93m[ETHERSCAN]\033[0m ETHSCAN_API_KEY is missing — v2 account APIs return no "
                "transactions. Create a key at https://etherscan.io/myapikey and set ETHSCAN_API_KEY in .env."
            )
        else:
            print(f"\033[92m[ETHERSCAN]\033[0m API key loaded ({len(ETHSCAN_API_KEY)} characters).")

        while True:
            if not tracked_eth_wallets:
                await asyncio.sleep(10)
                continue

            for wallet, label in list(tracked_eth_wallets.items()):
                try:
                    _ekey = f"&apikey={ETHSCAN_API_KEY}" if ETHSCAN_API_KEY else ""
                    erc20_url = f"https://api.etherscan.io/v2/api?chainid=1&module=account&action=tokentx&address={wallet}&page=1&offset={etherscan_offset}&sort=desc{_ekey}"
                    erc721_url = f"https://api.etherscan.io/v2/api?chainid=1&module=account&action=tokennfttx&address={wallet}&page=1&offset={etherscan_offset}&sort=desc{_ekey}"
                    
                    # Fetch ERC20
                    if enable_erc20 and token_channel_id:
                        async with session.get(erc20_url) as r:
                            if r.status != 200:
                                _throttled_etherscan_warn(
                                    "http20",
                                    f"tokentx HTTP {r.status} for {wallet[:10]}… — no ERC20 data.",
                                )
                            else:
                                try:
                                    d = await r.json()
                                except Exception as ex:
                                    _throttled_etherscan_warn("json20", f"tokentx invalid JSON: {ex}")
                                    d = None
                                if d is not None:
                                    rows, err = _etherscan_account_tx_rows(d)
                                    if err:
                                        _throttled_etherscan_warn(
                                            "tokentx",
                                            f"tokentx ({wallet[:10]}…): {err}",
                                        )
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
                        async with session.get(erc721_url) as r:
                            if r.status != 200:
                                _throttled_etherscan_warn(
                                    "http721",
                                    f"tokennfttx HTTP {r.status} for {wallet[:10]}… — no NFT data.",
                                )
                            else:
                                try:
                                    d = await r.json()
                                except Exception as ex:
                                    _throttled_etherscan_warn("json721", f"tokennfttx invalid JSON: {ex}")
                                    d = None
                                if d is not None:
                                    rows, err = _etherscan_account_tx_rows(d)
                                    if err:
                                        _throttled_etherscan_warn(
                                            "tokennfttx",
                                            f"tokennfttx ({wallet[:10]}…): {err}",
                                        )
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
                
                await asyncio.sleep(0.4) # Rate limit protection (Etherscan 5 req/s)
                
            print(f"\033[94m[ETH]\033[0m Scan complete for \033[93m{len(tracked_eth_wallets)}\033[0m wallets. Waiting...")
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
        target_ids = str(nft_channel_id).split(",") if nft_channel_id else str(token_channel_id).split(",")
        for t_id in target_ids:
            if not t_id.strip():
                continue
            chan_id = int(t_id.strip())
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
            if files:
                await ch.send(
                    content=content,
                    embed=embed,
                    view=view,
                    files=_fresh_discord_files(files),
                )
            else:
                await ch.send(content=content, embed=embed, view=view)
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
            target_ids = str(nft_channel_id).split(',') if nft_channel_id else str(token_channel_id).split(',')
            for t_id in target_ids:
                if not t_id.strip(): continue
                chan_id = int(t_id.strip())
                ch = client.get_channel(chan_id)
                if ch is None:
                    try:
                        ch = await client.fetch_channel(chan_id)
                    except Exception as e:
                        print(f"\033[91m[ETH ERROR]\033[0m Channel {chan_id} not found / no access: {e}")
                        continue
                print(f"\033[92m[ETH ALERT]\033[0m \033[1m{tx_type}\033[0m {action_type} for \033[96m{wallet}\033[0m: \033[90m{tx_hash}\033[0m")
                if files:
                    await ch.send(
                        content=content,
                        embed=embed,
                        view=view,
                        files=_fresh_discord_files(files),
                    )
                else:
                    await ch.send(content=content, embed=embed, view=view)
                
        else: # ERC20
            value_raw = int(tx["value"])
            decimals = int(tx["tokenDecimal"])
            amount = value_raw / (10 ** decimals) if decimals > 0 else value_raw
            action_type = "Received" if to_addr.lower() == wallet.lower() else "Sent"
            
            embed, files = await create_eth_embed(
                action=f"{action_type} Token", wallet=wallet, contract=contract,
                amount=amount, session=session, tx_hash=tx_hash
            )
            target_ids = str(token_channel_id).split(',') if token_channel_id else []
            for t_id in target_ids:
                if not t_id.strip(): continue
                chan_id = int(t_id.strip())
                ch = client.get_channel(chan_id)
                if ch is None:
                    try:
                        ch = await client.fetch_channel(chan_id)
                    except Exception as e:
                        print(f"\033[91m[ETH ERROR]\033[0m Channel {chan_id} not found / no access: {e}")
                        continue
                print(f"\033[92m[ETH ALERT]\033[0m \033[1m{tx_type}\033[0m {action_type} for \033[96m{wallet}\033[0m: \033[90m{tx_hash}\033[0m")
                if files:
                    await ch.send(embed=embed, files=_fresh_discord_files(files))
                else:
                    await ch.send(embed=embed)
                
    except Exception as e:
        print(f"\033[91m[ETH ERROR]\033[0m Failed alerting TX \033[90m{tx_hash}\033[0m: {e}")

import discord
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
    color = Color(_EMBED_NEUTRAL)
    emoji = "🔄"
    
    if from_addr.lower() == "0x0000000000000000000000000000000000000000":
        action_word = "MINTED"
        color = Color.green()
        emoji = "🟢"
    elif action_type == "Sent":
        action_word = "SOLD"
        color = Color.red()
        emoji = "🔴"
    elif action_type == "Received":
        action_word = "BOUGHT"
        color = Color.green()
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
    elif from_addr.lower() == "0x0000000000000000000000000000000000000000": source = "Mint"
    
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
    
    if price_source != "none":
        print(f"\033[92m[PRICE]\033[0m {price_eth:.4f} ETH via {price_source} for tx {tx_hash[:16]}...")
    else:
        print(f"\033[93m[PRICE]\033[0m Could not resolve price for tx {tx_hash[:16]}...")
    
    # --- Smart label correction ---
    # If price is 0 and no marketplace was detected, this is likely a direct
    # transfer (not a sale/purchase). Downgrade SOLD/BOUGHT -> TRANSFERRED.
    if price_eth == 0 and source == "Contract" and action_word in ("SOLD", "BOUGHT"):
        action_word = "TRANSFERRED"
        color = Color(_EMBED_NEUTRAL)
        emoji = "🔄"
    # If Alchemy confirmed a marketplace sale, upgrade source label
    if price_source == "alchemy" and source == "Contract":
        source = "Marketplace"
            
    # NOTE: Avoid per-embed HTTP price calls. We maintain _cached_eth_usd via refresh_eth_usd_price().
    usd_price_str = ""
    if price_eth >= 0 and _cached_eth_usd > 0:
        usd_price = price_eth * _cached_eth_usd
        usd_price_str = f" (${usd_price:,.0f})" if usd_price > 0 else " ($0)"
        
    info = await get_contract_info(contract)
    image_url = await fetch_token_image(contract, token_id)
    col_name = info.get("name")
    
    # Fetch floor price (optional; can be API-heavy)
    floor_eth = 0.0
    if enable_floor:
        floor_eth = await fetch_collection_floor_price(session, contract)
    
    if not col_name or col_name == "Unknown":
        # 1. Try Alchemy
        if ALCHEMY_API_KEY:
            try:
                url = f"https://eth-mainnet.g.alchemy.com/nft/v3/{ALCHEMY_API_KEY}/getContractMetadata"
                params = {"contractAddress": contract.lower()}
                async with session.get(url, params=params, timeout=5) as r:
                    if r.status == 200:
                        data = await r.json()
                        c_name = data.get("name")
                        if c_name:
                            col_name = c_name
            except: pass

        # 2. Try OpenSea
        if not col_name or col_name == "Unknown":
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
            except: pass
            
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
            except: pass

        if not col_name or col_name == "Unknown":
            col_name = "Collection"
            
    # Smart label correction for older collections
    if action_word == "MINTED" and floor_eth > 0:
        action_word = "CLAIMED"
        
    files = []
    avatar_url, pfp_local = await fetch_wallet_avatar(wallet, session)
    if pfp_local:
        files.append(discord.File(pfp_local, filename=f"pfp_{wallet.lower()}.{pfp_local.split('.')[-1]}"))

    bulk_suffix = f" ×{bulk_qty}" if bulk_qty else ""

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
    usd_str = f"  (${display_eth * _cached_eth_usd:,.0f})" if display_eth > 0 and _cached_eth_usd > 0 else ""

    if price_eth == 0 and source == "Mint" and floor_eth == 0:
        value_str = "FREE 🆓"
        price_desc = "FREE 🆓"
    elif price_eth == 0 and source == "Mint" and floor_eth > 0:
        value_str = f"FREE 🆓  ·  floor **{floor_eth:,.4f} ETH**{usd_str}"
        price_desc = "FREE 🆓"
    elif is_floor and bulk_qty:
        value_str = f"~**{display_eth:,.4f} ETH**{usd_str}  *(floor × {bulk_qty})*"
        price_desc = f"~**{display_eth:,.4f} ETH** (floor × {bulk_qty})"
    elif is_floor:
        value_str = f"~**{display_eth:,.4f} ETH**{usd_str}  *(floor)*"
        price_desc = f"~**{display_eth:,.4f} ETH** (floor)"
    elif price_eth == 0 and action_word == "TRANSFERRED":
        value_str = "—"
        price_desc = ""
    elif bulk_qty and price_eth > 0:
        value_str = f"**{display_eth:,.4f} ETH**{usd_str}  *(×{bulk_qty} total)*"
        price_desc = f"**{display_eth:,.4f} ETH** (×{bulk_qty})"
    else:
        value_str = f"**{display_eth:,.4f} ETH**{usd_str}"
        price_desc = f"**{display_eth:,.4f} ETH**"

    # ── Embed ─────────────────────────────────────────────────────────────────
    embed = discord.Embed(color=color)
    embed.set_author(
        name=f"{emoji} {wallet_label} · {action_word} · {col_name}{bulk_suffix}",
        icon_url=avatar_url,
    )
    if image_url:
        embed.set_thumbnail(url=image_url)

    time_str = f"<t:{int(time.time())}:R>"

    # ── Description ───────────────────────────────────────────────────────────
    bulk_desc = f" ×**{bulk_qty}**" if bulk_qty else ""
    if action_word == "TRANSFERRED" and price_eth == 0:
        embed.description = f"**{wallet_label}** transferred **{col_name}**{bulk_desc}"
    elif price_desc:
        embed.description = f"**{wallet_label}** {action_word.lower()} **{col_name}**{bulk_desc} for {price_desc}"
    else:
        embed.description = f"**{wallet_label}** {action_word.lower()} **{col_name}**{bulk_desc}"

    # ── Row 1: Trader · Collection · Action ───────────────────────────────────
    x_prof = wallet_database.get_x_url(wallet)
    trader_val = f"[{wallet_label}](https://etherscan.io/address/{wallet})"
    if x_prof:
        trader_val += f"  ·  [𝕏]({x_prof})"
    via = f"  via **{source}**" if source != "Contract" else ""
    embed.add_field(name="👤 Trader", value=trader_val, inline=True)
    embed.add_field(name="🖼 Collection", value=f"[{col_name}](https://opensea.io/assets/ethereum/{contract}/{token_id})", inline=True)
    embed.add_field(name="⚡ Action", value=f"**{action_word.capitalize()}**{via}", inline=True)

    # ── Row 2: Value · Time ────────────────────────────────────────────────────
    embed.add_field(name="💰 Value", value=value_str, inline=True)
    embed.add_field(name="🕒 Time", value=time_str, inline=True)
    embed.add_field(name="\u200b", value="\u200b", inline=True)

    # ── Bulk token IDs ────────────────────────────────────────────────────────
    if bulk_qty and all_token_ids:
        ids_preview = ", ".join(f"#{tid}" for tid in all_token_ids[:15])
        if len(all_token_ids) > 15:
            ids_preview += f"  +{len(all_token_ids) - 15} more"
        embed.add_field(name=f"🛒 Token IDs  ×{bulk_qty}", value=ids_preview[:1024], inline=False)

    # ── Socials & Links ───────────────────────────────────────────────────────
    socials_dict = {}
    if enable_socials:
        socials_dict = await fetch_eth_nft_socials(contract, session)

    opensea_url = f"https://opensea.io/assets/ethereum/{contract}/{token_id}"
    tx_url = f"https://etherscan.io/tx/{tx_hash}"
    contract_url = f"https://etherscan.io/address/{contract}"
    links = [f"[OpenSea]({opensea_url})", f"[Wallet](https://etherscan.io/address/{wallet})"]
    _x_prof = wallet_database.get_x_url(wallet)
    if _x_prof:
        links.append(f"[Trader 𝕏]({_x_prof})")
    links += [f"[TX]({tx_url})", f"[Contract]({contract_url})"]
    if socials_dict.get("twitter"):
        links.append(f"[Collection 𝕏]({socials_dict['twitter']})")
    if socials_dict.get("discord"):
        links.append(f"[Discord]({socials_dict['discord']})")
    if socials_dict.get("website"):
        links.append(f"[Website]({socials_dict['website']})")
    embed.add_field(name="🔗 Links", value="  ·  ".join(links), inline=False)

    embed.set_footer(
        text=f"{_BRAND_NAME} · ETH NFTs · {datetime.utcnow().strftime('%I:%M %p')} UTC",
    )
    content_tail = f" ×{bulk_qty}" if bulk_qty else ""
    return embed, f"**{wallet_label}** {action_word.lower()} **{col_name}**{content_tail}", None, files

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

