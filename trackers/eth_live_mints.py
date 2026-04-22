"""
Ethereum Live Mints Tracker
Polls the blockchain for Transfer events from address(0) to detect live minting activity.
"""
import asyncio
import time
import os
from collections import defaultdict
from dataclasses import replace
from typing import Set, Dict, List
from web3 import Web3
from web3.exceptions import ContractLogicError
import aiohttp
from discord.ext.commands import Bot

from trackers.mint_sources import ActiveMint, fetch_waypoint_collection
from trackers.active_mints_tracker import (
    build_active_mint_embed,
    build_radar_embed,
    should_alert_radar,
    should_alert_trending,
    mark_alerted,
)

# Web3 Setup
# This tracker is "global live mints" polling (heavy `eth_getLogs`).
# Some public RPCs restrict `eth_getLogs` without an `address` filter (exactly what we do here),
# so prefer a provider that supports broad log queries (or a paid/dedicated node).
def _build_eth_live_mints_rpc_urls() -> List[str]:
    raw = (os.getenv("ETHEREUM_MINT_RPC_URLS") or "").strip()
    if raw:
        urls = [u.strip() for u in raw.split(",") if u.strip()]
    else:
        primary = (os.getenv("ETHEREUM_RPC_URL") or "").strip()
        urls = [primary] if primary else []

    # If user explicitly provided ETHEREUM_MINT_RPC_URLS, trust the list as-is.
    # Otherwise (fallback to ETHEREUM_RPC_URL), avoid accidentally hammering Alchemy defaults.
    if not raw:
        urls = [u for u in urls if u and ("alchemy.com" not in u.lower())]

    if not urls:
        raise RuntimeError(
            "[EthLiveMints] No usable RPC URLs configured.\n"
            "- Set ETHEREUM_MINT_RPC_URLS (comma-separated) to endpoints that allow broad eth_getLogs, or\n"
            "- Set ETHEREUM_RPC_URL to a non-Alchemy endpoint.\n"
            "Note: Alchemy is only auto-excluded when falling back to ETHEREUM_RPC_URL; "
            "it is allowed if you set ETHEREUM_MINT_RPC_URLS explicitly."
        )
    return urls


try:
    RPC_URLS = _build_eth_live_mints_rpc_urls()
except Exception as e:
    # Do not crash the whole bot at import-time if mint RPCs aren't configured.
    RPC_URLS = []
    print(str(e))

_rpc_idx = 0
w3 = Web3(Web3.HTTPProvider(RPC_URLS[_rpc_idx])) if RPC_URLS else None


def _web3_for_reads():
    """
    Web3 used for light contract reads (name, tokenURI) from wallet-tracker embeds.
    Prefer this module's RPC pool; if live-mints RPCs are not configured, reuse eth_tracker's connection.
    """
    global w3
    if w3 is not None:
        try:
            if w3.is_connected():
                return w3
        except Exception:
            pass
    try:
        import trackers.eth_tracker as et

        ww = getattr(et, "w3", None)
        if ww is not None:
            try:
                if ww.is_connected():
                    return ww
            except Exception:
                pass
    except Exception:
        pass
    return None


# Throttle identical RPC error spam (same issue every ~5s loop)
_eth_mint_log_throttle: Dict[str, tuple[float, int]] = {}


def _throttled_eth_mint_log(key: str, line: str, interval_sec: float = 50.0) -> None:
    now = time.time()
    if key in _eth_mint_log_throttle:
        last_t, n = _eth_mint_log_throttle[key]
        if now - last_t < interval_sec:
            _eth_mint_log_throttle[key] = (last_t, n + 1)
            return
        if n > 1:
            print(f"[EthLiveMints] … suppressed {n - 1} repeat(s): {key[:100]}")
    _eth_mint_log_throttle[key] = (now, 1)
    print(line)


def _rpc_log_url(url: str) -> str:
    """Hide API key segments in RPC URLs for logs (Alchemy / Ankr path keys, etc.)."""
    if not url:
        return url
    if "/v2/" in url:
        pre, _, _rest = url.partition("/v2/")
        key_part = _rest.split("/")[0].split("?")[0]
        if len(key_part) > 8:
            return f"{pre}/v2/***"
    if "rpc.ankr.com/eth/" in url:
        base = "https://rpc.ankr.com/eth/"
        if url.startswith(base) and len(url) > len(base) + 5:
            return base + "***"
    return url


def _switch_rpc(reason: str = "") -> str:
    """Rotate to next RPC URL and rebuild Web3 provider."""
    global w3, _rpc_idx
    if not RPC_URLS:
        return ""
    _rpc_idx = (_rpc_idx + 1) % len(RPC_URLS)
    url = RPC_URLS[_rpc_idx]
    w3 = Web3(Web3.HTTPProvider(url))
    safe = _rpc_log_url(url)
    if reason:
        print(f"[EthLiveMints] Switched RPC -> {safe} ({reason})")
    else:
        print(f"[EthLiveMints] Switched RPC -> {safe}")
    return url

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
TRANSFER_SINGLE_TOPIC = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
NULL_ADDRESS_TOPIC = "0x0000000000000000000000000000000000000000000000000000000000000000"

ERC721_ABI = [
    {"constant": True, "inputs": [], "name": "name", "outputs": [{"name": "", "type": "string"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "totalSupply", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "maxSupply", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "MAX_SUPPLY", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
    {"inputs": [{"internalType": "uint256", "name": "tokenId", "type": "uint256"}], "name": "tokenURI", "outputs": [{"internalType": "string", "name": "", "type": "string"}], "stateMutability": "view", "type": "function"},
    {"constant": True, "inputs": [], "name": "mintPrice", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "price", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "cost", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "PRICE", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "MINT_PRICE", "outputs": [{"name": "", "type": "uint256"}], "type": "function"}
]

# Track mints: contract_address -> list of timestamps
mint_history: Dict[str, List[float]] = defaultdict(list)
# Track minters: contract_address -> set of unique addresses
minters_history: Dict[str, Set[str]] = defaultdict(set)
# Track last known token ID for accurate tokenURI fetching
last_token_id: Dict[str, int] = {}
# Track last transaction hash to fallback on exact msg.value cost
last_tx_hash: Dict[str, str] = {}

MINT_THRESHOLD = 20  # Number of mints required to trigger an alert
TIME_WINDOW = 300    # 5 minutes in seconds

async def get_contract_info(contract_address: str) -> dict:
    """Fetch name, symbol, and total supply from the contract."""
    w3r = _web3_for_reads()
    checksum_addr = Web3.to_checksum_address(contract_address)
    if w3r is None:
        return {"name": "Unknown", "symbol": "???", "totalSupply": 0, "maxSupply": 0, "mintPrice": "Unknown"}
    contract = w3r.eth.contract(address=checksum_addr, abi=ERC721_ABI)
    
    info = {"name": "Unknown", "symbol": "???", "totalSupply": 0, "maxSupply": 0, "mintPrice": "Unknown"}
    loop = asyncio.get_event_loop()
    
    try: info["name"] = await loop.run_in_executor(None, contract.functions.name().call)
    except: pass
    try: info["symbol"] = await loop.run_in_executor(None, contract.functions.symbol().call)
    except: pass
    try: info["totalSupply"] = await loop.run_in_executor(None, contract.functions.totalSupply().call)
    except: pass

    try: info["maxSupply"] = await loop.run_in_executor(None, contract.functions.maxSupply().call)
    except:
        try: info["maxSupply"] = await loop.run_in_executor(None, contract.functions.MAX_SUPPLY().call)
        except: pass

    price_wei = None
    try: price_wei = await loop.run_in_executor(None, contract.functions.mintPrice().call)
    except:
        try: price_wei = await loop.run_in_executor(None, contract.functions.price().call)
        except:
            try: price_wei = await loop.run_in_executor(None, contract.functions.cost().call)
            except:
                try: price_wei = await loop.run_in_executor(None, contract.functions.PRICE().call)
                except:
                    try: price_wei = await loop.run_in_executor(None, contract.functions.MINT_PRICE().call)
                    except: pass
                    
    # Ultimate Fallback: Scrape the exact value from the transaction if ABI calls fail
    if price_wei is None and contract_address in last_tx_hash:
        try:
            tx_data = await loop.run_in_executor(None, w3r.eth.get_transaction, last_tx_hash[contract_address])
            if tx_data and "value" in tx_data:
                price_wei = tx_data["value"]
        except Exception as e:
            pass
            
    if price_wei is not None:
        info["mintPrice"] = "Free" if price_wei == 0 else f"{price_wei / 1e18:.4f} ETH"
            
    return info

IPFS_GATEWAYS = [
    "https://ipfs.io/ipfs/",
    "https://dweb.link/ipfs/",
    "https://cf-ipfs.com/ipfs/",
    "https://gateway.pinata.cloud/ipfs/"
]

async def fetch_ipfs_json(session: aiohttp.ClientSession, ipfs_hash: str) -> dict:
    """Race multiple IPFS gateways to get the JSON metadata instantly."""
    hash_only = ipfs_hash.replace("ipfs://", "")
    
    async def _fetch(gw_url):
        try:
            async with session.get(gw_url + hash_only, timeout=5) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    if isinstance(data, dict):
                        return data
        except:
            pass
        return None

    tasks = [_fetch(gw) for gw in IPFS_GATEWAYS]
    for coro in asyncio.as_completed(tasks):
        res = await coro
        if res is not None:
            # We got a valid response, cancel the others by throwing away the references
            return res
            
    return None

async def fetch_external_url(contract_address: str) -> str:
    """Attempt to natively fetch the tokenURI(1) payload from the live Ethereum contract to grab its external_url."""
    try:
        w3r = _web3_for_reads()
        if w3r is None:
            return None
        checksum_addr = Web3.to_checksum_address(contract_address)
        contract = w3r.eth.contract(address=checksum_addr, abi=ERC721_ABI)
        
        # Run sync Web3 call in executor
        loop = asyncio.get_event_loop()
        try:
            uri = await loop.run_in_executor(None, contract.functions.tokenURI(1).call)
        except Exception:
            try:
                uri = await loop.run_in_executor(None, contract.functions.tokenURI(0).call)
            except Exception:
                return None
                
        if uri:
            if uri.startswith("data:application/json;base64,"):
                import base64
                import json
                try:
                    b64_data = uri.split(",", 1)[1]
                    data = json.loads(base64.b64decode(b64_data).decode("utf-8"))
                    return data.get("external_url")
                except Exception as e:
                    pass
            elif uri.startswith("data:application/json;utf8,"):
                import json
                try:
                    str_data = uri.split(",", 1)[1]
                    data = json.loads(str_data)
                    return data.get("external_url")
                except Exception as e:
                    pass
            
            if uri.startswith("ipfs://") or "ipfs/" in uri:
                ipfs_hash = uri.split("ipfs/")[-1] if "ipfs/" in uri else uri.replace("ipfs://", "")
                async with aiohttp.ClientSession() as session:
                    data = await fetch_ipfs_json(session, ipfs_hash)
                    if data:
                        return data.get("external_url")
            elif uri.startswith("http"):
                async with aiohttp.ClientSession() as session:
                    async with session.get(uri, timeout=4) as resp:
                        if resp.status == 200:
                            data = await resp.json(content_type=None)
                            if isinstance(data, dict):
                                return data.get("external_url")
    except Exception as e:
        pass
    return None

async def fetch_token_image(contract_address: str, token_id: int = 1) -> str:
    """Attempt to natively fetch the tokenURI(tokenId) payload to grab its image."""
    try:
        w3r = _web3_for_reads()
        if w3r is None:
            return None
        checksum_addr = Web3.to_checksum_address(contract_address)
        contract = w3r.eth.contract(address=checksum_addr, abi=ERC721_ABI)
        
        loop = asyncio.get_event_loop()
        try:
            uri = await loop.run_in_executor(None, contract.functions.tokenURI(token_id).call)
        except Exception:
            try:
                uri = await loop.run_in_executor(None, contract.functions.tokenURI(1).call)
            except Exception:
                try:
                    uri = await loop.run_in_executor(None, contract.functions.tokenURI(0).call)
                except Exception:
                    return None
                
        if uri:
            if uri.startswith("data:application/json;base64,"):
                import base64
                import json
                try:
                    b64_data = uri.split(",", 1)[1]
                    data = json.loads(base64.b64decode(b64_data).decode("utf-8"))
                    img = data.get("image") or data.get("image_url")
                    if img and img.startswith("ipfs://"):
                        img = img.replace("ipfs://", "https://cf-ipfs.com/ipfs/")
                    return img
                except Exception as e:
                    pass
            elif uri.startswith("data:application/json;utf8,"):
                import json
                try:
                    str_data = uri.split(",", 1)[1]
                    data = json.loads(str_data)
                    img = data.get("image") or data.get("image_url")
                    if img and img.startswith("ipfs://"):
                        img = img.replace("ipfs://", "https://cf-ipfs.com/ipfs/")
                    return img
                except Exception as e:
                    pass
            
            if uri.startswith("ipfs://") or "ipfs/" in uri:
                ipfs_hash = uri.split("ipfs/")[-1] if "ipfs/" in uri else uri.replace("ipfs://", "")
                async with aiohttp.ClientSession() as session:
                    data = await fetch_ipfs_json(session, ipfs_hash)
                    if data:
                        img = data.get("image") or data.get("image_url")
                        if img and img.startswith("ipfs://"):
                            img = img.replace("ipfs://", "https://cf-ipfs.com/ipfs/")
                        return img
            elif uri.startswith("http"):
                async with aiohttp.ClientSession() as session:
                    async with session.get(uri, timeout=4) as resp:
                        if resp.status == 200:
                            data = await resp.json(content_type=None)
                            if isinstance(data, dict):
                                img = data.get("image") or data.get("image_url")
                                if img and img.startswith("ipfs://"):
                                    img = img.replace("ipfs://", "https://cf-ipfs.com/ipfs/")
                                return img
    except Exception as e:
        pass
    return None

async def _build_eth_mint_obj(contract: str, recent_mints: int) -> ActiveMint:
    info = await get_contract_info(contract)
    website_url = await fetch_external_url(contract)
    t_id = last_token_id.get(contract, 1)
    image_url = await fetch_token_image(contract, t_id)
    
    minters_set = minters_history.get(contract, set())
    unique_minters = len(minters_set)
    import wallet_database as database
    tracked_eth = {addr.lower() for addr in database.get_wallets_by_chain("ETH").keys()}
    tracked_minters_count = len(minters_set.intersection(tracked_eth))
    
    waypoint_url = f"https://waypoint.tools/mintscan/#{contract}"
    return ActiveMint(
        name=info.get("name", "Unknown"),
        symbol=info.get("symbol", "???"),
        chain="ethereum",
        contract=contract,
        collection_id=contract,
        total_supply=info.get("maxSupply", 0) or 0,
        minted_count=info.get("totalSupply", recent_mints) or recent_mints,
        unique_minters=unique_minters,
        tracked_minters=tracked_minters_count,
        mint_price=info.get("mintPrice", "Unknown"),
        image_url=image_url,
        etherscan_url=f"https://etherscan.io/address/{contract}",
        opensea_url=f"https://opensea.io/assets/ethereum/{contract}",
        blur_url=f"https://blur.io/collection/{contract}",
        nftscan_url=f"https://mint.nftscan.com/{contract}",
        website_url=website_url,
        waypoint_url=waypoint_url,
        recent_mints_count=recent_mints,
    )

async def get_top_eth_mints(limit: int = 5) -> List[ActiveMint]:
    """Get the most active live ETH mints right now from memory."""
    top_contracts = sorted(mint_history.keys(), key=lambda c: len(mint_history[c]), reverse=True)[:limit]
    
    results = []
    for contract in top_contracts:
        recent_mints = len(mint_history[contract])
        info = await get_contract_info(contract)
        website_url = await fetch_external_url(contract)
        t_id = last_token_id.get(contract, 1)
        image_url = await fetch_token_image(contract, t_id)
        
        minters_set = minters_history.get(contract, set())
        unique_minters = len(minters_set)
        
        import wallet_database as database
        tracked_eth = {addr.lower() for addr in database.get_wallets_by_chain("ETH").keys()}
        tracked_minters_count = len(minters_set.intersection(tracked_eth))
        
        waypoint_url = f"https://waypoint.tools/mintscan/#{contract}"
        m = ActiveMint(
            name=info.get("name", "Unknown"),
            symbol=info.get("symbol", "???"),
            chain="ethereum",
            contract=contract,
            collection_id=contract,
            total_supply=info.get("maxSupply", 0) or 0,
            minted_count=info.get("totalSupply", recent_mints) or recent_mints,
            unique_minters=unique_minters,
            tracked_minters=tracked_minters_count,
            mint_price=info.get("mintPrice", "Unknown"),
            image_url=image_url,
            etherscan_url=f"https://etherscan.io/address/{contract}",
            opensea_url=f"https://opensea.io/assets/ethereum/{contract}",
            blur_url=f"https://blur.io/collection/{contract}",
            nftscan_url=f"https://mint.nftscan.com/{contract}",
            magiceden_url=None,
            tensor_url=None,
            solscan_url=None,
            website_url=website_url,
            waypoint_url=waypoint_url,
            recent_mints_count=recent_mints,
        )
        results.append(m)
    return results

eth_radar_sent: Set[str] = set()  # Legacy, kept for reference but no longer used

async def check_live_eth_mints(client: Bot, channel_ids: str, radar_channel_ids: str = None):
    if not RPC_URLS or w3 is None:
        print(
            "[EthLiveMints] Disabled: no non-Alchemy RPC configured. "
            "Set ETHEREUM_MINT_RPC_URLS (comma-separated) to enable."
        )
        return
    """Background task to poll for live ETH mints. Radar (new mints) -> radar_channel_ids; Trending -> channel_ids."""
    await asyncio.sleep(5)  # Wait for bot to be ready
    
    channels = []
    if channel_ids:
        for cid in str(channel_ids).split(','):
            if cid.strip():
                ch = client.get_channel(int(cid.strip()))
                if ch: channels.append(ch)
                else:
                    print(
                        f"[EthLiveMints] Channel {cid} not found / no access — "
                        f"trending mint alerts skipped for this ID."
                    )
                
    radar_channels = []
    if radar_channel_ids:
        for cid in str(radar_channel_ids).split(','):
            if cid.strip():
                ch = client.get_channel(int(cid.strip()))
                if ch: radar_channels.append(ch)
                else:
                    print(
                        f"[EthLiveMints] Radar channel {cid} not found / no access — "
                        f"radar mint alerts skipped for this ID."
                    )
                
    if radar_channel_ids and not radar_channels:
        print("[EthLiveMints] No valid radar channels found; new mints will go to mints channels.")

    print("[EthLiveMints] Started Live ETH Mint Tracker...")
    print(f"[EthLiveMints] RPC → {_rpc_log_url(RPC_URLS[0])}")
    last_block = None
    backoff_s = 5

    while True:
        try:
            if last_block is None:
                last_block = await asyncio.to_thread(lambda: w3.eth.block_number)
                
            current_block = await asyncio.to_thread(lambda: w3.eth.block_number)
            if current_block <= last_block:
                await asyncio.sleep(12)
                continue

            # Cap eth_getLogs range — Alchemy rejects large ranges (>2000 blocks).
            MAX_BLOCK_RANGE = 50
            if current_block - last_block > MAX_BLOCK_RANGE:
                last_block = current_block - MAX_BLOCK_RANGE
            
            # Use raw eth_getLogs for performance
            # Track whether 721 already rotated the RPC so 1155 can decide independently.
            _rpc_rotated = False

            logs_721 = []
            try:
                # ERC-721 Mints (from: 0x0...0)
                logs_721 = await asyncio.to_thread(
                    lambda: w3.eth.get_logs(
                        {
                            "fromBlock": last_block + 1,
                            "toBlock": current_block,
                            "topics": [TRANSFER_TOPIC, NULL_ADDRESS_TOPIC],
                        }
                    )
                )
            except Exception as e:
                es = str(e)
                es_lower = es.lower()
                if "429" in es or ("rate" in es_lower and "limit" in es_lower):
                    _switch_rpc("rate limited")
                    _rpc_rotated = True
                    _throttled_eth_mint_log("721_ratelimit", "[EthLiveMints] 721 rate limited — rotating RPC.")
                elif (
                    "-32000" in es
                    and ("invalid block range" in es_lower or "errupstreamsexhausted" in es_lower or "upstream" in es_lower)
                ):
                    _switch_rpc("upstream failure")
                    _rpc_rotated = True
                    _throttled_eth_mint_log("721_upstream", "[EthLiveMints] 721 upstream failure — rotating RPC.")
                elif "-32603" in es or "unreachable" in es_lower or "all rpc" in es_lower:
                    _switch_rpc("unreachable")
                    _rpc_rotated = True
                    _throttled_eth_mint_log("721_unreachable", "[EthLiveMints] 721 RPC unreachable — rotating.")
                else:
                    ek = f"721|{es[:100]}"
                    _throttled_eth_mint_log(ek, f"[EthLiveMints] 721 Logs Error ({last_block + 1} to {current_block}): {e}")

            logs_1155 = []
            try:
                # ERC-1155 Mints (from: 0x0...0 is topics[2])
                logs_1155 = await asyncio.to_thread(
                    lambda: w3.eth.get_logs(
                        {
                            "fromBlock": last_block + 1,
                            "toBlock": current_block,
                            "topics": [TRANSFER_SINGLE_TOPIC, None, NULL_ADDRESS_TOPIC],
                        }
                    )
                )
            except Exception as e:
                es = str(e)
                es_lower = es.lower()
                if "429" in es or ("rate" in es_lower and "limit" in es_lower):
                    _switch_rpc("rate limited")
                    _throttled_eth_mint_log("1155_ratelimit", "[EthLiveMints] 1155 rate limited — rotating RPC.")
                elif (
                    "-32000" in es
                    and ("invalid block range" in es_lower or "errupstreamsexhausted" in es_lower or "upstream" in es_lower)
                ):
                    if not _rpc_rotated:
                        _switch_rpc("upstream failure")
                    _throttled_eth_mint_log("1155_upstream", "[EthLiveMints] 1155 upstream failure — rotating RPC.")
                elif "-32603" in es or "unreachable" in es_lower or "all rpc" in es_lower:
                    if not _rpc_rotated:
                        _switch_rpc("unreachable")
                    _throttled_eth_mint_log("1155_unreachable", "[EthLiveMints] 1155 RPC unreachable — rotating.")
                else:
                    ek = f"1155|{es[:100]}"
                    _throttled_eth_mint_log(ek, f"[EthLiveMints] 1155 Logs Error ({last_block + 1} to {current_block}): {e}")
            
            logs = logs_721 + logs_1155
            
            now = time.time()
            
            # Process logs
            for log in logs:
                contract_address = log["address"].lower()
                # HexBytes.hex() returns WITHOUT 0x prefix, normalize comparison
                topic_sig = "0x" + log["topics"][0].hex()
                minter_address = None
                
                if topic_sig == TRANSFER_TOPIC:
                    # topics[1] is 'from', topics[2] is 'to', topics[3] is 'tokenId' for ERC-721
                    if len(log["topics"]) == 4:
                        minter_address = log["topics"][2].hex()
                        try:
                            last_token_id[contract_address] = int(log["topics"][3].hex(), 16)
                        except: pass
                elif topic_sig == TRANSFER_SINGLE_TOPIC:
                    # topics[1] is 'operator', topics[2] is 'from' (0x0), topics[3] is 'to'
                    if len(log["topics"]) == 4:
                        minter_address = log["topics"][3].hex()
                        # ERC1155 IDs are usually in the unindexed data field, but for proxy we just use 1 or what we can
                        try:
                            # 1155 data is (uint256 id, uint256 value)
                            data_hex = log["data"].hex() if hasattr(log["data"], 'hex') else str(log["data"]).replace('0x', '')
                            last_token_id[contract_address] = int(data_hex[:64], 16)
                        except: pass
                
                if not minter_address:
                    continue
                    
                # Clean up the padded address
                minter_address = "0x" + minter_address[-40:]
                minters_history[contract_address].add(minter_address)
                
                try:
                    last_tx_hash[contract_address] = log["transactionHash"].hex()
                except: pass
                
                mint_history[contract_address].append(now)
            
            # Clean up old data and check thresholds
            for contract in list(mint_history.keys()):
                # Remove timestamps older than TIME_WINDOW
                mint_history[contract] = [t for t in mint_history[contract] if now - t <= TIME_WINDOW]
                
                # If no recent mints, clear minters set to save memory
                if not mint_history[contract]:
                    del mint_history[contract]
                    if contract in minters_history:
                        del minters_history[contract]
                    continue
                
                recent_mints = len(mint_history[contract])
                
                if recent_mints >= 2 and should_alert_radar(contract, "ethereum"):
                    m = await _build_eth_mint_obj(contract, recent_mints)
                    try:
                        embed = await build_radar_embed(m)
                        if radar_channels:
                            for target in radar_channels:
                                await target.send(embed=embed)
                            mark_alerted(contract, "ethereum", tier=1, count=recent_mints)
                            print(f"[EthLiveMints] 🔵 RADAR alert for {contract} ({recent_mints} mints)")
                    except Exception as e:
                        print(f"[EthLiveMints] Radar Send error: {e}")
                
                # TIER 2: TRENDING ALERT -> mints channel only (enrich with Waypoint for image + links)
                if recent_mints >= MINT_THRESHOLD and should_alert_trending(contract, "ethereum", recent_mints):
                    m = await _build_eth_mint_obj(contract, recent_mints)
                    try:
                        async with aiohttp.ClientSession() as session:
                            detail = await fetch_waypoint_collection(session, contract)
                            if detail:
                                m = replace(
                                    m,
                                    image_url=detail.get("image_url") or m.image_url,
                                    website_url=detail.get("website") or m.website_url,
                                    twitter_url=detail.get("twitter") or m.twitter_url,
                                    discord_url=detail.get("discord_url") or m.discord_url,
                                )
                        embed = await build_active_mint_embed(m)
                        for target in channels:
                            await target.send(embed=embed)
                        mark_alerted(contract, "ethereum", tier=2, count=recent_mints)
                        print(f"[EthLiveMints] 🔥 TRENDING alert for {contract} ({recent_mints} mints)")
                    except Exception as e:
                        print(f"[EthLiveMints] Trending Send error: {e}")
            
            last_block = current_block
            backoff_s = 5  # Reset only after a fully successful block pass

        except Exception as e:
            err_str = str(e)
            err_lower = err_str.lower()

            # Handle RPC rate limits / transient provider failures by rotating + backing off
            if "429" in err_lower or "too many requests" in err_lower or "rate limit" in err_lower:
                _switch_rpc("rate limited")
                print(f"[EthLiveMints] Rate limited. Backing off for {backoff_s}s.")
                await asyncio.sleep(backoff_s)
                backoff_s = min(backoff_s * 2, 120)
                continue
            if "400" in err_lower or "bad request" in err_lower:
                _switch_rpc("bad request")
                print(f"[EthLiveMints] Bad request from RPC. Backing off for {backoff_s}s.")
                await asyncio.sleep(backoff_s)
                backoff_s = min(backoff_s * 2, 60)
                continue
            if "-32005" in err_lower or "limit exceeded" in err_lower or "cannot fulfill request" in err_lower or "-32046" in err_lower:
                _switch_rpc("provider limit")
                print(f"[EthLiveMints] Provider limit. Backing off for {backoff_s}s.")
                await asyncio.sleep(backoff_s)
                backoff_s = min(backoff_s * 2, 60)
                continue
            # Provider-level upstream / block range failures (llamarpc, dRPC, etc.)
            if (
                "-32000" in err_str
                and (
                    "invalid block range" in err_lower
                    or "errupstreamsexhausted" in err_lower
                    or "upstream" in err_lower
                )
            ):
                _switch_rpc("upstream failure")
                _throttled_eth_mint_log("rpc_upstream", "[EthLiveMints] Upstream exhausted / invalid block range. Rotating RPC.")
                await asyncio.sleep(backoff_s)
                backoff_s = min(backoff_s * 2, 90)
                continue
            # Public Ankr and some nodes return -32000 Unauthorized without a key
            if (
                "-32000" in err_str
                and ("unauthorized" in err_lower or "authenticate" in err_lower or "api key" in err_lower)
            ):
                _switch_rpc("RPC auth required")
                _throttled_eth_mint_log(
                    "rpc_unauth",
                    "[EthLiveMints] RPC rejected (missing/invalid API key). Rotating endpoint. "
                    "Set ANKR_ETH_RPC_URL or ANKR_API_KEY if using Ankr.",
                )
                await asyncio.sleep(backoff_s)
                backoff_s = min(backoff_s * 2, 120)
                continue
            if "-32603" in err_str or "unreachable" in err_lower:
                _switch_rpc("unreachable")
                _throttled_eth_mint_log("rpc_unreachable", f"[EthLiveMints] RPC unreachable: {e}")
                await asyncio.sleep(backoff_s)
                backoff_s = min(backoff_s * 2, 90)
                continue

            _throttled_eth_mint_log(f"loop:{err_str[:120]}", f"[EthLiveMints] Loop Error: {e}")
            
        await asyncio.sleep(5)
