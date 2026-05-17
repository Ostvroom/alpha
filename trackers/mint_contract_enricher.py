"""
Enrich raw on-chain mint data with human-readable names, images, real costs, and links.
Uses free public RPC calls + IPFS metadata. No API keys needed.
"""
import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Dict, Optional

import aiohttp

import config
from app_paths import DATA_DIR, ensure_dirs

logger = logging.getLogger(__name__)

RPC_ENDPOINTS = [
    "https://ethereum.publicnode.com",
    "https://eth.llamarpc.com",
    "https://rpc.ankr.com/eth",
]

IPFS_GATEWAYS = [
    "https://ipfs.io/ipfs",
    "https://cloudflare-ipfs.com/ipfs",
    "https://gateway.pinata.cloud/ipfs",
]

CONTRACT_CACHE_FILE = str(DATA_DIR / "mint_contract_cache.json")

NAME_SELECTOR = "0x06fdde03"
TOTAL_SUPPLY_SELECTOR = "0x18160ddd"
MAX_SUPPLY_SELECTOR = "0xd5abeb01"
MAX_SUPPLY_ALT_SELECTOR = "0x32cb6b0c"  # MAX_SUPPLY()
TOKEN_URI_SELECTOR = "0xc87b56dd"   # ERC721 tokenURI(uint256)
ERC1155_URI_SELECTOR = "0x0e89341c" # ERC1155 uri(uint256)

# Well-known contracts that don't expose name() via standard RPC
KNOWN_CONTRACTS = {
    "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85": {
        "name": "ENS",
        "image": "https://www.nftscan.com/images/og-img/home.png",
    },
    "0xc36442b4a4522e871399cd717abdd847ab11fe88": {
        "name": "Uniswap V3 Positions",
        "image": "https://www.nftscan.com/images/og-img/home.png",
    },
}

# CoinGecko NFT API (free, no key needed) — collection logos
COINGECKO_NFT_URL = "https://api.coingecko.com/api/v3/nfts/ethereum/contract/{contract}"

def _alchemy_nft_metadata_url(contract: str) -> str:
    key = (getattr(config, "ALCHEMY_NFT_API_KEY", None) or "").strip() or "demo"
    return (
        f"https://eth-mainnet.g.alchemy.com/nft/v3/{key}/getContractMetadata"
        f"?contractAddress={contract}"
    )


def _decode_string(hex_result: str) -> str:
    if not hex_result or hex_result == "0x":
        return ""
    try:
        data = hex_result[2:] if hex_result.startswith("0x") else hex_result
        if len(data) < 128:
            return ""
        offset = int(data[:64], 16) * 2
        length = int(data[offset:offset + 64], 16) * 2
        string_hex = data[offset + 64:offset + 64 + length]
        return bytes.fromhex(string_hex).decode("utf-8", errors="ignore").strip()
    except Exception:
        return ""


def _to_ipfs_http(uri: str) -> str:
    if uri.startswith("ipfs://"):
        cid = uri[7:]
        return f"{IPFS_GATEWAYS[0]}/{cid}"
    return uri


class MintContractEnricher:
    def __init__(self):
        ensure_dirs()
        self._contract_cache: Dict[str, Dict] = {}
        self._supply_cache: Dict[str, Dict] = {}
        self._supply_cache_ttl_sec = 45
        self._tx_cache: Dict[str, Dict] = {}
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()
        self._eth_price: float = 0.0
        self._eth_price_updated: datetime = datetime.min
        self._load_contract_cache()

    def _load_contract_cache(self):
        if os.path.exists(CONTRACT_CACHE_FILE):
            try:
                with open(CONTRACT_CACHE_FILE, "r", encoding="utf-8") as f:
                    self._contract_cache = json.load(f)
            except Exception:
                self._contract_cache = {}

    def _save_contract_cache(self):
        try:
            with open(CONTRACT_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(self._contract_cache, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save contract cache: {e}")

    async def get_eth_price(self) -> float:
        """Fetch ETH/USD price from CoinGecko (free, no API key)."""
        now = datetime.utcnow()
        if self._eth_price > 0 and (now - self._eth_price_updated).seconds < 300:
            return self._eth_price
        try:
            session = await self._get_session()
            async with session.get(
                "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self._eth_price = float(data.get("ethereum", {}).get("usd", 0))
                    self._eth_price_updated = now
                    return self._eth_price
        except Exception:
            pass
        return self._eth_price or 2500.0  # fallback estimate

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ──────────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────────

    async def enrich(self, mint: Dict) -> Dict:
        contract = mint.get("contract_address", "").lower()
        tx_hash = mint.get("tx_hash", "") or mint.get("hash", "")

        contract_data = await self._enrich_contract(contract, mint.get("token_id", "1"))
        tx_data = await self._enrich_tx(tx_hash) if tx_hash else {}

        return {**mint, **contract_data, **tx_data}

    async def batch_enrich(self, mints: list) -> list:
        if not mints:
            return mints
        # Limit concurrent enrichments to avoid RPC rate limits
        semaphore = asyncio.Semaphore(2)
        async def _enrich_one(m):
            async with semaphore:
                return await self.enrich(m)
        tasks = [_enrich_one(m) for m in mints]
        return await asyncio.gather(*tasks)

    # ──────────────────────────────────────────────────────────────────────────
    # Contract metadata (name + image + links)
    # ──────────────────────────────────────────────────────────────────────────

    async def _get_fresh_supply(self, contract: str) -> tuple:
        """On-chain minted count + max supply (short TTL cache; never use stale contract file cache)."""
        now = datetime.utcnow()
        cached = self._supply_cache.get(contract)
        if cached and (now - cached["updated"]).total_seconds() < self._supply_cache_ttl_sec:
            return cached.get("total_supply"), cached.get("max_supply")

        alchemy_meta = await self._get_alchemy_metadata(contract)
        total_supply = alchemy_meta.get("total_supply")
        if total_supply is None:
            total_supply = await self._get_total_supply(contract)
        max_supply = await self._get_max_supply(contract)

        self._supply_cache[contract] = {
            "total_supply": total_supply,
            "max_supply": max_supply,
            "updated": now,
        }
        return total_supply, max_supply

    async def _enrich_contract(self, contract: str, token_id: str) -> Dict:
        if not contract:
            return {}

        cached = self._contract_cache.get(contract)
        if cached:
            total_supply, max_supply = await self._get_fresh_supply(contract)
            out = dict(cached)
            if total_supply is not None:
                out["total_supply"] = total_supply
            if max_supply is not None:
                out["max_supply"] = max_supply
            return out

        # Check well-known contracts first
        known = KNOWN_CONTRACTS.get(contract)

        # ── Primary: Alchemy metadata (name + image + slug + supply) ──
        alchemy_meta = await self._get_alchemy_metadata(contract)

        # Start with Alchemy name if available
        name = alchemy_meta.get("name") or (known["name"] if known else "")

        # Only hit RPC if Alchemy didn't give us a name
        if not name:
            name = await self._get_name(contract)

        display_name = name or f"{contract[:6]}...{contract[-4:]}"

        # ── CoinGecko NFT API (only if Alchemy missed image or name) ──
        cg_logo = None
        cg_name = None
        if not alchemy_meta.get("image") or not name:
            cg_logo, cg_name = await self._get_coingecko_logo(contract)

        # Fetch image: Alchemy → CoinGecko → NFTScan → IPFS
        image = alchemy_meta.get("image")
        if not image:
            image = cg_logo
        if not image:
            image = await self._fetch_metadata_image(contract, token_id)
        if not image and known:
            image = known.get("image")

        # Fetch supply: Alchemy first, RPC fallback
        total_supply = alchemy_meta.get("total_supply")
        max_supply = None
        if total_supply is None:
            total_supply = await self._get_total_supply(contract)
        max_supply = await self._get_max_supply(contract)

        # Final name fallback: CoinGecko
        if not name and cg_name:
            name = cg_name
            display_name = name

        # OpenSea URL: collection page if slug available, asset page only for numeric token IDs
        if alchemy_meta.get("slug"):
            opensea_url = f"https://opensea.io/collection/{alchemy_meta['slug']}"
        elif str(token_id).lstrip('-').isdigit():
            opensea_url = f"https://opensea.io/assets/ethereum/{contract}/{token_id}"
        else:
            opensea_url = f"https://etherscan.io/address/{contract}"

        data = {
            "contract_name": display_name,
            "image_url": image or "",
            "nftscan_url": f"https://nftscan.com/{contract}",
            "etherscan_url": f"https://etherscan.io/address/{contract}",
            "opensea_url": opensea_url,
            "total_supply": total_supply,
            "max_supply": max_supply,
        }

        async with self._lock:
            self._contract_cache[contract] = {k: v for k, v in data.items() if k not in ("total_supply", "max_supply")}
            self._save_contract_cache()
            self._supply_cache[contract] = {
                "total_supply": total_supply,
                "max_supply": max_supply,
                "updated": datetime.utcnow(),
            }

        return data

    async def _get_coingecko_logo(self, contract: str) -> tuple:
        """Fetch collection logo from CoinGecko NFT API (free, no key).
        Returns (image_url, collection_name)."""
        try:
            session = await self._get_session()
            url = COINGECKO_NFT_URL.format(contract=contract)
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    img = data.get("image", {})
                    image_url = img.get("small_2x") or img.get("small")
                    name = data.get("name")
                    return image_url, name
                elif resp.status in (404, 429):
                    # Not indexed or rate limited — back off
                    return None, None
        except Exception:
            pass
        return None, None

    async def _get_alchemy_metadata(self, contract: str) -> Dict:
        """Fetch collection metadata from Alchemy NFT API (demo key, best effort).
        Returns dict with: image, name, slug, total_supply."""
        try:
            session = await self._get_session()
            url = _alchemy_nft_metadata_url(contract)
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    os_meta = data.get("openSeaMetadata", {})
                    result = {}
                    if os_meta.get("imageUrl"):
                        result["image"] = os_meta["imageUrl"]
                    if os_meta.get("collectionName"):
                        result["name"] = os_meta["collectionName"]
                    if os_meta.get("collectionSlug"):
                        result["slug"] = os_meta["collectionSlug"]
                    ts = data.get("totalSupply")
                    if ts:
                        try:
                            result["total_supply"] = int(ts)
                        except Exception:
                            pass
                    return result
        except Exception:
            pass
        return {}

    async def _fetch_metadata_image(self, contract: str, token_id: str) -> Optional[str]:
        """Fallback image sources (NFTScan logo, IPFS metadata)."""
        tid = int(token_id) if str(token_id).isdigit() else 1

        # ── Source 1: NFTScan logo CDN ──
        nftscan_logo = f"https://logo.nftscan.com/logo/{contract}.png"
        try:
            session = await self._get_session()
            async with session.head(nftscan_logo, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    return nftscan_logo
        except Exception:
            pass

        # ── Source 2: tokenURI → metadata JSON → image ──
        tid_hex = format(tid, "064x")
        uri = await self._rpc_string_call(contract, TOKEN_URI_SELECTOR + tid_hex)
        if not uri:
            uri = await self._rpc_string_call(contract, ERC1155_URI_SELECTOR + tid_hex)

        if uri:
            if "{id}" in uri:
                uri = uri.replace("{id}", format(tid, "064x"))
            uri = _to_ipfs_http(uri)

            if not uri.startswith("data:"):
                try:
                    session = await self._get_session()
                    async with session.get(uri, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            meta = await resp.json()
                            image = meta.get("image", "")
                            if image and not image.startswith("data:"):
                                return _to_ipfs_http(image)
                except Exception:
                    pass

        return None

    async def _get_name(self, contract: str) -> str:
        return await self._rpc_string_call(contract, NAME_SELECTOR)

    async def _get_total_supply(self, contract: str) -> Optional[int]:
        """Call totalSupply() on the contract."""
        result = await self._rpc_request("eth_call", [{"to": contract, "data": TOTAL_SUPPLY_SELECTOR}, "latest"])
        if result and result != "0x":
            try:
                return int(result, 16)
            except Exception:
                pass
        return None

    async def _get_max_supply(self, contract: str) -> Optional[int]:
        """Call maxSupply() or MAX_SUPPLY() on the contract."""
        for selector in (MAX_SUPPLY_SELECTOR, MAX_SUPPLY_ALT_SELECTOR):
            result = await self._rpc_request("eth_call", [{"to": contract, "data": selector}, "latest"])
            if result and result != "0x":
                try:
                    val = int(result, 16)
                    # Ignore uint256 max (unlimited/open edition)
                    if val < 2**255:
                        return val
                except Exception:
                    pass
        return None

    async def _rpc_string_call(self, contract: str, data: str) -> str:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [{"to": contract, "data": data}, "latest"]
        }
        for attempt in range(2):
            for rpc in RPC_ENDPOINTS:
                try:
                    session = await self._get_session()
                    async with session.post(rpc, json=payload) as resp:
                        result = (await resp.json()).get("result", "")
                        if result and result != "0x":
                            decoded = _decode_string(result)
                            if decoded:
                                return decoded
                except Exception:
                    continue
            if attempt == 0:
                await asyncio.sleep(0.5)
        return ""

    # ──────────────────────────────────────────────────────────────────────────
    # Transaction enrichment (real mint cost + gas)
    # ──────────────────────────────────────────────────────────────────────────

    async def _enrich_tx(self, tx_hash: str) -> Dict:
        if not tx_hash:
            return {}

        cached = self._tx_cache.get(tx_hash)
        if cached:
            return cached

        tx = await self._rpc_request("eth_getTransactionByHash", [tx_hash])
        receipt = await self._rpc_request("eth_getTransactionReceipt", [tx_hash])

        mint_cost = 0.0
        if tx and tx.get("value"):
            try:
                mint_cost = int(tx["value"], 16) / 1e18
            except Exception:
                pass

        gas_used = 0
        gas_fee = 0.0
        if receipt and receipt.get("gasUsed"):
            try:
                gas_used = int(receipt["gasUsed"], 16)
            except Exception:
                pass

        if receipt and receipt.get("effectiveGasPrice"):
            try:
                gas_price = int(receipt["effectiveGasPrice"], 16)
                gas_fee = (gas_used * gas_price) / 1e18
            except Exception:
                pass
        elif tx and tx.get("gasPrice"):
            try:
                gas_price = int(tx["gasPrice"], 16)
                gas_fee = (gas_used * gas_price) / 1e18
            except Exception:
                pass

        eth_price = await self.get_eth_price()
        data = {
            "mint_cost": mint_cost,
            "trade_price": mint_cost,
            "price": mint_cost,
            "mint_cost_usd": round(mint_cost * eth_price, 2) if mint_cost else 0,
            "gas_fee_usd": round(gas_fee * eth_price, 4) if gas_fee else 0,
            "gas_used": gas_used,
            "gas_fee": gas_fee,
        }

        async with self._lock:
            self._tx_cache[tx_hash] = data
        return data

    async def _rpc_request(self, method: str, params: list) -> Optional[Dict]:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params
        }
        for rpc in RPC_ENDPOINTS:
            try:
                session = await self._get_session()
                async with session.post(rpc, json=payload) as resp:
                    return (await resp.json()).get("result")
            except Exception:
                continue
        return None
