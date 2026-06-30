"""
Real-time Ethereum NFT mint listener via WebSocket.
No API keys needed — uses free public RPC nodes.
Subscribes to ERC721 Transfer + ERC1155 TransferSingle/TransferBatch events.
Emits:
  - Mint events (from == 0x0)
  - Whale events:
      * Known whales: alert on ANY mint/buy (instant)
      * Unknown wallets: alert only if 5+ NFTs in a SINGLE transaction (aggregated)
"""
import asyncio
import json
import os
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone, timedelta
import websockets
import logging

# HTTP RPCs used when WebSocket delivery is unavailable on cloud hosts.
def _build_http_poll_rpcs() -> List[str]:
    urls: List[str] = []
    for key in (
        "ETHEREUM_MINT_RPC_URLS",
        "NON_ALCHEMY_ETH_RPC_URL",
        "ETHEREUM_RPC_URL",
    ):
        raw = (os.getenv(key) or "").strip()
        if raw:
            urls.extend(part.strip() for part in raw.split(",") if part.strip())
    urls.extend(("https://1rpc.io/eth", "https://eth.drpc.org"))
    return list(dict.fromkeys(urls))


_HTTP_POLL_RPCS = _build_http_poll_rpcs()

logger = logging.getLogger(__name__)

# Event signatures
ERC721_TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ERC1155_TRANSFER_SINGLE = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
ERC1155_TRANSFER_BATCH = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
from trackers.eth_address import ZERO_ADDRESS, normalize_eth_address

ZERO_ADDRESS_SHORT = ZERO_ADDRESS
NULL_ADDRESS_TOPIC = "0x" + ("0" * 64)

# Quiet routine per-poll heartbeats unless LOG_HEARTBEATS is enabled. Mints are
# still processed and posted; only the chatty "HTTP poll: N mint log(s)" console
# line is suppressed so the logs stay readable.
_LOG_HEARTBEATS = os.getenv("LOG_HEARTBEATS", "0").strip().lower() in ("1", "true", "yes", "on")

def _decode_erc1155_batch(data: str) -> Tuple[List[int], List[int]]:
    """Decode the token-id and value arrays from TransferBatch event data."""
    try:
        payload = bytes.fromhex((data or "").removeprefix("0x"))
        if len(payload) < 64:
            return [], []
        ids_offset = int.from_bytes(payload[0:32], "big")
        values_offset = int.from_bytes(payload[32:64], "big")

        def read_array(offset: int) -> List[int]:
            if offset < 0 or offset + 32 > len(payload):
                return []
            length = int.from_bytes(payload[offset:offset + 32], "big")
            if length > 10000 or offset + 32 + (length * 32) > len(payload):
                return []
            start = offset + 32
            return [
                int.from_bytes(payload[start + (i * 32):start + ((i + 1) * 32)], "big")
                for i in range(length)
            ]

        return read_array(ids_offset), read_array(values_offset)
    except (TypeError, ValueError):
        return [], []


def _build_ws_endpoints() -> List[str]:
    """Build WebSocket endpoint list from env (private RPCs first, then public fallbacks)."""
    custom: List[str] = []
    raw = (os.getenv("ETHEREUM_WS_RPC_URLS") or "").strip()
    if raw:
        custom = [u.strip() for u in raw.split(",") if u.strip()]
    else:
        single = (os.getenv("ETHEREUM_WS_RPC_URL") or "").strip()
        if single:
            custom = [single]
    # Public fallbacks — these may block cloud IPs (Render, AWS, etc.)
    public = [
        "wss://ethereum.publicnode.com",
        "wss://eth.drpc.org",
        "wss://ethereum-rpc.publicnode.com",
    ]
    return custom + public

WS_ENDPOINTS = _build_ws_endpoints()

# ═══════════════════════════════════════════════════════════════════════════════
# SPAM / DUST FILTER
# ═══════════════════════════════════════════════════════════════════════════════
_SPAM_ENV = os.getenv("SPAM_CONTRACTS", "")
SPAM_CONTRACTS: set = set()
for _addr in _SPAM_ENV.split(","):
    _a = _addr.strip().lower()
    if _a:
        SPAM_CONTRACTS.add(_a)

SPAM_CONTRACTS.update({
    "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85",  # ENS
    "0xc36442b4a4522e871399cd717abdd847ab11fe88",  # Uniswap V3 Positions
    "0x0000000000000000000000000000000000000000",  # zero address events
})

SPAM_NAME_PATTERNS = [
    "ens", "ethereum name service",
    "uniswap v3", "uniswap v4", "uniswap position",
    "dns", "domain name service",
]

SUPPRESS_UNNAMED = os.getenv("SUPPRESS_UNNAMED", "0") == "1"

# ═══════════════════════════════════════════════════════════════════════════════
# WHALE WATCH CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
# Minimum NFTs in a single tx to trigger an alert for unknown wallets
WHALE_BULK_THRESHOLD = int(os.getenv("WHALE_BULK_THRESHOLD", "5"))

# v3 wallet tracker runs separately (trackers/eth_tracker.py)
WALLET_TRACKER_ENABLED = False

# Env-based whale wallets (supplement to DB)
_WHALE_ENV = os.getenv("WHALE_WALLETS", "")
WHALE_WALLETS: set = set()
for _addr in _WHALE_ENV.split(","):
    _a = _addr.strip().lower()
    if _a:
        if not _a.startswith("0x"):
            _a = "0x" + _a
        if len(_a) == 42:
            WHALE_WALLETS.add(_a)


class EthMintListener:
    def __init__(self, whale_addresses: set = None, tracked_addresses: set = None, max_queue: int = 100):
        self.max_queue = max_queue
        self.mints: List[Dict] = []
        self.whales: List[Dict] = []  # whale activity queue
        self.tracked_activity: List[Dict] = []  # tracked-wallet buys (for mint feed intel)
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._sub_id: Optional[str] = None
        self._last_polled_block: Optional[int] = None  # last fully processed safe block
        self._http_w3 = None  # cache the last RPC that accepted mint log filters

        # Whale tracking
        self.whale_addresses: set = whale_addresses or set()
        self.tracked_addresses: set = {a.lower() for a in (tracked_addresses or set()) if a}

        # Transaction-level aggregation for bulk whale detection
        self._tx_buffer: Dict[str, Dict] = {}  # tx_hash -> aggregated event
        self._tx_lock = asyncio.Lock()
        self._tx_cleanup_task: Optional[asyncio.Task] = None

        # Per-contract whale cooldown to prevent spam
        self._contract_whale_cooldown: Dict[str, datetime] = {}
        self._contract_whale_window: Dict[str, List[datetime]] = {}  # for dynamic spam detection
        self._dynamic_spam_contracts: Dict[str, datetime] = {}  # contract -> expiry

    # ──────────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ──────────────────────────────────────────────────────────────────────────

    async def start(self):
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        self._tx_cleanup_task = asyncio.create_task(self._tx_cleanup_loop())
        logger.info(f"EthMintListener started ({len(self.whale_addresses)} known whales, bulk threshold={WHALE_BULK_THRESHOLD})")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._tx_cleanup_task:
            self._tx_cleanup_task.cancel()
            try:
                await self._tx_cleanup_task
            except asyncio.CancelledError:
                pass
            self._tx_cleanup_task = None
        logger.info("EthMintListener stopped")

    async def get_recent_mints(self, limit: int = 10) -> List[Dict]:
        async with self._lock:
            return list(self.mints[-limit:])

    async def get_recent_whales(self, limit: int = 10) -> List[Dict]:
        async with self._lock:
            return list(self.whales[-limit:])

    async def flush_recent_mints(self, limit: int = 10) -> List[Dict]:
        """Pop and return the most recent mints from the queue."""
        async with self._lock:
            items = list(self.mints[-limit:])
            self.mints = self.mints[:-limit] if len(self.mints) > limit else []
            return items

    async def flush_recent_whales(self, limit: int = 10) -> List[Dict]:
        """Pop and return the most recent whale events from the queue."""
        async with self._lock:
            items = list(self.whales[-limit:])
            self.whales = self.whales[:-limit] if len(self.whales) > limit else []
            return items

    def update_whale_addresses(self, addresses: set):
        """Refresh the known whale list at runtime."""
        self.whale_addresses = addresses
        logger.info(f"Updated whale addresses: {len(addresses)} wallets")

    def update_tracked_addresses(self, addresses: set):
        """Refresh tracked wallet list for live/hot mint intel."""
        self.tracked_addresses = {a.lower() for a in addresses if a}
        logger.debug("Updated tracked addresses for mint intel: %s", len(self.tracked_addresses))

    def _is_tracked(self, addr: str) -> bool:
        return bool(addr and addr.lower() in self.tracked_addresses)

    async def flush_recent_tracked_activity(self, limit: int = 50) -> List[Dict]:
        async with self._lock:
            items = list(self.tracked_activity[-limit:])
            self.tracked_activity = self.tracked_activity[:-limit] if len(self.tracked_activity) > limit else []
            return items

    # ──────────────────────────────────────────────────────────────────────────
    # Transaction buffer cleanup
    # ──────────────────────────────────────────────────────────────────────────

    async def _tx_cleanup_loop(self):
        """Remove old transaction buffers every 10 seconds."""
        while self._running:
            await asyncio.sleep(10)
            await self._flush_old_tx_buffers()

    async def _flush_old_tx_buffers(self):
        """Emit aggregated events for expired buffers and clean up."""
        now = datetime.now(timezone.utc)
        to_emit = []
        async with self._tx_lock:
            expired = []
            for tx_hash, buf in self._tx_buffer.items():
                age = (now - buf["last_seen"]).total_seconds()
                if age >= 5:  # wait 5s after last event for tx to settle
                    expired.append(tx_hash)
                    # Only emit if we haven't already and count >= threshold
                    if not buf.get("emitted") and buf["count"] >= WHALE_BULK_THRESHOLD:
                        buf["emitted"] = True
                        to_emit.append(buf)
            for tx_hash in expired:
                del self._tx_buffer[tx_hash]

        for buf in to_emit:
            await self._enqueue_whale_bulk(buf)

    # ──────────────────────────────────────────────────────────────────────────
    # WebSocket loop
    # ──────────────────────────────────────────────────────────────────────────

    async def _run_loop(self):
        # On cloud hosts (Render/AWS) the free public WS nodes often accept the
        # subscription but never stream events — a silent dead-end where the
        # WS-error fallback never triggers. HTTP polling is reliable there and,
        # because it filters to mints (from==0x0) server-side, it's cheaper than
        # the WS firehose. Default to HTTP-poll mode; set MINT_LISTENER_HTTP_ONLY=0
        # to use the WebSocket subscription instead.
        http_only = os.getenv("MINT_LISTENER_HTTP_ONLY", "1").strip().lower() in ("1", "true", "yes", "on")
        if http_only:
            await self._http_poll_loop()
            return

        endpoint_idx = 0
        consecutive_errors = 0
        print(f"[MintWS] Listener loop started — {len(WS_ENDPOINTS)} WS endpoint(s) configured.")
        while self._running:
            endpoint = WS_ENDPOINTS[endpoint_idx % len(WS_ENDPOINTS)]
            try:
                await self._connect_and_listen(endpoint)
                consecutive_errors = 0
            except websockets.ConnectionClosed as e:
                logger.warning(f"WebSocket closed ({e.code}): {e.reason}")
                consecutive_errors += 1
            except Exception as e:
                err_str = str(e) or f"{type(e).__name__}"
                logger.error(f"Listener error: {err_str}")
                consecutive_errors += 1
            # Fallback: if WebSocket is consistently blocked (common on cloud IPs),
            # poll via HTTP RPC so live/hot mints keep flowing.
            if consecutive_errors >= 2 and self._running:
                print(
                    f"[MintWS] WS unavailable ({consecutive_errors} fails) — "
                    f"falling back to HTTP poll."
                )
                await self._http_poll_once()
            if self._running:
                wait = min(60, 5 * (2 ** min(consecutive_errors, 5)))
                logger.debug(f"Reconnecting in {wait}s...")
                await asyncio.sleep(wait)
                endpoint_idx += 1

    async def _http_poll_loop(self) -> None:
        """Primary mint source on cloud hosts: poll new blocks for mint logs at a
        fixed interval. Reliable and free (no WS firehose / no paid RPC needed)."""
        try:
            interval = max(8, int(os.getenv("LIVE_MINT_INTERVAL", "15")))
        except Exception:
            interval = 15
        print(f"[MintWS] HTTP-poll mode active (every {interval}s) — reliable on cloud hosts, no WS firehose.")
        while self._running:
            try:
                await self._http_poll_once()
            except Exception as e:
                print(f"[MintWS] HTTP poll loop error: {str(e)[:160]}")
            await asyncio.sleep(interval)

    async def _fetch_mint_logs(self, w3, from_block: int, to_block: int) -> List:
        """Fetch mint-only logs with the correct indexed-from topic per standard."""
        filters = (
            [ERC721_TRANSFER, NULL_ADDRESS_TOPIC],
            [ERC1155_TRANSFER_SINGLE, None, NULL_ADDRESS_TOPIC],
            [ERC1155_TRANSFER_BATCH, None, NULL_ADDRESS_TOPIC],
        )
        logs: List = []
        for topics in filters:
            batch = await asyncio.to_thread(
                w3.eth.get_logs,
                {"fromBlock": from_block, "toBlock": to_block, "topics": topics},
            )
            logs.extend(batch)
        return logs

    async def _http_poll_once(self) -> None:
        """Poll finalized mint logs via HTTP, failing over between configured RPCs."""
        candidates = []
        seen = set()

        def add_candidate(candidate) -> None:
            if candidate is None or id(candidate) in seen:
                return
            seen.add(id(candidate))
            candidates.append(candidate)

        add_candidate(self._http_w3)

        try:
            from web3 import Web3
            for url in _HTTP_POLL_RPCS:
                add_candidate(
                    Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 20}))
                )
        except Exception as exc:
            logger.debug("[HTTP Poll] Could not build RPC candidates: %s", exc)

        if not candidates:
            print(
                "[MintWS] HTTP poll: no usable RPC "
                "(set ETHEREUM_MINT_RPC_URLS for reliable mints)."
            )
            return

        errors = []
        for index, w3 in enumerate(candidates, start=1):
            try:
                current = int(await asyncio.to_thread(lambda: w3.eth.block_number))
                # Leave the newest block unprocessed until the next poll to reduce
                # duplicate/reverted alerts from short chain reorganizations.
                safe_head = max(current - 1, 0)
                if self._last_polled_block is None:
                    from_block = max(safe_head - 3, 0)
                else:
                    from_block = self._last_polled_block + 1
                    if safe_head - from_block > 25:
                        from_block = safe_head - 25
                if from_block > safe_head:
                    self._http_w3 = w3
                    return

                logs = await self._fetch_mint_logs(w3, from_block, safe_head)
                self._http_w3 = w3
                self._last_polled_block = safe_head

                def number(value, default: int = 0) -> int:
                    if value is None:
                        return default
                    if isinstance(value, str):
                        return int(value, 16) if value.startswith("0x") else int(value)
                    return int(value)

                def hex_value(value) -> str:
                    if value is None:
                        return "0x"
                    rendered = value.hex() if hasattr(value, "hex") else str(value)
                    return rendered if rendered.startswith("0x") else "0x" + rendered

                logs.sort(
                    key=lambda item: (
                        number(item.get("blockNumber")),
                        number(item.get("logIndex")),
                    )
                )
                if logs:
                    if _LOG_HEARTBEATS:
                        print(
                            f"[MintWS] HTTP poll: {len(logs)} mint log(s) "
                            f"from blocks {from_block}-{safe_head}"
                        )
                    logger.info(
                        "[HTTP Poll] Fetched %s mint event(s) from blocks %s-%s",
                        len(logs),
                        from_block,
                        safe_head,
                    )

                for log in logs:
                    raw_log = {
                        "address": str(log.get("address", "")).lower(),
                        "topics": [
                            hex_value(topic) for topic in (log.get("topics") or [])
                        ],
                        "data": hex_value(log.get("data")),
                        "transactionHash": hex_value(log.get("transactionHash")),
                        "blockNumber": hex(number(log.get("blockNumber"))),
                        "logIndex": hex(number(log.get("logIndex"))),
                    }
                    try:
                        await self._handle_log(raw_log)
                    except Exception as exc:
                        logger.warning(
                            "[HTTP Poll] Could not parse mint log %s: %s",
                            raw_log.get("transactionHash", "")[:18],
                            exc,
                        )
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if w3 is self._http_w3:
                    self._http_w3 = None
                errors.append(f"RPC {index}: {str(exc)[:100]}")

        detail = " | ".join(errors[-3:]) or "unknown RPC error"
        logger.warning("[HTTP Poll] All RPC candidates failed: %s", detail)
        print(f"[MintWS] HTTP poll failed on all RPCs: {detail[:240]}")

    async def _connect_and_listen(self, uri: str):
        async with websockets.connect(
            uri,
            ping_interval=30,
            ping_timeout=20,
            close_timeout=10,
            open_timeout=30,
            extra_headers={
                "Origin": "https://etherscan.io",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            },
        ) as ws:
            await ws.send(json.dumps({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["logs", {
                    "topics": [[ERC721_TRANSFER, ERC1155_TRANSFER_SINGLE, ERC1155_TRANSFER_BATCH]]
                }]
            }))
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=30)
            except asyncio.TimeoutError:
                logger.warning("Subscription ack timed out for %s", uri)
                return
            response = json.loads(raw)
            self._sub_id = response.get("result")
            if not self._sub_id:
                logger.error(f"Subscription failed: {response}")
                print(f"[MintWS] Subscription rejected by {uri}: {str(response)[:160]}")
                return
            print(f"[MintWS] ✅ Connected & subscribed to live mints via {uri}")

            while self._running:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=120)
                except asyncio.TimeoutError:
                    # Quiet chain periods are normal — keep the connection open.
                    continue
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                result = data.get("params", {}).get("result")
                if result:
                    await self._handle_log(result)

    # ──────────────────────────────────────────────────────────────────────────
    # Log parsing
    # ──────────────────────────────────────────────────────────────────────────

    def _is_spam_contract(self, contract: str) -> bool:
        if not contract:
            return True
        return contract.lower() in SPAM_CONTRACTS

    def _is_known_whale(self, addr: str) -> bool:
        if not addr or not self.whale_addresses:
            return False
        return addr.lower() in self.whale_addresses

    async def _add_to_tx_buffer(self, tx_hash: str, contract: str, token_id: str,
                                 amount: int, to_addr: str, from_addr: str,
                                 block_number: int, is_mint: bool) -> bool:
        """Add event to tx buffer. Returns True if threshold just reached."""
        now = datetime.now(timezone.utc)
        async with self._tx_lock:
            if tx_hash in self._tx_buffer:
                buf = self._tx_buffer[tx_hash]
                buf["count"] += amount
                buf["token_ids"].append(token_id)
                buf["last_seen"] = now
                # Check threshold
                if not buf.get("emitted") and buf["count"] >= WHALE_BULK_THRESHOLD:
                    buf["emitted"] = True
                    return True
            else:
                self._tx_buffer[tx_hash] = {
                    "tx_hash": tx_hash,
                    "contract_address": contract,
                    "to": to_addr,
                    "from": from_addr,
                    "count": amount,
                    "token_ids": [token_id],
                    "block_number": block_number,
                    "timestamp": now.isoformat(),
                    "type": "whale_mint" if is_mint else "whale_buy",
                    "emitted": False,
                    "last_seen": now,
                }
                # Single ERC1155 TransferSingle with high amount can trigger immediately
                if amount >= WHALE_BULK_THRESHOLD:
                    self._tx_buffer[tx_hash]["emitted"] = True
                    return True
        return False

    async def _handle_log(self, log: Dict):
        topics = log.get("topics", [])
        if not topics:
            return

        event_sig = topics[0].lower()
        contract = normalize_eth_address(log.get("address", ""))
        tx_hash = log.get("transactionHash", "")
        raw_block = log.get("blockNumber", "0x0")
        raw_log_index = log.get("logIndex", "0x0")
        block_number = (
            int(raw_block, 16) if isinstance(raw_block, str) else int(raw_block or 0)
        )
        log_index = (
            int(raw_log_index, 16)
            if isinstance(raw_log_index, str)
            else int(raw_log_index or 0)
        )

        if self._is_spam_contract(contract):
            return

        # ── ERC721 Transfer ──
        if event_sig == ERC721_TRANSFER and len(topics) >= 4:
            from_addr = normalize_eth_address(topics[1])
            to_addr = normalize_eth_address(topics[2])
            token_id = int(topics[3], 16)
            is_mint = from_addr == ZERO_ADDRESS
            is_whale_in = self._is_known_whale(to_addr)
            is_whale_out = self._is_known_whale(from_addr) and not is_mint

            if is_mint:
                await self._enqueue_mint({
                    "type": "erc721",
                    "contract_address": contract,
                    "token_id": str(token_id),
                    "to": to_addr,
                    "tx_hash": tx_hash,
                    "block_number": block_number,
                    "log_index": log_index,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "gas_fee": 0,
                    "mint_cost": 0,
                })
            elif self._is_tracked(to_addr):
                await self._enqueue_tracked_buy({
                    "type": "wallet_buy",
                    "contract_address": contract,
                    "token_id": str(token_id),
                    "amount": 1,
                    "from": from_addr,
                    "to": to_addr,
                    "tx_hash": tx_hash,
                    "block_number": block_number,
                    "log_index": log_index,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })

            # Wallet tracker: track mints, buys, and sells for known wallets
            if WALLET_TRACKER_ENABLED:
                if is_whale_in:
                    await self._enqueue_whale({
                        "type": "wallet_mint" if is_mint else "wallet_buy",
                        "contract_address": contract,
                        "token_id": str(token_id),
                        "amount": 1,
                        "from": from_addr,
                        "to": to_addr,
                        "tx_hash": tx_hash,
                        "block_number": block_number,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "is_known_whale": True,
                    })
                if is_whale_out:
                    await self._enqueue_whale({
                        "type": "wallet_sell",
                        "contract_address": contract,
                        "token_id": str(token_id),
                        "amount": 1,
                        "from": from_addr,
                        "to": to_addr,
                        "tx_hash": tx_hash,
                        "block_number": block_number,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "is_known_whale": True,
                    })

        # ── ERC1155 TransferSingle ──
        elif event_sig == ERC1155_TRANSFER_SINGLE and len(topics) >= 4:
            from_addr = normalize_eth_address(topics[2])
            to_addr = normalize_eth_address(topics[3])
            data = log.get("data", "")
            is_mint = from_addr == ZERO_ADDRESS and len(data) >= 130
            is_whale_in = self._is_known_whale(to_addr)
            is_whale_out = self._is_known_whale(from_addr) and not is_mint

            if is_mint and len(data) >= 130:
                token_id = int(data[2:66], 16)
                value = int(data[66:130], 16)
                await self._enqueue_mint({
                    "type": "erc1155",
                    "contract_address": contract,
                    "token_id": str(token_id),
                    "amount": value,
                    "to": to_addr,
                    "tx_hash": tx_hash,
                    "block_number": block_number,
                    "log_index": log_index,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "gas_fee": 0,
                    "mint_cost": 0,
                })
            elif self._is_tracked(to_addr) and len(data) >= 130:
                token_id = int(data[2:66], 16)
                value = int(data[66:130], 16)
                await self._enqueue_tracked_buy({
                    "type": "wallet_buy",
                    "contract_address": contract,
                    "token_id": str(token_id),
                    "amount": value,
                    "from": from_addr,
                    "to": to_addr,
                    "tx_hash": tx_hash,
                    "block_number": block_number,
                    "log_index": log_index,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })

            tid = str(int(data[2:66], 16)) if len(data) >= 130 else "?"
            value = int(data[66:130], 16) if len(data) >= 130 else 1

            if WALLET_TRACKER_ENABLED:
                if is_whale_in:
                    await self._enqueue_whale({
                        "type": "wallet_mint" if is_mint else "wallet_buy",
                        "contract_address": contract,
                        "token_id": tid,
                        "amount": value,
                        "from": from_addr,
                        "to": to_addr,
                        "tx_hash": tx_hash,
                        "block_number": block_number,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "is_known_whale": True,
                    })
                if is_whale_out:
                    await self._enqueue_whale({
                        "type": "wallet_sell",
                        "contract_address": contract,
                        "token_id": tid,
                        "amount": value,
                        "from": from_addr,
                        "to": to_addr,
                        "tx_hash": tx_hash,
                        "block_number": block_number,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "is_known_whale": True,
                    })

        # ── ERC1155 TransferBatch ──
        elif event_sig == ERC1155_TRANSFER_BATCH and len(topics) >= 4:
            from_addr = normalize_eth_address(topics[2])
            to_addr = normalize_eth_address(topics[3])
            token_ids, values = _decode_erc1155_batch(log.get("data", ""))
            batch_amount = sum(values) if values else max(len(token_ids), 1)
            token_label = str(token_ids[0]) if len(token_ids) == 1 else "batch"
            is_mint = from_addr == ZERO_ADDRESS
            is_whale_in = self._is_known_whale(to_addr)
            is_whale_out = self._is_known_whale(from_addr) and not is_mint

            if is_mint:
                await self._enqueue_mint({
                    "type": "erc1155_batch",
                    "contract_address": contract,
                    "token_id": token_label,
                    "amount": batch_amount,
                    "to": to_addr,
                    "tx_hash": tx_hash,
                    "block_number": block_number,
                    "log_index": log_index,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "gas_fee": 0,
                    "mint_cost": 0,
                })

            if WALLET_TRACKER_ENABLED:
                if is_whale_in:
                    await self._enqueue_whale({
                        "type": "wallet_mint" if is_mint else "wallet_buy",
                        "contract_address": contract,
                        "token_id": "?",
                        "amount": batch_amount,
                        "from": from_addr,
                        "to": to_addr,
                        "tx_hash": tx_hash,
                        "block_number": block_number,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "is_known_whale": True,
                    })
                if is_whale_out:
                    await self._enqueue_whale({
                        "type": "wallet_sell",
                        "contract_address": contract,
                        "token_id": "?",
                        "amount": batch_amount,
                        "from": from_addr,
                        "to": to_addr,
                        "tx_hash": tx_hash,
                        "block_number": block_number,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "is_known_whale": True,
                    })

    async def _enqueue_mint(self, item: Dict):
        async with self._lock:
            self.mints.append(item)
            if len(self.mints) > self.max_queue:
                self.mints.pop(0)
        logger.debug(f"Mint detected: {item['contract_address']} #{item['token_id']}")

    async def _enqueue_tracked_buy(self, item: Dict):
        if not self.tracked_addresses:
            return
        async with self._lock:
            self.tracked_activity.append(item)
            if len(self.tracked_activity) > self.max_queue:
                self.tracked_activity.pop(0)
        logger.debug(
            "Tracked wallet buy: %s -> %s #%s",
            item.get("to", "")[:10],
            item.get("contract_address", "")[:10],
            item.get("token_id"),
        )

    def _check_contract_cooldown(self, contract: str) -> bool:
        """Return True if contract is on cooldown and should be skipped."""
        now = datetime.now(timezone.utc)
        # Dynamic spam detection: >20 whale events in 60s = 24h ban
        if contract not in self._contract_whale_window:
            self._contract_whale_window[contract] = []
        self._contract_whale_window[contract].append(now)
        self._contract_whale_window[contract] = [
            t for t in self._contract_whale_window[contract] if (now - t).total_seconds() <= 60
        ]
        if len(self._contract_whale_window[contract]) > 20:
            if contract not in self._dynamic_spam_contracts:
                self._dynamic_spam_contracts[contract] = now + timedelta(hours=24)
                SPAM_CONTRACTS.add(contract)
                logger.warning(f"🚫 Auto-spam detected: {contract} (>20 whale events/min). 24h ban.")
            return True
        # Per-contract whale cooldown (60s)
        last = self._contract_whale_cooldown.get(contract)
        if last and (now - last).total_seconds() < 60:
            return True
        self._contract_whale_cooldown[contract] = now
        return False

    async def _enqueue_whale(self, item: Dict):
        """Emit a single known-whale event immediately."""
        contract = item.get("contract_address", "").lower()
        if self._check_contract_cooldown(contract):
            return
        async with self._lock:
            self.whales.append(item)
            if len(self.whales) > self.max_queue:
                self.whales.pop(0)
        label = item.get("to", "")[:10]
        logger.info(f"🐋 Known whale {item['type']}: {contract} x{item.get('amount',1)} -> {label}...")

    async def _enqueue_whale_bulk(self, buf: Dict):
        """Emit an aggregated bulk-whale event after tx settles."""
        contract = buf.get("contract_address", "").lower()
        if self._check_contract_cooldown(contract):
            return
        # Summarize token IDs
        tids = buf["token_ids"]
        if len(tids) > 3:
            token_summary = f"{tids[0]}, {tids[1]}, {tids[2]}... (+{len(tids)-3} more)"
        else:
            token_summary = ", ".join(tids)

        event = {
            "type": buf["type"],
            "contract_address": contract,
            "token_id": token_summary,
            "amount": buf["count"],
            "from": buf["from"],
            "to": buf["to"],
            "tx_hash": buf["tx_hash"],
            "block_number": buf["block_number"],
            "timestamp": buf["timestamp"],
            "is_known_whale": False,
            "is_bulk": True,
        }
        async with self._lock:
            self.whales.append(event)
            if len(self.whales) > self.max_queue:
                self.whales.pop(0)
        logger.info(f"🔥 Bulk whale {event['type']}: {contract} x{event['amount']} -> {event['to'][:10]}...")
