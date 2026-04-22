"""On-chain verification for premium payments (EVM native ETH, Solana native SOL)."""
from __future__ import annotations

import json
import re
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

import config

EVM_TX_RE = re.compile(r"^0x[a-fA-F0-9]{64}$")
# Solana signatures are base58; typical length 87–88
SOL_SIG_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{64,128}$")


def normalize_evm_tx_hash(raw: str) -> Optional[str]:
    s = raw.strip()
    if not s.startswith("0x"):
        s = "0x" + s
    if not EVM_TX_RE.match(s):
        return None
    return s.lower()


def normalize_sol_signature(raw: str) -> Optional[str]:
    s = raw.strip()
    if not SOL_SIG_RE.match(s):
        return None
    return s


def eth_to_wei(eth: float) -> int:
    d = Decimal(str(eth)) * Decimal(10**18)
    return int(d.quantize(Decimal("1"), rounding=ROUND_DOWN))


def sol_to_lamports(sol: float) -> int:
    d = Decimal(str(sol)) * Decimal(10**9)
    return int(d.quantize(Decimal("1"), rounding=ROUND_DOWN))


def _solana_rpc_url() -> str:
    key = getattr(config, "HELIUS_API_KEY", None) or ""
    if key:
        return f"https://mainnet.helius-rpc.com/?api-key={key}"
    return (config.SOLANA_RPC_URL or "").strip() or "https://api.mainnet-beta.solana.com"


def _parse_evm_result(result: Any) -> Optional[Dict[str, Any]]:
    if result is None or result == "":
        return None
    if isinstance(result, dict):
        return result if result else None
    if isinstance(result, str):
        try:
            parsed = json.loads(result)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


async def _etherscan_proxy(
    session: aiohttp.ClientSession,
    chain_id: int,
    action: str,
    tx_hash: str,
) -> Tuple[Optional[Dict[str, Any]], str]:
    api_key = (config.ETHSCAN_API_KEY or "").strip()
    if not api_key:
        return None, "ETHSCAN_API_KEY is not configured"
    url = (
        f"https://api.etherscan.io/v2/api?chainid={chain_id}&module=proxy"
        f"&action={action}&txhash={tx_hash}&apikey={api_key}"
    )
    async with session.get(url, timeout=45) as r:
        try:
            data = await r.json()
        except Exception as e:
            return None, f"Etherscan response error: {e}"
    if data.get("status") == "0":
        msg = data.get("message") or data.get("result") or "Etherscan error"
        if isinstance(msg, str) and "rate limit" in msg.lower():
            return None, "Etherscan rate limit — try again shortly"
        return None, str(msg)[:500]
    res = _parse_evm_result(data.get("result"))
    if not res:
        return None, "Transaction not found or still pending"
    return res, ""


async def _etherscan_block_number(session: aiohttp.ClientSession, chain_id: int) -> Tuple[Optional[int], str]:
    api_key = (config.ETHSCAN_API_KEY or "").strip()
    if not api_key:
        return None, "ETHSCAN_API_KEY is not configured"
    url = (
        f"https://api.etherscan.io/v2/api?chainid={chain_id}&module=proxy"
        f"&action=eth_blockNumber&apikey={api_key}"
    )
    async with session.get(url, timeout=45) as r:
        data = await r.json()
    res = data.get("result")
    if not res or not isinstance(res, str):
        return None, "Could not read block number"
    try:
        return int(res, 16), ""
    except ValueError:
        return None, "Invalid block number"


async def verify_evm_native_payment(
    session: aiohttp.ClientSession,
    chain_id: int,
    tx_hash: str,
    treasury: str,
    min_wei: int,
    min_confirmations: int,
) -> Tuple[bool, str, int]:
    """
    Verify a native ETH transfer to `treasury` with value >= min_wei.
    Returns (ok, message, value_wei).
    """
    if min_wei <= 0:
        return False, "This tier/chain is not enabled (minimum amount is 0).", 0
    t = treasury.strip().lower()
    if not t.startswith("0x"):
        t = "0x" + t
    tx, err = await _etherscan_proxy(session, chain_id, "eth_getTransactionByHash", tx_hash)
    if err:
        return False, err, 0
    assert tx is not None
    if not tx.get("blockNumber"):
        return False, "Transaction is still pending — wait for confirmations and try again.", 0
    to_addr = tx.get("to")
    if not to_addr:
        return False, "This transaction has no recipient (contract creation).", 0
    if str(to_addr).lower() != t:
        return False, "Recipient address does not match our payment wallet for this network.", 0
    value_hex = tx.get("value")
    if not value_hex:
        return False, "Transaction has no ETH value.", 0
    try:
        value_wei = int(value_hex, 16)
    except (TypeError, ValueError):
        return False, "Could not parse transaction value.", 0
    if value_wei < min_wei:
        return (
            False,
            f"Amount too low: sent {value_wei / 1e18:.6f} ETH, need at least {min_wei / 1e18:.6f} ETH.",
            value_wei,
        )
    receipt, rerr = await _etherscan_proxy(session, chain_id, "eth_getTransactionReceipt", tx_hash)
    if rerr or not receipt:
        return False, rerr or "Could not load transaction receipt.", value_wei
    status = receipt.get("status")
    if status not in ("0x1", 1, "1"):
        return False, "Transaction failed on-chain.", value_wei
    block_hex = tx.get("blockNumber")
    if not block_hex:
        return False, "Transaction has no block yet.", value_wei
    try:
        tx_block = int(block_hex, 16)
    except (TypeError, ValueError):
        return False, "Could not parse block number.", value_wei
    cur, berr = await _etherscan_block_number(session, chain_id)
    if berr or cur is None:
        return False, berr or "Could not get current block.", value_wei
    confs = cur - tx_block + 1
    if confs < min_confirmations:
        return (
            False,
            f"Not enough confirmations yet ({confs}/{min_confirmations}). Wait and try again.",
            value_wei,
        )
    return True, "Verified.", value_wei


def _pubkey_str(k: Any) -> str:
    if isinstance(k, dict):
        return str(k.get("pubkey") or k.get("pubKey") or "")
    return str(k)


def _full_solana_account_keys(tx: Dict[str, Any]) -> List[str]:
    """Account list order must match meta pre/post balances (incl. address lookup tables)."""
    msg = tx.get("transaction", {}).get("message", {})
    keys: List[str] = []
    raw_keys = msg.get("accountKeys") or []
    for k in raw_keys:
        keys.append(_pubkey_str(k))
    meta = tx.get("meta") or {}
    loaded = meta.get("loadedAddresses") or {}
    for k in loaded.get("writable", []) or []:
        keys.append(str(k))
    for k in loaded.get("readonly", []) or []:
        keys.append(str(k))
    return keys


async def verify_solana_native_payment(
    session: aiohttp.ClientSession,
    signature: str,
    treasury: str,
    min_lamports: int,
) -> Tuple[bool, str, int]:
    if min_lamports <= 0:
        return False, "This tier/chain is not enabled (minimum amount is 0).", 0
    tre = treasury.strip()
    rpc = _solana_rpc_url()
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0,
                "commitment": "finalized",
            },
        ],
    }
    async with session.post(rpc, json=payload, timeout=45) as r:
        try:
            body = await r.json()
        except Exception as e:
            return False, f"RPC error: {e}", 0
    err = body.get("error")
    if err:
        return False, f"Solana RPC: {err.get('message', err)}", 0
    result = body.get("result")
    if not result:
        return False, "Transaction not found or not yet finalized.", 0
    meta = result.get("meta") or {}
    if meta.get("err"):
        return False, "Transaction failed on-chain.", 0
    account_keys = _full_solana_account_keys(result)
    pre = meta.get("preBalances") or []
    post = meta.get("postBalances") or []
    if len(account_keys) != len(pre) or len(pre) != len(post):
        return False, "Could not parse account balances for this transaction.", 0
    try:
        idx = account_keys.index(tre)
    except ValueError:
        return False, "Treasury wallet is not in this transaction.", 0
    delta = post[idx] - pre[idx]
    if delta < min_lamports:
        return (
            False,
            f"Amount too low: received {delta / 1e9:.6f} SOL, need at least {min_lamports / 1e9:.6f} SOL.",
            delta,
        )
    return True, "Verified.", delta
