"""
Cross-link live/hot mint embeds with the ETH wallet tracker (tracked + seed smart wallets).
Shared per-contract activity store so engagement persists across all alerts for a collection.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import wallet_database
from trackers.eth_address import normalize_eth_address

COLOR_SMART_MINT = int(os.getenv("MINT_SMART_WALLET_MINT_COLOR", "FFD700"), 16)
COLOR_SMART_BUY = int(os.getenv("MINT_SMART_WALLET_BUY_COLOR", "5865F2"), 16)
COLOR_HOT_SMART = int(os.getenv("MINT_SMART_WALLET_HOT_COLOR", "A855F7"), 16)

_wallet_mint_by_tx: Dict[str, Dict[str, Any]] = {}
_WALLET_MINT_TX_TTL_SEC = 600

# Global per-contract engagement (mint/buy by tracked wallets)
_CONTRACT_ACTIVITY: Dict[str, List[Dict[str, Any]]] = {}


def get_contract_activity() -> Dict[str, List[Dict[str, Any]]]:
    return _CONTRACT_ACTIVITY


def register_wallet_tracker_mint(
    tx_hash: str,
    wallet: str,
    label: str,
    *,
    contract: str = "",
    token_id: Optional[str] = None,
) -> None:
    tx = (tx_hash or "").strip().lower()
    w = normalize_eth_address(wallet)
    if not tx or not w:
        return
    _wallet_mint_by_tx[tx] = {
        "wallet": w,
        "label": (label or "").strip(),
        "contract": (contract or "").lower(),
        "token_id": token_id,
        "ts": time.time(),
    }
    if len(_wallet_mint_by_tx) > 2000:
        cutoff = time.time() - _WALLET_MINT_TX_TTL_SEC
        for k in list(_wallet_mint_by_tx.keys()):
            if _wallet_mint_by_tx[k].get("ts", 0) < cutoff:
                del _wallet_mint_by_tx[k]


def _lookup_wallet_tracker_mint(mint: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    tx = ((mint.get("tx_hash") or mint.get("hash") or "")).strip().lower()
    if not tx:
        return None
    entry = _wallet_mint_by_tx.get(tx)
    if not entry:
        return None
    if time.time() - float(entry.get("ts") or 0) > _WALLET_MINT_TX_TTL_SEC:
        _wallet_mint_by_tx.pop(tx, None)
        return None
    return entry


def get_tracked_wallet_map() -> Dict[str, str]:
    try:
        from trackers.eth_tracker import tracked_eth_wallets

        out = {k.lower(): (v or "").strip() for k, v in tracked_eth_wallets.items() if k}
    except Exception:
        out = {}
    if not out:
        try:
            out = {
                k.lower(): (v or "").strip()
                for k, v in wallet_database.get_wallets_by_chain("ETH").items()
            }
        except Exception:
            pass
    return out


def trader_display(label: str, address: str) -> str:
    label = (label or "").strip()
    if label:
        return label
    addr = (address or "").lower()
    if len(addr) >= 10:
        return f"{addr[:6]}...{addr[-4:]}"
    return addr or "Unknown"


def _activity_store(store: Optional[Dict[str, List[Dict[str, Any]]]]) -> Dict[str, List[Dict[str, Any]]]:
    return store if store is not None else _CONTRACT_ACTIVITY


def _append_activity(
    store: Dict[str, List[Dict[str, Any]]],
    contract: str,
    entry: Dict[str, Any],
) -> None:
    contract = normalize_eth_address(contract) or contract.lower()
    if not contract:
        return
    store.setdefault(contract, []).append(entry)
    if len(store[contract]) > 200:
        store[contract] = store[contract][-200:]


def record_tracked_mint(
    store: Dict[str, List[Dict[str, Any]]],
    mint: Dict[str, Any],
    wallets: Optional[Dict[str, str]] = None,
) -> None:
    """Remember a tracked-wallet mint for this collection (shown on later alerts)."""
    wallets = wallets or get_tracked_wallet_map()
    to_addr = normalize_eth_address(mint.get("to") or "")
    if not to_addr or to_addr not in wallets:
        return
    contract = normalize_eth_address(mint.get("contract_address") or "")
    if not contract:
        return
    _append_activity(
        store,
        contract,
        {
            "wallet": to_addr,
            "label": trader_display(wallets[to_addr], to_addr),
            "action": "mint",
            "token_id": mint.get("token_id"),
            "tx_hash": mint.get("tx_hash") or mint.get("hash"),
            "timestamp": mint.get("timestamp") or datetime.now(timezone.utc).isoformat(),
            "x_url": wallet_database.get_x_url(to_addr),
        },
    )


def record_tracked_buy(
    store: Dict[str, List[Dict[str, Any]]],
    event: Dict[str, Any],
    wallets: Optional[Dict[str, str]] = None,
) -> None:
    wallets = wallets or get_tracked_wallet_map()
    to_addr = normalize_eth_address(event.get("to") or "")
    if not to_addr or to_addr not in wallets:
        return
    contract = normalize_eth_address(event.get("contract_address") or "")
    if not contract:
        return
    _append_activity(
        store,
        contract,
        {
            "wallet": to_addr,
            "label": trader_display(wallets[to_addr], to_addr),
            "action": "buy",
            "token_id": event.get("token_id"),
            "tx_hash": event.get("tx_hash"),
            "timestamp": event.get("timestamp") or datetime.now(timezone.utc).isoformat(),
            "x_url": wallet_database.get_x_url(to_addr),
        },
    )


def _events_in_window(
    contract_activity: Dict[str, List[Dict[str, Any]]],
    contract: str,
    window_seconds: int,
    action: Optional[str] = None,
) -> List[Dict[str, Any]]:
    contract = contract.lower()
    events = contract_activity.get(contract) or []
    if not events:
        return []
    now = datetime.now(timezone.utc)
    seen: set = set()
    out: List[Dict[str, Any]] = []
    for ev in reversed(events):
        if action and ev.get("action") != action:
            continue
        try:
            ts = datetime.fromisoformat(str(ev.get("timestamp", "")).replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if (now - ts).total_seconds() > window_seconds:
            continue
        w = (ev.get("wallet") or "").lower()
        key = (w, ev.get("action"), ev.get("token_id"), ev.get("tx_hash"))
        if w and key not in seen:
            seen.add(key)
            out.append(ev)
    return list(reversed(out))


def buyers_in_window(
    contract_activity: Dict[str, List[Dict[str, Any]]],
    contract: str,
    window_seconds: int,
) -> List[Dict[str, Any]]:
    return _events_in_window(contract_activity, contract, window_seconds, "buy")


def minters_in_window(
    contract_activity: Dict[str, List[Dict[str, Any]]],
    contract: str,
    window_seconds: int,
    *,
    exclude_wallet: str = "",
) -> List[Dict[str, Any]]:
    ex = normalize_eth_address(exclude_wallet)
    mints = _events_in_window(contract_activity, contract, window_seconds, "mint")
    if not ex:
        return mints
    return [m for m in mints if (m.get("wallet") or "").lower() != ex]


def enrich_mint_with_wallet_intel(mint: Dict[str, Any]) -> Dict[str, Any]:
    wallets = get_tracked_wallet_map()
    to_addr = normalize_eth_address(mint.get("to") or "")
    if to_addr:
        mint["to"] = to_addr

    label = ""
    wallet_key = ""
    if to_addr and to_addr in wallets:
        wallet_key = to_addr
        label = trader_display(wallets[to_addr], to_addr)
    else:
        cached = _lookup_wallet_tracker_mint(mint)
        if cached:
            wallet_key = cached.get("wallet") or ""
            label = trader_display(cached.get("label") or "", wallet_key)
            if wallet_key:
                mint["to"] = wallet_key

    if wallet_key and label:
        mint["tracked_trader"] = label
        mint["tracked_wallet"] = wallet_key
        mint["tracked_action"] = "mint"
        mint["is_smart_wallet_event"] = True
        mint["smart_embed_color"] = COLOR_SMART_MINT
        x_url = wallet_database.get_x_url(wallet_key)
        if x_url:
            mint["tracked_trader_x"] = x_url
    return mint


def attach_collection_wallet_intel(
    mint: Dict[str, Any],
    contract_activity: Dict[str, List[Dict[str, Any]]],
    window_seconds: int,
) -> Dict[str, Any]:
    """
    Attach current minter + recent tracked mints/buys on this collection to the embed payload.
    """
    store = _activity_store(contract_activity)
    mint = enrich_mint_with_wallet_intel(mint)
    contract = (mint.get("contract_address") or "").lower()

    if mint.get("tracked_wallet"):
        record_tracked_mint(store, mint)

    buyers = buyers_in_window(store, contract, window_seconds)
    other_minters = minters_in_window(
        store, contract, window_seconds, exclude_wallet=mint.get("tracked_wallet") or ""
    )

    if buyers:
        mint["smart_wallet_buys"] = buyers
    if other_minters:
        mint["smart_wallet_mints"] = other_minters

    if buyers or other_minters or mint.get("tracked_trader"):
        mint["collection_smart_engagement"] = True
        mint["is_smart_wallet_event"] = True
        if mint.get("tracked_action") == "mint":
            mint["smart_embed_color"] = COLOR_SMART_MINT
        elif buyers:
            mint["smart_embed_color"] = COLOR_SMART_BUY
        elif other_minters:
            mint["smart_embed_color"] = COLOR_SMART_BUY

    return mint


def attach_hot_mint_wallet_intel(
    mint: Dict[str, Any],
    contract_activity: Dict[str, List[Dict[str, Any]]],
    window_seconds: int,
) -> Dict[str, Any]:
    mint = attach_collection_wallet_intel(mint, contract_activity, window_seconds)
    if mint.get("tracked_action") == "mint" and mint.get("collection_smart_engagement"):
        mint["smart_embed_color"] = COLOR_HOT_SMART
    return mint


def prune_contract_activity(
    contract_activity: Dict[str, List[Dict[str, Any]]],
    window_seconds: int,
) -> None:
    now = datetime.now(timezone.utc)
    store = _activity_store(contract_activity)
    for contract in list(store.keys()):
        kept = []
        for ev in store[contract]:
            try:
                ts = datetime.fromisoformat(str(ev.get("timestamp", "")).replace("Z", "+00:00"))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if (now - ts).total_seconds() <= window_seconds * 2:
                    kept.append(ev)
            except Exception:
                pass
        if kept:
            store[contract] = kept[-200:]
        else:
            store.pop(contract, None)


def format_trader_line(mint: Dict[str, Any]) -> Optional[str]:
    label = mint.get("tracked_trader")
    wallet = mint.get("tracked_wallet")
    if not label or not wallet:
        return None
    x_url = mint.get("tracked_trader_x")
    if x_url:
        return f"🧠 **Smart wallet mint ·** [{label}]({x_url}) · [Etherscan](https://etherscan.io/address/{wallet})"
    return f"🧠 **Smart wallet mint ·** [{label}](https://etherscan.io/address/{wallet})"


def format_smart_buys_line(buyers: List[Dict[str, Any]], max_names: int = 5) -> Optional[str]:
    if not buyers:
        return None
    parts: List[str] = []
    for b in buyers[:max_names]:
        label = b.get("label") or "?"
        x_url = b.get("x_url")
        if x_url:
            parts.append(f"[**{label}**]({x_url})")
        else:
            w = b.get("wallet", "")
            parts.append(f"[**{label}**](https://etherscan.io/address/{w})")
    extra = len(buyers) - max_names
    line = "🛒 **Smart wallet buys ·** " + ", ".join(parts)
    if extra > 0:
        line += f" +{extra} more"
    return line


def format_smart_mints_line(minters: List[Dict[str, Any]], max_names: int = 5) -> Optional[str]:
    if not minters:
        return None
    parts: List[str] = []
    for m in minters[:max_names]:
        label = m.get("label") or "?"
        x_url = m.get("x_url")
        if x_url:
            parts.append(f"[**{label}**]({x_url})")
        else:
            w = m.get("wallet", "")
            parts.append(f"[**{label}**](https://etherscan.io/address/{w})")
    extra = len(minters) - max_names
    line = "⚡ **Smart wallet minted ·** " + ", ".join(parts)
    if extra > 0:
        line += f" +{extra} more"
    return line


def format_collection_engagement_lines(mint: Dict[str, Any]) -> List[str]:
    """Lines for live/hot embeds when smart wallets engaged with this collection."""
    lines: List[str] = []
    if not mint.get("collection_smart_engagement"):
        return lines
    header = "🧠 **Smart wallets engaged with this collection**"
    lines.append(header)
    trader_line = format_trader_line(mint)
    if trader_line and trader_line not in lines:
        lines.append(trader_line.replace("🧠 **Smart wallet mint ·**", "▸ Mint ·"))
    buys_line = format_smart_buys_line(mint.get("smart_wallet_buys") or [])
    if buys_line:
        lines.append(buys_line.replace("🛒 **Smart wallet buys ·**", "▸ Buys ·"))
    mints_line = format_smart_mints_line(mint.get("smart_wallet_mints") or [])
    if mints_line:
        lines.append(mints_line.replace("⚡ **Smart wallet minted ·**", "▸ Minted ·"))
    return lines


def resolve_embed_color(mint: Dict[str, Any], default: int) -> int:
    if mint.get("is_smart_wallet_event") and mint.get("smart_embed_color"):
        return int(mint["smart_embed_color"])
    return default
