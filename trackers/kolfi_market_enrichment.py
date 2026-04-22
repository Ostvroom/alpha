"""
On-chain / market telemetry for Solana mints — public pair APIs + optional holder APIs.
Used to ground Velcor3 AI briefs with real liquidity, pair age, holders.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp

DEX_TOKEN_URL = "https://api.dexscreener.com/tokens/v1/solana/{mint}"
BIRDEYE_OVERVIEW_URL = "https://public-api.birdeye.so/defi/token_overview"


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _pick_best_pair(pairs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    best: Optional[Dict[str, Any]] = None
    best_liq = -1.0
    for p in pairs:
        if not isinstance(p, dict):
            continue
        liq = _safe_float((p.get("liquidity") or {}).get("usd"))
        liq = liq or 0.0
        if liq > best_liq:
            best_liq = liq
            best = p
    return best


def _pair_age_days(pair: Dict[str, Any]) -> Optional[float]:
    pc = pair.get("pairCreatedAt")
    if pc is None:
        return None
    try:
        ts = float(pc)
        if ts > 1e12:
            ts /= 1000.0
        now = datetime.now(timezone.utc).timestamp()
        return max(0.0, (now - ts) / 86400.0)
    except (TypeError, ValueError):
        return None


async def fetch_dexscreener_solana(
    session: aiohttp.ClientSession, mint: str
) -> Dict[str, Any]:
    """Best-liquidity pair from public token endpoint."""
    out: Dict[str, Any] = {
        "ok": False,
        "liquidity_usd": None,
        "pair_age_days": None,
        "fdv_usd": None,
        "market_cap_usd": None,
        "price_usd": None,
        "price_change_h24_pct": None,
        "volume_h24_usd": None,
        "txns_h24": None,
        "dex_id": None,
        "pair_address": None,
        "pair_url": None,
        "quote_symbol": None,
    }
    if not mint:
        return out
    url = DEX_TOKEN_URL.format(mint=mint)
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=12),
            headers={"Accept": "application/json"},
        ) as r:
            if r.status != 200:
                return out
            data = await r.json()
    except Exception:
        return out
    if not isinstance(data, list) or not data:
        return out
    pair = _pick_best_pair(data)
    if not pair:
        return out
    out["ok"] = True
    liq = pair.get("liquidity") or {}
    out["liquidity_usd"] = _safe_float(liq.get("usd"))
    out["fdv_usd"] = _safe_float(pair.get("fdv"))
    out["market_cap_usd"] = _safe_float(pair.get("marketCap")) or _safe_float(pair.get("fdv"))
    out["price_usd"] = _safe_float(pair.get("priceUsd"))
    pc = pair.get("priceChange") or {}
    if isinstance(pc, dict):
        out["price_change_h24_pct"] = _safe_float(pc.get("h24"))
    out["volume_h24_usd"] = _safe_float((pair.get("volume") or {}).get("h24"))
    tx = pair.get("txns") or {}
    h24 = tx.get("h24") or {}
    bu = h24.get("buys")
    se = h24.get("sells")
    if bu is not None or se is not None:
        out["txns_h24"] = {"buys": bu, "sells": se}
    out["dex_id"] = pair.get("dexId")
    out["pair_address"] = pair.get("pairAddress")
    out["pair_url"] = pair.get("url")
    out["pair_age_days"] = _pair_age_days(pair)
    qt = pair.get("quoteToken") or {}
    out["quote_symbol"] = qt.get("symbol")
    return out


async def fetch_birdeye_overview(
    session: aiohttp.ClientSession,
    mint: str,
    api_key: str,
) -> Dict[str, Any]:
    """Token overview: holders, liquidity, unique wallets (requires API key)."""
    out: Dict[str, Any] = {
        "ok": False,
        "holder_count": None,
        "liquidity_usd": None,
        "circulating_supply": None,
        "unique_wallet_24h": None,
    }
    if not mint or not (api_key or "").strip():
        return out
    try:
        async with session.get(
            BIRDEYE_OVERVIEW_URL,
            params={"address": mint.strip()},
            headers={
                "X-API-KEY": api_key.strip(),
                "x-chain": "solana",
                "Accept": "application/json",
            },
            timeout=aiohttp.ClientTimeout(total=12),
        ) as r:
            if r.status != 200:
                return out
            raw = await r.json()
    except Exception:
        return out
    data = raw.get("data") if isinstance(raw, dict) else None
    if not isinstance(data, dict):
        return out
    out["ok"] = True
    out["holder_count"] = data.get("holder")
    if out["holder_count"] is not None:
        try:
            out["holder_count"] = int(out["holder_count"])
        except (TypeError, ValueError):
            out["holder_count"] = None
    out["liquidity_usd"] = _safe_float(data.get("liquidity"))
    out["circulating_supply"] = _safe_float(data.get("circulatingSupply"))
    uw = data.get("uniqueWallet24h")
    if uw is None:
        uw = data.get("unique_wallet_24h")
    out["unique_wallet_24h"] = uw
    try:
        if out["unique_wallet_24h"] is not None:
            out["unique_wallet_24h"] = int(out["unique_wallet_24h"])
    except (TypeError, ValueError):
        out["unique_wallet_24h"] = None
    return out


async def enrich_solana_mint(
    session: aiohttp.ClientSession,
    mint: str,
    birdeye_api_key: str = "",
) -> Dict[str, Any]:
    """Parallel pair + holder lookups; merge into one dict for AI."""
    dex_task = fetch_dexscreener_solana(session, mint)
    be_task = fetch_birdeye_overview(session, mint, birdeye_api_key)
    dex, be = await asyncio.gather(dex_task, be_task)
    return {
        "mint": mint,
        "dexscreener": dex,
        "birdeye": be,
        "fetched_at_unix": int(time.time()),
    }
