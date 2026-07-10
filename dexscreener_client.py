from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp


DEX_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search"


def _number(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _same_address(left: str, right: str) -> bool:
    if left.lower().startswith("0x") and right.lower().startswith("0x"):
        return left.lower() == right.lower()
    return left == right


async def find_dexscreener_token(contract_address: str) -> Optional[dict[str, Any]]:
    """Find an exact token address and select its strongest liquidity pool."""
    timeout = aiohttp.ClientTimeout(total=10)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                DEX_SEARCH_URL,
                params={"q": contract_address},
                headers={"Accept": "application/json"},
            ) as response:
                if response.status != 200:
                    return None
                payload = await response.json()
    except (aiohttp.ClientError, TimeoutError, ValueError):
        return None

    if not isinstance(payload, dict):
        return None

    exact_pairs = []
    for pair in payload.get("pairs") or []:
        if not isinstance(pair, dict):
            continue
        base = pair.get("baseToken") or {}
        quote = pair.get("quoteToken") or {}
        if (
            _same_address(str(base.get("address", "")), contract_address)
            or _same_address(str(quote.get("address", "")), contract_address)
        ):
            exact_pairs.append(pair)

    if not exact_pairs:
        return None

    pair = max(
        exact_pairs,
        key=lambda item: (
            _number((item.get("liquidity") or {}).get("usd")),
            _number((item.get("volume") or {}).get("h24")),
        ),
    )
    base = pair.get("baseToken") or {}
    quote = pair.get("quoteToken") or {}
    token = (
        base
        if _same_address(str(base.get("address", "")), contract_address)
        else quote
    )

    created_at = _number(pair.get("pairCreatedAt"))
    age_hours = None
    if created_at:
        if created_at > 10_000_000_000:
            created_at /= 1000
        age_hours = max(
            0.0,
            (datetime.now(timezone.utc).timestamp() - created_at) / 3600,
        )

    info = pair.get("info") or {}
    txns_h24 = ((pair.get("txns") or {}).get("h24") or {})
    return {
        "chain": str(pair.get("chainId") or "unknown"),
        "dex": str(pair.get("dexId") or "unknown"),
        "url": str(pair.get("url") or ""),
        "name": str(token.get("name") or "Unknown token"),
        "symbol": str(token.get("symbol") or "?"),
        "price": _number(pair.get("priceUsd")),
        "fdv": _number(pair.get("fdv")),
        "market_cap": _number(pair.get("marketCap")) or _number(pair.get("fdv")),
        "liquidity": _number((pair.get("liquidity") or {}).get("usd")),
        "volume_h24": _number((pair.get("volume") or {}).get("h24")),
        "change_h1": _number((pair.get("priceChange") or {}).get("h1")),
        "change_h24": _number((pair.get("priceChange") or {}).get("h24")),
        "buys_h24": int(_number(txns_h24.get("buys"))),
        "sells_h24": int(_number(txns_h24.get("sells"))),
        "age_hours": age_hours,
        "image_url": info.get("imageUrl"),
        "detected_chains": sorted({
            str(item.get("chainId"))
            for item in exact_pairs
            if item.get("chainId")
        }),
    }