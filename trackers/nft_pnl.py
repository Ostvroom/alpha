"""
NFT /pnl for Discord — **Moralis only** (`MORALIS_API_KEY`).

- `GET …/wallets/{address}/nfts/trades` — marketplace buys/sells
- `GET …/{address}/nft/transfers` — ERC-721/1155 transfers; **mints** = from `0x0` → wallet
"""
import asyncio
import aiohttp
import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from discord import Color, Embed

PNL_CHAIN_META: Dict[str, Tuple[str, str]] = {
    "eth": ("Ethereum", "ETH"),
    "polygon": ("Polygon", "MATIC"),
    "base": ("Base", "ETH"),
    "arbitrum": ("Arbitrum", "ETH"),
    "optimism": ("Optimism", "ETH"),
}

MORALIS_CHAIN: Dict[str, str] = {
    "eth": "eth",
    "polygon": "polygon",
    "base": "base",
    "arbitrum": "arbitrum",
    "optimism": "optimism",
}

MORALIS_TRADES_URL = "https://deep-index.moralis.io/api/v2.2/wallets/{address}/nfts/trades"
MORALIS_NFT_TRANSFERS_URL = "https://deep-index.moralis.io/api/v2.2/{address}/nft/transfers"

ZERO_ADDR = "0x0000000000000000000000000000000000000000"

PNL_VALID_CHAINS = frozenset(PNL_CHAIN_META.keys())


def _moralis_period_params(moralis_days_override: Optional[int] = None) -> Tuple[Dict[str, str], str]:
    """
    Build Moralis trade query params to limit time range (fewer rows → less CU).

    Priority:
    1. moralis_days_override: >0 = last N days UTC; 0 = no filter (all time).
    2. Env PNL_MORALIS_FROM_BLOCK / TO_BLOCK (either; Moralis may require a start bound).
    3. Env PNL_MORALIS_FROM_DATE / TO_DATE (ISO or strings Moralis accepts).
    4. Env PNL_MORALIS_DAYS = rolling window from now.
    5. No filter (all time).
    """
    p: Dict[str, str] = {}

    if moralis_days_override is not None:
        if moralis_days_override > 0:
            days = max(1, min(3650, int(moralis_days_override)))
            from_dt = datetime.now(timezone.utc) - timedelta(days=days)
            iso = from_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
            p["from_date"] = iso
            return p, f"last **{days}** day(s) from `{iso[:10]}` UTC (per-request)"
        return {}, "all time — no date filter (**higher CU** / pagination)"

    fb = (os.getenv("PNL_MORALIS_FROM_BLOCK") or "").strip()
    tb = (os.getenv("PNL_MORALIS_TO_BLOCK") or "").strip()
    if fb or tb:
        if fb:
            p["from_block"] = fb
        if tb:
            p["to_block"] = tb
        return p, f"blocks **{fb or '…'}** → **{tb or '…'}**"

    fd = (os.getenv("PNL_MORALIS_FROM_DATE") or "").strip()
    td = (os.getenv("PNL_MORALIS_TO_DATE") or "").strip()
    if fd or td:
        if fd:
            p["from_date"] = fd
        if td:
            p["to_date"] = td
        return p, f"dates **{fd or '…'}** → **{td or '…'}**"

    days_s = (os.getenv("PNL_MORALIS_DAYS") or "").strip()
    if days_s:
        try:
            days = max(1, min(3650, int(days_s)))
        except ValueError:
            days = 0
        if days:
            from_dt = datetime.now(timezone.utc) - timedelta(days=days)
            iso = from_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
            p["from_date"] = iso
            return p, f"last **{days}** day(s) from `{iso[:10]}` UTC (`PNL_MORALIS_DAYS`)"

    return {}, "all time — no filter (set **`PNL_MORALIS_DAYS`** or **`/pnl` … **`days`** to save CU)"


def _moralis_key() -> str:
    return (os.getenv("MORALIS_API_KEY") or "").strip()


async def _moralis_paginated_result(
    session: aiohttp.ClientSession,
    url: str,
    moralis_chain: str,
    api_key: str,
    page_limit: int,
    max_pages: int,
    period_params: Optional[Dict[str, str]],
    extra_params: Optional[Dict[str, str]] = None,
) -> Tuple[List[dict], bool, Optional[str]]:
    """Generic Moralis paginated `result` list. Returns (rows, hit_cap, error_message)."""
    all_rows: List[dict] = []
    hit_cap = False
    cursor: Optional[str] = None
    period_params = period_params or {}
    extra = extra_params or {}

    for page_idx in range(max_pages):
        params: Dict[str, str] = {
            "chain": moralis_chain,
            "limit": str(page_limit),
            **extra,
            **period_params,
        }
        if cursor:
            params["cursor"] = cursor
        headers = {"X-API-Key": api_key}
        try:
            async with session.get(
                url,
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=45),
            ) as r:
                text = await r.text()
                if r.status == 401:
                    return [], False, "Moralis returned **401** — check **MORALIS_API_KEY**."
                if r.status == 429:
                    return [], False, "Moralis **rate limit** — try again shortly."
                if r.status >= 400:
                    return [], False, f"Moralis HTTP **{r.status}**."
                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    return [], False, "Moralis returned non-JSON."
        except Exception as e:
            return [], False, f"Moralis request error: {e}"

        if not isinstance(data, dict):
            return [], False, "Unexpected Moralis response."

        rows = data.get("result")
        if not isinstance(rows, list):
            rows = []
        all_rows.extend([x for x in rows if isinstance(x, dict)])

        next_cursor = data.get("cursor")
        if not next_cursor or not rows:
            break
        if page_idx == max_pages - 1:
            hit_cap = True
            break
        cursor = str(next_cursor)
        await asyncio.sleep(0.12)

    return all_rows, hit_cap, None


async def _fetch_moralis_wallet_trades(
    session: aiohttp.ClientSession,
    wallet: str,
    moralis_chain: str,
    api_key: str,
    page_limit: int,
    max_pages: int,
    period_params: Optional[Dict[str, str]] = None,
) -> Tuple[List[dict], bool, Optional[str]]:
    url = MORALIS_TRADES_URL.format(address=wallet)
    return await _moralis_paginated_result(
        session,
        url,
        moralis_chain,
        api_key,
        page_limit,
        max_pages,
        period_params,
        {"nft_metadata": "false"},
    )


async def _fetch_moralis_nft_transfers(
    session: aiohttp.ClientSession,
    wallet: str,
    moralis_chain: str,
    api_key: str,
    page_limit: int,
    max_pages: int,
    period_params: Optional[Dict[str, str]] = None,
) -> Tuple[List[dict], bool, Optional[str]]:
    url = MORALIS_NFT_TRANSFERS_URL.format(address=wallet)
    return await _moralis_paginated_result(
        session,
        url,
        moralis_chain,
        api_key,
        page_limit,
        max_pages,
        period_params,
        {"order": "DESC", "format": "decimal", "nft_metadata": "false"},
    )


def _aggregate_moralis_trades(wallet_lower: str, trades: List[dict]) -> Tuple[int, int, int, int]:
    """Returns (buy_count, sell_count, buy_wei, sell_wei)."""
    buy_n = sell_n = 0
    buy_wei = sell_wei = 0
    for row in trades:
        buyer = (row.get("buyer_address") or "").lower()
        seller = (row.get("seller_address") or "").lower()
        try:
            price = int(row.get("price") or 0)
        except (TypeError, ValueError):
            price = 0
        if buyer == wallet_lower:
            buy_n += 1
            buy_wei += price
        if seller == wallet_lower:
            sell_n += 1
            sell_wei += price
    return buy_n, sell_n, buy_wei, sell_wei


def _aggregate_mints_from_transfers(wallet_lower: str, transfers: List[dict]) -> Tuple[int, int]:
    """
    Count mints (ERC-721-style: from zero address → wallet) and sum native `value` on those logs (wei).
    Mint paid via router/WETH may show value 0 here.
    """
    z = ZERO_ADDR.lower()
    mint_n = 0
    mint_wei = 0
    for row in transfers:
        to_a = (row.get("to_address") or "").lower()
        from_a = (row.get("from_address") or "").lower()
        if to_a != wallet_lower or from_a != z:
            continue
        mint_n += 1
        try:
            mint_wei += int(row.get("value") or 0)
        except (TypeError, ValueError):
            pass
    return mint_n, mint_wei


def _moralis_single_trade_extremes(wallet_lower: str, trades: List[dict]) -> Tuple[Optional[int], Optional[int]]:
    """Largest one-row price when wallet is seller (best sale) vs buyer (largest buy). Prices in wei."""
    max_sell: Optional[int] = None
    max_buy: Optional[int] = None
    for row in trades:
        buyer = (row.get("buyer_address") or "").lower()
        seller = (row.get("seller_address") or "").lower()
        try:
            price = int(row.get("price") or 0)
        except (TypeError, ValueError):
            price = 0
        if price <= 0:
            continue
        if seller == wallet_lower:
            max_sell = price if max_sell is None else max(max_sell, price)
        if buyer == wallet_lower:
            max_buy = price if max_buy is None else max(max_buy, price)
    return max_sell, max_buy


async def get_wallet_pnl(
    wallet_address: str,
    chain: str = "eth",
    moralis_days: Optional[int] = None,
) -> Dict[str, Any]:
    wallet_address = (wallet_address or "").strip()
    if not re.match(r"^0x[a-fA-F0-9]{40}$", wallet_address):
        return {"error": "Invalid wallet address. Use a 0x-prefixed 40-hex EVM address."}

    chain_key = (chain or "eth").lower()
    meta = PNL_CHAIN_META.get(chain_key)
    if not meta:
        return {"error": "Invalid chain."}

    chain_name, symbol = meta
    moralis_chain = MORALIS_CHAIN.get(chain_key)
    if not moralis_chain:
        return {"error": "Invalid chain."}

    if not _moralis_key():
        return {"error": "Add **MORALIS_API_KEY** to `.env` for `/pnl`."}

    try:
        moralis_limit = max(10, min(100, int(os.getenv("PNL_MORALIS_PAGE_LIMIT", "100"))))
    except ValueError:
        moralis_limit = 100
    try:
        moralis_max_pages = max(1, min(200, int(os.getenv("PNL_MORALIS_MAX_PAGES", "40"))))
    except ValueError:
        moralis_max_pages = 40

    wl = wallet_address.lower()
    moralis_period_q, moralis_period_note = _moralis_period_params(moralis_days)
    key = _moralis_key()

    async with aiohttp.ClientSession() as session:
        (trades, hit_cap, api_err), (xfers, hit_xfer_cap, xfer_err) = await asyncio.gather(
            _fetch_moralis_wallet_trades(
                session,
                wallet_address,
                moralis_chain,
                key,
                moralis_limit,
                moralis_max_pages,
                moralis_period_q,
            ),
            _fetch_moralis_nft_transfers(
                session,
                wallet_address,
                moralis_chain,
                key,
                moralis_limit,
                moralis_max_pages,
                moralis_period_q,
            ),
        )
    if api_err:
        return {"error": api_err}

    if xfer_err:
        xfers = []

    buy_n, sell_n, buy_wei, sell_wei = _aggregate_moralis_trades(wl, trades)
    est_buy = buy_wei / 1e18
    est_sell = sell_wei / 1e18
    mint_n, mint_wei = _aggregate_mints_from_transfers(wl, xfers)
    mint_spend = mint_wei / 1e18

    total_cost = est_buy + mint_spend
    net_t = est_sell - total_cost
    max_sell_w, max_buy_w = _moralis_single_trade_extremes(wl, trades)
    best_trade = max_sell_w / 1e18 if max_sell_w else None
    worst_trade = max_buy_w / 1e18 if max_buy_w else None
    if total_cost > 1e-18:
        pnl_percent = (net_t / total_cost) * 100.0
    else:
        pnl_percent = None

    max_rows = moralis_max_pages * moralis_limit
    scope_note = (
        f"**{len(trades):,}** marketplace · **{len(xfers):,}** transfer rows · _{moralis_period_note}_"
    )
    if xfer_err:
        scope_note += f" _(Mints: transfer API error — {xfer_err})_"
    if hit_cap and len(trades) >= max_rows:
        scope_note += f" · trades cap **{max_rows:,}**"
    if hit_xfer_cap and len(xfers) >= max_rows:
        scope_note += f" · transfers cap **{max_rows:,}**"

    return {
        "mode": "moralis_trades",
        "wallet": wallet_address,
        "chain": chain_name,
        "symbol": symbol,
        "moralis_period_note": moralis_period_note,
        "bought_trades": buy_n,
        "sold_trades": sell_n,
        "est_buy_volume": est_buy,
        "est_sell_volume": est_sell,
        "mint_count": mint_n,
        "mint_spend": mint_spend,
        "net_trades": net_t,
        "pnl_percent": pnl_percent,
        "best_trade": best_trade,
        "worst_trade": worst_trade,
        "trades_rows": len(trades),
        "transfer_rows": len(xfers),
        "hit_row_cap": hit_cap,
        "hit_transfer_cap": hit_xfer_cap,
        "xfer_fetch_error": xfer_err,
        "scope_note": scope_note,
    }


def format_pnl_embed(data: Dict[str, Any]) -> Embed:
    if "error" in data:
        return Embed(title="❌ PNL Error", description=str(data["error"]), color=Color.red())

    if data.get("mode") == "moralis_trades":
        sym = data["symbol"]
        net = float(data.get("net_trades") or 0)
        report_color = 5814783
        total_trades = int(data.get("trades_rows") or 0)
        mint_n = int(data.get("mint_count") or 0)
        mint_sp = float(data.get("mint_spend") or 0)
        buy_vol = float(data.get("est_buy_volume") or 0)
        sell_vol = float(data.get("est_sell_volume") or 0)
        pnl_pct = data.get("pnl_percent")
        pct_str = f"{pnl_pct:.1f}%" if isinstance(pnl_pct, (int, float)) else "N/A"
        best_t = data.get("best_trade")
        worst_t = data.get("worst_trade")
        best_str = f"`{best_t:.4f} {sym}`" if isinstance(best_t, (int, float)) and best_t > 0 else "—"
        worst_str = f"`{worst_t:.4f} {sym}`" if isinstance(worst_t, (int, float)) and worst_t > 0 else "—"
        timeframe = data.get("moralis_period_note") or "—"
        if data.get("hit_row_cap") or data.get("hit_transfer_cap"):
            timeframe += "\n_Page cap — raise `PNL_MORALIS_MAX_PAGES` for more rows._"

        embed = Embed(
            title="📊 NFT Trading PnL Report",
            color=report_color,
            timestamp=datetime.utcnow(),
        )
        embed.add_field(
            name="👤 Wallet",
            value=f"`{data['wallet']}`",
            inline=False,
        )
        embed.add_field(
            name="🔢 Marketplace trades",
            value=f"`{total_trades:,}`",
            inline=True,
        )
        embed.add_field(
            name="🪙 Mints",
            value=f"`{mint_n:,}`\n_From `0x…0` → you_",
            inline=True,
        )
        embed.add_field(
            name="💰 Secondary buy",
            value=f"`{buy_vol:.4f} {sym}`",
            inline=True,
        )
        embed.add_field(name="💸 Total sell", value=f"`{sell_vol:.4f} {sym}`", inline=True)
        embed.add_field(
            name="⛏️ Est. mint spend",
            value=f"`{mint_sp:.4f} {sym}`\n_Sum of `value` on mint logs_",
            inline=True,
        )
        embed.add_field(
            name="📈 Net PnL",
            value=f"`{net:.4f} {sym}` **({pct_str})**\n_Sell − secondary buy − mint spend_",
            inline=True,
        )
        embed.add_field(
            name="🏆 Best Trade",
            value=f"{best_str}\n_Single largest sale_",
            inline=True,
        )
        embed.add_field(
            name="📉 Worst Trade",
            value=f"{worst_str}\n_Single largest buy_",
            inline=True,
        )
        embed.add_field(
            name="⏱️ Timeframe",
            value=timeframe[:1024] if timeframe else "—",
            inline=False,
        )

        return embed

    return Embed(
        title="❌ PNL Error",
        description="Unknown PNL response.",
        color=Color.red(),
    )
