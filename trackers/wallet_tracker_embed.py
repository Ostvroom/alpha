"""
Premium Discord embed builder for ETH NFT wallet tracker alerts.
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp
import discord

import config

_BRAND = (
    (
        os.getenv("VELOCR3_BRAND_NAME")
        or os.getenv("VELCOR3_BRAND_NAME")
        or os.getenv("NERDS_BRAND_NAME")
        or "Velocr3"
    ).strip()
    or "Velocr3"
)

# Luxury palette
_COLOR_MINT = 0x00FF88
_COLOR_GOLD = 0xFFD700
_COLOR_BUY = 0x5865F2
_COLOR_SELL = 0xED4245
_COLOR_NEUTRAL = 0x4E5058
_COLOR_ALPHA = 0xA855F7


@dataclass
class NftWalletAlertContext:
    action_word: str  # BOUGHT, SOLD, MINTED, CLAIMED, TRANSFERRED
    wallet: str
    wallet_label: str
    display_name: str
    contract: str
    collection_name: str
    token_id: int
    tx_hash: str
    display_eth: float
    price_eth: float
    price_source: str
    is_floor_estimate: bool
    floor_eth: float
    marketplace_source: str
    eth_usd: float
    from_zero_address: bool
    portfolio_usd: float
    is_verified: bool
    joined_iso: str
    avatar_url: str
    thumb_url: str
    bulk_qty: Optional[int] = None
    all_token_ids: Optional[List[int]] = None
    chain: str = "Ethereum"
    trader_name: str = ""
    x_url: str = ""
    opensea_profile_url: str = ""


def _cfg_flag(name: str, default: str = "1") -> bool:
    return getattr(config, name, _env_local(name, default))


def _env_local(name: str, default: str) -> bool:
    return os.getenv(name, default).strip().lower() not in ("0", "false", "no", "off")


def _high_value_usd() -> float:
    try:
        return float(os.getenv("WALLET_TRACKER_HIGH_VALUE_USD", "500") or "500")
    except ValueError:
        return 500.0


def _short_addr(addr: str) -> str:
    a = (addr or "").strip()
    if len(a) < 12:
        return a
    return f"{a[:6]}...{a[-4:]}"


def _format_eth(amt: float) -> str:
    if amt <= 0:
        return "0"
    if amt >= 1:
        s = f"{amt:,.4f}"
    elif amt >= 0.0001:
        s = f"{amt:.4f}"
    else:
        s = f"{amt:.6f}"
    return s.rstrip("0").rstrip(".")


def _is_address_label(name: str, wallet: str) -> bool:
    n = (name or "").strip()
    if not n:
        return True
    if n.startswith("0x") or "..." in n:
        return True
    return n.lower() == _short_addr(wallet).lower()


def resolve_trader_name(wallet_label: str, profile_name: str) -> str:
    """Prefer tracked label / OpenSea name — never the raw 0x address as the headline."""
    for candidate in (wallet_label, profile_name):
        c = (candidate or "").strip()
        if not c or c.lower() == "batch import":
            continue
        if c.startswith("0x") or "..." in c:
            continue
        return c
    return "Trader"


def _compact_wallet_row(ctx: NftWalletAlertContext, wallet_url: str) -> str:
    name = (ctx.trader_name or resolve_trader_name(ctx.wallet_label, ctx.display_name)).strip() or "Trader"
    return f"👤 **Trader** · [**{name}**]({wallet_url})"


def _value_line(ctx: NftWalletAlertContext) -> str:
    sym = "WETH" if ctx.price_source == "weth_logs" else "ETH"
    if ctx.display_eth <= 0 and ctx.marketplace_source == "Mint" and ctx.floor_eth == 0:
        return "**FREE mint**"
    if ctx.display_eth <= 0 and ctx.action_word == "TRANSFERRED":
        return "—"
    if ctx.display_eth <= 0 and ctx.marketplace_source == "Mint" and ctx.floor_eth > 0:
        usd = f" · ≈ ${ctx.floor_eth * ctx.eth_usd:,.0f}" if ctx.eth_usd > 0 else ""
        return f"**FREE mint** · floor ~{_format_eth(ctx.floor_eth)} ETH{usd}"
    if ctx.display_eth <= 0:
        return "—"
    prefix = "~" if ctx.is_floor_estimate else ""
    line = f"{prefix}**{_format_eth(ctx.display_eth)} {sym}**"
    if ctx.eth_usd > 0:
        line += f" · ≈ **${ctx.display_eth * ctx.eth_usd:,.0f}**"
    return line


def _token_ids_line(ctx: NftWalletAlertContext) -> str:
    if ctx.all_token_ids:
        shown = ctx.all_token_ids[:20]
        parts = [f"`#{t}`" for t in shown]
        line = " · ".join(parts)
        if len(ctx.all_token_ids) > 20:
            line += f" · +{len(ctx.all_token_ids) - 20} more"
        return line
    return f"`#{ctx.token_id}`"


def _usd_value(ctx: NftWalletAlertContext) -> float:
    if ctx.eth_usd <= 0:
        return 0.0
    return max(0.0, float(ctx.display_eth) * float(ctx.eth_usd))


def _is_high_value(ctx: NftWalletAlertContext) -> bool:
    return _usd_value(ctx) >= _high_value_usd()


def _title_and_color(ctx: NftWalletAlertContext) -> tuple[str, int]:
    col = (ctx.collection_name or "Collection").strip()
    high = _is_high_value(ctx)
    aw = ctx.action_word

    if aw in ("MINTED", "CLAIMED"):
        icon = "⚡" if not high else "💎"
        return f"{icon} NFT Mint Alert — {col}", _COLOR_MINT if not high else _COLOR_GOLD

    if aw == "BOUGHT":
        if high:
            return f"💎 NFT Purchase — {col}", _COLOR_GOLD
        return f"🟢 NFT Purchase — {col}", _COLOR_BUY

    if aw == "SOLD":
        return f"🔴 NFT Sale — {col}", _COLOR_SELL

    return f"↔️ NFT Transfer — {col}", _COLOR_NEUTRAL


def _wallet_classification(ctx: NftWalletAlertContext) -> Optional[str]:
    if not _cfg_flag("WALLET_TRACKER_SHOW_WALLET_CLASS", "1"):
        return None
    label_l = (ctx.wallet_label or "").lower()
    if any(k in label_l for k in ("whale", "smart", "alpha", "legend")):
        if "whale" in label_l:
            return "🐋 Whale"
        return f"🧠 {ctx.wallet_label}"

    if ctx.portfolio_usd >= 250_000:
        return "🐋 Whale"
    if ctx.portfolio_usd >= 25_000:
        return "🧠 Smart Money"

    joined = (ctx.joined_iso or "").strip()
    if joined:
        try:
            joined_dt = datetime.fromisoformat(joined.replace("Z", "+00:00"))
            age_days = (datetime.now(timezone.utc) - joined_dt).days
            if age_days >= 0 and age_days < 45:
                return "🆕 New Wallet"
        except Exception:
            pass

    if ctx.from_zero_address and ctx.action_word in ("MINTED", "CLAIMED"):
        return "🆕 First Mint"

    return None


def _alpha_signal(ctx: NftWalletAlertContext) -> bool:
    if not _cfg_flag("WALLET_TRACKER_SHOW_ALPHA_SIGNAL", "1"):
        return False
    free_mint = ctx.display_eth <= 0 and ctx.marketplace_source == "Mint"
    early_wallet = ctx.portfolio_usd < 5000 or _wallet_classification(ctx) == "🆕 New Wallet"
    return free_mint and early_wallet and ctx.action_word in ("MINTED", "CLAIMED", "BOUGHT")


def build_premium_nft_wallet_embed(ctx: NftWalletAlertContext) -> discord.Embed:
    """Single rich embed — luxury on-chain intelligence layout."""
    title, color = _title_and_color(ctx)
    if _alpha_signal(ctx) and ctx.action_word in ("MINTED", "CLAIMED", "BOUGHT"):
        color = _COLOR_ALPHA

    opensea = f"https://opensea.io/assets/ethereum/{ctx.contract}/{ctx.token_id}"
    tx_url = f"https://etherscan.io/tx/{ctx.tx_hash}"
    wallet_url = f"https://etherscan.io/address/{ctx.wallet}"
    contract_url = f"https://etherscan.io/address/{ctx.contract}"

    embed = discord.Embed(
        title=title[:256],
        url=opensea,
        color=color,
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_author(
        name=f"{_BRAND} · Wallet Tracker v1",
        icon_url=ctx.avatar_url,
        url=wallet_url,
    )

    qty = ctx.bulk_qty if ctx.bulk_qty else 1
    action_display = {
        "BOUGHT": "Bought",
        "SOLD": "Sold",
        "MINTED": "Minted",
        "CLAIMED": "Claimed",
        "TRANSFERRED": "Transferred",
    }.get(ctx.action_word, ctx.action_word.title())

    embed.add_field(
        name="\u200b",
        value="\n".join(
            [
                _compact_wallet_row(ctx, wallet_url),
                f"🎯 **Action** · **{action_display}**",
            ]
        )[:1024],
        inline=False,
    )
    embed.add_field(name="💰 Value", value=_value_line(ctx), inline=True)
    embed.add_field(
        name="📦 Amount",
        value=f"**{qty}** NFT{'s' if qty != 1 else ''}",
        inline=True,
    )
    embed.add_field(name="⏱ Time", value=f"<t:{int(time.time())}:R>", inline=True)
    embed.add_field(name="🆔 Token", value=_token_ids_line(ctx), inline=False)

    links = f"[OpenSea]({opensea}) · [TX]({tx_url}) · [Contract]({contract_url})"
    embed.add_field(name="🔗", value=links, inline=False)

    if ctx.thumb_url:
        embed.set_thumbnail(url=ctx.thumb_url)

    embed.set_footer(text=f"{_BRAND} · Wallet Tracker")
    return embed


async def build_premium_nft_wallet_embed_async(
    ctx: NftWalletAlertContext,
    session: aiohttp.ClientSession,
) -> discord.Embed:
    del session  # market intel / rarity block removed for compact alerts
    return build_premium_nft_wallet_embed(ctx)
