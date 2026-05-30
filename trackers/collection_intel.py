"""
One-shot collection intel card for /collection slash command.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import discord

import config
from brand_assets import apply_live_mint_branding, brand_logo_embed_icon, brand_name
from trackers.mint_embeds import _build_progress_bar, _safe_int, _safe_str, _square_thumbnail
from trackers.mint_wallet_intel import (
    buyers_in_window,
    format_collection_engagement_lines,
    format_smart_buys_line,
    get_contract_activity,
    minters_in_window,
)
from trackers.mint_x_alpha import compute_mint_x_alpha, twitter_handle_from_socials
from trackers.nftscan_live_feed import get_nftscan_live_feed, normalize_eth_contract


async def fetch_collection_payload(
    contract: str,
    *,
    enricher=None,
    social_fetcher=None,
) -> Dict[str, Any]:
    """Build enriched mint-shaped dict for one contract."""
    contract = normalize_eth_contract(contract) or contract.lower()
    mint: Dict[str, Any] = {
        "contract_address": contract,
        "token_id": "1",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if enricher:
        try:
            mint = await enricher.enrich(mint)
        except Exception:
            pass

    if social_fetcher and not mint.get("social_links"):
        try:
            socials = await social_fetcher.fetch(contract, mint.get("contract_name", ""))
            if socials:
                mint["social_links"] = socials
                if socials.get("collection_name") and not mint.get("contract_name"):
                    mint["contract_name"] = socials["collection_name"]
        except Exception:
            pass

    activity = get_contract_activity()
    win = int(getattr(config, "MINT_SMART_ENGAGEMENT_WINDOW_SEC", 1800) or 1800)
    buyers = buyers_in_window(activity, contract, win)
    minters = minters_in_window(activity, contract, win)
    if buyers:
        mint["smart_wallet_buys"] = buyers
    if minters:
        mint["smart_wallet_mints"] = minters
    if buyers or minters:
        mint["collection_smart_engagement"] = True

    try:
        compute_mint_x_alpha(mint)
    except Exception:
        pass

    feed = get_nftscan_live_feed()
    hot_count = 0
    if feed:
        hot_count = feed._hot_mint_volume_in_window(contract)
    mint["intel_hot_count"] = hot_count
    mint["intel_hot_threshold"] = int(getattr(config, "HOT_MINT_THRESHOLD", 10) or 10)
    mint["intel_muted"] = bool(feed and contract in feed._muted_contracts)
    mint["intel_banned"] = bool(feed and contract in feed._spam_contracts)

    cook = 0
    try:
        from trackers.cook_score import get_contract_fire_total

        cook = get_contract_fire_total(contract)
    except Exception:
        pass
    mint["intel_cook_score"] = cook

    return mint


def build_collection_intel_embed(mint: Dict[str, Any]) -> discord.Embed:
    contract = (mint.get("contract_address") or "").lower()
    name = _safe_str(mint.get("contract_name"), contract[:10] + "…", max_len=200)
    total = mint.get("total_supply")
    max_s = mint.get("max_supply")
    image_url = mint.get("image_url", "")
    if image_url == "https://www.nftscan.com/images/og-img/home.png":
        image_url = ""

    brand = brand_name()
    embed = discord.Embed(
        title=f"📊 {brand} · Collection Intel",
        description=f"**{name}**",
        color=0x5865F2,
        timestamp=datetime.now(timezone.utc),
    )
    icon = brand_logo_embed_icon()
    if icon:
        embed.set_author(name=f"{brand} Intel", icon_url=icon)

    if total is not None:
        progress = ""
        if max_s and int(max_s) > 0:
            try:
                progress = _build_progress_bar(int(total), int(max_s))
            except (TypeError, ValueError):
                progress = ""
        supply_line = f"**Minted:** {_safe_int(total)}"
        if max_s:
            supply_line += f" / {_safe_int(max_s)}"
        if progress:
            supply_line += f"\n{progress}"
        embed.add_field(name="Supply", value=supply_line, inline=True)
    else:
        embed.add_field(name="Supply", value="Unknown (RPC)", inline=True)

    hot_n = int(mint.get("intel_hot_count") or 0)
    thresh = int(mint.get("intel_hot_threshold") or 10)
    hot_status = f"**{hot_n}** mints in hot window (threshold {thresh})"
    if hot_n >= thresh:
        hot_status += " · 🔥 **HOT**"
    embed.add_field(name="Live activity", value=hot_status, inline=True)

    cook = int(mint.get("intel_cook_score") or 0)
    cook_line = f"**{cook}** community 🔥 on recent live alerts"
    if cook >= int(getattr(config, "COOK_SCORE_HOT_THRESHOLD", 5) or 5):
        cook_line += " · trending with members"
    embed.add_field(name="Cook score", value=cook_line, inline=True)

    x_score = int(mint.get("x_alpha_score") or 0)
    handle = mint.get("x_alpha_handle") or twitter_handle_from_socials(mint.get("social_links"))
    x_lines = [f"**Alpha score:** {x_score}/100"]
    if handle:
        x_lines.append(f"**X:** [@{handle}](https://x.com/{handle})")
    h24 = int(mint.get("x_alpha_hvas_24h") or 0)
    h7 = int(mint.get("x_alpha_hvas_7d") or 0)
    if h24 or h7:
        x_lines.append(f"**HVA follows:** {h24} (24h) · {h7} (7d)")
    sf = mint.get("x_alpha_smart_followers") or []
    if sf:
        x_lines.append("**Smart followers:** " + ", ".join(f"`@{h}`" for h in sf[:6]))
    embed.add_field(name="X / HVA", value="\n".join(x_lines)[:1024], inline=False)

    eng_lines = format_collection_engagement_lines(mint)
    eng = "\n".join(eng_lines) if eng_lines else ""
    buys = format_smart_buys_line(mint.get("smart_wallet_buys") or [])
    smart_parts = [p for p in (eng, buys) if p]
    if smart_parts:
        embed.add_field(
            name="Smart wallets",
            value="\n".join(smart_parts)[:1024],
            inline=False,
        )
    else:
        embed.add_field(
            name="Smart wallets",
            value="_No tracked wallet activity in the engagement window._",
            inline=False,
        )

    links: List[str] = []
    if mint.get("opensea_url"):
        links.append(f"[OpenSea]({mint['opensea_url']})")
    if mint.get("etherscan_url"):
        links.append(f"[Etherscan]({mint['etherscan_url']})")
    else:
        links.append(f"[Etherscan](https://etherscan.io/address/{contract})")
    socials = mint.get("social_links") or {}
    if socials.get("twitter"):
        links.append(f"[X]({socials['twitter']})")
    if socials.get("discord"):
        links.append(f"[Discord]({socials['discord']})")
    if socials.get("website"):
        links.append(f"[Website]({socials['website']})")
    embed.add_field(name="Links", value=" · ".join(links)[:1024], inline=False)

    flags = []
    if mint.get("intel_muted"):
        flags.append("🔕 muted (hot alerts)")
    if mint.get("intel_banned"):
        flags.append("🚫 spam-banned")
    if flags:
        embed.add_field(name="Feed status", value=" · ".join(flags), inline=False)

    embed.add_field(
        name="Watchlist",
        value=f"Use `/watch add {contract}` for DM alerts on this collection.",
        inline=False,
    )

    thumb = _square_thumbnail(image_url) if image_url else ""
    if thumb:
        embed.set_thumbnail(url=thumb)

    apply_live_mint_branding(embed)
    embed.set_footer(text=f"`{contract}` · /watch add · /collection")
    return embed
