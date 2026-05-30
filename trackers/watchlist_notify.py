"""
DM users who watchlisted a contract when mint-feed activity fires.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List

import discord

import config
import user_watchlist
from brand_assets import brand_name

logger = logging.getLogger(__name__)


def _short_contract(c: str) -> str:
    c = (c or "").lower()
    if len(c) >= 10:
        return f"`{c[:6]}...{c[-4:]}`"
    return f"`{c}`"


def _watchlist_embed(mint: Dict[str, Any], alert_type: str) -> discord.Embed:
    contract = (mint.get("contract_address") or "").lower()
    name = (mint.get("contract_name") or contract[:10]).strip()
    brand = brand_name()
    titles = {
        "live": "⚡ Live mint",
        "hot": "🔥 Hot mint spike",
        "smart_buy": "🛒 Smart wallet buy",
        "community_hot": "🔥 Community pick",
    }
    embed = discord.Embed(
        title=f"{titles.get(alert_type, '📢 Alert')} · {brand}",
        description=f"**{name}**\n{_short_contract(contract)}",
        color=0xFEE75C if alert_type == "hot" else 0x57F287,
    )
    lines: List[str] = []
    if alert_type == "hot":
        cnt = mint.get("hot_mint_count") or mint.get("intel_hot_count")
        if cnt:
            lines.append(f"**{cnt}** mints in the hot window")
    if mint.get("tracked_trader"):
        lines.append(f"**Trader:** {mint.get('tracked_trader')}")
    if mint.get("x_alpha_score"):
        lines.append(f"**X alpha:** {mint.get('x_alpha_score')}/100")
    if lines:
        embed.add_field(name="Details", value="\n".join(lines), inline=False)
    links = []
    if mint.get("opensea_url"):
        links.append(f"[OpenSea]({mint['opensea_url']})")
    links.append(f"[Etherscan](https://etherscan.io/address/{contract})")
    links.append(f"Use `/collection {contract}` in the server for full intel")
    embed.add_field(
        name="Links",
        value=" · ".join(links[:3]),
        inline=False,
    )
    embed.set_footer(text="Watchlist alert · /watch list to manage")
    return embed


async def notify_watchlist_users(
    bot: discord.Client,
    contract: str,
    mint: Dict[str, Any],
    alert_type: str,
) -> None:
    if not getattr(config, "ENABLE_USER_WATCHLIST", True):
        return
    watchers = user_watchlist.watchers_for_contract(contract)
    if not watchers:
        return
    cooldown = int(getattr(config, "WATCHLIST_DM_COOLDOWN_SEC", 600) or 600)
    embed = _watchlist_embed(mint, alert_type)
    delay = float(getattr(config, "WATCHLIST_DM_DELAY_SEC", 1.0) or 1.0)

    for uid in watchers:
        if not user_watchlist._dm_allowed(uid, contract, cooldown):
            continue
        try:
            user = bot.get_user(uid) or await bot.fetch_user(uid)
            await user.send(embed=embed)
            await asyncio.sleep(delay)
        except discord.Forbidden:
            pass
        except Exception as e:
            logger.debug("watchlist DM to %s failed: %s", uid, e)
