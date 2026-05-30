"""
Community tools: /collection intel, /watch watchlist, cook-score reactions.
"""
from __future__ import annotations

import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

import config
import user_watchlist
from trackers.collection_intel import build_collection_intel_embed, fetch_collection_payload
from trackers.cook_score import increment_fire, reaction_is_fire, try_community_hot_boost
from trackers.nftscan_live_feed import get_nftscan_live_feed, normalize_eth_contract

logger = logging.getLogger(__name__)


def _feed_enrichers():
    feed = get_nftscan_live_feed()
    if feed:
        return feed.enricher, feed.social_fetcher
    return None, None


class CommunityMintCommands(commands.Cog):
    """Collection intel, personal watchlist, cook-score reactions."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        user_watchlist.init_db()

    watch_group = app_commands.Group(
        name="watch",
        description="Personal NFT contract watchlist (DM alerts)",
    )

    @watch_group.command(name="add", description="Watch a contract — get DM alerts on mint activity")
    @app_commands.describe(
        address="ERC-721/1155 contract address (0x…)",
        label="Optional nickname for your list",
    )
    async def watch_add(
        self,
        interaction: discord.Interaction,
        address: str,
        label: Optional[str] = None,
    ):
        max_n = int(getattr(config, "WATCHLIST_MAX_PER_USER", 15) or 15)
        ok, msg = user_watchlist.add_watch(
            interaction.user.id,
            address,
            label=label or "",
            max_per_user=max_n,
        )
        await interaction.response.send_message(
            ("✅ " if ok else "❌ ") + msg,
            ephemeral=True,
        )

    @watch_group.command(name="remove", description="Stop watching a contract")
    @app_commands.describe(address="Contract address to remove")
    async def watch_remove(self, interaction: discord.Interaction, address: str):
        ok, msg = user_watchlist.remove_watch(interaction.user.id, address)
        await interaction.response.send_message(
            ("✅ " if ok else "❌ ") + msg,
            ephemeral=True,
        )

    @watch_group.command(name="list", description="Show your watched contracts")
    async def watch_list(self, interaction: discord.Interaction):
        rows = user_watchlist.list_watches(interaction.user.id)
        if not rows:
            await interaction.response.send_message(
                "📭 Your watchlist is empty.\nUse `/watch add 0x…` to track a collection.",
                ephemeral=True,
            )
            return
        lines = []
        for i, row in enumerate(rows, 1):
            c = row["contract"]
            nick = f" — {row['label']}" if row.get("label") else ""
            lines.append(f"**{i}.** `{c}`{nick}")
        await interaction.response.send_message(
            "**Your watchlist**\n" + "\n".join(lines) + "\n\n`/collection <address>` for full intel.",
            ephemeral=True,
        )

    @app_commands.command(
        name="collection",
        description="One-shot intel card for an NFT collection contract",
    )
    @app_commands.describe(address="Contract address (0x…)")
    async def collection_intel(self, interaction: discord.Interaction, address: str):
        await interaction.response.defer(ephemeral=True)
        contract = normalize_eth_contract(address)
        if not contract:
            await interaction.followup.send(
                "❌ Invalid Ethereum address (42 chars, `0x` + 40 hex).",
                ephemeral=True,
            )
            return
        try:
            enricher, social = _feed_enrichers()
            if enricher is None:
                from trackers.mint_contract_enricher import MintContractEnricher

                enricher = MintContractEnricher()
            if social is None:
                from trackers.mint_social_fetcher import MintSocialFetcher

                social = MintSocialFetcher()
            mint = await fetch_collection_payload(
                contract,
                enricher=enricher,
                social_fetcher=social,
            )
            embed = build_collection_intel_embed(mint)
            await interaction.followup.send(embed=embed, ephemeral=True)
        except Exception as e:
            logger.exception("collection command failed")
            await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        if payload.user_id == self.bot.user.id:
            return
        if not getattr(config, "ENABLE_COOK_SCORE", True):
            return
        if not reaction_is_fire(payload):
            return
        row = increment_fire(payload.message_id)
        if not row:
            return
        await try_community_hot_boost(self.bot, row)


async def setup(bot: commands.Bot):
    if getattr(config, "ENABLE_COMMUNITY_MINT_TOOLS", True):
        await bot.add_cog(CommunityMintCommands(bot))
        logger.info("Community mint tools loaded (/collection, /watch, cook score)")
