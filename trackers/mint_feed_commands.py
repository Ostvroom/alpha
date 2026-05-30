"""
Slash commands ported from nftscan-discord-bot (live/hot mint feed admin + status).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

import config
from brand_assets import brand_name, collect_embed_attachment_files
from trackers.eth_ws_mint_listener import SPAM_CONTRACTS
from trackers.nftscan_live_feed import (
    MUTED_FILE,
    get_nftscan_live_feed,
    normalize_eth_contract,
)

logger = logging.getLogger(__name__)


def _require_feed():
    feed = get_nftscan_live_feed()
    if feed is None or not feed.listener:
        return None, "Live mint feed is not running. Enable `ENABLE_NFTSCAN_LIVE_MINTS=1` and restart the bot."
    return feed, None


class MintMuteView(discord.ui.View):
    def __init__(self, contracts: List[Tuple[str, str]], feed):
        super().__init__(timeout=180)
        self.feed = feed
        row, col = 0, 0
        for idx, (contract, name) in enumerate(contracts[:25]):
            label = f"🔕 {name[:12]}" if name else f"🔕 {contract[:6]}"
            btn = discord.ui.Button(
                label=label,
                style=discord.ButtonStyle.secondary,
                custom_id=f"mint_mute_{contract}_{idx}",
                row=row,
            )

            async def mute_callback(interaction: discord.Interaction, c=contract, n=name):
                self.feed.mute_contract(c)
                await interaction.response.send_message(
                    f"🔕 Muted `{n}` (`{c[:6]}...{c[-4:]}`) for hot mint alerts.",
                    ephemeral=True,
                )

            btn.callback = mute_callback
            self.add_item(btn)
            col += 1
            if col >= 5:
                col = 0
                row += 1
                if row >= 5:
                    break


class MintFeedCommands(commands.Cog):
    """nftscan-style live / hot mint slash commands."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="live_mints",
        description="Recent on-chain mints from the live feed (with mute buttons)",
    )
    async def live_mints(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        feed, err = _require_feed()
        if err:
            await interaction.followup.send(f"❌ {err}", ephemeral=True)
            return
        try:
            mints = await feed.listener.get_recent_mints(10)
            if mints and feed.enricher:
                mints = await feed.enricher.batch_enrich(mints)
            if not mints:
                await interaction.followup.send("❌ No recent mints.", ephemeral=True)
                return

            lines = []
            unique_contracts: List[Tuple[str, str]] = []
            seen_contracts = set()
            shown = 0
            for m in mints:
                if shown >= 10:
                    break
                name = m.get("contract_name") or "Unknown"
                contract = (m.get("contract_address") or "").lower()
                token_id = m.get("token_id", "?")
                cost = m.get("mint_cost", 0) or m.get("trade_price", 0) or 0
                cost_str = f"{float(cost):.4f}" if float(cost) > 0 else "Free"
                if contract in feed._muted_contracts:
                    continue
                if not name or name.startswith("0x"):
                    continue
                shown += 1
                lines.append(f"**{shown}.** {name} #{token_id} — `{cost_str} Ξ`")
                if contract and contract not in seen_contracts:
                    seen_contracts.add(contract)
                    unique_contracts.append((contract, name))

            brand = brand_name()
            embed = discord.Embed(
                title=f"⚡ {brand} · Recent Live Mints",
                description="\n".join(lines) if lines else "No displayable mints in queue.",
                color=0x2ECC71,
                timestamp=datetime.now(timezone.utc),
            )
            embed.set_footer(text="Click 🔕 to mute hot mint alerts for that contract")
            view = MintMuteView(unique_contracts, feed) if unique_contracts else None
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        except Exception as e:
            logger.exception("live_mints command failed")
            await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)

    @app_commands.command(
        name="mint_trackers",
        description="Contracts currently tracked for hot mint spikes (in-memory feed)",
    )
    async def mint_trackers(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        feed, err = _require_feed()
        if err:
            await interaction.followup.send(f"❌ {err}", ephemeral=True)
            return
        try:
            active = feed.active_hot_trackers()
            hidden = sum(
                1
                for c in feed._mint_tracker
                if c in feed._muted_contracts
            )
            if not active:
                msg = "No active mint trackers in the current window."
                if hidden:
                    msg += f" ({hidden} muted contract(s) hidden)"
                await interaction.followup.send(msg, ephemeral=True)
                return
            lines = [
                f"• `{contract[:6]}...{contract[-4:]}` — **{count}** mints"
                for contract, count in active[:20]
            ]
            msg = "\n".join(lines)
            if hidden:
                msg += f"\n\n_({hidden} muted contract(s) hidden)_"
            if len(msg) > 1900:
                msg = msg[:1900] + "\n... (truncated)"
            window_m = config.HOT_MINT_WINDOW // 60
            await interaction.followup.send(
                f"**Active mint trackers** (threshold: {config.HOT_MINT_THRESHOLD}, window: {window_m}m)\n{msg}",
                ephemeral=True,
            )
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)

    @app_commands.command(
        name="bancontract",
        description="Ban a contract from live mints for 48 hours (admin)",
    )
    @app_commands.describe(address="Contract address to ban")
    @app_commands.checks.has_permissions(administrator=True)
    async def bancontract(self, interaction: discord.Interaction, address: str):
        await interaction.response.defer(ephemeral=True)
        addr = normalize_eth_contract(address)
        if not addr:
            await interaction.followup.send(
                "❌ Invalid Ethereum address (42 chars, 0x…).", ephemeral=True
            )
            return
        feed, err = _require_feed()
        if err:
            await interaction.followup.send(f"❌ {err}", ephemeral=True)
            return
        feed.ban_contract(addr, hours=48)
        logger.info("Contract banned by %s: %s", interaction.user, addr)
        await interaction.followup.send(f"🚫 Banned `{addr}` for 48 hours.", ephemeral=True)

    @app_commands.command(name="unbancontract", description="Remove a contract ban")
    @app_commands.describe(address="Contract address to unban")
    async def unbancontract(self, interaction: discord.Interaction, address: str):
        await interaction.response.defer(ephemeral=True)
        addr = normalize_eth_contract(address)
        if not addr:
            await interaction.followup.send("❌ Invalid Ethereum address.", ephemeral=True)
            return
        feed, err = _require_feed()
        if feed:
            feed.unban_contract(addr)
        else:
            SPAM_CONTRACTS.discard(addr)
        await interaction.followup.send(f"✅ Unbanned `{addr}`.", ephemeral=True)

    @app_commands.command(name="spamlist", description="Show banned mint contracts")
    async def spamlist(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        feed = get_nftscan_live_feed()
        dynamic = sorted(feed._spam_contracts) if feed else []
        hardcoded = sorted(SPAM_CONTRACTS - set(dynamic))
        lines = []
        if hardcoded:
            lines.append("**Hardcoded bans:**")
            for addr in hardcoded[:20]:
                lines.append(f"• `{addr}`")
        if dynamic:
            lines.append("\n**Dynamic bans (48h):**")
            for addr in dynamic[:20]:
                exp = feed._spam_contract_expiry.get(addr) if feed else None
                exp_str = f" (expires <t:{int(exp.timestamp())}:R>)" if exp else ""
                lines.append(f"• `{addr}`{exp_str}")
        if not lines:
            await interaction.followup.send("No banned contracts.", ephemeral=True)
        else:
            msg = "\n".join(lines)
            if len(msg) > 1900:
                msg = msg[:1900] + "\n... (truncated)"
            await interaction.followup.send(msg, ephemeral=True)

    @app_commands.command(
        name="mutecontract",
        description="Mute hot mint alerts for a contract",
    )
    @app_commands.describe(address="Contract address to mute")
    async def mutecontract(self, interaction: discord.Interaction, address: str):
        await interaction.response.defer(ephemeral=True)
        addr = normalize_eth_contract(address)
        if not addr:
            await interaction.followup.send("❌ Invalid Ethereum address.", ephemeral=True)
            return
        feed, err = _require_feed()
        if err:
            await interaction.followup.send(f"❌ {err}", ephemeral=True)
            return
        feed.mute_contract(addr)
        logger.info("Contract muted by %s: %s", interaction.user, addr)
        await interaction.followup.send(
            f"🔕 Muted `{addr}`. Removed from hot mint tracker.", ephemeral=True
        )

    @app_commands.command(
        name="unmutecontract",
        description="Unmute hot mint alerts for a contract",
    )
    @app_commands.describe(address="Contract address to unmute")
    async def unmutecontract(self, interaction: discord.Interaction, address: str):
        await interaction.response.defer(ephemeral=True)
        addr = normalize_eth_contract(address)
        if not addr:
            await interaction.followup.send("❌ Invalid Ethereum address.", ephemeral=True)
            return
        feed, err = _require_feed()
        if err:
            await interaction.followup.send(f"❌ {err}", ephemeral=True)
            return
        feed.unmute_contract(addr)
        await interaction.followup.send(f"✅ Unmuted hot mint alerts for `{addr}`.", ephemeral=True)

    @app_commands.command(name="mutedlist", description="Show muted mint contracts")
    async def mutedlist(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        feed = get_nftscan_live_feed()
        muted = sorted(feed._muted_contracts) if feed else []
        if not muted and os.path.exists(MUTED_FILE):
            try:
                with open(MUTED_FILE, "r", encoding="utf-8") as f:
                    muted = sorted(json.load(f))
            except Exception:
                pass
        if not muted:
            await interaction.followup.send("No muted contracts.", ephemeral=True)
            return
        lines = [f"• `{addr}`" for addr in muted[:25]]
        await interaction.followup.send("**Muted contracts:**\n" + "\n".join(lines), ephemeral=True)

    @app_commands.command(name="checkmute", description="Check if a contract is muted")
    @app_commands.describe(address="Contract address to check")
    async def checkmute(self, interaction: discord.Interaction, address: str):
        await interaction.response.defer(ephemeral=True)
        addr = normalize_eth_contract(address)
        if not addr:
            await interaction.followup.send("❌ Invalid Ethereum address.", ephemeral=True)
            return
        feed = get_nftscan_live_feed()
        in_memory = addr in feed._muted_contracts if feed else False
        file_muted = False
        if os.path.exists(MUTED_FILE):
            try:
                with open(MUTED_FILE, "r", encoding="utf-8") as f:
                    file_muted = addr in set(json.load(f))
            except Exception:
                pass
        status = "🔕 MUTED" if (in_memory or file_muted) else "✅ NOT muted"
        await interaction.followup.send(
            f"{status}\n`{addr}`\n\nMemory: {in_memory} | File: {file_muted}",
            ephemeral=True,
        )

    @app_commands.command(
        name="test_wallet_embed",
        description="Post a test NFT wallet tracker embed to the ETH NFT channel",
    )
    async def test_wallet_embed(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        ch_id = config.DISCORD_NFT_CHANNEL_ID
        if not ch_id:
            await interaction.followup.send(
                "❌ `DISCORD_NFT_CHANNEL_ID` is not set.", ephemeral=True
            )
            return
        channel = self.bot.get_channel(ch_id) or await self.bot.fetch_channel(ch_id)
        if not channel:
            await interaction.followup.send(f"❌ Channel {ch_id} not found.", ephemeral=True)
            return
        try:
            from trackers import eth_tracker

            eth_tracker.connect_web3()
            wallet = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
            contract = "0xBC4CA0Eda7647A8Ab7c2061c2E118A18a936f13D"
            tx_hash = "0xeb89af798293422caef87503463226472f226cb87266483229692be82778f65"
            token_id = 1
            eth_tracker.tracked_eth_wallets[wallet.lower()] = "Test wallet"

            async with aiohttp.ClientSession() as session:
                embed, content, view, files = await eth_tracker.create_eth_nft_embed(
                    action_type="Received",
                    wallet=wallet,
                    contract=contract,
                    session=session,
                    tx_hash=tx_hash,
                    token_id=token_id,
                    from_addr="0x0000000000000000000000000000000000000000",
                    to_addr=wallet,
                )
            kwargs = {"embed": embed}
            if content:
                kwargs["content"] = content
            if view is not None:
                kwargs["view"] = view
            if files:
                kwargs["files"] = eth_tracker._fresh_discord_files(files)
                brand_files = collect_embed_attachment_files([embed])
                existing = {f.filename for f in kwargs["files"]}
                for bf in brand_files:
                    if bf.filename not in existing:
                        kwargs["files"].append(bf)
            await channel.send(**kwargs)
            await interaction.followup.send(
                f"✅ Sent test wallet embed to <#{ch_id}>.", ephemeral=True
            )
        except Exception as e:
            logger.exception("test_wallet_embed failed")
            await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)

    @bancontract.error
    async def bancontract_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.MissingPermissions):
            msg = "❌ You need **Administrator** to ban contracts."
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)


async def setup(bot: commands.Bot):
    if getattr(config, "ENABLE_NFTSCAN_LIVE_MINTS", True):
        await bot.add_cog(MintFeedCommands(bot))
    if getattr(config, "ENABLE_COMMUNITY_MINT_TOOLS", True):
        from trackers.community_mint import setup as setup_community_mint

        await setup_community_mint(bot)
