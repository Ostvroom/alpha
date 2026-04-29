"""
Slash commands: owner issues per-guild license keys; admins activate and one-click channel install.
"""
from __future__ import annotations

import discord
from discord import app_commands
from discord.ext import commands

import config
import guild_license


async def _is_bot_owner(interaction: discord.Interaction) -> bool:
    uid = interaction.user.id
    owners = getattr(config, "BOT_OWNER_IDS", []) or []
    if owners and uid in owners:
        return True
    try:
        app = await interaction.client.application_info()
        if app.owner and app.owner.id == uid:
            return True
    except Exception:
        pass
    return False


class GuildLicenseCommands(commands.Cog):
    """Per-guild licenses and alert channel setup."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    owner_license = app_commands.Group(
        name="owner_license",
        description="Bot owner: issue and manage server license keys",
    )

    alerts = app_commands.Group(
        name="alerts",
        description="Connect this server to Velcor3 discovery alerts",
    )

    wallet_tracker = app_commands.Group(
        name="wallet_tracker",
        description="One-click install for wallet/NFT tracker feeds (per server)",
    )

    async def _wallet_install_impl(self, interaction: discord.Interaction) -> None:
        """Shared implementation for 1-click installs (wallet tracker + daily finds)."""
        if not interaction.guild:
            await interaction.response.send_message("Use this in a server.", ephemeral=True)
            return
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Administrator permission required.", ephemeral=True)
            return
        if not interaction.user.guild_permissions.manage_channels:
            await interaction.response.send_message(
                "I need **Manage Channels** (and the bot needs **Manage Channels**) for 1-click install.",
                ephemeral=True,
            )
            return
        sub = guild_license.get_subscription(interaction.guild.id)
        if not sub:
            await interaction.response.send_message(
                "No license for this server. Ask the owner for a key, then run `/alerts activate` first.",
                ephemeral=True,
            )
            return
        if not sub[0]:
            await interaction.response.send_message(
                "Activate your license first: `/alerts activate`", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        me = guild.me
        if not me or not me.guild_permissions.manage_channels:
            await interaction.followup.send(
                "The bot lacks **Manage Channels** in this server. Re-invite with that permission.",
                ephemeral=True,
            )
            return

        # Reuse existing "Velcor3" category if present; otherwise create it.
        category = discord.utils.get(guild.categories, name="Velcor3")
        try:
            if category is None:
                category = await guild.create_category("Velcor3", reason="Velcor3 feeds")
            ch_wallet = discord.utils.get(guild.text_channels, name="velcor3-wallets")
            if ch_wallet is None:
                ch_wallet = await guild.create_text_channel(
                    "velcor3-wallets",
                    category=category,
                    topic="ETH wallet/NFT tracker alerts",
                )
            ch_finds = discord.utils.get(guild.text_channels, name="velcor3-daily-finds")
            if ch_finds is None:
                ch_finds = await guild.create_text_channel(
                    "velcor3-daily-finds",
                    category=category,
                    topic="Daily 24h digest of projects we alerted (rolling finds recap)",
                )
        except discord.Forbidden:
            await interaction.followup.send("Missing permission to create channels.", ephemeral=True)
            return
        except Exception as e:
            await interaction.followup.send(f"Install failed: {e}", ephemeral=True)
            return

        guild_license.set_wallet_nft_channel(guild.id, ch_wallet.id)
        guild_license.set_daily_finds_channel(guild.id, ch_finds.id)

        if hasattr(self.bot, "rebuild_channel_caches"):
            await self.bot.rebuild_channel_caches()

        await interaction.followup.send(
            f"✅ Wallet tracker installed.\n"
            f"- Wallet/NFT alerts → {ch_wallet.mention}\n"
            f"- Daily finds digest → {ch_finds.mention}",
            ephemeral=True,
        )

    @owner_license.command(name="issue")
    @app_commands.describe(guild_id="Discord server ID the key will be locked to (Developer Mode → Copy Server ID)")
    async def owner_issue(self, interaction: discord.Interaction, guild_id: str) -> None:
        if not await _is_bot_owner(interaction):
            await interaction.response.send_message(
                "Only the bot owner can issue licenses.", ephemeral=True
            )
            return
        try:
            gid = int(str(guild_id).strip())
        except ValueError:
            await interaction.response.send_message("Invalid `guild_id`.", ephemeral=True)
            return
        g = self.bot.get_guild(gid)
        if not g:
            await interaction.response.send_message(
                f"Bot is not in a server with ID `{gid}`. Invite the bot first, then issue the key.",
                ephemeral=True,
            )
            return
        key, msg = guild_license.issue_license(gid)
        if not key:
            await interaction.response.send_message(msg, ephemeral=True)
            return
        await interaction.response.send_message(
            f"**Server:** {g.name} (`{gid}`)\n\n"
            f"**License key** (single use, guard like a password):\n```\n{key}\n```\n"
            f"{msg}\n"
            f"Admin runs `/alerts activate` with this key, then `/alerts setup`.",
            ephemeral=True,
        )

    @owner_license.command(name="list")
    async def owner_list(self, interaction: discord.Interaction) -> None:
        if not await _is_bot_owner(interaction):
            await interaction.response.send_message(
                "Only the bot owner can list licenses.", ephemeral=True
            )
            return
        guild_license.init_db()
        rows = guild_license.list_all_rows()
        if not rows:
            await interaction.response.send_message("No license rows yet.", ephemeral=True)
            return
        lines = []
        for gid, issued, act, cn, ce, cx, ct in rows[:25]:
            name = ""
            g = self.bot.get_guild(int(gid))
            if g:
                name = g.name[:40]
            act_s = "activated" if act else "pending"
            lines.append(
                f"`{gid}` {name or '—'} · {act_s} · ch new/est/esc/tr: "
                f"{cn or 0}/{ce or 0}/{cx or 0}/{ct or 0}"
            )
        extra = f"\n… and {len(rows) - 25} more." if len(rows) > 25 else ""
        await interaction.response.send_message(
            "**Subscriptions**\n" + "\n".join(lines) + extra, ephemeral=True
        )

    @owner_license.command(name="revoke")
    @app_commands.describe(guild_id="Remove license row for this server")
    async def owner_revoke(self, interaction: discord.Interaction, guild_id: str) -> None:
        if not await _is_bot_owner(interaction):
            await interaction.response.send_message("Only the bot owner.", ephemeral=True)
            return
        try:
            gid = int(str(guild_id).strip())
        except ValueError:
            await interaction.response.send_message("Invalid guild id.", ephemeral=True)
            return
        if guild_license.revoke_license(gid):
            if hasattr(self.bot, "rebuild_channel_caches"):
                await self.bot.rebuild_channel_caches()
            await interaction.response.send_message(f"Revoked license for `{gid}`.", ephemeral=True)
        else:
            await interaction.response.send_message("No row for that guild.", ephemeral=True)

    @alerts.command(name="activate")
    @app_commands.describe(license_key="Key sent by the bot owner")
    async def alerts_activate(self, interaction: discord.Interaction, license_key: str) -> None:
        if not interaction.guild:
            await interaction.response.send_message("Use this in a server.", ephemeral=True)
            return
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message(
                "Administrator permission required.", ephemeral=True
            )
            return
        ok, msg = guild_license.activate_license(interaction.guild.id, license_key)
        if ok and hasattr(self.bot, "rebuild_channel_caches"):
            await self.bot.rebuild_channel_caches()
        await interaction.response.send_message(msg, ephemeral=True)

    @alerts.command(name="setup")
    async def alerts_setup(self, interaction: discord.Interaction) -> None:
        if not interaction.guild:
            await interaction.response.send_message("Use this in a server.", ephemeral=True)
            return
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message(
                "Administrator permission required.", ephemeral=True
            )
            return
        if not interaction.user.guild_permissions.manage_channels:
            await interaction.response.send_message(
                "I need **Manage Channels** (and the bot needs **Manage Channels**) to create feeds.",
                ephemeral=True,
            )
            return
        sub = guild_license.get_subscription(interaction.guild.id)
        if not sub:
            await interaction.response.send_message(
                "No license for this server. Ask the owner for a key issued for this guild ID.",
                ephemeral=True,
            )
            return
        if not sub[0]:
            await interaction.response.send_message(
                "Activate your license first: `/alerts activate`", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        me = guild.me
        if not me or not me.guild_permissions.manage_channels:
            await interaction.followup.send(
                "The bot lacks **Manage Channels** in this server. Re-invite with that permission.",
                ephemeral=True,
            )
            return

        try:
            category = await guild.create_category(
                "Velcor3",
                reason="Velcor3 alert feeds",
            )
            ch_new = await guild.create_text_channel(
                "velcor3-new",
                category=category,
                topic="Projects ≤30d — discovery alerts",
            )
            ch_est = await guild.create_text_channel(
                "velcor3-established",
                category=category,
                topic="Projects 31–130d — discovery alerts",
            )
            ch_esc = await guild.create_text_channel(
                "velcor3-escalation",
                category=category,
                topic="Momentum & escalation signals",
            )
            ch_tr = await guild.create_text_channel(
                "velcor3-trending",
                category=category,
                topic="Trending reports (periodic)",
            )
        except discord.Forbidden:
            await interaction.followup.send(
                "Missing permission to create channels.", ephemeral=True
            )
            return
        except Exception as e:
            await interaction.followup.send(f"Setup failed: {e}", ephemeral=True)
            return

        guild_license.set_install_channels(
            guild.id,
            ch_new.id,
            ch_est.id,
            ch_esc.id,
            ch_tr.id,
        )
        if hasattr(self.bot, "rebuild_channel_caches"):
            await self.bot.rebuild_channel_caches()

        await interaction.followup.send(
            f"Channels created under {category.mention}.\n"
            f"{ch_new.mention} · {ch_est.mention} · {ch_esc.mention} · {ch_tr.mention}\n"
            f"Alerts will begin on the next bot cycle (usually within minutes).",
            ephemeral=True,
        )

    @alerts.command(name="status")
    async def alerts_status(self, interaction: discord.Interaction) -> None:
        if not interaction.guild:
            await interaction.response.send_message("Use in a server.", ephemeral=True)
            return
        sub = guild_license.get_subscription(interaction.guild.id)
        if not sub:
            await interaction.response.send_message(
                "No license record. Owner must `/owner_license issue` with this server's ID.",
                ephemeral=True,
            )
            return
        act, _issued, cn, ce, cx, ct, cw, cdf = sub
        state = "activated" if act else "pending activation"
        await interaction.response.send_message(
            f"**License:** {state}\n"
            f"**Guild ID:** `{interaction.guild.id}`\n"
            f"**Channels:** new `{cn}` · established `{ce}` · escalation `{cx}` · trending `{ct}`\n"
            f"**Wallet tracker:** `{cw}`\n"
            f"**Daily finds:** `{cdf}`\n"
            f"(0 = run `/alerts setup` after activation.)",
            ephemeral=True,
        )

    # Alias under /alerts (more discoverable + already in use)
    @alerts.command(name="wallet_install")
    async def alerts_wallet_install(self, interaction: discord.Interaction) -> None:
        await self._wallet_install_impl(interaction)

    @wallet_tracker.command(name="install")
    async def wallet_install(self, interaction: discord.Interaction) -> None:
        await self._wallet_install_impl(interaction)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(GuildLicenseCommands(bot))
