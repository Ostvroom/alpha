"""Slash command /claim_premium and monthly subscription expiry task."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import discord
from discord import Embed, Interaction, app_commands
from discord.ext import commands, tasks

import config
import payment_database
import payment_verify


def _min_amount_for(tier: str, chain: str) -> float:
    if tier == "lifetime":
        if chain == "eth_mainnet":
            return config.PAYMENT_LIFETIME_MIN_ETH_MAINNET
        if chain == "eth_base":
            return config.PAYMENT_LIFETIME_MIN_ETH_BASE
        return config.PAYMENT_LIFETIME_MIN_SOL
    if chain == "eth_mainnet":
        return config.PAYMENT_MONTHLY_MIN_ETH_MAINNET
    if chain == "eth_base":
        return config.PAYMENT_MONTHLY_MIN_ETH_BASE
    return config.PAYMENT_MONTHLY_MIN_SOL


def _treasury_for(chain: str) -> Optional[str]:
    if chain == "eth_mainnet":
        return config.PAYMENT_TREASURY_ETH_MAINNET
    if chain == "eth_base":
        return config.PAYMENT_TREASURY_ETH_BASE
    return config.PAYMENT_TREASURY_SOL


def _chain_disabled(tier: str, chain: str) -> bool:
    return _min_amount_for(tier, chain) <= 0


class PremiumPaymentCommands(commands.Cog):
    """Automatic premium activation from on-chain payments."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def cog_load(self) -> None:
        self.expire_monthly_subscriptions.start()

    async def cog_unload(self) -> None:
        self.expire_monthly_subscriptions.cancel()

    @app_commands.command(name="claim_premium", description="Verify a crypto payment and assign your premium role")
    @app_commands.describe(
        tier="Product you paid for",
        chain="Network you used",
        tx_hash="Transaction signature (EVM: 0x… hash; Solana: base58 signature)",
    )
    @app_commands.choices(
        tier=[
            app_commands.Choice(name="Lifetime", value="lifetime"),
            app_commands.Choice(name="Monthly premium", value="monthly"),
        ],
        chain=[
            app_commands.Choice(name="Ethereum mainnet (ETH)", value="eth_mainnet"),
            app_commands.Choice(name="Base (ETH)", value="eth_base"),
            app_commands.Choice(name="Solana (SOL)", value="solana"),
        ],
    )
    async def claim_premium(
        self,
        interaction: Interaction,
        tier: app_commands.Choice[str],
        chain: app_commands.Choice[str],
        tx_hash: str,
    ) -> None:
        await self._do_claim(interaction, tier.value, chain.value, tx_hash)

    async def _do_claim(
        self,
        interaction: Interaction,
        tier_value: str,
        chain_value: str,
        tx_raw: str,
    ) -> None:
        if not interaction.guild:
            await interaction.response.send_message(
                "Use this command inside the server.",
                ephemeral=True,
            )
            return

        if _chain_disabled(tier_value, chain_value):
            await interaction.response.send_message(
                f"This tier is not enabled for the selected chain (minimum amount is 0). "
                f"Check `.env` (`PAYMENT_*_MIN_*`) or ask an admin.",
                ephemeral=True,
            )
            return

        treasury = _treasury_for(chain_value)
        if not treasury:
            await interaction.response.send_message(
                "Payment wallet for this network is not configured. Ask an admin.",
                ephemeral=True,
            )
            return

        if chain_value in ("eth_mainnet", "eth_base"):
            h = payment_verify.normalize_evm_tx_hash(tx_raw)
            if not h:
                await interaction.response.send_message(
                    "Invalid EVM transaction hash (expected `0x` + 64 hex characters).",
                    ephemeral=True,
                )
                return
            tx_store = h
        else:
            h = payment_verify.normalize_sol_signature(tx_raw)
            if not h:
                await interaction.response.send_message(
                    "Invalid Solana transaction signature (base58).",
                    ephemeral=True,
                )
                return
            tx_store = h

        if payment_database.claim_exists(tx_store, chain_value):
            await interaction.response.send_message(
                "This transaction was already used for a claim.",
                ephemeral=True,
            )
            return

        if payment_database.claims_today_utc(interaction.user.id) >= config.PAYMENT_MAX_CLAIMS_PER_DAY:
            await interaction.response.send_message(
                f"You have reached the daily limit of {config.PAYMENT_MAX_CLAIMS_PER_DAY} claims. Try again tomorrow.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        lifetime_rid = config.PREMIUM_LIFETIME_ROLE_ID
        monthly_rid = config.PREMIUM_MONTHLY_ROLE_ID
        if not lifetime_rid or not monthly_rid:
            await interaction.followup.send(
                "Premium roles are not configured (`PREMIUM_*_ROLE_ID`). Ask an admin.",
                ephemeral=True,
            )
            return

        member = interaction.user
        if not isinstance(member, discord.Member):
            await interaction.followup.send("Could not resolve your member profile.", ephemeral=True)
            return

        ok = False
        msg = ""
        amount_raw_int = 0

        async with aiohttp.ClientSession() as session:
            if chain_value == "eth_mainnet":
                min_wei = payment_verify.eth_to_wei(_min_amount_for(tier_value, chain_value))
                ok, msg, amount_raw_int = await payment_verify.verify_evm_native_payment(
                    session,
                    1,
                    tx_store,
                    treasury,
                    min_wei,
                    config.PAYMENT_MIN_CONFIRMATIONS_ETH,
                )
            elif chain_value == "eth_base":
                min_wei = payment_verify.eth_to_wei(_min_amount_for(tier_value, chain_value))
                ok, msg, amount_raw_int = await payment_verify.verify_evm_native_payment(
                    session,
                    8453,
                    tx_store,
                    treasury,
                    min_wei,
                    config.PAYMENT_MIN_CONFIRMATIONS_BASE,
                )
            else:
                min_lamports = payment_verify.sol_to_lamports(_min_amount_for(tier_value, chain_value))
                ok, msg, amount_raw_int = await payment_verify.verify_solana_native_payment(
                    session,
                    tx_store,
                    treasury,
                    min_lamports,
                )

        if not ok:
            await interaction.followup.send(f"Could not verify payment: {msg}", ephemeral=True)
            await self._log(
                interaction,
                tier_value,
                chain_value,
                tx_store,
                False,
                msg,
                amount_raw_int,
            )
            return

        # Referral credit (20% by default) — only after payment is verified
        referrer_id: Optional[int] = None
        credited_raw: int = 0
        if getattr(config, "ENABLE_REFERRALS", True):
            referrer_id = payment_database.get_referrer(interaction.user.id, interaction.guild.id)
            if referrer_id and int(referrer_id) != int(interaction.user.id):
                try:
                    pct = float(getattr(config, "REFERRAL_PAYOUT_PCT", 0.20))
                    if pct > 0:
                        credited_raw = int(int(amount_raw_int) * pct)
                except Exception:
                    credited_raw = 0

        try:
            payment_database.insert_claim(
                tx_store,
                chain_value,
                interaction.user.id,
                interaction.guild.id,
                tier_value,
                str(amount_raw_int),
            )
        except sqlite3.IntegrityError:
            await interaction.followup.send(
                "This transaction was already claimed (race). If this is wrong, contact staff.",
                ephemeral=True,
            )
            return
        except Exception as e:
            await interaction.followup.send(
                f"Payment verified but saving failed: {e}. Contact staff with your tx hash.",
                ephemeral=True,
            )
            return

        lifetime_role = interaction.guild.get_role(lifetime_rid)
        monthly_role = interaction.guild.get_role(monthly_rid)
        if tier_value == "lifetime":
            if not lifetime_role:
                await interaction.followup.send(
                    "Lifetime role ID is invalid in this server. Staff have been notified via logs if configured.",
                    ephemeral=True,
                )
                return
            try:
                if config.LIFETIME_REMOVES_MONTHLY and monthly_role and monthly_role in member.roles:
                    await member.remove_roles(monthly_role, reason="Lifetime premium — monthly replaced")
                payment_database.delete_subscription(member.id, interaction.guild.id)
                await member.add_roles(lifetime_role, reason="Premium payment verified (lifetime)")
            except discord.Forbidden:
                await interaction.followup.send(
                    "I cannot assign the lifetime role (missing permissions). Ask an admin.",
                    ephemeral=True,
                )
                return
            await interaction.followup.send(
                f"Payment verified. You now have {lifetime_role.mention}. Thank you!",
                ephemeral=True,
            )
        else:
            if not monthly_role:
                await interaction.followup.send(
                    "Monthly role ID is invalid in this server.",
                    ephemeral=True,
                )
                return
            try:
                exp = payment_database.upsert_monthly_subscription(
                    member.id,
                    interaction.guild.id,
                    tx_store,
                    chain_value,
                    config.PREMIUM_MONTHLY_DAYS,
                )
                await member.add_roles(monthly_role, reason="Premium payment verified (monthly)")
            except discord.Forbidden:
                await interaction.followup.send(
                    "I cannot assign the monthly role (missing permissions). Ask an admin.",
                    ephemeral=True,
                )
                return
            exp_s = exp.strftime("%Y-%m-%d %H:%M UTC")
            await interaction.followup.send(
                f"Payment verified. You have {monthly_role.mention} until **{exp_s}** (renew before expiry).",
                ephemeral=True,
            )

        # Apply referral credit only after role assignment succeeded
        if referrer_id and credited_raw > 0:
            inserted = payment_database.insert_referral_credit(
                referrer_user_id=int(referrer_id),
                referred_user_id=int(interaction.user.id),
                tx_hash=tx_store,
                chain=chain_value,
                tier=tier_value,
                amount_raw=int(amount_raw_int),
                credited_raw=int(credited_raw),
            )
            if inserted:
                await self._log_referral_credit(
                    interaction,
                    referrer_id=int(referrer_id),
                    credited_raw=int(credited_raw),
                    chain_value=chain_value,
                    tier_value=tier_value,
                    tx_store=tx_store,
                )

        await self._log(
            interaction,
            tier_value,
            chain_value,
            tx_store,
            True,
            msg,
            amount_raw_int,
        )

    async def _log(
        self,
        interaction: Interaction,
        tier_value: str,
        chain_value: str,
        tx_store: str,
        success: bool,
        detail: str,
        amount_raw: int,
    ) -> None:
        cid = config.PAYMENT_LOG_CHANNEL_ID
        if not cid:
            return
        ch = self.bot.get_channel(cid)
        if ch is None:
            try:
                ch = await self.bot.fetch_channel(cid)
            except Exception:
                return
        if not isinstance(ch, discord.TextChannel):
            return
        title = "Premium claim — success" if success else "Premium claim — failed"
        color = discord.Color.green() if success else discord.Color.red()
        e = Embed(title=title, color=color)
        e.add_field(name="User", value=f"{interaction.user} ({interaction.user.id})", inline=False)
        e.add_field(name="Tier", value=tier_value, inline=True)
        e.add_field(name="Chain", value=chain_value, inline=True)
        e.add_field(name="Tx", value=f"`{tx_store[:20]}…`", inline=True)
        e.add_field(name="Amount (raw units)", value=str(amount_raw), inline=True)
        if not success:
            e.add_field(name="Reason", value=detail[:1000], inline=False)
        try:
            await ch.send(embed=e)
        except Exception:
            pass

    async def _log_referral_credit(
        self,
        interaction: Interaction,
        *,
        referrer_id: int,
        credited_raw: int,
        chain_value: str,
        tier_value: str,
        tx_store: str,
    ) -> None:
        cid = getattr(config, "REFERRAL_LOG_CHANNEL_ID", 0) or 0
        if not cid:
            return
        ch = self.bot.get_channel(cid)
        if ch is None:
            try:
                ch = await self.bot.fetch_channel(cid)
            except Exception:
                return
        if not isinstance(ch, discord.TextChannel):
            return
        e = Embed(title="Referral credit", color=discord.Color.blurple())
        e.add_field(name="Referrer", value=f"<@{referrer_id}> ({referrer_id})", inline=False)
        e.add_field(name="Referred buyer", value=f"{interaction.user} ({interaction.user.id})", inline=False)
        e.add_field(name="Tier", value=tier_value, inline=True)
        e.add_field(name="Chain", value=chain_value, inline=True)
        e.add_field(name="Tx", value=f"`{tx_store[:20]}…`", inline=True)
        e.add_field(name="Credited (raw units)", value=str(int(credited_raw)), inline=True)
        try:
            await ch.send(embed=e)
        except Exception:
            pass

    @tasks.loop(hours=1)
    async def expire_monthly_subscriptions(self) -> None:
        now = datetime.now(timezone.utc)
        pairs = payment_database.list_expired_subscriptions(now)
        monthly_rid = config.PREMIUM_MONTHLY_ROLE_ID
        if not monthly_rid or not pairs:
            return
        for user_id, guild_id in pairs:
            guild = self.bot.get_guild(guild_id)
            if guild is None:
                try:
                    guild = await self.bot.fetch_guild(guild_id)
                except Exception:
                    payment_database.delete_subscription(user_id, guild_id)
                    continue
            role = guild.get_role(monthly_rid)
            if not role:
                payment_database.delete_subscription(user_id, guild_id)
                continue
            try:
                member = guild.get_member(user_id) or await guild.fetch_member(user_id)
            except Exception:
                payment_database.delete_subscription(user_id, guild_id)
                continue
            if role in member.roles:
                try:
                    await member.remove_roles(role, reason="Monthly premium expired")
                except discord.Forbidden:
                    continue
            payment_database.delete_subscription(user_id, guild_id)

    @expire_monthly_subscriptions.before_loop
    async def _before_expire(self) -> None:
        await self.bot.wait_until_ready()


class ReferralCommands(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="referral_code", description="Get your referral code")
    async def referral_code(self, interaction: Interaction) -> None:
        if not getattr(config, "ENABLE_REFERRALS", True):
            await interaction.response.send_message("Referrals are disabled.", ephemeral=True)
            return
        payment_database.init_db()
        code = payment_database.get_or_create_referral_code(interaction.user.id)
        await interaction.response.send_message(
            f"Your referral code is **`{code}`**\nShare it: when someone uses it and buys premium, you earn **{int(getattr(config,'REFERRAL_PAYOUT_PCT',0.2)*100)}%**.",
            ephemeral=True,
        )

    @app_commands.command(name="use_referral", description="Link yourself to someone’s referral code (one-time)")
    @app_commands.describe(code="Referral code")
    async def use_referral(self, interaction: Interaction, code: str) -> None:
        if not getattr(config, "ENABLE_REFERRALS", True):
            await interaction.response.send_message("Referrals are disabled.", ephemeral=True)
            return
        payment_database.init_db()
        referrer = payment_database.lookup_referrer_by_code(code)
        if not referrer:
            await interaction.response.send_message("Invalid referral code.", ephemeral=True)
            return
        ok, msg = payment_database.set_referral(
            interaction.user.id,
            referrer,
            code_used=code,
            guild_id=interaction.guild.id if interaction.guild else None,
            source="code",
        )
        await interaction.response.send_message(("✅ " if ok else "❌ ") + msg, ephemeral=True)

    @app_commands.command(
        name="referral_set",
        description="(Admin) Manually link referrer → referred user (fallback if invite tracking missed).",
    )
    @app_commands.describe(referrer="Who referred", referred="Who was referred")
    async def referral_set(
        self,
        interaction: Interaction,
        referrer: discord.Member,
        referred: discord.Member,
    ) -> None:
        if not interaction.guild:
            await interaction.response.send_message("Use this in a server.", ephemeral=True)
            return
        if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("You need **Manage Server** to use this.", ephemeral=True)
            return
        payment_database.init_db()
        ok, msg = payment_database.set_referral(
            referred.id,
            referrer.id,
            guild_id=interaction.guild.id,
            source="manual",
        )
        await interaction.response.send_message(("✅ " if ok else "❌ ") + msg, ephemeral=True)

    @app_commands.command(name="referral_balance", description="See your referral earnings (raw units by chain)")
    async def referral_balance(self, interaction: Interaction) -> None:
        if not getattr(config, "ENABLE_REFERRALS", True):
            await interaction.response.send_message("Referrals are disabled.", ephemeral=True)
            return
        payment_database.init_db()
        bal = payment_database.referral_balance_by_chain(interaction.user.id)
        if not bal:
            await interaction.response.send_message("No referral credits yet.", ephemeral=True)
            return
        lines = [f"**{ch}**: `{amt}` raw" for ch, amt in bal.items()]
        await interaction.response.send_message("Your referral credits:\n" + "\n".join(lines), ephemeral=True)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(PremiumPaymentCommands(bot))
    await bot.add_cog(ReferralCommands(bot))
