"""
Discord UI panels: member verification (role) and crypto payment instructions.
Configure role IDs and addresses in config / .env.
"""
from __future__ import annotations

from datetime import datetime, timezone

import aiohttp
import discord
from discord import app_commands, Interaction, Embed, Color
from discord.ext import commands
from discord.utils import format_dt
from typing import Optional, Tuple

import config
import payment_database


def _treasury_eth() -> str:
    return (getattr(config, "PAYMENT_TREASURY_ETH_MAINNET", None) or config.CRYPTO_ETH_ADDRESS or "").strip()


def _treasury_base() -> str:
    return (getattr(config, "PAYMENT_TREASURY_ETH_BASE", None) or config.CRYPTO_ETH_ADDRESS or "").strip()


def _treasury_sol() -> str:
    return (getattr(config, "PAYMENT_TREASURY_SOL", None) or config.CRYPTO_SOL_ADDRESS or "").strip()


async def _fetch_eth_sol_usd(session: aiohttp.ClientSession) -> Tuple[float, float]:
    """CoinGecko spot USD prices for display only."""
    url = "https://api.coingecko.com/api/v3/simple/price?ids=ethereum,solana&vs_currencies=usd"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
        if r.status != 200:
            raise RuntimeError(f"CoinGecko HTTP {r.status}")
        data = await r.json()
    eth = float(data["ethereum"]["usd"])
    sol = float(data["solana"]["usd"])
    return eth, sol


async def _native_amounts_for_usd(session: Optional[aiohttp.ClientSession], usd: float) -> Tuple[str, str, str]:
    """
    Returns (eth_line, sol_line, source_note).
    ETH/Base use same ~USD/ETH; Base native is ETH.
    """
    try:
        if session is None:
            raise RuntimeError("no session")
        eth_usd, sol_usd = await _fetch_eth_sol_usd(session)
        src = "Live rates (CoinGecko)"
    except Exception:
        eth_usd = float(getattr(config, "PAYMENT_PRICE_FALLBACK_ETH_USD", 3500.0))
        sol_usd = float(getattr(config, "PAYMENT_PRICE_FALLBACK_SOL_USD", 150.0))
        src = "Fallback rates (set `PAYMENT_PRICE_FALLBACK_*_USD` in `.env`)"

    if eth_usd <= 0 or sol_usd <= 0:
        eth_usd = float(getattr(config, "PAYMENT_PRICE_FALLBACK_ETH_USD", 3500.0))
        sol_usd = float(getattr(config, "PAYMENT_PRICE_FALLBACK_SOL_USD", 150.0))
        src = "Fallback rates"

    eth_amt = usd / eth_usd
    sol_amt = usd / sol_usd
    eth_s = f"{eth_amt:.6f}".rstrip("0").rstrip(".")
    sol_s = f"{sol_amt:.4f}".rstrip("0").rstrip(".")
    eth_line = f"**Ethereum mainnet** & **Base:** ~`{eth_s}` ETH _(≈ ${usd:.0f})_"
    sol_line = f"**Solana:** ~`{sol_s}` SOL _(≈ ${usd:.0f})_"
    return eth_line, sol_line, src


def _verification_embed() -> Embed:
    rules = getattr(
        config,
        "VERIFICATION_RULES_TEXT",
        "Read the server rules and community guidelines. Click the button below to unlock the server.",
    )
    e = Embed(
        title="Member verification",
        description=rules,
        color=Color.green(),
    )
    e.add_field(
        name="How it works",
        value="1. Read the server rules.\n2. Press **I agree — verify me**.\n3. You receive the verified role.",
        inline=False,
    )
    e.set_footer(text="Velcor3 • Verification")
    return e


async def _send_verification_log(
    client: discord.Client,
    guild: discord.Guild,
    member: discord.Member,
) -> None:
    log_id = int(getattr(config, "VERIFICATION_LOG_CHANNEL_ID", 0) or 0)
    if not log_id:
        return
    ch = client.get_channel(log_id)
    if ch is None:
        try:
            ch = await client.fetch_channel(log_id)
        except Exception:
            return
    if not isinstance(ch, discord.abc.Messageable):
        return

    payment_database.init_db()
    ref = payment_database.get_referral_record(member.id)
    inviter_lines: list[str] = []
    if ref:
        rid = ref.referrer_user_id
        inv_m = guild.get_member(rid)
        if inv_m is None:
            try:
                inv_m = await guild.fetch_member(rid)
            except Exception:
                inv_m = None
        if inv_m is not None:
            inviter_lines.append(f"{inv_m.mention} — `{inv_m}` · ID `{rid}`")
        else:
            try:
                u = await client.fetch_user(rid)
                inviter_lines.append(f"`{u}` · ID `{rid}` _(not in server)_")
            except Exception:
                inviter_lines.append(f"User ID `{rid}` _(could not resolve)_")
        if ref.invite_code:
            inviter_lines.append(f"Invite code: `{ref.invite_code}`")
        if ref.source:
            inviter_lines.append(f"Tracked source: `{ref.source}`")
        if ref.created_at:
            inviter_lines.append(f"Invite tracked at: `{ref.created_at}`")
    else:
        inviter_lines.append(
            "Not recorded — Server Discovery, vanity URL, bot offline at join, "
            "or invite tracking disabled (`ENABLE_INVITE_REFERRALS`)."
        )
    inviter_block = "\n".join(inviter_lines)[:1024]

    tag = f"{member.name}" + (f"#{member.discriminator}" if member.discriminator != "0" else "")
    lines = [
        f"**Mention:** {member.mention}",
        f"**Tag:** `{tag}`",
        f"**Global name:** `{member.global_name or '—'}`",
        f"**Server nick:** `{member.nick or '—'}`",
        f"**User ID:** `{member.id}`",
    ]
    details = "\n".join(lines)

    e = Embed(
        title="Member verified",
        color=Color.green(),
        timestamp=datetime.now(timezone.utc),
    )
    e.set_author(name=str(member), icon_url=member.display_avatar.url if member.display_avatar else None)
    e.add_field(name="Discord", value=details[:1024], inline=False)
    e.add_field(name="Account created", value=format_dt(member.created_at, "F"), inline=True)
    joined = member.joined_at
    e.add_field(
        name="Joined server",
        value=format_dt(joined, "F") if joined else "—",
        inline=True,
    )
    e.add_field(name="Invited by", value=inviter_block, inline=False)
    e.set_footer(text=f"{guild.name} · verification log")

    await ch.send(embed=e)


async def _payment_embed(session: aiohttp.ClientSession) -> Embed:
    notes = getattr(
        config,
        "PAYMENT_PANEL_NOTES",
        "Use the buttons to copy treasury addresses. Then use /claim_premium with your tx hash.",
    )
    price_usd = float(getattr(config, "PAYMENT_PANEL_PRICE_USD", 30.0) or 30.0)
    eth_line, sol_line, rate_src = await _native_amounts_for_usd(session, price_usd)

    eth = _treasury_eth() or "— not set —"
    base = _treasury_base() or "— not set —"
    sol = _treasury_sol() or "— not set —"
    usdt = getattr(config, "CRYPTO_USDT_ERC20_ADDRESS", "") or "— not set —"
    monthly_days = int(getattr(config, "PREMIUM_MONTHLY_DAYS", 30) or 30)

    sub_lines = (
        f"**Monthly premium — ~${price_usd:.0f} USD** (shown in native below; set `PAYMENT_PANEL_PRICE_USD` to change).\n"
        f"Role for **{monthly_days} days** (renew before expiry).\n"
        f"**Lifetime premium** — set min amounts in `.env` (`PAYMENT_*_MIN_*`); then pay & claim.\n"
        f"{eth_line}\n{sol_line}\n"
        f"_Rates: {rate_src} — amounts are approximate; send at least your **`/claim_premium`** minimum._\n"
        f"After paying, run **`/claim_premium`** → tier → **Ethereum mainnet**, **Base**, or **Solana** → paste **tx hash**."
    )
    e = Embed(
        title="Payments & premium subscription",
        description=notes,
        color=Color.blue(),
    )
    e.add_field(name="Premium subscription", value=sub_lines[:1024], inline=False)
    e.add_field(
        name="Treasury — Ethereum mainnet (native ETH)",
        value=f"`{eth}`",
        inline=False,
    )
    e.add_field(
        name="Treasury — Base (native ETH)",
        value=f"`{base}`",
        inline=False,
    )
    e.add_field(
        name="Treasury — Solana (native SOL)",
        value=f"`{sol}`",
        inline=False,
    )
    e.add_field(name="Other — USDT (ERC-20)", value=f"`{usdt}`", inline=False)
    extra = getattr(config, "PAYMENT_EXTRA_INSTRUCTIONS", "")
    if extra:
        e.add_field(name="Additional instructions", value=extra[:1024], inline=False)
    e.set_footer(text="Velcor3 • Treasuries match /claim_premium — staff never DM you first")
    return e


class VerificationView(discord.ui.View):
    """Persistent view — custom_id must stay stable across restarts."""

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="I agree — verify me",
        style=discord.ButtonStyle.success,
        custom_id="nerds_verify_v1",
    )
    async def verify(self, interaction: discord.Interaction, button: discord.ui.Button):
        rid = int(getattr(config, "VERIFIED_ROLE_ID", 0) or 0)
        if not rid:
            await interaction.response.send_message(
                "Verification is not configured (missing `VERIFIED_ROLE_ID`). Ask an admin.",
                ephemeral=True,
            )
            return
        if not interaction.guild:
            await interaction.response.send_message("Use this inside a server.", ephemeral=True)
            return
        role = interaction.guild.get_role(rid)
        if not role:
            await interaction.response.send_message(
                "Verified role not found. Ask an admin to check `VERIFIED_ROLE_ID`.",
                ephemeral=True,
            )
            return
        member = interaction.user
        if not isinstance(member, discord.Member):
            await interaction.response.send_message("Could not resolve member.", ephemeral=True)
            return
        if role in member.roles:
            await interaction.response.send_message("You are already verified.", ephemeral=True)
            return
        try:
            await member.add_roles(role, reason="Verification panel")
        except discord.Forbidden:
            await interaction.response.send_message(
                "I cannot assign that role (missing permissions). Ask an admin.",
                ephemeral=True,
            )
            return
        except discord.HTTPException as e:
            await interaction.response.send_message(f"Could not assign role: {e}", ephemeral=True)
            return
        try:
            await _send_verification_log(interaction.client, interaction.guild, member)
        except Exception:
            pass
        await interaction.response.send_message(
            f"You are verified. Welcome — you now have {role.mention}.",
            ephemeral=True,
        )


class CryptoPaymentView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def _send_address(self, interaction: discord.Interaction, label: str, addr: str):
        if not addr or addr.startswith("—"):
            await interaction.response.send_message(
                f"{label} is not configured. Ask an admin to set it in `.env`.",
                ephemeral=True,
            )
            return
        await interaction.response.send_message(
            f"**{label}**\n```{addr}```\n_Copy on desktop; long-press to copy on mobile._",
            ephemeral=True,
        )

    @discord.ui.button(label="ETH mainnet", style=discord.ButtonStyle.primary, custom_id="nerds_pay_eth_v1")
    async def btn_eth(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._send_address(
            interaction,
            "Ethereum treasury (mainnet — use for /claim_premium)",
            _treasury_eth(),
        )

    @discord.ui.button(label="Base ETH", style=discord.ButtonStyle.primary, custom_id="nerds_pay_base_v1")
    async def btn_base(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._send_address(
            interaction,
            "Base treasury (native ETH — use /claim_premium → Base)",
            _treasury_base(),
        )

    @discord.ui.button(label="SOL", style=discord.ButtonStyle.primary, custom_id="nerds_pay_sol_v1")
    async def btn_sol(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._send_address(
            interaction,
            "Solana treasury (use for /claim_premium)",
            _treasury_sol(),
        )

    @discord.ui.button(label="USDT (ERC-20)", style=discord.ButtonStyle.secondary, custom_id="nerds_pay_usdt_v1")
    async def btn_usdt(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._send_address(
            interaction,
            "USDT (ERC-20) address",
            getattr(config, "CRYPTO_USDT_ERC20_ADDRESS", "") or "",
        )

    @discord.ui.button(label="I sent a payment", style=discord.ButtonStyle.success, custom_id="nerds_pay_notify_v1")
    async def btn_notify(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "For **premium**, run **`/claim_premium`** here and submit your **tx hash** (and tier + chain). "
            "For other payments, open a **support ticket** with tx hash and amount. "
            "Staff will never ask for your seed phrase.",
            ephemeral=True,
        )


async def post_verification_to_channel(channel: discord.TextChannel) -> None:
    """Send verification embed + buttons (used by slash and prefix)."""
    await channel.send(embed=_verification_embed(), view=VerificationView())


async def post_crypto_to_channel(channel: discord.TextChannel) -> None:
    """Send crypto payment embed + buttons (fetches live USD→native hints)."""
    async with aiohttp.ClientSession() as session:
        embed = await _payment_embed(session)
    await channel.send(embed=embed, view=CryptoPaymentView())


def _needs_manage_guild(interaction: Interaction) -> bool:
    if not interaction.guild:
        return False
    m = interaction.user
    if not isinstance(m, discord.Member):
        return False
    return m.guild_permissions.manage_guild


class PanelCommands(commands.Cog):
    """Slash commands to post verification and payment panels (Manage Server)."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="verification_panel", description="Post the member verification panel (Manage Server)")
    @app_commands.describe(channel="Where to post (default: this channel)")
    async def verification_panel(
        self,
        interaction: Interaction,
        channel: Optional[discord.TextChannel] = None,
    ):
        if not _needs_manage_guild(interaction):
            await interaction.response.send_message(
                "You need **Manage Server** to post this panel.",
                ephemeral=True,
            )
            return
        ch = channel or interaction.channel
        if not isinstance(ch, discord.TextChannel):
            await interaction.response.send_message("Use this in a text channel.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        await post_verification_to_channel(ch)
        await interaction.followup.send(f"Posted verification panel in {ch.mention}.", ephemeral=True)

    @app_commands.command(name="crypto_payment_panel", description="Post the crypto payment info panel (Manage Server)")
    @app_commands.describe(channel="Where to post (default: this channel)")
    async def crypto_payment_panel(
        self,
        interaction: Interaction,
        channel: Optional[discord.TextChannel] = None,
    ):
        if not _needs_manage_guild(interaction):
            await interaction.response.send_message(
                "You need **Manage Server** to post this panel.",
                ephemeral=True,
            )
            return
        ch = channel or interaction.channel
        if not isinstance(ch, discord.TextChannel):
            await interaction.response.send_message("Use this in a text channel.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        await post_crypto_to_channel(ch)
        await interaction.followup.send(f"Posted crypto payment panel in {ch.mention}.", ephemeral=True)
