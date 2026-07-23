"""Discord commands for the engagement points system.

Read-only reporting over the `engagement_events` ledger. Because every total
is derived from recorded events rather than a bare counter, each number here
can be broken down into the activity that produced it.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import discord
from discord.ext import commands

import engagement
from brand_assets import brand_logo_embed_icon

logger = logging.getLogger(__name__)

_BRAND = "VELCOR3"
_COLOR = 0x00C4FF

# Friendly labels for the ledger's event_type values.
_TYPE_LABELS = {
    "message_activity": "💬 Chat",
    "alpha_call": "📣 Calls posted",
    "vote_cast": "🗳️ Votes cast",
    "call_vote_received": "⭐ Votes received",
    "x_engage": "🚀 X engagement",
}


def _label(event_type: str) -> str:
    return _TYPE_LABELS.get(event_type, f"`{event_type}`")


def _fmt_dt(raw: str) -> str:
    """Ledger timestamps are ISO-8601 UTC; render as a Discord relative stamp."""
    if not raw:
        return "—"
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return f"<t:{int(dt.timestamp())}:R>"
    except Exception:
        return raw[:19]


class EngagementCommands(commands.Cog, name="Engagement"):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    # ---------------------------------------------------------------- leaderboard
    @commands.command(name="engagelb", aliases=["engageleaderboard", "pointslb"])
    async def engage_leaderboard(self, ctx: commands.Context, limit: int = 10) -> None:
        """Top engagement earners. `!velcor3 engagelb [limit]`"""
        if not ctx.guild:
            return
        limit = max(1, min(25, int(limit)))
        rows = engagement.get_points_leaderboard(limit)

        embed = discord.Embed(
            title="🏆 Engagement Leaderboard",
            color=_COLOR,
            timestamp=datetime.now(timezone.utc),
        )
        icon = brand_logo_embed_icon()
        if icon:
            embed.set_author(name=f"{_BRAND} · Top Earners", icon_url=icon)

        if not rows:
            embed.description = (
                "No engagement recorded yet.\n"
                "If this is unexpected, check that `ENGAGE_POINTS_CHANNEL_IDS` is set."
            )
        else:
            medals = {0: "🥇", 1: "🥈", 2: "🥉"}
            lines = []
            for i, r in enumerate(rows):
                rank = medals.get(i, f"`#{i + 1}`")
                lines.append(
                    f"{rank} <@{r['user_id']}> — **{int(r['points']):+d} pts** "
                    f"({int(r['events'])} events)"
                )
            embed.description = "\n".join(lines)[:4000]

        embed.set_footer(text=f"All-time · top {limit}")
        await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

    # ---------------------------------------------------------------- weekly
    @commands.command(name="engageweek", aliases=["weeklylb"])
    async def engage_week(self, ctx: commands.Context, limit: int = 10) -> None:
        """Top earners over the last 7 days. `!velcor3 engageweek [limit]`"""
        if not ctx.guild:
            return
        limit = max(1, min(25, int(limit)))
        rows = engagement.get_points_leaderboard(limit, days=7)

        embed = discord.Embed(
            title="📅 Weekly Engagement Leaderboard",
            color=_COLOR,
            timestamp=datetime.now(timezone.utc),
        )
        icon = brand_logo_embed_icon()
        if icon:
            embed.set_author(name=f"{_BRAND} · Last 7 Days", icon_url=icon)

        if not rows:
            embed.description = "No engagement in the last 7 days."
        else:
            medals = {0: "🥇", 1: "🥈", 2: "🥉"}
            embed.description = "\n".join(
                f"{medals.get(i, f'`#{i + 1}`')} <@{r['user_id']}> — "
                f"**{int(r['points']):+d} pts** ({int(r['events'])} events)"
                for i, r in enumerate(rows)
            )[:4000]

        embed.set_footer(text=f"Rolling 7 days · top {limit}")
        await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

    # ---------------------------------------------------------------- member
    @commands.command(name="engagescore", aliases=["points", "myscore"])
    async def engage_score(self, ctx: commands.Context,
                           member: Optional[discord.Member] = None) -> None:
        """A member's engagement breakdown. `!velcor3 points [@member]`"""
        if not ctx.guild:
            return
        target = member or ctx.author
        s = engagement.get_user_breakdown(target.id)

        embed = discord.Embed(
            title=f"{target.display_name}'s Engagement",
            description=target.mention,
            color=_COLOR,
            timestamp=datetime.now(timezone.utc),
        )
        icon = brand_logo_embed_icon()
        if icon:
            embed.set_author(name=f"{_BRAND} · Member Activity", icon_url=icon)
        embed.set_thumbnail(url=target.display_avatar.url)

        rank = f"#{s['rank']}" if s["rank"] else "Unranked"
        embed.add_field(name="Total", value=f"**{s['total']:+d} pts**", inline=True)
        embed.add_field(name="Rank", value=f"**{rank}**", inline=True)
        embed.add_field(name="Events", value=f"**{s['events']}**", inline=True)
        embed.add_field(name="Today", value=f"**{s['today']:+d} pts**", inline=True)
        embed.add_field(name="Last 7d", value=f"**{s['week']:+d} pts**", inline=True)
        embed.add_field(
            name="Daily cap",
            value=f"**{s['today']}/{engagement.GLOBAL_DAILY_CAP}**",
            inline=True,
        )

        if s["by_type"]:
            embed.add_field(
                name="Breakdown",
                value="\n".join(
                    f"{_label(t['event_type'])} — **{int(t['points']):+d} pts** "
                    f"({int(t['events'])}x)"
                    for t in s["by_type"]
                )[:1024],
                inline=False,
            )
        else:
            embed.add_field(
                name="Breakdown",
                value="No activity recorded yet.",
                inline=False,
            )

        if s["events"]:
            embed.add_field(
                name="Activity",
                value=f"First: {_fmt_dt(s['first_at'])}\nLast: {_fmt_dt(s['last_at'])}",
                inline=False,
            )

        embed.set_footer(text="Engagement points · separate from $V3 and Alpha Score")
        await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

    # ---------------------------------------------------------------- server
    @commands.command(name="engagestats", aliases=["activity"])
    async def engage_stats(self, ctx: commands.Context, days: int = 7) -> None:
        """Server-wide activity overview. `!velcor3 engagestats [days]`"""
        if not ctx.guild:
            return
        days = max(1, min(90, int(days)))
        s = engagement.get_activity_summary(days)
        win, today, allt = s["window"], s["today"], s["alltime"]

        embed = discord.Embed(
            title="📊 Engagement Activity",
            color=_COLOR,
            timestamp=datetime.now(timezone.utc),
        )
        icon = brand_logo_embed_icon()
        if icon:
            embed.set_author(name=f"{_BRAND} · Server Activity", icon_url=icon)

        embed.add_field(
            name="Today",
            value=(
                f"**{int(today.get('points') or 0):+d} pts**\n"
                f"{int(today.get('events') or 0)} events · "
                f"{int(today.get('members') or 0)} members"
            ),
            inline=True,
        )
        embed.add_field(
            name=f"Last {days}d",
            value=(
                f"**{int(win.get('points') or 0):+d} pts**\n"
                f"{int(win.get('events') or 0)} events · "
                f"{int(win.get('members') or 0)} members"
            ),
            inline=True,
        )
        embed.add_field(
            name="All time",
            value=(
                f"**{int(allt.get('points') or 0):+d} pts**\n"
                f"{int(allt.get('events') or 0)} events · "
                f"{int(allt.get('members') or 0)} members"
            ),
            inline=True,
        )

        if s["by_type"]:
            embed.add_field(
                name=f"By activity (last {days}d)",
                value="\n".join(
                    f"{_label(t['event_type'])} — **{int(t['points']):+d} pts** · "
                    f"{int(t['events'])} events · {int(t['members'])} members"
                    for t in s["by_type"]
                )[:1024],
                inline=False,
            )
        else:
            embed.add_field(
                name=f"By activity (last {days}d)",
                value="No activity in this window.",
                inline=False,
            )

        backlog = engagement.log_backlog_size()
        footer = f"Rolling {days} days"
        if backlog:
            footer += f" · {backlog} log entries queued"
        embed.set_footer(text=footer)
        await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

    # ---------------------------------------------------------------- config check
    @commands.command(name="engageconfig")
    @commands.has_permissions(manage_guild=True)
    async def engage_config(self, ctx: commands.Context) -> None:
        """Show the live earning rules and channel setup (admin only).

        Exists mainly to debug the most common failure: chat earning nothing
        because no channel allowlist is configured.
        """
        if not ctx.guild:
            return
        pts_ids = engagement._points_channel_ids()
        alpha_ids = engagement._alpha_channel_ids()
        earning = pts_ids | alpha_ids

        embed = discord.Embed(
            title="⚙️ Engagement Configuration",
            color=_COLOR,
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(
            name="Points per action",
            value=(
                f"Message **{engagement.MSG_POINTS}** · Reply **{engagement.MSG_REPLY_POINTS}**\n"
                f"Alpha channel **×{engagement.ALPHA_CHANNEL_MULTIPLIER}** · "
                f"Holder **×{engagement.HOLDER_MULTIPLIER}**\n"
                f"Call **{engagement.CALL_POINTS}** · Vote cast **{engagement.VOTE_CAST_POINTS}**\n"
                f"Cook received **{engagement.COOK_RECEIVED_POINTS}** · "
                f"Skip received **{engagement.SKIP_RECEIVED_POINTS}**\n"
                f"X engage **{engagement.X_ENGAGE_POINTS}**"
            ),
            inline=False,
        )
        embed.add_field(
            name="Daily caps",
            value=(
                f"Chat **{engagement.MSG_DAILY_CAP}** · Calls **{engagement.CALL_DAILY_CAP}**\n"
                f"Votes **{engagement.VOTE_CAST_DAILY_CAP}** · "
                f"Cook received **{engagement.COOK_RECEIVED_DAILY_CAP}**\n"
                f"X engage **{engagement.X_ENGAGE_DAILY_CAP}/day** · "
                f"**Global {engagement.GLOBAL_DAILY_CAP}**"
            ),
            inline=False,
        )
        embed.add_field(
            name="Anti-spam",
            value=(
                f"Cooldown **{engagement.MSG_COOLDOWN_SEC}s** · "
                f"Min length **{engagement.MSG_MIN_CHARS}**\n"
                f"Account age **{engagement.MIN_ACCOUNT_AGE_DAYS}d** · "
                f"Member age **{engagement.MIN_MEMBER_AGE_HOURS}h**"
            ),
            inline=False,
        )
        if earning:
            shown = ", ".join(f"<#{c}>" for c in list(earning)[:15])
            embed.add_field(
                name=f"Earning channels ({len(earning)})",
                value=shown[:1024]
                + (f"\n*alpha ×{engagement.ALPHA_CHANNEL_MULTIPLIER}: "
                   + ", ".join(f"<#{c}>" for c in list(alpha_ids)[:10]) + "*" if alpha_ids else ""),
                inline=False,
            )
        else:
            embed.add_field(
                name="⚠️ Earning channels",
                value=(
                    "**None configured — chat earns nothing.**\n"
                    "Set `ENGAGE_POINTS_CHANNEL_IDS` (and optionally "
                    "`ENGAGE_ALPHA_CHANNEL_IDS`) to enable chat points."
                ),
                inline=False,
            )
        log_state = (
            f"<#{engagement.LOG_CHANNEL_ID}> every {engagement.LOG_FLUSH_SECONDS}s"
            if engagement.LOG_ENABLED and engagement.LOG_CHANNEL_ID
            else "disabled"
        )
        embed.add_field(name="Audit log", value=log_state, inline=False)
        await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

    @engage_config.error
    async def _engage_config_error(self, ctx: commands.Context, error) -> None:
        if isinstance(error, commands.MissingPermissions):
            await ctx.send("You need **Manage Server** to view this.", delete_after=8)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(EngagementCommands(bot))
