"""
alpha_ping.py — Alpha channel auto-intel card + voting + score tracking.

Flow:
  1. Member posts ANY message in ALPHA_CHANNEL_ID
  2. Bot deletes raw message → posts loading embed
  3. If message contains X link/handle → fetches project profile + VELCOR3 research
  4. Final embed: project intel card + poster's cumulative alpha score + voting buttons
  5. !velcor3 alphaping — pings ALPHA_TARGET_ROLE_ID (only works in alpha channel,
     requires ALPHA_POSTER_ROLE_ID)

Config:
  ALPHA_CHANNEL_ID      1517874398537711706
  ALPHA_POSTER_ROLE_ID  1506380353877577738  (required for alphaping cmd)
  ALPHA_TARGET_ROLE_ID  1505260948707999774
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import discord
from discord.ext import commands

import database
from app_paths import DATA_DIR
from brand_assets import brand_logo_embed_icon
from trackers.wl_request_handler import (
    X_PROFILE_RE,
    _RESERVED_X_PATHS,
    _account_age_days,
    _fmt_account_age,
    _fmt_followers,
    _normalize_pfp_url,
    _research_block,
    fetch_live_profile,
    hydrate_wl_embed_art,
    parse_x_handle,
    resolve_first_alert_link,
    _enrich_profile_art,
)

logger = logging.getLogger(__name__)

_BRAND = "VELCOR3"

# ── Config ────────────────────────────────────────────────────────────────────

ALPHA_CHANNEL_ID     = int(os.getenv("ALPHA_CHANNEL_ID",     "1517874398537711706"))
ALPHA_POSTER_ROLE_ID = int(os.getenv("ALPHA_POSTER_ROLE_ID", "1506380353877577738"))
ALPHA_TARGET_ROLE_ID = int(os.getenv("ALPHA_TARGET_ROLE_ID", "1505260948707999774"))

SCORE_GOOD = int(os.getenv("ALPHA_SCORE_GOOD",  "1"))
SCORE_NOT  = int(os.getenv("ALPHA_SCORE_NOT",  "-1"))

DB_PATH = DATA_DIR / "user_stats.db"


# ── Database ──────────────────────────────────────────────────────────────────

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_alpha_tables() -> None:
    conn = _db()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS alpha_posts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id    INTEGER NOT NULL,
            channel_id  INTEGER NOT NULL,
            message_id  INTEGER NOT NULL UNIQUE,
            poster_id   INTEGER NOT NULL,
            content     TEXT    NOT NULL,
            handle      TEXT,
            posted_at   TEXT    NOT NULL
        );
        CREATE TABLE IF NOT EXISTS alpha_votes (
            post_id   INTEGER NOT NULL,
            voter_id  INTEGER NOT NULL,
            vote      TEXT    NOT NULL,
            voted_at  TEXT    NOT NULL,
            PRIMARY KEY (post_id, voter_id)
        );
        CREATE TABLE IF NOT EXISTS alpha_scores (
            guild_id  INTEGER NOT NULL,
            user_id   INTEGER NOT NULL,
            score     INTEGER DEFAULT 0,
            posts     INTEGER DEFAULT 0,
            PRIMARY KEY (guild_id, user_id)
        );
        """
    )
    conn.commit()
    conn.close()


def save_alpha_post(guild_id: int, channel_id: int, message_id: int,
                    poster_id: int, content: str, handle: Optional[str] = None) -> int:
    conn = _db()
    c = conn.cursor()
    c.execute(
        """
        INSERT OR IGNORE INTO alpha_posts
            (guild_id, channel_id, message_id, poster_id, content, handle, posted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (guild_id, channel_id, message_id, poster_id, content[:500], handle,
         datetime.now(timezone.utc).isoformat()),
    )
    post_id = c.lastrowid
    c.execute(
        """
        INSERT INTO alpha_scores (guild_id, user_id, score, posts) VALUES (?, ?, 0, 1)
        ON CONFLICT(guild_id, user_id) DO UPDATE SET posts = posts + 1
        """,
        (guild_id, poster_id),
    )
    conn.commit()
    conn.close()
    return post_id


def get_post_by_message(message_id: int) -> Optional[sqlite3.Row]:
    conn = _db()
    row = conn.execute(
        "SELECT * FROM alpha_posts WHERE message_id = ?", (message_id,)
    ).fetchone()
    conn.close()
    return row


def get_poster_score(guild_id: int, user_id: int) -> Tuple[int, int]:
    """Returns (score, posts)."""
    conn = _db()
    row = conn.execute(
        "SELECT score, posts FROM alpha_scores WHERE guild_id = ? AND user_id = ?",
        (guild_id, user_id),
    ).fetchone()
    conn.close()
    if not row:
        return 0, 0
    return row["score"], row["posts"]


def cast_vote(post_id: int, voter_id: int, vote: str,
              poster_id: int, guild_id: int) -> str:
    """Returns 'new' | 'changed' | 'already_same'."""
    conn = _db()
    c = conn.cursor()
    existing = c.execute(
        "SELECT vote FROM alpha_votes WHERE post_id = ? AND voter_id = ?",
        (post_id, voter_id),
    ).fetchone()

    if existing:
        if existing["vote"] == vote:
            conn.close()
            return "already_same"
        old_delta = SCORE_GOOD if existing["vote"] == "good" else SCORE_NOT
        new_delta = SCORE_GOOD if vote == "good" else SCORE_NOT
        net = new_delta - old_delta
        c.execute(
            "UPDATE alpha_votes SET vote = ?, voted_at = ? WHERE post_id = ? AND voter_id = ?",
            (vote, datetime.now(timezone.utc).isoformat(), post_id, voter_id),
        )
        result = "changed"
    else:
        net = SCORE_GOOD if vote == "good" else SCORE_NOT
        c.execute(
            "INSERT INTO alpha_votes (post_id, voter_id, vote, voted_at) VALUES (?, ?, ?, ?)",
            (post_id, voter_id, vote, datetime.now(timezone.utc).isoformat()),
        )
        result = "new"

    c.execute(
        """
        INSERT INTO alpha_scores (guild_id, user_id, score, posts) VALUES (?, ?, ?, 0)
        ON CONFLICT(guild_id, user_id) DO UPDATE SET score = score + ?
        """,
        (guild_id, poster_id, net, net),
    )
    conn.commit()
    conn.close()
    return result


def get_vote_counts(post_id: int) -> Tuple[int, int]:
    conn = _db()
    rows = conn.execute(
        "SELECT vote, COUNT(*) as n FROM alpha_votes WHERE post_id = ? GROUP BY vote",
        (post_id,),
    ).fetchall()
    conn.close()
    counts = {r["vote"]: r["n"] for r in rows}
    return counts.get("good", 0), counts.get("not", 0)


def get_leaderboard(guild_id: int, limit: int = 10) -> list:
    conn = _db()
    rows = conn.execute(
        """
        SELECT user_id, score, posts FROM alpha_scores
        WHERE guild_id = ?
        ORDER BY score DESC, posts DESC
        LIMIT ?
        """,
        (guild_id, limit),
    ).fetchall()
    conn.close()
    return rows


# ── Score tier label ──────────────────────────────────────────────────────────

def _score_tier(score: int) -> str:
    if score >= 50:  return "🏆 Alpha Legend"
    if score >= 20:  return "💎 Diamond Caller"
    if score >= 10:  return "🔥 Hot Caller"
    if score >= 3:   return "✅ Trusted"
    if score >= 0:   return "🆕 New"
    return "⚠️ Low Rep"


# ── Embed builders ────────────────────────────────────────────────────────────

def build_loading_embed(poster: discord.Member, content: str, handle: Optional[str]) -> discord.Embed:
    desc = f"⏳ **Building alpha card**…\n\n> {content[:300]}"
    if handle:
        desc += f"\n\nFetching [@{handle}](https://x.com/{handle}) · VELCOR3 research"
    embed = discord.Embed(description=desc, color=0x2C2F33,
                          timestamp=datetime.now(timezone.utc))
    icon = brand_logo_embed_icon()
    if icon:
        embed.set_author(name=f"{_BRAND} · Alpha", icon_url=icon)
    embed.set_footer(text="Building card…")
    embed.set_thumbnail(url=poster.display_avatar.url)
    return embed


def build_alpha_embed(
    *,
    poster: discord.Member,
    content: str,
    handle: Optional[str],
    profile: Optional[Dict[str, Any]],
    research_text: Optional[str],
    score: int,
    posts: int,
) -> discord.Embed:
    icon = brand_logo_embed_icon()

    # Header: project-focused if handle found, content-focused otherwise
    if handle and profile:
        name = profile.get("name") or f"@{handle}"
        url = f"https://x.com/{handle}"
        embed = discord.Embed(
            title=f"📡 Alpha Drop · @{handle}",
            description=f"**{name}**\n{url}\n\n> {content[:300]}",
            color=0x00C4FF,
            timestamp=datetime.now(timezone.utc),
        )
    else:
        embed = discord.Embed(
            description=f"> {content[:400]}",
            color=0x00C4FF,
            timestamp=datetime.now(timezone.utc),
        )
        embed.title = "📡 Alpha Drop"

    if icon:
        embed.set_author(name=f"{_BRAND} · Alpha Channel", icon_url=icon)

    # ── Poster block ──────────────────────────────────────────────────────────
    tier = _score_tier(score)
    embed.add_field(
        name="Posted by",
        value=f"{poster.mention}\n{tier}",
        inline=True,
    )
    embed.add_field(
        name="Alpha Score",
        value=f"`{score:+d}` · {posts} post(s)",
        inline=True,
    )

    # ── Project block (only if X handle detected) ─────────────────────────────
    if handle and profile:
        embed.add_field(
            name="Followers",
            value=_fmt_followers(profile.get("followers_count")),
            inline=True,
        )
        embed.add_field(
            name="Account age",
            value=_fmt_account_age(profile.get("age_days"), profile.get("created_at")),
            inline=True,
        )
        bio = (profile.get("description") or "").strip()
        if bio:
            embed.add_field(name="Bio", value=bio[:512], inline=False)

        if research_text:
            embed.add_field(name=f"{_BRAND} Research", value=research_text[:1024], inline=False)

        embed.set_thumbnail(url=poster.display_avatar.url)
    else:
        embed.set_thumbnail(url=poster.display_avatar.url)

    # ── Voting guide ──────────────────────────────────────────────────────────
    embed.add_field(
        name="Votes",
        value="✅ 0  ·  ❌ 0",
        inline=True,
    )
    embed.set_footer(text=f"{_BRAND} · ✅ Good Alpha +1 rep  ·  ❌ Not Alpha −1 rep")
    return embed


# ── Voting View ───────────────────────────────────────────────────────────────

class AlphaVoteView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="✅ Good Alpha", style=discord.ButtonStyle.success,
                       custom_id="alpha_vote_good")
    async def vote_good(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self._handle(interaction, "good")

    @discord.ui.button(label="❌ Not Alpha", style=discord.ButtonStyle.danger,
                       custom_id="alpha_vote_not")
    async def vote_not(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self._handle(interaction, "not")

    async def _handle(self, interaction: discord.Interaction, vote: str):
        post = get_post_by_message(interaction.message.id)
        if not post:
            await interaction.response.send_message("Post not in DB.", ephemeral=True)
            return

        if interaction.user.id == post["poster_id"]:
            await interaction.response.send_message(
                "You can't vote on your own alpha.", ephemeral=True
            )
            return

        result = cast_vote(post["id"], interaction.user.id, vote,
                           post["poster_id"], post["guild_id"])

        if result == "already_same":
            label = "✅ Good Alpha" if vote == "good" else "❌ Not Alpha"
            await interaction.response.send_message(
                f"You already voted **{label}**.", ephemeral=True
            )
            return

        good, not_ = get_vote_counts(post["id"])
        # Update poster score shown in embed
        new_score, new_posts = get_poster_score(post["guild_id"], post["poster_id"])
        tier = _score_tier(new_score)

        if interaction.message.embeds:
            embed = interaction.message.embeds[0].copy()
            for i, f in enumerate(embed.fields):
                if f.name == "Votes":
                    embed.set_field_at(i, name="Votes",
                                       value=f"✅ {good}  ·  ❌ {not_}", inline=True)
                if f.name == "Alpha Score":
                    embed.set_field_at(i, name="Alpha Score",
                                       value=f"`{new_score:+d}` · {new_posts} post(s)", inline=True)
                if f.name == "Posted by":
                    # update tier
                    poster_mention = f.value.split("\n")[0]
                    embed.set_field_at(i, name="Posted by",
                                       value=f"{poster_mention}\n{tier}", inline=True)
            await interaction.message.edit(embed=embed, view=self)

        verb = "changed to" if result == "changed" else "cast as"
        label = "✅ Good Alpha" if vote == "good" else "❌ Not Alpha"
        await interaction.response.send_message(
            f"Vote {verb} **{label}**!", ephemeral=True
        )


# ── Cog ──────────────────────────────────────────────────────────────────────

class AlphaPingCog(commands.Cog, name="AlphaPing"):

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        init_alpha_tables()
        bot.add_view(AlphaVoteView())

    # ── on_message — auto-convert to intel card ───────────────────────────────

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.channel.id != ALPHA_CHANNEL_ID:
            return
        if message.author.bot:
            return
        if not message.guild:
            return

        content = (message.content or "").strip()
        if not content:
            return

        handle = parse_x_handle(content)
        poster = message.author

        # Delete raw message
        try:
            await message.delete()
        except (discord.NotFound, discord.Forbidden):
            pass

        # Post loading embed immediately
        loading = build_loading_embed(poster, content, handle)
        try:
            posted = await message.channel.send(embed=loading, view=AlphaVoteView())
        except Exception as e:
            logger.warning("[AlphaPing] Failed to post loading embed: %s", e)
            return

        # Fetch project data async
        profile: Optional[Dict[str, Any]] = None
        research_text: Optional[str] = None

        if handle:
            try:
                alert_link_task = asyncio.create_task(
                    resolve_first_alert_link(self.bot, handle)
                )
                live_task = asyncio.create_task(
                    fetch_live_profile(self.bot, handle)
                )
                alert_link, live = await asyncio.gather(
                    alert_link_task, live_task, return_exceptions=True
                )
                alert_link = alert_link if not isinstance(alert_link, Exception) else None
                live = live if not isinstance(live, Exception) else {}

                research_text, _ = _research_block(handle, alert_link=alert_link)

                profile = {"handle": handle, "description": ""}
                row = database.get_project_by_handle(handle)
                if row:
                    profile["name"] = row[2]
                    profile["description"] = (row[3] or "")[:300]
                    profile["created_at"] = row[4]
                    profile["age_days"] = _account_age_days(row[4])
                    profile["followers_count"] = live.get("followers_count") or row[8]
                    if live.get("age_days") is not None:
                        profile["age_days"] = live.get("age_days")
                        profile["created_at"] = live.get("created_at") or profile.get("created_at")
                else:
                    profile.update(live or {})

                await _enrich_profile_art(self.bot, handle, profile)
            except Exception as e:
                logger.warning("[AlphaPing] Project data fetch failed @%s: %s", handle, e)
                profile = None
                research_text = None

        # Save to DB using the final posted message ID (buttons live here)
        score, posts = get_poster_score(message.guild.id, poster.id)
        save_alpha_post(message.guild.id, message.channel.id, posted.id,
                        poster.id, content, handle)
        # Re-read score after save (posts incremented)
        score, posts = get_poster_score(message.guild.id, poster.id)

        # Build final embed
        embed = build_alpha_embed(
            poster=poster,
            content=content,
            handle=handle,
            profile=profile,
            research_text=research_text,
            score=score,
            posts=posts,
        )

        # Attach project art if available
        art_files: List[discord.File] = []
        if handle and profile:
            try:
                embed, art_files = await hydrate_wl_embed_art(embed, profile, handle, files=art_files)
            except Exception as e:
                logger.debug("[AlphaPing] Art hydrate: %s", e)

        try:
            if art_files:
                await posted.edit(embed=embed, attachments=art_files, view=AlphaVoteView())
            else:
                await posted.edit(embed=embed, view=AlphaVoteView())
        except Exception as e:
            logger.warning("[AlphaPing] Edit failed: %s", e)

    # ── !velcor3 alphaping — ping degen role (alpha channel only) ─────────────

    @commands.command(name="alphaping", help="Ping the degen role (alpha channel only).")
    async def ping_degen(self, ctx: commands.Context):
        if ctx.channel.id != ALPHA_CHANNEL_ID:
            return

        poster_role = ctx.guild.get_role(ALPHA_POSTER_ROLE_ID)
        if poster_role not in ctx.author.roles:
            await ctx.message.delete(delay=3)
            await ctx.send(
                f"You need the **{poster_role.name if poster_role else 'required'}** role.",
                delete_after=6,
            )
            return

        target_role = ctx.guild.get_role(ALPHA_TARGET_ROLE_ID)
        if target_role is None:
            await ctx.send("Target role not found.", delete_after=6)
            return

        await ctx.message.delete()
        await ctx.send(
            f"{target_role.mention} — alpha incoming from {ctx.author.mention}!"
        )

    # ── !velcor3 alphaleaderboard ─────────────────────────────────────────────

    @commands.command(name="alphaleaderboard", aliases=["alphalb"])
    async def alpha_leaderboard(self, ctx: commands.Context):
        rows = get_leaderboard(ctx.guild.id)
        if not rows:
            await ctx.send("No alpha votes recorded yet.")
            return
        medals = ["🥇", "🥈", "🥉"]
        lines = []
        for i, row in enumerate(rows):
            member = ctx.guild.get_member(row["user_id"])
            name = member.display_name if member else f"<@{row['user_id']}>"
            prefix = medals[i] if i < 3 else f"`#{i+1}`"
            tier = _score_tier(row["score"])
            lines.append(
                f"{prefix} **{name}** {tier} — `{row['score']:+d}` pts · {row['posts']} post(s)"
            )
        embed = discord.Embed(
            title="🏆 Alpha Leaderboard",
            description="\n".join(lines),
            color=0x00C4FF,
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_footer(text="+1 Good Alpha · −1 Not Alpha")
        await ctx.send(embed=embed)

    # ── !velcor3 alphascore [@member] ────────────────────────────────────────

    @commands.command(name="alphascore")
    async def alpha_score(self, ctx: commands.Context, member: discord.Member = None):
        target = member or ctx.author
        score, posts = get_poster_score(ctx.guild.id, target.id)
        embed = discord.Embed(color=0x00C4FF)
        embed.set_author(name=target.display_name, icon_url=target.display_avatar.url)
        embed.add_field(name="Alpha Score", value=f"`{score:+d}`", inline=True)
        embed.add_field(name="Posts", value=f"`{posts}`", inline=True)
        embed.add_field(name="Tier", value=_score_tier(score), inline=True)
        await ctx.send(embed=embed)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(AlphaPingCog(bot))
