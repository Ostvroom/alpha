# Explicit alpha alerts, community voting, and caller score tracking.
from __future__ import annotations

import logging
import os
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from typing import Optional, Tuple

import discord
from discord.ext import commands

from app_paths import DATA_DIR
from brand_assets import brand_logo_embed_icon

logger = logging.getLogger(__name__)
_BRAND = "VELCOR3"
_URL_RE = re.compile(r"^https?://[^\s]+$", re.IGNORECASE)

ALPHA_CHANNEL_ID = int(os.getenv("ALPHA_CHANNEL_ID", "1517874398537711706"))
ALPHA_TARGET_ROLE_ID = int(os.getenv("ALPHA_TARGET_ROLE_ID", "1505260948707999774"))
SCORE_COOK = int(os.getenv("ALPHA_SCORE_COOK", os.getenv("ALPHA_SCORE_GOOD", "1")))
SCORE_SKIP = int(os.getenv("ALPHA_SCORE_SKIP", os.getenv("ALPHA_SCORE_NOT", "-1")))
DB_PATH = DATA_DIR / "user_stats.db"


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_alpha_tables() -> None:
    with closing(_db()) as conn, conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS alpha_posts (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "guild_id INTEGER NOT NULL, channel_id INTEGER NOT NULL, "
            "message_id INTEGER NOT NULL UNIQUE, poster_id INTEGER NOT NULL, "
            "content TEXT NOT NULL, handle TEXT, posted_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS alpha_votes (post_id INTEGER NOT NULL, "
            "voter_id INTEGER NOT NULL, vote TEXT NOT NULL, voted_at TEXT NOT NULL, "
            "PRIMARY KEY (post_id, voter_id))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS alpha_scores (guild_id INTEGER NOT NULL, "
            "user_id INTEGER NOT NULL, score INTEGER DEFAULT 0, posts INTEGER DEFAULT 0, "
            "PRIMARY KEY (guild_id, user_id))"
        )


def save_alpha_post(guild_id: int, channel_id: int, message_id: int,
                    poster_id: int, content: str) -> None:
    with closing(_db()) as conn, conn:
        cursor = conn.execute(
            "INSERT OR IGNORE INTO alpha_posts "
            "(guild_id, channel_id, message_id, poster_id, content, handle, posted_at) "
            "VALUES (?, ?, ?, ?, ?, NULL, ?)",
            (guild_id, channel_id, message_id, poster_id, content[:4000],
             datetime.now(timezone.utc).isoformat()),
        )
        if cursor.rowcount:
            conn.execute(
                "INSERT INTO alpha_scores (guild_id, user_id, score, posts) "
                "VALUES (?, ?, 0, 1) ON CONFLICT(guild_id, user_id) "
                "DO UPDATE SET posts = posts + 1",
                (guild_id, poster_id),
            )


def get_post_by_message(message_id: int) -> Optional[sqlite3.Row]:
    with closing(_db()) as conn, conn:
        return conn.execute(
            "SELECT * FROM alpha_posts WHERE message_id = ?", (message_id,)
        ).fetchone()


def get_poster_score(guild_id: int, user_id: int) -> Tuple[int, int]:
    with closing(_db()) as conn, conn:
        row = conn.execute(
            "SELECT score, posts FROM alpha_scores WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        ).fetchone()
    return (row["score"], row["posts"]) if row else (0, 0)


def _vote_delta(vote: str) -> int:
    # Keep votes from the previous Good/Not Alpha template compatible.
    return SCORE_COOK if vote in {"cook", "good"} else SCORE_SKIP


def cast_vote(post_id: int, voter_id: int, vote: str,
              poster_id: int, guild_id: int) -> str:
    with closing(_db()) as conn, conn:
        existing = conn.execute(
            "SELECT vote FROM alpha_votes WHERE post_id = ? AND voter_id = ?",
            (post_id, voter_id),
        ).fetchone()
        if existing and existing["vote"] == vote:
            return "already_same"
        if existing:
            net = _vote_delta(vote) - _vote_delta(existing["vote"])
            conn.execute(
                "UPDATE alpha_votes SET vote = ?, voted_at = ? "
                "WHERE post_id = ? AND voter_id = ?",
                (vote, datetime.now(timezone.utc).isoformat(), post_id, voter_id),
            )
            result = "changed"
        else:
            net = _vote_delta(vote)
            conn.execute(
                "INSERT INTO alpha_votes (post_id, voter_id, vote, voted_at) "
                "VALUES (?, ?, ?, ?)",
                (post_id, voter_id, vote, datetime.now(timezone.utc).isoformat()),
            )
            result = "new"
        conn.execute(
            "INSERT INTO alpha_scores (guild_id, user_id, score, posts) "
            "VALUES (?, ?, ?, 0) ON CONFLICT(guild_id, user_id) "
            "DO UPDATE SET score = score + ?",
            (guild_id, poster_id, net, net),
        )
        return result


def get_vote_counts(post_id: int) -> Tuple[int, int]:
    with closing(_db()) as conn, conn:
        rows = conn.execute(
            "SELECT vote, COUNT(*) AS n FROM alpha_votes "
            "WHERE post_id = ? GROUP BY vote", (post_id,)
        ).fetchall()
    counts = {row["vote"]: row["n"] for row in rows}
    return (
        counts.get("cook", 0) + counts.get("good", 0),
        counts.get("skip", 0) + counts.get("not", 0),
    )



def build_alpha_embed(caller: discord.Member, link: str, text: str, score: int,
                      cook_votes: int = 0, skip_votes: int = 0) -> discord.Embed:
    embed = discord.Embed(
        title="Alpha Call",
        description=text[:3500],
        color=0x00C4FF,
        timestamp=datetime.now(timezone.utc),
    )
    icon = brand_logo_embed_icon()
    if icon:
        embed.set_author(name=f"{_BRAND} Alpha", icon_url=icon)
    embed.add_field(name="Link", value=link, inline=False)
    embed.add_field(name="Caller", value=caller.mention, inline=True)
    embed.add_field(name="Caller Score", value=f"`{score:+d}`", inline=True)
    embed.add_field(
        name="Votes",
        value=f"🍳 Cook **{cook_votes}**  ·  ⏭️ Skip **{skip_votes}**",
        inline=False,
    )
    embed.set_thumbnail(url=caller.display_avatar.url)
    embed.set_footer(text=f"Cook {SCORE_COOK:+d} · Skip {SCORE_SKIP:+d}")
    return embed


class AlphaVoteView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(label="Cook", emoji="🍳",
                       style=discord.ButtonStyle.success,
                       custom_id="alpha_vote_cook")
    async def vote_cook(self, interaction: discord.Interaction,
                        _: discord.ui.Button) -> None:
        await self._handle(interaction, "cook")

    @discord.ui.button(label="Skip", emoji="⏭️",
                       style=discord.ButtonStyle.secondary,
                       custom_id="alpha_vote_skip")
    async def vote_skip(self, interaction: discord.Interaction,
                        _: discord.ui.Button) -> None:
        await self._handle(interaction, "skip")

    async def _handle(self, interaction: discord.Interaction, vote: str) -> None:
        post = get_post_by_message(interaction.message.id)
        if not post:
            await interaction.response.send_message(
                "This alpha call is no longer active.", ephemeral=True
            )
            return
        if interaction.user.id == post["poster_id"]:
            await interaction.response.send_message(
                "You cannot vote on your own alpha call.", ephemeral=True
            )
            return

        result = cast_vote(
            post["id"], interaction.user.id, vote,
            post["poster_id"], post["guild_id"],
        )
        label = "Cook" if vote == "cook" else "Skip"
        if result == "already_same":
            await interaction.response.send_message(
                f"You already voted **{label}**.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)
        cook_votes, skip_votes = get_vote_counts(post["id"])
        score, _ = get_poster_score(post["guild_id"], post["poster_id"])
        if interaction.message.embeds:
            embed = interaction.message.embeds[0].copy()
            for index, field in enumerate(embed.fields):
                if field.name == "Caller Score":
                    embed.set_field_at(
                        index, name="Caller Score",
                        value=f"`{score:+d}`", inline=True,
                    )
                elif field.name == "Votes":
                    embed.set_field_at(
                        index, name="Votes",
                        value=f"🍳 Cook **{cook_votes}**  ·  ⏭️ Skip **{skip_votes}**",
                        inline=False,
                    )
            await interaction.message.edit(embed=embed, view=self)

        action = "changed to" if result == "changed" else "recorded as"
        await interaction.followup.send(
            f"Your vote was {action} **{label}**.", ephemeral=True
        )



class AlphaPingCog(commands.Cog, name="AlphaPing"):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        init_alpha_tables()
        bot.add_view(AlphaVoteView())

    @commands.command(name="alpha")
    async def alpha(self, ctx: commands.Context,
                    *, args: Optional[str] = None) -> None:
        # The link can appear before or after the alert text.
        if not ctx.guild:
            return
        if ctx.channel.id != ALPHA_CHANNEL_ID:
            await ctx.send("Use this command in the alpha channel.", delete_after=8)
            return

        parts = (args or "").split()
        link_index = next(
            (index for index, part in enumerate(parts)
             if _URL_RE.fullmatch(part.strip("<>"))),
            None,
        )
        link = parts[link_index].strip("<>") if link_index is not None else ""
        text = " ".join(
            part for index, part in enumerate(parts) if index != link_index
        ).strip()
        if not link or not text:
            await ctx.send(
                "Usage: `!velcor3 alpha <link> <text>` or "
                "`!velcor3 alpha <text> <link>`\n"
                "Both the link and text are required.",
                delete_after=10,
            )
            return

        target_role = ctx.guild.get_role(ALPHA_TARGET_ROLE_ID)
        if target_role is None:
            await ctx.send("The degen alert role is not configured.", delete_after=8)
            return

        score, _ = get_poster_score(ctx.guild.id, ctx.author.id)
        embed = build_alpha_embed(ctx.author, link, text, score)
        try:
            await ctx.message.delete()
        except (discord.Forbidden, discord.NotFound):
            pass

        try:
            posted = await ctx.send(
                target_role.mention,
                embed=embed,
                view=AlphaVoteView(),
                allowed_mentions=discord.AllowedMentions(
                    everyone=False, users=False, roles=True, replied_user=False
                ),
            )
            save_alpha_post(
                ctx.guild.id, ctx.channel.id, posted.id, ctx.author.id,
                f"{link}\n{text}",
            )
        except Exception:
            logger.exception("[AlphaPing] Failed to create alpha alert")
            await ctx.send("Could not create the alpha alert.", delete_after=8)

    @commands.command(name="alphascore")
    async def alpha_score(
        self, ctx: commands.Context,
        member: Optional[discord.Member] = None,
    ) -> None:
        if not ctx.guild:
            return
        target = member or ctx.author
        score, posts = get_poster_score(ctx.guild.id, target.id)
        embed = discord.Embed(title="Alpha Caller", color=0x00C4FF)
        embed.set_author(
            name=target.display_name, icon_url=target.display_avatar.url
        )
        embed.add_field(name="Score", value=f"`{score:+d}`", inline=True)
        embed.add_field(name="Calls", value=f"`{posts}`", inline=True)
        await ctx.send(embed=embed)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(AlphaPingCog(bot))
