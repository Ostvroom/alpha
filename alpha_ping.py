"""
alpha_ping.py — Alpha channel auto-voting + role ping.

Two features:
  1. AUTO-VOTE  — any (non-bot) message in ALPHA_CHANNEL_ID gets ✅/❌ voting buttons
                  attached automatically. Votes build a score for the poster.
  2. ROLE PING  — `!velcor3 ping degen` (only usable inside ALPHA_CHANNEL_ID)
                  pings ALPHA_TARGET_ROLE_ID. Only members with ALPHA_POSTER_ROLE_ID
                  can trigger it.

Config:
  ALPHA_CHANNEL_ID      — channel where alpha is posted        (1517874398537711706)
  ALPHA_POSTER_ROLE_ID  — required role to use !velcor3 ping   (1506380353877577738)
  ALPHA_TARGET_ROLE_ID  — role that gets pinged                (1505260948707999774)
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional

import discord
from discord.ext import commands

from app_paths import DATA_DIR

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
    c = conn.cursor()
    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS alpha_posts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id    INTEGER NOT NULL,
            channel_id  INTEGER NOT NULL,
            message_id  INTEGER NOT NULL UNIQUE,
            poster_id   INTEGER NOT NULL,
            content     TEXT    NOT NULL,
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
                    poster_id: int, content: str) -> int:
    conn = _db()
    c = conn.cursor()
    c.execute(
        """
        INSERT OR IGNORE INTO alpha_posts
            (guild_id, channel_id, message_id, poster_id, content, posted_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (guild_id, channel_id, message_id, poster_id, content,
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


def get_vote_counts(post_id: int) -> tuple[int, int]:
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


# ── Voting View ───────────────────────────────────────────────────────────────

class AlphaVoteView(discord.ui.View):
    """Persistent voting buttons — survive bot restarts."""

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
            await interaction.response.send_message("Post not found in DB.", ephemeral=True)
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
        vote_text = f"✅ {good}  ·  ❌ {not_}"

        # Update embed vote line
        if interaction.message.embeds:
            embed = interaction.message.embeds[0].copy()
            for i, f in enumerate(embed.fields):
                if f.name == "Votes":
                    embed.set_field_at(i, name="Votes", value=vote_text, inline=True)
                    break
            else:
                embed.add_field(name="Votes", value=vote_text, inline=True)
            await interaction.message.edit(embed=embed, view=self)

        verb = "changed to" if result == "changed" else "cast as"
        label = "✅ Good Alpha" if vote == "good" else "❌ Not Alpha"
        await interaction.response.send_message(
            f"Vote {verb} **{label}** — thanks!", ephemeral=True
        )


# ── Cog ──────────────────────────────────────────────────────────────────────

class AlphaPingCog(commands.Cog, name="AlphaPing"):

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        init_alpha_tables()
        bot.add_view(AlphaVoteView())

    # ── Auto-attach voting buttons to every message in the alpha channel ──────

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        # Only act in the alpha channel, ignore bots
        if message.channel.id != ALPHA_CHANNEL_ID:
            return
        if message.author.bot:
            return

        # Build voting embed footer
        embed = discord.Embed(color=0x00C4FF)
        embed.set_author(
            name=message.author.display_name,
            icon_url=message.author.display_avatar.url,
        )
        embed.add_field(name="Votes", value="✅ 0  ·  ❌ 0", inline=True)
        embed.set_footer(text="Vote on this alpha ↑")

        view = AlphaVoteView()
        vote_msg = await message.channel.send(embed=embed, view=view, reference=message)

        save_alpha_post(
            message.guild.id,
            message.channel.id,
            vote_msg.id,        # voting message is what buttons live on
            message.author.id,
            message.content[:500],
        )

    # ── !velcor3 ping degen — only works inside ALPHA_CHANNEL_ID ─────────────

    @commands.command(name="ping", help="Ping the degen role (alpha channel only).")
    async def ping_degen(self, ctx: commands.Context):
        if ctx.channel.id != ALPHA_CHANNEL_ID:
            return  # silently ignore — not our channel

        poster_role = ctx.guild.get_role(ALPHA_POSTER_ROLE_ID)
        if poster_role not in ctx.author.roles:
            await ctx.message.delete(delay=3)
            await ctx.send(
                f"You need the **{poster_role.name if poster_role else 'required'}** role to ping.",
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
            lines.append(f"{prefix} **{name}** — `{row['score']:+d}` pts · {row['posts']} post(s)")
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
        conn = _db()
        row = conn.execute(
            "SELECT score, posts FROM alpha_scores WHERE guild_id = ? AND user_id = ?",
            (ctx.guild.id, target.id),
        ).fetchone()
        conn.close()
        if not row:
            await ctx.send(f"{target.display_name} hasn't posted any alpha yet.")
            return
        embed = discord.Embed(color=0x00C4FF)
        embed.set_author(name=target.display_name, icon_url=target.display_avatar.url)
        embed.add_field(name="Score", value=f"`{row['score']:+d}`", inline=True)
        embed.add_field(name="Posts", value=f"`{row['posts']}`", inline=True)
        await ctx.send(embed=embed)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(AlphaPingCog(bot))
