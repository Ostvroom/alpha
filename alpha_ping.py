# Explicit alpha + meme-coin alerts, community voting, and caller score tracking.
from __future__ import annotations

import logging
import os
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from typing import Optional, Tuple

import aiohttp
import discord
from discord.ext import commands

from app_paths import DATA_DIR
from brand_assets import brand_logo_embed_icon
from trackers.daily_mints_client import twitter_handle_from_url

logger = logging.getLogger(__name__)
_BRAND = "VELCOR3"
_URL_RE = re.compile(r"^https?://[^\s]+$", re.IGNORECASE)
_EVM_CA_RE = re.compile(r"0x[a-fA-F0-9]{40}")

ALPHA_CHANNEL_ID = int(os.getenv("ALPHA_CHANNEL_ID", "1506376120252239872"))
ALPHA_TARGET_ROLE_ID = int(os.getenv("ALPHA_TARGET_ROLE_ID", "1505260948707999774"))
MEME_CHANNEL_ID = int(os.getenv("MEME_CHANNEL_ID", "1248943502084018186"))
# Optional — leave unset (0) to post meme calls without pinging a role.
MEME_TARGET_ROLE_ID = int(os.getenv("MEME_TARGET_ROLE_ID", "0"))
MEME_DEX_CHAIN = (os.getenv("MEME_DEX_CHAIN", "base") or "base").strip().lower()
SCORE_COOK = int(os.getenv("ALPHA_SCORE_COOK", os.getenv("ALPHA_SCORE_GOOD", "1")))
SCORE_SKIP = int(os.getenv("ALPHA_SCORE_SKIP", os.getenv("ALPHA_SCORE_NOT", "-1")))
DB_PATH = DATA_DIR / "user_stats.db"

# Per-alert-type presentation. Both kinds share one voting/scoring engine
# (same DB tables, `kind` column) but keep separate leaderboards/scores.
KIND_META = {
    "alpha": {
        "label": "Alpha Call",
        "verb": "Alpha",
        "color": 0x00C4FF,
        "emoji": "🧠",
        "profile_title": "Alpha Profile",
    },
    "meme": {
        "label": "Meme Call",
        "verb": "Meme",
        "color": 0xF5A623,
        "emoji": "🚀",
        "profile_title": "Meme Profile",
    },
}


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _column_names(conn: sqlite3.Connection, table: str) -> set:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def _migrate_alpha_posts_kind(conn: sqlite3.Connection) -> None:
    cols = _column_names(conn, "alpha_posts")
    if cols and "kind" not in cols:
        conn.execute("ALTER TABLE alpha_posts ADD COLUMN kind TEXT NOT NULL DEFAULT 'alpha'")


def _migrate_alpha_scores_kind(conn: sqlite3.Connection) -> None:
    # alpha_scores' PRIMARY KEY must grow to (guild_id, user_id, kind) so alpha
    # and meme reputations are tracked separately. SQLite can't ALTER a PK, so
    # older installs get rebuilt: rename -> recreate -> copy (kind='alpha') -> drop.
    cols = _column_names(conn, "alpha_scores")
    if not cols or "kind" in cols:
        return
    conn.execute("ALTER TABLE alpha_scores RENAME TO alpha_scores_legacy")
    conn.execute(
        "CREATE TABLE alpha_scores (guild_id INTEGER NOT NULL, "
        "user_id INTEGER NOT NULL, kind TEXT NOT NULL DEFAULT 'alpha', "
        "score INTEGER DEFAULT 0, posts INTEGER DEFAULT 0, "
        "PRIMARY KEY (guild_id, user_id, kind))"
    )
    conn.execute(
        "INSERT INTO alpha_scores (guild_id, user_id, kind, score, posts) "
        "SELECT guild_id, user_id, 'alpha', score, posts FROM alpha_scores_legacy"
    )
    conn.execute("DROP TABLE alpha_scores_legacy")


def init_alpha_tables() -> None:
    with closing(_db()) as conn, conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS alpha_posts (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "guild_id INTEGER NOT NULL, channel_id INTEGER NOT NULL, "
            "message_id INTEGER NOT NULL UNIQUE, poster_id INTEGER NOT NULL, "
            "content TEXT NOT NULL, handle TEXT, kind TEXT NOT NULL DEFAULT 'alpha', "
            "posted_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS alpha_votes (post_id INTEGER NOT NULL, "
            "voter_id INTEGER NOT NULL, vote TEXT NOT NULL, voted_at TEXT NOT NULL, "
            "PRIMARY KEY (post_id, voter_id))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS alpha_scores (guild_id INTEGER NOT NULL, "
            "user_id INTEGER NOT NULL, kind TEXT NOT NULL DEFAULT 'alpha', "
            "score INTEGER DEFAULT 0, posts INTEGER DEFAULT 0, "
            "PRIMARY KEY (guild_id, user_id, kind))"
        )
        try:
            _migrate_alpha_posts_kind(conn)
            _migrate_alpha_scores_kind(conn)
        except Exception:
            logger.exception("[AlphaPing] Schema migration for kind column failed")


def save_alpha_post(guild_id: int, channel_id: int, message_id: int,
                    poster_id: int, content: str, kind: str = "alpha") -> None:
    with closing(_db()) as conn, conn:
        cursor = conn.execute(
            "INSERT OR IGNORE INTO alpha_posts "
            "(guild_id, channel_id, message_id, poster_id, content, handle, kind, posted_at) "
            "VALUES (?, ?, ?, ?, ?, NULL, ?, ?)",
            (guild_id, channel_id, message_id, poster_id, content[:4000], kind,
             datetime.now(timezone.utc).isoformat()),
        )
        if cursor.rowcount:
            conn.execute(
                "INSERT INTO alpha_scores (guild_id, user_id, kind, score, posts) "
                "VALUES (?, ?, ?, 0, 1) ON CONFLICT(guild_id, user_id, kind) "
                "DO UPDATE SET posts = posts + 1",
                (guild_id, poster_id, kind),
            )


def get_post_by_message(message_id: int) -> Optional[sqlite3.Row]:
    with closing(_db()) as conn, conn:
        return conn.execute(
            "SELECT * FROM alpha_posts WHERE message_id = ?", (message_id,)
        ).fetchone()


def get_poster_score(guild_id: int, user_id: int, kind: str = "alpha") -> Tuple[int, int]:
    with closing(_db()) as conn, conn:
        row = conn.execute(
            "SELECT score, posts FROM alpha_scores WHERE guild_id = ? AND user_id = ? AND kind = ?",
            (guild_id, user_id, kind),
        ).fetchone()
    return (row["score"], row["posts"]) if row else (0, 0)


def _vote_delta(vote: str) -> int:
    # Keep votes from the previous Good/Not Alpha template compatible.
    return SCORE_COOK if vote in {"cook", "good"} else SCORE_SKIP


def cast_vote(post_id: int, voter_id: int, vote: str,
              poster_id: int, guild_id: int, kind: str = "alpha") -> str:
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
            "INSERT INTO alpha_scores (guild_id, user_id, kind, score, posts) "
            "VALUES (?, ?, ?, ?, 0) ON CONFLICT(guild_id, user_id, kind) "
            "DO UPDATE SET score = score + ?",
            (guild_id, poster_id, kind, net, net),
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


def get_caller_stats(guild_id: int, user_id: int, kind: str = "alpha") -> dict:
    score, calls = get_poster_score(guild_id, user_id, kind)
    with closing(_db()) as conn, conn:
        totals = conn.execute(
            "SELECT "
            "SUM(CASE WHEN v.vote IN ('cook', 'good') THEN 1 ELSE 0 END) AS cook, "
            "SUM(CASE WHEN v.vote IN ('skip', 'not') THEN 1 ELSE 0 END) AS skip "
            "FROM alpha_posts p LEFT JOIN alpha_votes v ON v.post_id = p.id "
            "WHERE p.guild_id = ? AND p.poster_id = ? AND p.kind = ?",
            (guild_id, user_id, kind),
        ).fetchone()
        best = conn.execute(
            "SELECT p.channel_id, p.message_id, p.content, "
            "SUM(CASE WHEN v.vote IN ('cook', 'good') THEN ? "
            "WHEN v.vote IN ('skip', 'not') THEN ? ELSE 0 END) AS call_score, "
            "SUM(CASE WHEN v.vote IN ('cook', 'good') THEN 1 ELSE 0 END) AS cook, "
            "SUM(CASE WHEN v.vote IN ('skip', 'not') THEN 1 ELSE 0 END) AS skip "
            "FROM alpha_posts p LEFT JOIN alpha_votes v ON v.post_id = p.id "
            "WHERE p.guild_id = ? AND p.poster_id = ? AND p.kind = ? GROUP BY p.id "
            "ORDER BY call_score DESC, cook DESC, p.posted_at DESC LIMIT 1",
            (SCORE_COOK, SCORE_SKIP, guild_id, user_id, kind),
        ).fetchone()
        rank = conn.execute(
            "SELECT 1 + COUNT(*) AS rank FROM alpha_scores "
            "WHERE guild_id = ? AND kind = ? AND (score > ? OR (score = ? AND posts > ?))",
            (guild_id, kind, score, score, calls),
        ).fetchone()["rank"]

    cook = int(totals["cook"] or 0)
    skip = int(totals["skip"] or 0)
    total_votes = cook + skip
    return {
        "score": score,
        "calls": calls,
        "rank": rank if calls else None,
        "cook": cook,
        "skip": skip,
        "total_votes": total_votes,
        "approval": round(cook * 100 / total_votes) if total_votes else None,
        "average": score / calls if calls else 0.0,
        "best": dict(best) if best else None,
    }


def get_leaderboard(guild_id: int, kind: str = "alpha", limit: int = 10) -> list:
    with closing(_db()) as conn, conn:
        rows = conn.execute(
            "SELECT user_id, score, posts FROM alpha_scores "
            "WHERE guild_id = ? AND kind = ? AND posts > 0 "
            "ORDER BY score DESC, posts DESC LIMIT ?",
            (guild_id, kind, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def _caller_tier(score: int) -> str:
    if score >= 50:
        return "🏆 Alpha Legend"
    if score >= 20:
        return "💎 Elite Caller"
    if score >= 10:
        return "🔥 Trusted Caller"
    if score >= 3:
        return "📈 Rising Caller"
    if score >= 0:
        return "🌱 New Caller"
    return "⚠️ Unproven Caller"


async def resolve_project_pfp(link: str) -> Optional[str]:
    """Best-effort project avatar for the shared link (X/Twitter profiles only).

    Uses unavatar.io — a public, unauthenticated avatar proxy — so this NEVER
    touches the Twikit session pool / residential proxies reserved for the
    brain scan and TweetWatcher. Returns None (caller falls back to their own
    Discord avatar) if the link isn't an X profile or has no resolvable pfp.
    """
    handle = twitter_handle_from_url(link)
    if not handle:
        return None
    url = f"https://unavatar.io/twitter/{handle}"
    check_url = f"{url}?fallback=false"
    try:
        timeout = aiohttp.ClientTimeout(total=6)
        async with aiohttp.ClientSession(timeout=timeout) as http:
            async with http.head(check_url, allow_redirects=True) as resp:
                if resp.status == 200:
                    return url
    except Exception:
        pass
    return None


def _vote_bar(pct: int) -> str:
    filled = max(0, min(10, round(pct / 10)))
    return "█" * filled + "░" * (10 - filled)


def _sentiment_field(cook_votes: int, skip_votes: int, *, compact: bool = False) -> str:
    total = cook_votes + skip_votes
    cook_pct = round(cook_votes * 100 / total) if total else 0
    skip_pct = round(skip_votes * 100 / total) if total else 0
    if compact:
        return f"**🍳 Cook** `{cook_votes}` ({cook_pct}%)     **⏭️ Skip** `{skip_votes}` ({skip_pct}%)"
    return (
        f"**🍳 Cook**  `{cook_votes}`  ({cook_pct}%)\n{_vote_bar(cook_pct)}\n\n"
        f"**⏭️ Skip**  `{skip_votes}`  ({skip_pct}%)\n{_vote_bar(skip_pct)}"
    )


def build_alert_embed(kind: str, caller: discord.Member, link: str, text: str,
                      score: int, cook_votes: int = 0, skip_votes: int = 0,
                      thumbnail_url: Optional[str] = None,
                      image_url: Optional[str] = None,
                      contract_address: Optional[str] = None) -> discord.Embed:
    meta = KIND_META.get(kind, KIND_META["alpha"])
    if kind == "meme":
        title = None
        description = text[:1800]
    else:
        title = f"{meta['emoji']} {meta['label']}"
        description = (
            f"{text[:1500]}\n\n"
            "```\nLive community voting panel\n```\n"
            "━━━━━━━━━━━━━━━━━━"
        )
    embed = discord.Embed(
        title=title,
        description=description[:4000],
        color=meta["color"],
        timestamp=datetime.now(timezone.utc),
    )
    icon = brand_logo_embed_icon()
    author_name = f"{_BRAND} MEME COIN CALL" if kind == "meme" else f"{_BRAND} {meta['verb'].upper()} CALL"
    if icon:
        embed.set_author(name=author_name, icon_url=icon)
    else:
        embed.set_author(name=author_name)
    if kind == "meme":
        embed.add_field(name="CA", value=f"`{contract_address or link}`", inline=False)
        embed.add_field(
            name="Caller",
            value=f"{caller.mention}  ·  Score `{score:+d}`  ·  {_caller_tier(score)}",
            inline=False,
        )
    else:
        # Link is shown as a plain, fully-visible URL (not masked behind text).
        embed.add_field(name="🔗 Project", value=link, inline=True)
        embed.add_field(name="👤 Caller", value=caller.mention, inline=True)
        embed.add_field(
            name="📊 Score",
            value=f"`{score:+d}`\n{_caller_tier(score)}",
            inline=True,
        )
    embed.add_field(
        name="🗳️ Community Sentiment",
        value=_sentiment_field(cook_votes, skip_votes, compact=(kind == "meme")),
        inline=False,
    )
    if kind != "meme":
        embed.add_field(
            name="📌 Status",
            value="`LIVE VOTING` • `COMMUNITY SIGNAL`",
            inline=False,
        )
    embed.set_thumbnail(url=thumbnail_url or caller.display_avatar.url)
    if image_url:
        embed.set_image(url=image_url)
    embed.set_footer(text=f"{_BRAND} • Cook {SCORE_COOK:+d} · Skip {SCORE_SKIP:+d} · Vote below")
    return embed


class AlertVoteView(discord.ui.View):
    """Single persistent view shared by alpha + meme calls. The alert `kind`
    is looked up from the post row (by message id) rather than encoded in the
    custom_id, so one registered view instance routes clicks for both alert
    types. cook_votes/skip_votes/link only shape THIS message's rendered
    button labels + Link button at send time — Discord preserves each sent
    message's components independently of the bot process, so a fresh
    zero-state view registered at boot (see MarketAlertsCog.__init__) is all
    that's needed for interaction routing to keep working across restarts.
    """

    def __init__(self, cook_votes: int = 0, skip_votes: int = 0,
                link: Optional[str] = None) -> None:
        super().__init__(timeout=None)
        self.cook_button = discord.ui.Button(
            label=f"Cook {cook_votes}", emoji="🍳",
            style=discord.ButtonStyle.success, custom_id="alpha_vote_cook",
        )
        self.cook_button.callback = self._on_cook
        self.skip_button = discord.ui.Button(
            label=f"Skip {skip_votes}", emoji="⏭️",
            style=discord.ButtonStyle.secondary, custom_id="alpha_vote_skip",
        )
        self.skip_button.callback = self._on_skip
        self.add_item(self.cook_button)
        self.add_item(self.skip_button)
        if link:
            # Link-style buttons open the URL client-side; Discord never
            # routes them through the bot, so no custom_id/persistence needed.
            self.add_item(discord.ui.Button(
                label="Project", emoji="🔗",
                style=discord.ButtonStyle.link, url=link,
            ))

    async def _on_cook(self, interaction: discord.Interaction) -> None:
        await self._handle(interaction, "cook")

    async def _on_skip(self, interaction: discord.Interaction) -> None:
        await self._handle(interaction, "skip")

    async def _handle(self, interaction: discord.Interaction, vote: str) -> None:
        post = get_post_by_message(interaction.message.id)
        if not post:
            await interaction.response.send_message(
                "This call is no longer active.", ephemeral=True
            )
            return
        if interaction.user.id == post["poster_id"]:
            await interaction.response.send_message(
                "You cannot vote on your own call.", ephemeral=True
            )
            return

        kind = post["kind"] if "kind" in post.keys() else "alpha"
        result = cast_vote(
            post["id"], interaction.user.id, vote,
            post["poster_id"], post["guild_id"], kind,
        )
        label = "Cook" if vote == "cook" else "Skip"
        if result == "already_same":
            await interaction.response.send_message(
                f"You already voted **{label}**.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)
        cook_votes, skip_votes = get_vote_counts(post["id"])
        score, _ = get_poster_score(post["guild_id"], post["poster_id"], kind)
        self.cook_button.label = f"Cook {cook_votes}"
        self.skip_button.label = f"Skip {skip_votes}"
        if interaction.message.embeds:
            embed = interaction.message.embeds[0].copy()
            for index, field in enumerate(embed.fields):
                if field.name == "📊 Score":
                    embed.set_field_at(
                        index, name="📊 Score",
                        value=f"`{score:+d}`\n{_caller_tier(score)}", inline=True,
                    )
                elif field.name == "Caller":
                    embed.set_field_at(
                        index, name="Caller",
                        value=f"<@{post['poster_id']}>  ·  Score `{score:+d}`  ·  {_caller_tier(score)}",
                        inline=False,
                    )
                elif field.name == "🗳️ Community Sentiment":
                    embed.set_field_at(
                        index, name="🗳️ Community Sentiment",
                        value=_sentiment_field(cook_votes, skip_votes, compact=(kind == "meme")),
                        inline=False,
                    )
            await interaction.message.edit(embed=embed, view=self)

        action = "changed to" if result == "changed" else "recorded as"
        await interaction.followup.send(
            f"Your vote was {action} **{label}**.", ephemeral=True
        )


def _parse_link_and_text(args: Optional[str]) -> Tuple[str, str]:
    """The link can appear before or after the alert text."""
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
    return link, text


def _dexscreener_url_from_ca(contract_address: str, chain: str = MEME_DEX_CHAIN) -> str:
    return f"https://dexscreener.com/{chain}/{contract_address}"


def _parse_ca_and_text(args: Optional[str]) -> Tuple[str, str]:
    """The contract address can appear before or after the alert text."""
    parts = (args or "").split()
    ca_index = None
    ca = ""
    for index, part in enumerate(parts):
        match = _EVM_CA_RE.search(part.strip("<>"))
        if match:
            ca_index = index
            ca = match.group(0)
            break
    text = " ".join(
        part for index, part in enumerate(parts) if index != ca_index
    ).strip()
    return ca, text


class MarketAlertsCog(commands.Cog, name="MarketAlerts"):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        init_alpha_tables()
        bot.add_view(AlertVoteView())

    async def _post_alert(self, ctx: commands.Context, kind: str,
                          channel_id: int, role_id: int,
                          args: Optional[str], usage_name: str) -> None:
        if not ctx.guild:
            return
        if ctx.channel.id != channel_id:
            await ctx.send(f"Use this command in the {usage_name} channel.", delete_after=8)
            return

        contract_address = None
        if kind == "meme":
            contract_address, text = _parse_ca_and_text(args)
            link = _dexscreener_url_from_ca(contract_address) if contract_address else ""
            usage_detail = "<contract-address> <description>"
            required_detail = "Both the contract address and description are required."
        else:
            link, text = _parse_link_and_text(args)
            usage_detail = "<link> <text>"
            required_detail = "Both the link and text are required."
        if not link or not text:
            await ctx.send(
                f"Usage: `!velcor3 {usage_name} {usage_detail}` or "
                f"`!velcor3 {usage_name} <description> {usage_detail.split()[0]}`\n"
                f"{required_detail}",
                delete_after=10,
            )
            return

        target_role = ctx.guild.get_role(role_id) if role_id else None
        if role_id and target_role is None:
            await ctx.send("The alert role is not configured.", delete_after=8)
            return

        score, _ = get_poster_score(ctx.guild.id, ctx.author.id, kind)
        thumbnail_url = None if kind == "meme" else await resolve_project_pfp(link)
        image_url = next(
            (a.url for a in ctx.message.attachments
             if (a.content_type or "").startswith("image/")),
            None,
        )
        embed = build_alert_embed(
            kind, ctx.author, link, text, score,
            thumbnail_url=thumbnail_url, image_url=image_url,
            contract_address=contract_address,
        )
        try:
            await ctx.message.delete()
        except (discord.Forbidden, discord.NotFound):
            pass

        try:
            posted = await ctx.send(
                target_role.mention if target_role else None,
                embed=embed,
                view=AlertVoteView(cook_votes=0, skip_votes=0, link=link),
                allowed_mentions=discord.AllowedMentions(
                    everyone=False, users=False, roles=True, replied_user=False
                ),
            )
            save_alpha_post(
                ctx.guild.id, ctx.channel.id, posted.id, ctx.author.id,
                f"{contract_address or link}\n{text}", kind,
            )
        except Exception:
            logger.exception(f"[AlphaPing] Failed to create {kind} alert")
            await ctx.send("Could not create the alert.", delete_after=8)

    @commands.command(name="alpha")
    async def alpha(self, ctx: commands.Context,
                    *, args: Optional[str] = None) -> None:
        await self._post_alert(ctx, "alpha", ALPHA_CHANNEL_ID, ALPHA_TARGET_ROLE_ID,
                               args, "alpha")

    @commands.command(name="meme")
    async def meme(self, ctx: commands.Context,
                   *, args: Optional[str] = None) -> None:
        await self._post_alert(ctx, "meme", MEME_CHANNEL_ID, MEME_TARGET_ROLE_ID,
                               args, "meme")

    async def _send_caller_profile(self, ctx: commands.Context, kind: str,
                                   member: Optional[discord.Member]) -> None:
        if not ctx.guild:
            return
        meta = KIND_META.get(kind, KIND_META["alpha"])
        target = member or ctx.author
        stats = get_caller_stats(ctx.guild.id, target.id, kind)
        score = stats["score"]
        color = 0x57F287 if score >= 10 else (0xED4245 if score < 0 else meta["color"])
        embed = discord.Embed(
            title=f"{target.display_name}'s {meta['profile_title']}",
            description=f"{target.mention}\n**{_caller_tier(score)}**",
            color=color,
            timestamp=datetime.now(timezone.utc),
        )
        icon = brand_logo_embed_icon()
        if icon:
            embed.set_author(name=f"{_BRAND} · Caller Analytics", icon_url=icon)
        embed.set_thumbnail(url=target.display_avatar.url)

        rank = f"#{stats['rank']}" if stats["rank"] else "Unranked"
        approval = (
            f"{stats['approval']}%" if stats["approval"] is not None else "No votes"
        )
        embed.add_field(name="Reputation", value=f"**{score:+d} pts**", inline=True)
        embed.add_field(name=f"{meta['verb']} Calls", value=f"**{stats['calls']}**", inline=True)
        embed.add_field(name="Server Rank", value=f"**{rank}**", inline=True)
        embed.add_field(name="🍳 Cook", value=f"**{stats['cook']}**", inline=True)
        embed.add_field(name="⏭️ Skip", value=f"**{stats['skip']}**", inline=True)
        embed.add_field(name="Approval", value=f"**{approval}**", inline=True)
        embed.add_field(
            name="Performance",
            value=(
                f"Average per call: **{stats['average']:+.1f} pts**\n"
                f"Community votes: **{stats['total_votes']}**"
            ),
            inline=False,
        )

        best = stats["best"]
        if best:
            jump_url = (
                f"https://discord.com/channels/{ctx.guild.id}/"
                f"{best['channel_id']}/{best['message_id']}"
            )
            lines = (best["content"] or "").splitlines()
            summary = " ".join(lines[1:] if len(lines) > 1 else lines).strip()
            best_score = int(best["call_score"] or 0)
            best_value = (
                f"[Open best call]({jump_url}) · **{best_score:+d} pts**\n"
                f"🍳 {int(best['cook'] or 0)}  ·  ⏭️ {int(best['skip'] or 0)}"
            )
            if summary:
                best_value += f"\n> {summary[:220]}"
            embed.add_field(name=f"🏅 Best {meta['verb']} Call", value=best_value, inline=False)
        else:
            embed.add_field(
                name=f"🏅 Best {meta['verb']} Call",
                value=f"No {meta['verb'].lower()} calls yet.",
                inline=False,
            )

        embed.set_footer(
            text=f"Cook {SCORE_COOK:+d} · Skip {SCORE_SKIP:+d} · Live statistics"
        )
        await ctx.send(embed=embed)

    @commands.command(name="alphascore")
    async def alpha_score(
        self, ctx: commands.Context,
        member: Optional[discord.Member] = None,
    ) -> None:
        await self._send_caller_profile(ctx, "alpha", member)

    @commands.command(name="memescore")
    async def meme_score(
        self, ctx: commands.Context,
        member: Optional[discord.Member] = None,
    ) -> None:
        await self._send_caller_profile(ctx, "meme", member)

    async def _send_leaderboard(self, ctx: commands.Context, kind: str,
                                limit: int) -> None:
        if not ctx.guild:
            return
        meta = KIND_META.get(kind, KIND_META["alpha"])
        limit = max(1, min(20, limit))
        rows = get_leaderboard(ctx.guild.id, kind, limit)

        embed = discord.Embed(
            title=f"🏆 {meta['verb']} Leaderboard",
            color=meta["color"],
            timestamp=datetime.now(timezone.utc),
        )
        icon = brand_logo_embed_icon()
        if icon:
            embed.set_author(name=f"{_BRAND} · Top Callers", icon_url=icon)

        if not rows:
            embed.description = f"No {meta['verb'].lower()} calls tracked yet."
        else:
            medals = {0: "🥇", 1: "🥈", 2: "🥉"}
            lines = []
            for index, row in enumerate(rows):
                member = ctx.guild.get_member(row["user_id"])
                name = member.mention if member else f"<@{row['user_id']}>"
                rank_label = medals.get(index, f"`#{index + 1}`")
                lines.append(
                    f"{rank_label} {name} — **{row['score']:+d} pts** "
                    f"({row['posts']} call{'s' if row['posts'] != 1 else ''})"
                )
            embed.description = "\n".join(lines)

        embed.set_footer(text=f"Cook {SCORE_COOK:+d} · Skip {SCORE_SKIP:+d} · Top {limit}")
        await ctx.send(embed=embed)

    @commands.command(name="alphaleaderboard", aliases=["alphalb"])
    async def alpha_leaderboard(self, ctx: commands.Context, limit: int = 10) -> None:
        await self._send_leaderboard(ctx, "alpha", limit)

    @commands.command(name="memeleaderboard", aliases=["memelb"])
    async def meme_leaderboard(self, ctx: commands.Context, limit: int = 10) -> None:
        await self._send_leaderboard(ctx, "meme", limit)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(MarketAlertsCog(bot))
