"""
#wl-request channel: holders post an X profile → bot replaces with intel embed + vote reactions.
"""
from __future__ import annotations

import logging
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import discord
from discord.ext import commands

import config
import database
from app_paths import DATA_DIR, ensure_dirs
from brand_assets import brand_logo_embed_icon, brand_name

logger = logging.getLogger(__name__)

ensure_dirs()
DB_PATH = DATA_DIR / "wl_requests.db"

X_PROFILE_RE = re.compile(
    r"(?:https?://)?(?:www\.)?(?:x\.com|twitter\.com)/(@?)([a-zA-Z0-9_]{1,15})",
    re.IGNORECASE,
)
_RESERVED_X_PATHS = frozenset(
    {
        "home",
        "i",
        "intent",
        "search",
        "hashtag",
        "explore",
        "settings",
        "compose",
        "messages",
        "share",
        "account",
    }
)

_VOTE_THUMBS = "👍"
_VOTE_FIRE = "🔥"


def _conn():
    return sqlite3.connect(str(DB_PATH))


def init_db() -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS wl_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            submitter_id INTEGER NOT NULL,
            handle TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_wl_req_handle ON wl_requests(guild_id, handle, created_at)"
    )
    conn.commit()
    conn.close()


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def parse_x_handle(text: str) -> Optional[str]:
    """Extract first X/Twitter profile handle from message text."""
    if not text:
        return None
    for m in X_PROFILE_RE.finditer(text):
        handle = (m.group(2) or "").lower()
        if handle in _RESERVED_X_PATHS:
            continue
        return handle
    raw = text.strip().lstrip("@").split()[0] if text.strip().startswith("@") else None
    if raw and re.match(r"^[a-zA-Z0-9_]{1,15}$", raw):
        return raw.lower()
    return None


def _strip_link_from_text(text: str, handle: str) -> str:
    """User's optional 'why collab' note after the link."""
    if not text or not handle:
        return ""
    t = text.strip()
    t = re.sub(
        rf"https?://(?:www\.)?(?:x\.com|twitter\.com)/@?{re.escape(handle)}[^\s]*",
        "",
        t,
        flags=re.I,
    )
    t = t.replace(f"@{handle}", "").strip()
    return t[:500] if len(t) > 15 else ""


def _fmt_followers(n: Optional[int]) -> str:
    if n is None:
        return "—"
    try:
        v = int(n)
    except (TypeError, ValueError):
        return "—"
    if v >= 1_000_000:
        return f"{v / 1_000_000:.1f}M".replace(".0M", "M")
    if v >= 1_000:
        return f"{v / 1_000:.1f}K".replace(".0K", "K")
    return f"{v:,}"


def _recent_duplicate(
    guild_id: int, channel_id: int, handle: str, hours: float
) -> Optional[Tuple[int, int]]:
    init_db()
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    since_s = since.strftime("%Y-%m-%d %H:%M:%S")
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT message_id, channel_id FROM wl_requests
        WHERE guild_id = ? AND channel_id = ? AND handle = ? AND created_at >= ?
        ORDER BY id DESC LIMIT 1
        """,
        (int(guild_id), int(channel_id), handle.lower(), since_s),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return int(row[0]), int(row[1])


def record_request(
    *,
    guild_id: int,
    channel_id: int,
    message_id: int,
    submitter_id: int,
    handle: str,
) -> None:
    init_db()
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO wl_requests (guild_id, channel_id, message_id, submitter_id, handle, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (int(guild_id), int(channel_id), int(message_id), int(submitter_id), handle.lower(), _now_iso()),
    )
    conn.commit()
    conn.close()


def _research_block(handle: str) -> Tuple[str, Dict[str, Any]]:
    """Velcor DB + HVA signals for this handle."""
    meta: Dict[str, Any] = {"in_db": False}
    row = database.get_project_by_handle(handle)
    if not row:
        return (
            "Not in Velcor research yet — first community request for this project.",
            meta,
        )
    meta["in_db"] = True
    tid, db_handle, name, desc, created_at, alerted_at, cat, summary, followers, smarts = row
    meta["twitter_id"] = tid
    meta["name"] = name
    sf = database.calculate_project_smart_followers_v2(str(tid))
    events = database.get_project_follow_events(str(tid), limit=15)
    hva_lines: List[str] = []
    seen = set()
    for ev in events[:8]:
        if not ev or len(ev) < 2:
            continue
        hva = str(ev[0] or "").lstrip("@")
        if not hva or hva.lower() in seen:
            continue
        seen.add(hva.lower())
        itype = str(ev[1] or "follow").replace("_", " ")
        hva_lines.append(f"• [@{hva}](https://x.com/{hva}) — {itype}")
        if len(hva_lines) >= 5:
            break

    lines = ["**In Velcor research** ✅"]
    if alerted_at:
        lines.append(f"**Alerted:** {alerted_at}")
    if cat:
        lines.append(f"**Category:** {cat}")
    if summary:
        lines.append(f"**AI summary:** {str(summary)[:200]}")
    lines.append(
        f"**Smart signal:** {sf.get('unique_hvas', 0)} HVAs · "
        f"{sf.get('hvas_24h', 0)} (24h) · {sf.get('hvas_7d', 0)} (7d)"
    )
    if smarts:
        lines.append(f"**Posted smarts:** {smarts}")
    if hva_lines:
        lines.append("**Recent HVA activity:**\n" + "\n".join(hva_lines))
    else:
        lines.append("_No HVA follow events logged yet._")
    return "\n".join(lines)[:1024], meta


async def fetch_live_profile(bot, handle: str) -> Dict[str, Any]:
    """X profile via bot twitter client when not fully in DB."""
    out: Dict[str, Any] = {"handle": handle}
    twitter = getattr(bot, "twitter", None)
    if not twitter:
        return out
    try:
        uid = await twitter.get_user_id(handle)
        if not uid:
            return out
        acc = await twitter.get_user_info(uid, handle=handle)
        if acc is None:
            return out
        out["followers_count"] = getattr(acc, "followers_count", None)
        out["description"] = (getattr(acc, "description", None) or "")[:300]
        out["name"] = getattr(acc, "name", None) or getattr(acc, "screen_name", handle)
        out["twitter_id"] = str(uid)
        created = getattr(acc, "created_at", None)
        if created:
            try:
                if created.tzinfo is None:
                    created = created.replace(tzinfo=timezone.utc)
                age_d = (datetime.now(timezone.utc) - created).days
                out["age_days"] = age_d
            except Exception:
                pass
    except Exception as e:
        logger.debug("wl_request profile fetch @%s: %s", handle, e)
    return out


def build_wl_request_embed(
    *,
    submitter: discord.abc.User,
    handle: str,
    note: str,
    profile: Dict[str, Any],
    research_text: str,
    duplicate_jump: Optional[Tuple[int, int, int]] = None,
) -> discord.Embed:
    brand = brand_name()
    name = profile.get("name") or handle
    url = f"https://x.com/{handle}"
    embed = discord.Embed(
        title=f"🗳️ WL collab request · @{handle}",
        description=f"**{name}**\n{url}",
        color=0x9B59B6,
        timestamp=datetime.now(timezone.utc),
    )
    icon = brand_logo_embed_icon()
    if icon:
        embed.set_author(name=f"{brand} · WL Request", icon_url=icon)

    embed.add_field(
        name="Submitted by",
        value=submitter.mention,
        inline=True,
    )
    embed.add_field(
        name="Followers",
        value=_fmt_followers(profile.get("followers_count")),
        inline=True,
    )
    age = profile.get("age_days")
    embed.add_field(
        name="Account age",
        value=f"{age}d" if age is not None else "—",
        inline=True,
    )

    bio = (profile.get("description") or "").strip()
    if bio:
        embed.add_field(name="Bio", value=bio[:1024], inline=False)

    embed.add_field(name="Velcor research", value=research_text[:1024], inline=False)

    if note:
        embed.add_field(name="Why collab (submitter)", value=note[:1024], inline=False)

    if duplicate_jump:
        gid, cid, mid = duplicate_jump
        embed.add_field(
            name="Note",
            value=(
                "This project was requested recently — "
                f"[see prior request](https://discord.com/channels/{gid}/{cid}/{mid})."
            ),
            inline=False,
        )

    vote_thumbs = getattr(config, "WL_REQUEST_VOTE_THUMBS", _VOTE_THUMBS) or _VOTE_THUMBS
    vote_fire = getattr(config, "WL_REQUEST_VOTE_FIRE", _VOTE_FIRE) or _VOTE_FIRE
    embed.add_field(
        name="Vote",
        value=f"{vote_thumbs} = I want this WL\n{vote_fire} = Strong yes",
        inline=False,
    )
    embed.set_footer(text=f"{brand} · Staff: review → #wl-giveaways")
    return embed


class WlRequestHandler(commands.Cog):
    """Auto-format #wl-request posts into intel cards."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        init_db()

    def _channel_ids(self) -> List[int]:
        raw = (getattr(config, "WL_REQUEST_CHANNEL_IDS", None) or []) or []
        if raw:
            return [int(x) for x in raw if int(x)]
        single = int(getattr(config, "WL_REQUEST_CHANNEL_ID", 0) or 0)
        return [single] if single else []

    def _holder_ok(self, member: discord.Member) -> bool:
        if not getattr(config, "WL_REQUEST_REQUIRE_HOLDER", True):
            return True
        rid = int(getattr(config, "HOLDER_VERIFIED_ROLE_ID", 0) or 0)
        if not rid:
            return True
        return any(r.id == rid for r in member.roles)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or not message.guild:
            return
        if not getattr(config, "ENABLE_WL_REQUEST_HANDLER", True):
            return
        if message.channel.id not in self._channel_ids():
            return
        if not isinstance(message.author, discord.Member):
            return
        if not self._holder_ok(message.author):
            try:
                await message.reply(
                    "Only **verified holders** can submit WL requests here.",
                    delete_after=12,
                )
            except Exception:
                pass
            return

        handle = parse_x_handle(message.content or "")
        if not handle:
            return

        ch = message.channel
        me = message.guild.me
        if not me:
            return

        dedupe_h = float(getattr(config, "WL_REQUEST_DEDUPE_HOURS", 168) or 168)
        dup = _recent_duplicate(message.guild.id, ch.id, handle, dedupe_h)
        note = _strip_link_from_text(message.content or "", handle)

        research_text, meta = _research_block(handle)
        profile: Dict[str, Any] = {"handle": handle, "description": ""}

        row = database.get_project_by_handle(handle)
        if row:
            profile["name"] = row[2]
            profile["description"] = (row[3] or "")[:300]
            profile["followers_count"] = row[8]
        else:
            live = await fetch_live_profile(self.bot, handle)
            profile.update(live)

        embed = build_wl_request_embed(
            submitter=message.author,
            handle=handle,
            note=note,
            profile=profile,
            research_text=research_text,
            duplicate_jump=(
                (message.guild.id, dup[1], dup[0]) if dup else None
            ),
        )

        try:
            posted = await ch.send(embed=embed)
        except discord.Forbidden:
            logger.warning("[WlRequest] Cannot send in channel %s", ch.id)
            return
        except Exception as e:
            logger.warning("[WlRequest] send failed: %s", e)
            return

        vote_thumbs = getattr(config, "WL_REQUEST_VOTE_THUMBS", _VOTE_THUMBS) or _VOTE_THUMBS
        vote_fire = getattr(config, "WL_REQUEST_VOTE_FIRE", _VOTE_FIRE) or _VOTE_FIRE
        for emoji in (vote_thumbs, vote_fire):
            try:
                await posted.add_reaction(emoji)
            except Exception:
                pass

        record_request(
            guild_id=message.guild.id,
            channel_id=ch.id,
            message_id=posted.id,
            submitter_id=message.author.id,
            handle=handle,
        )

        if me.guild_permissions.manage_messages:
            try:
                await message.delete()
            except discord.NotFound:
                pass
            except Exception as e:
                logger.debug("[WlRequest] delete user msg: %s", e)
        else:
            try:
                await message.reply(
                    "✅ Request logged above. (I need **Manage Messages** to remove link posts.)",
                    delete_after=15,
                )
            except Exception:
                pass

        logger.info(
            "[WlRequest] @%s from %s → msg %s (dup=%s)",
            handle,
            message.author,
            posted.id,
            bool(dup),
        )


async def setup(bot: commands.Bot):
    if getattr(config, "ENABLE_WL_REQUEST_HANDLER", True):
        await bot.add_cog(WlRequestHandler(bot))
        chs = []
        raw = getattr(config, "WL_REQUEST_CHANNEL_IDS", None) or []
        if raw:
            chs = list(raw)
        elif getattr(config, "WL_REQUEST_CHANNEL_ID", 0):
            chs = [int(config.WL_REQUEST_CHANNEL_ID)]
        if chs:
            logger.info("WL request handler enabled for channel(s): %s", chs)
