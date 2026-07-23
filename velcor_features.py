"""
VelcorFeatures — Discord.py cog ported from Velcor3 invite_blocker.py.
Loaded into the existing BlockBrainBot so all commands share the same prefix.
"""
from __future__ import annotations

import discord
from discord.ext import commands
import re
import asyncio
import json
import sys
import os
import io
import sqlite3
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from pathlib import Path

import config
import holder_welcome_store
from app_paths import DATA_DIR, BASE_DIR, ensure_dirs

# ── Ensure data directory exists ────────────────────────────────────────────
ensure_dirs()

# ── Database path (Render-compatible) ───────────────────────────────────────
DB_PATH = DATA_DIR / "user_stats.db"

# ── Configuration constants ─────────────────────────────────────────────────
DELETE_MESSAGE = True
PERMISSION_MESSAGE = "I need the `Manage Messages` permission to delete links!"

AUTO_ROLE_ID = getattr(config, "VELCOR_AUTO_ROLE_ID", 0)
WELCOME_CHANNEL_ID = getattr(config, "VELCOR_WELCOME_CHANNEL_ID", 0)
AUTO_REACT_CHANNEL_ID = getattr(config, "VELCOR_AUTO_REACT_CHANNEL_ID", 0)
ABOUT_CHANNEL_ID = getattr(config, "VELCOR_ABOUT_CHANNEL_ID", 0)
ENABLE_HOLDER_WELCOME = getattr(config, "ENABLE_HOLDER_WELCOME", True)
HOLDER_WELCOME_PAUSED = getattr(config, "HOLDER_WELCOME_PAUSED", False)
HOLDER_VERIFIED_ROLE_ID = int(getattr(config, "HOLDER_VERIFIED_ROLE_ID", 0) or 0)
HOLDER_WELCOME_CHANNEL_ID = int(getattr(config, "HOLDER_WELCOME_CHANNEL_ID", 0) or 0)
HOLDER_CLAIM_ROLES_CHANNEL_ID = int(getattr(config, "HOLDER_CLAIM_ROLES_CHANNEL_ID", 0) or 0)
HOLDER_WELCOME_MESSAGE = getattr(config, "HOLDER_WELCOME_MESSAGE", "")


def _auto_react_emoji() -> discord.PartialEmoji | str:
    """Custom guild emoji or unicode fallback for success-channel auto-reactions."""
    emoji_id = int(getattr(config, "VELCOR_AUTO_REACT_EMOJI_ID", 0) or 0)
    if emoji_id:
        return discord.PartialEmoji(
            name=getattr(config, "VELCOR_AUTO_REACT_EMOJI_NAME", "velcor3") or "velcor3",
            id=emoji_id,
            animated=bool(getattr(config, "VELCOR_AUTO_REACT_EMOJI_ANIMATED", False)),
        )
    return "🔥"
WELCOME_BANNER_PATH = getattr(
    config,
    "VELCOR_WELCOME_BANNER_PATH",
    str(BASE_DIR / "assets" / "velcor_banner.jpg"),
)

# ── Regex patterns ──────────────────────────────────────────────────────────
DISCORD_INVITE_REGEX = r"(?:https?://)?(?:www\.)?(?:discord(?:app)?\.com/(?:invite/)?|discord\.gg/|discord\.io/|discord\.me/|discord\.li/)([A-Za-z0-9\-]+)"
URL_REGEX = r"(https?://[^\s]+|www\.[^\s]+|\b[a-zA-Z0-9-]+\.[a-zA-Z]{2,}(?:/[^\s]*)?)"
SOCIAL_MEDIA_REGEX = r"(?:https?://)?(?:www\.)?(?:youtube\.com|youtu\.be|twitter\.com|x\.com|instagram\.com|facebook\.com|tiktok\.com|reddit\.com|twitch\.tv|spotify\.com|soundcloud\.com)/[^\s]+"
FILE_SHARING_REGEX = r"(?:https?://)?(?:www\.)?(?:mediafire\.com|mega\.nz|dropbox\.com|drive\.google\.com|onedrive\.live\.com)/[^\s]+"
# Discord GIF picker + common hosts (allowed when allow_gifs is on)
GIF_HOST_REGEX = re.compile(
    r"(?:^|//)(?:"
    r"(?:[\w-]+\.)?tenor\.com|tenor\.co|"
    r"(?:[\w-]+\.)?giphy\.com|"
    r"media\.tenor\.com|media\.giphy\.com|"
    r"(?:[\w-]+\.)?discordapp\.(?:net|com)|"
    r"(?:[\w-]+\.)?discord\.com/(?:attachments|ephemeral-attachments)"
    r")",
    re.IGNORECASE,
)

# ── Permission helper ───────────────────────────────────────────────────────
def is_owner_or_admin(ctx):
    if getattr(ctx.author, "id", None) in getattr(config, "BOT_OWNER_IDS", []):
        return True
    if ctx.author.guild_permissions.administrator:
        return True
    raise commands.CheckFailure("You do not have permission to use this command! (Requires Administrator)")


# ════════════════════════════════════════════════════════════════════════════
# Database helpers
# ════════════════════════════════════════════════════════════════════════════

def get_db_connection():
    return sqlite3.connect(str(DB_PATH))


def init_database():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS user_activity (
            guild_id INTEGER,
            user_id INTEGER,
            message_count INTEGER DEFAULT 0,
            last_seen TEXT,
            first_seen TEXT,
            last_message_id INTEGER,
            PRIMARY KEY (guild_id, user_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS scan_history (
            guild_id INTEGER,
            channel_id INTEGER,
            last_scanned TEXT,
            messages_scanned INTEGER DEFAULT 0,
            PRIMARY KEY (guild_id, channel_id)
        )
        """
    )

    # invite_tracking kept for backward-compat reads but we no longer write here
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS invite_tracking (
            guild_id INTEGER,
            user_id INTEGER,
            inviter_id INTEGER,
            invite_code TEXT,
            joined_at TEXT,
            PRIMARY KEY (guild_id, user_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS inviter_stats (
            guild_id INTEGER,
            user_id INTEGER,
            total_invites INTEGER DEFAULT 0,
            successful_invites INTEGER DEFAULT 0,
            PRIMARY KEY (guild_id, user_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS guild_settings (
            guild_id INTEGER PRIMARY KEY,
            delete_links INTEGER DEFAULT 1,
            delete_discord_invites INTEGER DEFAULT 1,
            delete_social_media INTEGER DEFAULT 1,
            delete_file_sharing INTEGER DEFAULT 1,
            delete_all_links INTEGER DEFAULT 0,
            protected_channels TEXT DEFAULT '[]'
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS whitelisted_domains (
            domain TEXT PRIMARY KEY
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS whitelisted_invites (
            invite_code TEXT PRIMARY KEY
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_user_activity (
            guild_id INTEGER,
            user_id INTEGER,
            date TEXT,
            message_count INTEGER DEFAULT 0,
            PRIMARY KEY (guild_id, user_id, date)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ping_roles (
            guild_id INTEGER,
            role_id INTEGER,
            label TEXT,
            emoji TEXT,
            PRIMARY KEY (guild_id, role_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS claim_role_panels (
            message_id INTEGER PRIMARY KEY,
            guild_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            created_at TEXT
        )
        """
    )

    try:
        cursor.execute("ALTER TABLE guild_settings ADD COLUMN allow_gifs INTEGER DEFAULT 1")
    except sqlite3.OperationalError:
        pass

    try:
        cursor.execute("ALTER TABLE ping_roles ADD COLUMN description TEXT DEFAULT ''")
    except sqlite3.OperationalError:
        pass

    conn.commit()
    conn.close()
    print("[VelcorFeatures] Database initialized")
    sys.stdout.flush()


def update_daily_activity(guild_id: int, user_id: int, count: int = 1, date_str: str = None):
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO daily_user_activity (guild_id, user_id, date, message_count)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(guild_id, user_id, date) DO UPDATE SET
            message_count = message_count + excluded.message_count
        """,
        (guild_id, user_id, date_str, count),
    )
    conn.commit()
    conn.close()


def update_user_activity(guild_id: int, user_id: int, message_time: datetime = None):
    if message_time is None:
        message_time = datetime.now(timezone.utc)
    now_iso = message_time.isoformat()
    today = message_time.strftime("%Y-%m-%d")

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT message_count, first_seen FROM user_activity WHERE guild_id = ? AND user_id = ?",
        (guild_id, user_id),
    )
    row = cursor.fetchone()

    if row:
        cursor.execute(
            "UPDATE user_activity SET message_count = message_count + 1, last_seen = ? WHERE guild_id = ? AND user_id = ?",
            (now_iso, guild_id, user_id),
        )
    else:
        cursor.execute(
            "INSERT INTO user_activity (guild_id, user_id, message_count, last_seen, first_seen) VALUES (?, ?, 1, ?, ?)",
            (guild_id, user_id, now_iso, now_iso),
        )

    conn.commit()
    conn.close()
    update_daily_activity(guild_id, user_id, count=1, date_str=today)


def get_stats_window(guild_id: int, user_id: int, days: int):
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT SUM(message_count) FROM daily_user_activity WHERE guild_id = ? AND user_id = ? AND date >= ?",
        (guild_id, user_id, since),
    )
    row = cursor.fetchone()
    conn.close()
    return row[0] or 0


def get_user_stats(guild_id: int, user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT message_count, last_seen, first_seen FROM user_activity WHERE guild_id = ? AND user_id = ?",
        (guild_id, user_id),
    )
    row = cursor.fetchone()
    conn.close()
    if row:
        return {
            "message_count": row[0] or 0,
            "last_seen": datetime.fromisoformat(row[1]) if row[1] else None,
            "first_seen": datetime.fromisoformat(row[2]) if row[2] else None,
        }
    return {"message_count": 0, "last_seen": None, "first_seen": None}


def get_all_user_stats(guild_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT user_id, message_count, last_seen, first_seen FROM user_activity WHERE guild_id = ?",
        (guild_id,),
    )
    rows = cursor.fetchall()
    conn.close()
    return {
        r[0]: {
            "message_count": r[1] or 0,
            "last_seen": datetime.fromisoformat(r[2]) if r[2] else None,
            "first_seen": datetime.fromisoformat(r[3]) if r[3] else None,
        }
        for r in rows
    }


def bulk_update_activity(guild_id: int, user_data: dict, daily_data: dict = None):
    conn = get_db_connection()
    cursor = conn.cursor()

    for user_id, count in user_data.items():
        now_iso = datetime.now(timezone.utc).isoformat()
        cursor.execute(
            "SELECT message_count FROM user_activity WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        )
        row = cursor.fetchone()
        if row:
            cursor.execute(
                "UPDATE user_activity SET message_count = message_count + ?, last_seen = ? WHERE guild_id = ? AND user_id = ?",
                (count, now_iso, guild_id, user_id),
            )
        else:
            cursor.execute(
                "INSERT INTO user_activity (guild_id, user_id, message_count, last_seen, first_seen) VALUES (?, ?, ?, ?, ?)",
                (guild_id, user_id, count, now_iso, now_iso),
            )

    if daily_data:
        for (user_id, date_str), count in daily_data.items():
            cursor.execute(
                """
                INSERT INTO daily_user_activity (guild_id, user_id, date, message_count)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(guild_id, user_id, date) DO UPDATE SET
                    message_count = message_count + excluded.message_count
                """,
                (guild_id, user_id, date_str, count),
            )

    conn.commit()
    conn.close()


def mark_channel_scanned(guild_id: int, channel_id: int, messages_scanned: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    now_iso = datetime.now(timezone.utc).isoformat()
    cursor.execute(
        """
        INSERT INTO scan_history (guild_id, channel_id, last_scanned, messages_scanned)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(guild_id, channel_id) DO UPDATE SET
            last_scanned = excluded.last_scanned,
            messages_scanned = messages_scanned + excluded.messages_scanned
        """,
        (guild_id, channel_id, now_iso, messages_scanned),
    )
    conn.commit()
    conn.close()


def get_scan_status(guild_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT channel_id, last_scanned, messages_scanned FROM scan_history WHERE guild_id = ?",
        (guild_id,),
    )
    rows = cursor.fetchall()
    conn.close()
    return {r[0]: {"last_scanned": r[1], "messages_scanned": r[2]} for r in rows}


def get_invites(guild_id: int, user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT total_invites, successful_invites FROM inviter_stats WHERE guild_id = ? AND user_id = ?",
        (guild_id, user_id),
    )
    row = cursor.fetchone()
    conn.close()
    return row or (0, 0)


def get_who_invited(guild_id: int, user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT inviter_id, invite_code, joined_at FROM invite_tracking WHERE guild_id = ? AND user_id = ?",
        (guild_id, user_id),
    )
    row = cursor.fetchone()
    conn.close()
    return row


def save_guild_settings(guild_id, settings_dict):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT OR REPLACE INTO guild_settings
        (guild_id, delete_links, delete_discord_invites, delete_social_media, delete_file_sharing, delete_all_links, protected_channels, allow_gifs)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            guild_id,
            1 if settings_dict.get("delete_links") else 0,
            1 if settings_dict.get("delete_discord_invites") else 0,
            1 if settings_dict.get("delete_social_media") else 0,
            1 if settings_dict.get("delete_file_sharing") else 0,
            1 if settings_dict.get("delete_all_links") else 0,
            json.dumps(settings_dict.get("protected_channels", [])),
            1 if settings_dict.get("allow_gifs", True) else 0,
        ),
    )
    conn.commit()
    conn.close()


def load_all_settings():
    conn = get_db_connection()
    cursor = conn.cursor()
    settings = {}
    cursor.execute("SELECT * FROM guild_settings")
    for row in cursor.fetchall():
        settings[row[0]] = {
            "delete_links": bool(row[1]),
            "delete_discord_invites": bool(row[2]),
            "delete_social_media": bool(row[3]),
            "delete_file_sharing": bool(row[4]),
            "delete_all_links": bool(row[5]),
            "protected_channels": json.loads(row[6]),
            "allow_gifs": (bool(row[7]) if row[7] is not None else True) if len(row) > 7 else True,
            "whitelisted_channels": [],
        }
    conn.close()
    return settings


def db_add_whitelisted_domain(domain: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO whitelisted_domains (domain) VALUES (?)", (domain.lower(),))
    conn.commit()
    conn.close()


def db_remove_whitelisted_domain(domain: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM whitelisted_domains WHERE domain = ?", (domain.lower(),))
    conn.commit()
    conn.close()


def db_add_whitelisted_invite(code: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO whitelisted_invites (invite_code) VALUES (?)", (code.lower(),))
    conn.commit()
    conn.close()


def db_remove_whitelisted_invite(code: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM whitelisted_invites WHERE invite_code = ?", (code.lower(),))
    conn.commit()
    conn.close()


def get_whitelisted_domains():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT domain FROM whitelisted_domains")
    rows = cursor.fetchall()
    conn.close()
    return [r[0] for r in rows]


def get_whitelisted_invites():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT invite_code FROM whitelisted_invites")
    rows = cursor.fetchall()
    conn.close()
    return [r[0] for r in rows]


def get_all_links(text: str):
    return re.findall(r"https?://[^\s]+", text) + re.findall(r"www\.[^\s]+", text)


def contains_any_link(text: str):
    return bool(re.search(r"https?://[^\s]+", text) or re.search(r"www\.[^\s]+", text))


def is_whitelisted_domain(url: str):
    url_lower = url.lower()
    for domain in get_whitelisted_domains():
        if domain in url_lower:
            return True
    return False


def is_gif_url(url: str) -> bool:
    """True for Tenor, Giphy, Discord CDN, or obvious .gif URLs."""
    u = (url or "").lower().strip()
    if not u:
        return False
    if u.endswith(".gif") or ".gif?" in u:
        return True
    return bool(GIF_HOST_REGEX.search(u))


def _attachments_are_gif_only(message: discord.Message) -> bool:
    if not message.attachments:
        return False
    for att in message.attachments:
        ct = (att.content_type or "").lower()
        fn = (att.filename or "").lower()
        if "gif" in ct or fn.endswith(".gif"):
            continue
        return False
    return True


def _stickers_are_gif_only(message: discord.Message) -> bool:
    stickers = getattr(message, "stickers", None) or []
    if not stickers:
        return False
    for sticker in stickers:
        fmt = getattr(sticker, "format", None)
        if fmt == discord.StickerFormatType.gif:
            continue
        return False
    return True


def _embeds_are_gif_only(message: discord.Message) -> bool:
    if not message.embeds:
        return False
    for emb in message.embeds:
        et = str(getattr(emb, "type", "") or "").lower()
        urls = [
            emb.url or "",
            (emb.image.url if emb.image else "") or "",
            (emb.thumbnail.url if emb.thumbnail else "") or "",
            (emb.video.url if emb.video else "") or "",
        ]
        if et in ("gif", "gifv"):
            continue
        if any(is_gif_url(u) for u in urls if u):
            continue
        if et == "image" and any(".gif" in (u or "").lower() for u in urls):
            continue
        return False
    return True


def _collect_message_urls(message: discord.Message, links_from_content: list) -> list:
    urls = list(links_from_content)
    for emb in message.embeds:
        for u in (
            emb.url,
            emb.image.url if emb.image else None,
            emb.thumbnail.url if emb.thumbnail else None,
            emb.video.url if emb.video else None,
        ):
            if u and u not in urls:
                urls.append(u)
    return urls


def should_allow_gif_message(
    message: discord.Message,
    links_found: list,
    settings: dict,
) -> bool:
    """Allow GIF-only posts in link-filtered channels (picker URLs, files, embeds, stickers)."""
    if not settings.get("allow_gifs", True):
        return False

    if _stickers_are_gif_only(message) and not links_found:
        return True
    if _attachments_are_gif_only(message) and not links_found:
        return True
    if not links_found and _embeds_are_gif_only(message):
        return True

    all_urls = _collect_message_urls(message, links_found)
    if not all_urls:
        return False

    for link in all_urls:
        link_lower = link.lower()
        if is_whitelisted_domain(link_lower):
            continue
        if is_gif_url(link_lower):
            continue
        discord_match = re.search(DISCORD_INVITE_REGEX, link_lower)
        if discord_match and settings.get("delete_discord_invites"):
            if discord_match.group(1).lower() not in get_whitelisted_invites():
                return False
        if re.search(SOCIAL_MEDIA_REGEX, link_lower) and settings.get("delete_social_media"):
            return False
        if re.search(FILE_SHARING_REGEX, link_lower) and settings.get("delete_file_sharing"):
            return False
        if settings.get("delete_all_links"):
            return False
        if re.search(URL_REGEX, link_lower) and settings.get("delete_links"):
            return False

    return any(is_gif_url(u) for u in all_urls) or _attachments_are_gif_only(message)


# ════════════════════════════════════════════════════════════════════════════
# Claim roles — reaction panels (+ legacy buttons for old messages)
# ════════════════════════════════════════════════════════════════════════════


def load_ping_roles(guild_id: int) -> list:
    """Rows: role_id, label, emoji, description for one guild."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT role_id, label, emoji, COALESCE(description, '')
            FROM ping_roles WHERE guild_id = ? ORDER BY label
            """,
            (guild_id,),
        )
        rows = [
            {"role_id": r[0], "label": r[1], "emoji": r[2], "description": r[3] or ""}
            for r in cursor.fetchall()
        ]
        conn.close()
        return rows
    except Exception as e:
        print(f"[VelcorFeatures] load_ping_roles: {e}")
        return []


_ADDPINGROLE_LINE_RE = re.compile(
    r"(?:!?\s*velcor3\s+)?addpingrole\s+"
    r"(?:<@&(?P<role_id>\d+)>)\s+"
    r"(?P<emoji><a?:\w+:\d+>|\d+|\S+)\s+"
    r"(?P<description>.+?)\s*$",
    re.IGNORECASE,
)


def parse_addpingrole_lines(content: str, guild: discord.Guild) -> list:
    """Parse one or more addpingrole lines from a single message (paste-friendly)."""
    rows: list = []
    for line in (content or "").splitlines():
        line = line.strip()
        if not line or "addpingrole" not in line.lower():
            continue
        m = _ADDPINGROLE_LINE_RE.search(line)
        if not m:
            continue
        role = guild.get_role(int(m.group("role_id")))
        if not role:
            continue
        emoji_raw = (m.group("emoji") or "").strip()
        if emoji_raw.startswith("<") and emoji_raw.endswith(">"):
            em_id = re.search(r":(\d+)>", emoji_raw)
            emoji_raw = em_id.group(1) if em_id else emoji_raw
        rows.append(
            {
                "role": role,
                "role_id": role.id,
                "label": role.name,
                "emoji": emoji_raw,
                "description": (m.group("description") or "").strip(),
            }
        )
    return rows


def _pingrole_description_from_message(content: str, role: discord.Role, emoji: str) -> str:
    """Description for a single addpingrole (one line only — avoids swallowing pasted batches)."""
    for row in parse_addpingrole_lines(content, role.guild):
        if row["role_id"] == role.id:
            return row["description"]
    text = (content or "").strip()
    if "\n" in text and text.lower().count("addpingrole") > 1:
        for line in text.splitlines():
            if role.mention in line or f"<@&{role.id}>" in line:
                sub = parse_addpingrole_lines(line, role.guild)
                if sub:
                    return sub[0]["description"]
    text = re.sub(r"(?i)^\s*!?\s*velcor3\s+addpingrole\s*", "", text).strip()
    for needle in (role.mention, f"<@&{role.id}>"):
        if needle in text:
            text = text.replace(needle, " ", 1)
    emoji_s = (emoji or "").strip()
    if emoji_s and emoji_s in text:
        text = text.replace(emoji_s, " ", 1)
    if "addpingrole" in text.lower():
        text = text.split("addpingrole")[0]
    return " ".join(text.split()).strip()


def save_ping_role(guild_id: int, role_id: int, label: str, emoji: str, description: str) -> None:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT OR REPLACE INTO ping_roles (guild_id, role_id, label, emoji, description)
        VALUES (?, ?, ?, ?, ?)
        """,
        (guild_id, role_id, label, emoji, description),
    )
    conn.commit()
    conn.close()


def format_claim_roles_bullets(guild: discord.Guild | None, roles: list) -> str:
    """Role list for the embed (dropdown shows the same options)."""
    lines: list[str] = []
    for row in roles:
        rid = int(row["role_id"])
        role = guild.get_role(rid) if guild else None
        mention = role.mention if role else f"<@&{rid}>"
        desc = (row.get("description") or "").strip()
        if "addpingrole" in desc.lower():
            desc = ""
        if not desc:
            desc = (row.get("label") or "").strip()
        if desc:
            lines.append(f"• {mention} — {desc}")
        else:
            lines.append(f"• {mention}")
    return "\n".join(lines)


def parse_stored_emoji(
    emoji_str: str,
    guild: discord.Guild | None = None,
) -> discord.PartialEmoji | str | None:
    s = (emoji_str or "").strip()
    if not s:
        return None
    if s.isdigit():
        eid = int(s)
        if guild:
            em = guild.get_emoji(eid)
            if em:
                return em
        return discord.PartialEmoji(name="emoji", id=eid)
    return s


def reaction_emoji_key(emoji: discord.PartialEmoji | str) -> str:
    if isinstance(emoji, discord.PartialEmoji):
        return f"custom:{emoji.id}"
    return f"unicode:{emoji}"


def format_role_emoji_display(guild: discord.Guild | None, emoji_str: str) -> str:
    """Markdown-safe emoji for embed fields."""
    s = (emoji_str or "").strip()
    if not s:
        return "▫️"
    if s.isdigit() and guild:
        em = guild.get_emoji(int(s))
        if em:
            return str(em)
        return "▫️"
    return s


def reaction_matches_stored(
    stored_emoji: str,
    reaction_emoji,
    guild: discord.Guild | None = None,
) -> bool:
    parsed = parse_stored_emoji(stored_emoji, guild)
    if isinstance(parsed, discord.PartialEmoji):
        rid = getattr(reaction_emoji, "id", None)
        return rid is not None and int(rid) == int(parsed.id)
    return str(reaction_emoji) == str(parsed)


def role_id_for_reaction(
    guild_id: int,
    reaction_emoji,
    guild: discord.Guild | None = None,
) -> int | None:
    for row in load_ping_roles(guild_id):
        if reaction_matches_stored(row["emoji"], reaction_emoji, guild):
            return int(row["role_id"])
    return None


def emoji_already_used(guild_id: int, emoji_str: str, guild: discord.Guild | None = None) -> str | None:
    """Return label of existing role using this emoji, or None."""
    probe = parse_stored_emoji(emoji_str, guild)
    if probe is None:
        return None
    key = reaction_emoji_key(probe)
    for row in load_ping_roles(guild_id):
        other = parse_stored_emoji(row["emoji"], guild)
        if other is not None and reaction_emoji_key(other) == key:
            return row.get("label") or str(row["role_id"])
    return None


def register_claim_panel(message_id: int, guild_id: int, channel_id: int) -> None:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT OR REPLACE INTO claim_role_panels (message_id, guild_id, channel_id, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (message_id, guild_id, channel_id, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    conn.close()


def get_claim_panel(message_id: int) -> dict | None:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT guild_id, channel_id FROM claim_role_panels WHERE message_id = ?",
        (message_id,),
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    return {"guild_id": row[0], "channel_id": row[1]}


def is_claim_panel_message(message_id: int) -> bool:
    return get_claim_panel(message_id) is not None


class DynamicPingRoleButton(discord.ui.Button):
    def __init__(self, role_id: int, label: str, emoji: str):
        btn_kwargs = {"label": label, "style": discord.ButtonStyle.primary, "custom_id": f"pingrole_{role_id}"}
        if emoji and emoji.strip():
            emoji_str = emoji.strip()
            if emoji_str.isdigit():
                btn_kwargs["emoji"] = discord.PartialEmoji(name="custom", id=int(emoji_str))
            else:
                btn_kwargs["emoji"] = emoji_str
        super().__init__(**btn_kwargs)
        self.role_id = role_id

    async def callback(self, interaction: discord.Interaction):
        role = interaction.guild.get_role(self.role_id)
        if not role:
            await interaction.response.send_message("Role not found. It may have been deleted.", ephemeral=True)
            return
        if role in interaction.user.roles:
            await interaction.user.remove_roles(role)
            await interaction.response.send_message(f"Removed **{self.label}** role!", ephemeral=True)
        else:
            await interaction.user.add_roles(role)
            await interaction.response.send_message(f"Added **{self.label}** role!", ephemeral=True)


class PingRolesView(discord.ui.View):
    """
    Legacy self-assignable role buttons (old panels only). custom_id: pingrole_{role_id}.
    """

    def __init__(self, guild_id: int | None = None):
        super().__init__(timeout=None)
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            if guild_id is None:
                cursor.execute("SELECT role_id, label, emoji FROM ping_roles")
            else:
                cursor.execute(
                    "SELECT role_id, label, emoji FROM ping_roles WHERE guild_id = ?",
                    (guild_id,),
                )
            for role_id, label, emoji in cursor.fetchall():
                self.add_item(DynamicPingRoleButton(role_id, label, emoji))
            conn.close()
        except Exception as e:
            print(f"[VelcorFeatures] Error loading ping roles: {e}")


def distinct_ping_roles_guild_ids() -> list[int]:
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT guild_id FROM ping_roles")
        ids = [int(r[0]) for r in cursor.fetchall()]
        conn.close()
        return ids
    except Exception:
        return []


def _select_option_emoji(
    guild: discord.Guild | None, emoji_str: str
) -> str | discord.PartialEmoji | None:
    parsed = parse_stored_emoji(emoji_str, guild)
    if parsed is None:
        return None
    return parsed


class ClaimRolesSelect(discord.ui.Select):
    """Multi-select dropdown — only adds selected roles."""

    def __init__(
        self,
        guild_id: int,
        roles: list,
        guild: discord.Guild | None = None,
    ):
        self.guild_id = guild_id
        options: list[discord.SelectOption] = []
        for row in roles[:25]:
            rid = int(row["role_id"])
            label = (row.get("label") or "Role")[:100]
            desc = (row.get("description") or row.get("label") or "")[:100]
            if "addpingrole" in desc.lower():
                desc = (row.get("label") or "")[:100]
            em = _select_option_emoji(guild, row.get("emoji", ""))
            opt_kw: dict = {"label": label, "value": str(rid)}
            if desc:
                opt_kw["description"] = desc
            if em is not None:
                opt_kw["emoji"] = em
            options.append(discord.SelectOption(**opt_kw))

        super().__init__(
            placeholder="Choose your roles…",
            min_values=0,
            max_values=max(1, len(options)),
            options=options,
            custom_id=f"velcor_claim_roles_{guild_id}",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if not interaction.guild or interaction.guild.id != self.guild_id:
            await interaction.response.send_message(
                "This menu is no longer valid. Ask staff to post a new claim panel.",
                ephemeral=True,
            )
            return
        member = interaction.user
        if not isinstance(member, discord.Member):
            await interaction.response.send_message("Could not resolve member.", ephemeral=True)
            return

        rows = load_ping_roles(self.guild_id)
        claimable = {int(r["role_id"]) for r in rows}
        selected = {int(v) for v in self.values if v.isdigit()}

        added: list[str] = []
        failed: list[str] = []

        for rid in claimable:
            role = interaction.guild.get_role(rid)
            if not role:
                continue
            has = role in member.roles
            want = rid in selected
            try:
                if want and not has:
                    await member.add_roles(role, reason="Claim roles dropdown")
                    added.append(role.mention)
            except discord.Forbidden:
                failed.append(role.name)
            except discord.HTTPException:
                failed.append(role.name)

        if failed:
            err_lines: list[str] = []
            if added:
                err_lines.append("✅ **Added:** " + ", ".join(added))
            err_lines.append("⚠️ Could not update: " + ", ".join(failed))
            await interaction.response.send_message("\n".join(err_lines), ephemeral=True)
        else:
            await interaction.response.defer(ephemeral=True)
            # Reset dropdown so Discord doesn't persist claimed roles in header
            try:
                await interaction.message.edit(view=None)
                roles = load_ping_roles(self.guild_id)
                view = ClaimRolesView(self.guild_id, roles, interaction.guild)
                await interaction.message.edit(view=view)
            except Exception:
                pass
            if added:
                try:
                    await interaction.followup.send(
                        "✅ **Claimed:** " + ", ".join(added), ephemeral=True
                    )
                except Exception:
                    pass


class ClaimRolesView(discord.ui.View):
    """Persistent dropdown for claim roles (one view per guild_id)."""

    def __init__(self, guild_id: int, roles: list, guild: discord.Guild | None = None):
        super().__init__(timeout=None)
        if roles:
            self.add_item(ClaimRolesSelect(guild_id, roles, guild))


def register_claim_role_views(bot: commands.Bot) -> int:
    """Re-register dropdown views after restart (one per guild with ping_roles)."""
    count = 0
    for gid in distinct_ping_roles_guild_ids():
        roles = load_ping_roles(gid)
        if not roles:
            continue
        guild = bot.get_guild(gid)
        bot.add_view(ClaimRolesView(gid, roles, guild))
        count += 1
    return count


def build_claim_roles_embed(
    *,
    guild: discord.Guild | None = None,
    roles: list | None = None,
    for_empty_config: bool = False,
) -> discord.Embed:
    """Rich embed for claim-roles dropdown panel."""
    from brand_assets import apply_claim_roles_branding

    title = getattr(config, "CLAIM_ROLES_EMBED_TITLE", "🎭  Claim your roles")
    if for_empty_config or not roles:
        description = (
            "No claim roles are configured for this server yet.\n\n"
            "**Staff setup**\n"
            "• `!velcor3 addpingrole @Role emoji description here` — add a reaction role\n"
            "• `!velcor3 post_claim_roles` — post this panel again"
        )
        embed = discord.Embed(title=title, description=description)
        apply_claim_roles_branding(embed)
        return embed

    intro = getattr(
        config,
        "CLAIM_ROLES_EMBED_DESCRIPTION",
        "Use the **dropdown below** to pick your roles. You can select more than one.",
    )
    roles_field_name = getattr(config, "CLAIM_ROLES_ROLES_FIELD_NAME", "Available roles")
    role_list = format_claim_roles_bullets(guild, roles)

    embed = discord.Embed(
        title=title,
        description=intro,
    )
    embed.add_field(
        name=roles_field_name,
        value=role_list[:1024] or "—",
        inline=False,
    )
    embed.add_field(
        name="How to claim",
        value=(
            "1️⃣ Open **Choose your roles…** below\n"
            "2️⃣ Tick every role you want (multi-select)\n"
            "3️⃣ Submit — selected roles are added\n"
            "4️⃣ Open again anytime to claim more"
        ),
        inline=False,
    )

    apply_claim_roles_branding(embed)

    thumb_url = getattr(config, "CLAIM_ROLES_EMBED_THUMBNAIL_URL", "") or ""
    if thumb_url:
        embed.set_thumbnail(url=thumb_url)
    elif guild and guild.icon:
        embed.set_thumbnail(url=guild.icon.url)

    return embed


async def send_claim_roles_panel(
    channel,
    *,
    guild_id: Optional[int] = None,
    bot: Optional[commands.Bot] = None,
) -> None:
    """Post claim-roles embed with a multi-select dropdown."""
    from brand_assets import hydrate_claim_roles_banner_attachment, prepare_claim_roles_panel

    guild = getattr(channel, "guild", None)
    gid = guild_id if guild_id is not None else getattr(guild, "id", None)
    if not gid:
        raise ValueError("send_claim_roles_panel requires a guild channel")

    roles = load_ping_roles(int(gid))
    embed = build_claim_roles_embed(
        guild=guild,
        roles=roles,
        for_empty_config=not roles,
    )
    embed, files = prepare_claim_roles_panel(embed)
    embed, files = await hydrate_claim_roles_banner_attachment(embed, files)
    view = ClaimRolesView(int(gid), roles, guild) if roles else None
    kwargs: dict = {"embed": embed}
    if files:
        kwargs["files"] = files
    if view is not None:
        kwargs["view"] = view
    message = await channel.send(**kwargs)

    if not roles:
        return

    register_claim_panel(message.id, int(gid), channel.id)

    if bot is not None:
        bot.add_view(ClaimRolesView(int(gid), roles, guild))


# ════════════════════════════════════════════════════════════════════════════
# Cog
# ════════════════════════════════════════════════════════════════════════════

class VelcorFeatures(commands.Cog):
    """Velcor3 community & moderation features ported into BlockBrainBot."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.guild_settings: dict = {}
        self.scan_in_progress: dict = {}
        init_database()
        self.guild_settings = load_all_settings()

    # ── Listeners ──────────────────────────────────────────────────────────

    @commands.Cog.listener()
    async def on_ready(self):
        n = register_claim_role_views(self.bot)
        print(f"[VelcorFeatures] Cog loaded — claim-role dropdowns registered: {n}")

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        if not ENABLE_HOLDER_WELCOME or HOLDER_WELCOME_PAUSED:
            return
        if not HOLDER_VERIFIED_ROLE_ID or not HOLDER_WELCOME_CHANNEL_ID:
            return
        if after.bot or not after.guild:
            return

        before_role_ids = {int(role.id) for role in before.roles}
        after_role_ids = {int(role.id) for role in after.roles}
        if HOLDER_VERIFIED_ROLE_ID not in after_role_ids:
            return
        if HOLDER_VERIFIED_ROLE_ID in before_role_ids:
            return

        try:
            holder_welcome_store.init_db()
            if holder_welcome_store.has_welcomed_holder(after.guild.id, after.id):
                holder_welcome_store.mark_holder_seen(
                    guild_id=after.guild.id,
                    user_id=after.id,
                    role_id=HOLDER_VERIFIED_ROLE_ID,
                )
                return
        except Exception as e:
            print(f"[VelcorFeatures] Holder welcome dedupe unavailable; skipping welcome: {e}")
            return

        channel = after.guild.get_channel(HOLDER_WELCOME_CHANNEL_ID)
        if channel is None:
            try:
                channel = await after.guild.fetch_channel(HOLDER_WELCOME_CHANNEL_ID)
            except Exception as e:
                print(f"[VelcorFeatures] Holder welcome channel unavailable: {e}")
                return

        claim_channel = None
        if HOLDER_CLAIM_ROLES_CHANNEL_ID:
            claim_channel = after.guild.get_channel(HOLDER_CLAIM_ROLES_CHANNEL_ID)
        claim_channel_text = claim_channel.mention if claim_channel else f"<#{HOLDER_CLAIM_ROLES_CHANNEL_ID}>"
        body = (HOLDER_WELCOME_MESSAGE or "Welcome, {mention} to the Velcorians family!").format(
            mention=after.mention,
            claim_roles_channel=claim_channel_text,
            user=after.display_name,
            guild=after.guild.name,
        )

        try:
            sent = await channel.send(body)
        except discord.Forbidden:
            print(f"[VelcorFeatures] No permission in holder welcome channel {HOLDER_WELCOME_CHANNEL_ID}")
            return
        except Exception as e:
            print(f"[VelcorFeatures] Holder welcome send failed: {e}")
            return

        try:
            holder_welcome_store.mark_holder_seen(
                guild_id=after.guild.id,
                user_id=after.id,
                role_id=HOLDER_VERIFIED_ROLE_ID,
                welcome_message_id=getattr(sent, "id", None),
            )
        except Exception as e:
            print(f"[VelcorFeatures] Holder welcome sent but dedupe save failed: {e}")

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent) -> None:
        if payload.user_id == self.bot.user.id:
            return
        panel = get_claim_panel(payload.message_id)
        if not panel or payload.guild_id != panel["guild_id"]:
            return

        guild = self.bot.get_guild(payload.guild_id)
        if not guild:
            return

        role_id = role_id_for_reaction(panel["guild_id"], payload.emoji, guild)
        if not role_id:
            return

        role = guild.get_role(role_id)
        if not role:
            return

        member = guild.get_member(payload.user_id)
        if member is None:
            try:
                member = await guild.fetch_member(payload.user_id)
            except Exception:
                return

        if role in member.roles:
            return
        try:
            await member.add_roles(role, reason="Claim roles panel — reaction add")
        except discord.Forbidden:
            print(f"[VelcorFeatures] No permission to add role {role_id}")
        except Exception as e:
            print(f"[VelcorFeatures] add_roles failed: {e}")

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent) -> None:
        if payload.user_id == self.bot.user.id:
            return
        panel = get_claim_panel(payload.message_id)
        if not panel or payload.guild_id != panel["guild_id"]:
            return

        guild = self.bot.get_guild(payload.guild_id)
        if not guild:
            return

        role_id = role_id_for_reaction(panel["guild_id"], payload.emoji, guild)
        if not role_id:
            return

        role = guild.get_role(role_id)
        if not role:
            return

        member = guild.get_member(payload.user_id)
        if member is None:
            try:
                member = await guild.fetch_member(payload.user_id)
            except Exception:
                return

        if role not in member.roles:
            return
        try:
            await member.remove_roles(role, reason="Claim roles panel — reaction removed")
        except discord.Forbidden:
            print(f"[VelcorFeatures] No permission to remove role {role_id}")
        except Exception as e:
            print(f"[VelcorFeatures] remove_roles failed: {e}")

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        guild = member.guild
        # --- Auto-role ---
        if AUTO_ROLE_ID and len(member.roles) == 1:
            role = guild.get_role(AUTO_ROLE_ID)
            if role:
                try:
                    await member.add_roles(role, reason="Automatic role assignment for new users")
                    print(f"[VelcorFeatures] Auto-assigned role '{role.name}' to {member.name}")
                except discord.Forbidden:
                    print(f"[VelcorFeatures] No permission to assign role {AUTO_ROLE_ID}")
                except Exception as e:
                    print(f"[VelcorFeatures] Error assigning role: {e}")
            else:
                print(f"[VelcorFeatures] Role {AUTO_ROLE_ID} not found in {guild.name}")

        # --- Welcome message ---
        if WELCOME_CHANNEL_ID:
            welcome_channel = guild.get_channel(WELCOME_CHANNEL_ID)
            if welcome_channel:
                description = f"Welcome to **{guild.name}**, **{member.display_name}**!\n\nWe're glad to have you here."
                if ABOUT_CHANNEL_ID:
                    description += f"\n\nLearn more about Velcorians here <#{ABOUT_CHANNEL_ID}>"
                embed = discord.Embed(
                    title="✨ New Member Joined!",
                    description=description,
                    color=0x2F3136,
                    timestamp=datetime.now(timezone.utc),
                )
                if member.avatar:
                    embed.set_thumbnail(url=member.avatar.url)
                file = None
                banner_set = False
                if WELCOME_BANNER_PATH:
                    if WELCOME_BANNER_PATH.startswith(("http://", "https://")):
                        embed.set_image(url=WELCOME_BANNER_PATH)
                        banner_set = True
                    elif os.path.exists(WELCOME_BANNER_PATH):
                        file = discord.File(WELCOME_BANNER_PATH, filename="banner.jpg")
                        embed.set_image(url="attachment://banner.jpg")
                        banner_set = True
                try:
                    # Send mention in content so user gets pinged; use display_name in embed
                    if file:
                        await welcome_channel.send(content=member.mention, file=file, embed=embed)
                    else:
                        await welcome_channel.send(content=member.mention, embed=embed)
                    print(f"[VelcorFeatures] Welcome message sent for {member.name} in #{welcome_channel.name}")
                except discord.Forbidden:
                    print(f"[VelcorFeatures] No permission in welcome channel {WELCOME_CHANNEL_ID}")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return
        if not message.guild:
            return

        guild_id = message.guild.id
        timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S")

        # Activity tracking
        try:
            update_user_activity(guild_id, message.author.id)
        except Exception as e:
            print(f"[{timestamp}] [VelcorFeatures] ERROR tracking activity: {e}")
            import traceback
            traceback.print_exc()

        # Engagement points. Runs off the event loop because the ledger does
        # blocking sqlite I/O and this handler fires on EVERY message in the
        # guild — awaiting it inline would throttle the bot under chat load.
        try:
            import asyncio as _asyncio

            import engagement

            _asyncio.create_task(_asyncio.to_thread(engagement.award_message, message))
        except Exception as e:
            print(f"[{timestamp}] [VelcorFeatures] ERROR awarding engagement: {e}")

        # Auto-reactions
        if AUTO_REACT_CHANNEL_ID and message.channel.id == AUTO_REACT_CHANNEL_ID:
            try:
                await message.add_reaction(_auto_react_emoji())
                print(f"[{timestamp}] ✅ Auto-reacted in #{message.channel.name}")
            except Exception as e:
                print(f"[{timestamp}] ⚠️ Failed auto-reaction: {e}")

        # Link blocking
        if guild_id not in self.guild_settings:
            self.guild_settings[guild_id] = {
                "delete_links": True,
                "delete_discord_invites": True,
                "delete_social_media": True,
                "delete_file_sharing": True,
                "delete_all_links": False,
                "allow_gifs": True,
                "whitelisted_channels": [],
                "protected_channels": [],
            }

        settings = self.guild_settings[guild_id]
        if message.channel.id not in settings.get("protected_channels", []):
            return

        if not message.channel.permissions_for(message.guild.me).manage_messages:
            return

        links_found = get_all_links(message.content)
        has_gif_media = (
            bool(message.attachments)
            or bool(message.embeds)
            or bool(getattr(message, "stickers", None))
        )
        if not links_found and not has_gif_media:
            return

        if should_allow_gif_message(message, links_found, settings):
            return

        blocked_links = []
        for link in links_found:
            link_lower = link.lower()
            if is_whitelisted_domain(link_lower):
                continue
            discord_match = re.search(DISCORD_INVITE_REGEX, link_lower)
            if discord_match:
                invite_code = discord_match.group(1)
                if invite_code.lower() in get_whitelisted_invites():
                    continue
                if settings.get("delete_discord_invites"):
                    blocked_links.append(f"Discord: {link}")
                continue
            if re.search(SOCIAL_MEDIA_REGEX, link_lower) and settings.get("delete_social_media"):
                blocked_links.append(f"Social: {link}")
                continue
            if re.search(FILE_SHARING_REGEX, link_lower) and settings.get("delete_file_sharing"):
                blocked_links.append(f"File: {link}")
                continue
            if re.search(URL_REGEX, link_lower) and settings.get("delete_links"):
                blocked_links.append(f"Link: {link}")
                continue

        if settings.get("delete_all_links"):
            blocked_links = [f"All: {link}" for link in links_found if not is_whitelisted_domain(link.lower())]

        if blocked_links and DELETE_MESSAGE:
            try:
                await message.delete()
                print(f"🚫 Deleted message from {message.author} in #{message.channel.name}")
                warning = discord.Embed(
                    title="🚫 Link Detected",
                    description=f"{message.author.mention}, links are not allowed in this channel!",
                    color=discord.Color.red(),
                )
                warning.add_field(
                    name="Links Removed",
                    value=f"```{'chr(10)'.join([link.split(': ', 1)[-1] for link in blocked_links[:3]])}```",
                    inline=False,
                )
                if len(blocked_links) > 3:
                    warning.add_field(name="Note", value=f"+ {len(blocked_links) - 3} more link(s)", inline=False)
                await message.channel.send(embed=warning, delete_after=10.0)
            except discord.Forbidden:
                try:
                    await message.channel.send(PERMISSION_MESSAGE, delete_after=10.0)
                except Exception:
                    pass
            except discord.NotFound:
                pass
            except Exception as e:
                print(f"⚠️ Link-block error: {e}")

    # ── Commands ───────────────────────────────────────────────────────────

    @commands.command(name="vping", help="Test Velcor3 features responsiveness")
    async def vping(self, ctx):
        await ctx.send(f"🏓 Velcor3 Pong! Latency: {round(self.bot.latency * 1000)}ms")

    @commands.command(
        name="testwallets",
        help="Post the latest NFT tx for tracked wallets as a test. Usage: !velcor3 testwallets [count]",
    )
    @commands.has_permissions(administrator=True)
    async def testwallets(self, ctx, count: int = 0):
        """Fetch each tracked wallet's most recent NFT transfer and post the real
        alert embed in THIS channel. Bypasses the live 'only new tx' filter so you
        can confirm the wallet tracker formatting / sending works end-to-end."""
        import aiohttp
        import wallet_database
        from trackers import eth_tracker

        # Make sure wallet labels are loaded.
        try:
            wallet_database.init_db()
            eth_tracker.tracked_eth_wallets.update(
                wallet_database.get_wallets_by_chain("ETH")
            )
        except Exception:
            pass

        wallets = list(eth_tracker.tracked_eth_wallets.items())
        if not wallets:
            await ctx.send("⚠️ No tracked ETH wallets found in the database.")
            return
        if count and count > 0:
            wallets = wallets[:count]

        ekey = eth_tracker.ETHSCAN_API_KEY
        if not ekey:
            await ctx.send("⚠️ ETHSCAN_API_KEY is missing — cannot fetch transactions.")
            return

        # Resolve the real wallet-tracker channel (same as the live tracker) so this
        # test confirms that exact channel works — not the channel you typed in.
        import config as _config
        import guild_license as _gl

        target_ids = []
        if getattr(_config, "DISCORD_NFT_CHANNEL_ID", 0):
            target_ids.append(int(_config.DISCORD_NFT_CHANNEL_ID))
        try:
            for cid in _gl.all_wallet_nft_channel_ids():
                if cid and int(cid) not in target_ids:
                    target_ids.append(int(cid))
        except Exception:
            pass

        target_channel = None
        for cid in target_ids:
            try:
                ch = self.bot.get_channel(cid) or await self.bot.fetch_channel(cid)
            except Exception:
                ch = None
            if ch:
                target_channel = ch
                break

        if target_channel is None:
            await ctx.send(
                "⚠️ No reachable wallet-tracker channel found "
                "(check DISCORD_NFT_CHANNEL_ID / bot permissions)."
            )
            return

        status = await ctx.send(
            f"🧪 Testing **{len(wallets)}** wallet(s) → posting to "
            f"#{getattr(target_channel, 'name', target_channel.id)}…"
        )

        sent = 0
        no_tx = 0
        errors = 0
        async with aiohttp.ClientSession() as session:
            for wallet, label in wallets:
                try:
                    url = (
                        f"https://api.etherscan.io/v2/api?chainid=1&module=account"
                        f"&action=tokennfttx&address={wallet}&page=1&offset=1&sort=desc&apikey={ekey}"
                    )
                    d, err0 = await eth_tracker._etherscan_fetch_json(
                        session, url, "tokennfttx", wallet
                    )
                    rows, err = eth_tracker._etherscan_account_tx_rows(d) if d else (None, err0)
                    if err or not rows:
                        no_tx += 1
                        continue
                    # Build the same grouped embed the live tracker sends, newest only.
                    groups = eth_tracker._group_erc721_transactions(rows, wallet)
                    if not groups:
                        no_tx += 1
                        continue
                    group = groups[0]
                    txs = group["txs"]
                    first = txs[0]
                    action_type = "Sent" if group["kind"] == "sell" else "Received"
                    embed, content, view, files = await eth_tracker.create_eth_nft_embed(
                        action_type=action_type,
                        wallet=wallet,
                        contract=first["contractAddress"],
                        session=session,
                        tx_hash=first["hash"],
                        token_id=int(first["tokenID"]),
                        from_addr=first["from"],
                        to_addr=first["to"],
                        bulk_quantity=len(txs) if len(txs) > 1 else None,
                        all_token_ids=[int(t["tokenID"]) for t in txs] if len(txs) > 1 else None,
                    )
                    await eth_tracker._safe_discord_send(
                        target_channel, content=content, embed=embed, view=view, files=files
                    )
                    sent += 1
                except Exception as e:
                    errors += 1
                    print(f"[testwallets] {wallet[:10]}… failed: {e}")
                await asyncio.sleep(0.5)

        await status.edit(
            content=(
                f"✅ Test done — sent **{sent}** embed(s) · "
                f"{no_tx} wallet(s) with no NFT tx · {errors} error(s)."
            )
        )

    @commands.command(name="test_welcome", help="Manually trigger welcome + auto-role for a user (Admin only)")
    @commands.has_permissions(administrator=True)
    async def test_welcome(self, ctx, member: discord.Member = None):
        if member is None:
            member = ctx.author
        await ctx.send(f"🧪 Testing welcome + auto-role for {member.mention}...")
        # Manually fire the on_member_join logic
        await self.on_member_join(member)
        await ctx.send("✅ Test complete. Check bot logs for details.")

    @commands.command(name="setup", help="Protect current channel from links (Admin only)")
    @commands.has_permissions(administrator=True)
    async def setup_protection(self, ctx):
        guild_id = ctx.guild.id
        if guild_id not in self.guild_settings:
            self.guild_settings[guild_id] = {
                "delete_links": True,
                "delete_discord_invites": True,
                "delete_social_media": True,
                "delete_file_sharing": True,
                "delete_all_links": False,
                "allow_gifs": True,
                "whitelisted_channels": [],
                "protected_channels": [],
            }
        if ctx.channel.id not in self.guild_settings[guild_id]["protected_channels"]:
            self.guild_settings[guild_id]["protected_channels"].append(ctx.channel.id)
            save_guild_settings(guild_id, self.guild_settings[guild_id])
            embed = discord.Embed(
                title="✅ Channel Protected",
                description=f"**{ctx.channel.mention}** is now protected against links!",
                color=discord.Color.green(),
            )
            embed.add_field(
                name="What's blocked",
                value="• Discord invites\n• Social media links\n• File sharing links\n• General URLs",
                inline=False,
            )
            embed.add_field(
                name="Allowed",
                value="• GIFs (Tenor / Giphy / Discord picker) when **Allow GIFs** is on",
                inline=False,
            )
            await ctx.send(embed=embed)
        else:
            await ctx.send("⚠️ This channel is already protected!")

    @commands.command(name="remove_protection", help="Remove link protection from current channel (Admin only)")
    @commands.has_permissions(administrator=True)
    async def remove_protection(self, ctx):
        guild_id = ctx.guild.id
        if guild_id in self.guild_settings and ctx.channel.id in self.guild_settings[guild_id]["protected_channels"]:
            self.guild_settings[guild_id]["protected_channels"].remove(ctx.channel.id)
            save_guild_settings(guild_id, self.guild_settings[guild_id])
            await ctx.send(f"✅ **{ctx.channel.mention}** is no longer protected.")
        else:
            await ctx.send("⚠️ This channel is not protected.")

    @commands.command(name="whitelist_domain", help="Add a domain to whitelist (Admin only)")
    @commands.has_permissions(administrator=True)
    async def whitelist_domain(self, ctx, domain: str):
        domain = domain.lower().strip()
        if domain.startswith("http://") or domain.startswith("https://"):
            domain = domain.split("//", 1)[1]
        if "/" in domain:
            domain = domain.split("/", 1)[0]
        db_add_whitelisted_domain(domain)
        await ctx.send(f"✅ `**{domain}**` added to whitelist.")

    @commands.command(name="unwhitelist_domain", help="Remove a domain from whitelist (Admin only)")
    @commands.has_permissions(administrator=True)
    async def unwhitelist_domain(self, ctx, domain: str):
        domain = domain.lower().strip()
        db_remove_whitelisted_domain(domain)
        await ctx.send(f"✅ `**{domain}**` removed from whitelist.")

    @commands.command(name="settings", help="Show current protection settings (Admin only)")
    @commands.has_permissions(administrator=True)
    async def show_settings(self, ctx):
        guild_id = ctx.guild.id
        settings = self.guild_settings.get(guild_id, {
            "delete_links": True,
            "delete_discord_invites": True,
            "delete_social_media": True,
            "delete_file_sharing": True,
            "delete_all_links": False,
            "allow_gifs": True,
            "protected_channels": [],
        })
        protected = settings.get("protected_channels", [])
        protected_mentions = "\n".join([f"<#{cid}>" for cid in protected]) if protected else "None"
        domains = get_whitelisted_domains()
        invites = get_whitelisted_invites()

        embed = discord.Embed(title="🛡️ Protection Settings", color=discord.Color.blue())
        embed.add_field(name="Discord Invites", value="🟢 ON" if settings.get("delete_discord_invites") else "🔴 OFF", inline=True)
        embed.add_field(name="Social Media", value="🟢 ON" if settings.get("delete_social_media") else "🔴 OFF", inline=True)
        embed.add_field(name="File Sharing", value="🟢 ON" if settings.get("delete_file_sharing") else "🔴 OFF", inline=True)
        embed.add_field(name="General Links", value="🟢 ON" if settings.get("delete_links") else "🔴 OFF", inline=True)
        embed.add_field(name="Delete ALL Links", value="🟢 ON" if settings.get("delete_all_links") else "🔴 OFF", inline=True)
        embed.add_field(name="Allow GIFs", value="🟢 ON" if settings.get("allow_gifs", True) else "🔴 OFF", inline=True)
        embed.add_field(name="Protected Channels", value=protected_mentions, inline=False)
        embed.add_field(name="Whitelisted Domains", value="\n".join(domains) if domains else "None", inline=False)
        embed.add_field(name="Whitelisted Invites", value="\n".join(invites) if invites else "None", inline=False)
        await ctx.send(embed=embed)

    @commands.command(name="toggle_all", help="Toggle deletion of ALL links (Admin only)")
    @commands.has_permissions(administrator=True)
    async def toggle_all(self, ctx):
        guild_id = ctx.guild.id
        if guild_id not in self.guild_settings:
            self.guild_settings[guild_id] = {
                "delete_links": True,
                "delete_discord_invites": True,
                "delete_social_media": True,
                "delete_file_sharing": True,
                "delete_all_links": False,
                "allow_gifs": True,
                "whitelisted_channels": [],
                "protected_channels": [],
            }
        self.guild_settings[guild_id]["delete_all_links"] = not self.guild_settings[guild_id]["delete_all_links"]
        save_guild_settings(guild_id, self.guild_settings[guild_id])
        status = "🟢 ON" if self.guild_settings[guild_id]["delete_all_links"] else "🔴 OFF"
        await ctx.send(f"Delete ALL links is now **{status}**")

    @commands.command(name="toggle_gifs", help="Toggle allowing GIFs in protected channels (Admin only)")
    @commands.has_permissions(administrator=True)
    async def toggle_gifs(self, ctx):
        guild_id = ctx.guild.id
        if guild_id not in self.guild_settings:
            self.guild_settings[guild_id] = {
                "delete_links": True,
                "delete_discord_invites": True,
                "delete_social_media": True,
                "delete_file_sharing": True,
                "delete_all_links": False,
                "allow_gifs": True,
                "whitelisted_channels": [],
                "protected_channels": [],
            }
        cur = self.guild_settings[guild_id].get("allow_gifs", True)
        self.guild_settings[guild_id]["allow_gifs"] = not cur
        save_guild_settings(guild_id, self.guild_settings[guild_id])
        status = "🟢 ON" if self.guild_settings[guild_id]["allow_gifs"] else "🔴 OFF"
        await ctx.send(
            f"Allow GIFs in protected channels is now **{status}** "
            f"(Tenor, Giphy, Discord GIF picker, `.gif` uploads)."
        )

    @commands.command(name="test_link", help="Test if a link would be blocked")
    async def test_link(self, ctx, *, link: str):
        link_lower = link.lower()
        if is_gif_url(link_lower):
            await ctx.send(f"✅ GIF link — would **NOT** be blocked when **Allow GIFs** is on.")
            return
        if is_whitelisted_domain(link_lower):
            await ctx.send(f"✅ `**{link}**` is whitelisted — would **NOT** be blocked.")
            return
        discord_match = re.search(DISCORD_INVITE_REGEX, link_lower)
        if discord_match:
            code = discord_match.group(1)
            if code.lower() in get_whitelisted_invites():
                await ctx.send(f"✅ Discord invite `**{code}**` is whitelisted.")
                return
            await ctx.send(f"🚫 Discord invite `**{code}**` would be **BLOCKED**.")
            return
        if re.search(SOCIAL_MEDIA_REGEX, link_lower):
            await ctx.send(f"🚫 Social media link would be **BLOCKED**.")
            return
        if re.search(FILE_SHARING_REGEX, link_lower):
            await ctx.send(f"🚫 File sharing link would be **BLOCKED**.")
            return
        if re.search(URL_REGEX, link_lower):
            await ctx.send(f"🚫 General link would be **BLOCKED**.")
            return
        await ctx.send("ℹ️ No link detected in the provided text.")

    @commands.command(name="list_domains", help="List all whitelisted domains")
    async def list_domains(self, ctx):
        domains = get_whitelisted_domains()
        if domains:
            await ctx.send(f"📋 **Whitelisted Domains:**\n" + "\n".join(f"• `{d}`" for d in domains))
        else:
            await ctx.send("📋 No whitelisted domains.")

    @commands.command(name="scan", help="Scan last N messages for links (Admin only)")
    @commands.has_permissions(administrator=True)
    async def scan(self, ctx, limit: int = 10):
        if limit > 100:
            limit = 100
        deleted = 0
        async for msg in ctx.channel.history(limit=limit):
            if msg.author.bot:
                continue
            links = get_all_links(msg.content)
            if links:
                try:
                    await msg.delete()
                    deleted += 1
                except Exception:
                    pass
        await ctx.send(f"✅ Scanned {limit} messages — deleted **{deleted}** containing links.")


    @commands.command(name="velcor", help="Show all Velcor3 feature commands")
    async def velcor_help(self, ctx):
        embed = discord.Embed(
            title="🛡️ Velcor3 Features — Help",
            description="Community & moderation commands",
            color=discord.Color.blue(),
        )
        general = [
            "`!velcor3 setup` — Protect current channel",
            "`!velcor3 remove_protection` — Remove protection",
            "`!velcor3 settings` — Show settings",
            "`!velcor3 toggle_all` — Toggle all-link deletion",
            "`!velcor3 toggle_gifs` — Allow/block GIFs in protected channels",
            "`!velcor3 whitelist_domain <domain>` — Add whitelist",
            "`!velcor3 list_domains` — List whitelist",
            "`!velcor3 test_link <link>` — Test link blocker",
            "`!velcor3 scan [limit]` — Scan recent messages",
            "`!velcor3 vping` — Check latency",
        ]
        stats = [
            "`!velcor3 scan_history [days] [mode]` — Scan history",
            "`!velcor3 scan_status` — Check database status",
            "`!velcor3 userstats [@user]` — User stats",
            "`!velcor3 inactive [days]` — List inactive users",
            "`!velcor3 serverstats` — Server stats",
            "`!velcor3 purge_list [days] export` — Purge list",
            "`!velcor3 top_active [limit]` — Top active users",
            "`!velcor3 allusers [page]` — All users",
            "`!velcor3 users_by_status [status]` — Users by status",
        ]
        roles = [
            "`!velcor3 batchrole @Role <ids>` — Batch assign role",
            "`!velcor3 role_roleless @Role` — Assign to roleless",
            "`!velcor3 addpingrole @Role emoji for WL raffle/giveaways` — Add claim-role reaction",
            "`!velcor3 setpingroledesc @Role description:...` — Update role line text",
            "`!velcor3 removepingrole @Role` — Remove claim-role reaction",
            "`!velcor3 pingroles` — Post claim-roles dropdown panel (here)",
            "`!velcor3 post_claim_roles` — Post claim-roles dropdown panel (Manage Server)",
        ]
        embed.add_field(name="🛡️ General", value="\n".join(general), inline=False)
        embed.add_field(name="📈 Stats", value="\n".join(stats), inline=False)
        embed.add_field(name="🎭 Roles", value="\n".join(roles), inline=False)
        embed.set_footer(text="Admin permissions required for most commands")
        await ctx.send(embed=embed)

    @commands.command(name="scan_history", help="Scan channel history. Usage: !velcor3 scan_history [days] [mode: normal/repair]")
    @commands.has_permissions(administrator=True)
    async def scan_history(self, ctx, days: int = None, mode: str = "normal"):
        guild = ctx.guild
        guild_id = guild.id
        if self.scan_in_progress.get(guild_id, False):
            await ctx.send("⚠️ A scan is already in progress.")
            return
        self.scan_in_progress[guild_id] = True
        await ctx.send("🚀 Starting historical scan… This may take a while.")

        try:
            total_messages = 0
            user_data = defaultdict(int)
            daily_data = defaultdict(int)
            after_date = None
            if days and days > 0:
                after_date = datetime.now(timezone.utc) - timedelta(days=days)

            for channel in guild.text_channels:
                try:
                    perms = channel.permissions_for(guild.me)
                    if not perms.read_message_history or not perms.view_channel:
                        continue
                    async for msg in channel.history(limit=None, after=after_date):
                        if msg.author.bot:
                            continue
                        total_messages += 1
                        user_data[msg.author.id] += 1
                        day = msg.created_at.strftime("%Y-%m-%d")
                        daily_data[(msg.author.id, day)] += 1
                except Exception as e:
                    print(f"[VelcorFeatures] Scan error in #{channel.name}: {e}")

            if mode.lower() == "repair":
                bulk_update_activity(guild_id, user_data, daily_data)
            else:
                bulk_update_activity(guild_id, user_data, daily_data)

            await ctx.send(f"✅ Scan complete! Processed **{total_messages}** messages across all channels.")
        finally:
            self.scan_in_progress[guild_id] = False

    @commands.command(name="scan_status", help="Check scan status for this server (Admin only)")
    @commands.has_permissions(administrator=True)
    async def scan_status(self, ctx):
        status = get_scan_status(ctx.guild.id)
        if not status:
            await ctx.send("📊 No scan history found.")
            return
        embed = discord.Embed(title="📊 Scan Status", color=discord.Color.blue())
        total = 0
        for cid, info in list(status.items())[:20]:
            ch = ctx.guild.get_channel(cid)
            name = ch.mention if ch else f"`{cid}`"
            embed.add_field(
                name=name,
                value=f"Scanned: {info['messages_scanned']}\nLast: {info['last_scanned'][:19]}",
                inline=True,
            )
            total += info["messages_scanned"]
        embed.set_footer(text=f"Total messages scanned: {total}")
        await ctx.send(embed=embed)

    @commands.command(name="userstats", help="Show stats for a specific user (Admin only)")
    @commands.has_permissions(administrator=True)
    async def userstats(self, ctx, member: discord.Member = None):
        if member is None:
            member = ctx.author
        guild_id = ctx.guild.id
        days_in_server = (datetime.now(timezone.utc) - member.joined_at.replace(tzinfo=timezone.utc)).days if member.joined_at else 0
        account_age = (datetime.now(timezone.utc) - member.created_at.replace(tzinfo=timezone.utc)).days
        stats = get_user_stats(guild_id, member.id)
        msgs_7d = get_stats_window(guild_id, member.id, 7)
        msgs_30d = get_stats_window(guild_id, member.id, 30)
        last_seen = stats["last_seen"]
        if last_seen:
            inactive_days = (datetime.now(timezone.utc) - last_seen).days
            last_active_str = last_seen.strftime("%Y-%m-%d %H:%M UTC")
        else:
            inactive_days = days_in_server
            last_active_str = "Never recorded"
        first_seen = stats["first_seen"]
        first_seen_str = first_seen.strftime("%Y-%m-%d %H:%M UTC") if first_seen else "Never recorded"

        status_emoji = {
            discord.Status.online: "🟢 Online",
            discord.Status.idle: "🟡 Idle",
            discord.Status.dnd: "🔴 Do Not Disturb",
            discord.Status.offline: "⚫ Offline",
        }
        current_status = status_emoji.get(member.status, "⚫ Unknown")

        invites_created = 0
        total_invite_uses = 0
        try:
            for inv in await ctx.guild.invites():
                if inv.inviter and inv.inviter.id == member.id:
                    invites_created += 1
                    total_invite_uses += inv.uses or 0
        except Exception:
            pass

        roles = [role.mention for role in member.roles[1:]]
        roles_str = ", ".join(roles[:10]) if roles else "No roles"
        if len(roles) > 10:
            roles_str += f" (+{len(roles)-10} more)"

        embed = discord.Embed(
            title=f"📊 User Stats: {member.display_name}",
            color=member.color if member.color != discord.Color.default() else discord.Color.blue(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(
            name="📅 Server Activity",
            value=(
                f"**Days in Server:** {days_in_server:,}\n"
                f"**Messages (Total):** {stats['message_count']:,}\n"
                f"**Messages (7d):** {msgs_7d:,}\n"
                f"**Messages (30d):** {msgs_30d:,}\n"
                f"**Last Message:** {last_active_str}\n"
                f"**Inactive Days:** {inactive_days}"
            ),
            inline=False,
        )
        embed.add_field(
            name="👤 Account Info",
            value=(
                f"**Account Age:** {account_age:,} days\n"
                f"**Status:** {current_status}\n"
                f"**Bot:** {'Yes' if member.bot else 'No'}\n"
                f"**Boosting:** {'Yes' if member.premium_since else 'No'}"
            ),
            inline=False,
        )
        embed.add_field(name=f"🎭 Roles ({len(roles)})", value=roles_str, inline=False)
        if inactive_days >= 30:
            embed.add_field(name="⚠️ Warning", value=f"User has been inactive for **{inactive_days} days**!", inline=False)
        embed.set_footer(text=f"User ID: {member.id}")
        if WELCOME_BANNER_PATH and os.path.exists(WELCOME_BANNER_PATH):
            file = discord.File(WELCOME_BANNER_PATH, filename="banner.jpg")
            embed.set_image(url="attachment://banner.jpg")
            await ctx.send(file=file, embed=embed)
        else:
            await ctx.send(embed=embed)

    @commands.command(name="batchrole", help="Assign a role to a batch of users (Admin only). Attach a file or list IDs.")
    @commands.has_permissions(administrator=True)
    async def batchrole(self, ctx, role: discord.Role, *, user_ids: str = None):
        ids = []
        if ctx.message.attachments:
            for att in ctx.message.attachments:
                if att.filename.endswith(".txt"):
                    try:
                        content = await att.read()
                        for line in content.decode("utf-8").splitlines():
                            line = line.strip()
                            if line.isdigit():
                                ids.append(int(line))
                    except Exception as e:
                        await ctx.send(f"⚠️ Error reading attachment: {e}")
                        return
        if user_ids:
            for part in user_ids.replace(",", " ").split():
                p = part.strip()
                if p.isdigit():
                    ids.append(int(p))
        if not ids:
            await ctx.send("⚠️ Provide user IDs or attach a .txt file.")
            return

        success = 0
        failed = 0
        for uid in ids:
            member = ctx.guild.get_member(uid)
            if not member:
                failed += 1
                continue
            try:
                await member.add_roles(role)
                success += 1
            except Exception:
                failed += 1
        await ctx.send(f"✅ Assigned **{role.name}** to **{success}** users. Failed: **{failed}**.")

    @commands.command(name="role_roleless", help="Assign a role to ALL users with NO roles (Admin only)")
    @commands.has_permissions(administrator=True)
    async def role_roleless(self, ctx, role: discord.Role):
        count = 0
        for member in ctx.guild.members:
            if not member.bot and len(member.roles) == 1:
                try:
                    await member.add_roles(role)
                    count += 1
                except Exception:
                    pass
        await ctx.send(f"✅ Assigned **{role.name}** to **{count}** roleless users.")

    @commands.command(name="inactive", help="List inactive users (Admin only)")
    @commands.has_permissions(administrator=True)
    async def inactive(self, ctx, days: int = 30):
        if days < 1:
            days = 1
        if days > 365:
            days = 365
        guild_id = ctx.guild.id
        now = datetime.now(timezone.utc)
        all_stats = get_all_user_stats(guild_id)
        inactive_users = []
        for member in ctx.guild.members:
            if member.bot:
                continue
            user_data = all_stats.get(member.id, {})
            last_seen = user_data.get("last_seen")
            status_note = ""
            if last_seen:
                inactive_since = (now - last_seen).days
            else:
                if member.joined_at:
                    inactive_since = (now - member.joined_at.replace(tzinfo=timezone.utc)).days
                    status_note = " (No Data)"
                else:
                    inactive_since = 999
                    status_note = " (?)"
            if inactive_since >= days:
                inactive_users.append({
                    "member": member,
                    "days": inactive_since,
                    "messages": user_data.get("message_count", 0),
                    "status": status_note,
                })
        inactive_users.sort(key=lambda x: x["days"], reverse=True)
        if not inactive_users:
            await ctx.send(f"✅ No users inactive for {days}+ days!")
            return
        embed = discord.Embed(
            title=f"💤 Inactive Users ({days}+ days)",
            description=f"Found **{len(inactive_users)}** inactive members",
            color=discord.Color.orange(),
            timestamp=now,
        )
        user_list = ""
        for i, data in enumerate(inactive_users[:15], 1):
            user_list += f"**{i}.** {data['member'].mention} - {data['days']} days{data['status']} (📧 {data['messages']} msgs)\n"
        if len(inactive_users) > 15:
            user_list += f"\n*… and {len(inactive_users) - 15} more*"
        embed.add_field(name="Most Inactive Members", value=user_list, inline=False)
        embed.set_footer(text=f"Use !velcor3 purge_list {days} for full list")
        await ctx.send(embed=embed)

    @commands.command(name="serverstats", help="Show server activity statistics (Admin only)")
    @commands.has_permissions(administrator=True)
    async def serverstats(self, ctx):
        guild = ctx.guild
        guild_id = guild.id
        now = datetime.now(timezone.utc)
        all_stats = get_all_user_stats(guild_id)
        total_members = len(guild.members)
        bot_count = sum(1 for m in guild.members if m.bot)
        human_count = total_members - bot_count
        online = sum(1 for m in guild.members if m.status == discord.Status.online and not m.bot)
        idle = sum(1 for m in guild.members if m.status == discord.Status.idle and not m.bot)
        dnd = sum(1 for m in guild.members if m.status == discord.Status.dnd and not m.bot)
        offline = sum(1 for m in guild.members if m.status == discord.Status.offline and not m.bot)

        inactive_7 = inactive_14 = inactive_30 = inactive_60 = inactive_90 = 0
        for member in guild.members:
            if member.bot:
                continue
            user_data = all_stats.get(member.id, {})
            last_seen = user_data.get("last_seen")
            days = (now - last_seen).days if last_seen else 0
            if days >= 90:
                inactive_90 += 1
            elif days >= 60:
                inactive_60 += 1
            elif days >= 30:
                inactive_30 += 1
            elif days >= 14:
                inactive_14 += 1
            elif days >= 7:
                inactive_7 += 1

        total_messages = sum(d.get("message_count", 0) for d in all_stats.values())
        active_users = len([uid for uid, d in all_stats.items() if d.get("message_count", 0) > 0])

        embed = discord.Embed(title=f"📊 Server Stats: {guild.name}", color=discord.Color.blue(), timestamp=now)
        embed.set_thumbnail(url=guild.icon.url if guild.icon else None)
        embed.add_field(name="👥 Members", value=f"**Total:** {total_members}\n**Humans:** {human_count}\n**Bots:** {bot_count}", inline=True)
        embed.add_field(name="🚦 Status", value=f"🟢 **Online:** {online}\n🟡 **Idle:** {idle}\n🔴 **DND:** {dnd}\n⚫ **Offline:** {offline}", inline=True)
        embed.add_field(name="📧 Activity", value=f"**Messages:** {total_messages:,}\n**Tracked:** {active_users:,}/{human_count}", inline=True)
        total_inactive = inactive_7 + inactive_14 + inactive_30 + inactive_60 + inactive_90
        embed.add_field(
            name="💤 Inactivity Breakdown",
            value=(
                f"**7-13 days:** {inactive_7:,}\n"
                f"**14-29 days:** {inactive_14:,}\n"
                f"**30-59 days:** {inactive_30:,}\n"
                f"**60-89 days:** {inactive_60:,}\n"
                f"**90+ days:** {inactive_90:,}\n"
                f"───────────\n"
                f"**Total Inactive (7+ days):** {total_inactive:,}"
            ),
            inline=False,
        )
        if inactive_30 + inactive_60 + inactive_90 > 0:
            embed.add_field(
                name="🧹 Purge Recommendation",
                value=f"**{inactive_30 + inactive_60 + inactive_90}** users inactive 30+ days\nUse `!velcor3 purge_list 30`",
                inline=False,
            )
        await ctx.send(embed=embed)

    @commands.command(name="invites", help="Show invite stats for a user")
    async def invites(self, ctx, member: discord.Member = None):
        if member is None:
            member = ctx.author
        total, successful = get_invites(ctx.guild.id, member.id)
        embed = discord.Embed(title=f"📨 Invite Stats: {member.display_name}", color=discord.Color.blue())
        embed.add_field(name="Total Invites", value=str(total), inline=True)
        embed.add_field(name="Successful", value=str(successful), inline=True)
        await ctx.send(embed=embed)

    @commands.command(name="role_activity", help="Show activity for a specific role")
    @commands.has_permissions(administrator=True)
    async def role_activity(self, ctx, role: discord.Role):
        guild_id = ctx.guild.id
        all_stats = get_all_user_stats(guild_id)
        total = 0
        active = 0
        msgs = 0
        for member in role.members:
            if member.bot:
                continue
            total += 1
            data = all_stats.get(member.id, {})
            if data.get("message_count", 0) > 0:
                active += 1
            msgs += data.get("message_count", 0)
        embed = discord.Embed(title=f"📊 Activity for {role.name}", color=role.color if role.color != discord.Color.default() else discord.Color.blue())
        embed.add_field(name="Members", value=str(total), inline=True)
        embed.add_field(name="Active (messaged)", value=str(active), inline=True)
        embed.add_field(name="Total Messages", value=str(msgs), inline=True)
        await ctx.send(embed=embed)

    @commands.command(name="purge_list", help="Generate a list of inactive users to purge (Admin only)")
    @commands.has_permissions(administrator=True)
    async def purge_list(self, ctx, days: int = 30, export: str = None):
        if days < 7:
            await ctx.send("⚠️ Minimum inactivity period is 7 days.")
            return
        guild_id = ctx.guild.id
        now = datetime.now(timezone.utc)
        all_stats = get_all_user_stats(guild_id)
        candidates = []
        for member in ctx.guild.members:
            if member.bot:
                continue
            if member.guild_permissions.administrator or member.guild_permissions.manage_guild:
                continue
            user_data = all_stats.get(member.id, {})
            last_seen = user_data.get("last_seen")
            if last_seen:
                inactive_days = (now - last_seen).days
            else:
                if member.joined_at:
                    inactive_days = (now - member.joined_at.replace(tzinfo=timezone.utc)).days
                else:
                    inactive_days = 999
            if inactive_days >= days:
                candidates.append({
                    "member": member,
                    "days": inactive_days,
                    "messages": user_data.get("message_count", 0),
                    "joined": member.joined_at.strftime("%Y-%m-%d") if member.joined_at else "Unknown",
                })
        candidates.sort(key=lambda x: x["days"], reverse=True)
        if not candidates:
            await ctx.send(f"✅ No users meet the {days}-day inactivity criteria.")
            return
        embed = discord.Embed(
            title=f"🧹 Purge Candidates ({days}+ days)",
            description=f"Found **{len(candidates)}** members",
            color=discord.Color.red(),
            timestamp=now,
        )
        text = ""
        for i, data in enumerate(candidates[:20], 1):
            m = data["member"]
            text += f"`{i:02d}.` {m.name} | {data['days']}d | {data['messages']} msgs\n"
        if len(candidates) > 20:
            text += f"\n*… and {len(candidates) - 20} more*"
        embed.add_field(name="📋 Candidates", value=text, inline=False)
        super_inactive = len([c for c in candidates if c["days"] >= 90])
        embed.add_field(
            name="📊 Summary",
            value=f"**Total:** {len(candidates)}\n**90+ days:** {super_inactive}\n**Avg:** {sum(c['days'] for c in candidates)//len(candidates)} days",
            inline=True,
        )
        embed.add_field(
            name="⚠️ Warning",
            value="This is a list only. No users have been removed.",
            inline=False,
        )
        embed.set_footer(text="Excludes: Bots, Admins, Mods")
        await ctx.send(embed=embed)

        if export and export.lower() == "export":
            export_text = f"PURGE CANDIDATES — {days}+ DAYS INACTIVE\nGenerated: {now.strftime('%Y-%m-%d %H:%M UTC')}\nServer: {ctx.guild.name}\n{'='*60}\n\n"
            for i, data in enumerate(candidates, 1):
                m = data["member"]
                export_text += f"{i}. {m.name} (ID: {m.id})\n   Inactive: {data['days']} days | Messages: {data['messages']} | Joined: {data['joined']}\n\n"
            file = discord.File(io.StringIO(export_text), filename=f"purge_list_{ctx.guild.id}_{days}days.txt")
            await ctx.send("📄 **Full export:**", file=file)

    @commands.command(name="top_active", help="Show most active users (Admin only)")
    @commands.has_permissions(administrator=True)
    async def top_active(self, ctx, limit: int = 10):
        if limit < 1:
            limit = 10
        if limit > 50:
            limit = 50
        guild_id = ctx.guild.id
        all_stats = get_all_user_stats(guild_id)
        ranked = sorted(all_stats.items(), key=lambda x: x[1].get("message_count", 0), reverse=True)[:limit]
        embed = discord.Embed(title=f"🏆 Top Active Users", color=discord.Color.gold(), timestamp=datetime.now(timezone.utc))
        lines = []
        for i, (uid, data) in enumerate(ranked, 1):
            member = ctx.guild.get_member(uid)
            name = member.mention if member else f"`{uid}`"
            lines.append(f"**{i}.** {name} — {data.get('message_count', 0):,} msgs")
        embed.description = "\n".join(lines) if lines else "No activity data yet."
        await ctx.send(embed=embed)

    @commands.command(name="allusers", help="Show all users in the server (Admin only)")
    @commands.has_permissions(administrator=True)
    async def allusers(self, ctx, page: int = 1):
        members = [m for m in ctx.guild.members if not m.bot]
        per_page = 20
        total_pages = max(1, (len(members) + per_page - 1) // per_page)
        if page < 1:
            page = 1
        if page > total_pages:
            page = total_pages
        start = (page - 1) * per_page
        end = start + per_page
        chunk = members[start:end]
        embed = discord.Embed(
            title=f"👥 All Users — Page {page}/{total_pages}",
            description=f"Total: **{len(members)}** humans",
            color=discord.Color.blue(),
        )
        for m in chunk:
            embed.add_field(
                name=f"{m.name}",
                value=f"ID: `{m.id}` | Status: {str(m.status).title()}",
                inline=True,
            )
        embed.set_footer(text=f"Use !velcor3 allusers <page>")
        await ctx.send(embed=embed)

    @commands.command(name="users_by_status", help="Show users filtered by status (Admin only)")
    @commands.has_permissions(administrator=True)
    async def users_by_status(self, ctx, status: str = "all"):
        status = status.lower()
        mapping = {
            "online": discord.Status.online,
            "idle": discord.Status.idle,
            "dnd": discord.Status.dnd,
            "offline": discord.Status.offline,
        }
        if status in mapping:
            members = [m for m in ctx.guild.members if m.status == mapping[status] and not m.bot]
            title = f"{status.title()} Users"
        else:
            members = [m for m in ctx.guild.members if not m.bot]
            title = "All Users by Status"
        embed = discord.Embed(title=f"👥 {title}", description=f"Count: **{len(members)}**", color=discord.Color.blue())
        for m in members[:25]:
            embed.add_field(name=m.name, value=f"ID: `{m.id}` | {str(m.status).title()}", inline=True)
        if len(members) > 25:
            embed.set_footer(text=f"… and {len(members)-25} more")
        await ctx.send(embed=embed)


    # ── Ping Roles ─────────────────────────────────────────────────────────

    @commands.command(name="addpingrole", help="Add claim-role reaction(s). Paste multiple lines at once.")
    @commands.has_permissions(administrator=True)
    async def addpingrole(
        self,
        ctx,
        role: discord.Role = None,
        emoji: str = None,
    ):
        """
        One role per line when pasting a batch, e.g.:
        !velcor3 addpingrole @WL Realm 🎫 for WL raffle/giveaways
        !velcor3 addpingrole @Alpha Realm 🚀 for alpha plays
        """
        parsed = parse_addpingrole_lines(ctx.message.content or "", ctx.guild)

        if len(parsed) > 1:
            added: list[str] = []
            skipped: list[str] = []
            for row in parsed:
                dup = emoji_already_used(ctx.guild.id, row["emoji"], ctx.guild)
                if dup:
                    skipped.append(f"{row['label']} (emoji used by {dup})")
                    continue
                save_ping_role(
                    ctx.guild.id,
                    row["role_id"],
                    row["label"],
                    row["emoji"],
                    row["description"],
                )
                added.append(
                    f"• {row['role'].mention} — {row['description']}"
                    if row["description"]
                    else f"• {row['role'].mention}"
                )
            lines = []
            if added:
                lines.append(f"✅ **Added {len(added)} reaction role(s):**\n" + "\n".join(added))
            if skipped:
                lines.append(
                    "⚠️ **Skipped** (duplicate emoji — use a different emoji per role):\n"
                    + "\n".join(f"• {s}" for s in skipped)
                )
            await ctx.send("\n\n".join(lines) or "Nothing added.")
            return

        if len(parsed) == 1:
            row = parsed[0]
            role = row["role"]
            emoji = row["emoji"]
            description = row["description"]
        else:
            if role is None or emoji is None:
                await ctx.send(
                    "Usage: `!velcor3 addpingrole @Role 🎫 short description`\n"
                    "Or paste **one command per line** to add several roles at once."
                )
                return
            description = _pingrole_description_from_message(
                ctx.message.content or "", role, emoji
            )

        dup = emoji_already_used(ctx.guild.id, emoji, ctx.guild)
        if dup:
            await ctx.send(
                f"⚠️ That emoji is already used for **{dup}**. "
                "Each role needs a **unique** reaction emoji."
            )
            return
        save_ping_role(ctx.guild.id, role.id, role.name.strip(), emoji, description)
        preview = f"• {role.mention} — {description}" if description else f"• {role.mention}"
        await ctx.send(f"✅ Added reaction role\n{preview}")

    @commands.command(name="listpingroles", help="List configured claim-role reactions (Admin only)")
    @commands.has_permissions(administrator=True)
    async def listpingroles(self, ctx):
        roles = load_ping_roles(ctx.guild.id)
        if not roles:
            await ctx.send("No claim roles configured. Use `!velcor3 addpingrole`.")
            return
        lines = []
        for row in roles:
            r = ctx.guild.get_role(int(row["role_id"]))
            mention = r.mention if r else f"<@&{row['role_id']}>"
            em = format_role_emoji_display(ctx.guild, row["emoji"])
            desc = (row.get("description") or row.get("label") or "").strip()
            lines.append(f"{em} {mention} — `{row['emoji']}` — {desc}")
        await ctx.send("**Claim roles:**\n" + "\n".join(lines))

    @commands.command(name="setpingroledesc", help="Update a claim-role line description (Admin only)")
    @commands.has_permissions(administrator=True)
    async def setpingroledesc(self, ctx, role: discord.Role, *, description: str):
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE ping_roles SET description = ? WHERE guild_id = ? AND role_id = ?",
            (description.strip(), ctx.guild.id, role.id),
        )
        if cursor.rowcount == 0:
            conn.close()
            await ctx.send(f"⚠️ No ping role configured for {role.mention}. Use `addpingrole` first.")
            return
        conn.commit()
        conn.close()
        await ctx.send(f"✅ Updated {role.mention} — {description.strip()}")

    @commands.command(name="removepingrole", help="Remove a claim-role reaction (Admin only)")
    @commands.has_permissions(administrator=True)
    async def removepingrole(self, ctx, role: discord.Role):
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM ping_roles WHERE guild_id = ? AND role_id = ?", (ctx.guild.id, role.id))
        conn.commit()
        conn.close()
        await ctx.send(f"✅ Removed ping role for {role.mention}")

    @commands.command(name="pingroles", help="Post the claim-roles reaction panel in this channel")
    async def pingroles(self, ctx):
        await send_claim_roles_panel(ctx.channel, guild_id=ctx.guild.id, bot=ctx.bot)

    # ── DM Sender (owner-only) ─────────────────────────────────────────────

    @commands.command(name="mass_dm", help="Send DMs to all users with a role (Bot owner only)")
    async def mass_dm(self, ctx, role: discord.Role, *, message: str):
        if ctx.author.id not in getattr(config, "BOT_OWNER_IDS", []):
            await ctx.send("⛔ Bot owner only.")
            return
        target_members = [m for m in role.members if not m.bot]
        if not target_members:
            await ctx.send("⚠️ No members found with that role.")
            return
        confirm = await ctx.send(
            f"⚠️ About to DM **{len(target_members)}** members. React ✅ to confirm or ❌ to cancel."
        )
        await confirm.add_reaction("✅")
        await confirm.add_reaction("❌")

        def check(reaction, user):
            return user == ctx.author and str(reaction.emoji) in ("✅", "❌")

        try:
            reaction, _ = await self.bot.wait_for("reaction_add", timeout=60.0, check=check)
        except asyncio.TimeoutError:
            await ctx.send("🛑 Timed out.")
            return

        if str(reaction.emoji) != "✅":
            await ctx.send("🛑 Cancelled.")
            return

        success = 0
        failed = 0
        for i, member in enumerate(target_members, 1):
            try:
                await member.send(message)
                success += 1
            except discord.Forbidden:
                failed += 1
            except Exception as e:
                failed += 1
                print(f"[VelcorFeatures] DM error to {member.id}: {e}")
            if i < len(target_members):
                await asyncio.sleep(2)
        await ctx.send(f"✅ DMs sent: **{success}** | Failed: **{failed}**")


# ════════════════════════════════════════════════════════════════════════════
# Setup helper for discord_bot.py
# ════════════════════════════════════════════════════════════════════════════

async def setup(bot: commands.Bot):
    await bot.add_cog(VelcorFeatures(bot))
