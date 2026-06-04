"""
Tweet Watcher — monitor specific X accounts and post new tweets to Discord.
Uses the existing TwitterClient session pool for reliable fetching.
"""
from __future__ import annotations

import asyncio
import os
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple

import discord
from discord import Embed
from discord.ui import View, Button

import config
import database


# Match t.co short URLs Twitter appends to tweet text (we hide them — image
# is rendered via set_image, and the “Engage” button carries the canonical URL)
_TCO_RE = re.compile(r"https?://t\.co/\S+", re.IGNORECASE)
_TWITTER_BLUE = 0x1D9BF0


def _fmt_count(n: Any) -> str:
    try:
        n = int(n or 0)
    except Exception:
        return "0"
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M".replace(".0M", "M")
    if n >= 1_000:
        return f"{n/1_000:.1f}K".replace(".0K", "K")
    return f"{n:,}"


def _parse_created_at(raw: Any) -> Optional[datetime]:
    if not raw:
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    s = str(raw)
    # Twitter classic format: "Wed Oct 10 20:19:24 +0000 2018"
    try:
        return datetime.strptime(s, "%a %b %d %H:%M:%S %z %Y")
    except Exception:
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def _classify_tweet(tweet: Any) -> Tuple[str, str]:
    """Returns (kind_label, emoji) — e.g. ('Reply', '💬'), ('Retweet', '🔁')."""
    if getattr(tweet, "retweeted_tweet", None) or getattr(tweet, "retweeted_status", None):
        return ("Retweet", "🔁")
    if getattr(tweet, "quoted_tweet", None) or getattr(tweet, "quoted_status", None) or getattr(tweet, "is_quote_status", False):
        return ("Quote", "💬")
    in_reply = (
        getattr(tweet, "in_reply_to_status_id", None)
        or getattr(tweet, "in_reply_to_tweet_id", None)
        or getattr(tweet, "replied_to", None)
    )
    if in_reply:
        return ("Reply", "↩️")
    return ("New post", "🐦")


def _extract_media(tweet: Any) -> List[Any]:
    media = getattr(tweet, "media", None)
    if not media:
        ext = getattr(tweet, "extended_entities", None)
        if isinstance(ext, dict):
            media = ext.get("media") or []
    return list(media or [])


def _media_url(m: Any) -> str:
    if isinstance(m, dict):
        return m.get("media_url_https") or m.get("media_url") or ""
    return (
        getattr(m, "media_url_https", "")
        or getattr(m, "media_url", "")
        or getattr(m, "url", "")
        or ""
    )


def _media_type(m: Any) -> str:
    if isinstance(m, dict):
        return (m.get("type") or "").lower()
    return str(getattr(m, "type", "") or "").lower()


def _build_engage_view(tweet_url: str) -> Optional[View]:
    """Single 🚀 Engage link-button that opens the tweet on X."""
    if not tweet_url:
        return None
    try:
        view = View(timeout=None)
        view.add_item(Button(style=discord.ButtonStyle.link, label="🚀 Engage on X", url=tweet_url))
        return view
    except Exception:
        return None

# ── Config (read directly from os.getenv to avoid ALL module caches) ─────────
def _channel_id() -> int:
    return int(os.getenv("TWEET_WATCHER_CHANNEL_ID", "0") or "0")


def _role_id() -> int:
    return int(os.getenv("TWEET_WATCHER_ROLE_ID", "0") or "0")


def _interval_min() -> int:
    return max(1, int(os.getenv("TWEET_WATCHER_INTERVAL_MIN", "3") or "3"))


def _max_tweets() -> int:
    return max(1, min(10, int(os.getenv("TWEET_WATCHER_MAX_TWEETS", "3"))))


# Legacy module-level aliases (kept for backward compat, but prefer functions above)
_WATCHER_INTERVAL_MIN = _interval_min()
_WATCHER_CHANNEL_ID = _channel_id()
_WATCHER_ROLE_ID = _role_id()
_WATCHER_MAX_TWEETS_PER_CHECK = _max_tweets()


# ── Core watcher logic ───────────────────────────────────────────────────────

async def _fetch_recent_tweets(twitter_client, handle: str, count: int = 10) -> List[Any]:
    """Fetch recent tweets for a handle using the existing TwitterClient.

    Resolution order: full user object → cached user_id → twikit fallback.
    Using get_user_id() first lets us hit the resolver cache and avoid the
    slow Scweet aget_user_info path that has been failing for some handles.
    """
    try:
        user = None
        user_id = None
        screen_name = handle

        # 1) Try the cached user_id path first (fast + cache-friendly).
        try:
            print(f"[TweetWatcher] Resolving uid for @{handle} (cache/get_user_id)")
            user_id = await twitter_client.get_user_id(handle)
        except Exception as e:
            print(f"[TweetWatcher] get_user_id error for @{handle}: {e}")

        # 2) If that failed, fall back to the heavier get_user_by_handle path.
        if not user_id:
            print(f"[TweetWatcher] Resolving full user: @{handle}")
            user = await twitter_client.get_user_by_handle(handle)
            if user:
                user_id = getattr(user, "id", None) or getattr(user, "user_id", None)
                screen_name = (
                    getattr(user, "screen_name", "")
                    or getattr(user, "username", "")
                    or handle
                )

        if not user_id:
            print(f"[TweetWatcher] Could not resolve user: {handle}")
            return []

        print(f"[TweetWatcher] @{screen_name} resolved to uid {user_id}")

        # Fetch timeline
        print(f"[TweetWatcher] Fetching timeline for @{handle} (count={count})")
        tweets = await twitter_client.get_user_timeline(user_id, count=count, handle=handle)
        print(f"[TweetWatcher] Got {len(tweets or [])} tweets for @{handle}")
        return tweets or []
    except Exception as e:
        print(f"[TweetWatcher] Error fetching tweets for {handle}: {e}")
        import traceback
        traceback.print_exc()
        return []


def _build_tweet_embed(tweet: Any, user: Any) -> Tuple[Embed, str]:
    """Build a clean Discord embed. Returns (embed, tweet_url)."""
    handle = getattr(user, "screen_name", "") or getattr(user, "username", "") or "unknown"
    name = getattr(user, "name", "") or handle
    text_raw = getattr(tweet, "full_text", "") or getattr(tweet, "text", "") or ""
    tweet_id = getattr(tweet, "id", "") or getattr(tweet, "tweet_id", "")
    pfp = getattr(user, "profile_image_url_https", "") or getattr(user, "profile_image_url", "") or ""
    verified = bool(getattr(user, "verified", False) or getattr(user, "is_blue_verified", False))

    tweet_url = f"https://x.com/{handle}/status/{tweet_id}" if tweet_id else f"https://x.com/{handle}"

    # Strip t.co URLs and tidy whitespace
    text = _TCO_RE.sub("", text_raw).strip()
    text = re.sub(r"\n{3,}", "\n\n", text)

    embed = Embed(color=_TWITTER_BLUE, timestamp=datetime.now(timezone.utc))

    # Author line: "Name (@handle) ✓"
    author_name = f"{name} (@{handle})" + (" ✓" if verified else "")
    author_kwargs: dict = {"name": author_name[:256], "url": f"https://x.com/{handle}"}
    if pfp:
        author_kwargs["icon_url"] = pfp
    embed.set_author(**author_kwargs)

    # Tweet body
    embed.description = text[:4000] if text else "*(no text)*"

    # First image (if any)
    for m in _extract_media(tweet):
        url = _media_url(m)
        if url and any(ext in url.lower() for ext in (".jpg", ".jpeg", ".png", ".webp")):
            try:
                embed.set_image(url=url)
            except Exception:
                pass
            break

    embed.set_footer(text=f"@{handle} · X")
    return embed, tweet_url


async def check_watched_accounts(
    bot,
    twitter_client,
    channel: Optional[discord.TextChannel] = None,
) -> int:
    """
    Check all watched accounts for new tweets and post to Discord.
    Returns number of new tweets posted.
    """
    ch_id = _channel_id()
    r_id = _role_id()
    interval = _interval_min()
    print(f"[TweetWatcher] Starting check — channel={ch_id}, role={r_id}, interval={interval}min")
    if not ch_id:
        print("[TweetWatcher] No channel configured — skipping")
        return 0

    if channel is None:
        channel = bot.get_channel(ch_id)
        if channel is None:
            try:
                channel = await bot.fetch_channel(ch_id)
                print(f"[TweetWatcher] Fetched channel: #{getattr(channel, 'name', ch_id)}")
            except Exception as e:
                print(f"[TweetWatcher] Cannot access channel {ch_id}: {e}")
                return 0

    watched = database.list_tweet_watcher_handles()
    print(f"[TweetWatcher] Watching {len(watched)} account(s): {[w['handle'] for w in watched]}")
    if not watched:
        return 0

    posted = 0
    for entry in watched:
        handle = entry["handle"]
        last_seen = entry.get("last_seen_tweet_id") or ""
        print(f"[TweetWatcher] Checking @{handle} — last_seen={last_seen or 'none'}")

        tweets = await _fetch_recent_tweets(twitter_client, handle, count=10)
        if not tweets:
            print(f"[TweetWatcher] No tweets returned for @{handle}")
            continue

        # Sort by tweet ID ascending (oldest first) so we post chronologically
        # Twitter tweet IDs are snowflakes — higher = newer
        try:
            tweets_sorted = sorted(tweets, key=lambda t: int(getattr(t, "id", "0") or getattr(t, "tweet_id", "0") or 0))
        except Exception:
            tweets_sorted = tweets

        # First-time watch: just record the newest tweet ID, don't flood the channel
        if not last_seen:
            newest_tid = str(getattr(tweets_sorted[-1], "id", "") or getattr(tweets_sorted[-1], "tweet_id", "")) if tweets_sorted else ""
            if newest_tid:
                user_id = getattr(tweets_sorted[-1], "user", None)
                uid = getattr(user_id, "id", "") if user_id else ""
                database.upsert_tweet_watcher_state(handle, str(uid), newest_tid)
                print(f"[TweetWatcher] First check for @{handle} — baseline set to {newest_tid}")
            continue

        new_tweets: List[Any] = []
        for t in tweets_sorted:
            tid = str(getattr(t, "id", "") or getattr(t, "tweet_id", ""))
            if not tid:
                continue
            print(f"[TweetWatcher] Comparing tid={tid} vs last_seen={last_seen} for @{handle} — new={tid > last_seen}")
            if tid <= last_seen:
                continue
            new_tweets.append(t)
            if len(new_tweets) >= _WATCHER_MAX_TWEETS_PER_CHECK:
                break

        print(f"[TweetWatcher] Found {len(new_tweets)} new tweet(s) for @{handle}")
        if not new_tweets:
            continue

        # Get user object for embed author
        user = getattr(new_tweets[-1], "user", None) or await twitter_client.get_user_by_handle(handle)
        if not user:
            user = new_tweets[-1]  # fallback

        # Post each new tweet
        for tweet in new_tweets:
            tid = str(getattr(tweet, "id", "") or getattr(tweet, "tweet_id", ""))
            try:
                embed, tweet_url = _build_tweet_embed(tweet, user)
                view = _build_engage_view(tweet_url)
                content = None
                r_id = _role_id()
                if r_id:
                    content = f"<@&{r_id}>"

                if hasattr(bot, "safe_send"):
                    await bot.safe_send(channel, content=content, embed=embed, view=view)
                else:
                    if view is not None:
                        await channel.send(content=content, embed=embed, view=view)
                    else:
                        await channel.send(content=content, embed=embed)

                posted += 1
                print(f"[TweetWatcher] Posted tweet {tid} from @{handle}")
                await asyncio.sleep(1.5)  # gentle pace between posts
            except Exception as e:
                print(f"[TweetWatcher] Failed to post tweet {tid}: {e}")

        # Update state with the newest tweet ID we saw
        newest_tid = str(getattr(new_tweets[-1], "id", "") or getattr(new_tweets[-1], "tweet_id", ""))
        if newest_tid:
            user_id = getattr(user, "id", "") or getattr(user, "user_id", "") or ""
            database.upsert_tweet_watcher_state(handle, str(user_id), newest_tid)

    return posted


# ── Command helpers ──────────────────────────────────────────────────────────

async def add_watched_handle(handle: str) -> tuple[bool, str]:
    """Add a handle to the watch list. Returns (success, message)."""
    handle = handle.strip().lstrip("@").lower()
    if not handle:
        return False, "Invalid handle."

    existing = database.get_tweet_watcher_state(handle)
    if existing:
        return False, f"@{handle} is already being watched."

    # Add directly — the watcher loop will resolve the user ID on first check
    database.upsert_tweet_watcher_state(handle, "", "")
    return True, f"✅ Now watching @{handle}. New tweets will be posted to <#{_channel_id()}>"



async def remove_watched_handle(handle: str) -> tuple[bool, str]:
    handle = handle.strip().lstrip("@").lower()
    database.remove_tweet_watcher_state(handle)
    return True, f"🗑️ Removed @{handle} from watch list."


def list_watched_handles() -> List[dict]:
    return database.list_tweet_watcher_handles()
