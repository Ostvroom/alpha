"""
Tweet Watcher — monitor specific X accounts and post new tweets to Discord.
Uses the existing TwitterClient session pool for reliable fetching.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

import discord
from discord import Embed

import config
import database

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
    """Fetch recent tweets for a handle using the existing TwitterClient."""
    try:
        # Resolve handle -> user_id (cached by TwitterClient)
        print(f"[TweetWatcher] Resolving user: @{handle}")
        user = await twitter_client.get_user_by_handle(handle)
        if not user:
            print(f"[TweetWatcher] Could not resolve user: {handle}")
            return []
        user_id = getattr(user, "id", None) or getattr(user, "user_id", None)
        screen_name = getattr(user, "screen_name", "") or getattr(user, "username", "") or handle
        if not user_id:
            print(f"[TweetWatcher] No user_id for: {handle}")
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


def _build_tweet_embed(tweet: Any, user: Any) -> Embed:
    """Build a Discord embed from a tweet object."""
    handle = getattr(user, "screen_name", "") or getattr(user, "username", "") or "unknown"
    name = getattr(user, "name", "") or handle
    text = getattr(tweet, "full_text", "") or getattr(tweet, "text", "") or ""
    tweet_id = getattr(tweet, "id", "") or getattr(tweet, "tweet_id", "")
    created_at = getattr(tweet, "created_at", "")
    pfp = getattr(user, "profile_image_url", "") or getattr(user, "profile_image_url_https", "")

    embed = Embed(
        title=f"🐦 New Tweet from @{handle}",
        url=f"https://x.com/{handle}/status/{tweet_id}",
        color=0x1DA1F2,
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_author(name=name, icon_url=pfp or "", url=f"https://x.com/{handle}")
    embed.description = text[:4000] if text else "*(no text)*"

    # Media attachment indicator
    media = getattr(tweet, "media", []) or getattr(tweet, "extended_entities", {}).get("media", [])
    if media:
        embed.add_field(name="🖼️ Media", value=f"{len(media)} attachment(s)", inline=False)
        # Try to use first image as embed image
        for m in media:
            media_url = m.get("media_url_https") or m.get("media_url") or ""
            if media_url and (".jpg" in media_url or ".png" in media_url):
                embed.set_image(url=media_url)
                break

    # Stats
    likes = getattr(tweet, "favorite_count", None) or getattr(tweet, "like_count", 0)
    retweets = getattr(tweet, "retweet_count", 0)
    replies = getattr(tweet, "reply_count", 0)
    if likes or retweets or replies:
        stats = f"❤️ {likes:,}  ·  🔁 {retweets:,}  ·  💬 {replies:,}"
        embed.add_field(name="Stats", value=stats, inline=False)

    embed.set_footer(text="Tweet Watcher · X")
    return embed


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
                embed = _build_tweet_embed(tweet, user)
                content = None
                r_id = _role_id()
                if r_id:
                    content = f"<@&{r_id}>"

                if hasattr(bot, "safe_send"):
                    await bot.safe_send(channel, content=content, embed=embed)
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
