"""
Tweet Watcher — monitor specific X accounts and post new tweets to Discord.
Uses the existing TwitterClient session pool for reliable fetching.
"""
from __future__ import annotations

import asyncio
import os
import re
import time
from datetime import datetime, timezone, timedelta
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
    if isinstance(media, dict) and isinstance(media.get("image_links"), list):
        return [{"media_url_https": u, "type": "photo"} for u in media.get("image_links") or []]
    if not media:
        ext = getattr(tweet, "extended_entities", None)
        if isinstance(ext, dict):
            media = ext.get("media") or []
    if not isinstance(media, (list, tuple)):
        return []
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


def _retweet_source(tweet: Any) -> Optional[Any]:
    return getattr(tweet, "retweeted_tweet", None) or getattr(tweet, "retweeted_status", None)


def _tweet_user(tweet: Any, fallback: Any) -> Any:
    return getattr(tweet, "user", None) or fallback


def _tweet_text(tweet: Any) -> str:
    return getattr(tweet, "full_text", "") or getattr(tweet, "text", "") or ""


def _tweet_id(tweet: Any) -> str:
    return str(getattr(tweet, "id", "") or getattr(tweet, "tweet_id", "") or "")


def _tweet_id_int(tweet: Any) -> int:
    try:
        return int(_tweet_id(tweet) or 0)
    except Exception:
        return 0


def _id_int(raw: Any) -> int:
    try:
        return int(str(raw or "").strip() or 0)
    except Exception:
        return 0


def _is_newer_tweet_id(tid: str, last_seen: str) -> bool:
    tid_i = _id_int(tid)
    last_i = _id_int(last_seen)
    if tid_i and last_i:
        return tid_i > last_i
    return str(tid) > str(last_seen)


def _tweet_created_at(tweet: Any) -> Optional[datetime]:
    parsed = _parse_created_at(getattr(tweet, "created_at", None))
    if parsed:
        return parsed.astimezone(timezone.utc)

    tid_i = _tweet_id_int(tweet)
    if not tid_i:
        return None

    # Twitter/X snowflake timestamp: milliseconds since 2010-11-04.
    try:
        ms = (tid_i >> 22) + 1288834974657
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    except Exception:
        return None


def _is_after_watch_start(tweet: Any, watched_at_raw: Any) -> bool:
    watched_at = _parse_created_at(watched_at_raw)
    created_at = _tweet_created_at(tweet)
    if not watched_at or not created_at:
        return False
    watched_at = watched_at.astimezone(timezone.utc) - timedelta(seconds=5)
    return created_at >= watched_at


def _is_recent_first_check_tweet(tweet: Any, now: datetime) -> bool:
    window_min = _first_check_recent_min()
    if window_min <= 0:
        return False
    created_at = _tweet_created_at(tweet)
    if not created_at:
        return False
    return created_at >= now.astimezone(timezone.utc) - timedelta(minutes=window_min)


def _tweet_url(tweet: Any, user: Any) -> str:
    handle = getattr(user, "screen_name", "") or getattr(user, "username", "") or ""
    tid = _tweet_id(tweet)
    if handle and tid:
        return f"https://x.com/{handle}/status/{tid}"
    if handle:
        return f"https://x.com/{handle}"
    return ""


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


def _fetch_count() -> int:
    try:
        raw = int(os.getenv("TWEET_WATCHER_FETCH_COUNT", "20") or "20")
    except ValueError:
        raw = 20
    return max(10, min(50, raw))


def _first_check_recent_min() -> int:
    try:
        raw = int(os.getenv("TWEET_WATCHER_FIRST_CHECK_RECENT_MIN", "15") or "15")
    except ValueError:
        raw = 15
    return max(0, min(240, raw))


def _handle_timeout_sec() -> float:
    try:
        raw = float(os.getenv("TWEET_WATCHER_HANDLE_TIMEOUT_SEC", "45") or "45")
    except ValueError:
        raw = 45.0
    return max(5.0, min(90.0, raw))


def _verbose_logs() -> bool:
    return (os.getenv("TWEET_WATCHER_VERBOSE", "0") or "0").strip().lower() in ("1", "true", "yes", "on")


def _log(message: str, *, verbose: bool = False) -> None:
    if verbose and not _verbose_logs():
        return
    print(message)


# Legacy module-level aliases (kept for backward compat, but prefer functions above)
_WATCHER_INTERVAL_MIN = _interval_min()
_WATCHER_CHANNEL_ID = _channel_id()
_WATCHER_ROLE_ID = _role_id()


# ── Core watcher logic ───────────────────────────────────────────────────────

async def _fetch_recent_tweets(
    twitter_client,
    handle: str,
    count: int = 10,
    known_user_id: Optional[str] = None,
) -> List[Any]:
    """Fetch recent tweets for a handle using the existing TwitterClient.

    Resolution order: full user object → cached user_id → twikit fallback.
    Using get_user_id() first lets us hit the resolver cache and avoid the
    slow Scweet aget_user_info path that has been failing for some handles.
    """
    try:
        user = None
        user_id = str(known_user_id or "").strip() or None
        screen_name = handle

        # 1) Try the cached user_id path first (fast + cache-friendly).
        if not user_id:
            try:
                _log(f"[TweetWatcher] Resolving uid for @{handle} (cache/get_user_id)", verbose=True)
                user_id = await twitter_client.get_user_id(handle)
            except Exception as e:
                print(f"[TweetWatcher] get_user_id error for @{handle}: {e}")
        else:
            _log(f"[TweetWatcher] Reusing stored uid {user_id} for @{handle}", verbose=True)

        # 2) If that failed, fall back to the heavier get_user_by_handle path.
        if not user_id:
            _log(f"[TweetWatcher] Resolving full user: @{handle}", verbose=True)
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

        _log(f"[TweetWatcher] @{screen_name} resolved to uid {user_id}", verbose=True)

        # Fetch timeline
        _log(f"[TweetWatcher] Fetching timeline for @{handle} (count={count})", verbose=True)
        tweets = await twitter_client.get_user_timeline(user_id, count=count, handle=handle)
        _log(f"[TweetWatcher] Got {len(tweets or [])} tweets for @{handle}", verbose=True)
        return tweets or []
    except Exception as e:
        print(f"[TweetWatcher] Error fetching tweets for {handle}: {e}")
        import traceback
        traceback.print_exc()
        return []


def _build_tweet_embed(tweet: Any, user: Any) -> Tuple[Embed, str]:
    """Build a clean Discord embed. Returns (embed, tweet_url)."""
    kind_label, _kind_emoji = _classify_tweet(tweet)
    retweeted = _retweet_source(tweet)
    display_tweet = retweeted or tweet
    display_user = _tweet_user(display_tweet, user)
    retweeter_handle = getattr(user, "screen_name", "") or getattr(user, "username", "") or "unknown"
    handle = getattr(display_user, "screen_name", "") or getattr(display_user, "username", "") or retweeter_handle
    name = getattr(display_user, "name", "") or handle
    text_raw = _tweet_text(display_tweet)
    pfp = getattr(display_user, "profile_image_url_https", "") or getattr(display_user, "profile_image_url", "") or ""
    verified = bool(getattr(display_user, "verified", False) or getattr(display_user, "is_blue_verified", False))

    alert_url = _tweet_url(tweet, user)
    original_url = _tweet_url(display_tweet, display_user)
    tweet_url = alert_url or original_url

    # Strip t.co URLs and tidy whitespace
    text = _TCO_RE.sub("", text_raw).strip()
    text = re.sub(r"\n{3,}", "\n\n", text)

    title = f"X Alert - {kind_label}"
    if retweeted:
        title = f"X Alert - Retweet by @{retweeter_handle}"
    embed = Embed(title=title, color=_TWITTER_BLUE, timestamp=datetime.now(timezone.utc))

    # Author line: "Name (@handle) ✓"
    author_name = f"{name} (@{handle})" + (" ✓" if verified else "")
    author_kwargs: dict = {"name": author_name[:256], "url": original_url or f"https://x.com/{handle}"}
    if pfp:
        author_kwargs["icon_url"] = pfp
    embed.set_author(**author_kwargs)

    # Tweet body
    embed.description = text[:4000] if text else "*(no text)*"
    if retweeted:
        embed.add_field(name="Retweeted by", value=f"[@{retweeter_handle}](https://x.com/{retweeter_handle})", inline=False)

    # First image (if any)
    for m in _extract_media(display_tweet):
        url = _media_url(m)
        if url and any(ext in url.lower() for ext in (".jpg", ".jpeg", ".png", ".webp")):
            try:
                embed.set_image(url=url)
            except Exception:
                pass
            break

    embed.set_footer(text=f"@{handle} - {kind_label} - X")
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
    started_at = time.monotonic()
    _log(f"[TweetWatcher] Starting check - channel={ch_id}, role={r_id}, interval={interval}min", verbose=True)
    if not ch_id:
        print("[TweetWatcher] No channel configured — skipping")
        return 0

    if channel is None:
        channel = bot.get_channel(ch_id)
        if channel is None:
            try:
                channel = await bot.fetch_channel(ch_id)
                _log(f"[TweetWatcher] Fetched channel: #{getattr(channel, 'name', ch_id)}", verbose=True)
            except Exception as e:
                print(f"[TweetWatcher] Cannot access channel {ch_id}: {e}")
                return 0

    watched = database.list_tweet_watcher_handles()
    _log(f"[TweetWatcher] Watching {len(watched)} account(s): {[w['handle'] for w in watched]}", verbose=True)
    if not watched:
        return 0

    posted = 0
    for entry in watched:
        handle = entry["handle"]
        twitter_id = str(entry.get("twitter_id") or "").strip()
        last_seen = entry.get("last_seen_tweet_id") or ""
        _log(f"[TweetWatcher] Checking @{handle} - last_seen={last_seen or 'none'}", verbose=True)

        try:
            tweets = await asyncio.wait_for(
                _fetch_recent_tweets(twitter_client, handle, count=_fetch_count(), known_user_id=twitter_id),
                timeout=_handle_timeout_sec(),
            )
        except asyncio.TimeoutError:
            print(
                f"[TweetWatcher] Timeout fetching @{handle} after "
                f"{_handle_timeout_sec():.0f}s; skipping this account for now."
            )
            continue
        if not tweets:
            print(f"[TweetWatcher] No tweets returned for @{handle}; keeping last_seen unchanged.")
            continue

        # Sort by tweet ID ascending (oldest first) so we post chronologically
        # Twitter tweet IDs are snowflakes — higher = newer
        try:
            tweets_sorted = sorted(tweets, key=_tweet_id_int)
        except Exception:
            tweets_sorted = tweets

        new_tweets: List[Any] = []

        # First-time watch: baseline old history, but do alert posts created after
        # the handle was added. This avoids swallowing a real-time test tweet when
        # the first scheduled watcher run is delayed.
        if not last_seen:
            watched_at = entry.get("last_checked_at")
            now_utc = datetime.now(timezone.utc)
            for t in tweets_sorted:
                if not _tweet_id(t):
                    continue
                if not (
                    _is_after_watch_start(t, watched_at)
                    or _is_recent_first_check_tweet(t, now_utc)
                ):
                    continue
                new_tweets.append(t)
                if len(new_tweets) >= _max_tweets():
                    break

            if new_tweets:
                print(
                    f"[TweetWatcher] First check for @{handle} found "
                    f"{len(new_tweets)} recent/new post(s); posting."
                )
            else:
                newest_tid = _tweet_id(tweets_sorted[-1]) if tweets_sorted else ""
                if newest_tid:
                    user_obj = getattr(tweets_sorted[-1], "user", None)
                    uid = getattr(user_obj, "id", "") if user_obj else twitter_id
                    database.upsert_tweet_watcher_state(handle, str(uid), newest_tid)
                    print(f"[TweetWatcher] First check for @{handle} — baseline set to {newest_tid}")
                continue
        else:
            for t in tweets_sorted:
                tid = _tweet_id(t)
                if not tid:
                    continue
                is_new = _is_newer_tweet_id(tid, last_seen)
                _log(f"[TweetWatcher] Comparing tid={tid} vs last_seen={last_seen} for @{handle} - new={is_new}", verbose=True)
                if not is_new:
                    continue
                new_tweets.append(t)
                if len(new_tweets) >= _max_tweets():
                    break

        _log(f"[TweetWatcher] Found {len(new_tweets)} new tweet(s) for @{handle}", verbose=True)
        if not new_tweets:
            continue

        # Get user object for embed author
        user = getattr(new_tweets[-1], "user", None) or await twitter_client.get_user_by_handle(handle)
        if not user:
            user = new_tweets[-1]  # fallback

        # Post each new tweet
        newest_posted_tid = ""
        for tweet in new_tweets:
            tid = _tweet_id(tweet)
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
                newest_posted_tid = tid
                print(f"[TweetWatcher] Posted tweet {tid} from @{handle}")
                await asyncio.sleep(1.5)  # gentle pace between posts
            except Exception as e:
                print(f"[TweetWatcher] Failed to post tweet {tid}: {e}")

        # Update state only through the newest tweet that was actually posted.
        # If Discord send fails, keeping last_seen behind lets the next cycle retry.
        if newest_posted_tid:
            user_id = getattr(user, "id", "") or getattr(user, "user_id", "") or ""
            database.upsert_tweet_watcher_state(handle, str(user_id or twitter_id), newest_posted_tid)

    elapsed = time.monotonic() - started_at
    if not posted and elapsed > max(60.0, interval * 60.0):
        print(
            f"[TweetWatcher] Check took {elapsed:.1f}s with no new tweets; "
            f"interval is {interval}min. Consider raising TWEET_WATCHER_INTERVAL_MIN."
        )
    return posted


# ── Command helpers ──────────────────────────────────────────────────────────

async def post_latest_for_handle(
    bot,
    twitter_client,
    handle: Optional[str] = None,
    channel: Optional[discord.TextChannel] = None,
    *,
    update_state: bool = False,
) -> tuple[bool, str]:
    """Fetch and post the latest tweet/RT for one watched handle as a manual test.

    Unlike check_watched_accounts(), this ignores last_seen_tweet_id so operators
    can verify the watcher pipeline even when there are no new tweets.
    """
    ch_id = _channel_id()
    if not ch_id:
        return False, "TweetWatcher channel is not configured."

    if channel is None:
        channel = bot.get_channel(ch_id)
        if channel is None:
            try:
                channel = await bot.fetch_channel(ch_id)
            except Exception as e:
                return False, f"Cannot access TweetWatcher channel {ch_id}: {e}"

    h = (handle or "").strip().lstrip("@").lower()
    if not h:
        watched = database.list_tweet_watcher_handles()
        if not watched:
            return False, "No watched X accounts configured. Add one with /watch_x first."
        h = str(watched[0].get("handle") or "").strip().lstrip("@").lower()

    if not h:
        return False, "Invalid X handle."

    state = database.get_tweet_watcher_state(h) or {}
    twitter_id = str(state.get("twitter_id") or "").strip()
    try:
        tweets = await asyncio.wait_for(
            _fetch_recent_tweets(twitter_client, h, count=_fetch_count(), known_user_id=twitter_id),
            timeout=_handle_timeout_sec(),
        )
    except asyncio.TimeoutError:
        return False, f"Timed out fetching @{h} after {_handle_timeout_sec():.0f}s."
    if not tweets:
        return False, f"No tweets returned for @{h}."

    try:
        latest = max(
            tweets,
            key=_tweet_id_int,
        )
    except Exception:
        latest = tweets[0]

    user = getattr(latest, "user", None) or await twitter_client.get_user_by_handle(h)
    if not user:
        user = latest

    embed, tweet_url = _build_tweet_embed(latest, user)
    embed.title = f"TweetWatcher Test - {embed.title.replace('X Alert - ', '')}"
    view = _build_engage_view(tweet_url)
    content = f"TweetWatcher: latest fetched post/RT for `@{h}`"

    if hasattr(bot, "safe_send"):
        await bot.safe_send(channel, content=content, embed=embed, view=view)
    else:
        if view is not None:
            await channel.send(content=content, embed=embed, view=view)
        else:
            await channel.send(content=content, embed=embed)

    tid = _tweet_id(latest)
    if update_state and tid:
        uid = getattr(user, "id", "") or getattr(user, "user_id", "") or ""
        database.upsert_tweet_watcher_state(h, str(uid), tid)

    print(f"[TweetWatcher] Test posted latest tweet {tid or '?'} from @{h}")
    return True, f"Posted latest fetched tweet/RT for @{h} to <#{getattr(channel, 'id', ch_id)}>."


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
