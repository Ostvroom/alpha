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


_TWITTER_BLUE = 0x1D9BF0
_DESCRIPTION_LIMIT = 2200  # leaves room under Discord's 6000-char multi-embed cap


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
    image_links = getattr(media, "image_links", None)
    if isinstance(image_links, list):
        return [{"media_url_https": u, "type": "photo"} for u in image_links]
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


def _video_url(m: Any) -> str:
    """Return the highest-quality direct MP4 variant when X supplies one."""
    info = m.get("video_info", {}) if isinstance(m, dict) else getattr(m, "video_info", {})
    variants = info.get("variants", []) if isinstance(info, dict) else []
    candidates = [
        item for item in variants
        if isinstance(item, dict)
        and "video/mp4" in str(item.get("content_type") or "")
        and item.get("url")
    ]
    if not candidates:
        return ""
    return str(max(candidates, key=lambda item: int(item.get("bitrate") or 0)).get("url") or "")


def _retweet_source(tweet: Any) -> Optional[Any]:
    return getattr(tweet, "retweeted_tweet", None) or getattr(tweet, "retweeted_status", None)


def _quote_source(tweet: Any) -> Optional[Any]:
    return getattr(tweet, "quoted_tweet", None) or getattr(tweet, "quoted_status", None)


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


ENGAGE_CLAIM_CUSTOM_ID = "tweet_engage_claim"


class TweetEngageView(View):
    """🚀 link-button (opens X) + ✅ claim button (bot-detectable).

    The link button is handled entirely client-side by Discord — the bot never
    receives an interaction for it, so it can never award points. The claim
    button carries a STATIC custom_id and resolves its tweet from
    interaction.message.id, which keeps the view restart-safe: registering one
    zero-state instance at boot is enough for Discord to route clicks on every
    previously-posted message.
    """

    def __init__(self, tweet_url: str = "", claim_count: int = 0) -> None:
        super().__init__(timeout=None)
        if tweet_url:
            self.add_item(
                Button(style=discord.ButtonStyle.link, label="🚀 Engage on X", url=tweet_url)
            )
        label = f"✅ I Engaged ({claim_count})" if claim_count else "✅ I Engaged"
        self.claim_button = Button(
            style=discord.ButtonStyle.success,
            label=label,
            custom_id=ENGAGE_CLAIM_CUSTOM_ID,
        )
        self.claim_button.callback = self._on_claim
        self.add_item(self.claim_button)

    async def _on_claim(self, interaction: discord.Interaction) -> None:
        import engagement

        row = engagement.get_tweet_for_message(interaction.message.id)
        if not row:
            await interaction.response.send_message(
                "This alert is no longer claimable.", ephemeral=True
            )
            return

        tweet_id = str(row.get("tweet_id") or "")
        ok, reason, pts = engagement.claim_tweet_engagement(interaction.user.id, tweet_id)

        if ok:
            msg = f"✅ **+{pts} points** — thanks for engaging!"
        elif reason == "already_claimed":
            msg = "You already claimed this tweet."
        elif reason in ("daily_cap", "global_cap"):
            msg = (
                f"Daily limit reached ({engagement.X_ENGAGE_DAILY_CAP}/"
                f"{engagement.X_ENGAGE_DAILY_CAP}). Resets at 00:00 UTC."
            )
        else:
            msg = "Could not record that claim — try again shortly."
        await interaction.response.send_message(msg, ephemeral=True)

        if ok:
            # Refresh the public counter so others see the social proof.
            try:
                count = engagement.get_engage_claim_count(tweet_id)
                self.claim_button.label = f"✅ I Engaged ({count})"
                await interaction.message.edit(view=self)
            except Exception:
                pass


def _build_engage_view(tweet_url: str) -> Optional[View]:
    """Engage link-button plus the claim button."""
    try:
        return TweetEngageView(tweet_url=tweet_url or "")
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


# ── Dead-handle quarantine ───────────────────────────────────────────────────
# A handle that can't be resolved (suspended/renamed/blocked) burns the full
# timeout every cycle. After N consecutive failures, skip it for a cooldown.
_handle_fail_state: Dict[str, Dict[str, float]] = {}


def _fail_threshold() -> int:
    try:
        return max(1, int(os.getenv("TWEET_WATCHER_FAIL_QUARANTINE_THRESHOLD", "3") or "3"))
    except ValueError:
        return 3


def _quarantine_sec() -> float:
    try:
        return max(60.0, float(os.getenv("TWEET_WATCHER_QUARANTINE_SEC", "1800") or "1800"))
    except ValueError:
        return 1800.0


def _is_quarantined(handle: str) -> bool:
    st = _handle_fail_state.get(handle.lower())
    return bool(st and st.get("until", 0.0) > time.time())


def _quarantine_remaining(handle: str) -> int:
    st = _handle_fail_state.get(handle.lower())
    return max(0, int(st.get("until", 0.0) - time.time())) if st else 0


def _record_handle_failure(handle: str) -> None:
    key = handle.lower()
    st = _handle_fail_state.setdefault(key, {"fails": 0, "until": 0.0})
    st["fails"] = float(st.get("fails", 0)) + 1
    if st["fails"] >= _fail_threshold():
        st["until"] = time.time() + _quarantine_sec()
        print(
            f"[TweetWatcher] ⏸️ @{handle} quarantined for {int(_quarantine_sec())}s "
            f"after {int(st['fails'])} consecutive failures."
        )


def _record_handle_success(handle: str) -> None:
    _handle_fail_state.pop(handle.lower(), None)


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


def _clean_tweet_text(tweet: Any, *, has_attachment: bool = False) -> str:
    text = _tweet_text(tweet).strip()
    # X appends one trailing t.co URL for native media and quoted posts. Remove
    # only that attachment token; retain links that are part of the actual text.
    if has_attachment:
        text = re.sub(r"(?:\s+|^)https?://t\.co/\S+\s*$", "", text, flags=re.IGNORECASE).strip()
    return re.sub(r"\n{3,}", "\n\n", text)


def _text_continuations(tweet: Any) -> List[str]:
    """Return Discord-sized continuation messages so long Notes are never lost."""
    retweeted = _retweet_source(tweet)
    display_tweet = retweeted or tweet
    quoted = _quote_source(display_tweet)
    entries = [
        (
            "Tweet continued",
            _clean_tweet_text(
                display_tweet,
                has_attachment=bool(_extract_media(display_tweet) or quoted),
            ),
        )
    ]
    if quoted:
        entries.append(
            (
                "Quoted post continued",
                _clean_tweet_text(quoted, has_attachment=bool(_extract_media(quoted))),
            )
        )

    messages: List[str] = []
    for label, full_text in entries:
        remaining = full_text[_DESCRIPTION_LIMIT:]
        part = 2
        while remaining:
            prefix = f"**{label} (part {part})**\n"
            room = 2000 - len(prefix)
            chunk = remaining[:room]
            split_at = chunk.rfind("\n")
            if split_at < room // 2:
                split_at = chunk.rfind(" ")
            if split_at >= room // 2:
                chunk = chunk[:split_at]
            messages.append(prefix + chunk)
            remaining = remaining[len(chunk):].lstrip()
            part += 1
    return messages


def _set_embed_author(embed: Embed, tweet: Any, fallback_user: Any, url: str = "") -> Tuple[Any, str]:
    author = _tweet_user(tweet, fallback_user)
    handle = getattr(author, "screen_name", "") or getattr(author, "username", "") or "unknown"
    name = getattr(author, "name", "") or handle
    pfp = getattr(author, "profile_image_url_https", "") or getattr(author, "profile_image_url", "") or ""
    verified = bool(getattr(author, "verified", False) or getattr(author, "is_blue_verified", False))
    kwargs: dict = {
        "name": (f"{name} (@{handle})" + (" ✓" if verified else ""))[:256],
        "url": url or f"https://x.com/{handle}",
    }
    if pfp:
        kwargs["icon_url"] = pfp
    embed.set_author(**kwargs)
    return author, handle


def _add_media_embeds(
    embeds: List[Embed], owner_embed: Embed, media: List[Any], *, label: str,
) -> None:
    """Render every image/video poster; Discord allows one image per embed."""
    seen: set[str] = set()
    media_items: List[Tuple[str, Any]] = []
    video_links: List[str] = []
    for item in media:
        image_url = _media_url(item)
        if image_url and image_url not in seen:
            seen.add(image_url)
            media_items.append((image_url, item))
        direct_video = _video_url(item)
        if direct_video and direct_video not in video_links:
            video_links.append(direct_video)

    if media_items:
        owner_embed.set_image(url=media_items[0][0])
    for index, (image_url, _item) in enumerate(media_items[1:], start=2):
        if len(embeds) >= 10:
            break
        media_embed = Embed(color=_TWITTER_BLUE)
        media_embed.set_image(url=image_url)
        media_embed.set_footer(text=f"{label} media {index}/{len(media_items)}")
        embeds.append(media_embed)

    if video_links:
        links = [f"[Play video {i} in full quality]({url})" for i, url in enumerate(video_links, 1)]
        owner_embed.add_field(name="Video", value="\n".join(links)[:1024], inline=False)


def _build_tweet_embeds(tweet: Any, user: Any) -> Tuple[List[Embed], str]:
    """Build the tweet, quote, and all media as one Discord multi-embed alert."""
    kind_label, _kind_emoji = _classify_tweet(tweet)
    retweeted = _retweet_source(tweet)
    display_tweet = retweeted or tweet
    display_user = _tweet_user(display_tweet, user)
    quoted = _quote_source(display_tweet)
    retweeter_handle = getattr(user, "screen_name", "") or getattr(user, "username", "") or "unknown"
    handle = getattr(display_user, "screen_name", "") or getattr(display_user, "username", "") or retweeter_handle

    alert_url = _tweet_url(tweet, user)
    original_url = _tweet_url(display_tweet, display_user)
    tweet_url = alert_url or original_url
    display_media = _extract_media(display_tweet)
    if not display_media and retweeted:
        display_media = _extract_media(tweet)
    text = _clean_tweet_text(display_tweet, has_attachment=bool(display_media or quoted))

    title = f"X Alert - {kind_label}"
    if retweeted:
        title = f"X Alert - Retweet by @{retweeter_handle}"
    embed = Embed(title=title, color=_TWITTER_BLUE, timestamp=datetime.now(timezone.utc))
    embeds = [embed]
    _set_embed_author(embed, display_tweet, display_user, original_url)
    embed.description = text[:_DESCRIPTION_LIMIT] if text else "*(no text)*"
    if retweeted:
        embed.add_field(name="Retweeted by", value=f"[@{retweeter_handle}](https://x.com/{retweeter_handle})", inline=False)
    embed.set_footer(text=f"@{handle} - {kind_label} - X")
    _add_media_embeds(embeds, embed, display_media, label="Tweet")

    if quoted and len(embeds) < 10:
        quoted_user = _tweet_user(quoted, display_user)
        quoted_url = _tweet_url(quoted, quoted_user)
        quoted_media = _extract_media(quoted)
        quote_embed = Embed(title="Quoted post", color=0x536471, url=quoted_url or None)
        _set_embed_author(quote_embed, quoted, quoted_user, quoted_url)
        quote_text = _clean_tweet_text(quoted, has_attachment=bool(quoted_media))
        quote_embed.description = quote_text[:_DESCRIPTION_LIMIT] if quote_text else "*(no text)*"
        quote_handle = getattr(quoted_user, "screen_name", "") or getattr(quoted_user, "username", "") or "unknown"
        quote_embed.set_footer(text=f"@{quote_handle} - Quoted post - X")
        embeds.append(quote_embed)
        _add_media_embeds(embeds, quote_embed, quoted_media, label="Quoted post")

    return embeds[:10], tweet_url


def _build_tweet_embed(tweet: Any, user: Any) -> Tuple[Embed, str]:
    """Backward-compatible single-embed builder."""
    embeds, tweet_url = _build_tweet_embeds(tweet, user)
    return embeds[0], tweet_url


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

        if _is_quarantined(handle):
            _log(
                f"[TweetWatcher] ⏸️ Skipping @{handle} (quarantined, "
                f"{_quarantine_remaining(handle)}s remaining).",
                verbose=True,
            )
            continue

        try:
            tweets = await asyncio.wait_for(
                _fetch_recent_tweets(twitter_client, handle, count=_fetch_count(), known_user_id=twitter_id),
                timeout=_handle_timeout_sec(),
            )
        except asyncio.TimeoutError:
            _record_handle_failure(handle)
            print(
                f"[TweetWatcher] Timeout fetching @{handle} after "
                f"{_handle_timeout_sec():.0f}s; skipping this account for now."
            )
            continue
        if not tweets:
            _record_handle_failure(handle)
            print(f"[TweetWatcher] No tweets returned for @{handle}; keeping last_seen unchanged.")
            continue

        _record_handle_success(handle)

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
                embeds, tweet_url = _build_tweet_embeds(tweet, user)
                view = _build_engage_view(tweet_url)
                content = None
                r_id = _role_id()
                if r_id:
                    content = f"<@&{r_id}>"

                if hasattr(bot, "safe_send"):
                    sent = await bot.safe_send(channel, content=content, embeds=embeds, view=view)
                    for continuation in _text_continuations(tweet):
                        await bot.safe_send(channel, content=continuation)
                else:
                    sent = await channel.send(content=content, embeds=embeds, view=view)
                    for continuation in _text_continuations(tweet):
                        await channel.send(content=continuation)

                # Map the posted message to its tweet so the claim button can
                # resolve which tweet it belongs to (keeps custom_id static).
                if sent is not None and tid:
                    try:
                        import engagement

                        engagement.register_tweet_post(sent.id, tid, handle)
                    except Exception as e:
                        print(f"[TweetWatcher] Could not register engage post: {e}")

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

    embeds, tweet_url = _build_tweet_embeds(latest, user)
    embeds[0].title = f"TweetWatcher Test - {embeds[0].title.replace('X Alert - ', '')}"
    view = _build_engage_view(tweet_url)
    content = f"TweetWatcher: latest fetched post/RT for `@{h}`"

    if hasattr(bot, "safe_send"):
        await bot.safe_send(channel, content=content, embeds=embeds, view=view)
        for continuation in _text_continuations(latest):
            await bot.safe_send(channel, content=continuation)
    else:
        await channel.send(content=content, embeds=embeds, view=view)
        for continuation in _text_continuations(latest):
            await channel.send(content=continuation)

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
