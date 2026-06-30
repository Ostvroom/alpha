import asyncio
import os
import json
import random
import re
from twikit import Client
import config
from datetime import datetime, timezone, timedelta
import time
from typing import Any, Optional

# Auto-apply twikit reduce() patch on every startup (safe if already patched)
try:
    import patch_twikit
    patch_twikit.apply_patch()
except Exception:
    pass


# ── Date parsing helpers ──────────────────────────────────────────────────

def _parse_twitter_date(value):
    """Parse Twitter created_at into a timezone-aware datetime.
    Handles legacy Twitter format, ISO 8601, and datetime objects."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if not isinstance(value, str):
        return None
    value = value.strip()
    # Legacy Twitter: Wed May 07 20:30:00 +0000 2025
    formats = [
        "%a %b %d %H:%M:%S %z %Y",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
    ]
    for fmt in formats:
        try:
            if fmt.endswith("Z"):
                parsed = datetime.strptime(value.replace("Z", "+0000"), fmt.replace("Z", "%z"))
            else:
                parsed = datetime.strptime(value, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            continue
    # Try fromisoformat as last resort (Python 3.7+)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        pass
    return None


# ── Scweet compatibility wrappers ───────────────────────────────────────────

class _ScweetUser:
    """Mimics twikit.User so callers don't need changes."""

    __slots__ = (
        "_data",
        "id",
        "screen_name",
        "name",
        "description",
        "followers_count",
        "created_at",
        "profile_image_url",
        "profile_image_url_https",
        "profile_banner_url",
        "url",
    )

    def __init__(self, data: dict):
        self._data = data
        self.id = data.get("user_id") or data.get("id") or data.get("rest_id")
        self.screen_name = (
            data.get("username") or data.get("screen_name") or data.get("handle")
        )
        self.name = data.get("name") or data.get("display_name")
        self.description = data.get("description") or data.get("bio")
        self.followers_count = data.get("followers_count")
        self.created_at = data.get("created_at")
        self.profile_image_url = (
            data.get("profile_image_url") or data.get("profile_image_url_https")
        )
        self.profile_image_url_https = self.profile_image_url
        self.profile_banner_url = data.get("profile_banner_url")
        self.url = data.get("url")

    def __repr__(self):
        return f"<_ScweetUser @{self.screen_name}>"


class _ScweetTweet:
    """Mimics twikit.Tweet so callers don't need changes."""

    __slots__ = (
        "_data",
        "id",
        "tweet_id",
        "text",
        "full_text",
        "user",
        "retweeted_tweet",
        "retweeted_status",
        "media",
        "extended_entities",
        "created_at",
    )

    def __init__(self, data: dict):
        self._data = data
        self.id = data.get("tweet_id") or data.get("id")
        self.tweet_id = self.id
        raw = _normalize_graphql_tweet_result(data.get("raw", {}))
        self.text = data.get("text") or data.get("full_text") or _extract_graphql_tweet_text(raw)
        self.full_text = self.text
        self.created_at = data.get("created_at")

        user_data = data.get("user")

        # Extract user_id from raw GraphQL core if available
        user_id = None
        if isinstance(raw, dict):
            core = raw.get("core", {}) or {}
            user_results = core.get("user_results", {}) or {}
            user_result = user_results.get("result", {}) or {}
            user_id = user_result.get("rest_id")

        if isinstance(user_data, dict):
            self.user = _ScweetUser(
                {
                    "user_id": user_id,
                    "username": user_data.get("screen_name"),
                    "name": user_data.get("name"),
                }
            )
        else:
            self.user = None

        self.retweeted_tweet = None
        self.retweeted_status = None
        self.media = data.get("media") or _extract_graphql_media(raw)
        self.extended_entities = {"media": self.media} if self.media else {}

        if isinstance(raw, dict):
            legacy = raw.get("legacy", {}) or {}
            rt_result = legacy.get("retweeted_status_result", {})
            if isinstance(rt_result, dict) and rt_result:
                self.retweeted_tweet = _build_retweet_tweet(rt_result)
                self.retweeted_status = self.retweeted_tweet

    def __repr__(self):
        return f"<_ScweetTweet {self.id}>"


def _normalize_graphql_tweet_result(result: Any) -> Any:
    """Unwrap common GraphQL tweet containers into the actual tweet result."""
    if not isinstance(result, dict):
        return result
    tweet = result.get("tweet")
    if isinstance(tweet, dict):
        return tweet
    return result


def _extract_graphql_tweet_text(result: Any) -> str:
    """Return full tweet text from common X GraphQL shapes, including long Notes."""
    result = _normalize_graphql_tweet_result(result)
    if not isinstance(result, dict):
        return ""
    note = result.get("note_tweet") or {}
    if isinstance(note, dict):
        note_result = (note.get("note_tweet_results") or {}).get("result") or {}
        if isinstance(note_result, dict):
            text = note_result.get("text")
            if text:
                return str(text)
    legacy = result.get("legacy") or {}
    if isinstance(legacy, dict):
        return str(legacy.get("full_text") or legacy.get("text") or "")
    return ""


def _extract_graphql_media(result: Any) -> list:
    result = _normalize_graphql_tweet_result(result)
    if not isinstance(result, dict):
        return []
    legacy = result.get("legacy") or {}
    if not isinstance(legacy, dict):
        return []
    ext = legacy.get("extended_entities") or {}
    if isinstance(ext, dict) and ext.get("media"):
        return list(ext.get("media") or [])
    ents = legacy.get("entities") or {}
    if isinstance(ents, dict) and ents.get("media"):
        return list(ents.get("media") or [])
    return []


def _build_retweet_tweet(rt_result: dict) -> Optional[_ScweetTweet]:
    """Build a fake tweet object from a retweeted_status_result GraphQL node."""
    if not isinstance(rt_result, dict):
        return None
    result = _normalize_graphql_tweet_result(rt_result.get("result", {}))
    if not isinstance(result, dict):
        return None
    legacy = result.get("legacy", {}) or {}
    core = result.get("core", {}) or {}
    user_results = core.get("user_results", {}) or {}
    user_result = user_results.get("result", {}) or {}
    user_legacy = user_result.get("legacy", {}) or {}

    user = _ScweetUser(
        {
            "user_id": user_result.get("rest_id"),
            "username": user_legacy.get("screen_name"),
            "name": user_legacy.get("name"),
            "description": user_legacy.get("description"),
            "followers_count": user_legacy.get("followers_count"),
            "created_at": user_legacy.get("created_at"),
            "profile_image_url": user_legacy.get("profile_image_url_https"),
        }
    )

    tweet = _ScweetTweet(
        {
            "tweet_id": legacy.get("id_str"),
            "text": _extract_graphql_tweet_text(result),
            "created_at": legacy.get("created_at"),
            "media": _extract_graphql_media(result),
            "user": {
                "screen_name": user_legacy.get("screen_name"),
                "name": user_legacy.get("name"),
            },
            "raw": result,
        }
    )
    tweet.user = user
    return tweet


def _scweet_error_to_str(exc: Exception) -> str:
    """Convert a Scweet exception into a string that _mark_session_blocked understands."""
    msg = str(exc) or f"{type(exc).__name__}"
    # Scweet wraps HTTP status codes in diagnostics sometimes
    diagnostics = getattr(exc, "diagnostics", None) or {}
    status_code = diagnostics.get("status_code")
    if status_code:
        msg = f"status: {status_code}, message: {msg}"
    elif isinstance(exc, Exception):
        # Heuristic: look for status codes in the message
        if "429" in msg:
            msg = f"status: 429, message: {msg}"
        elif "403" in msg:
            msg = f"status: 403, message: {msg}"
        elif "401" in msg:
            msg = f"status: 401, message: {msg}"
        elif "502" in msg:
            msg = f"status: 502, message: {msg}"
        elif "504" in msg:
            msg = f"status: 504, message: {msg}"
    return msg


class TwitterClient:
    def __init__(self, cookie_allowlist=None, cookie_blocklist=None, proxy_slice=None, session_slice=None, label="brain"):
        from app_paths import BASE_DIR, DATA_DIR, ensure_dirs

        ensure_dirs()
        self._base_dir = str(BASE_DIR)
        self._cache_path = os.path.join(DATA_DIR, "user_id_cache.json")
        self._accounts_path = os.path.join(DATA_DIR, "accounts.json")
        self._cookies_dir = str(DATA_DIR)
        self._user_id_cache, self._user_id_neg = self._load_cache()
        self._id_handle_cache: dict[str, str] = {}   # reverse lookup
        self.is_rate_limited = False
        self.cooldown_ends = None

        # Pool partitioning: restrict which cookie files / proxies this client owns
        # so two clients (e.g. Brain vs TweetWatcher) never share sessions or quota.
        self.label = str(label or "brain")
        self._cookie_allowlist = {str(c).strip() for c in (cookie_allowlist or []) if str(c).strip()}
        self._cookie_blocklist = {str(c).strip() for c in (cookie_blocklist or []) if str(c).strip()}
        # Filename-agnostic partition: keep only sessions[start:end] after loading
        # all cookies in deterministic order. Both clients load the same set, then
        # each trims to its slice — so the watcher reserve works on any deployment.
        self._session_slice = session_slice

        # Proxy rotation (optionally sliced so two clients use disjoint proxy ranges)
        all_proxies = config.get_proxies()
        if proxy_slice and len(all_proxies) > 1:
            try:
                start, end = int(proxy_slice[0]), int(proxy_slice[1])
                sliced = all_proxies[start:end]
                if sliced:
                    all_proxies = sliced
            except Exception:
                pass
        self._all_proxies = all_proxies
        self._proxy_idx = 0
        
        # User-Agents for stealth
        self._user_agents = [
            # Keep these up-to-date with real Chrome/Firefox releases — stale UAs are a CF signal
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4; rv:125.0) Gecko/20100101 Firefox/125.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0'
        ]
        
        # Session rotation
        self._sessions = []  # List of (client, account_info, logged_in, rate_limited)
        self._current_session_idx = 0
        self._next_twikit_call_ts = 0.0
        self._global_backoff_until_ts = 0.0
        self._cf403_streak = 0
        self._x_failure_events = []
        self._degraded_until_ts = 0.0
        self._degraded_logged_until_ts = 0.0
        # asyncio lock created lazily (can't use asyncio primitives before event loop starts)
        self._session_lock = None  # will be asyncio.Lock once event loop is running
        self._cf403_last_ts = 0.0
        self._cf_hard_blocks_total = 0   # total CF403 hard-blocks across all sessions this run
        self._log_session_rotation = bool(getattr(config, "LOG_TWITTER_SESSION_ROTATION", False))
        self._log_session_health = bool(getattr(config, "LOG_TWITTER_SESSION_HEALTH", False))
        self._log_timeline_fetch = bool(getattr(config, "LOG_TWITTER_TIMELINE_FETCH", False))
        self._log_proxy_backoff = bool(getattr(config, "LOG_TWITTER_PROXY_BACKOFF", False))
        self._load_accounts()

    @staticmethod
    def _redact_proxy(proxy: str) -> str:
        """Hide proxy credentials in logs while keeping host:port visible."""
        p = str(proxy or "").strip()
        if not p:
            return "none"
        if "@" not in p:
            return p
        try:
            scheme, rest = (p.split("://", 1) + [""])[:2] if "://" in p else ("http", p)
            host = rest.split("@")[-1]
            return f"{scheme}://***@{host}"
        except Exception:
            return "***"

    def _health_log(self, message: str) -> None:
        if self._log_session_health:
            print(message)

    def _rotation_log(self, message: str) -> None:
        if self._log_session_rotation:
            print(message)

    def _timeline_log(self, message: str) -> None:
        if self._log_timeline_fetch:
            print(message)

    def _proxy_backoff_log(self, message: str) -> None:
        if self._log_proxy_backoff:
            print(message)

    def _ensure_session_health_fields(self, session) -> None:
        if session is None:
            return
        session.setdefault("health_score", 0)
        session.setdefault("success_count", 0)
        session.setdefault("failure_count", 0)
        session.setdefault("last_success_ts", 0.0)
        session.setdefault("last_failure_ts", 0.0)
        session.setdefault("last_failure_reason", "")

    def _record_session_success(self, session) -> None:
        if session is None:
            return
        self._ensure_session_health_fields(session)
        reward = int(getattr(config, "TWITTER_SESSION_HEALTH_SUCCESS_REWARD", 2) or 2)
        session["success_count"] = int(session.get("success_count", 0) or 0) + 1
        session["last_success_ts"] = time.time()
        session["health_score"] = min(100, int(session.get("health_score", 0) or 0) + max(1, reward))

    def _record_session_failure(self, session, reason: str) -> None:
        if session is None:
            return
        self._ensure_session_health_fields(session)
        penalty = int(getattr(config, "TWITTER_SESSION_HEALTH_FAILURE_PENALTY", 8) or 8)
        session["failure_count"] = int(session.get("failure_count", 0) or 0) + 1
        session["last_failure_ts"] = time.time()
        session["last_failure_reason"] = str(reason or "")[:120]
        session["health_score"] = max(-100, int(session.get("health_score", 0) or 0) - max(1, penalty))
        self._record_x_failure(reason)

    def _record_x_failure(self, reason: str) -> None:
        if not getattr(config, "TWITTER_DEGRADED_MODE", True):
            return
        now = time.time()
        window = float(getattr(config, "TWITTER_DEGRADED_FAILURE_WINDOW_SEC", 300.0) or 300.0)
        self._x_failure_events = [
            (ts, msg)
            for ts, msg in list(getattr(self, "_x_failure_events", []) or [])
            if now - float(ts) <= window
        ]
        self._x_failure_events.append((now, str(reason or "")[:80]))
        threshold = int(getattr(config, "TWITTER_DEGRADED_FAILURE_THRESHOLD", 5) or 5)
        if len(self._x_failure_events) >= max(2, threshold):
            cooldown = float(getattr(config, "TWITTER_DEGRADED_COOLDOWN_SEC", 900.0) or 900.0)
            self._degraded_until_ts = max(float(getattr(self, "_degraded_until_ts", 0.0) or 0.0), now + cooldown)
            if now >= float(getattr(self, "_degraded_logged_until_ts", 0.0) or 0.0):
                until = datetime.fromtimestamp(self._degraded_until_ts).strftime("%H:%M:%S")
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] X degraded mode ON until {until}: "
                    f"{len(self._x_failure_events)} failures in {int(window)}s."
                )
                self._degraded_logged_until_ts = now + 120.0

    def is_degraded_mode(self) -> bool:
        if not getattr(config, "TWITTER_DEGRADED_MODE", True):
            return False
        return float(getattr(self, "_degraded_until_ts", 0.0) or 0.0) > time.time()

    def degraded_mode_summary(self) -> str:
        if not self.is_degraded_mode():
            return "off"
        remaining = max(0, int(float(self._degraded_until_ts) - time.time()))
        return f"on ({remaining}s remaining)"

    def session_health_snapshot(self) -> list:
        rows = []
        for idx, session in enumerate(list(self._sessions or [])):
            self._ensure_session_health_fields(session)
            rows.append(
                {
                    "idx": idx,
                    "label": self._session_debug_label(idx),
                    "health_score": int(session.get("health_score", 0) or 0),
                    "success_count": int(session.get("success_count", 0) or 0),
                    "failure_count": int(session.get("failure_count", 0) or 0),
                    "rate_limited": bool(session.get("rate_limited")),
                    "backoff_until_ts": float(session.get("backoff_until_ts", 0.0) or 0.0),
                    "last_failure_reason": str(session.get("last_failure_reason") or ""),
                }
            )
        return rows

    def _eligible_session_indices(self):
        if not self._sessions:
            return []
        now = time.time()
        min_score = int(getattr(config, "TWITTER_SESSION_HEALTH_MIN_SCORE", -40) or -40)
        eligible = []
        fallback = []
        for idx, session in enumerate(self._sessions):
            self._ensure_session_health_fields(session)
            if session.get("rate_limited"):
                continue
            in_cf_quarantine = float(session.get("cf_quarantine_until_ts", 0.0) or 0.0) > now
            if in_cf_quarantine or self._is_session_backing_off(session):
                continue
            fallback.append(idx)
            if int(session.get("health_score", 0) or 0) >= min_score:
                eligible.append(idx)
        return eligible or fallback

    def _select_best_session_idx(self):
        eligible = self._eligible_session_indices()
        if not eligible:
            return None
        now = time.time()

        def score(idx):
            s = self._sessions[idx]
            health = int(s.get("health_score", 0) or 0)
            # Favor healthy sessions, but penalize very recent use so one good
            # account does not carry the full scan until it fails.
            last_success = float(s.get("last_success_ts", 0.0) or 0.0)
            age_bonus = min(5.0, max(0.0, now - last_success) / 300.0)
            recent_use_penalty = max(0.0, 10.0 - (max(0.0, now - last_success) / 6.0)) if last_success else 0.0
            return (health + age_bonus - recent_use_penalty, -int(s.get("failure_count", 0) or 0), -idx)

        return max(eligible, key=score)

    def _load_accounts(self):
        """Load accounts from accounts.json and create client sessions."""
        def get_next_proxy():
            if not self._all_proxies: return None
            proxy = self._all_proxies[self._proxy_idx % len(self._all_proxies)]
            self._proxy_idx += 1
            return proxy
            
        import random
        def get_random_ua():
            return random.choice(self._user_agents)

        def _cookie_allowed(name: str) -> bool:
            base = os.path.basename(str(name or "").strip())
            if self._cookie_allowlist and base not in self._cookie_allowlist:
                return False
            if base in self._cookie_blocklist:
                return False
            return True

        # Primary cookies: DATA_DIR, project root, Render "Secret Files" (/etc/secrets/<name>).
        def _pick_cookie_file(name: str):
            for base in (self._cookies_dir, self._base_dir, "/etc/secrets"):
                p = os.path.join(base, name)
                if os.path.isfile(p):
                    return p
            return None

        main_cookie_path = (os.getenv("TWIKIT_COOKIES_FILE") or "").strip()
        if main_cookie_path and not os.path.isfile(main_cookie_path):
            main_cookie_path = ""
        if not main_cookie_path:
            main_cookie_path = _pick_cookie_file("cookies.json")
        if main_cookie_path and not _cookie_allowed(main_cookie_path):
            main_cookie_path = None
        if main_cookie_path:
            current_proxy = get_next_proxy()
            client = Client('en-US', proxy=current_proxy)
            client.user_agent = get_random_ua()
            self._sessions.append({
                'client': client,
                'account': None,
                'logged_in': False,
                'rate_limited': False,
                'soft_429_count': 0,
                'soft_403_count': 0,
                'cookie_path': main_cookie_path,
                'proxy': current_proxy,
                'proxy_fails': 0,
                'backoff_exp': 0,
                'backoff_until_ts': 0.0,
                'cf_hard_blocks': 0,
                'cf_consecutive': 0,
                'cf_quarantine_until_ts': 0.0,
                'cookie_file_mtime': 0.0,
                'scweet': None,
                '_auth_token': None,
            })
            proxy_msg = f" (Proxy: {self._redact_proxy(current_proxy)})" if current_proxy else ""
            self._health_log(f"Primary session: cookies.json{proxy_msg}")
        
        # Backup cookies — only load from /etc/secrets (Render Secret Files).
        # DATA_DIR persists across deploys so stale backup files there would
        # keep loading even after they're removed from Secret Files.
        # Supports: cookies_backup.json, cookies_backup2.json, ... cookies_backup20.json
        backup_names = ["cookies_backup.json"] + [f"cookies_backup{i}.json" for i in range(2, 21)]
        for backup_name in backup_names:
            if not _cookie_allowed(backup_name):
                continue
            # Only search /etc/secrets — never DATA_DIR or project root for backups
            backup_cookie_path = None
            secrets_path = os.path.join("/etc/secrets", backup_name)
            if os.path.isfile(secrets_path):
                backup_cookie_path = secrets_path
            if not backup_cookie_path:
                continue
            current_proxy = get_next_proxy()
            client = Client('en-US', proxy=current_proxy)
            client.user_agent = get_random_ua()
            self._sessions.append({
                'client': client,
                'account': None,
                'logged_in': False,
                'rate_limited': False,
                'soft_429_count': 0,
                'soft_403_count': 0,
                'cookie_path': backup_cookie_path,
                'proxy': current_proxy,
                'proxy_fails': 0,
                'backoff_exp': 0,
                'backoff_until_ts': 0.0,
                'cf_hard_blocks': 0,
                'cf_consecutive': 0,
                'cf_quarantine_until_ts': 0.0,
                'cookie_file_mtime': 0.0,
                'scweet': None,
                '_auth_token': None,
            })
            self._health_log(f"   + Backup session: {backup_name} (Proxy: {self._redact_proxy(current_proxy)})")
        
        # Then add accounts from accounts.json as backup sessions.
        # accounts.json may live in DATA_DIR, project root, or Render /etc/secrets —
        # search all three (same as cookie files) so prod secret-file uploads load.
        accounts_path = self._accounts_path
        if not os.path.isfile(accounts_path):
            for base in (self._cookies_dir, self._base_dir, "/etc/secrets"):
                cand = os.path.join(base, "accounts.json")
                if os.path.isfile(cand):
                    accounts_path = cand
                    break
        if os.path.isfile(accounts_path):
            try:
                with open(accounts_path, 'r') as f:
                    accounts = json.load(f)

                # Purge stale per-account cookie files from DATA_DIR for any
                # account that has auth_token+ct0 in accounts.json.  Those files
                # accumulate across deploys and contain expired __cf_bm tokens;
                # sending them merges stale CF state into fresh auth, causing
                # immediate re-blocks.  We always re-auth from accounts.json on
                # startup, so the files are never needed.
                for acc in accounts:
                    if acc.get('auth_token'):
                        stale_path = os.path.join(self._cookies_dir, f"cookies_{acc['username']}.json")
                        if os.path.isfile(stale_path):
                            try:
                                os.remove(stale_path)
                            except Exception:
                                pass

                for acc in accounts:
                    # Resolve the account's cookie file wherever it lives (DATA_DIR,
                    # root, or /etc/secrets); fall back to DATA_DIR path for new logins.
                    cookie_name = f"cookies_{acc['username']}.json"
                    cookie_file = _pick_cookie_file(cookie_name) or os.path.join(self._cookies_dir, cookie_name)
                    if not _cookie_allowed(cookie_file):
                        continue
                    # Add session even if cookies don't exist yet - _login will handle it
                    current_proxy = get_next_proxy()
                    client = Client('en-US', proxy=current_proxy)
                    client.user_agent = get_random_ua()
                    self._sessions.append({
                        'client': client,
                        'account': acc,
                        'logged_in': False,
                        'rate_limited': False,
                        'soft_429_count': 0,
                        'soft_403_count': 0,
                        'cookie_path': cookie_file,
                        'proxy': current_proxy,
                        'proxy_fails': 0,
                        'backoff_exp': 0,
                        'backoff_until_ts': 0.0,
                        'cf_hard_blocks': 0,
                        'cf_consecutive': 0,
                        'cf_quarantine_until_ts': 0.0,
                        'cookie_file_mtime': 0.0,
                        'scweet': None,
                        '_auth_token': None,
                    })
                    has_cookies = os.path.exists(cookie_file)
                    cookie_msg = "(with cookies)" if has_cookies else "(new login)"
                    self._health_log(f"   + Backup session: @{acc['username']} {cookie_msg} (Proxy: {self._redact_proxy(current_proxy)})")
            except Exception as e:
                print(f"WARN: Error loading accounts.json: {e}")
        
        if not self._sessions:
            print("ERROR: No sessions available! Need cookies.json or account cookies.", flush=True)
            print(f"   TIP Put cookies here (recommended): {os.path.join(self._cookies_dir, 'cookies.json')}", flush=True)
            print(f"   TIP Or next to main.py (legacy): {os.path.join(self._base_dir, 'cookies.json')}", flush=True)
            print(
                "   TIP Render Secret Files: upload as cookies.json → available at /etc/secrets/cookies.json",
                flush=True,
            )
            print("   TIP Or set TWIKIT_COOKIES_FILE=/absolute/path/to/cookies.json", flush=True)
        else:
            self._health_log(
                f"Total sessions available: {len(self._sessions)} | Proxies loaded: {len(self._all_proxies)}"
            )
        # Apply count-based partition (filename-agnostic). Only trim if it leaves
        # at least one session; otherwise keep all (caller falls back to shared pool).
        if self._session_slice and self._sessions:
            start, end = self._session_slice
            trimmed = self._sessions[start:end]
            if trimmed:
                self._sessions = trimmed
        # Always-on summary so pool separation is visible in production logs.
        _files = [os.path.basename(s.get('cookie_path') or '?') for s in self._sessions]
        print(
            f"[Pool:{self.label}] {len(self._sessions)} session(s), "
            f"{len(self._all_proxies)} proxies: {_files}",
            flush=True,
        )
        self._normalize_session_idx()

    # ── Scweet helpers ───────────────────────────────────────────────────────

    def _extract_auth_token(self, cookie_path: str) -> Optional[str]:
        """Extract auth_token from a browser-export cookie file."""
        if not os.path.exists(cookie_path):
            return None
        try:
            with open(cookie_path, "r", encoding="utf-8") as f:
                cookies = json.load(f)
            if isinstance(cookies, list):
                for c in cookies:
                    if isinstance(c, dict) and c.get("name") == "auth_token":
                        return c.get("value")
            elif isinstance(cookies, dict):
                return cookies.get("auth_token")
        except Exception:
            pass
        return None

    def _ensure_scweet(self, session: dict) -> Optional[Any]:
        """Lazy-init a Scweet instance for the given session."""
        if session.get("scweet") is not None:
            return session["scweet"]

        auth_token = session.get("_auth_token")
        if not auth_token:
            auth_token = self._extract_auth_token(session.get("cookie_path", ""))
            session["_auth_token"] = auth_token

        if not auth_token:
            return None

        proxy = session.get("proxy")
        # Unique DB per session to avoid SQLite conflicts
        cookie_file = session.get("cookie_path", "default")
        db_name = (
            "scweet_state_"
            + os.path.splitext(os.path.basename(cookie_file))[0]
            + ".db"
        )
        db_path = os.path.join(self._cookies_dir, db_name)

        try:
            from Scweet import Scweet

            scweet = Scweet(
                auth_token=auth_token,
                proxy=proxy,
                db_path=db_path,
                manifest_scrape_on_init=False,
            )
            session["scweet"] = scweet
            return scweet
        except Exception as e:
            print(f"      WARN Failed to init Scweet for session: {e}")
            return None

    def _normalize_session_idx(self):
        """Keep _current_session_idx in range (e.g. after deploy / pool changes)."""
        if not self._sessions:
            self._current_session_idx = 0
            return
        n = len(self._sessions)
        if self._current_session_idx < 0 or self._current_session_idx >= n:
            self._current_session_idx %= n

    def _get_current_session(self):
        """Get current active session, rotating if rate limited."""
        if not self._sessions:
            self.is_rate_limited = False
            return None
        self._normalize_session_idx()
        if getattr(config, "TWITTER_SESSION_HEALTH_SCORING", True):
            best_idx = self._select_best_session_idx()
            if best_idx is not None:
                self._current_session_idx = int(best_idx)
                return self._sessions[self._current_session_idx]

        has_non_limited = False
        attempts = 0
        while attempts < len(self._sessions):
            session = self._sessions[self._current_session_idx]
            if not session["rate_limited"]:
                has_non_limited = True
                in_cf_quarantine = float(session.get("cf_quarantine_until_ts", 0.0) or 0.0) > time.time()
                if not self._is_session_backing_off(session) and not in_cf_quarantine:
                    return session
            self._rotate_session()
            attempts += 1

        # Backoff-only state (not all hard rate-limited): wait and retry upstream.
        if has_non_limited:
            self.is_rate_limited = False
            return None

        # All sessions hard rate-limited
        self.is_rate_limited = True
        if not self._sessions:
            return None
        self._normalize_session_idx()
        return self._sessions[self._current_session_idx]

    def check_cooldown(self):
        """Checks if the cooldown period has expired and resets flags."""
        if self.cooldown_ends:
            if datetime.now() > self.cooldown_ends:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Cooldown expired. Resuming operations.")
                self.cooldown_ends = None
                self.is_rate_limited = False
                self._global_backoff_until_ts = 0.0
                stale_sessions = []
                for s in self._sessions:
                    s['rate_limited'] = False
                    s['soft_429_count'] = 0
                    s['soft_403_count'] = 0
                    s['backoff_exp'] = 0
                    s['backoff_until_ts'] = 0.0
                    s['cf_hard_blocks'] = 0
                    s['cf_consecutive'] = 0
                    s['cf_quarantine_until_ts'] = 0.0
                    # Force cookie reload from disk — picks up any freshly dropped cookie file.
                    s['logged_in'] = False
                    # Check whether the cookie file on disk is still the same burned one.
                    cookie_path = s.get('cookie_path', '')
                    stored_mtime = float(s.get('cookie_file_mtime') or 0.0)
                    if stored_mtime > 0 and cookie_path and os.path.exists(cookie_path):
                        try:
                            current_mtime = os.path.getmtime(cookie_path)
                            if abs(current_mtime - stored_mtime) < 2.0:
                                uname = s['account']['username'] if s.get('account') else 'default'
                                stale_sessions.append((uname, cookie_path))
                        except Exception:
                            pass
                if stale_sessions:
                    print(
                        f"[{datetime.now().strftime('%H:%M:%S')}] "
                        "⚠️  STALE COOKIES DETECTED — the following cookie file(s) have NOT been "
                        "refreshed since they were blocked by Cloudflare:"
                    )
                    for uname, path in stale_sessions:
                        print(f"   @{uname} → {path}")
                    print(
                        "   The bot will retry, but will likely hit CF403 again immediately.\n"
                        "   ACTION REQUIRED: export fresh browser cookies and overwrite the file(s) above."
                    )
                self._cf_hard_blocks_total = 0
            else:
                remaining = int((self.cooldown_ends - datetime.now()).total_seconds() / 60)
                # Optional: print(f"Info: Cooling down... {remaining}m remaining")

    async def _twikit_pace(self):
        """Small gap between Twikit calls to reduce 429 bursts (same pool for HVA + search + art)."""
        base_gap = float(getattr(config, "TWIKIT_REQUEST_GAP_SEC", 0) or 0)
        burst_factor = float(getattr(config, "TWIKIT_BURST_REDUCTION_FACTOR", 1.0) or 1.0)
        jitter = float(getattr(config, "TWIKIT_REQUEST_GAP_JITTER_SEC", 0) or 0)
        gap = max(0.0, base_gap * max(1.0, burst_factor))
        jitter = max(0.0, jitter)

        now = time.monotonic()
        if now < self._next_twikit_call_ts:
            await asyncio.sleep(self._next_twikit_call_ts - now)

        self._next_twikit_call_ts = max(time.monotonic(), self._next_twikit_call_ts) + gap + random.uniform(0, jitter)

        # Legacy round-robin rotation. In health-scoring mode, _ensure_session()
        # chooses the next session before the request and we keep the current
        # index stable so failures are charged to the session that made the call.
        if not getattr(config, "TWITTER_SESSION_HEALTH_SCORING", True):
            self._rotate_session()

    @staticmethod
    def _reset_soft_429(session):
        if session is not None:
            session["soft_429_count"] = 0
            session["soft_403_count"] = 0
            session["backoff_exp"] = 0
            session["backoff_until_ts"] = 0.0
            session["proxy_fails"] = 0
            session["cf_consecutive"] = 0
            session["success_count"] = int(session.get("success_count", 0) or 0) + 1
            session["last_success_ts"] = time.time()
            reward = int(getattr(config, "TWITTER_SESSION_HEALTH_SUCCESS_REWARD", 2) or 2)
            session["health_score"] = min(100, int(session.get("health_score", 0) or 0) + max(1, reward))

    def _is_session_backing_off(self, session):
        until_ts = float(session.get("backoff_until_ts", 0.0) or 0.0)
        return until_ts > time.time()

    def _in_global_backoff(self):
        return float(self._global_backoff_until_ts or 0.0) > time.time()

    def _register_cf403_and_maybe_global_cooldown(self):
        now = time.time()
        window_sec = float(getattr(config, "TWIKIT_CF_STREAK_WINDOW_SEC", 120.0) or 120.0)
        trigger_n = int(getattr(config, "TWIKIT_CF_STREAK_FOR_GLOBAL_COOLDOWN", 3) or 3)
        cool_sec = float(getattr(config, "TWIKIT_CF_GLOBAL_COOLDOWN_SEC", 180.0) or 180.0)

        if (now - float(self._cf403_last_ts or 0.0)) <= max(1.0, window_sec):
            self._cf403_streak += 1
        else:
            self._cf403_streak = 1
        self._cf403_last_ts = now

        if self._cf403_streak >= max(1, trigger_n):
            self._cf403_streak = 0
            # Don't extend if we're already in a longer backoff window.
            new_until = now + max(5.0, cool_sec)
            if new_until > float(self._global_backoff_until_ts or 0.0):
                self._global_backoff_until_ts = new_until
                resume_at = datetime.fromtimestamp(self._global_backoff_until_ts).strftime("%H:%M:%S")
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] Cloudflare 403 streak detected. "
                    f"Pausing Twikit pool until {resume_at} (~{int(cool_sec)}s)."
                )

    def _schedule_session_backoff(self, session, reason):
        base = float(getattr(config, "TWIKIT_BACKOFF_BASE_SEC", 8.0) or 8.0)
        max_sec = float(getattr(config, "TWIKIT_BACKOFF_MAX_SEC", 300.0) or 300.0)
        exp = int(session.get("backoff_exp", 0) or 0) + 1
        session["backoff_exp"] = min(exp, 10)
        wait = min(max_sec, base * (2 ** max(0, session["backoff_exp"] - 1)))
        jitter = random.uniform(0, max(1.0, base * 0.5))
        wait = float(wait + jitter)
        session["backoff_until_ts"] = time.time() + wait
        username = session['account']['username'] if session.get('account') else 'default'
        self._proxy_backoff_log(
            f"[{datetime.now().strftime('%H:%M:%S')}] Backoff @{username}: wait {wait:.1f}s "
            f"(exp={session['backoff_exp']}, reason={reason[:80]})"
        )

    def _rotate_proxy_for_session(self, session, reason_tag):
        if not self._all_proxies:
            return False
        max_proxy_fails = max(1, int(getattr(config, "TWIKIT_PROXY_ROTATIONS_PER_SESSION", 8) or 8))
        if int(session.get("proxy_fails", 0) or 0) >= max_proxy_fails:
            return False

        session["proxy_fails"] = int(session.get("proxy_fails", 0) or 0) + 1
        new_proxy = self._all_proxies[self._proxy_idx % len(self._all_proxies)]
        self._proxy_idx += 1
        old_ua = getattr(session['client'], 'user_agent', random.choice(self._user_agents))
        session['client'] = Client('en-US', proxy=new_proxy)
        session['client'].user_agent = old_ua
        session['proxy'] = new_proxy
        session['logged_in'] = False  # Force cookie reload on next ensure
        # Also reset Scweet so it picks up the new proxy next init
        session['scweet'] = None
        session['_auth_token'] = None
        try:
            sid = self._sessions.index(session)
        except ValueError:
            sid = None
        transport_name = type(getattr(session['client'], 'http', None)).__name__
        self._proxy_backoff_log(
            f"[{datetime.now().strftime('%H:%M:%S')}] {reason_tag} for "
            f"{self._session_debug_label(sid)}. Rotated proxy -> {self._redact_proxy(new_proxy)} "
            f"(transport: {transport_name})"
        )
        return True

    def get_current_username(self):
        """Get the username of the current session."""
        if not self._sessions:
            return "default"
        self._normalize_session_idx()
        session = self._sessions[self._current_session_idx]
        return session['account']['username'] if session.get('account') else 'default'

    def _session_debug_label(self, idx: Optional[int] = None) -> str:
        """Readable session label with slot, cookie file and proxy tail."""
        if not self._sessions:
            return "@default [no-sessions]"
        if idx is None:
            self._normalize_session_idx()
            idx = self._current_session_idx
        try:
            s = self._sessions[int(idx)]
        except Exception:
            return "@default [invalid-session]"
        uname = s['account']['username'] if s.get('account') else 'default'
        cpath = str(s.get('cookie_path') or "")
        cfile = os.path.basename(cpath) if cpath else "no-cookie-file"
        proxy = str(s.get('proxy') or "")
        proxy_tail = self._redact_proxy(proxy)
        slot = int(idx) + 1
        total = len(self._sessions)
        return f"@{uname} [slot {slot}/{total}] cookie={cfile} proxy={proxy_tail or 'none'}"

    def _rotate_session(self):
        """Rotate to the next non-rate-limited session, or full circle."""
        if not self._sessions:
            return False
        if len(self._sessions) <= 1:
            return False
        self._normalize_session_idx()
        if getattr(config, "TWITTER_SESSION_HEALTH_SCORING", True):
            best_idx = self._select_best_session_idx()
            if best_idx is None:
                return False
            changed = int(best_idx) != self._current_session_idx
            self._current_session_idx = int(best_idx)
            if changed:
                self._rotation_log(
                    f"[{datetime.now().strftime('%H:%M:%S')}] Session Rotation: "
                    f"Selected healthy {self._session_debug_label(self._current_session_idx)}"
                )
            return True
        original_idx = self._current_session_idx
        while True:
            self._current_session_idx = (self._current_session_idx + 1) % len(self._sessions)
            if not self._sessions[self._current_session_idx]['rate_limited']:
                self._rotation_log(
                    f"[{datetime.now().strftime('%H:%M:%S')}] Session Rotation: "
                    f"Switched to {self._session_debug_label(self._current_session_idx)}"
                )
                return True
            if self._current_session_idx == original_idx:
                return False

    def _mark_session_blocked(self, reason):
        """Mark current session as blocked/rate-limited and rotate."""
        if not self._sessions:
            self.is_rate_limited = False
            return
        reason = str(reason or "")
        self._normalize_session_idx()
        session = self._sessions[self._current_session_idx]
        username = session['account']['username'] if session['account'] else 'default'
        self._record_session_failure(session, reason)
        
        # Proxy / transport errors (include ReadTimeout — else slow proxies block the whole pool)
        is_proxy_err = any(
            x in reason
            for x in (
                "522",
                "502",
                "504",
                "500",
                "ConnectTimeout",
                "ReadTimeout",
                "Timeout",
                "Connection reset",
                "Connection aborted",
            )
        )
        lower_reason = (reason or "").lower()
        is_cf_403 = "403" in reason and (
            "cloudflare" in lower_reason or "<html" in lower_reason or "forbidden" in lower_reason
        )

        if is_proxy_err:
            self._schedule_session_backoff(session, reason or "proxy/network")
            self._rotate_proxy_for_session(session, "Proxy/Network error")
            self._rotate_session()
            return

        # Transient X throttling: a few 429s → rotate cookie sessions instead of locking everyone out.
        if "429" in reason and not is_proxy_err:
            soft = int(session.get("soft_429_count", 0) or 0) + 1
            session["soft_429_count"] = soft
            self._schedule_session_backoff(session, reason or "429")
            cap = int(getattr(config, "TWIKIT_429_SOFT_PER_SESSION", 8) or 8)
            if soft < cap:
                self._proxy_backoff_log(
                    f"[{datetime.now().strftime('%H:%M:%S')}] WAIT 429 throttle ({soft}/{cap}) @{username} — "
                    "rotating session/proxy (soft backoff; not hard-blocking yet)."
                )
                self._rotate_proxy_for_session(session, "HTTP 429 throttle")
                self._rotate_session()
                return
            session["soft_429_count"] = 0

        # Cloudflare 403 often means cookie+IP fingerprint risk on this route.
        # Soft-handle first with proxy/session rotation + exponential backoff.
        if is_cf_403:
            soft403 = int(session.get("soft_403_count", 0) or 0) + 1
            session["soft_403_count"] = soft403
            session["cf_consecutive"] = int(session.get("cf_consecutive", 0) or 0) + 1
            self._schedule_session_backoff(session, reason or "Cloudflare 403")
            self._register_cf403_and_maybe_global_cooldown()
            rotated_proxy = self._rotate_proxy_for_session(session, "Cloudflare 403 block")
            cap403 = int(getattr(config, "TWIKIT_403_SOFT_PER_SESSION", 2) or 2)
            if rotated_proxy and soft403 < cap403:
                self._proxy_backoff_log(
                    f"[{datetime.now().strftime('%H:%M:%S')}] WAIT Cloudflare 403 ({soft403}/{cap403}) @{username} — "
                    "proxy rotated, soft block."
                )
                self._rotate_session()
                return
            if not rotated_proxy and soft403 < cap403:
                self._proxy_backoff_log(
                    f"[{datetime.now().strftime('%H:%M:%S')}] WAIT Cloudflare 403 ({soft403}/{cap403}) @{username} — "
                    "soft block, no proxy left."
                )
                self._rotate_session()
                return
            # Exhausted soft budget — hard-block this session and count the burn.
            session["soft_403_count"] = 0
            session["cf_hard_blocks"] = int(session.get("cf_hard_blocks", 0) or 0) + 1
            self._cf_hard_blocks_total += 1
            # Quarantine burned cookie session to avoid immediate reuse thrashing.
            qsec = float(getattr(config, "TWIKIT_CF_SESSION_QUARANTINE_SEC", 1800.0) or 1800.0)
            session["cf_quarantine_until_ts"] = max(
                float(session.get("cf_quarantine_until_ts", 0.0) or 0.0),
                time.time() + max(60.0, qsec),
            )

        session['rate_limited'] = True
        
        # Clean up reason if it's a massive HTML block
        clean_reason = reason
        if "403" in reason and "<html" in reason.lower():
            ray_id = "Unknown"
            match = re.search(r"Cloudflare Ray ID: <strong>(.*?)</strong>", reason)
            if match: ray_id = match.group(1)
            clean_reason = f"Cloudflare 403 Block (Ray ID: {ray_id}). Cookies/IP flagged."
        elif len(reason) > 150:
            clean_reason = reason[:147] + "..."

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Session @{username} blocked: {clean_reason}")
        self._rotate_session()
        
        if self._sessions and all(s['rate_limited'] for s in self._sessions):
            self.is_rate_limited = True
            cool_m = int(getattr(config, "TWIKIT_ALL_SESSIONS_COOLDOWN_MIN", 45) or 45)
            self.cooldown_ends = datetime.now() + timedelta(minutes=cool_m)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ALL SESSIONS BLOCKED. Cooldown until {self.cooldown_ends.strftime('%H:%M:%S')} (~{cool_m}m).")
            print("Bot will automatically retry after cooldown.")
            if self._cf_hard_blocks_total >= 2:
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] 🚨 COOKIES BURNED "
                    f"({self._cf_hard_blocks_total} CF403 hard-block(s) this run). "
                    "Proxy rotation cannot fix this — the cookies themselves are flagged by Cloudflare."
                )
                for s in self._sessions:
                    cp = s.get('cookie_path', '')
                    uname = s['account']['username'] if s.get('account') else 'default'
                    if cp:
                        print(f"   ACTION: export fresh browser cookies → {cp}  (@{uname})")
                print(
                    "   The bot will retry after the cooldown but will fail again unless you "
                    "replace the cookie file(s) above with freshly exported ones."
                )


    async def verify_all_sessions(self):
        """Verify all sessions and proxies on startup."""
        self._health_log(f"\n[{datetime.now().strftime('%H:%M:%S')}] Checking session health...")
        valid_count = 0
        for i, session in enumerate(self._sessions):
            username = session['account']['username'] if session['account'] else 'default'
            
            attempts = 0
            while attempts < 3: # Up to 3 attempts with different proxies
                proxy_str = f" | Proxy: {self._redact_proxy(session.get('proxy'))}" if session.get('proxy') else ""
                try:
                    # Ensure logged in (twikit)
                    if not session['logged_in']:
                        success, err = await self._login(session)
                        if not success:
                            # If failed, treat as error and check for proxy rotation
                            raise Exception(err or "Login failed")
                    
                    # Test connectivity/auth with Scweet (preferred) or twikit fallback
                    healthy = False
                    scweet = self._ensure_scweet(session)
                    if scweet is not None:
                        try:
                            profiles = await scweet.aget_user_info(["Twitter"])
                            if profiles:
                                healthy = True
                                self._health_log(f"   OK Session @{username} is healthy (Scweet){proxy_str}")
                            else:
                                raise Exception("Scweet returned empty")
                        except Exception as se_err:
                            # Scweet failed, try twikit fallback
                            err_msg = _scweet_error_to_str(se_err)
                            if "403" in err_msg or "429" in err_msg:
                                raise Exception(err_msg)
                            # Non-auth error: try twikit
                            pass
                    
                    if not healthy:
                        try:
                            await session['client'].get_user_by_screen_name('Twitter')
                            healthy = True
                            transport_name = type(getattr(session['client'], 'http', None)).__name__
                            self._health_log(f"   OK Session @{username} is healthy (twikit fallback){proxy_str} (transport: {transport_name})")
                        except KeyError as ke:
                            transport_name = type(getattr(session['client'], 'http', None)).__name__
                            self._health_log(f"   OK Session @{username} cookies valid (twikit parse warn: {ke}){proxy_str} (transport: {transport_name})")
                            healthy = True

                    if healthy:
                        valid_count += 1
                        break
                    
                except Exception as e:
                    err_msg = str(e)
                    # KEY_BYTE / ClientTransaction errors are Twitter-side JS drift,
                    # NOT auth failures. Treat them like transient network errors so
                    # we don't wrongly kill the session pool.
                    is_twikit_internal = any(
                        kw in err_msg
                        for kw in [
                            "KEY_BYTE",
                            "key_byte",
                            "ClientTransaction",
                            "Couldn't get key",
                            "invalid response",
                            "twitter-site-verification",
                        ]
                    )
                    is_network_err = is_twikit_internal or any(
                        code in err_msg
                        for code in [
                            "522",
                            "502",
                            "504",
                            "500",
                            "ConnectTimeout",
                            "ReadTimeout",
                            "Timeout",
                        ]
                    )
                    # Error 353 = CSRF mismatch — stale ct0, not a permanent IP/cookie block.
                    # Reset session cookies so next attempt re-derives fresh CSRF from X.
                    is_csrf_err = "353" in err_msg
                    if is_csrf_err:
                        session['logged_in'] = False
                        try:
                            session['client'].http.cookies.clear()
                        except Exception:
                            pass
                    is_blocked_err = "403" in err_msg and not is_twikit_internal and not is_csrf_err
                    
                    # CSRF error (353): already reset session above — let it retry once without proxy rotation
                    if is_csrf_err and attempts == 0:
                        attempts += 1
                        continue

                    # If network/proxy error and we have more proxies, rotate and retry
                    max_proxy_fails = min(len(self._all_proxies), 3) if self._all_proxies else 0
                    if (is_network_err or is_blocked_err) and self._all_proxies and attempts < max_proxy_fails:
                        attempts += 1
                        new_proxy = self._all_proxies[self._proxy_idx % len(self._all_proxies)]
                        self._proxy_idx += 1
                        
                        # Self-healing proxy rotation — quiet by default to keep the
                        # console focused on scan activity. Set LOG_PROXY_ROTATION=1 to see it.
                        if os.getenv("LOG_PROXY_ROTATION", "0").strip().lower() in ("1", "true", "yes", "on"):
                            err_type = "Timed Out (522)" if "522" in err_msg else ("Flagged (403)" if "403" in err_msg else "Network Failure")
                            print(f"   WARN Session @{username} {err_type}. Rotating proxy and retrying... (Attempt {attempts}/{max_proxy_fails})")
                        
                        # Re-initialize client
                        import random
                        old_ua = getattr(session['client'], 'user_agent', random.choice(self._user_agents))
                        session['client'] = Client('en-US', proxy=new_proxy)
                        session['client'].user_agent = old_ua
                        session['proxy'] = new_proxy
                        session['logged_in'] = False
                        session['scweet'] = None
                        session['_auth_token'] = None
                        continue # Try again with new proxy
                    else:
                        # Permanent failure or out of retries
                        reason = f"[{type(e).__name__}] {err_msg}" if err_msg else f"[{type(e).__name__}] (Empty error)"
                        if is_twikit_internal:
                            print(f"   WARN Session @{username} twikit-internal error (Twitter JS drift): {reason}{proxy_str}")
                            print(
                                "      TIP This is usually temporary — monkey-patch should handle it. "
                                "If persistent, re-export cookies or update twikit."
                            )
                        elif is_csrf_err:
                            print(f"   WARN Session @{username} CSRF error (353) — stale ct0, session reset{proxy_str}")
                        elif "403" in err_msg:
                            print(f"   WARN Session @{username} is BLOCKED (403): {reason}{proxy_str}")
                        else:
                            print(f"   ERROR Session @{username} failure: {reason}{proxy_str}")
                            if "401" in err_msg or "Unauthorized" in err_msg or "Could not authenticate" in err_msg:
                                print(
                                    "      TIP Twitter auth failed — export fresh cookies for this account or check proxy/IP."
                                )
                        
                        session['rate_limited'] = True # Disable for now
                        break # Give up on this session
        
        if valid_count == 0:
            if not self._sessions:
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] ERROR No Twikit cookie sessions configured "
                    "(brain scan / X search need cookies.json)."
                )
                print(f"   TIP Export cookies to: {os.path.join(self._cookies_dir, 'cookies.json')}")
                # Not the same as "rate limited" — do not set global cooldown when there is no pool.
                self.is_rate_limited = False
                self.cooldown_ends = None
                return
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR NO VALID SESSIONS FOUND. Entering Cooldown Mode.")
            self.is_rate_limited = True
            cool_m = int(getattr(config, "TWIKIT_ALL_SESSIONS_COOLDOWN_MIN", 45) or 45)
            self.cooldown_ends = datetime.now() + timedelta(minutes=cool_m)
            print(f"Bot will retry in ~{cool_m} minutes (at {self.cooldown_ends.strftime('%H:%M:%S')}).")
        else:
            self._health_log(f"Startup check complete. {valid_count}/{len(self._sessions)} sessions ready.\n")
            # Reset current session to first valid one
            for i, s in enumerate(self._sessions):
                if not s['rate_limited']:
                    self._current_session_idx = i
                    break

    async def _ensure_session(self):
        """Ensure a logged-in session is available.

        Uses an asyncio.Lock so concurrent callers don't all wake up at once
        after a backoff window and hammer X simultaneously.
        """
        if not self._sessions:
            return None

        # Lazy-init lock (must be inside a running event loop).
        if self._session_lock is None:
            self._session_lock = asyncio.Lock()

        # Outer check: if we're in a long hard-cooldown, wait outside the lock
        # so we don't hold it for minutes.
        if self.cooldown_ends and datetime.now() < self.cooldown_ends:
            wait_sec = max(1.0, (self.cooldown_ends - datetime.now()).total_seconds())
            await asyncio.sleep(min(wait_sec, 30.0))

        async with self._session_lock:
            self.check_cooldown()

            # Global CF-403 backoff: sleep the full remaining time, once, under lock.
            if self._in_global_backoff():
                wait = max(0.5, float(self._global_backoff_until_ts) - time.time())
                await asyncio.sleep(wait)
                self._global_backoff_until_ts = 0.0
                # Extra mandatory rest after a CF streak cooldown so we don't burst immediately.
                post_jitter = float(getattr(config, "TWIKIT_CF_POST_COOLDOWN_JITTER_SEC", 90.0) or 0.0)
                if post_jitter > 0:
                    extra = random.uniform(post_jitter * 0.5, post_jitter)
                    await asyncio.sleep(extra)

            if self.is_rate_limited:
                return None

            session = self._get_current_session()
            if not session:
                # All sessions in per-session backoff: sleep until the soonest one expires.
                backoffs = [float(s.get("backoff_until_ts", 0.0) or 0.0) for s in (self._sessions or [])]
                now_ts = time.time()
                future = [t for t in backoffs if t > now_ts]
                if future:
                    wait = max(0.5, min(future) - now_ts)
                    await asyncio.sleep(wait)
                    session = self._get_current_session()
                if not session:
                    return None

            success, err = await self._login(session)
            if success:
                return session
            self._mark_session_blocked(err or "Login failed")
            return None

    def _load_cache(self):
        """
        Load resolver cache.
        Backwards compatible:
        - old format: {handle: "12345", ...}
        - new format: {"ids": {...}, "neg": {...}}
        """
        ids: dict = {}
        neg: dict = {}
        if os.path.exists(self._cache_path):
            try:
                with open(self._cache_path, "r", encoding="utf-8") as f:
                    data = json.load(f) or {}
                if isinstance(data, dict) and ("ids" in data or "neg" in data):
                    ids = data.get("ids") or {}
                    neg = data.get("neg") or {}
                elif isinstance(data, dict):
                    ids = data
                    neg = {}
                # normalize
                ids = {str(k).lower(): str(v) for k, v in (ids or {}).items() if v is not None}
                neg = {
                    str(k).lower(): float(v)
                    for k, v in (neg or {}).items()
                    if v is not None and str(v).replace(".", "", 1).isdigit()
                }
                print(
                    f"Loaded {len(ids)} IDs (+{len(neg)} negative) from Resolver Cache "
                    f"(prevents re-searching handles)."
                )
            except Exception:
                print("WARN: Error loading User ID cache.")
                ids, neg = {}, {}
        return ids, neg

    def _save_cache(self):
        try:
            payload = {"ids": self._user_id_cache, "neg": self._user_id_neg}
            with open(self._cache_path, "w", encoding="utf-8") as f:
                json.dump(payload, f)
        except Exception:
            pass

    async def _login(self, session=None):
        """Login a specific session or current session. Returns (success, error_reason)."""
        if session is None:
            session = self._get_current_session()
        
        if session['logged_in']:
            return True, None

        client = session['client']
        cookie_path = session['cookie_path']
        account = session['account']

        try:
            # Load any cookies that exist on disk first.
            file_cookies = None
            if os.path.exists(cookie_path):
                with open(cookie_path, 'r') as f:
                    cookies_data = json.load(f)
                if isinstance(cookies_data, list):
                    file_cookies = {
                        c['name']: c['value']
                        for c in cookies_data if 'name' in c and 'value' in c
                    }
                elif isinstance(cookies_data, dict):
                    file_cookies = dict(cookies_data)

            # twikit builds the X-Csrf-Token header from the ct0 *cookie*. A cookie
            # set without ct0 → no CSRF header → every request fails with error 353.
            # The vendor accounts give us auth_token (40-char) + ct0 (160-char) in
            # accounts.json, so if the on-disk file is missing ct0 (or doesn't exist),
            # rebuild authoritative cookies from the account credentials.
            has_token_creds = bool(account and account.get('auth_token'))

            # If accounts.json has auth_token+ct0, always use those — they are the
            # authoritative vendor credentials. Stale cookie files have caused
            # "invalid response" blocks when their auth_token expired on X's side.
            # Fall back to cookie file ONLY when accounts.json has no credentials.
            if not has_token_creds:
                if file_cookies:
                    client.set_cookies(file_cookies)
                    username = account['username'] if account else 'default'
                    self._health_log(f"OK Loaded cookies for @{username}")
                    session['logged_in'] = True
                    try:
                        session['cookie_file_mtime'] = os.path.getmtime(cookie_path)
                    except Exception:
                        pass
                    return True, None

            # Build canonical cookies from accounts.json credentials.
            if has_token_creds:
                username_str = account.get('username', '?')
                cookies = {'auth_token': account['auth_token']}
                # ct0 cookie MUST be present so twikit sends a matching CSRF header.
                # Use the account's ct0 if given; otherwise generate one (X uses the
                # double-submit pattern, so any cookie==header value is accepted).
                ct0 = account.get('ct0')
                if not ct0:
                    import secrets
                    ct0 = secrets.token_hex(80)  # 160 hex chars, X ct0 format
                cookies['ct0'] = ct0
                # Do NOT merge stale passive cookies (__cf_bm, guest_id) from disk.
                # __cf_bm is Cloudflare's Bot Manager token — it is bound to the
                # originating IP + TLS fingerprint + expiry. Sending an expired or
                # mismatched __cf_bm tells CF "this is a replay from a flagged session"
                # and triggers an immediate 403, even when auth_token+ct0 are valid.
                print(f"Using auth_token + ct0 for @{username_str}...")
                client.set_cookies(cookies)
                session['logged_in'] = True
                # Set mtime=0 so the stale-cookie warning never fires for
                # accounts.json sessions — we always re-auth from credentials,
                # not from disk files, so the file mtime is irrelevant.
                session['cookie_file_mtime'] = 0.0
                return True, None

            if account:

                print(f"🔐 Logging in as @{account['username']}...")
                await client.login(
                    auth_info_1=account['username'],
                    auth_info_2=account['email'],
                    password=account['password'],
                    enable_ui_metrics=True
                )
                client.save_cookies(cookie_path)
                session['logged_in'] = True
                print(f"OK Logged in and saved cookies for @{account['username']}")
                return True, None
            else:
                print("ERROR No credentials available for login")
                return False, "No credentials"
                
        except Exception as e:
            username = account['username'] if account else 'default'
            timestamp = datetime.now().strftime('%H:%M:%S')
            err_msg = str(e)
            if "403" in err_msg:
                print(f"[{timestamp}] Cloudflare Block (403) for @{username}. Sessions may need refresh.")
            elif "429" in err_msg:
                print(f"[{timestamp}] WAIT Rate Limit (429) during login for @{username}")
            else:
                print(f"[{timestamp}] ERROR Login failed for @{username}: {e}")
            return False, err_msg



    # ── Scweet-powered read methods ──────────────────────────────────────────

    async def get_user_id(self, handle, _retry_depth=0):
        # Normalize handle
        handle = handle.lower()
        if handle in self._user_id_cache:
            return self._user_id_cache[handle]

        # Negative cache: avoid repeatedly querying handles that don't exist / are inaccessible.
        until = float(self._user_id_neg.get(handle) or 0)
        if until and time.time() < until:
            return None

        if self.is_rate_limited:
            return None

        print(f"      📡 Looking up ID for @{handle}...")
        session = await self._ensure_session()
        if not session:
            return None

        scweet = self._ensure_scweet(session)
        if scweet is None:
            # Fallback to twikit
            return await self._get_user_id_twikit(handle, session, _retry_depth)

        try:
            await self._twikit_pace()
            profiles = await scweet.aget_user_info([handle])
            if profiles:
                profile = profiles[0]
                user_id = profile.get("user_id")
                if user_id:
                    self._reset_soft_429(session)
                    self._user_id_cache[handle] = str(user_id)
                    self._id_handle_cache[str(user_id)] = handle
                    self._save_cache()
                    return str(user_id)
            return None
        except Exception as e:
            err_msg = _scweet_error_to_str(e)
            if not err_msg:
                err_msg = f"Empty {type(e).__name__}"

            if any(code in err_msg for code in ["429", "503", "403", "502", "504"]) or "Empty" in err_msg or "Timeout" in err_msg or "SSL" in err_msg or "invalid response" in err_msg:
                self._mark_session_blocked(err_msg)
                if _retry_depth < 2:
                    return await self.get_user_id(handle, _retry_depth=_retry_depth + 1)
                return None
            low = err_msg.lower()
            if "does not exist" in low or "not found" in low or "no such user" in low:
                self._user_id_neg[handle] = time.time() + (7 * 24 * 3600)
                self._save_cache()
            print(f"      ERROR Lookup error for {handle}: {err_msg}")
            return None

    async def _get_user_id_twikit(self, handle, session, _retry_depth=0):
        """Twikit fallback for get_user_id."""
        try:
            await self._twikit_pace()
            user = await session['client'].get_user_by_screen_name(handle)
            if user:
                self._reset_soft_429(session)
                self._user_id_cache[handle] = user.id
                self._id_handle_cache[str(user.id)] = handle
                self._save_cache()
                return user.id
            return None
        except Exception as e:
            err_msg = str(e)
            if any(code in err_msg for code in ["429", "503", "403", "502", "504"]) or "Empty" in err_msg or "Timeout" in err_msg or "SSL" in err_msg or "invalid response" in err_msg:
                self._mark_session_blocked(err_msg)
                if _retry_depth < 2:
                    return await self._get_user_id_twikit(handle, session, _retry_depth + 1)
                return None
            print(f"      ERROR Lookup error for {handle}: {err_msg}")
            return None

    async def get_new_following(self, user_id, _retry_depth=0):
        """Deprecated wrapper — delegates to with_delta version."""
        result, _ = await self.get_new_following_with_delta(user_id, "")
        return result

    async def get_new_following_with_delta(self, user_id, hva_handle, _retry_depth=0):
        """Get new following with delta detection - only processes when count changes."""
        import database

        if self.is_rate_limited:
            return [], 0

        session = await self._ensure_session()
        if not session:
            return [], 0

        scweet = self._ensure_scweet(session)
        if scweet is None:
            return await self._get_new_following_with_delta_twikit(user_id, hva_handle, _retry_depth)

        handle = hva_handle or self._id_handle_cache.get(str(user_id))
        if not handle:
            print(f"      WARN Cannot fetch following without handle for user_id {user_id}")
            return [], 0

        try:
            await self._twikit_pace()
            profiles = await scweet.aget_following([handle], limit=20, raw_json=True)
            if not profiles:
                return [], 0

            current_count = len(profiles)
            last_count = database.get_hva_last_follows_count(hva_handle)
            database.update_hva_follows_count(hva_handle, current_count)
            delta = current_count - last_count if last_count > 0 else current_count

            newly_followed_accounts = []
            seen_ids = set()
            now = datetime.utcfromtimestamp(datetime.now().timestamp()).replace(tzinfo=timezone.utc)
            count_new = 0
            for profile in profiles:
                try:
                    uid = profile.get("user_id")
                    if uid is not None:
                        uid_key = str(uid)
                        if uid_key in seen_ids:
                            continue
                        seen_ids.add(uid_key)
                    created_at = _parse_twitter_date(profile.get("created_at"))
                    if created_at is None:
                        print(f"         ⚠️  Could not parse created_at for {profile.get('username') or profile.get('screen_name') or profile.get('user_id')}: {profile.get('created_at')!r}")
                        continue
                    if (now - created_at).days <= config.SNIPER_MAX_AGE_DAYS:
                        newly_followed_accounts.append(_ScweetUser(profile))
                        count_new += 1
                except Exception as _exc:
                    print(f"         ⚠️  Error processing follow profile: {_exc}")
                    continue

            # Populate reverse cache so process_discovery can use Scweet for these accounts
            for user in newly_followed_accounts:
                if user.id and user.screen_name:
                    self._id_handle_cache[str(user.id)] = user.screen_name.lower()

            print(f"      ✔ Found {current_count} follows ({count_new} Potential Projects, delta: {delta})")
            self._reset_soft_429(session)
            return newly_followed_accounts, delta
        except Exception as e:
            err_msg = _scweet_error_to_str(e)
            if not err_msg:
                err_msg = f"Empty {type(e).__name__}"

            if any(code in err_msg for code in ["429", "503", "403", "502", "504", "522"]) or "Empty" in err_msg or "Timeout" in err_msg or "SSL" in err_msg or "invalid response" in err_msg:
                print(f"      Retrying due to {err_msg}...")
                self._mark_session_blocked(err_msg)
                if _retry_depth < 2:
                    return await self.get_new_following_with_delta(user_id, hva_handle, _retry_depth=_retry_depth + 1)
                return [], 0
            print(f"      ERROR Following error (ID: {user_id}): {err_msg}")
            return [], 0

    async def _get_new_following_with_delta_twikit(self, user_id, hva_handle, _retry_depth=0):
        """Twikit fallback for following."""
        import database

        session = await self._ensure_session()
        if not session:
            return [], 0
        try:
            await self._twikit_pace()
            following = await session['client'].get_user_following(user_id, count=20)
            if not following:
                return [], 0

            current_count = len(following)
            last_count = database.get_hva_last_follows_count(hva_handle)
            database.update_hva_follows_count(hva_handle, current_count)
            delta = current_count - last_count if last_count > 0 else current_count

            newly_followed_accounts = []
            seen_ids = set()
            now = datetime.utcfromtimestamp(datetime.now().timestamp()).replace(tzinfo=timezone.utc)
            count_new = 0
            for user in following:
                try:
                    uid = getattr(user, "id", None)
                    if uid is not None:
                        uid_key = str(uid)
                        if uid_key in seen_ids:
                            continue
                        seen_ids.add(uid_key)
                    created_at = _parse_twitter_date(user.created_at)
                    if created_at is None:
                        print(f"         ⚠️  Could not parse created_at for {getattr(user, 'screen_name', getattr(user, 'name', getattr(user, 'id', '?')))}: {getattr(user, 'created_at', None)!r}")
                        continue
                    if (now - created_at).days <= config.SNIPER_MAX_AGE_DAYS:
                        newly_followed_accounts.append(user)
                        count_new += 1
                except Exception as _exc:
                    print(f"         ⚠️  Error processing follow user: {_exc}")
                    continue

            # Populate reverse cache so process_discovery can use Scweet for these accounts
            for user in newly_followed_accounts:
                if getattr(user, "id", None) and getattr(user, "screen_name", None):
                    self._id_handle_cache[str(user.id)] = user.screen_name.lower()

            print(f"      ✔ Found {current_count} follows ({count_new} Potential Projects, delta: {delta})")
            self._reset_soft_429(session)
            return newly_followed_accounts, delta
        except Exception as e:
            err_msg = str(e)
            if not err_msg:
                err_msg = f"Empty {type(e).__name__}"
            if any(code in err_msg for code in ["429", "503", "403", "502", "504", "522"]) or "Empty" in err_msg or "Timeout" in err_msg or "SSL" in err_msg or "invalid response" in err_msg:
                print(f"      Retrying due to {err_msg}...")
                self._mark_session_blocked(err_msg)
                if _retry_depth < 2:
                    return await self._get_new_following_with_delta_twikit(user_id, hva_handle, _retry_depth + 1)
                return [], 0
            print(f"      ERROR Following error (ID: {user_id}): {err_msg}")
            return [], 0

    async def get_user_timeline(self, user_id, count=20, handle=None, _retry_depth=0):
        if self.is_rate_limited:
            return []
        if not user_id:
            return []
        session = await self._ensure_session()
        if not session:
            return []

        # Scweet needs a handle; resolve from cache or parameter
        lookup_handle = handle or self._id_handle_cache.get(str(user_id))
        if not lookup_handle:
            # Try twikit fallback which works with IDs
            return await self._get_user_timeline_twikit(user_id, count, _retry_depth)

        scweet = self._ensure_scweet(session)
        if scweet is None:
            return await self._get_user_timeline_twikit(user_id, count, _retry_depth)

        try:
            await self._twikit_pace()
            timeline_timeout = float(getattr(config, "TWITTER_TIMELINE_CLIENT_TIMEOUT_SEC", 18.0) or 18.0)
            tweets = await asyncio.wait_for(
                scweet.aget_profile_tweets([lookup_handle], limit=count),
                timeout=timeline_timeout,
            )
            wrapped = [_ScweetTweet(t) for t in tweets]
            self._timeline_log(f"      ✔ Fetched {len(wrapped)} timeline items")
            self._reset_soft_429(session)
            if not wrapped:
                # Scweet sometimes silently returns [] for accounts that do have tweets
                # (rate limit, graph reshape, etc). Try twikit before giving up.
                tw = await self._get_user_timeline_twikit(user_id, count, _retry_depth)
                if tw:
                    return tw
            return wrapped
        except Exception as e:
            err_msg = _scweet_error_to_str(e)
            if "'value'" in err_msg or "'entries'" in err_msg:
                self._timeline_log("      ℹ️ Timeline: Scweet empty graph — falling back to twikit")
                tw = await self._get_user_timeline_twikit(user_id, count, _retry_depth)
                return tw or []
            if not err_msg:
                err_msg = f"Empty {type(e).__name__}"
            if "Timeout" in err_msg or "invalid response" in err_msg:
                self._mark_session_blocked(err_msg)
                self._timeline_log("      ℹ️ Timeline: Scweet stalled — falling back to twikit")
                tw = await self._get_user_timeline_twikit(user_id, count, _retry_depth)
                if tw:
                    return tw
            if any(code in err_msg for code in ["429", "503", "403", "502", "504", "522"]) or "Empty" in err_msg or "Timeout" in err_msg or "SSL" in err_msg or "invalid response" in err_msg:
                self._mark_session_blocked(err_msg)
                max_retries = int(getattr(config, "TWITTER_TIMELINE_CLIENT_RETRIES", 0) or 0)
                if _retry_depth < max_retries:
                    return await self.get_user_timeline(user_id, count, handle=handle, _retry_depth=_retry_depth + 1)
                return []
            print(f"      ERROR Timeline error (ID: {user_id}): {err_msg}")
            return []

    async def _get_user_timeline_twikit(self, user_id, count=20, _retry_depth=0):
        """Twikit fallback for timeline."""
        session = await self._ensure_session()
        if not session:
            return []
        try:
            await self._twikit_pace()
            timeline_timeout = float(getattr(config, "TWITTER_TIMELINE_CLIENT_TIMEOUT_SEC", 18.0) or 18.0)
            tweets = await asyncio.wait_for(
                session['client'].get_user_tweets(user_id, 'Tweets', count=count),
                timeout=timeline_timeout,
            )
            self._timeline_log(f"      ✔ Fetched {len(tweets)} timeline items")
            self._reset_soft_429(session)
            return tweets
        except Exception as e:
            err_msg = str(e)
            if "'value'" in err_msg or "'entries'" in err_msg:
                self._timeline_log("      ℹ️ Timeline: No tweets / empty graph (0 tweets, restricted, or API shape)")
                return []
            if not err_msg:
                err_msg = f"Empty {type(e).__name__}"
            if any(code in err_msg for code in ["429", "503", "403", "502", "504", "522"]) or "Empty" in err_msg or "Timeout" in err_msg or "SSL" in err_msg or "invalid response" in err_msg:
                self._mark_session_blocked(err_msg)
                max_retries = int(getattr(config, "TWITTER_TIMELINE_CLIENT_RETRIES", 0) or 0)
                if _retry_depth < max_retries:
                    return await self._get_user_timeline_twikit(user_id, count, _retry_depth + 1)
                return []
            print(f"      ERROR Timeline error (ID: {user_id}): {err_msg}")
            return []

    async def get_user_info(self, user_id, handle=None, _retry_depth=0):
        if self.is_rate_limited:
            return None
        if not user_id:
            return None
        session = await self._ensure_session()
        if not session:
            return None

        lookup_handle = handle or self._id_handle_cache.get(str(user_id))
        if not lookup_handle:
            return await self._get_user_info_twikit(user_id, _retry_depth)

        scweet = self._ensure_scweet(session)
        if scweet is None:
            return await self._get_user_info_twikit(user_id, _retry_depth)

        try:
            await self._twikit_pace()
            profiles = await scweet.aget_user_info([lookup_handle])
            if profiles:
                self._reset_soft_429(session)
                return _ScweetUser(profiles[0])
            return None
        except Exception as e:
            err_msg = _scweet_error_to_str(e)
            if not err_msg:
                err_msg = f"Empty {type(e).__name__}"
            if any(code in err_msg for code in ["429", "503", "403", "502", "504", "522"]) or "Empty" in err_msg or "Timeout" in err_msg or "SSL" in err_msg or "invalid response" in err_msg:
                self._mark_session_blocked(err_msg)
                if _retry_depth < 2:
                    return await self.get_user_info(user_id, handle=handle, _retry_depth=_retry_depth + 1)
                return None
            print(f"      ERROR User info error (ID: {user_id}): {err_msg}")
            return None

    async def _get_user_info_twikit(self, user_id, _retry_depth=0):
        """Twikit fallback for user info."""
        session = await self._ensure_session()
        if not session:
            return None
        try:
            await self._twikit_pace()
            u = await session['client'].get_user_by_id(user_id)
            self._reset_soft_429(session)
            return u
        except Exception as e:
            err_msg = str(e)
            if not err_msg:
                err_msg = f"Empty {type(e).__name__}"
            if any(code in err_msg for code in ["429", "503", "403", "502", "504", "522"]) or "Empty" in err_msg or "Timeout" in err_msg or "SSL" in err_msg or "invalid response" in err_msg:
                self._mark_session_blocked(err_msg)
                if _retry_depth < 2:
                    return await self._get_user_info_twikit(user_id, _retry_depth + 1)
                return None
            print(f"      ERROR User info error (ID: {user_id}): {err_msg}")
            return None

    async def get_user_by_handle(self, handle: str, _retry_depth=0):
        """Fetch a full user object from a @handle (screen_name)."""
        handle = (handle or "").strip().lstrip("@")
        if not handle:
            return None

        if self.is_rate_limited:
            return None
        session = await self._ensure_session()
        if not session:
            return None

        scweet = self._ensure_scweet(session)

        # If Scweet's aget_user_info has been timing out repeatedly for this session,
        # bypass it entirely and go straight to twikit (aget_profile_tweets still works).
        _scweet_ui_fails = int(session.get("_scweet_user_info_timeouts", 0) or 0)
        _scweet_ui_skip = int(getattr(config, "MENTION_SCWEET_BYPASS_AFTER_TIMEOUTS", 3) or 3)

        if scweet is None or _scweet_ui_fails >= _scweet_ui_skip:
            return await self._get_user_by_handle_twikit(handle, _retry_depth)

        # Internal timeout: aget_user_info can hang indefinitely with curl_cffi
        # (curl doesn't honour asyncio cancellation). Apply a hard cap here so
        # the caller's outer asyncio.wait_for doesn't have to do all the work.
        # Split outer timeout 50/50 between Scweet and Twikit fallback so
        # Twikit actually gets time to respond instead of being starved.
        _outer = float(getattr(config, "MENTION_RESOLVE_TIMEOUT_SEC", 12.0) or 12.0)
        _inner_timeout = max(5.0, _outer * 0.5)
        try:
            await self._twikit_pace()
            profiles = await asyncio.wait_for(
                scweet.aget_user_info([handle]),
                timeout=_inner_timeout,
            )
            if profiles:
                self._reset_soft_429(session)
                session["_scweet_user_info_timeouts"] = 0  # reset on success
                user = _ScweetUser(profiles[0])
                # Cache the mapping
                if user.id:
                    self._user_id_cache[handle.lower()] = str(user.id)
                    self._id_handle_cache[str(user.id)] = handle.lower()
                    self._save_cache()
                return user
            return None
        except asyncio.TimeoutError:
            # Track consecutive Scweet user_info timeouts for this session
            session["_scweet_user_info_timeouts"] = _scweet_ui_fails + 1
            if _scweet_ui_fails + 1 >= _scweet_ui_skip:
                print(
                    f"      ⚠️ Scweet aget_user_info timed out {_scweet_ui_fails + 1}x — "
                    "switching to twikit for user lookups this session."
                )
            # Mark session blocked to rotate proxy, then fall through to twikit
            self._mark_session_blocked("ReadTimeout")
            return await self._get_user_by_handle_twikit(handle, _retry_depth)
        except Exception as e:
            err_msg = _scweet_error_to_str(e)
            if not err_msg:
                err_msg = f"Empty {type(e).__name__}"
            if any(code in err_msg for code in ["429", "503", "403", "502", "504", "522"]) or "Timeout" in err_msg or "SSL" in err_msg or "invalid response" in err_msg:
                self._mark_session_blocked(err_msg)
                if _retry_depth < 2:
                    return await self.get_user_by_handle(handle, _retry_depth=_retry_depth + 1)
                return None
            return None

    async def _get_user_by_handle_twikit(self, handle: str, _retry_depth=0):
        """Twikit fallback for get_user_by_handle."""
        session = await self._ensure_session()
        if not session:
            return None
        try:
            await self._twikit_pace()
            user = await session["client"].get_user_by_screen_name(handle)
            self._reset_soft_429(session)
            return user
        except Exception as e:
            err_msg = str(e) or f"Empty {type(e).__name__}"
            if any(code in err_msg for code in ["429", "503", "403", "502", "504", "522"]) or "Timeout" in err_msg or "SSL" in err_msg or "invalid response" in err_msg:
                self._mark_session_blocked(err_msg)
                if _retry_depth < 2:
                    return await self._get_user_by_handle_twikit(handle, _retry_depth + 1)
                return None
            return None

    async def search_recent_tweets(self, query: str, count: int = 15, _retry_depth: int = 0):
        """
        Best-effort recent search using Scweet.
        Returns a list of tweet objects (may be empty).
        """
        if self.is_rate_limited:
            return []
        query = (query or "").strip()
        if not query:
            return []

        session = await self._ensure_session()
        if not session:
            return []

        count = max(1, min(50, int(count or 15)))

        scweet = self._ensure_scweet(session)
        if scweet is None:
            return await self._search_recent_tweets_twikit(query, count, _retry_depth)

        try:
            await self._twikit_pace()
            search_timeout = float(getattr(config, "X_PROJECT_SEARCH_CLIENT_TIMEOUT_SEC", 18.0) or 18.0)
            tweets = await asyncio.wait_for(
                scweet.asearch(query, limit=count),
                timeout=search_timeout,
            )
            wrapped = [_ScweetTweet(t) for t in tweets]
            # Cache tweet authors so downstream process_discovery can use Scweet
            for t in wrapped:
                if t.user and t.user.id and t.user.screen_name:
                    self._id_handle_cache[str(t.user.id)] = t.user.screen_name.lower()
            self._reset_soft_429(session)
            return wrapped
        except Exception as e:
            err_msg = _scweet_error_to_str(e)
            if not err_msg:
                err_msg = f"Empty {type(e).__name__}"
            if any(code in err_msg for code in ["429", "503", "403", "502", "504", "522"]) or "Timeout" in err_msg or "SSL" in err_msg or "invalid response" in err_msg:
                self._mark_session_blocked(err_msg)
                max_retries = int(getattr(config, "X_PROJECT_SEARCH_CLIENT_RETRIES", 0) or 0)
                if _retry_depth < max_retries:
                    return await self.search_recent_tweets(query, count=count, _retry_depth=_retry_depth + 1)
                return []
            return []

    async def _search_recent_tweets_twikit(self, query: str, count: int = 15, _retry_depth: int = 0):
        """Twikit fallback for search."""
        session = await self._ensure_session()
        if not session:
            return []
        try:
            await self._twikit_pace()
            client = session["client"]
            search_timeout = float(getattr(config, "X_PROJECT_SEARCH_CLIENT_TIMEOUT_SEC", 18.0) or 18.0)
            if hasattr(client, "search_tweet"):
                try:
                    out = list(await asyncio.wait_for(
                        client.search_tweet(query, product="Latest", count=count),
                        timeout=search_timeout,
                    ))
                except TypeError:
                    out = list(await asyncio.wait_for(
                        client.search_tweet(query, "Latest", count=count),
                        timeout=search_timeout,
                    ))
                self._reset_soft_429(session)
                return out
            if hasattr(client, "search_tweets"):
                try:
                    out = list(await asyncio.wait_for(
                        client.search_tweets(query, product="Latest", count=count),
                        timeout=search_timeout,
                    ))
                except TypeError:
                    out = list(await asyncio.wait_for(
                        client.search_tweets(query, "Latest", count=count),
                        timeout=search_timeout,
                    ))
                self._reset_soft_429(session)
                return out
        except Exception as e:
            err_msg = str(e) or f"Empty {type(e).__name__}"
            if any(code in err_msg for code in ["429", "503", "403", "502", "504", "522"]) or "Timeout" in err_msg or "SSL" in err_msg or "invalid response" in err_msg:
                self._mark_session_blocked(err_msg)
                max_retries = int(getattr(config, "X_PROJECT_SEARCH_CLIENT_RETRIES", 0) or 0)
                if _retry_depth < max_retries:
                    return await self._search_recent_tweets_twikit(query, count, _retry_depth + 1)
                return []
        return []

    async def get_x_profile_art(self, handle: str):
        """
        Return (profile_image_https, banner_https) for a handle.
        Uses Scweet when possible; falls back to unavatar.io for PFP only.
        """
        handle = (handle or "").strip().lstrip("@")
        if not handle:
            return None, None

        fallback_pfp = f"https://unavatar.io/twitter/{handle}"

        if self.is_rate_limited:
            return fallback_pfp, None

        session = await self._ensure_session()
        if not session:
            return fallback_pfp, None

        scweet = self._ensure_scweet(session)
        if scweet is None:
            return await self._get_x_profile_art_twikit(handle, session)

        try:
            await self._twikit_pace()
            profiles = await scweet.aget_user_info([handle])
            if not profiles:
                return fallback_pfp, None

            self._reset_soft_429(session)
            profile = profiles[0]
            pfp = profile.get("profile_image_url")
            if pfp and "_normal" in str(pfp):
                pfp = str(pfp).replace("_normal", "_400x400")

            banner = profile.get("profile_banner_url")
            return (pfp or fallback_pfp), banner
        except Exception as e:
            err_msg = _scweet_error_to_str(e)
            if any(code in err_msg for code in ["429", "503", "403", "502", "504", "522"]):
                self._mark_session_blocked(err_msg)
            return fallback_pfp, None

    async def _get_x_profile_art_twikit(self, handle: str, session):
        """Twikit fallback for profile art."""
        fallback_pfp = f"https://unavatar.io/twitter/{handle}"
        try:
            await self._twikit_pace()
            user = await session["client"].get_user_by_screen_name(handle)
            if not user:
                return fallback_pfp, None

            self._reset_soft_429(session)
            pfp = getattr(user, "profile_image_url", None) or getattr(
                user, "profile_image_url_https", None
            )
            if pfp and "_normal" in str(pfp):
                pfp = str(pfp).replace("_normal", "_400x400")

            banner = getattr(user, "profile_banner_url", None)
            if not banner and hasattr(user, "_data"):
                leg = (user._data or {}).get("legacy") or {}
                banner = leg.get("profile_banner_url") or leg.get(
                    "profile_banner_url_https"
                )

            return (pfp or fallback_pfp), banner
        except Exception as e:
            err_msg = str(e)
            if any(
                code in err_msg
                for code in ["429", "503", "403", "502", "504", "522"]
            ):
                self._mark_session_blocked(err_msg)
            if "ClientTransaction" in err_msg and "attribute" in err_msg:
                return fallback_pfp, None
            if "Multiple cookies exist" in err_msg:
                return fallback_pfp, None
            if isinstance(e, KeyError):
                return fallback_pfp, None
            print(f"      WARN get_x_profile_art @{handle}: {err_msg[:120]}")
            return fallback_pfp, None

    async def create_tweet(self, text):
        """Creates a new tweet using the primary session (twikit)."""
        if self.is_rate_limited:
            return False
        
        # Always use the first session for posting (primary account)
        session = await self._ensure_session()
        if not session:
            return False
        
        try:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Posting to X...")
            response = await session['client'].create_tweet(text)
            print(f"   OK Tweet posted! ID: {getattr(response, 'id', 'Unknown')}")
            return True
        except Exception as e:
            err_msg = str(e)
            print(f"   ERROR Tweet failed: {err_msg}")
            if any(code in err_msg for code in ["429", "503", "403"]):
                 self._mark_session_blocked(err_msg)
            return False

    async def get_hva_followers(self, user_id, handle=None):
        """Get list of HVAs from our list that follow this account."""
        if self.is_rate_limited:
            return []
        if not user_id:
            return []
        session = await self._ensure_session()
        if not session:
            return []

        lookup_handle = handle or self._id_handle_cache.get(str(user_id))
        if not lookup_handle:
            return await self._get_hva_followers_twikit(user_id)

        scweet = self._ensure_scweet(session)
        if scweet is None:
            return await self._get_hva_followers_twikit(user_id)

        try:
            profiles = await scweet.aget_followers([lookup_handle], limit=100, raw_json=True)
            if not profiles:
                return []

            hva_set = set([h.lower() for h in config.HVA_LIST])
            matching_hvas = []
            for profile in profiles:
                screen_name = (profile.get("username") or "").lower()
                if screen_name in hva_set:
                    matching_hvas.append(screen_name)
            return matching_hvas
        except Exception as e:
            err_msg = _scweet_error_to_str(e)
            if any(code in err_msg for code in ["429", "503", "403"]):
                self._mark_session_blocked(err_msg)
            return await self.get_hva_followers(user_id, handle=handle)

    async def _get_hva_followers_twikit(self, user_id):
        """Twikit fallback for HVA followers."""
        session = await self._ensure_session()
        if not session:
            return []
        try:
            followers = await session['client'].get_user_followers(user_id, count=100)
            if not followers:
                return []
            
            hva_set = set([h.lower() for h in config.HVA_LIST])
            matching_hvas = []
            for follower in followers:
                screen_name = getattr(follower, 'screen_name', '').lower()
                if screen_name in hva_set:
                    matching_hvas.append(follower.screen_name)
            return matching_hvas
        except Exception as e:
            err_msg = str(e)
            if any(code in err_msg for code in ["429", "503", "403"]):
                self._mark_session_blocked(err_msg)
            return await self._get_hva_followers_twikit(user_id)

    async def get_first_followers(self, user_id, limit=1000, screen_name: str | None = None):
        """Fetch followers (newest-first). Uses Scweet; falls back to twikit."""
        if self.is_rate_limited:
            return None
        if not user_id and not screen_name:
            return None
        if not await self._ensure_session():
            return None

        lookup_handle = screen_name or self._id_handle_cache.get(str(user_id))
        if not lookup_handle:
            return await self._get_first_followers_twikit(user_id, limit, screen_name)

        session = await self._ensure_session()
        if not session:
            return None

        scweet = self._ensure_scweet(session)
        if scweet is None:
            return await self._get_first_followers_twikit(user_id, limit, screen_name)

        try:
            profiles = await scweet.aget_followers([lookup_handle], limit=limit, raw_json=True)
            if not profiles:
                return [], True
            wrapped = [_ScweetUser(p) for p in profiles]
            # Cache followers so downstream lookups can use Scweet
            for u in wrapped:
                if u.id and u.screen_name:
                    self._id_handle_cache[str(u.id)] = u.screen_name.lower()
            return wrapped, True
        except Exception as e:
            err_msg = _scweet_error_to_str(e)
            print(f"      WARN get_first_followers Scweet error: {err_msg}")
            return await self._get_first_followers_twikit(user_id, limit, screen_name)

    async def _get_first_followers_twikit(self, user_id, limit=1000, screen_name: str | None = None):
        """Twikit fallback for first followers (REST followers/list)."""
        if self.is_rate_limited:
            return None
        if not await self._ensure_session():
            return None

        uid_s = (str(user_id).strip() if user_id is not None else "") or None
        sn_s = (str(screen_name or "").strip().lstrip("@") if screen_name else "") or None
        count_target = int(limit or 0) or 1000

        return await self._get_followers_via_followers_list(uid_s, sn_s, count_target)

    @staticmethod
    async def _followers_list_paged_one_client(
        client,
        uid: str | None,
        sn: str | None,
        limit: int,
    ) -> tuple[list, bool]:
        """REST followers/list (newest first) for one twikit Client instance."""
        if not hasattr(client, "get_latest_followers"):
            return [], True
        all_followers: list = []
        cursor = None
        count_target = max(1, int(limit or 1000))
        while len(all_followers) < count_target:
            page = min(200, max(1, count_target - len(all_followers)))
            resp = await client.get_latest_followers(
                user_id=uid,
                screen_name=sn if not uid else None,
                count=page,
                cursor=cursor,
            )
            batch = list(resp)
            if not batch:
                break
            all_followers.extend(batch)
            nc = getattr(resp, "next_cursor", None)
            if nc is None or nc == 0 or (isinstance(nc, str) and nc.strip() in ("", "0")):
                break
            cursor = nc
            await asyncio.sleep(1.0)
        is_partial = len(all_followers) >= count_target
        return all_followers, is_partial

    async def _get_followers_via_followers_list(
        self,
        user_id: str | None,
        screen_name: str | None,
        limit: int,
    ) -> tuple[list, bool]:
        """
        REST 1.1 followers/list via twikit get_latest_followers (GraphQL Followers often 404s).

        Tries every non-blocked cookie session; one account may hit limits while another works.
        """
        uid = (str(user_id).strip() if user_id else "") or None
        sn = (str(screen_name or "").strip().lstrip("@") if screen_name else "") or None
        if not uid and not sn:
            return [], True

        sessions = list(self._sessions or [])
        if not sessions:
            return [], True

        errs: list[str] = []
        for si, sess in enumerate(sessions):
            if sess.get("rate_limited"):
                continue
            ok, err = await self._login(sess)
            if not ok:
                errs.append(f"login:{err}")
                continue
            client = sess["client"]
            un = (sess.get("account") or {}).get("username") or str(si)
            try:
                rows, partial = await self._followers_list_paged_one_client(
                    client, uid, sn, int(limit or 1000)
                )
                if rows:
                    self._reset_soft_429(sess)
                    return rows, partial
            except Exception as e:
                msg = (str(e) or type(e).__name__)[:200]
                errs.append(f"@{un}:{msg}")
                continue

        if errs:
            print(
                f"      WARN followers/list: no session returned data ({len(errs)} tries). "
                f"First: {errs[0]}"
            )
        return [], True

    @staticmethod
    def _about_account_url() -> str:
        from twikit.constants import DOMAIN

        # AboutAccountQuery (same operation as twikit PR #398 "About this account").
        return f"https://{DOMAIN}/i/api/graphql/zs_jFPFT78rBpXv9Z3U2YQ/AboutAccountQuery"

    @staticmethod
    def _parse_about_account_response(response: dict) -> dict | None:
        """Parse AboutAccountQuery JSON into a flat dict (count / region / verification)."""
        data = (response or {}).get("data") or {}
        ur = data.get("user_result_by_screen_name")
        if not isinstance(ur, dict):
            return None
        user_data = ur.get("result")
        if not isinstance(user_data, dict):
            return None
        if user_data.get("__typename") == "UserUnavailable":
            return None

        about = user_data.get("about_profile") or {}
        core = user_data.get("core") or {}
        verification = user_data.get("verification_info") or {}
        reason = verification.get("reason") or {}
        uc = about.get("username_changes") or {}

        def _to_int(v):
            if v is None:
                return None
            try:
                return int(v)
            except (TypeError, ValueError):
                return None

        count = _to_int(uc.get("count"))
        last_ms = _to_int(uc.get("last_changed_at_msec"))
        verified_since = _to_int(reason.get("verified_since_msec"))

        last_iso = None
        if last_ms is not None:
            try:
                last_iso = datetime.fromtimestamp(
                    last_ms / 1000.0, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M UTC")
            except Exception:
                last_iso = None

        return {
            "rest_id": user_data.get("rest_id"),
            "screen_name": core.get("screen_name"),
            "name": core.get("name"),
            "account_based_in": about.get("account_based_in"),
            "location_accurate": about.get("location_accurate"),
            "affiliate_username": about.get("affiliate_username"),
            "source": about.get("source"),
            "username_change_count": count,
            "username_last_changed_ms": last_ms,
            "username_last_changed_iso": last_iso,
            "is_identity_verified": verification.get("is_identity_verified"),
            "verified_since_ms": verified_since,
        }

    async def fetch_about_account(self, screen_name: str) -> dict | None:
        """
        X "About this account" (GraphQL): username change count, last change time, region, etc.
        Does not include the list of old @handles (X does not return them here).
        Uses twikit (custom GraphQL not supported by Scweet).
        """
        sn = (screen_name or "").strip().lstrip("@")
        if not sn:
            return None
        if self.is_rate_limited:
            return None

        url = self._about_account_url()
        variables = {"screenName": sn}
        features = {"responsive_web_graphql_timeline_navigation_enabled": True}

        for sess in list(self._sessions or []):
            if sess.get("rate_limited"):
                continue
            ok, _err = await self._login(sess)
            if not ok:
                continue
            client = sess.get("client")
            gql = getattr(client, "gql", None) if client else None
            if gql is None:
                continue
            try:
                await self._twikit_pace()
                response, _ = await gql.gql_get(url, variables, features)
                parsed = self._parse_about_account_response(response)
                if parsed is not None:
                    self._reset_soft_429(sess)
                    return parsed
            except Exception as e:
                err_msg = str(e) or type(e).__name__
                # Do not treat 403/404 as pool-wide blocks; try other sessions.
                if any(code in err_msg for code in ["429", "503", "502", "504"]):
                    self._mark_session_blocked(err_msg)
                continue

        return None


class AIAnalyzer:
    def __init__(self):
        from openai import AsyncOpenAI
        
        # Priority: Check model prefix to decide provider
        is_openai_model = config.AI_MODEL.startswith('gpt')
        self._provider_name = "disabled"
        self._quota_cooldown_until = None
        self._quota_log_suppressed_until = None
        self._quota_cooldown_minutes = max(5, int(getattr(config, "AI_QUOTA_COOLDOWN_MIN", 15) or 15))
        self._quota_log_window_seconds = max(30, int(getattr(config, "AI_QUOTA_LOG_WINDOW_SEC", 300) or 300))
        
        if is_openai_model and config.OPENAI_API_KEY:
            print(f"AI Analysis: Using OpenAI ({config.AI_MODEL})")
            self.client = AsyncOpenAI(api_key=config.OPENAI_API_KEY)
            self._provider_name = "openai"
        elif config.XAI_API_KEY:
            print(f"AI Analysis: Using xAI (Grok - {config.AI_MODEL})")
            self.client = AsyncOpenAI(
                api_key=config.XAI_API_KEY,
                base_url="https://api.x.ai/v1"
            )
            self._provider_name = "xai"
        elif config.OPENAI_API_KEY:
            print(f"AI Analysis: Using OpenAI ({config.AI_MODEL})")
            self.client = AsyncOpenAI(api_key=config.OPENAI_API_KEY)
            self._provider_name = "openai"
        else:
            self.client = None
            print("WARN No suitable AI API Key found. AI Analysis disabled.")

    async def analyze_project(self, account, tweets):
        if not self.client:
            return None
        now = datetime.now(timezone.utc)
        if self._quota_cooldown_until and now < self._quota_cooldown_until:
            # Don't spam logs while quota is exhausted.
            if not self._quota_log_suppressed_until or now >= self._quota_log_suppressed_until:
                resume_at = self._quota_cooldown_until.strftime("%H:%M:%S UTC")
                print(
                    f"      ℹ️ AI analysis paused ({self._provider_name} quota cooldown) until {resume_at}"
                )
                self._quota_log_suppressed_until = now + timedelta(seconds=self._quota_log_window_seconds)
            return None

        # Prepare tweet text
        tweet_summary = ""
        for i, t in enumerate(tweets[:5]):
            text = getattr(t, 'text', '') or getattr(t, 'full_text', '')
            tweet_summary += f"{i+1}. {text}\n"

        prompt = f"""
        Analyze this Twitter account and determine if it's a high-quality Web3/Crypto project (DEX, NFT, Infra, Meme, etc.) or just a personal account/engagement farmer.
        
        Your analysis is for the Velcor3 monitoring system. If you say is_project: false, we will NOT post this to Discord.

        Account Name: {account.name}
        Handle: @{account.screen_name}
        Bio: {account.description}
        Followers: {account.followers_count}
        Created At: {account.created_at}

        Recent Tweets:
        {tweet_summary if tweet_summary else "NO TWEETS YET (Possibly a stealth launch or very early profile)"}

        DECISION RULES (BE MORE LENIENT WITH EARLY PROJECTS, BUT RESPECT TWEET EVIDENCE):
        1. POST (true): If it's a protocol, token, NFT collection, AI agent, infrastructure tool, gaming project, or anything that could be a Web3 initiative.
        2. POST (true): If the bio/handle suggests a project identity (even with 0 tweets) - examples: company names, product descriptions, .xyz/.com domains, official-looking handles.
        3. POST (true): If it mentions any Web3 keywords: DeFi, NFT, protocol, chain, network, DAO, dApp, mint, airdrop, testnet, mainnet, launch, building, ecosystem.
        3b. SKIP (false): If the bio is empty/very short AND recent tweets clearly have nothing to do with Web3/crypto/projects (e.g. personal memes, movies, politics, generic life posts), you MUST set is_project to false. Do NOT infer Web3 relevance from who might follow them — only from this account's own bio, handle, and tweet text.
        4. SKIP (false): REJECT IMMEDIATELY if the summary/reasoning contains personal account indicators:
           - "associated with", "involvement in", "working with", "helping", "supporting", "advising"
           - "growth strategies", "marketing", "consultant", "advisor role"
           - "multiple projects" (suggests consultant, not a single project)
           - "potential involvement" (vague language = personal account)
        5. SKIP (false): If bio clearly states personal role: "founder of", "building at", "investor", "advisor", "content creator", "trader", "CT".
        6. SKIP (false): If the account is obviously engagement farming (asking for follows, generic spam, no clear project identity).
        7. DEFAULT TO POST only when tweets/bio are ambiguous or empty (possible stealth). If tweets exist and are clearly non-crypto/non-project, SKIP. ALWAYS reject consultants/advisors/marketers.

        Examples of accounts to POST:
        - @LighterFluidxyz (Even with 0 tweets, the .xyz domain suggests a project)
        - @local_host3000 ("a living website and public sandbox" = creative project)
        - @NWOdotfun ("A new world order. Join the movement" + .fun domain = likely a project)

        Examples of accounts to SKIP:
        - "Founder of XYZ" (personal profile)
        - "Trader" (personal account)
        - "Content creator" (personal influencer)
        - "Associated with multiple Web3 projects" (consultant/marketer, NOT a project)
        - "Potential involvement in growth strategies" (consultant language, NOT a project)

        Return your analysis in JSON format:
        {{
            "is_project": true/false,
            "category": "Meme/DeFi/NFT/Infra/AI/Gaming/Creative/Personal/Other",
            "summary": "Short 1-sentence summary of what this is and why it's trending.",
            "brain_score": 0 to 100, (How much potential does this project have based on bio/tweets?)
            "confidence": 0.0 to 1.0,
            "reasoning": "Brief explanation for your decision."
        }}
        """

        try:
            response = await self.client.chat.completions.create(
                model=config.AI_MODEL,
                messages=[
                    {"role": "system", "content": "You are the Velcor3 AI researcher. Identify early Web3 projects and filter noise (personal accounts, engagement farmers, advisors). Prefer posting when data is missing (no tweets / stealth). If the bio is empty or generic and recent tweets are clearly unrelated to Web3 or a product launch, say is_project false — do not treat hypothetical follower interest as evidence."},
                    {"role": "user", "content": prompt}
                ],
                response_format={ "type": "json_object" }
            )
            import json
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            err_msg = str(e)
            err_lower = err_msg.lower()
            is_quota_exhausted = (
                "insufficient_quota" in err_lower
                or ("error code: 429" in err_lower and "quota" in err_lower)
                or ("429" in err_lower and "billing" in err_lower)
            )
            if is_quota_exhausted:
                now = datetime.now(timezone.utc)
                self._quota_cooldown_until = now + timedelta(minutes=self._quota_cooldown_minutes)
                self._quota_log_suppressed_until = now + timedelta(seconds=self._quota_log_window_seconds)
                resume_at = self._quota_cooldown_until.strftime("%H:%M:%S UTC")
                print(
                    f"      ⚠️ AI quota exhausted on {self._provider_name}; pausing AI calls until {resume_at}"
                )
                return None

            print(f"      🤖 AI Analysis Error: {err_msg}")
            return None

if __name__ == "__main__":
    async def test():
        client = TwitterClient()
        uid = await client.get_user_id("a16z")
        print(f"ID: {uid}")
    
    asyncio.run(test())
