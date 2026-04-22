import asyncio
import os
import json
from twikit import Client
import config
from datetime import datetime, timezone, timedelta
import time

# Auto-apply twikit reduce() patch on every startup (safe if already patched)
try:
    import patch_twikit
    patch_twikit.apply_patch()
except Exception:
    pass

class TwitterClient:
    def __init__(self):
        from app_paths import BASE_DIR, DATA_DIR, ensure_dirs

        ensure_dirs()
        self._base_dir = str(BASE_DIR)
        self._cache_path = os.path.join(DATA_DIR, "user_id_cache.json")
        self._accounts_path = os.path.join(DATA_DIR, "accounts.json")
        self._cookies_dir = str(DATA_DIR)
        self._user_id_cache, self._user_id_neg = self._load_cache()
        self.is_rate_limited = False
        self.cooldown_ends = None
        
        # Proxy rotation
        self._all_proxies = config.get_proxies()
        self._proxy_idx = 0
        
        # User-Agents for stealth
        self._user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0'
        ]
        
        # Session rotation
        self._sessions = []  # List of (client, account_info, logged_in, rate_limited)
        self._current_session_idx = 0
        self._load_accounts()
    
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

        # Primary cookies: prefer data/cookies.json, fall back to project root (legacy layouts).
        def _pick_cookie_file(name: str):
            for base in (self._cookies_dir, self._base_dir):
                p = os.path.join(base, name)
                if os.path.isfile(p):
                    return p
            return None

        main_cookie_path = _pick_cookie_file("cookies.json")
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
                'cookie_path': main_cookie_path,
                'proxy': current_proxy,
                'proxy_fails': 0
            })
            proxy_msg = f" (Proxy: {current_proxy})" if current_proxy else ""
            print(f"📦 Primary session: cookies.json{proxy_msg}")
        
        # Backup cookies (data/ or project root)
        for backup_name in ("cookies_backup.json", "cookies_backup2.json"):
            backup_cookie_path = _pick_cookie_file(backup_name)
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
                'cookie_path': backup_cookie_path,
                'proxy': current_proxy,
                'proxy_fails': 0
            })
            print(f"   + Backup session: {backup_name} (Proxy: {current_proxy})")
        
        # Then add accounts from accounts.json as backup sessions
        if os.path.exists(self._accounts_path):
            try:
                with open(self._accounts_path, 'r') as f:
                    accounts = json.load(f)
                
                for acc in accounts:
                    cookie_file = os.path.join(self._cookies_dir, f"cookies_{acc['username']}.json")
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
                        'cookie_path': cookie_file,
                        'proxy': current_proxy,
                        'proxy_fails': 0
                    })
                    has_cookies = os.path.exists(cookie_file)
                    cookie_msg = "(with cookies)" if has_cookies else "(new login)"
                    print(f"   + Backup session: @{acc['username']} {cookie_msg} (Proxy: {current_proxy})")
            except Exception as e:
                print(f"⚠️ Error loading accounts.json: {e}")
        
        if not self._sessions:
            print("❌ No sessions available! Need cookies.json or account cookies.")
            print(f"   💡 Put cookies here (recommended): {os.path.join(self._cookies_dir, 'cookies.json')}")
            print(f"   💡 Or next to main.py (legacy): {os.path.join(self._base_dir, 'cookies.json')}")
        else:
            print(f"🔄 Total sessions available: {len(self._sessions)} | Proxies loaded: {len(self._all_proxies)}")
    
    def _get_current_session(self):
        """Get current active session, rotating if rate limited."""
        attempts = 0
        while attempts < len(self._sessions):
            session = self._sessions[self._current_session_idx]
            if not session['rate_limited']:
                return session
            # Try next session
            self._rotate_session()
            attempts += 1
        
        # All sessions rate limited
        self.is_rate_limited = True
        return self._sessions[self._current_session_idx]
    
    def _rotate_session(self):
        """Rotate to next available session."""
        old_idx = self._current_session_idx
        self._current_session_idx = (self._current_session_idx + 1) % len(self._sessions)
        if len(self._sessions) > 1:
            new_user = self._sessions[self._current_session_idx]['account']
            username = new_user['username'] if new_user else 'default'
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔄 Session Rotation: Switched to @{username}")
    
    def check_cooldown(self):
        """Checks if the cooldown period has expired and resets flags."""
        if self.cooldown_ends:
            if datetime.now() > self.cooldown_ends:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔄 Cooldown expired. Resuming operations.")
                self.cooldown_ends = None
                self.is_rate_limited = False
                for s in self._sessions:
                    s['rate_limited'] = False
                    s['soft_429_count'] = 0
            else:
                remaining = int((self.cooldown_ends - datetime.now()).total_seconds() / 60)
                # Optional: print(f"Info: Cooling down... {remaining}m remaining")

    async def _twikit_pace(self):
        """Small gap between Twikit calls to reduce 429 bursts (same pool for HVA + search + art)."""
        gap = float(getattr(config, "TWIKIT_REQUEST_GAP_SEC", 0) or 0)
        if gap > 0:
            await asyncio.sleep(gap)

    @staticmethod
    def _reset_soft_429(session):
        if session is not None:
            session["soft_429_count"] = 0

    def get_current_username(self):
        """Get the username of the current session."""
        session = self._sessions[self._current_session_idx]
        return session['account']['username'] if session.get('account') else 'default'

    def _rotate_session(self):
        """Rotate to the next session."""
        original_idx = self._current_session_idx
        while True:
            self._current_session_idx = (self._current_session_idx + 1) % len(self._sessions)
            if not self._sessions[self._current_session_idx]['rate_limited']:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔄 Session Rotation: Switched to @{self.get_current_username()}")
                return True
            if self._current_session_idx == original_idx:
                return False

    def _mark_session_blocked(self, reason):
        """Mark current session as blocked/rate-limited and rotate."""
        session = self._sessions[self._current_session_idx]
        username = session['account']['username'] if session['account'] else 'default'
        
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
        max_proxy_fails = 5 # Allow more retries for network glitches
        
        if is_proxy_err and session['proxy_fails'] < max_proxy_fails:
            session['proxy_fails'] += 1
            if not self._all_proxies: return # Can't rotate proxy if no list
            
            new_proxy = self._all_proxies[self._proxy_idx % len(self._all_proxies)]
            self._proxy_idx += 1
            
            err_type = "Timed Out (522)" if "522" in reason else ("Flagged (403)" if "403" in reason else "Network Error")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔄 Proxy {err_type} for @{username}. Trying new IP: {new_proxy}")
            
            # Re-initialize client with new proxy but same UA if possible
            import random
            old_ua = getattr(session['client'], 'user_agent', random.choice(self._user_agents))
            session['client'] = Client('en-US', proxy=new_proxy)
            session['client'].user_agent = old_ua
            session['proxy'] = new_proxy
            session['logged_in'] = False # Force reload cookies on next ensure
            return

        # Transient X throttling: a few 429s → rotate cookie sessions instead of locking everyone out.
        if "429" in reason and not is_proxy_err:
            soft = int(session.get("soft_429_count", 0) or 0) + 1
            session["soft_429_count"] = soft
            cap = int(getattr(config, "TWIKIT_429_SOFT_PER_SESSION", 8) or 8)
            if soft < cap:
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] ⏳ 429 throttle ({soft}/{cap}) @{username} — "
                    "rotating session (soft backoff; not hard-blocking yet)."
                )
                self._rotate_session()
                return
            session["soft_429_count"] = 0

        session['rate_limited'] = True
        
        # Clean up reason if it's a massive HTML block
        clean_reason = reason
        if "403" in reason and "<html" in reason.lower():
            ray_id = "Unknown"
            import re
            match = re.search(r"Cloudflare Ray ID: <strong>(.*?)</strong>", reason)
            if match: ray_id = match.group(1)
            clean_reason = f"Cloudflare 403 Block (Ray ID: {ray_id}). Cookies/IP flagged."
        elif len(reason) > 150:
            clean_reason = reason[:147] + "..."

        print(f"[{datetime.now().strftime('%H:%M:%S')}] 🛑 Session @{username} blocked: {clean_reason}")
        self._rotate_session()
        
        if self._sessions and all(s['rate_limited'] for s in self._sessions):
            self.is_rate_limited = True
            cool_m = int(getattr(config, "TWIKIT_ALL_SESSIONS_COOLDOWN_MIN", 45) or 45)
            self.cooldown_ends = datetime.now() + timedelta(minutes=cool_m)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ⛔ ALL SESSIONS BLOCKED. Cooldown until {self.cooldown_ends.strftime('%H:%M:%S')} (~{cool_m}m).")
            print("💤 The bot will automatically retry after cooldown.")


    async def verify_all_sessions(self):
        """Verify all sessions and proxies on startup."""
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 🩺 Checking session health...")
        valid_count = 0
        for i, session in enumerate(self._sessions):
            username = session['account']['username'] if session['account'] else 'default'
            
            attempts = 0
            while attempts < 3: # Up to 3 attempts with different proxies
                proxy_str = f" | Proxy: {session['proxy']}" if session.get('proxy') else ""
                try:
                    # Ensure logged in
                    if not session['logged_in']:
                        success, err = await self._login(session)
                        if not success:
                            # If failed, treat as error and check for proxy rotation
                            raise Exception(err or "Login failed")
                    
                    # Test connectivity/auth with a simple call
                    await session['client'].get_user_by_screen_name('Twitter')
                    print(f"   ✅ Session @{username} is healthy{proxy_str}")
                    valid_count += 1
                    break # Success!
                    
                except Exception as e:
                    err_msg = str(e)
                    is_network_err = any(
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
                    is_blocked_err = "403" in err_msg
                    
                    # If network/proxy error and we have more proxies, rotate and retry
                    max_proxy_fails = min(len(self._all_proxies), 3) if self._all_proxies else 0
                    if (is_network_err or is_blocked_err) and self._all_proxies and attempts < max_proxy_fails:
                        attempts += 1
                        new_proxy = self._all_proxies[self._proxy_idx % len(self._all_proxies)]
                        self._proxy_idx += 1
                        
                        err_type = "Timed Out (522)" if "522" in err_msg else ("Flagged (403)" if "403" in err_msg else "Network Failure")
                        print(f"   ⚠️ Session @{username} {err_type}. Rotating proxy and retrying... (Attempt {attempts}/{max_proxy_fails})")
                        
                        # Re-initialize client
                        import random
                        old_ua = getattr(session['client'], 'user_agent', random.choice(self._user_agents))
                        session['client'] = Client('en-US', proxy=new_proxy)
                        session['client'].user_agent = old_ua
                        session['proxy'] = new_proxy
                        session['logged_in'] = False
                        continue # Try again with new proxy
                    else:
                        # Permanent failure or out of retries
                        reason = f"[{type(e).__name__}] {err_msg}" if err_msg else f"[{type(e).__name__}] (Empty error)"
                        if "403" in err_msg:
                            print(f"   ⚠️ Session @{username} is BLOCKED (403): {reason}{proxy_str}")
                        else:
                            print(f"   ❌ Session @{username} failure: {reason}{proxy_str}")
                            if "401" in err_msg or "Unauthorized" in err_msg or "Could not authenticate" in err_msg:
                                print(
                                    "      💡 Twitter auth failed — export fresh cookies for this account or check proxy/IP."
                                )
                        
                        session['rate_limited'] = True # Disable for now
                        break # Give up on this session
        
        if valid_count == 0:
            if not self._sessions:
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] ❌ No Twikit cookie sessions configured "
                    "(brain scan / X search need cookies.json)."
                )
                print(f"   💡 Export cookies to: {os.path.join(self._cookies_dir, 'cookies.json')}")
                # Not the same as “rate limited” — do not set global cooldown when there is no pool.
                self.is_rate_limited = False
                self.cooldown_ends = None
                return
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ NO VALID SESSIONS FOUND. Entering Cooldown Mode.")
            self.is_rate_limited = True
            cool_m = int(getattr(config, "TWIKIT_ALL_SESSIONS_COOLDOWN_MIN", 45) or 45)
            self.cooldown_ends = datetime.now() + timedelta(minutes=cool_m)
            print(f"💤 Bot will retry in ~{cool_m} minutes (at {self.cooldown_ends.strftime('%H:%M:%S')}).")
        else:
            print(f"✨ Startup check complete. {valid_count}/{len(self._sessions)} sessions ready.\n")
            # Reset current session to first valid one
            for i, s in enumerate(self._sessions):
                if not s['rate_limited']:
                    self._current_session_idx = i
                    break

    async def _ensure_session(self):
        """Ensure a logged-in session is available, rotating if necessary."""
        self.check_cooldown()
        while not self.is_rate_limited:
            session = self._get_current_session()
            success, err = await self._login(session)
            if success:
                return session
            # If login failed, mark this session as blocked and try next
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
                    f"📦 Loaded {len(ids)} IDs (+{len(neg)} negative) from Resolver Cache "
                    f"(prevents re-searching handles)."
                )
            except Exception:
                print("⚠️ Error loading User ID cache.")
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
            # Try loading cookies first
            if os.path.exists(cookie_path):
                with open(cookie_path, 'r') as f:
                    cookies_data = json.load(f)
                
                if isinstance(cookies_data, list):
                    cookies_dict = {c['name']: c['value'] for c in cookies_data if 'name' in c and 'value' in c}
                    client.set_cookies(cookies_dict)
                else:
                    client.set_cookies(cookies_data)
                
                username = account['username'] if account else 'default'
                print(f"✅ Loaded cookies for @{username}")
                session['logged_in'] = True
                return True, None
            
            # No cookies, try login with credentials/auth_token
            if account:
                # Use auth_token if available (safer)
                if account.get('auth_token'):
                    print(f"🔑 Using auth_token for @{account['username']}...")
                    client.set_cookies({'auth_token': account['auth_token']})
                    session['logged_in'] = True
                    return True, None

                print(f"🔐 Logging in as @{account['username']}...")
                await client.login(
                    auth_info_1=account['username'],
                    auth_info_2=account['email'],
                    password=account['password'],
                    enable_ui_metrics=True
                )
                client.save_cookies(cookie_path)
                session['logged_in'] = True
                print(f"✅ Logged in and saved cookies for @{account['username']}")
                return True, None
            else:
                print("❌ No credentials available for login")
                return False, "No credentials"
                
        except Exception as e:
            username = account['username'] if account else 'default'
            timestamp = datetime.now().strftime('%H:%M:%S')
            err_msg = str(e)
            if "403" in err_msg:
                print(f"[{timestamp}] ⛔ Cloudflare Block (403) for @{username}. Sessions may need refresh.")
            elif "429" in err_msg:
                print(f"[{timestamp}] ⏳ Rate Limit (429) during login for @{username}")
            else:
                print(f"[{timestamp}] ❌ Login failed for @{username}: {e}")
            return False, err_msg

    async def get_user_id(self, handle):
        # Normalize handle
        handle = handle.lower()
        if handle in self._user_id_cache:
            return self._user_id_cache[handle]

        # Negative cache: avoid repeatedly querying handles that don't exist / are inaccessible.
        until = float(self._user_id_neg.get(handle) or 0)
        if until and time.time() < until:
            return None

        if self.is_rate_limited: return None
        
        print(f"      📡 Looking up ID for @{handle}...")
        session = await self._ensure_session()
        if not session: return None
            
        try:
            await self._twikit_pace()
            user = await session['client'].get_user_by_screen_name(handle)
            if user:
                self._reset_soft_429(session)
                self._user_id_cache[handle] = user.id
                self._save_cache()
                return user.id
            return None
        except Exception as e:
            err_msg = str(e)
            if not err_msg:
                err_msg = f"Empty {type(e).__name__}"

            if any(code in err_msg for code in ["429", "503", "403", "502", "504"]) or "Empty" in err_msg or "Timeout" in err_msg:
                self._mark_session_blocked(err_msg)
                return await self.get_user_id(handle)
            # For hard "does not exist" / 404-style errors, cache a cooldown to reduce spam.
            low = err_msg.lower()
            if "does not exist" in low or "not found" in low or "no such user" in low:
                # 7 days
                self._user_id_neg[handle] = time.time() + (7 * 24 * 3600)
                self._save_cache()
            print(f"      ❌ Lookup error for {handle}: {err_msg}")
            return None

    async def get_new_following(self, user_id):
        if self.is_rate_limited: return [] 
        session = await self._ensure_session()
        if not session: return []
            
        try:
            await self._twikit_pace()
            following = await session['client'].get_user_following(user_id, count=20)
            if not following: return []
            
            newly_followed_accounts = []
            now = datetime.now(timezone.utc)
            for user in following:
                try:
                    created_at = user.created_at
                    if isinstance(created_at, str):
                         created_at = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
                    if (now - created_at).days <= config.SNIPER_MAX_AGE_DAYS:
                        newly_followed_accounts.append(user)
                except: continue
            self._reset_soft_429(session)
            return newly_followed_accounts
        except Exception as e:
            err_msg = str(e)
            if not err_msg:
                err_msg = f"Empty {type(e).__name__}"
            
            if any(code in err_msg for code in ["429", "503", "403", "502", "504"]) or "Empty" in err_msg or "Timeout" in err_msg:
                self._mark_session_blocked(err_msg)
                return await self.get_new_following(user_id)
            print(f"      ❌ Following error (ID: {user_id}): {err_msg}")
            return []

    async def get_new_following_with_delta(self, user_id, hva_handle):
        """Get new following with delta detection - only processes when count changes."""
        import database
        if self.is_rate_limited: return [], 0 
            
        session = await self._ensure_session()
        if not session: return [], 0
            
        try:
            await self._twikit_pace()
            following = await session['client'].get_user_following(user_id, count=20)
            if not following: return [], 0
            
            current_count = len(following)
            last_count = database.get_hva_last_follows_count(hva_handle)
            database.update_hva_follows_count(hva_handle, current_count)
            delta = current_count - last_count if last_count > 0 else current_count
            
            newly_followed_accounts = []
            now = datetime.utcfromtimestamp(datetime.now().timestamp()).replace(tzinfo=timezone.utc)
            count_new = 0
            for user in following:
                try:
                    created_at = user.created_at
                    if isinstance(created_at, str):
                        created_at = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
                    if (now - created_at).days <= config.SNIPER_MAX_AGE_DAYS:
                        newly_followed_accounts.append(user); count_new += 1
                except: continue
            
            print(f"      ✔ Found {current_count} follows ({count_new} Potential Projects, delta: {delta})")
            self._reset_soft_429(session)
            return newly_followed_accounts, delta
        except Exception as e:
            err_msg = str(e)
            if not err_msg:
                err_msg = f"Empty {type(e).__name__}"

            if any(code in err_msg for code in ["429", "503", "403", "502", "504", "522"]) or "Empty" in err_msg or "Timeout" in err_msg:
                print(f"      🔄 Retrying due to {err_msg}...")
                self._mark_session_blocked(err_msg)
                return await self.get_new_following_with_delta(user_id, hva_handle)
            print(f"      ❌ Following error (ID: {user_id}): {err_msg}")
            return [], 0

    async def get_user_timeline(self, user_id, count=20):
        if self.is_rate_limited: return [] 
        session = await self._ensure_session()
        if not session: return []
        try:
            await self._twikit_pace()
            tweets = await session['client'].get_user_tweets(user_id, 'Tweets', count=count)
            print(f"      ✔ Fetched {len(tweets)} timeline items")
            self._reset_soft_429(session)
            return tweets
        except Exception as e:
            err_msg = str(e)
            if "'value'" in err_msg:
                print(f"      ℹ️ Timeline: No tweets found (0 tweets or restricted)")
                return []

            if not err_msg:
                err_msg = f"Empty {type(e).__name__}"

            if any(code in err_msg for code in ["429", "503", "403", "502", "504", "522"]) or "Empty" in err_msg or "Timeout" in err_msg:
                self._mark_session_blocked(err_msg)
                return await self.get_user_timeline(user_id, count)
            print(f"      ❌ Timeline error (ID: {user_id}): {err_msg}")
            return []

    async def get_user_info(self, user_id):
        if self.is_rate_limited: return None
        session = await self._ensure_session()
        if not session: return None
        try:
            await self._twikit_pace()
            u = await session['client'].get_user_by_id(user_id)
            self._reset_soft_429(session)
            return u
        except Exception as e:
            err_msg = str(e)
            if not err_msg:
                err_msg = f"Empty {type(e).__name__}"

            if any(code in err_msg for code in ["429", "503", "403", "502", "504", "522"]) or "Empty" in err_msg or "Timeout" in err_msg:
                self._mark_session_blocked(err_msg)
                return await self.get_user_info(user_id)
            print(f"      ❌ User info error (ID: {user_id}): {err_msg}")
            return None

    async def get_user_by_handle(self, handle: str):
        """Fetch a full user object from a @handle (screen_name)."""
        handle = (handle or "").strip().lstrip("@")
        if not handle:
            return None

        if self.is_rate_limited:
            return None
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
            if any(code in err_msg for code in ["429", "503", "403", "502", "504", "522"]) or "Timeout" in err_msg:
                self._mark_session_blocked(err_msg)
                return await self.get_user_by_handle(handle)
            return None

    async def search_recent_tweets(self, query: str, count: int = 15):
        """
        Best-effort recent search using twikit client.
        Returns a list of tweet objects (may be empty).

        Notes:
        - twikit API differs across versions; this method tries multiple call shapes.
        - Uses the active rotating session/proxy/cookies logic.
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
        client = session["client"]

        try:
            await self._twikit_pace()
            # Try common twikit shapes
            if hasattr(client, "search_tweet"):
                try:
                    out = list(await client.search_tweet(query, product="Latest", count=count))
                except TypeError:
                    out = list(await client.search_tweet(query, "Latest", count=count))
                self._reset_soft_429(session)
                return out
            if hasattr(client, "search_tweets"):
                try:
                    out = list(await client.search_tweets(query, product="Latest", count=count))
                except TypeError:
                    out = list(await client.search_tweets(query, "Latest", count=count))
                self._reset_soft_429(session)
                return out
        except Exception as e:
            err_msg = str(e) or f"Empty {type(e).__name__}"
            if any(code in err_msg for code in ["429", "503", "403", "502", "504", "522"]) or "Timeout" in err_msg:
                self._mark_session_blocked(err_msg)
                return await self.search_recent_tweets(query, count=count)
        return []

    async def get_x_profile_art(self, handle: str):
        """
        Return (profile_image_https, banner_https) for a handle.
        Uses twikit when possible; falls back to unavatar.io for PFP only.
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
            # Suppress known twikit non-fatal warnings (fallback used)
            if "ClientTransaction" in err_msg and "attribute" in err_msg:
                return fallback_pfp, None
            if "Multiple cookies exist" in err_msg:
                return fallback_pfp, None
            print(f"      ⚠️ get_x_profile_art @{handle}: {err_msg[:120]}")
            return fallback_pfp, None

    async def create_tweet(self, text):
        """Creates a new tweet using the primary session."""
        if self.is_rate_limited: return False
        
        # Always use the first session for posting (primary account)
        session = await self._ensure_session()
        if not session: return False
        
        try:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 🐦 Posting to X...")
            response = await session['client'].create_tweet(text)
            print(f"   ✅ Tweet posted! ID: {getattr(response, 'id', 'Unknown')}")
            return True
        except Exception as e:
            err_msg = str(e)
            print(f"   ❌ Tweet failed: {err_msg}")
            if any(code in err_msg for code in ["429", "503", "403"]):
                 self._mark_session_blocked(err_msg)
            return False

    async def get_hva_followers(self, user_id):
        """Get list of HVAs from our list that follow this account."""
        if self.is_rate_limited: return []
        session = await self._ensure_session()
        if not session: return []
        
        try:
            followers = await session['client'].get_user_followers(user_id, count=100)
            if not followers: return []
            
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
                return await self.get_hva_followers(user_id)
            return []

    async def get_first_followers(self, user_id, limit=1000):
        """Fetch ~1000 followers and return the OLDEST ones (last in the list)."""
        if self.is_rate_limited: return None
        session = await self._ensure_session()
        if not session: return None
        
        all_followers = []
        cursor = None
        count_target = limit
        
        try:
            while len(all_followers) < count_target:
                # Fetch batch (twikit usually gives ~20-50 per call)
                response = await session['client'].get_user_followers(user_id, count=100, cursor=cursor)
                if not response: break
                
                batch = list(response)
                all_followers.extend(batch)
                
                # Check pagination
                if hasattr(response, 'next_cursor') and response.next_cursor:
                    cursor = response.next_cursor
                else:
                    break # End of list
                
                import asyncio
                await asyncio.sleep(1.5) # Gentle rate limit handling

            # If we hit the limit but didn't finish, we just take what we have.
            # Twitter returns NEWEST first. So the limit cuts off the OLDEST.
            # Wait, user wants FIRST followers.
            # If account has 200 followers -> we get all 200. End is oldest.
            # If account has 5000 followers -> we stop at 1000. 
            # These 1000 are the NEWEST 1000. We unfortunately CANNOT reach the old ones without scrolling.
            # SO: The logic "fetch 1000" only works for accounts with < 1000 followers.
            # If len > limit, we return a warning flag.
            
            is_partial = len(all_followers) >= limit
            return all_followers, is_partial
            
        except Exception as e:
            err_msg = str(e)
            if any(code in err_msg for code in ["429", "503", "403", "502", "504"]):
                self._mark_session_blocked(err_msg)
                # Retry once? No, expensive.
            print(f"      ❌ First followers error: {err_msg}")
            return all_followers, True

class AIAnalyzer:
    def __init__(self):
        from openai import AsyncOpenAI
        
        # Priority: Check model prefix to decide provider
        is_openai_model = config.AI_MODEL.startswith('gpt')
        
        if is_openai_model and config.OPENAI_API_KEY:
            print(f"🤖 AI Analysis: Using OpenAI ({config.AI_MODEL})")
            self.client = AsyncOpenAI(api_key=config.OPENAI_API_KEY)
        elif config.XAI_API_KEY:
            print(f"🤖 AI Analysis: Using xAI (Grok - {config.AI_MODEL})")
            self.client = AsyncOpenAI(
                api_key=config.XAI_API_KEY,
                base_url="https://api.x.ai/v1"
            )
        elif config.OPENAI_API_KEY:
            print(f"🤖 AI Analysis: Using OpenAI ({config.AI_MODEL})")
            self.client = AsyncOpenAI(api_key=config.OPENAI_API_KEY)
        else:
            self.client = None
            print("⚠️ No suitable AI API Key found. AI Analysis disabled.")

    async def analyze_project(self, account, tweets):
        if not self.client:
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

        DECISION RULES (BE MORE LENIENT WITH EARLY PROJECTS):
        1. POST (true): If it's a protocol, token, NFT collection, AI agent, infrastructure tool, gaming project, or anything that could be a Web3 initiative.
        2. POST (true): If the bio/handle suggests a project identity (even with 0 tweets) - examples: company names, product descriptions, .xyz/.com domains, official-looking handles.
        3. POST (true): If it mentions any Web3 keywords: DeFi, NFT, protocol, chain, network, DAO, dApp, mint, airdrop, testnet, mainnet, launch, building, ecosystem.
        4. SKIP (false): REJECT IMMEDIATELY if the summary/reasoning contains personal account indicators:
           - "associated with", "involvement in", "working with", "helping", "supporting", "advising"
           - "growth strategies", "marketing", "consultant", "advisor role"
           - "multiple projects" (suggests consultant, not a single project)
           - "potential involvement" (vague language = personal account)
        5. SKIP (false): If bio clearly states personal role: "founder of", "building at", "investor", "advisor", "content creator", "trader", "CT".
        6. SKIP (false): If the account is obviously engagement farming (asking for follows, generic spam, no clear project identity).
        7. DEFAULT TO POST: When in doubt about NEW projects with minimal data, POST it. But ALWAYS reject consultants/advisors/marketers.

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
                    {"role": "system", "content": "You are the Velcor3 AI researcher. Your goal is to identify early, high-potential Web3 projects and filter out noise (personal accounts, engagement farmers, advisors). When in doubt, err on the side of posting - we prefer false positives (a few non-projects get through) over false negatives (missing real projects). Be especially lenient with brand-new accounts that have minimal tweets but show project indicators in their bio or handle."},
                    {"role": "user", "content": prompt}
                ],
                response_format={ "type": "json_object" }
            )
            import json
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            print(f"      🤖 AI Analysis Error: {e}")
            return None

if __name__ == "__main__":
    async def test():
        client = TwitterClient()
        uid = await client.get_user_id("a16z")
        print(f"ID: {uid}")
    
    asyncio.run(test())
