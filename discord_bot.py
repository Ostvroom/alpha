import discord
from discord.ext import commands, tasks
import config
import database
import json
import os
import re
import textwrap
import time
from typing import List, Dict, Optional, Set, Tuple, Any
from twitter_client import TwitterClient, AIAnalyzer
from datetime import datetime, timedelta, timezone
import asyncio
import random
import aiohttp
import wallet_database
import payment_database
import payment_commands
from trackers.eth_tracker import (
    tracked_eth_wallets,
    check_eth_block,
    refresh_eth_usd_price,
)
from trackers.nft_pnl import PNL_VALID_CHAINS, get_wallet_pnl, format_pnl_embed
from trackers.kolfi_tokens_client import (
    run_kolfi_feed_once,
    run_kolfi_leaderboard_once,
    run_kolfi_alert_watchlist_daily_once,
    fetch_kolfi_top_movers_rows,
    format_kolfi_leaderboard_window,
)
from trackers.telegram_calls_tracker import run_telegram_calls_bridge
from trackers.x_daily_recap import format_x_top_movers_tweet, format_x_daily_finds_tweet
from trackers.waypoint_mints_overview import run_overview_once
from trackers.ct_domain_watcher import (
    poll_new_domains,
    init_ct_db,
    CRYPTO_KEYWORDS,
    TARGET_TLDS,
)
from discord import app_commands, Object, Interaction, Embed, Color
from server_panels import (
    PanelCommands,
    VerificationView,
    CryptoPaymentView,
    post_verification_to_channel,
    post_crypto_to_channel,
)
import guild_license
from guild_alerts_commands import GuildLicenseCommands

BRAND_NAME = "Velcor3"

try:
    import feed_events
except Exception:
    feed_events = None


def _resolve_brand_assets():
    """Logo + banner for embeds: prefer project-root `banner.jpg`, then `v/banner.*`."""
    root = os.path.dirname(os.path.abspath(__file__))
    logo_path, logo_file = None, None
    for name in (
        "velcor3_logo.png",
        "velcor3_logo.jpg",
        "alpha_logo.png",
        "alpha_logo.jpg",
        "logo.png",
        "block_brain_logo.png",
    ):
        p = os.path.join(root, name)
        if os.path.isfile(p):
            logo_path = p
            logo_file = "logo." + name.rsplit(".", 1)[-1].lower()
            break
    banner_path, banner_file = None, None
    vdir = os.path.join(root, "v")
    # Hard preference: project root `banner.jpg` (what the user edits).
    p_root_banner = os.path.join(root, "banner.jpg")
    if os.path.isfile(p_root_banner):
        banner_path = p_root_banner
        banner_file = "banner.jpg"
    else:
        for name in (
            "banner.png",
            "banner.jpg",
            "banner.jpeg",
            "velcor3_banner.png",
            "velcor3_banner.jpg",
            "alpha_banner.jpg",
            "alpha_banner.png",
        ):
            for base in (vdir, root):
                p = os.path.join(base, name)
                if os.path.isfile(p):
                    banner_path = p
                    banner_file = "banner." + name.rsplit(".", 1)[-1].lower()
                    break
            if banner_path:
                break
    return logo_path, logo_file, banner_path, banner_file


BRAND_LOGO_PATH, BRAND_LOGO_FILE, BRAND_BANNER_PATH, BRAND_BANNER_FILE = _resolve_brand_assets()

from app_paths import DATA_DIR, ensure_dirs

ensure_dirs()
_DAILY_FINDS_DIGEST_STATE = os.path.join(DATA_DIR, "daily_finds_digest_state.json")


def _embed_has_image(e: Optional[discord.Embed]) -> bool:
    if not e:
        return False
    try:
        url = getattr(getattr(e, "image", None), "url", None)
    except Exception:
        return False
    return bool(url)


def _daily_finds_load_state() -> Dict[int, dict]:
    try:
        with open(_DAILY_FINDS_DIGEST_STATE, "r", encoding="utf-8") as f:
            d = json.load(f)
        # New format: {"channels": {"123": {"message_ids":[...], "last_alerted_at":"..."}, ...}}
        if isinstance(d, dict) and isinstance(d.get("channels"), dict):
            out: Dict[int, dict] = {}
            for k, v in (d.get("channels") or {}).items():
                try:
                    cid = int(k)
                except Exception:
                    continue
                if not isinstance(v, (dict, list)):
                    continue
                # Backward compatibility with prior multi-channel format: {"123":[mid,mid]}
                if isinstance(v, list):
                    mids = []
                    for x in v:
                        try:
                            mids.append(int(x))
                        except Exception:
                            pass
                    out[cid] = {"message_ids": mids, "last_alerted_at": ""}
                    continue
                mids = []
                for x in (v.get("message_ids") or []):
                    try:
                        mids.append(int(x))
                    except Exception:
                        pass
                out[cid] = {"message_ids": mids, "last_alerted_at": str(v.get("last_alerted_at") or "")}
            return out
        # Backward compatibility: {"channel_id": 123, "message_ids": [...]}
        cid = int((d or {}).get("channel_id") or 0)
        mids = [int(x) for x in ((d or {}).get("message_ids") or []) if x is not None]
        return {cid: {"message_ids": mids, "last_alerted_at": ""}} if cid else {}
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return {}


def _daily_finds_save_state(state: Dict[int, dict]) -> None:
    try:
        with open(_DAILY_FINDS_DIGEST_STATE, "w", encoding="utf-8") as f:
            ch = {}
            for k, v in (state or {}).items():
                if not k:
                    continue
                if isinstance(v, dict):
                    ch[str(k)] = {
                        "message_ids": [int(x) for x in (v.get("message_ids") or []) if x is not None],
                        "last_alerted_at": str(v.get("last_alerted_at") or ""),
                    }
                elif isinstance(v, list):
                    ch[str(k)] = {"message_ids": [int(x) for x in v if x is not None], "last_alerted_at": ""}
            json.dump(
                {"channels": ch},
                f,
                indent=2,
            )
    except OSError as e:
        print(f"[Daily finds] could not save digest state: {e}")


def _fmt_followers_display(n: Optional[Any]) -> str:
    """Compact follower count (X-style) for daily-finds lines."""
    if n is None:
        return "—"
    if isinstance(n, str):
        n = n.replace(",", "").strip()
    try:
        n = int(n)
    except (TypeError, ValueError):
        return "—"
    n = max(0, n)
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M".replace(".0M", "M")
    if n >= 10_000:
        return f"{n / 1_000:.1f}k".replace(".0k", "k")
    if n >= 1_000:
        return f"{n / 1_000:.1f}k".replace(".0k", "k")
    return f"{n:,}"


async def _run_daily_mints_post(
    bot: "BlockBrainBot",
    target: discord.abc.Messageable,
    scope: str,
    limit: int,
    enrich_x_art: bool = True,
    posted_urls: Optional[set] = None,
) -> Tuple[int, str]:
    """
    Scrape public HTML from daily-mints.com (see trackers/daily_mints_client.py).
    Returns (number_of_embeds_posted, error_message_or_empty).
    """
    from trackers.daily_mints_client import (
        build_daily_mint_embeds,
        fetch_index_paths,
        fetch_mint_details,
        get_last_daily_mints_error,
        twitter_handle_from_url,
    )

    limit = max(1, min(25, int(limit)))
    scope_kw = "today" if scope.lower() == "today" else "all"
    async with aiohttp.ClientSession() as session:
        paths = await fetch_index_paths(session, scope_kw)
        if not paths:
            err = get_last_daily_mints_error() or "No mint links found on the calendar index."
            return 0, err
        # When scope='today' the listing already groups by today — don't also filter
        # by detail-page mint_date (those pages can lag behind the index by hours).
        # Only date-filter when pulling from the full 'all' listing.
        filter_today = scope_kw != "today"
        details = await fetch_mint_details(session, paths, limit=limit, concurrency=3, filter_today=filter_today)
    if not details:
        err = get_last_daily_mints_error() or "No mint detail pages could be loaded."
        return 0, err

    icon = BRAND_LOGO_FILE
    posted = 0
    for d in details:
        # Skip mints already posted in this session (auto-feed dedup)
        if posted_urls is not None and d.source_url in posted_urls:
            continue
        x_handle = twitter_handle_from_url(d.twitter_url)
        pfp_url = None
        ban_url = None
        if enrich_x_art and x_handle:
            try:
                pfp_url, ban_url = await bot.twitter.get_x_profile_art(x_handle)
            except Exception as ex:
                # Render workers often run without cookies.json; don't crash the whole task.
                pfp_url, ban_url = None, None
        embeds = build_daily_mint_embeds(
            d,
            BRAND_NAME,
            footer_icon_attachment=icon,
            x_pfp_url=pfp_url,
            x_banner_url=ban_url,
            x_handle=x_handle,
        )
        for emb in embeds:
            files: List[discord.File] = []
            lf = bot.brand_logo_file()
            if lf:
                files.append(lf)
            emb, files = bot._with_brand_banner_fallback(emb, files)
            if files:
                await target.send(embed=emb, files=files)
            else:
                await target.send(embed=emb)
            posted += 1
            if posted_urls is not None:
                posted_urls.add(d.source_url)
            await asyncio.sleep(0.65)
    return posted, ""


class BlockBrainBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        super().__init__(command_prefix="!velcor3 ", intents=intents, help_command=None)
        self.twitter = TwitterClient()
        self.ai = AIAnalyzer()
        self.current_scan_discoveries = 0
        self.total_processed_today = 0
        # Daily mints dedup: track URLs already posted today (reset each auto-feed run)
        self._daily_mints_posted_today: set = set()
        self._kolfi_leaderboard_boot: bool = False
        self._kolfi_top_movers_boot: bool = False
        self._escalation_daily_boot: bool = False
        self._daily_mints_boot: bool = False
        self._invite_cache: Dict[int, Dict[str, int]] = {}
        self._telegram_calls_task: Optional[asyncio.Task] = None
        # Active channel caches (populated in on_ready)
        self.active_main_channels = []
        self.active_escalation_channels = []
        self._slash_tree_synced = False

    def _get_log_prefix(self):
        return f"[{datetime.now().strftime('%H:%M:%S')}]"

    def brand_logo_file(self) -> Optional[discord.File]:
        if not BRAND_LOGO_PATH or not BRAND_LOGO_FILE:
            return None
        return discord.File(BRAND_LOGO_PATH, filename=BRAND_LOGO_FILE)

    def brand_banner_file(self) -> Optional[discord.File]:
        if not BRAND_BANNER_PATH or not BRAND_BANNER_FILE:
            return None
        return discord.File(BRAND_BANNER_PATH, filename=BRAND_BANNER_FILE)

    def _with_brand_banner_fallback(
        self,
        embed: Optional[discord.Embed],
        files: Optional[List[discord.File]] = None,
    ) -> Tuple[Optional[discord.Embed], List[discord.File]]:
        """
        If `embed` has no image set, attach + set the brand banner (`banner.jpg`) when available.
        Returns (embed, files) suitable for `send(embed=..., files=...)`.
        """
        out_files: List[discord.File] = list(files or [])
        if not embed or _embed_has_image(embed):
            return embed, out_files
        bf = self.brand_banner_file()
        if not bf:
            return embed, out_files
        # Avoid duplicating the same attachment filename.
        if not any(getattr(f, "filename", None) == bf.filename for f in out_files):
            out_files.append(bf)
        embed.set_image(url=f"attachment://{bf.filename}")
        return embed, out_files

    def _get_grade(self, score):
        if score >= 80: return "S", "🏆"
        if score >= 60: return "A", "⭐"
        if score >= 40: return "B", "✨"
        if score >= 20: return "C", "💫"
        return "D", "🌱"

    async def setup_hook(self):
        database.init_db()
        database.seed_x_project_search_keywords_if_empty()
        database.seed_x_project_search_keywords_baseline()
        payment_database.init_db()
        guild_license.init_db()
        # Initialize HVAs in DB if empty
        existing = database.get_all_hvas()
        if not existing:
            print(f"📦 Initializing HVA list in database with {len(config.HVA_LIST)} accounts...")
            for hva in config.HVA_LIST:
                database.add_hva(hva)
        else:
            print(f"📦 Loaded {len(existing)} HVAs from database.")
        
        await self.add_cog(BrainCommands(self))
        await self.add_cog(WalletCommands(self))
        await self.add_cog(PanelCommands(self))
        await self.add_cog(GuildLicenseCommands(self))
        await payment_commands.setup(self)
        self.add_view(VerificationView())
        self.add_view(CryptoPaymentView())
        self.monitor_twitter.start()
        self.trending_report.start()
        self.daily_x_trending_task.start()
        if config.ENABLE_X_PROJECT_SEARCH:
            self.x_project_first_search_task.start()
        if config.ENABLE_X_DAILY_RECAP:
            self.x_daily_recap_task.start()
        self.recalculate_hva_scores.start()
        self.daily_mints_auto_feed.start()
        if config.ENABLE_KOLFI_FEED and config.KOLFI_API_KEY:
            self.kolfi_tokens_feed.start()
        if config.ENABLE_KOLFI_LEADERBOARD and config.KOLFI_API_KEY and config.KOLFI_LEADERBOARD_CHANNEL_ID:
            self.kolfi_leaderboard_daily.start()
        if (
            config.ENABLE_KOLFI_DAILY_TOP_MOVERS
            and config.KOLFI_API_KEY
            and config.KOLFI_DAILY_TOP_MOVERS_CHANNEL_ID
        ):
            self.kolfi_daily_top_movers.start()
        if config.ENABLE_ESCALATION_DAILY_TOP_MOVERS and config.ESCALATION_DAILY_TOP_MOVERS_CHANNEL_ID:
            # Daily finds digest now runs on a fixed interval (every 8 hours).
            self.escalation_daily_interval.start()
        if config.ENABLE_MINTS_OVERVIEW and config.MINTS_OVERVIEW_CHANNEL_ID:
            self.mints_overview_feed.start()

        # Telegram calls bridge (user session; no bot)
        if getattr(config, "ENABLE_TELEGRAM_CALLS", False):
            if not self._telegram_calls_task:
                self._telegram_calls_task = asyncio.create_task(run_telegram_calls_bridge(self))
                def _tg_done(t: asyncio.Task):
                    try:
                        exc = t.exception()
                    except asyncio.CancelledError:
                        print("    [TelegramCalls] Task cancelled.")
                        return
                    except Exception as e:
                        print(f"    [TelegramCalls] Task exception lookup failed: {e}")
                        return
                    if exc:
                        print(f"    [TelegramCalls] Task crashed: {exc}")
                self._telegram_calls_task.add_done_callback(_tg_done)
                print(
                    f"    [TelegramCalls] Enabled → Discord channel {getattr(config, 'TELEGRAM_CALLS_DISCORD_CHANNEL_ID', 0)} "
                    f"| sources={len(getattr(config, 'TELEGRAM_CALLS_SOURCES', []) or [])}"
                )

        # CT Domain Watcher — independent of HVA scan, own channel
        if config.ENABLE_CT_WATCHER and config.CT_WATCHER_CHANNEL_ID:
            init_ct_db()
            self.ct_domain_watcher_task.start()
            print(f"    [CTWatcher] Enabled → channel {config.CT_WATCHER_CHANNEL_ID} | every {config.CT_POLL_MINUTES}m")

        # --- Wallet Tracker Initialization ---
        wallet_database.init_db()
        print("    [DB] Wallet database initialized.")

        # Start ETH background tasks — NFTs only (tokens disabled)
        self.loop.create_task(refresh_eth_usd_price(aiohttp.ClientSession()))
        eth_t_id = 0  # ERC20 tokens disabled
        # Multi-guild wallet tracker: env channel + any licensed guild wallet channels.
        wallet_ids: List[int] = []
        if getattr(config, "DISCORD_NFT_CHANNEL_ID", 0):
            wallet_ids.append(int(config.DISCORD_NFT_CHANNEL_ID))
        try:
            for cid in guild_license.all_wallet_nft_channel_ids():
                if cid and int(cid) not in wallet_ids:
                    wallet_ids.append(int(cid))
        except Exception:
            pass
        eth_n_id = ",".join(str(x) for x in wallet_ids if x)
        if eth_n_id:
            try:
                # Just validate the first target exists; the tracker handles splitting/each channel send.
                first_id = int(str(eth_n_id).split(",")[0])
                _eth_ch = self.get_channel(first_id) or await self.fetch_channel(first_id)
            except Exception:
                _eth_ch = None
            if _eth_ch:
                self.loop.create_task(check_eth_block(self, eth_t_id, eth_n_id))
                print(f"    [WalletTracker] ETH NFTs → {len(wallet_ids)} channel(s) (first: #{getattr(_eth_ch, 'name', 'unknown')})")
            else:
                print(
                    f"    [WalletTracker] ETH NFT channel {eth_n_id} not found / no access — "
                    f"wallet tracker skipped (check DISCORD_NFT_CHANNEL_ID and bot permissions: View Channel, Send Messages, Embed Links)."
                )

        # Start Live Mints trackers — verify channel exists before launching
        mints_id = config.DISCORD_MINTS_CHANNEL_ID
        radar_id = config.DISCORD_NEW_MINTS_CHANNEL_ID
        if mints_id:
            try:
                _mints_ch = self.get_channel(mints_id) or await self.fetch_channel(mints_id)
            except Exception:
                _mints_ch = None
            if _mints_ch:
                from trackers.eth_live_mints import check_live_eth_mints

                self.loop.create_task(check_live_eth_mints(self, mints_id, radar_id))
                print(f"    [LiveMints] Started (ETH) → #{getattr(_mints_ch, 'name', mints_id)}")
            else:
                print(
                    f"    [LiveMints] DISCORD_MINTS_CHANNEL_ID={mints_id} not found / no access — "
                    f"ETH live mints not started. Fix the ID or invite the bot with View Channel. "
                    f"Set DISCORD_MINTS_CHANNEL_ID=0 to silence."
                )

    async def rebuild_channel_caches(self) -> None:
        """Reload .env channel IDs plus licensed multi-tenant destinations from guild_licenses.db."""
        guild_license.init_db()
        self.active_main_channels = []
        self.active_escalation_channels = []
        self.new_projects_channels = []
        self.established_projects_channels = []

        for cid in config.DISCORD_CHANNEL_IDS:
            try:
                ch = self.get_channel(cid) or await self.fetch_channel(cid)
                if ch and ch not in self.active_main_channels:
                    self.active_main_channels.append(ch)
            except Exception:
                pass

        for cid in config.ESCALATION_CHANNEL_IDS:
            try:
                ch = self.get_channel(cid) or await self.fetch_channel(cid)
                if ch and ch not in self.active_escalation_channels:
                    self.active_escalation_channels.append(ch)
            except Exception:
                pass

        for cid in config.NEW_PROJECTS_CHANNEL_IDS:
            try:
                ch = self.get_channel(cid) or await self.fetch_channel(cid)
                if ch and ch not in self.new_projects_channels:
                    self.new_projects_channels.append(ch)
            except Exception:
                pass

        for cid in config.ESTABLISHED_PROJECTS_CHANNEL_IDS:
            try:
                ch = self.get_channel(cid) or await self.fetch_channel(cid)
                if ch and ch not in self.established_projects_channels:
                    self.established_projects_channels.append(ch)
            except Exception:
                pass

        for row in guild_license.iter_active_subscriptions():
            for cid, lst in (
                (row.channel_new_id, self.new_projects_channels),
                (row.channel_established_id, self.established_projects_channels),
                (row.channel_escalation_id, self.active_escalation_channels),
            ):
                if not cid:
                    continue
                try:
                    ch = self.get_channel(cid) or await self.fetch_channel(cid)
                    if ch and ch not in lst:
                        lst.append(ch)
                except Exception:
                    pass

    async def on_ready(self):
        print(f"{self._get_log_prefix()} Logged in as {self.user} (ID: {self.user.id})")
        print("------")

        await self.rebuild_channel_caches()

        print(f"📡 Channels loaded: {len(self.active_main_channels)} main, {len(self.active_escalation_channels)} escalation")
        print(f"   Age-based: {len(self.new_projects_channels)} new (≤30d), {len(self.established_projects_channels)} established (30-100d)")
        print(f"👉 Try typing '!velcor3 ping' in your server to test responsiveness.")
        if config.ENABLE_DAILY_MINTS_AUTO and config.DAILY_MINTS_AUTO_CHANNEL_ID:
            print(
                f"   📅 Daily Mints auto: {config.DAILY_MINTS_AUTO_TIME} UTC → channel {config.DAILY_MINTS_AUTO_CHANNEL_ID} "
                f"(scope={config.DAILY_MINTS_AUTO_SCOPE}, limit={config.DAILY_MINTS_AUTO_LIMIT})"
            )
        if config.ENABLE_KOLFI_FEED and config.KOLFI_API_KEY:
            print(
                f"   📡 Token alerts feed: every {config.KOLFI_POLL_MINUTES}m → low / 100K+ / 1M+ channels "
                f"({config.KOLFI_CHANNEL_LOW_ID}, {config.KOLFI_CHANNEL_100K_ID}, {config.KOLFI_CHANNEL_1M_ID})"
            )
        elif config.ENABLE_KOLFI_FEED and not config.KOLFI_API_KEY:
            print("   ⚠️ Token alerts feed enabled but KOLFI_API_KEY is empty — set it in .env")
        if config.ENABLE_KOLFI_LEADERBOARD and config.KOLFI_API_KEY and config.KOLFI_LEADERBOARD_CHANNEL_ID:
            _lb_win = format_kolfi_leaderboard_window(float(config.KOLFI_LEADERBOARD_MAX_CALL_AGE_HOURS))
            print(
                f"   📈 Kolfi ATH leaderboard: daily {config.KOLFI_LEADERBOARD_TIME_UTC} UTC → channel {config.KOLFI_LEADERBOARD_CHANNEL_ID} "
                f"(top {config.KOLFI_LEADERBOARD_TOP_N}, first call in {_lb_win} · by ATH)"
            )
        elif config.ENABLE_KOLFI_LEADERBOARD and not config.KOLFI_LEADERBOARD_CHANNEL_ID:
            print("   ⚠️ ENABLE_KOLFI_LEADERBOARD=1 but KOLFI_LEADERBOARD_CHANNEL_ID is not set")
        if (
            config.ENABLE_KOLFI_DAILY_TOP_MOVERS
            and config.KOLFI_API_KEY
            and config.KOLFI_DAILY_TOP_MOVERS_CHANNEL_ID
        ):
            print(
                f"   🏁 Our alerts · 24h top %% recap: {config.KOLFI_DAILY_TOP_MOVERS_TIME_UTC} UTC → "
                f"channel {config.KOLFI_DAILY_TOP_MOVERS_CHANNEL_ID} (top {config.KOLFI_DAILY_TOP_MOVERS_TOP_N})"
            )
        elif config.ENABLE_KOLFI_DAILY_TOP_MOVERS and not config.KOLFI_DAILY_TOP_MOVERS_CHANNEL_ID:
            print("   ⚠️ ENABLE_KOLFI_DAILY_TOP_MOVERS=1 but KOLFI_DAILY_TOP_MOVERS_CHANNEL_ID is not set")
        if config.ENABLE_ESCALATION_DAILY_TOP_MOVERS and config.ESCALATION_DAILY_TOP_MOVERS_CHANNEL_ID:
            print(
                f"   📣 Daily finds (rolling 24h UTC): every 8h → "
                f"channel {config.ESCALATION_DAILY_TOP_MOVERS_CHANNEL_ID} "
                f"(up to {config.ESCALATION_DAILY_TOP_MOVERS_TOP_N})"
            )
        elif config.ENABLE_ESCALATION_DAILY_TOP_MOVERS and not config.ESCALATION_DAILY_TOP_MOVERS_CHANNEL_ID:
            print("   ⚠️ ENABLE_ESCALATION_DAILY_TOP_MOVERS=1 but ESCALATION_DAILY_TOP_MOVERS_CHANNEL_ID is not set")
        if config.ENABLE_MINTS_OVERVIEW and config.MINTS_OVERVIEW_CHANNEL_ID:
            print(
                f"   📊 Mints Overview: every {config.MINTS_OVERVIEW_POLL_MINUTES}m "
                f"→ channel {config.MINTS_OVERVIEW_CHANNEL_ID} (top {config.MINTS_OVERVIEW_TOP_N})"
            )
        elif config.ENABLE_MINTS_OVERVIEW and not config.MINTS_OVERVIEW_CHANNEL_ID:
            print("   ⚠️ ENABLE_MINTS_OVERVIEW=1 but MINTS_OVERVIEW_CHANNEL_ID is not set")
        if config.ENABLE_X_DAILY_RECAP:
            bits = []
            if config.X_DAILY_RECAP_POST_TOKENS:
                bits.append(f"Solana movers (needs KOLFI)" if not config.KOLFI_API_KEY else "Solana movers")
            if config.X_DAILY_RECAP_POST_FINDS:
                bits.append("24h finds")
            print(
                f"   🐦 X daily recap: {config.X_DAILY_RECAP_TIME_UTC} UTC → "
                f"{', '.join(bits) or 'off'} (top {config.X_DAILY_RECAP_TOP_N}) · cookies.json session"
            )
            if config.X_DAILY_RECAP_POST_TOKENS and not config.KOLFI_API_KEY:
                print("   ⚠️ X recap token post enabled but KOLFI_API_KEY is empty — token tweet will be skipped")

        # Load Tracked Wallets
        eth_wallets = wallet_database.get_wallets_by_chain("ETH")
        tracked_eth_wallets.update(eth_wallets)
        print(f"    [*] Loaded {len(eth_wallets)} ETH whale wallet(s) for real-time tracking.")

        if (
            config.ENABLE_KOLFI_LEADERBOARD
            and config.KOLFI_API_KEY
            and config.KOLFI_LEADERBOARD_CHANNEL_ID
            and not self._kolfi_leaderboard_boot
        ):
            self._kolfi_leaderboard_boot = True
            asyncio.create_task(self._run_kolfi_leaderboard_post())

        # Boot-post top movers once (daily loop runs at configured UTC time)
        if (
            config.ENABLE_KOLFI_DAILY_TOP_MOVERS
            and config.KOLFI_API_KEY
            and config.KOLFI_DAILY_TOP_MOVERS_CHANNEL_ID
            and not self._kolfi_top_movers_boot
        ):
            self._kolfi_top_movers_boot = True
            asyncio.create_task(self._run_kolfi_top_movers_post())

        if (
            config.ENABLE_ESCALATION_DAILY_TOP_MOVERS
            and config.ESCALATION_DAILY_TOP_MOVERS_CHANNEL_ID
            and not self._escalation_daily_boot
        ):
            self._escalation_daily_boot = True
            asyncio.create_task(self._run_escalation_daily_top_post())

        # Boot-post daily mints once on startup (scheduled loop runs at configured UTC time)
        if (
            config.ENABLE_DAILY_MINTS_AUTO
            and config.DAILY_MINTS_AUTO_CHANNEL_ID
            and not self._daily_mints_boot
        ):
            self._daily_mints_boot = True
            asyncio.create_task(self._run_daily_mints_boot_post())

        # Prime invite usage cache (for automatic referrals)
        if getattr(config, "ENABLE_INVITE_REFERRALS", True):
            await self._refresh_invite_cache()

        # Slash sync once (cogs are registered only in setup_hook — do not add_cog again here)
        if not self._slash_tree_synced:
            self._slash_tree_synced = True
            try:
                if config.DISCORD_GUILD_ID:
                    guild = Object(id=int(config.DISCORD_GUILD_ID))
                    self.tree.copy_global_to(guild=guild)
                    synced = await self.tree.sync(guild=guild)
                    n = len(synced) if synced is not None else 0
                    print(f"[*] Slash commands synced to guild {config.DISCORD_GUILD_ID} ({n} command(s)).")
                synced_g = await self.tree.sync()
                n_g = len(synced_g) if synced_g is not None else 0
                print(
                    f"[*] Global slash sync ({n_g} command(s)) — `/alerts` & `/owner_license` work in every server."
                )
            except Exception as e:
                print(f"[X] Slash sync error: {e}")
        
        # Manually trigger trending report on startup safely
        await asyncio.sleep(2)
        if not self.trending_report.is_running():
            self.trending_report.start()
        else:
            # If already running, just trigger one execution now
            try:
                # Force the task to run once now
                pass 
            except: pass

    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return
            
        content = message.content.strip()
        
        # Process other commands (like !velcor3 ...)
        await self.process_commands(message)

    async def _refresh_invite_cache(self) -> None:
        """Cache invite uses per guild. Requires Manage Guild permission in the server."""
        self._invite_cache = {}
        for g in list(self.guilds or []):
            try:
                invites = await g.invites()
            except Exception:
                continue
            self._invite_cache[g.id] = {str(i.code): int(i.uses or 0) for i in invites}

    async def on_guild_join(self, guild: discord.Guild):
        if getattr(config, "ENABLE_INVITE_REFERRALS", True):
            try:
                invites = await guild.invites()
                self._invite_cache[guild.id] = {str(i.code): int(i.uses or 0) for i in invites}
            except Exception:
                pass

    async def on_invite_create(self, invite: discord.Invite):
        if not getattr(config, "ENABLE_INVITE_REFERRALS", True):
            return
        g = invite.guild
        if not g:
            return
        d = self._invite_cache.setdefault(g.id, {})
        d[str(invite.code)] = int(invite.uses or 0)

    async def on_invite_delete(self, invite: discord.Invite):
        if not getattr(config, "ENABLE_INVITE_REFERRALS", True):
            return
        g = invite.guild
        if not g:
            return
        d = self._invite_cache.setdefault(g.id, {})
        d.pop(str(invite.code), None)

    async def on_member_join(self, member: discord.Member):
        """Auto-referral: detect which invite code got used, store referrer→referred in DB."""
        if not getattr(config, "ENABLE_INVITE_REFERRALS", True):
            return
        if not member.guild:
            return
        g = member.guild
        before = dict(self._invite_cache.get(g.id) or {})
        try:
            invites = await g.invites()
        except Exception:
            return
        after = {str(i.code): int(i.uses or 0) for i in invites}
        self._invite_cache[g.id] = dict(after)

        used_code = None
        for code, uses_after in after.items():
            uses_before = int(before.get(code, 0))
            if uses_after > uses_before:
                used_code = code
                break
        if not used_code:
            return
        inv = next((i for i in invites if str(i.code) == used_code), None)
        inviter = getattr(inv, "inviter", None) if inv else None
        if not inviter or not getattr(inviter, "id", None):
            return
        referrer_id = int(inviter.id)
        if referrer_id == int(member.id):
            return
        # Store referral (one-time)
        try:
            payment_database.init_db()
            ok, _ = payment_database.set_referral(
                referred_user_id=int(member.id),
                referrer_user_id=referrer_id,
                guild_id=int(g.id),
                source="invite",
                invite_code=str(used_code),
            )
            if ok:
                print(f"{self._get_log_prefix()} [Referral] {referrer_id} referred {member.id} via invite {used_code}")
        except Exception:
            return

    def _create_premium_embed(self, title, color=0x00B3FF, description=""):
        embed = discord.Embed(title=title, color=color, description=description)
        foot = f"{BRAND_NAME} • {datetime.now().strftime('%H:%M:%S')} • Created by Sultan"
        if BRAND_LOGO_FILE:
            embed.set_footer(text=foot, icon_url=f"attachment://{BRAND_LOGO_FILE}")
        else:
            embed.set_footer(text=foot)
        return embed

    def _get_stats_field(self, fields_dict):
        val = ""
        for label, value in fields_dict.items():
            val += f"**{label}:** {value}\n"
        val += "\u200b"
        return val

    def _get_activity_field(self, items_list):
        if not items_list: return "No recent activity recorded.\n\u200b"
        val = "\n".join(items_list)
        val += "\n\u200b"
        return val

    def is_personal_profile(self, account):
        """Checks if an account is likely a personal profile/worker instead of a project."""
        text = f"{account.name} {account.description or ''}".lower()
        handle = account.screen_name.lower()
        
        # DISABLED: Was blocking real early projects like @Djinnmarket, @IntentLayerSOL
        # if account.followers_count < 100 and len(bio) < 50:
        #     return True, "low followers + minimal bio"
        
        # Handle patterns that indicate personal accounts (short 0x handles)
        if handle.startswith('0x') and len(handle) <= 10:
            return True, "0x handle pattern"
        
        # 🚨 HIGH-PRIORITY BAN WORDS (Check FIRST, before project indicators)
        critical_bans = [
            "manager", "collab manager", "moderator", "ambassador", "contributor",
            "personal account", "alpha caller", "content creator", "researcher",
            "trader", "shitpost", "ct", "thread", "calling", "consultant",
            "growth", "marketing", "strategies", "associated with", "involvement in",
            "working with", "helping", "supporting", "advising"
        ]
        for word in critical_bans:
            if re.search(rf"\b{re.escape(word)}\b", text):
                return True, word
        
        # 🛡️ PROJECT INDICATORS (Priority Overrides)
        project_indicators = [
            "official", "building", "$", "coin", "token", "ecosystem", "protocol", 
            "solana", "ether", "network", "utility", "launching", "mainnet", 
            "testnet", "whitelist", "presale", "airdrop", "eth", "web3", "art", 
            ".art", ".xyz", ".com", "pfp", "collection", "minting", "meme",
            "defi", "game", "infra", "agent", "neural", "gpu", "swap", "liquidity",
            "prediction", "market", "lab", "velcor3"
        ]
        if any(indicator in text for indicator in project_indicators):
            return False, None

        # 🚫 OTHER BAN WORDS (Regex with word boundaries)
        ban_words = [
            "advisor", "enthusiast", "collector", "printer", "writer", "team",
            "analyst", "builder", "developer", "engineer", "designer", "artist",
            "founder", "ceo", "co-founder", "partner", "intern", "marketing",
            "growth", "strategy", "head of", "lead", "investor", "member",
            "lover", "fan", "founder of", "moderator of", "builder of",
            "nft trader", "influencer", "advisor", "freelance",
            "waifu", "anime", "otaku", "cosplay"
        ]
        
        for word in ban_words:
            if re.search(rf"\b{re.escape(word)}\b", text):
                return True, word

        return False, None

    async def on_command(self, ctx):
        print(f"{self._get_log_prefix()} 🤖 [COMMAND] {ctx.author}: {ctx.message.content}")

    async def on_command_error(self, ctx, error):
        """Global error handler for commands."""
        print(f"{self._get_log_prefix()} ❌ Command Error from {ctx.author}: {error}")
        if isinstance(error, commands.CommandNotFound):
            return # Ignore unknown commands
        try:
            await ctx.send(f"❌ **Bot Error:** {str(error)}")
        except: pass

    @tasks.loop(seconds=config.CHECK_INTERVAL_SECONDS)
    async def monitor_twitter(self):
        try:
            current_hour = datetime.now().hour
            is_off_peak = config.OFF_PEAK_HOURS[0] <= current_hour < config.OFF_PEAK_HOURS[1]
            scan_mode = "🌙 OFF-PEAK" if is_off_peak else "☀️ PEAK"
            
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] --- 📡 BRAIN SCAN ({scan_mode}) ---")
            self.current_scan_discoveries = 0
            
            # Load HVAs from DB
            priority_list = database.get_hva_priority_list()
            total_batches = (len(priority_list) + config.BATCH_SIZE - 1) // config.BATCH_SIZE
            print(f"      ✔ Scanning {len(priority_list)} hunters across {total_batches} batches.")
            
            if not priority_list:
                print(f"      ⚠️ No HVAs found in priority list. Check config.HVA_LIST or database.")
            
            # Ensure channels are populated (fallback if on_ready hasn't finished)
            if not self.active_main_channels:
                for channel_id in config.DISCORD_CHANNEL_IDS:
                    try:
                        ch = self.get_channel(channel_id) or await self.fetch_channel(channel_id)
                        if ch: self.active_main_channels.append(ch)
                    except Exception as e:
                        print(f"      ⚠️ Access Error: Could not load main channel {channel_id}: {e}")
            
            if not self.active_escalation_channels:
                for cid in config.ESCALATION_CHANNEL_IDS:
                    try:
                        ch = self.get_channel(cid) or await self.fetch_channel(cid)
                        if ch: self.active_escalation_channels.append(ch)
                    except Exception as e:
                        print(f"      ⚠️ Access Error: Could not load escalation channel {cid}: {e}")

            # Let other on_ready tasks (reports, etc.) finish before Twikit-heavy HVA batches.
            await asyncio.sleep(5)

            for batch_num in range(total_batches):
                await asyncio.sleep(0) # Yield for heartbeats
                self.twitter.check_cooldown()
                if self.twitter.is_rate_limited:
                    print(f"{self._get_log_prefix()} ⛔ Rate limited. Skipping remaining batches for this cycle.")
                    break
                
                batch = priority_list[batch_num*config.BATCH_SIZE : (batch_num+1)*config.BATCH_SIZE]
                print(f"\n{self._get_log_prefix()} 📦 [BATCH {batch_num + 1}/{total_batches}] Checking {len(batch)} {BRAND_NAME} hunters...")
                
                for hva_handle in batch:
                    await asyncio.sleep(0) # Yield for heartbeats
                    if self.twitter.is_rate_limited: break
                    if random.random() < 0.03: continue

                    print(f"{self._get_log_prefix()} 🔎 [SCANNING] Checking to see what @{hva_handle} is up to...")
                    
                    # Update scan timestamp at the start of each scan
                    database.update_hva_scan_timestamp(hva_handle)
                    
                    try:
                        hva_id = await self.twitter.get_user_id(hva_handle)
                        if not hva_id:
                            print(f"      ⚠️ Skip @{hva_handle}: Could not resolve User ID.")
                            continue

                        following, _ = await self.twitter.get_new_following_with_delta(hva_id, hva_handle)
                        if following:
                            print(f"      ✅ SCAN COMPLETE: Found {len(following)} potential projects from @{hva_handle}")
                            for account in following:
                                await self.process_discovery(account, hva_handle, 'follow', self.active_main_channels)
                        else:
                            print(f"      ℹ️ SCAN COMPLETE: No new projects found from @{hva_handle} (This scan logged in database)")
                        
                        await asyncio.sleep(random.uniform(8, 18))

                        timeline = await self.twitter.get_user_timeline(hva_id, count=15)
                        # Same account can appear on many RT rows — process once per HVA scan
                        seen_rt_user_ids: Set[str] = set()
                        # Mentions can repeat across tweets/threads — dedup per HVA scan
                        seen_mention_handles: Set[str] = set()
                        for tweet in timeline:
                            await asyncio.sleep(0) # Yield for heartbeats
                            retweeted_user = None
                            if hasattr(tweet, 'retweeted_tweet') and tweet.retweeted_tweet:
                                retweeted_user = tweet.retweeted_tweet.user
                            elif hasattr(tweet, 'retweeted_status') and tweet.retweeted_status:
                                retweeted_user = tweet.retweeted_status.user
                            
                            if retweeted_user:
                                rid = getattr(retweeted_user, "id", None)
                                if rid is not None:
                                    rid_key = str(rid)
                                    if rid_key in seen_rt_user_ids:
                                        continue
                                    seen_rt_user_ids.add(rid_key)
                                await self.process_discovery(retweeted_user, hva_handle, 'retweet', self.active_main_channels)

                            # Also catch projects HVAs talk about without following/RT:
                            # extract @mentions from tweet text and resolve to accounts.
                            try:
                                text = getattr(tweet, "text", None) or getattr(tweet, "full_text", None) or ""
                            except Exception:
                                text = ""
                            if text:
                                for m in re.findall(r"@([A-Za-z0-9_]{1,15})", text):
                                    h = (m or "").strip().lower()
                                    if not h:
                                        continue
                                    if h == hva_handle.lower():
                                        continue
                                    if h in seen_mention_handles:
                                        continue
                                    seen_mention_handles.add(h)
                                    user_obj = await self.twitter.get_user_by_handle(h)
                                    if user_obj:
                                        await self.process_discovery(
                                            user_obj, hva_handle, "mention", self.active_main_channels
                                        )
                    except Exception as e:
                        print(f"      ❌ Scan error for @{hva_handle}: {e}")
                    
                    await asyncio.sleep(random.uniform(15, 35))
                
                if batch_num < total_batches - 1:
                    print(f"{self._get_log_prefix()} ☕ Batch complete. Resting for {config.BATCH_BREAK_SECONDS}s...")
                    for i in range(config.BATCH_BREAK_SECONDS // 30):
                        await asyncio.sleep(30)
                        remaining = config.BATCH_BREAK_SECONDS - ((i + 1) * 30)
                        if remaining > 0:
                            print(f"      ...resting ({remaining}s remaining)")

            next_scan = datetime.now() + timedelta(seconds=config.CHECK_INTERVAL_SECONDS)
            print(f"\n{self._get_log_prefix()} --- ✅ SCAN COMPLETE ({self.current_scan_discoveries} new) ---")
            print(f"{self._get_log_prefix()} 💤 Sleeping... Next scan starts around: {next_scan.strftime('%H:%M:%S')}")

        except Exception as e:
            print(f"❌ CRITICAL Error in monitor_twitter: {e}")
            import traceback
            traceback.print_exc()

    @tasks.loop(hours=4)
    async def trending_report(self):
        print(f"{self._get_log_prefix()} 📊 [REPORT] Generating Trending Report...")
        report_channels_ids = list(
            getattr(config, "TRENDING_REPORT_CHANNELS", config.TRENDING_CHANNEL_IDS)
        )
        for tid in guild_license.all_trending_channel_ids():
            if tid and tid not in report_channels_ids:
                report_channels_ids.append(tid)
        report_channels = []
        for cid in report_channels_ids:
            try:
                ch = self.get_channel(cid) or await self.fetch_channel(cid)
                if ch: report_channels.append(ch)
            except Exception as e:
                print(f"      ⚠️ Could not load channel {cid}: {e}")
        
        if not report_channels:
            print(f"      ❌ Skip Report: No valid trending channels found.")
            return
        
        # Wait for bot to fully settle
        await asyncio.sleep(2)

        embed = self._create_premium_embed(f"🔥 {BRAND_NAME} Trending Report", color=discord.Color.orange())
        
        def get_section_text(hours, limit=10, min_age=0, max_age=9999):
            results = database.get_trending_projects(hours=hours, limit=50)
            if not results: return "No data available."
            lines = []
            count = 0
            seen_ids = set()
            for p_id, handle, name, description, created_at, s24h, s7d, total, ai_summary, ai_category in results:
                if p_id in seen_ids: continue
                age = self.get_account_age_days(created_at)
                if age < min_age or age > max_age: continue
                display_name = name if name and name.lower() != handle.lower() else f"@{handle}"
                
                # Simple Metrics: [Total Smarts] + [Age]
                metrics = f"💎 **{total} Smarts** | `{age}d`"
                cat_label = f"• {ai_category}" if ai_category else ""
                
                new_line = f"{count+1}. **{display_name}** ([@{handle}](https://x.com/{handle}))\n   └ {metrics} {cat_label}\n"
                
                # Discord Field Limit is 1024. Stop if we get close.
                if len("\n".join(lines)) + len(new_line) > 950:
                    lines.append(f"...and {len(results) - count} more.")
                    break
                    
                lines.append(new_line.strip())
                seen_ids.add(p_id)
                count += 1
                if count >= limit: break
            return "\n".join(lines) if lines else "No trending projects found."

        embed.add_field(name="🚀 New Projects (≤30d)", value=get_section_text(24, 10, min_age=0, max_age=30), inline=False)
        embed.add_field(name="📈 Established Projects (30-100d)", value=get_section_text(24, 10, min_age=31, max_age=100), inline=False)
        foot = f"{BRAND_NAME} • Created by Sultan • {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        if BRAND_LOGO_FILE:
            embed.set_footer(text=foot, icon_url=f"attachment://{BRAND_LOGO_FILE}")
        else:
            embed.set_footer(text=foot)

        if BRAND_LOGO_PATH:
            embed.set_thumbnail(url=f"attachment://{BRAND_LOGO_FILE}")
        if BRAND_BANNER_PATH:
            embed.set_image(url=f"attachment://{BRAND_BANNER_FILE}")

        for ch in report_channels:
            current_files = []
            if BRAND_LOGO_PATH:
                current_files.append(discord.File(BRAND_LOGO_PATH, filename=BRAND_LOGO_FILE))
            # Trending report always wants the banner; also ensures the attachment exists.
            if BRAND_BANNER_PATH:
                current_files.append(discord.File(BRAND_BANNER_PATH, filename=BRAND_BANNER_FILE))
                
            try: 
                await ch.send(embed=embed, files=current_files)
            except Exception as e: 
                print(f"❌ Failed to send trending report: {e}")

    @tasks.loop(time=datetime.strptime(config.X_POST_TIME_UTC, "%H:%M").time())
    async def daily_x_trending_task(self):
        if not config.ENABLE_X_POST_DAILY: return
        all_results = database.get_trending_projects_24h()
        if not all_results: return
        top_projects = []
        for p_id, handle, name, description, created_at, count in all_results:
            if self.is_personal_profile(type('obj', (object,), {'name': name, 'description': description}))[0]: continue
            top_projects.append((handle, count))
            if len(top_projects) >= 5: break
        if not top_projects: return
        lines = [f"{BRAND_NAME}: Daily Trending Recap", "Top 5 Projects gaining Smarts (last 24h):\n"]
        for i, (handle, count) in enumerate(top_projects):
            emoji_num = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣"][i]
            lines.append(f"{emoji_num} @{handle} - {count} Smarts\n")
        lines.extend(["Early signals. High conviction.", "#Velcor3 #Crypto #early"])
        tweet_text = "\n".join(lines)
        await self.twitter.create_tweet(tweet_text)

    @tasks.loop(time=datetime.strptime(config.X_DAILY_RECAP_TIME_UTC, "%H:%M").time())
    async def x_daily_recap_task(self):
        """Daily X: top Solana movers (Kolfi) + top Velcor3 finds (24h), via primary twikit session."""
        if not config.ENABLE_X_DAILY_RECAP:
            return
        pfx = self._get_log_prefix()
        token_attempted = False
        if config.X_DAILY_RECAP_POST_TOKENS and config.KOLFI_API_KEY:
            token_attempted = True
            try:
                async with aiohttp.ClientSession() as session:
                    rows, err = await fetch_kolfi_top_movers_rows(session, config.KOLFI_API_KEY)
                if err and not rows:
                    print(f"{pfx} [X recap] Top movers: {err}")
                else:
                    text = format_x_top_movers_tweet(
                        rows,
                        brand_name=BRAND_NAME,
                        top_n=config.X_DAILY_RECAP_TOP_N,
                    )
                    if await self.twitter.create_tweet(text):
                        print(f"{pfx} [X recap] Posted token movers (top {config.X_DAILY_RECAP_TOP_N})")
            except Exception as e:
                print(f"{pfx} [X recap] Top movers error: {e}")

        if token_attempted and config.X_DAILY_RECAP_POST_FINDS:
            await asyncio.sleep(5)

        if config.X_DAILY_RECAP_POST_FINDS:
            try:
                rows = database.get_projects_finds_24h(config.X_DAILY_RECAP_TOP_N)
                text = format_x_daily_finds_tweet(
                    rows,
                    brand_name=BRAND_NAME,
                    top_n=config.X_DAILY_RECAP_TOP_N,
                )
                if await self.twitter.create_tweet(text):
                    print(f"{pfx} [X recap] Posted daily finds (top {config.X_DAILY_RECAP_TOP_N})")
            except Exception as e:
                print(f"{pfx} [X recap] Daily finds error: {e}")

    @x_daily_recap_task.before_loop
    async def before_x_daily_recap_task(self):
        await self.wait_until_ready()

    @tasks.loop(hours=24)
    async def recalculate_hva_scores(self):
        """Self-learning: Periodically update HVA quality scores based on performance."""
        print(f"{self._get_log_prefix()} 🧠 [SELF-LEARNING] Recalculating HVA Quality Scores...")
        database.update_hva_quality_scores()
        print(f"      ✅ Scores updated based on discovery performance.")

    @tasks.loop(time=config.DAILY_MINTS_AUTO_TIME)
    async def daily_mints_auto_feed(self):
        """Daily post of daily-mints.com calendar (one embed per mint, X PFP + banner when linked)."""
        if not config.ENABLE_DAILY_MINTS_AUTO:
            return
        cid = config.DAILY_MINTS_AUTO_CHANNEL_ID
        if not cid:
            return
        try:
            ch = self.get_channel(cid) or await self.fetch_channel(cid)
        except Exception as e:
            print(f"{self._get_log_prefix()} [DailyMints] Channel resolve error: {e}")
            return
        if not ch:
            print(f"{self._get_log_prefix()} [DailyMints] Channel {cid} not found.")
            return
        # Reset dedup set at start of each daily run (new day = new posts OK)
        self._daily_mints_posted_today = set()
        print(f"{self._get_log_prefix()} [DailyMints] Posting calendar (scope={config.DAILY_MINTS_AUTO_SCOPE}, limit={config.DAILY_MINTS_AUTO_LIMIT})…")
        n, err = await _run_daily_mints_post(
            self,
            ch,
            config.DAILY_MINTS_AUTO_SCOPE,
            config.DAILY_MINTS_AUTO_LIMIT,
            enrich_x_art=True,
            posted_urls=self._daily_mints_posted_today,
        )
        if err:
            print(f"{self._get_log_prefix()} [DailyMints] {err}")
        else:
            nm = getattr(ch, "name", None) or str(cid)
            print(f"{self._get_log_prefix()} [DailyMints] Posted {n} embed(s) → #{nm}")

    @daily_mints_auto_feed.before_loop
    async def before_daily_mints_auto_feed(self):
        await self.wait_until_ready()

    @tasks.loop(minutes=config.KOLFI_POLL_MINUTES)
    async def kolfi_tokens_feed(self):
        """Poll token API; post on new calls / MC or ATH updates (not bare new listings)."""
        if not config.ENABLE_KOLFI_FEED or not config.KOLFI_API_KEY:
            return
        # Backoff on transient network/DNS failures (prevents log spam).
        now = time.time()
        until = float(getattr(self, "_kolfi_backoff_until", 0) or 0)
        if until and now < until:
            return
        ch_map = {
            "low": config.KOLFI_CHANNEL_LOW_ID,
            "100k": config.KOLFI_CHANNEL_100K_ID,
            "1m": config.KOLFI_CHANNEL_1M_ID,
        }
        if not any(ch_map.values()):
            return
        b_path = BRAND_BANNER_PATH if config.KOLFI_USE_BRAND_BANNER else None
        b_file = BRAND_BANNER_FILE if config.KOLFI_USE_BRAND_BANNER else None
        try:
            async with aiohttp.ClientSession() as session:
                n, err, kst = await run_kolfi_feed_once(
                    self,
                    session,
                    config.KOLFI_API_KEY,
                    ch_map,
                    send_delay_sec=config.KOLFI_SEND_DELAY_SEC,
                    max_alerts_per_bucket=config.KOLFI_MAX_ALERTS_PER_BUCKET,
                    brand_name=BRAND_NAME,
                    embed_color=0x202025,
                    banner_path=b_path,
                    banner_filename=b_file,
                    mc_move_pct=config.KOLFI_MC_MOVE_ALERT_PCT,
                    ath_break_pct=config.KOLFI_ATH_BREAK_PCT,
                    enable_ai_review=config.KOLFI_AI_REVIEW,
                )
            pfx = self._get_log_prefix()
            if err:
                # Transient network issues (DNS/getaddrinfo, connect errors, timeouts) should backoff.
                err_s = str(err)
                err_l = err_s.lower()
                if any(
                    k in err_l
                    for k in (
                        "cannot connect to host",
                        "getaddrinfo failed",
                        "name or service not known",
                        "temporary failure",
                        "timeout",
                        "timed out",
                        "connection reset",
                        "connection aborted",
                    )
                ):
                    backoff = float(getattr(self, "_kolfi_backoff_sec", 60.0) or 60.0)
                    backoff = min(max(60.0, backoff), 30 * 60.0)
                    self._kolfi_backoff_sec = min(backoff * 2.0, 30 * 60.0)
                    self._kolfi_backoff_until = time.time() + backoff
                    last = float(getattr(self, "_kolfi_last_err_log", 0) or 0)
                    if time.time() - last >= 90:
                        self._kolfi_last_err_log = time.time()
                        print(f"{pfx} [Token alerts] {err_s} (backing off {int(backoff)}s)")
                else:
                    print(f"{pfx} [Token alerts] {err_s}")
            else:
                # reset backoff on success
                self._kolfi_backoff_sec = 60.0
                self._kolfi_backoff_until = 0
                ni = kst.get("items", 0)
                nq = kst.get("queued", 0)
                if n:
                    print(f"{pfx} [Token alerts] Polled {ni} tokens, {nq} alert(s) queued → posted {n}")
                else:
                    print(
                        f"{pfx} [Token alerts] Polled {ni} tokens, 0 alerts "
                        f"(new calls vs last poll, or MC ≥{config.KOLFI_MC_MOVE_ALERT_PCT}% / "
                        f"ATH +{config.KOLFI_ATH_BREAK_PCT}% vs last alert baseline)"
                    )
        except Exception as e:
            print(f"{self._get_log_prefix()} [Token alerts] Error: {e}")

    @kolfi_tokens_feed.before_loop
    async def before_kolfi_tokens_feed(self):
        await self.wait_until_ready()

    async def _run_kolfi_leaderboard_post(self):
        """Daily call-performance leaderboard — new message each run (for role pings)."""
        if not config.ENABLE_KOLFI_LEADERBOARD or not config.KOLFI_API_KEY:
            return
        if not config.KOLFI_LEADERBOARD_CHANNEL_ID:
            return
        pfx = self._get_log_prefix()
        try:
            async with aiohttp.ClientSession() as session:
                ok, err = await run_kolfi_leaderboard_once(
                    self,
                    session,
                    config.KOLFI_API_KEY,
                    config.KOLFI_LEADERBOARD_CHANNEL_ID,
                    brand_name=BRAND_NAME,
                    embed_color=0x202025,
                    top_n=config.KOLFI_LEADERBOARD_TOP_N,
                    max_call_age_hours=config.KOLFI_LEADERBOARD_MAX_CALL_AGE_HOURS,
                )
            if err:
                print(f"{pfx} [Call leaderboard] {err}")
            elif ok:
                _w = format_kolfi_leaderboard_window(float(config.KOLFI_LEADERBOARD_MAX_CALL_AGE_HOURS))
                print(
                    f"{pfx} [Call leaderboard] posted → #{config.KOLFI_LEADERBOARD_CHANNEL_ID} "
                    f"(first call in {_w} · by ATH)"
                )
        except Exception as e:
            print(f"{pfx} [Call leaderboard] Error: {e}")

    async def _run_kolfi_top_movers_post(self):
        """Daily post: top **24h %%** among mints **we first alerted** in the last 24h (watchlist), Dex-enriched."""
        if not config.ENABLE_KOLFI_DAILY_TOP_MOVERS or not config.KOLFI_API_KEY:
            return
        cid = config.KOLFI_DAILY_TOP_MOVERS_CHANNEL_ID
        if not cid:
            return
        pfx = self._get_log_prefix()
        try:
            async with aiohttp.ClientSession() as session:
                ok, err = await run_kolfi_alert_watchlist_daily_once(
                    self,
                    session,
                    config.KOLFI_API_KEY,
                    cid,
                    brand_name=BRAND_NAME,
                    embed_color=0x202025,
                    top_n=config.KOLFI_DAILY_TOP_MOVERS_TOP_N,
                )
            if err:
                print(f"{pfx} [Our alerts 24h recap] {err}")
            elif ok:
                print(f"{pfx} [Our alerts 24h recap] posted → channel {cid} (ranked by Dex 24h %%)")
        except Exception as e:
            print(f"{pfx} [Our alerts 24h recap] Error: {e}")

    async def _run_escalation_daily_top_post(self):
        """Daily Discord post: all Velcor3 finds (alerted projects) in the last 24h."""
        if not config.ENABLE_ESCALATION_DAILY_TOP_MOVERS:
            return
        target_ids: List[int] = []
        if getattr(config, "ESCALATION_DAILY_TOP_MOVERS_CHANNEL_ID", 0):
            target_ids.append(int(config.ESCALATION_DAILY_TOP_MOVERS_CHANNEL_ID))
        try:
            for cid in guild_license.all_daily_finds_channel_ids():
                if cid and int(cid) not in target_ids:
                    target_ids.append(int(cid))
        except Exception:
            pass
        if not target_ids:
            return
        pfx = self._get_log_prefix()
        try:
            state = _daily_finds_load_state()

            for cid in target_ids:
                try:
                    ch = self.get_channel(cid) or await self.fetch_channel(cid)
                except Exception:
                    ch = None
                if not ch:
                    print(f"{pfx} [Daily finds] channel {cid} not found")
                    continue

                ch_state = (state or {}).get(int(cid), {}) if isinstance(state, dict) else {}
                prev_ids = list((ch_state.get("message_ids") or []) if isinstance(ch_state, dict) else [])
                last_at = str((ch_state.get("last_alerted_at") or "") if isinstance(ch_state, dict) else "")

                # Only post NEW finds since last run (per channel). If first run, use rolling 24h.
                if last_at:
                    rows = database.get_projects_alerted_since_utc(last_at, config.ESCALATION_DAILY_TOP_MOVERS_TOP_N)
                else:
                    rows = database.get_projects_finds_24h(config.ESCALATION_DAILY_TOP_MOVERS_TOP_N)
                if not rows:
                    print(f"{pfx} [Daily finds] channel {cid}: 0 new finds (skipping)")
                    continue
                rows = await self._maybe_backfill_daily_finds_followers(rows)
                embeds = self._build_daily_finds_embeds(rows)

                if prev_ids:
                    for mid in prev_ids:
                        try:
                            m = await ch.fetch_message(mid)
                            if m.author.id == self.user.id:
                                await m.delete()
                        except discord.NotFound:
                            pass
                        except discord.Forbidden:
                            print(f"{pfx} [Daily finds] cannot delete message {mid} (missing permissions)")
                        except Exception as ex:
                            print(f"{pfx} [Daily finds] delete {mid}: {ex}")
                        await asyncio.sleep(0.35)

                new_ids: List[int] = []
                for emb in embeds:
                    emb2, files = self._with_brand_banner_fallback(emb, [])
                    sent = await ch.send(embed=emb2, files=files) if files else await ch.send(embed=emb2)
                    new_ids.append(sent.id)
                # Track max alerted_at we posted so we don't repost the same projects next run.
                try:
                    newest_alerted = str(rows[0][5] or "")
                except Exception:
                    newest_alerted = ""
                state[int(cid)] = {"message_ids": new_ids, "last_alerted_at": newest_alerted}
                print(f"{pfx} [Daily finds] posted → channel {cid} ({len(rows)} finds, {len(embeds)} message(s))")

            _daily_finds_save_state(state)
        except Exception as e:
            print(f"{pfx} [Daily finds] Error: {e}")

    @tasks.loop(hours=8)
    async def escalation_daily_interval(self):
        """Every 8 hours: post the rolling 24h daily-finds digest."""
        await self._run_escalation_daily_top_post()

    @escalation_daily_interval.before_loop
    async def before_escalation_daily_interval(self):
        await self.wait_until_ready()

    async def _maybe_backfill_daily_finds_followers(
        self, rows: List[tuple], *, max_fetch: int = 35
    ) -> List[tuple]:
        """Fill NULL followers_count via X lookup (capped) so the digest always shows numbers when possible."""
        if not rows:
            return rows
        out: List[tuple] = []
        fetched = 0
        pfx = self._get_log_prefix()
        for row in rows:
            r = list(row) + [None] * max(0, 9 - len(row))
            r = r[:9]
            if r[8] is None and fetched < max_fetch and r[0]:
                fetched += 1
                try:
                    acc = await self.twitter.get_user_info(str(r[0]))
                    if acc is not None:
                        fc = getattr(acc, "followers_count", None)
                        if fc is not None:
                            database.update_project_followers_count(r[0], fc)
                            r[8] = int(fc)
                except Exception as ex:
                    print(f"{pfx} [Daily finds] follower backfill @{r[1]}: {ex}")
                await asyncio.sleep(0.45)
            out.append(tuple(r))
        if fetched:
            print(f"{pfx} [Daily finds] backfilled followers for up to {fetched} account(s)")
        return out

    async def _run_daily_mints_boot_post(self):
        """Boot-time post of the daily mints calendar — mirrors what the scheduled task does."""
        if not config.ENABLE_DAILY_MINTS_AUTO:
            return
        cid = config.DAILY_MINTS_AUTO_CHANNEL_ID
        if not cid:
            return
        pfx = self._get_log_prefix()
        try:
            ch = self.get_channel(cid) or await self.fetch_channel(cid)
        except Exception as e:
            print(f"{pfx} [DailyMints] Channel resolve error: {e}")
            return
        if not ch:
            print(f"{pfx} [DailyMints] Channel {cid} not found.")
            return
        self._daily_mints_posted_today = set()
        n, err = await _run_daily_mints_post(
            self,
            ch,
            config.DAILY_MINTS_AUTO_SCOPE,
            config.DAILY_MINTS_AUTO_LIMIT,
            enrich_x_art=True,
            posted_urls=self._daily_mints_posted_today,
        )
        if err:
            print(f"{pfx} [DailyMints] {err}")
        else:
            nm = getattr(ch, "name", None) or str(cid)
            print(f"{pfx} [DailyMints] Boot post: {n} embed(s) → #{nm}")

    def _snippet_daily_find_report(self, row: tuple) -> str:
        """One short line for the daily finds recap: AI summary, else trimmed bio/description."""
        r9 = (list(row) + [None] * 9)[:9]
        _pid, _handle, _name, desc, _created_at, _alerted_at, _ai_cat, ai_summary, _fol = r9
        raw = (ai_summary or "").strip()
        if not raw and desc:
            raw = str(desc).strip()
        if not raw:
            return ""
        one = " ".join(raw.split())
        max_len = 240
        if len(one) > max_len:
            one = one[: max_len - 1].rstrip() + "…"
        return one

    def _format_daily_find_line(self, index: int, row: tuple) -> str:
        r9 = (list(row) + [None] * 9)[:9]
        _pid, handle, name, _desc, _created_at, alerted_at, ai_cat, _ai_sum, followers = r9
        display_name = name if name and str(name).lower() != str(handle).lower() else f"@{handle}"
        cat_label = (str(ai_cat).strip() if ai_cat else "") or "—"
        cat_label = cat_label.replace("`", "'")[:80]
        fol = _fmt_followers_display(followers)
        t = ""
        if alerted_at:
            ts = str(alerted_at).replace("T", " ")[:19]
            t = f" · `{ts} UTC`"
        head = (
            f"`{index}.` **{display_name}** ([@{handle}](https://x.com/{handle}))"
            f" · **Followers:** `{fol}` · **Category:** `{cat_label}`{t}"
        )
        snippet = self._snippet_daily_find_report(row)
        if snippet:
            return f"{head}\n*— {snippet}*"
        return head

    def _build_daily_finds_embeds(self, rows):
        """One or more embeds; Discord description max 4096 chars per embed."""
        ts = datetime.now(timezone.utc)
        foot = f"{BRAND_NAME} · rolling 24h finds · Not financial advice"
        base_title = f"🔍 {BRAND_NAME} · Daily finds · rolling 24h (UTC)"
        if not rows:
            e = Embed(
                title=base_title,
                description="_No Velcor3 finds were posted in the last 24h._",
                color=Color.orange(),
                timestamp=ts,
            )
            e.set_footer(text=foot)
            return [e]

        intro = (
            f"**{len(rows)}** find(s) with `alerted_at` in the rolling **last 24 hours UTC** (newest first). "
            f"**Followers** = X snapshot (we refresh on re-scan; missing rows are backfilled when this digest posts).\n\n"
        )
        pieces = [intro.rstrip()]
        for i, row in enumerate(rows, 1):
            pieces.append(self._format_daily_find_line(i, row))

        chunks = []
        current = pieces[0]
        for p in pieces[1:]:
            candidate = current + "\n\n" + p
            if len(candidate) <= 4096:
                current = candidate
            else:
                chunks.append(current)
                current = p if len(p) <= 4096 else (p[:4093] + "...")
        if current:
            chunks.append(current)

        embeds = []
        nch = len(chunks)
        for pi, chunk in enumerate(chunks, 1):
            title = base_title if nch == 1 else f"{base_title} · {pi}/{nch}"
            e = Embed(title=title, description=chunk, color=Color.orange(), timestamp=ts)
            e.set_footer(text=foot)
            embeds.append(e)
        return embeds

    @tasks.loop(time=config.ESCALATION_DAILY_TOP_MOVERS_TIME_UTC)
    async def escalation_daily_top_movers(self):
        await self._run_escalation_daily_top_post()

    @escalation_daily_top_movers.before_loop
    async def before_escalation_daily_top_movers(self):
        await self.wait_until_ready()

    @tasks.loop(time=config.KOLFI_LEADERBOARD_TIME_UTC)
    async def kolfi_leaderboard_daily(self):
        await self._run_kolfi_leaderboard_post()

    @kolfi_leaderboard_daily.before_loop
    async def before_kolfi_leaderboard_daily(self):
        await self.wait_until_ready()

    @tasks.loop(time=config.KOLFI_DAILY_TOP_MOVERS_TIME_UTC)
    async def kolfi_daily_top_movers(self):
        """Daily leaderboard: top 24h % movers for overview tokens."""
        await self._run_kolfi_top_movers_post()

    @kolfi_daily_top_movers.before_loop
    async def before_kolfi_daily_top_movers(self):
        await self.wait_until_ready()

    @tasks.loop(minutes=config.MINTS_OVERVIEW_POLL_MINUTES)
    async def mints_overview_feed(self):
        """Post a new Waypoint 1h Mints Overview leaderboard each run (for role pings)."""
        if not config.ENABLE_MINTS_OVERVIEW or not config.MINTS_OVERVIEW_CHANNEL_ID:
            return
        pfx = self._get_log_prefix()
        try:
            async with aiohttp.ClientSession() as session:
                ok, err = await run_overview_once(
                    self,
                    session,
                    config.MINTS_OVERVIEW_CHANNEL_ID,
                    brand_name=BRAND_NAME,
                    embed_color=0x202025,
                    top_n=config.MINTS_OVERVIEW_TOP_N,
                )
            if err:
                print(f"{pfx} [MintsOverview] Error: {err}")
            elif ok:
                print(f"{pfx} [MintsOverview] posted → channel {config.MINTS_OVERVIEW_CHANNEL_ID}")
        except Exception as e:
            print(f"{pfx} [MintsOverview] Unhandled error: {e}")

    @mints_overview_feed.before_loop
    async def before_mints_overview_feed(self):
        await self.wait_until_ready()

    def format_age(self, created_at):
        if isinstance(created_at, str):
            try: dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
            except: return "Unknown"
        else: dt = created_at
        now = datetime.now(dt.tzinfo)
        diff = now - dt
        if diff.days == 0: return "today"
        elif diff.days == 1: return "a day ago"
        else: return f"{diff.days} days ago"

    def get_account_age_days(self, created_at):
        if isinstance(created_at, str):
            try: dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
            except: return 999
        else: dt = created_at
        now = datetime.now(dt.tzinfo)
        return (now - dt).days

    def classify_project(self, account):
        text = f"{getattr(account, 'name', '')} {getattr(account, 'description', '')}".lower()
        for category, keywords in config.PROJECT_CATEGORIES.items():
            for keyword in keywords:
                if keyword in text: return category
        return "❓ Unknown"

    def calculate_signal_strength(self, project_id):
        signal_data = database.get_signal_data(project_id)
        if not signal_data: return None, 0, 0
        first_discovered_str, current_level = signal_data
        try: first_discovered = datetime.fromisoformat(first_discovered_str)
        except: return None, 0, 0
        
        hours_elapsed = (datetime.now() - first_discovered).total_seconds() / 3600
        interactions = database.get_project_follows(project_id)
        hva_count = len(set([i[0] for i in interactions]))
        
        new_level = 'none'
        if hva_count >= 11 and hours_elapsed <= 6: new_level = 'strong'
        elif hva_count >= 5 and hours_elapsed <= 4: new_level = 'medium'
        elif hva_count >= 2 and hours_elapsed <= 2: new_level = 'initial'
        return new_level, hva_count, hours_elapsed

    async def check_signal_escalation(self, account):
        is_personal, _ = self.is_personal_profile(account)
        if is_personal: return
        signal_data = database.get_signal_data(account.id)
        if not signal_data: return
        current_level = signal_data[1]
        new_level, hva_count, hours_elapsed = self.calculate_signal_strength(account.id)
        levels = ['none', 'initial', 'medium', 'strong']
        icon = '🚀' if levels.index(new_level) > levels.index(current_level) else '⚪'
        print(f"      {icon} [SIGNAL] @{account.screen_name} | {current_level.upper()} → {new_level.upper()} | Smarts: {hva_count} | {hours_elapsed:.1f}h")
        if levels.index(new_level) > levels.index(current_level):
            database.update_signal_level(account.id, new_level)

    def get_status_label(self, followers_count):
        if followers_count >= 1000: return "🔥 Could be Something"
        if followers_count >= 500: return "✨ Early Brain"
        if followers_count >= 100: return "🌱 Fresh"
        return "🐣 New born"

    def create_score_bar(self, score):
        if score >= 80: grade, emoji, color = "S", "🏆", 0x00FF88
        elif score >= 60: grade, emoji, color = "A", "⭐", 0xFFDD00
        elif score >= 40: grade, emoji, color = "B", "✨", 0xFF8800
        elif score >= 20: grade, emoji, color = "C", "💫", 0x5599FF
        else: grade, emoji, color = "D", "🌱", 0x888888
        
        # 28 blocks (Extended Full Width)
        length = 28
        filled = int((score / 100) * length)
        bar = "█" * filled + "▒" * (length - filled)
        return f"`{bar}` **{score}%**", grade, emoji, color

    @trending_report.before_loop
    async def before_trending(self):
        await self.wait_until_ready()

    @daily_x_trending_task.before_loop
    async def before_x_post(self):
        await self.wait_until_ready()

    @recalculate_hva_scores.before_loop
    async def before_recalculate(self):
        await self.wait_until_ready()

    @monitor_twitter.before_loop
    async def before_monitor(self):
        await self.wait_until_ready()
        await self.twitter.verify_all_sessions()

    # ─────────────────────────────────────────────────────────────────────────
    # CT DOMAIN WATCHER  (certificate transparency → new crypto domains)
    # ─────────────────────────────────────────────────────────────────────────

    @tasks.loop(minutes=config.CT_POLL_MINUTES)
    async def ct_domain_watcher_task(self):
        """
        Every CT_POLL_MINUTES: poll crt.sh for new TLS certs whose domain names
        match crypto/NFT/gaming keyword patterns.

        Flow per new domain:
          1. Post a "New Domain Detected" embed to the dedicated CT channel.
          2. If an X handle was found on the homepage → resolve user → feed into
             process_discovery() so the normal AI + DB + scoring pipeline runs.
        """
        pfx = self._get_log_prefix()
        print(f"{pfx} [CTWatcher] Polling CT logs (lookback: {config.CT_POLL_MINUTES + 15}m)...")

        # Resolve CT channel
        try:
            ct_ch = (
                self.get_channel(config.CT_WATCHER_CHANNEL_ID)
                or await self.fetch_channel(config.CT_WATCHER_CHANNEL_ID)
            )
        except Exception as e:
            print(f"{pfx} [CTWatcher] Cannot access channel {config.CT_WATCHER_CHANNEL_ID}: {e}")
            return

        # Poll crt.sh
        try:
            candidates = await poll_new_domains(
                lookback_minutes=config.CT_POLL_MINUTES + 15,
                max_new=config.CT_MAX_PER_CYCLE,
            )
        except Exception as e:
            print(f"{pfx} [CTWatcher] Poll error: {e}")
            return

        if not candidates:
            print(f"{pfx} [CTWatcher] No new domains this cycle.")
            return

        print(f"{pfx} [CTWatcher] {len(candidates)} new domain(s) found. Processing...")

        for domain, cert_id, x_handle in candidates:
            try:
                embed = self._build_ct_embed(domain, x_handle)
                files: List[discord.File] = []
                lf = self.brand_logo_file()
                if lf:
                    files.append(lf)
                embed, files = self._with_brand_banner_fallback(embed, files)
                if files:
                    await ct_ch.send(embed=embed, files=files)
                else:
                    await ct_ch.send(embed=embed)

                print(f"      [CTWatcher] ✅ Posted: {domain}" + (f" → @{x_handle}" if x_handle else ""))

                # If X handle found, also route into the discovery pipeline
                if x_handle:
                    user_obj = await self.twitter.get_user_by_handle(x_handle)
                    if user_obj:
                        print(f"      [CTWatcher] Routing @{x_handle} into process_discovery...")
                        await self.process_discovery(
                            user_obj,
                            f"ct:{domain}",
                            "ct_domain",
                            self.active_main_channels,
                        )

                await asyncio.sleep(1.5)  # gentle Discord rate-limit buffer

            except Exception as e:
                print(f"      [CTWatcher] Error on {domain}: {e}")

    @ct_domain_watcher_task.before_loop
    async def before_ct_watcher(self):
        await self.wait_until_ready()

    def _build_ct_embed(self, domain: str, x_handle: Optional[str]) -> discord.Embed:
        """Build the 'New Crypto Domain Detected' embed for the CT channel."""
        d_lower = domain.lower()
        matched_kws = [kw for kw in CRYPTO_KEYWORDS if kw in d_lower][:6]
        tld_match = next((tld for tld in TARGET_TLDS if d_lower.endswith(tld)), "")

        # Purple when domain-only; X-blue when a handle was found (higher conviction)
        color = 0x1D9BF0 if x_handle else 0x7C3AED

        embed = discord.Embed(
            title="🌐 New Crypto Domain Detected",
            color=color,
            timestamp=datetime.now(timezone.utc),
        )

        embed.add_field(
            name="🔗 Domain",
            value=f"[`{domain}`](https://{domain})",
            inline=True,
        )
        embed.add_field(
            name="🏷️ TLD",
            value=f"`{tld_match}`" if tld_match else "—",
            inline=True,
        )
        embed.add_field(
            name="🔑 Keywords",
            value=", ".join(f"`{k}`" for k in matched_kws) if matched_kws else "—",
            inline=True,
        )

        if x_handle:
            embed.add_field(
                name="🐦 X Account Found",
                value=f"[@{x_handle}](https://x.com/{x_handle}) ← routing to Discovery pipeline",
                inline=False,
            )
        else:
            embed.add_field(
                name="🐦 X Account",
                value="Not found on homepage — manual check may be needed.",
                inline=False,
            )

        embed.add_field(
            name="🔎 Investigate",
            value=(
                f"[Open site](https://{domain}) • "
                f"[crt.sh](https://crt.sh/?q={domain}) • "
                f"[Whois](https://who.is/whois/{domain})"
            ),
            inline=False,
        )

        foot = f"{BRAND_NAME} CT Watcher • {datetime.now().strftime('%H:%M UTC')} • Source: crt.sh"
        if BRAND_LOGO_FILE:
            embed.set_footer(text=foot, icon_url=f"attachment://{BRAND_LOGO_FILE}")
        else:
            embed.set_footer(text=foot)

        return embed

    # ─────────────────────────────────────────────────────────────────────────
    # X PROJECT-FIRST SEARCH (keyword-based discovery; no HVAs required)
    # ─────────────────────────────────────────────────────────────────────────

    @tasks.loop(minutes=config.X_PROJECT_SEARCH_POLL_MINUTES)
    async def x_project_first_search_task(self):
        """
        Searches recent tweets for project keywords, dedups by tweet-id in SQLite,
        and routes candidate accounts into process_discovery() using interaction_type='keyword_search'.
        """
        pfx = self._get_log_prefix()
        if self.twitter.is_rate_limited:
            print(f"{pfx} [XSearch] Skipped: rate limited.")
            return

        try:
            keywords = database.get_x_project_search_keywords(config.X_PROJECT_SEARCH_KEYWORDS_LIMIT)
        except Exception as e:
            print(f"{pfx} [XSearch] Failed to load keywords: {e}")
            return

        if not keywords:
            print(f"{pfx} [XSearch] No keywords in DB.")
            return

        print(f"{pfx} [XSearch] Running project-first scan over {len(keywords)} keywords...")

        candidates_processed = 0
        seen_handles_this_cycle: set[str] = set()

        for kw in keywords:
            if candidates_processed >= config.X_PROJECT_SEARCH_MAX_CANDIDATES_PER_CYCLE:
                break

            tweets = await self.twitter.search_recent_tweets(
                query=str(kw),
                count=config.X_PROJECT_SEARCH_MAX_TWEETS_PER_KEYWORD,
            )
            if not tweets:
                await asyncio.sleep(max(1.0, float(getattr(config, "TWIKIT_REQUEST_GAP_SEC", 1.35) or 1.0)))
                continue

            for t in tweets:
                if candidates_processed >= config.X_PROJECT_SEARCH_MAX_CANDIDATES_PER_CYCLE:
                    break

                tid = str(getattr(t, "id", "") or getattr(t, "tweet_id", "") or "")
                if tid:
                    if not database.is_x_project_search_tweet_new(tid):
                        continue
                    database.mark_x_project_search_tweet_seen(tid)

                # Candidate 1: tweet author
                author = getattr(t, "user", None) or getattr(t, "author", None)
                if author:
                    h = (getattr(author, "screen_name", "") or "").lower()
                    if h and h not in seen_handles_this_cycle:
                        seen_handles_this_cycle.add(h)
                        try:
                            age = self.get_account_age_days(getattr(author, "created_at", None))
                        except Exception:
                            age = 9999
                        if age <= config.NEW_ACCS_MAX_AGE_DAYS:
                            await self.process_discovery(
                                author,
                                f"search:{kw}",
                                "keyword_search",
                                self.active_main_channels,
                            )
                            candidates_processed += 1

                # Candidate 2: mentioned handles in tweet text
                try:
                    text = getattr(t, "text", None) or getattr(t, "full_text", None) or ""
                except Exception:
                    text = ""
                if text:
                    for m in re.findall(r"@([A-Za-z0-9_]{1,15})", text):
                        if candidates_processed >= config.X_PROJECT_SEARCH_MAX_CANDIDATES_PER_CYCLE:
                            break
                        mh = (m or "").strip().lower()
                        if not mh or mh in seen_handles_this_cycle:
                            continue
                        seen_handles_this_cycle.add(mh)
                        u = await self.twitter.get_user_by_handle(mh)
                        if not u:
                            continue
                        try:
                            age = self.get_account_age_days(getattr(u, "created_at", None))
                        except Exception:
                            age = 9999
                        if age <= config.NEW_ACCS_MAX_AGE_DAYS:
                            await self.process_discovery(
                                u,
                                f"search:{kw}",
                                "keyword_search",
                                self.active_main_channels,
                            )
                            candidates_processed += 1

                await asyncio.sleep(max(0.55, float(getattr(config, "TWIKIT_REQUEST_GAP_SEC", 1.35) or 0.55) * 0.4))

            await asyncio.sleep(max(2.0, float(getattr(config, "TWIKIT_REQUEST_GAP_SEC", 1.35) or 2.0) * 1.4))

        print(f"{pfx} [XSearch] Done. Routed {candidates_processed} candidate account(s) this cycle.")

    @x_project_first_search_task.before_loop
    async def before_x_project_first_search_task(self):
        await self.wait_until_ready()
        # Stagger keyword search vs brain scan so both don't hammer Twikit the same second.
        await asyncio.sleep(random.uniform(60.0, 150.0))

    async def process_discovery(self, account, hva_handle, interaction_type, channels):
        # VERBOSE: Log every account we check
        age = self.get_account_age_days(account.created_at)
        bio_preview = (account.description or "")[:40].replace("\n", " ")
        print(f"      📋 Checking @{account.screen_name} | Followers: {account.followers_count} | Age: {age}d | Bio: \"{bio_preview}...\"")
        
        # HVA Filter
        hvas = [h[0].lower() for h in database.get_all_hvas()]
        if account.screen_name.lower() in hvas: 
            print(f"         ❌ SKIP: Is already an HVA")
            return

        # Personal Profile Filter
        is_personal, reason = self.is_personal_profile(account)
        if is_personal:
            print(f"         ❌ SKIP Personal: {reason}")
            return

        is_new = database.is_project_new(account.id)
        is_new_follow = database.save_follow(account.id, hva_handle, interaction_type)
        if not is_new:
            database.update_project_followers_count(
                account.id, getattr(account, "followers_count", None)
            )

        if is_new:
            print(f"         ✅ NEW PROJECT! Will process...")
        else:
            if is_new_follow:
                print(f"         ✔ TRACKED: +1 Smart (Now: {database.get_posted_smarts(account.id)+1} HVAs)")
            else:
                print(f"         ⏸️ TRACKED: Already recorded this follow")
        age = self.get_account_age_days(account.created_at)
        
        # DISABLED: Sniper alerts now handled by age-based routing to prevent duplicates
        # if is_new and age < config.SNIPER_MAX_AGE_DAYS and age >= config.MAX_ACCOUNT_AGE_DAYS:
        #     print(f"      🎯 [SNIPER HIT!] @{account.screen_name} ({age}d)")
        #     sniper_ch = self.get_channel(config.SNIPER_CHANNEL_ID) or await self.fetch_channel(config.SNIPER_CHANNEL_ID)
        #     if sniper_ch:
        #         ...sniper alert logic removed to prevent channel duplication...

        if is_new:
            # We'll save it later after AI analysis
            print(f"      💾 Found NEW project @{account.screen_name} (Age: {age}d)")
        
        # --- AI ANALYSIS STEP ---
        ai_data = None
        if is_new and age < config.SNIPER_MAX_AGE_DAYS:
            print(f"      🤖 [AI] Analyzing @{account.screen_name}...")
            tweets = await self.twitter.get_user_timeline(account.id, count=10)
            ai_data = await self.ai.analyze_project(account, tweets)
            
            if ai_data:
                # Enhanced logging with all AI decision details
                is_proj = ai_data.get('is_project', True)
                category = ai_data.get('category', 'Unknown')
                confidence = ai_data.get('confidence', 0.0)
                alpha_score = ai_data.get('alpha_score', 0)
                reasoning = ai_data.get('reasoning', 'No reason provided')
                summary = ai_data.get('summary', 'No summary')
                
                # EXTRA VALIDATION: Check if AI reasoning contains consultant/personal patterns
                # Even if AI said "is_project: true", reject if reasoning shows personal account language
                consultant_patterns = [
                    "associated with", "working with", "helping",
                    "supporting", "advising", "growth strategies", "marketing",
                    "consultant", "multiple projects", "potential involvement"
                ]
                
                # Context-aware patterns (only reject if combined with other indicators)
                contextual_patterns = [
                    "involvement in"  # Only reject if near "multiple projects" or similar
                ]
                
                reasoning_lower = reasoning.lower()
                summary_lower = summary.lower()
                detected_pattern = None
                
                # Check hard patterns (always reject)
                for pattern in consultant_patterns:
                    if pattern in reasoning_lower or pattern in summary_lower:
                        detected_pattern = pattern
                        break
                
                # Check contextual patterns (reject only if suspicious context)
                if not detected_pattern:
                    for pattern in contextual_patterns:
                        if pattern in reasoning_lower or pattern in summary_lower:
                            # Only reject if it's about a PERSON's involvement in multiple projects
                            # NOT about a PROJECT's involvement in activities
                            suspicious_context = any([
                                "multiple projects" in reasoning_lower,
                                "multiple projects" in summary_lower,
                                "several projects" in reasoning_lower,
                                "various projects" in reasoning_lower,
                                "many projects" in reasoning_lower,
                                "different projects" in reasoning_lower
                            ])
                            
                            if suspicious_context:
                                detected_pattern = f"{pattern} (with multiple projects context)"
                                break
                
                if detected_pattern:
                    print(f"         ❌ AI VALIDATION OVERRIDE: Detected consultant pattern '{detected_pattern}'")
                    print(f"            ├─ AI said is_project={is_proj}, but reasoning suggests personal account")
                    print(f"            ├─ Reasoning: {reasoning}")
                    print(f"            └─ Decision: REJECT (Consultant/Marketer detected)")
                    return  # Override AI decision and reject
                
                if not is_proj:
                    print(f"         ❌ AI REJECT: {reasoning}")
                    print(f"            ├─ Category: {category} | Confidence: {confidence:.0%} | Brain Score: {alpha_score}/100")
                    print(f"            └─ Summary: {summary}")
                    return # Skip personal/noise accounts
                
                print(f"         ✨ AI PASS: {summary}")
                print(f"            ├─ Category: {category} | Confidence: {confidence:.0%} | Alpha Score: {alpha_score}/100")
                print(f"            └─ Decision: POST TO DISCORD (is_project=true)")
        elif not is_new:
            # For existing projects, retrieve saved AI data
            ai_data = database.get_project_ai_data(account.id)

        # Gate for silent saves - projects over 100d are saved but not alerted
        if is_new and age >= config.SNIPER_MAX_AGE_DAYS:
            database.save_project(
                account.id,
                account.screen_name,
                account.name,
                account.description or "",
                str(account.created_at),
                ai_summary=ai_data.get("summary") if ai_data else None,
                ai_category=ai_data.get("category") if ai_data else None,
                followers_count=getattr(account, "followers_count", None),
            )
            print(f"      ⏩ SILENT SAVE @{account.screen_name}: Project aged {age}d > {config.SNIPER_MAX_AGE_DAYS}d limit (Archive Only)")
            return

        interactions = database.get_project_follows(account.id)
        score, num_hvas = self.calculate_score(account, interactions)
        grade, emoji = self._get_grade(score)

        if is_new:
            self.current_scan_discoveries += 1
            print(f"{self._get_log_prefix()} 🌟 [NEW PROJECT FOUND!] @{hva_handle} found @{account.screen_name} | {grade} {emoji} | {age}d")
            
            ai_summary = ai_data.get('summary') if ai_data else None
            ai_category = ai_data.get('category') if ai_data else None
            ai_alpha_score = ai_data.get('alpha_score', 0) if ai_data else 0
            
            database.save_project(
                account.id,
                account.screen_name,
                account.name,
                account.description or "",
                str(account.created_at),
                ai_summary,
                ai_category,
                ai_alpha_score,
                followers_count=getattr(account, "followers_count", None),
            )
            
            database.init_signal_tracking(account.id)
            database.update_posted_smarts(account.id, num_hvas)

            # Age-based channel routing
            target_channels = []
            if age <= 30:
                target_channels = self.new_projects_channels
                age_label = "≤30d (NEW)"
            elif 30 < age <= 100:
                target_channels = self.established_projects_channels
                age_label = "30-100d (ESTABLISHED)"
            else:
                # Projects > 100 days are silently saved (handled earlier)
                pass
            
            if target_channels:
                print(f"      → Routing to {age_label} channels")
                sent_any = False
                for ch in target_channels:
                    try:
                        res = self.create_embed(account, hva_handle, interaction_type, ai_data=ai_data)
                        files = []
                        lf = self.brand_logo_file()
                        if lf:
                            files.append(lf)
                        
                        if isinstance(res, tuple):
                            files.append(res[1]) # The banner.jpg
                            emb0, files = self._with_brand_banner_fallback(res[0], files)
                            await ch.send(embed=emb0, files=files)
                        else:
                            emb0, files = self._with_brand_banner_fallback(res, files)
                            await ch.send(embed=emb0, files=files) if files else await ch.send(embed=emb0)
                        sent_any = True
                    except Exception as e:
                        print(f"      ❌ Failed to send Discord alert: {e}")
                if sent_any:
                    database.mark_alerted(account.id)  # only mark after successful Discord post
                    if feed_events is not None:
                        try:
                            pfp = (
                                getattr(account, "profile_image_url_https", None)
                                or getattr(account, "profile_image_url", None)
                                or ""
                            )
                            ban = (
                                getattr(account, "profile_banner_url", None)
                                or ""
                            )
                            # Best-effort: persist the actual Discord embed payload so the website can render
                            # the alert exactly like Discord (fields, links, thumbnail, etc.).
                            embed_payload = {}
                            try:
                                embed_payload = emb0.to_dict() if emb0 else {}
                            except Exception:
                                embed_payload = {}
                            feed_events.add_event(
                                kind="discovery",
                                guild_id=int(getattr(getattr(ch, "guild", None), "id", 0) or 0),
                                channel_id=int(getattr(ch, "id", 0) or 0),
                                title=f"@{account.screen_name} · {ai_category or 'Project'}",
                                body=(ai_summary or account.description or "")[:1500],
                                url=f"https://x.com/{account.screen_name}",
                                extra={
                                    "handle": account.screen_name,
                                    "age_days": int(age),
                                    "followers": int(getattr(account, "followers_count", 0) or 0),
                                    "hva": str(hva_handle or ""),
                                    "interaction": str(interaction_type or ""),
                                    "pfp_url": str(pfp or ""),
                                    "banner_url": str(ban or ""),
                                    "embed": embed_payload,
                                },
                            )
                        except Exception:
                            pass
        else:
            last = database.get_posted_smarts(account.id)
            if num_hvas > last:
                type_label = "NEW SMART" if is_new_follow else "MOMENTUM"
                status_suffix = ""
                if age >= config.SNIPER_MAX_AGE_DAYS:
                    status_suffix = " (Archive Only - No Alert)"
                
                print(f"{self._get_log_prefix()} 📈 [{type_label} DETECTED!] @{account.screen_name} -> {num_hvas} Smarts! 🚀{status_suffix}")
                database.update_posted_smarts(account.id, num_hvas)
                
                # GATE: Only send Discord alerts if project is < 100 days old
                if age < config.SNIPER_MAX_AGE_DAYS:
                    # Escalation requires: 1+ HVAs (catch early signals)
                    if num_hvas >= 1:
                        # Check for velocity (🔥 HOT PROJECT)
                        is_velocity = self.check_velocity(account.id)
                        if is_velocity:
                            print(f"{self._get_log_prefix()} 🔥 [VELOCITY ALERT!] @{account.screen_name} has {config.VELOCITY_THRESHOLD}+ HVAs in {config.VELOCITY_WINDOW_HOURS}h!")
                        
                        # Leaderboard-only channel (ESCALATION_DAILY_TOP_MOVERS_CHANNEL_ID) gets the daily
                        # smart-follow top list only — never live Medium/Strong signal embeds.
                        _lb_cid = config.ESCALATION_DAILY_TOP_MOVERS_CHANNEL_ID
                        targets = [
                            ch
                            for ch in (self.active_escalation_channels or [])
                            if not _lb_cid or ch.id != _lb_cid
                        ]
                        res = self.create_embed(account, hva_handle, interaction_type, is_escalation=True, is_velocity=is_velocity, ai_data=ai_data)
                        
                        sent_any = False
                        _first_sent_ch = None
                        _first_sent_emb0 = None
                        for ch in targets:
                            try:
                                res = self.create_embed(account, hva_handle, interaction_type, is_escalation=True, is_velocity=is_velocity, ai_data=ai_data)
                                files = []
                                lf = self.brand_logo_file()
                                if lf:
                                    files.append(lf)
                                
                                if isinstance(res, tuple):
                                    files.append(res[1]) # The banner.jpg
                                    emb0, files = self._with_brand_banner_fallback(res[0], files)
                                    await ch.send(embed=emb0, files=files)
                                else:
                                    emb0, files = self._with_brand_banner_fallback(res, files)
                                    await ch.send(embed=emb0, files=files) if files else await ch.send(embed=emb0)
                                sent_any = True
                                if _first_sent_ch is None:
                                    _first_sent_ch = ch
                                    _first_sent_emb0 = emb0
                            except Exception as e:
                                print(f"      ❌ Failed to send escalation alert: {e}")
                        
                        if sent_any:
                            if feed_events is not None:
                                try:
                                    _ch = _first_sent_ch or (targets[0] if targets else None)
                                    embed_payload = {}
                                    try:
                                        embed_payload = _first_sent_emb0.to_dict() if _first_sent_emb0 else {}
                                    except Exception:
                                        embed_payload = {}
                                    pfp = (
                                        getattr(account, "profile_image_url_https", None)
                                        or getattr(account, "profile_image_url", None)
                                        or ""
                                    )
                                    ban = getattr(account, "profile_banner_url", None) or ""
                                    feed_events.add_event(
                                        kind="escalation",
                                        guild_id=int(getattr(getattr(_ch, "guild", None), "id", 0) or 0),
                                        channel_id=int(getattr(_ch, "id", 0) or 0),
                                        title=f"@{account.screen_name} · Escalation",
                                        body=(
                                            (ai_data.get("summary") if isinstance(ai_data, dict) else "")
                                            or (account.description or "")
                                        )[:1500],
                                        url=f"https://x.com/{account.screen_name}",
                                        extra={
                                            "handle": account.screen_name,
                                            "age_days": int(age) if isinstance(age, (int, float)) else None,
                                            "followers": int(getattr(account, "followers_count", 0) or 0),
                                            "hva": str(hva_handle or ""),
                                            "interaction": str(interaction_type or ""),
                                            "pfp_url": str(pfp or ""),
                                            "banner_url": str(ban or ""),
                                            "embed": embed_payload,
                                        },
                                    )
                                except Exception:
                                    pass
                            database.mark_alerted(account.id) # Ensure it now shows in stats/trending
                            print(f"      ✅ ESCALATION SENT: Alerted to escalation channel (HVAs: {num_hvas})")
                    else:
                        # This should never happen (num_hvas >= 1 always true if we're here)
                        print(f"      ⚠️ LOGIC ERROR: num_hvas={num_hvas} (should be >= 1)")

    def calculate_score(self, account, interactions):
        """Calculate project score with HVA quality weighting and AI insights."""
        score, unique_hvas = 0, set([i[0].lower() for i in interactions])
        
        # Get AI Brain Score if available
        ai_data = database.get_project_ai_data(account.id)
        ai_alpha = ai_data.get('alpha_score', 0) if ai_data else 0
        
        # Base AI Boost (30% of total score potential)
        score += (ai_alpha * 0.3)
        
        # HVA Quality Weighting (Load dynamic weights from DB)
        conn = database.sqlite3.connect(database.DB_PATH)
        cursor = conn.cursor()
        
        t1 = [h.lower() for h in config.TIER_1_HVAs]
        
        for h in unique_hvas:
            # Check if this HVA has a high quality score in DB
            cursor.execute("SELECT quality_score FROM hva_stats WHERE hva_handle = ?", (h.lower(),))
            res = cursor.fetchone()
            hva_perf = res[0] if res else 0
            
            # Weighted addition
            weight = 1.0
            if h in t1: weight = 3.0
            
            # Performance multiplier (0.5x to 2.0x based on quality_score)
            perf_mult = max(0.5, min(2.0, hva_perf / 50.0)) if hva_perf > 0 else 1.0
            
            score += 15 * weight * perf_mult
        
        conn.close()
        
        # Engagement type bonuses
        for h, it in interactions:
            if it == 'retweet': score += 5
            elif it == 'reply': score += 3
        
        # Multi-HVA bonuses
        if len(unique_hvas) >= 3: score += 20
        elif len(unique_hvas) >= 2: score += 10
        
        return min(round(score), 100), len(unique_hvas)

    def get_signal_tier(self, num_hvas):
        """Get signal strength tier based on HVA count."""
        if num_hvas >= 4:
            return config.SIGNAL_TIERS.get(4, ("🔴 Strong Signal", 0xFF0000))
        elif num_hvas >= 2:
            return config.SIGNAL_TIERS.get(2, ("🟡 Medium Signal", 0xFFFF00))
        else:
            return config.SIGNAL_TIERS.get(1, ("🟢 Initial Signal", 0x00FF00))

    def check_velocity(self, project_id):
        """Check if project has velocity (3+ HVAs in 24h window)."""
        recent_follows = database.get_recent_follows(project_id, config.VELOCITY_WINDOW_HOURS)
        return len(recent_follows) >= config.VELOCITY_THRESHOLD

    def create_embed(self, account, hva_handle, interaction_type, is_escalation=False, is_velocity=False, ai_data=None):
        score, num_hvas = self.calculate_score(account, database.get_project_follows(account.id))
        score_bar_text, grade, grade_emoji, embed_color = self.create_score_bar(score)
        
        # Override category if AI provides it
        category = ai_data.get('category', self.classify_project(account)) if ai_data else self.classify_project(account)
        
        status_label = self.get_status_label(account.followers_count)
        is_verified = getattr(account, 'is_blue_verified', False) or getattr(account, 'verified', False)

        # Determine title and color based on alert type
        if is_velocity:
            signal_label = "🔥 VELOCITY ALERT"
            embed_color = 0xFF4500  # OrangeRed
            title = f"{signal_label}: {num_hvas}+ HVAs in {config.VELOCITY_WINDOW_HOURS}h!"
        elif is_escalation:
            signal_label, tier_color = self.get_signal_tier(num_hvas)
            embed_color = tier_color
            title = f"📢 {signal_label}: Momentum Level {num_hvas}"
        else:
            title = f"🔍 {BRAND_NAME} Discovery"
        
        embed = discord.Embed(title=title, color=embed_color)
        embed.set_thumbnail(url=account.profile_image_url.replace("_normal", "_400x400"))
        
        # Section 1: Profile
        profile_value = (
            f"**Name:** [{account.name}](https://x.com/{account.screen_name}) | **Category:** {category}\n"
            f"\n"
            f"**Age:** `{self.format_age(account.created_at)}` | **Followers:** `{account.followers_count:,}`\n"
            f"\u200b"
        )
        embed.add_field(name="👤 Profile", value=profile_value, inline=False)

        # Section 2: Social Score
        score_value = (
            f"{score_bar_text}\n"
            f"\n"
            f"**Grade:** {grade} {grade_emoji} | **Status:** {status_label} | **Verified:** {'✅' if is_verified else '❌'}\n"
            f"\u200b"
        )
        embed.add_field(name="📊 Social Score", value=score_value, inline=False)

        # Section 2.5: AI Summary (Professional Wide-Report Style)
        if ai_data:
            summary = ai_data.get('summary', 'No summary available.')
            score = ai_data.get('alpha_score', 'N/A')
            
            # Create a structured report inside the box (Category & Strength removed for simplicity)
            report_lines = [
                f"ANALYSIS: {summary}"
            ]
            
            formatted_lines = []
            for line in report_lines:
                # Wrap to 85 chars to fill the wide embed
                wrapped = textwrap.wrap(line, width=85)
                for i, w_line in enumerate(wrapped):
                    prefix = "+ " if i == 0 else "  "
                    formatted_lines.append(f"{prefix}{w_line}")
            
            report_text = "\n".join(formatted_lines)
            # Using ANSI styling for a more modern 'different' style
            embed.add_field(name=f"🧠 {BRAND_NAME} Research", value=f"```ansi\n\u001b[1;36m[ANALYSIS]\u001b[0m \u001b[0;37m{summary}\u001b[0m\n```", inline=False)

        # Section 3: hunter count
        seen_lower = set()
        count_smarts = 0
        for i in database.get_project_follows(account.id):
            h = i[0]
            if h.lower() not in seen_lower:
                seen_lower.add(h.lower())
                count_smarts += 1
        
        embed.add_field(name=f"{BRAND_NAME} engaged", value=f"🧠 {count_smarts} Smart Engager{'s' if count_smarts != 1 else ''}", inline=False)
        
        # banner_url is used for dynamic profile banners
        banner_url = getattr(account, 'profile_banner_url', None)
        attachment_file = None
        if banner_url:
            embed.set_image(url=banner_url)
        else:
            # Fallback to local brand banner
            if BRAND_BANNER_PATH and BRAND_BANNER_FILE:
                attachment_file = discord.File(BRAND_BANNER_PATH, filename=BRAND_BANNER_FILE)
                embed.set_image(url=f"attachment://{BRAND_BANNER_FILE}")

        # Footer
        foot = f"{BRAND_NAME} • Created by Sultan • {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        if BRAND_LOGO_FILE:
            embed.set_footer(text=foot, icon_url=f"attachment://{BRAND_LOGO_FILE}")
        else:
            embed.set_footer(text=foot)
        
        if attachment_file:
            # We must return both to ensure the file is attached in SEND
            return embed, attachment_file
        return embed

class BrainCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    async def _send_brand_embed(self, ctx, embed):
        files: List[discord.File] = []
        f = self.bot.brand_logo_file()
        if f:
            files.append(f)
        embed, files = self.bot._with_brand_banner_fallback(embed, files)
        if files:
            await ctx.send(embed=embed, files=files)
        else:
            await ctx.send(embed=embed)

    @commands.command(name="help")
    async def help_cmd(self, ctx):
        embed = self.bot._create_premium_embed(f"🧬 {BRAND_NAME} — Command Menu")
        cmds = {
            "`!velcor3 trending`": "Show trending projects (24h/7d).",
            "`!velcor3 stats`": "View global bot metrics.",
            "`!velcor3 hva_list`": "Display all monitored hunters.",
            "`!velcor3 add_hva @user`": "Add a new hunter to monitor.",
            "`!velcor3 remove_hva @user`": "Remove a hunter.",
            "`!velcor3 first @user`": "Analyze first 1000 followers.",
            "`!velcor3 ping`": "Test bot responsiveness.",
            "`!velcor3 post_verification`": "Post verification panel (Manage Server).",
            "`!velcor3 post_payment`": "Post crypto payment panel (Manage Server).",
            "`/verification_panel`": "Post verification UI (slash).",
            "`/crypto_payment_panel`": "Post crypto payment UI (slash).",
            "`/alerts`": "Multi-server: `activate` (license key) → `setup` (create channels) → feeds.",
            "`/owner_license`": "Bot owner: `issue` / `list` / `revoke` (per-guild keys).",
            "`!velcor3 daily_mints`": "Scrape daily-mints.com — AI scores, verdict, flags (add `today` or a number 1–10).",
            "`/daily_mints`": "Post daily-mints.com cards in the current channel (slash).",
            "**Token feed**": "Background: low / $100K+ / $1M+ channels — alerts on **new callers** or **meaningful MC/ATH moves**; banner + token icon when available.",
        }
        embed.add_field(name="🚀 Commands", value=self.bot._get_stats_field(cmds), inline=False)
        await self._send_brand_embed(ctx, embed)

    @commands.command(name="ping")
    async def ping_cmd(self, ctx):
        """Test bot responsiveness."""
        await ctx.send(f"🏓 **Pong!** Latency: {round(self.bot.latency * 1000)}ms")

    @commands.command(name="post_verification")
    @commands.has_permissions(manage_guild=True)
    async def post_verification_cmd(self, ctx):
        """Post the verification embed + buttons in this channel."""
        await post_verification_to_channel(ctx.channel)
        await ctx.send("✅ Verification panel posted.", delete_after=15)

    @commands.command(name="post_payment")
    @commands.has_permissions(manage_guild=True)
    async def post_payment_cmd(self, ctx):
        """Post the crypto payment embed + buttons in this channel."""
        await post_crypto_to_channel(ctx.channel)
        await ctx.send("✅ Crypto payment panel posted.", delete_after=15)

    @commands.command(name="add_hva")
    async def add_hva_cmd(self, ctx, handle: str):
        handle = handle.replace("@", "").strip()
        database.add_hva(handle)
        await ctx.send(f"✅ **@{handle}** added to tracking list!")

    @commands.command(name="remove_hva")
    async def remove_hva_cmd(self, ctx, handle: str):
        handle = handle.replace("@", "").strip()
        database.remove_hva(handle)
        await ctx.send(f"🗑️ **@{handle}** removed from tracking.")

    @commands.command(name="hva_list")
    async def hva_list_cmd(self, ctx):
        hvas = sorted([h[0] for h in database.get_all_hvas()])
        if not hvas: return await ctx.send("📋 Hunter list is empty.")
        embed = self.bot._create_premium_embed(f"📋 Full Monitored {BRAND_NAME} Fleet")
        for i in range(0, len(hvas), 50):
            chunk = hvas[i:i+50]
            embed.add_field(name=f"Hunters {i+1}-{i+len(chunk)}", value=", ".join([f"@{h}" for h in chunk]), inline=False)
        await self._send_brand_embed(ctx, embed)

    @commands.command(name="trending")
    async def trending_cmd(self, ctx, duration: str = "24h"):
        """Show trending projects. Usage: !velcor3 trending [24h/7d]"""
        hours = 168 if duration.lower() == "7d" else 24
        results = database.get_trending_projects(hours=hours, limit=10)
        
        if not results:
            return await ctx.send(f"❌ No trending data found for the last {duration}.")
            
        embed = self.bot._create_premium_embed(f"🔥 {BRAND_NAME} Momentum ({duration})", color=discord.Color.orange())
        lines = []
        for i, (p_id, handle, name, desc, created_at, s24h, s7d, total, ai_sum, ai_cat) in enumerate(results):
            age = self.bot.get_account_age_days(created_at)
            display_name = name or handle
            
            # Simple metrics: Total Smarts + Age
            metrics = f"💎 **{total} Smarts** | `{age}d`"
            cat_label = f"• {ai_cat}" if ai_cat else ""
            
            lines.append(f"**{i+1}. {display_name}** ([@{handle}](https://x.com/{handle}))\n   └ {metrics} {cat_label}")
            
        embed.add_field(name="Top 10 High-Velocity Projects", value="\n".join(lines), inline=False)
        await self._send_brand_embed(ctx, embed)

    @commands.command(name="hva_health", aliases=["health"])
    async def hva_health_cmd(self, ctx):
        """Show HVA performance and identify inactive hunters with detailed analysis."""
        import database
        
        embed = self.bot._create_premium_embed("💊 HVA Fleet Health Report", color=discord.Color.blue())
        
        # Get all HVAs with their stats
        all_hvas = database.get_all_hvas()
        
        # Categorize by performance
        top_performers = []  # 5+ discoveries
        active = []  # 1-4 discoveries
        inactive = []  # 0 discoveries
        
        for hva_handle, status, discovery_count in all_hvas:
            if discovery_count >= 5:
                top_performers.append((hva_handle, discovery_count))
            elif discovery_count >= 1:
                active.append((hva_handle, discovery_count))
            else:
                inactive.append(hva_handle)
        
        # Sort by performance
        top_performers.sort(key=lambda x: x[1], reverse=True)
        active.sort(key=lambda x: x[1], reverse=True)
        
        # Build performance sections
        if top_performers:
            top_lines = [f"⭐ **@{h}**: {c} projects" for h, c in top_performers[:10]]
            embed.add_field(name="🏆 Top Performers (5+ Projects)", value="\n".join(top_lines) or "None", inline=False)
        
        if active:
            active_lines = [f"✅ **@{h}**: {c} projects" for h, c in active[:15]]
            embed.add_field(name="🔹 Active Hunters (1-4 Projects)", value="\n".join(active_lines) or "None", inline=False)
        
        # --- ENHANCED INACTIVE ANALYSIS ---
        if inactive:
            analysis = database.get_inactive_hva_analysis()
            
            inactive_summary = f"""
**Total Inactive HVAs**: {analysis['total_inactive']} ({analysis['total_inactive']/analysis['total_hvas']*100:.1f}% of fleet)

**Breakdown**:
            """.strip()
            
            embed.add_field(name="⚠️ Inactive Analysis", value=inactive_summary, inline=False)
            
            # Never Scanned (Bot hasn't reached them yet)
            if analysis['never_scanned']:
                never_scanned_count = len(analysis['never_scanned'])
                sample = analysis['never_scanned'][:10]
                never_text = f"🔴 **{never_scanned_count} HVAs - Never Scanned Yet**\n"
                never_text += f"Bot is still working through the list.\n"
                never_text += f"Sample: {', '.join([f'@{h}' for h in sample])}"
                if never_scanned_count > 10:
                    never_text += f" (+{never_scanned_count - 10} more)"
                embed.add_field(name="👣 Never Scanned", value=never_text, inline=False)
            
            # Recently Scanned (Give them time)
            if analysis['recently_scanned']:
                recent_count = len(analysis['recently_scanned'])
                recent_text = f"🟡 **{recent_count} HVAs - Recently Scanned (<7 days)**\n"
                recent_text += "Still fresh - may find projects soon. Keep monitoring."
                embed.add_field(name="⏳ Recently Scanned", value=recent_text, inline=False)
            
            # Stale Scanned (Safe to remove - scanned 3+ times over weeks, found nothing)
            if analysis['stale_scanned']:
                stale_count = len(analysis['stale_scanned'])
                sample_stale = analysis['stale_scanned'][:15]
                stale_text = f"🔴 **{stale_count} HVAs - Scanned 3+ Times, No Results**\n"
                stale_text += f"❗ **Safe to Remove** - These hunters consistently find nothing.\n\n"
                
                stale_lines = []
                for h, last_scan, days_ago in sample_stale:
                    try:
                        last_dt = datetime.fromisoformat(last_scan)
                        days_display = f"{days_ago}d ago"
                    except:
                        days_display = "Unknown"
                    stale_lines.append(f"@{h} (Last: {days_display})")
                
                stale_text += ", ".join(stale_lines[:10])
                if stale_count > 10:
                    stale_text += f" (+{stale_count - 10} more)"
                
                embed.add_field(name="🗑️ Safe to Remove", value=stale_text, inline=False)
            
            # Other Scanned (Scanned but not enough data yet)
            if analysis['scanned_no_results']:
                other_count = len(analysis['scanned_no_results'])
                other_text = f"🟠 **{other_count} HVAs - Scanned 1-2 Times**\n"
                other_text += "Not enough data yet. Monitor for a few more scans."
                embed.add_field(name="🔍 Needs More Monitoring", value=other_text, inline=False)
        
        # Overall Summary
        summary = f"""
**Total HVAs**: {len(all_hvas)}
**Top Performers**: {len(top_performers)}
**Active**: {len(active)}
**Inactive**: {len(inactive)}

💡 **Tips**:
• Use `!velcor3 remove_hva @username` to clean up stale hunters.
• Inactive HVAs in "Never Scanned" will be checked eventually.
• Focus on removing HVAs scanned 3+ times with no results.
        """
        embed.add_field(name="📊 Summary", value=summary, inline=False)
        
        await self._send_brand_embed(ctx, embed)

    @commands.command(name="stats")
    async def stats_cmd(self, ctx):
        p, f, h, categories = database.get_db_stats()
        
        embed = self.bot._create_premium_embed(f"🤖 {BRAND_NAME} Fleet Metrics")
        
        # Core Stats
        core_stats = {
            "Total Projects": f"{p:,}",
            "Interactions": f"{f:,}",
            "Active Hunters": f"{h:,}"
        }
        embed.add_field(name="📊 System Overview", value=self.bot._get_stats_field(core_stats), inline=False)
        
        # AI Category Breakdown
        if categories:
            total_analyzed = sum(c[1] for c in categories)
            cat_lines = []
            for cat, count in categories:
                percentage = (count / total_analyzed) * 100
                # Map emojis to categories if possible
                emoji = ""
                for e, keywords in config.PROJECT_CATEGORIES.items():
                    if cat.lower() in "".join(keywords).lower() or cat.lower() in e.lower():
                        emoji = e.split()[0] + " "
                        break
                cat_lines.append(f"{emoji}**{cat}:** {count} ({percentage:.1f}%)")
            
            embed.add_field(name="🧠 AI Brain Distribution", value="\n".join(cat_lines), inline=False)
        
        await self._send_brand_embed(ctx, embed)

    @commands.command(name="first")
    async def first_followers_cmd(self, ctx, handle: str):
        handle = handle.replace("@", "")
        await ctx.send(f"🔍 Analyzing first followers for @{handle}...")
        try:
            uid = await self.bot.twitter.get_user_id(handle)
            fols, _ = await self.bot.twitter.get_first_followers(uid, limit=100)
            if not fols: return await ctx.send("❌ No followers found.")
            res = "\n".join([f"{i+1}. @{u.screen_name}" for i, u in enumerate(reversed(fols[:20]))])
            await self._send_brand_embed(ctx, self.bot._create_premium_embed(f"🌱 Early Followers: @{handle}", description=res))
        except Exception as e: await ctx.send(f"❌ Error: {e}")

    @commands.command(name="daily_mints")
    async def daily_mints_cmd(self, ctx, *, args: str = ""):
        """Post mints from daily-mints.com. Usage: `!velcor3 daily_mints` | `today` | `5` | `today 5`"""
        tokens = [t.strip() for t in args.split() if t.strip()]
        # Default to "today" — pass "all" explicitly to get full listing
        scope = "all" if any(t.lower() == "all" for t in tokens) else "today"
        limit = 5
        for t in tokens:
            if t.isdigit():
                limit = max(1, min(25, int(t)))
                break
        await ctx.send("⏳ Fetching daily-mints.com …", delete_after=30)
        # Post in this channel only (not DISCORD_MINTS_CHANNEL_ID — that is for /active_mints)
        target = ctx.channel
        n, err = await _run_daily_mints_post(self.bot, target, scope, limit, enrich_x_art=True)
        if err:
            await ctx.send(f"❌ {err}")
            return
        await ctx.send(
            f"✅ Posted **{n}** embed(s) to {target.mention} (scope: **{scope}**).",
            delete_after=60,
        )


class WalletCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="pnl", description="NFT PNL from Moralis indexed trades (requires MORALIS_API_KEY)")
    @app_commands.describe(
        wallet="The wallet address (0x...)",
        chain="Chain",
        moralis_days="Only trades in the last N days (saves CU). Leave empty for .env / all time.",
    )
    @app_commands.choices(
        chain=[
            app_commands.Choice(name="Ethereum", value="eth"),
            app_commands.Choice(name="Polygon", value="polygon"),
            app_commands.Choice(name="Base", value="base"),
            app_commands.Choice(name="Arbitrum", value="arbitrum"),
            app_commands.Choice(name="Optimism", value="optimism"),
        ]
    )
    async def pnl(
        self,
        interaction: Interaction,
        wallet: str,
        chain: str = "eth",
        moralis_days: Optional[int] = None,
    ):
        await interaction.response.defer(thinking=True)
        try:
            md = moralis_days
            if md is not None:
                md = max(0, min(3650, int(md)))
            data = await get_wallet_pnl(wallet, chain, moralis_days=md)
            embed = format_pnl_embed(data)
            embed, files = self.bot._with_brand_banner_fallback(embed, [])
            await interaction.followup.send(embed=embed, files=files) if files else await interaction.followup.send(embed=embed)
        except Exception as e:
            await interaction.followup.send(f"❌ Error fetching PNL: {e}", ephemeral=True)

    @app_commands.command(name="active_mints", description="Fetch current most active NFT mints from Reservoir")
    @app_commands.describe(limit="Number of mints to fetch (1-10)")
    async def active_mints(self, interaction: Interaction, limit: int = 5):
        await interaction.response.defer(thinking=True)
        try:
            from trackers.mint_sources import fetch_active_mints
            from trackers.active_mints_tracker import build_active_mint_embed
            
            # Determine target channel (use configured mints channel or current interaction channel)
            mints_id = config.DISCORD_MINTS_CHANNEL_ID
            target = interaction.channel
            if mints_id:
                try:
                    target_id = str(mints_id).split(',')[0].strip()
                    target = self.bot.get_channel(int(target_id)) or interaction.channel
                except: pass

            async with aiohttp.ClientSession() as session:
                mints = await fetch_active_mints(session, limit=limit, include_eth=True, include_solana=False)
            
            if not mints:
                await interaction.followup.send("No active mints found right now.", ephemeral=True)
                return

            sent = 0
            for m in mints[:limit]:
                embed = await build_active_mint_embed(m)
                embed, files = self.bot._with_brand_banner_fallback(embed, [])
                await target.send(embed=embed, files=files) if files else await target.send(embed=embed)
                sent += 1
                await asyncio.sleep(0.5)
            
            await interaction.followup.send(f"✅ Posted **{sent}** active mint(s) to {target.mention}.")
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)

    @app_commands.command(
        name="daily_mints",
        description="Post AI-scored upcoming NFT mints from daily-mints.com (HTML source)",
    )
    @app_commands.describe(
        scope="Filter mints (default: today only)",
        limit="How many mints to post (1–25)",
    )
    @app_commands.choices(
        scope=[
            app_commands.Choice(name="All upcoming (calendar)", value="all"),
            app_commands.Choice(name="Today", value="today"),
        ]
    )
    async def daily_mints_slash(
        self,
        interaction: Interaction,
        scope: str = "today",
        limit: int = 5,
    ):
        await interaction.response.defer(thinking=True)
        try:
            limit = max(1, min(25, int(limit)))
            # Current channel only — not the active-mints / Reservoir channel
            target = interaction.channel
            n, err = await _run_daily_mints_post(self.bot, target, scope, limit, enrich_x_art=True)
            if err:
                await interaction.followup.send(f"❌ {err}", ephemeral=True)
                return
            await interaction.followup.send(
                f"✅ Posted **{n}** mint card(s) to {target.mention}.",
                ephemeral=True,
            )
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)

    @app_commands.command(name="mints_status", description="Show active mints feature status and last API error")
    async def mints_status(self, interaction: Interaction):
        from trackers.mint_sources import get_last_fetch_error
        mints_id = config.DISCORD_MINTS_CHANNEL_ID
        
        embed = Embed(title="Active Mints – Status", color=Color.blue())
        embed.add_field(
            name="Channel",
            value=f"<#{mints_id}>" if mints_id else "Not set",
            inline=False,
        )
        embed.add_field(
            name="Loop",
            value="Running (ETH live mints)" if mints_id else "Disabled (no channel)",
            inline=False,
        )
        err = get_last_fetch_error()
        embed.add_field(
            name="Last API result",
            value=f"Error: `{err[:100]}`" if err else "OK (no error yet or mints were returned)",
            inline=False,
        )
        embed.set_footer(text="Use /active_mints to fetch once. Data from in-memory ETH feed & Waypoint.")
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="track_eth", description="Add an Ethereum wallet to the real-time tracker")
    @app_commands.describe(address="The 0x... wallet address", label="A nickname for this whale")
    async def track_eth(self, interaction: Interaction, address: str, label: str = "Manual"):
        address = address.strip().lower()
        if not address.startswith("0x") or len(address) != 42:
            return await interaction.response.send_message("❌ Invalid ETH address format.", ephemeral=True)
        
        if wallet_database.add_wallet_db(address, "ETH", label):
            tracked_eth_wallets[address] = label
            await interaction.response.send_message(f"✅ Now tracking ETH Whale: `{address}` ({label})")
        else:
            await interaction.response.send_message(f"⚠️ Wallet `{address}` is already being tracked!")

    @app_commands.command(name="list_wallets", description="List all currently tracked whale wallets")
    async def list_wallets(self, interaction: Interaction):
        wallets = wallet_database.get_all_wallets_db()
        if not wallets:
            return await interaction.response.send_message("📋 No wallets are currently being tracked.")
        
        embed = Embed(title=f"📋 {BRAND_NAME} — Tracked Wallets", color=Color.blue())
        eth_list = []
        for addr, chain, label, x_url in wallets:
            if chain != "ETH":
                continue
            line = f"`{addr[:6]}...{addr[-4:]}` - **{label}**"
            if x_url:
                line += f" · [X]({x_url})"
            eth_list.append(line)

        if eth_list:
            embed.add_field(name="🔹 Ethereum", value="\n".join(eth_list), inline=False)
        else:
            embed.description = "_No Ethereum wallets tracked._"
        
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="remove_wallet", description="Stop tracking a specific wallet")
    @app_commands.describe(address="The wallet address to remove")
    async def remove_wallet(self, interaction: Interaction, address: str):
        address = address.strip().lower()
        if wallet_database.remove_wallet_db(address):
            tracked_eth_wallets.pop(address, None)
            await interaction.response.send_message(f"🗑️ Removed `{address}` from tracking.")
        else:
            await interaction.response.send_message(f"❌ Wallet `{address}` not found in database.")

if __name__ == "__main__":
    BlockBrainBot().run(config.DISCORD_TOKEN)
