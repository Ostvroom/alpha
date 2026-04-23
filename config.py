import os
from dotenv import load_dotenv

# Ensure variables in .env override any system environment variables (to bypass corrupted tokens in memory)
load_dotenv(override=True)

# Discord Configuration
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DISCORD_GUILD_ID = os.getenv("DISCORD_GUILD_ID")  # Optional: For guild-specific commands

# Multi-tenant alert licenses (see guild_license.py / slash commands alerts + owner_license)
def parse_user_ids(env_var: str):
    val = os.getenv(env_var, "")
    if not val:
        return []
    out = []
    for x in val.split(","):
        s = x.strip()
        if s.isdigit():
            try:
                out.append(int(s))
            except ValueError:
                pass
    return out


BOT_OWNER_IDS = parse_user_ids("BOT_OWNER_IDS")
LICENSE_KEY_PEPPER = (os.getenv("LICENSE_KEY_PEPPER") or "").strip()

def parse_channel_ids(env_var):
    val = os.getenv(env_var, "")
    if not val:
        return []
    # Handle single ID or comma-separated list
    return [int(id.strip()) for id in val.split(",") if id.strip().isdigit()]


def parse_channel_id(env_var: str, default: int = 0) -> int:
    """Single Discord channel snowflake from env (first ID if comma-separated)."""
    val = (os.getenv(env_var) or "").strip()
    if not val:
        return default
    first = val.split(",")[0].strip()
    if not first.isdigit():
        return default
    try:
        return int(first)
    except ValueError:
        return default

# --- CHANNEL CONFIGURATION (Dynamic from .env) ---
DISCORD_CHANNEL_IDS = parse_channel_ids("DISCORD_CHANNEL_ID")
TRENDING_CHANNEL_IDS = parse_channel_ids("TRENDING_CHANNEL_ID")
ESCALATION_CHANNEL_IDS = parse_channel_ids("ESCALATION_CHANNEL_ID")

# Age-Based Discovery Routing (from .env)
# NEW_ACCS_CHANNEL_ID -> ≤30 days
# OLDER_ACCS_CHANNEL_ID -> 30-100 days
NEW_PROJECTS_CHANNEL_IDS = parse_channel_ids("NEW_ACCS_CHANNEL_ID")
ESTABLISHED_PROJECTS_CHANNEL_IDS = parse_channel_ids("OLDER_ACCS_CHANNEL_ID")

# Fallbacks if specific age-based channels aren't set
if not NEW_PROJECTS_CHANNEL_IDS:
    NEW_PROJECTS_CHANNEL_IDS = DISCORD_CHANNEL_IDS
if not ESTABLISHED_PROJECTS_CHANNEL_IDS:
    ESTABLISHED_PROJECTS_CHANNEL_IDS = DISCORD_CHANNEL_IDS

# Dedicated Reporting Channels
TRENDING_REPORT_CHANNELS = TRENDING_CHANNEL_IDS

# Large embed banner on the periodic trending report (default off — wide banners often read as a black bar on dark Discord).
# Set TRENDING_REPORT_SHOW_BANNER=1 when you ship a proper banner asset.
TRENDING_REPORT_SHOW_BANNER = os.getenv("TRENDING_REPORT_SHOW_BANNER", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)

# Sniper Filter Channel (Usually established projects or a dedicated feed)
SNIPER_CHANNEL_ID = parse_channel_ids("OLDER_ACCS_CHANNEL_ID")[0] if parse_channel_ids("OLDER_ACCS_CHANNEL_ID") else (DISCORD_CHANNEL_IDS[0] if DISCORD_CHANNEL_IDS else 0)
SNIPER_MAX_AGE_DAYS = 100
SNIPER_MAX_TWEETS = 4

# Consolidated list used by bot main loops
MAIN_CHANNELS = list(set(DISCORD_CHANNEL_IDS + TRENDING_CHANNEL_IDS + ESCALATION_CHANNEL_IDS + NEW_PROJECTS_CHANNEL_IDS + ESTABLISHED_PROJECTS_CHANNEL_IDS))

# For backward compatibility
DISCORD_CHANNEL_ID = DISCORD_CHANNEL_IDS[0] if DISCORD_CHANNEL_IDS else 0

# New-Accs-Signal Settings
NEW_ACCS_MAX_AGE_DAYS = 7  # Ultra-fresh accounts only

# Project Classification Keywords
PROJECT_CATEGORIES = {
    "🐸 Meme": ["meme", "pepe", "doge", "shiba", "wojak", "frog", "pump", "moon", "ape", "cat", "dog", "inu", "elon", "based"],
    "💰 DeFi": ["defi", "yield", "swap", "liquidity", "lending", "dao", "vault", "staking", "farm", "apy", "tvl", "dex", "amm"],
    "🖼️ NFT": ["nft", "pfp", "collection", "mint", "art", "jpeg", "ordinals", "rune", "inscription", "generative"],
    "🎮 Gaming": ["game", "play", "metaverse", "p2e", "guild", "esport", "virtual", "world"],
    "⛓️ Infra": ["chain", "layer", "protocol", "bridge", "rollup", "zk", "node", "validator", "rpc", "sdk", "api"],
    "🤖 AI": ["ai", "agent", "neural", "gpu", "inference", "learning", "compute"]
}

# Bio Keyword Weights (Boost score for high-quality founders/VCs)
BIO_WEIGHTS = {
    "Founder": 15,
    "CEO": 10,
    "VC": 15,
    "Partner": 10,
    "Seed": 10,
    "Stealth": 15,
    "Built by": 15,
    "Ex-": 5,
    "Investor": 10
}

# Twitter API Configuration
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET")

# Twitter Account Credentials (for Scraping)
TWITTER_USERNAME = os.getenv("TWITTER_USERNAME")
TWITTER_EMAIL = os.getenv("TWITTER_EMAIL")
TWITTER_PASSWORD = os.getenv("TWITTER_PASSWORD")

# Monitoring Settings — HVA "brain scan" loop (Twikit + cookies.json).
# Override on Render for testing, e.g. BRAIN_SCAN_INTERVAL_SECONDS=900 (15m). Clamped 300–86400.
try:
    _brain_scan_sec = int((os.getenv("BRAIN_SCAN_INTERVAL_SECONDS") or "14400").strip() or "14400")
except ValueError:
    _brain_scan_sec = 14400
CHECK_INTERVAL_SECONDS = max(300, min(86400, _brain_scan_sec))
MAX_ACCOUNT_AGE_DAYS = 30      # Consider "newly created" if < 30 days old

# Off-Peak Scanning (Unified to 2 hours as requested)
OFF_PEAK_HOURS = (1, 7)        # 1 AM - 7 AM local time
OFF_PEAK_INTERVAL = 14400      # 4 hours
PEAK_INTERVAL = 14400           # 4 hours

# Daily X Trending Report
ENABLE_X_POST_DAILY = False
X_POST_TIME_UTC = "18:00"      # 6 PM UTC

# Daily X recap (twikit primary session): Solana top movers + Velcor3 finds (24h)
ENABLE_X_DAILY_RECAP = os.getenv("ENABLE_X_DAILY_RECAP", "0").strip().lower() in ("1", "true", "yes")
X_DAILY_RECAP_TIME_UTC = os.getenv("X_DAILY_RECAP_TIME_UTC", "12:00").strip() or "12:00"

def _env_flag(name: str, default: str = "1") -> bool:
    return os.getenv(name, default).strip().lower() not in ("0", "false", "no", "off")

X_DAILY_RECAP_POST_TOKENS = _env_flag("X_DAILY_RECAP_POST_TOKENS", "1")
X_DAILY_RECAP_POST_FINDS = _env_flag("X_DAILY_RECAP_POST_FINDS", "1")
try:
    X_DAILY_RECAP_TOP_N = max(1, min(5, int(os.getenv("X_DAILY_RECAP_TOP_N", "3") or "3")))
except ValueError:
    X_DAILY_RECAP_TOP_N = 3

# Parallel Processing
MAX_CONCURRENT_REQUESTS = 3    # Max concurrent API requests

# HVA Tiers for Credibility
TIER_1_HVAs = [
    "a16z", "paradigm", "balajis", "vitalikbuterin", "naval",
    "frankdegods", "punk9059", "pranksy", "garyvee", "farokh", "punk6529"
]

# HVA Quality Weighting (for Signal Score calculation)
HVA_TIER_WEIGHTS = {
    "tier1": 3,  # VCs, big founders (a16z, paradigm, etc.)
    "tier2": 2,  # Verified Velcor3 hunters
    "tier3": 1   # Regular hunters
}

# Velocity Alert Settings
VELOCITY_THRESHOLD = 3      # Number of HVAs within window to trigger
VELOCITY_WINDOW_HOURS = 24  # Time window for velocity detection

# Signal Strength Tiers (for Escalation embeds)
SIGNAL_TIERS = {
    1: ("🟢 Initial Signal", 0x00FF00),   # Green
    2: ("🟡 Medium Signal", 0xFFFF00),    # Yellow
    3: ("🟡 Medium Signal", 0xFFFF00),    # Yellow
    4: ("🔴 Strong Signal", 0xFF0000),    # Red
}

LOW_CREDIBILITY_HVAs = [
]

# List of High Value Accounts to monitor (Twitter User IDs or Handles)
HVA_LIST = [
    "0itsali0", "0xHarassment", "0xRohitz", "0xRyderr", "0xvietnguyen",
    "AbrarTheCrypto_", "AvalancheXBT", "Cady_btc", "CryptoUsopp", "Degensultan",
    "DinamuWeb3", "Guga_2787", "I_am_patrimonio", "IbraTheDabra", "MannersST",
    "Mohitt_NFT", "RiceRiddler", "SOLBigBrain", "TheDream", "Trappwurld",
    "WhyCaptainY", "alphaeye_", "alphafries", "ashrobin", "bearzverse",
    "bribe", "bricexeth", "brommmyy", "garyvee", "huseyin1tekin",
    "hustlepedia", "iammattiex", "mango_", "mooneyy", "naval",
    "nayverfayver", "noothe43", "notab2d", "nunooeu", "roulacase1",
    "ruledout_", "ruthybuilds", "superteam", "what3verman", "yakuzadaddy"

    # Added HVAs
    , "Amordeev", "xmartsol", "tenacious_ar", "seeyaarar", "0xJeeya",
    "Labosssxxf", "living_stone696", "XBukkyExplorer", "ChessDaoo", "stake_mm",
    "StormFrens", "FerreWeb3", "sigmadaoo", "weingfo", "jayjaynft4", "GuarEmperor"
]

# Twitter Client Settings
TWITTER_PROXY = os.getenv("TWITTER_PROXY") # Single proxy fallback

# OpenAI / xAI (Grok) Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
XAI_API_KEY = os.getenv("XAI_API_KEY")
AI_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini") # Default to gpt-4o-mini

# --- Wallet Tracker Configuration ---
SOLSCAN_API_KEY = os.getenv("SOLSCAN_API_KEY")
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL")
ETHEREUM_RPC_URL = os.getenv("ETHEREUM_RPC_URL")
# Alchemy NFT API key (used only for wallet NFT tracking enrichment)
ALCHEMY_NFT_API_KEY = os.getenv("ALCHEMY_NFT_API_KEY")
# Etherscan v2 requires a valid key for account/token tx endpoints (no silent free tier).
ETHSCAN_API_KEY = (os.getenv("ETHSCAN_API_KEY") or os.getenv("ETHERSCAN_API_KEY") or "").strip()
MORALIS_API_KEY = os.getenv("MORALIS_API_KEY")
RESERVOIR_API_KEY = os.getenv("RESERVOIR_API_KEY")
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")

DISCORD_TOKEN_CHANNEL_ID = parse_channel_id("DISCORD_TOKEN_CHANNEL_ID")
DISCORD_NFT_CHANNEL_ID = parse_channel_id("DISCORD_NFT_CHANNEL_ID", default=1486393083179569152)
# Solana NFT wallet + live mint trackers removed — leave unset (0)
DISCORD_SOL_NFT_CHANNEL_ID = parse_channel_id("DISCORD_SOL_NFT_CHANNEL_ID", default=0)
DISCORD_MINTS_CHANNEL_ID = parse_channel_id("DISCORD_MINTS_CHANNEL_ID")
DISCORD_NEW_MINTS_CHANNEL_ID = parse_channel_id("DISCORD_NEW_MINTS_CHANNEL_ID")
ENABLE_ETH_ERC20 = os.getenv("ENABLE_ETH_ERC20", "0") == "1"
ENABLE_ETH_NFT = os.getenv("ENABLE_ETH_NFT", "1") == "1"

# --- Verification & crypto payment panels (/verification_panel, /crypto_payment_panel) ---
def _env_int(name: str, default: int = 0) -> int:
    v = os.getenv(name, "").strip()
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float((os.getenv(name) or str(default)).strip())
    except ValueError:
        return default


# HVA brain-scan batching — lower / slower = gentler on X + residential proxies (optional .env)
BATCH_SIZE = max(1, min(25, _env_int("HVA_BATCH_SIZE", 10)))
BATCH_BREAK_SECONDS = max(30, min(900, _env_int("HVA_BATCH_BREAK_SECONDS", 120)))

# Twikit (cookie / web-style X traffic) — reduce 429 bursts and “all sessions blocked” cooldowns
# Minimum pause before each Twikit call in the hot paths below (seconds).
TWIKIT_REQUEST_GAP_SEC = max(0.0, min(20.0, _env_float("TWIKIT_REQUEST_GAP_SEC", 1.35)))
# On HTTP 429, rotate to another cookie session this many times before hard-blocking the current session.
TWIKIT_429_SOFT_PER_SESSION = max(1, min(30, _env_int("TWIKIT_429_SOFT_PER_SESSION", 8)))
# When every session is hard-blocked, pause the whole Twikit pool (minutes) before retrying.
TWIKIT_ALL_SESSIONS_COOLDOWN_MIN = max(5, min(180, _env_int("TWIKIT_ALL_SESSIONS_COOLDOWN_MIN", 45)))

# X project-first discovery (keyword search → new accounts → discovery pipeline)
ENABLE_X_PROJECT_SEARCH = _env_flag("ENABLE_X_PROJECT_SEARCH", "1")  # default ON
X_PROJECT_SEARCH_POLL_MINUTES = max(10, min(120, _env_int("X_PROJECT_SEARCH_POLL_MINUTES", 25)))
X_PROJECT_SEARCH_KEYWORDS_LIMIT = max(5, min(200, _env_int("X_PROJECT_SEARCH_KEYWORDS_LIMIT", 50)))
# Safety caps per cycle (prevents spam + rate-limit issues)
X_PROJECT_SEARCH_MAX_TWEETS_PER_KEYWORD = max(3, min(30, _env_int("X_PROJECT_SEARCH_MAX_TWEETS_PER_KEYWORD", 8)))
X_PROJECT_SEARCH_MAX_CANDIDATES_PER_CYCLE = max(5, min(80, _env_int("X_PROJECT_SEARCH_MAX_CANDIDATES_PER_CYCLE", 25)))


def _parse_time_hhmm(val: str, default: str = "00:00"):
    from datetime import datetime

    try:
        return datetime.strptime((val or default).strip(), "%H:%M").time()
    except ValueError:
        return datetime.strptime(default, "%H:%M").time()


# Daily Mints auto-feed (daily-mints.com HTML scrape → Discord; optional X PFP/banner)
DAILY_MINTS_AUTO_CHANNEL_ID = parse_channel_id(
    "DAILY_MINTS_AUTO_CHANNEL_ID", default=1490771474804375652
)
ENABLE_DAILY_MINTS_AUTO = os.getenv("ENABLE_DAILY_MINTS_AUTO", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
# "today" = only the Minting Today section on /mints (recommended for daily alerts)
DAILY_MINTS_AUTO_SCOPE = (os.getenv("DAILY_MINTS_AUTO_SCOPE", "today") or "today").strip().lower()
if DAILY_MINTS_AUTO_SCOPE not in ("all", "today"):
    DAILY_MINTS_AUTO_SCOPE = "today"
DAILY_MINTS_AUTO_LIMIT = max(1, min(25, _env_int("DAILY_MINTS_AUTO_LIMIT", 10)))
# discord.ext.tasks daily `time=` is interpreted in UTC
DAILY_MINTS_AUTO_TIME = _parse_time_hhmm(os.getenv("DAILY_MINTS_AUTO_TIME_UTC", "00:00"))

# Solana token overview API (KOLFI_API_KEY in .env — x-api-key header)
KOLFI_API_KEY = (os.getenv("KOLFI_API_KEY") or "").strip()
ENABLE_KOLFI_FEED = os.getenv("ENABLE_KOLFI_FEED", "1").strip().lower() in ("1", "true", "yes", "on")
KOLFI_POLL_MINUTES = max(2, min(120, _env_int("KOLFI_POLL_MINUTES", 10)))
KOLFI_CHANNEL_LOW_ID = parse_channel_id("KOLFI_CHANNEL_LOW_ID", default=1490737082698825948)
KOLFI_CHANNEL_100K_ID = parse_channel_id("KOLFI_CHANNEL_100K_ID", default=1490738906818154607)
KOLFI_CHANNEL_1M_ID = parse_channel_id("KOLFI_CHANNEL_1M_ID", default=1490738930826346607)
# One Discord message per new token; delay avoids channel rate limits on large batches
KOLFI_SEND_DELAY_SEC = max(0.0, min(10.0, _env_float("KOLFI_SEND_DELAY_SEC", 1.25)))
# 0 = no cap (may hit Discord limits if hundreds of new mints appear at once)
KOLFI_MAX_ALERTS_PER_BUCKET = max(0, _env_int("KOLFI_MAX_ALERTS_PER_BUCKET", 0))
# Alerts only when new caller rows appear or MC/ATH moves meaningfully (not “new coin on board”)
KOLFI_MC_MOVE_ALERT_PCT = max(5.0, min(80.0, _env_float("KOLFI_MC_MOVE_ALERT_PCT", 15.0)))
KOLFI_ATH_BREAK_PCT = max(0.5, min(50.0, _env_float("KOLFI_ATH_BREAK_PCT", 2.0)))
KOLFI_USE_BRAND_BANNER = os.getenv("KOLFI_USE_BRAND_BANNER", "1").strip().lower() in ("1", "true", "yes", "on")
# Compact token alerts: one snapshot line, merged links, earliest KOL only, no AI block (set 0 for legacy verbose embed)
KOLFI_SIMPLE_ALERT_EMBED = os.getenv("KOLFI_SIMPLE_ALERT_EMBED", "1").strip().lower() in ("1", "true", "yes", "on")
# Short AI “Quick read” on token alerts (uses OPENAI_API_KEY + AI_MODEL)
KOLFI_AI_REVIEW = os.getenv("KOLFI_AI_REVIEW", "1").strip().lower() in ("1", "true", "yes", "on")
# Optional model override for token alerts only (empty = use AI_MODEL)
KOLFI_AI_MODEL = (os.getenv("KOLFI_AI_MODEL") or "").strip()
# standard = short blurbs | deep = structured brief (trigger / tape / callers / risk)
_kolfi_depth = (os.getenv("KOLFI_AI_REVIEW_DEPTH", "deep") or "deep").strip().lower()
KOLFI_AI_REVIEW_DEPTH = _kolfi_depth if _kolfi_depth in ("standard", "deep") else "deep"
KOLFI_AI_MAX_TOKENS = max(200, min(1200, _env_int("KOLFI_AI_MAX_TOKENS", 550)))
# Public pair APIs + optional holder API for liquidity / holders / pair age — grounds AI briefs
KOLFI_MARKET_ENRICH = os.getenv("KOLFI_MARKET_ENRICH", "1").strip().lower() in ("1", "true", "yes", "on")
BIRDEYE_API_KEY = (os.getenv("BIRDEYE_API_KEY") or "").strip()
# deep briefs: step1 JSON analysis → step2 Discord lines (set 0 for single LLM + facts JSON)
KOLFI_AI_TWO_STEP = os.getenv("KOLFI_AI_TWO_STEP", "1").strip().lower() in ("1", "true", "yes", "on")
KOLFI_AI_MODEL_STEP1 = (os.getenv("KOLFI_AI_MODEL_STEP1") or "").strip()

# Call leaderboard (ATH by first KOL call in window) — optional; off by default (use daily “our alerts” recap instead).
ENABLE_KOLFI_LEADERBOARD = os.getenv("ENABLE_KOLFI_LEADERBOARD", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
KOLFI_LEADERBOARD_CHANNEL_ID = parse_channel_id(
    "KOLFI_LEADERBOARD_CHANNEL_ID", default=1490837149795483728
)
# discord.ext.tasks daily `time=` is interpreted in UTC
KOLFI_LEADERBOARD_TIME_UTC = _parse_time_hhmm(
    os.getenv("KOLFI_LEADERBOARD_TIME_UTC", "12:00")
)
# Leaderboard rows (hard cap 10)
KOLFI_LEADERBOARD_TOP_N = max(1, min(10, _env_int("KOLFI_LEADERBOARD_TOP_N", 10)))
# ATH leaderboard: **first** KOL call (with MC) must fall within this rolling window (default 30 days).
# Override with KOLFI_LEADERBOARD_MAX_CALL_AGE_HOURS for exact hours (e.g. 720 = 30d).
KOLFI_LEADERBOARD_MAX_CALL_AGE_DAYS = max(1, min(90, _env_int("KOLFI_LEADERBOARD_MAX_CALL_AGE_DAYS", 30)))
_env_lb_h = os.getenv("KOLFI_LEADERBOARD_MAX_CALL_AGE_HOURS")
if _env_lb_h is not None and str(_env_lb_h).strip() != "":
    KOLFI_LEADERBOARD_MAX_CALL_AGE_HOURS = max(0.0, float(str(_env_lb_h).strip()))
else:
    KOLFI_LEADERBOARD_MAX_CALL_AGE_HOURS = float(KOLFI_LEADERBOARD_MAX_CALL_AGE_DAYS * 24)

# Daily “top movers” (24h %%): require at least one KOL call within this window — **separate** from ATH leaderboard.
KOLFI_TOP_MOVERS_KOL_CALL_MAX_HOURS = max(0.0, _env_float("KOLFI_TOP_MOVERS_KOL_CALL_MAX_HOURS", 24.0))

# “Our alerts” daily recap (KOLFI_DAILY_TOP_MOVERS_CHANNEL_ID): KOL first-call messageTs must be within this many hours.
# Default 24 so the recap matches “fresh calls” only; set 0 to disable (shows older KOL first-call ages).
KOLFI_ALERT_RECAP_FIRST_CALL_MAX_HOURS = max(0.0, _env_float("KOLFI_ALERT_RECAP_FIRST_CALL_MAX_HOURS", 24.0))

# Daily top movers (24h) — ranked by 24h % change for the best-liquidity pair
ENABLE_KOLFI_DAILY_TOP_MOVERS = os.getenv("ENABLE_KOLFI_DAILY_TOP_MOVERS", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
KOLFI_DAILY_TOP_MOVERS_CHANNEL_ID = parse_channel_id(
    "KOLFI_DAILY_TOP_MOVERS_CHANNEL_ID", default=1491406034156261487
)
# discord.ext.tasks daily `time=` is interpreted in UTC
KOLFI_DAILY_TOP_MOVERS_TIME_UTC = _parse_time_hhmm(os.getenv("KOLFI_DAILY_TOP_MOVERS_TIME_UTC", "00:00"))
KOLFI_DAILY_TOP_MOVERS_TOP_N = max(1, min(25, _env_int("KOLFI_DAILY_TOP_MOVERS_TOP_N", 10)))
KOLFI_DAILY_TOP_MOVERS_MIN_LIQ_USD = max(0.0, _env_float("KOLFI_DAILY_TOP_MOVERS_MIN_LIQ_USD", 25_000.0))
# Dexscreener 24h %% is often nonsense on thin pools (e.g. millions %%). Cap display + flag low-liq pairs.
# Set KOLFI_H24_PCT_DISPLAY_CAP=0 to disable capping (not recommended for public feeds).
KOLFI_H24_PCT_DISPLAY_CAP = max(0.0, _env_float("KOLFI_H24_PCT_DISPLAY_CAP", 2500.0))
KOLFI_H24_MIN_LIQ_USD_FOR_PCT = max(0.0, _env_float("KOLFI_H24_MIN_LIQ_USD_FOR_PCT", 25_000.0))

# ---------------------------------------------------------------------------
# Telegram calls → Discord bridge (no bot; uses your Telegram user session)
# ---------------------------------------------------------------------------
ENABLE_TELEGRAM_CALLS = os.getenv("ENABLE_TELEGRAM_CALLS", "0").strip().lower() in ("1", "true", "yes", "on")
TELEGRAM_API_ID = (os.getenv("TELEGRAM_API_ID") or "").strip()
TELEGRAM_API_HASH = (os.getenv("TELEGRAM_API_HASH") or "").strip()
# Telethon StringSession (recommended). Generate once using telegram_session.py
TELEGRAM_SESSION = (os.getenv("TELEGRAM_SESSION") or "").strip()
# Comma-separated list of public channels/groups: @username, t.me/username, or just username
TELEGRAM_CALLS_SOURCES = [s.strip() for s in (os.getenv("TELEGRAM_CALLS_SOURCES") or "").split(",") if s.strip()]
# Where to post forwarded calls
TELEGRAM_CALLS_DISCORD_CHANNEL_ID = parse_channel_id(
    "TELEGRAM_CALLS_DISCORD_CHANNEL_ID", default=1493956824401973418
)
# Optional safety / formatting
TELEGRAM_CALLS_INCLUDE_RAW_TEXT = os.getenv("TELEGRAM_CALLS_INCLUDE_RAW_TEXT", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
TELEGRAM_CALLS_MAX_TEXT_CHARS = max(120, min(3500, _env_int("TELEGRAM_CALLS_MAX_TEXT_CHARS", 900)))
TELEGRAM_CALLS_DEDUP_SECONDS = max(0, min(3600, _env_int("TELEGRAM_CALLS_DEDUP_SECONDS", 180)))
TELEGRAM_CALLS_CHAINS = [
    s.strip().lower()
    for s in (os.getenv("TELEGRAM_CALLS_CHAINS") or "solana,bsc,base,ethereum").split(",")
    if s.strip()
]

# Daily finds — rolling last-24h projects we alerted (see ESCALATION_DAILY_FINDS_MODE for when it posts)
# Live momentum alerts never post here (discord_bot excludes this ID from escalation targets).
ENABLE_ESCALATION_DAILY_TOP_MOVERS = os.getenv("ENABLE_ESCALATION_DAILY_TOP_MOVERS", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
ESCALATION_DAILY_TOP_MOVERS_CHANNEL_ID = parse_channel_id(
    "ESCALATION_DAILY_TOP_MOVERS_CHANNEL_ID", default=1491477999105740911
)
ESCALATION_DAILY_TOP_MOVERS_TIME_UTC = _parse_time_hhmm(
    os.getenv("ESCALATION_DAILY_TOP_MOVERS_TIME_UTC", "00:00")
)
ESCALATION_DAILY_TOP_MOVERS_TOP_N = max(1, min(500, _env_int("ESCALATION_DAILY_TOP_MOVERS_TOP_N", 200)))
# Daily finds digest channel: **scan** = post full rolling-24h recap after each twitter monitor cycle;
# **scheduled** = only at ESCALATION_DAILY_TOP_MOVERS_TIME_UTC; **both** = cycle + scheduled.
_ESCALATION_DAILY_MODE = (os.getenv("ESCALATION_DAILY_FINDS_MODE", "scan") or "scan").strip().lower()
if _ESCALATION_DAILY_MODE not in ("scan", "scheduled", "both"):
    _ESCALATION_DAILY_MODE = "scan"
ESCALATION_DAILY_FINDS_MODE = _ESCALATION_DAILY_MODE

# Waypoint MintScan — 1h Mints Overview leaderboard (ETH); new post each interval (default hourly)
ENABLE_MINTS_OVERVIEW = os.getenv("ENABLE_MINTS_OVERVIEW", "1").strip().lower() in ("1", "true", "yes", "on")
MINTS_OVERVIEW_CHANNEL_ID = parse_channel_id("MINTS_OVERVIEW_CHANNEL_ID")
MINTS_OVERVIEW_POLL_MINUTES = max(5, min(120, _env_int("MINTS_OVERVIEW_POLL_MINUTES", 60)))
MINTS_OVERVIEW_TOP_N = max(3, min(20, _env_int("MINTS_OVERVIEW_TOP_N", 12)))


VERIFIED_ROLE_ID = _env_int("VERIFIED_ROLE_ID", 0)
# Staff log channel for verification events (set to 0 to disable)
VERIFICATION_LOG_CHANNEL_ID = parse_channel_id("VERIFICATION_LOG_CHANNEL_ID", default=1486392698763083790)
VERIFICATION_RULES_TEXT = os.getenv(
    "VERIFICATION_RULES_TEXT",
    "Welcome. Please read our rules, then verify below to access the server.",
)
_DEFAULT_ETH_TREASURY = "0xDB673105123c54611043144f18e8f145AFE052Df"
_DEFAULT_SOL_TREASURY = "CoNzMiAKSqZBUGu5XgEXRpiE3ByiP32wW3fJB76sf1p2"
CRYPTO_ETH_ADDRESS = (os.getenv("CRYPTO_ETH_ADDRESS", _DEFAULT_ETH_TREASURY) or "").strip()
CRYPTO_SOL_ADDRESS = (os.getenv("CRYPTO_SOL_ADDRESS", _DEFAULT_SOL_TREASURY) or "").strip()
CRYPTO_USDT_ERC20_ADDRESS = os.getenv("CRYPTO_USDT_ERC20_ADDRESS", "")
PAYMENT_PANEL_NOTES = os.getenv(
    "PAYMENT_PANEL_NOTES",
    "Send **native ETH** (Ethereum mainnet or **Base**) or **native SOL** to the treasuries below. "
    "Then run **`/claim_premium`**, pick the **same network** you used, and paste your **tx hash**.",
)
PAYMENT_EXTRA_INSTRUCTIONS = os.getenv("PAYMENT_EXTRA_INSTRUCTIONS", "")

# Panel display: monthly price in USD → shown as native ETH/SOL (live rates; fallbacks if API fails)
PAYMENT_PANEL_PRICE_USD = max(1.0, min(1_000_000.0, _env_float("PAYMENT_PANEL_PRICE_USD", 30.0)))
PAYMENT_PRICE_FALLBACK_ETH_USD = max(1.0, _env_float("PAYMENT_PRICE_FALLBACK_ETH_USD", 3500.0))
PAYMENT_PRICE_FALLBACK_SOL_USD = max(0.01, _env_float("PAYMENT_PRICE_FALLBACK_SOL_USD", 150.0))

# --- Premium crypto payments (/claim_premium): native ETH (mainnet + Base), native SOL ---
# Treasuries: mainnet can fall back to CRYPTO_ETH_ADDRESS; Sol can fall back to CRYPTO_SOL_ADDRESS.
PAYMENT_TREASURY_ETH_MAINNET = (
    (os.getenv("PAYMENT_TREASURY_ETH_MAINNET") or "").strip() or CRYPTO_ETH_ADDRESS or None
)
# Base uses same EVM address by default (override with PAYMENT_TREASURY_ETH_BASE in .env if different)
PAYMENT_TREASURY_ETH_BASE = (
    (os.getenv("PAYMENT_TREASURY_ETH_BASE") or "").strip() or CRYPTO_ETH_ADDRESS or None
)
PAYMENT_TREASURY_SOL = (
    (os.getenv("PAYMENT_TREASURY_SOL") or "").strip() or CRYPTO_SOL_ADDRESS or None
)

PREMIUM_LIFETIME_ROLE_ID = _env_int("PREMIUM_LIFETIME_ROLE_ID", 1491165334491627560)
PREMIUM_MONTHLY_ROLE_ID = _env_int("PREMIUM_MONTHLY_ROLE_ID", 1486392697760649394)

# Minimum accepted amounts (native). 0 = that tier/chain combo is disabled.
PAYMENT_LIFETIME_MIN_ETH_MAINNET = _env_float("PAYMENT_LIFETIME_MIN_ETH_MAINNET", 0.0)
PAYMENT_LIFETIME_MIN_ETH_BASE = _env_float("PAYMENT_LIFETIME_MIN_ETH_BASE", 0.0)
PAYMENT_LIFETIME_MIN_SOL = _env_float("PAYMENT_LIFETIME_MIN_SOL", 0.0)
PAYMENT_MONTHLY_MIN_ETH_MAINNET = _env_float("PAYMENT_MONTHLY_MIN_ETH_MAINNET", 0.0)
PAYMENT_MONTHLY_MIN_ETH_BASE = _env_float("PAYMENT_MONTHLY_MIN_ETH_BASE", 0.0)
PAYMENT_MONTHLY_MIN_SOL = _env_float("PAYMENT_MONTHLY_MIN_SOL", 0.0)

PAYMENT_MIN_CONFIRMATIONS_ETH = max(1, min(256, _env_int("PAYMENT_MIN_CONFIRMATIONS_ETH", 12)))
PAYMENT_MIN_CONFIRMATIONS_BASE = max(1, min(256, _env_int("PAYMENT_MIN_CONFIRMATIONS_BASE", 12)))
PREMIUM_MONTHLY_DAYS = max(1, min(365, _env_int("PREMIUM_MONTHLY_DAYS", 30)))
LIFETIME_REMOVES_MONTHLY = os.getenv("LIFETIME_REMOVES_MONTHLY", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
PAYMENT_MAX_CLAIMS_PER_DAY = max(1, min(100, _env_int("PAYMENT_MAX_CLAIMS_PER_DAY", 5)))
PAYMENT_LOG_CHANNEL_ID = parse_channel_id("PAYMENT_LOG_CHANNEL_ID", default=0)

# Referrals (internal credits)
ENABLE_REFERRALS = os.getenv("ENABLE_REFERRALS", "1").strip().lower() in ("1", "true", "yes", "on")
REFERRAL_PAYOUT_PCT = max(0.0, min(0.8, _env_float("REFERRAL_PAYOUT_PCT", 0.20)))
REFERRAL_LOG_CHANNEL_ID = parse_channel_id("REFERRAL_LOG_CHANNEL_ID", default=0)
# Auto-referrals via Discord invites (requires bot permission: Manage Server to read invites)
ENABLE_INVITE_REFERRALS = os.getenv("ENABLE_INVITE_REFERRALS", "1").strip().lower() in ("1", "true", "yes", "on")

# ── CT (Certificate Transparency) Domain Watcher ─────────────────────────────
# Polls crt.sh for new TLS certs whose domains match crypto/NFT/gaming patterns.
# Completely independent of the HVA scan — posts to its own Discord channel.
ENABLE_CT_WATCHER = _env_flag("ENABLE_CT_WATCHER", "1")          # default ON
# Default 0: set CT_WATCHER_CHANNEL_ID in .env to a text channel the bot can post to.
CT_WATCHER_CHANNEL_ID = parse_channel_id("CT_WATCHER_CHANNEL_ID", default=0)
# How often to poll crt.sh (minutes). Min 15, max 120.
CT_POLL_MINUTES = max(15, min(120, _env_int("CT_POLL_MINUTES", 30)))
# Max new domains to process per cycle (Discord spam safety cap).
CT_MAX_PER_CYCLE = max(1, min(25, _env_int("CT_MAX_PER_CYCLE", 10)))


def _proxy_url_from_parts(host, port, user, pw):
    """Build http proxy URL for twikit/httpx. Do not percent-encode '=' in passwords — Decodo expects literal credentials."""
    return f"http://{user}:{pw}@{host}:{port}"


def get_proxies():
    """Load proxies from proxies.txt and format them for twikit."""
    proxies = []
    from app_paths import DATA_DIR, ensure_dirs

    ensure_dirs()
    proxy_file = os.path.join(DATA_DIR, "proxies.txt")
    if os.path.exists(proxy_file):
        with open(proxy_file, "r", encoding="utf-8-sig") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                # host:port:user:password — use max 3 splits so passwords may contain ':'
                parts = line.split(":", 3)
                if len(parts) == 4:
                    host, port, user, pw = parts
                    proxies.append(_proxy_url_from_parts(host.strip(), port.strip(), user, pw))
                elif len(parts) == 2:
                    host, port = parts[0].strip(), parts[1].strip()
                    proxies.append(f"http://{host}:{port}")
                    print(
                        f"⚠️ proxies.txt line {line_num}: host:port only (no user/password). "
                        "Decodo usually needs host:port:user:pass — sessions may fail."
                    )
                else:
                    print(f"⚠️ proxies.txt line {line_num}: skipped (expected host:port:user:pass or host:port).")

    # Optional single proxy from .env (normalize trailing slash)
    env_proxy = (TWITTER_PROXY or "").strip().rstrip("/")
    if env_proxy and env_proxy not in proxies:
        if "@" not in env_proxy.split("://", 1)[-1]:
            print(
                "⚠️ TWITTER_PROXY has no credentials (no user:pass before @). "
                "Traffic may be rejected; prefer full URL or use proxies.txt with host:port:user:pass."
            )
        proxies.append(env_proxy)

    for p in proxies:
        if p.startswith("http://") and "@" not in p[7:]:
            print(f"⚠️ Loaded proxy without auth in URL: {p[:32]}... — X/twikit often fails (KEY_BYTE, 407, etc.).")

    return proxies
