import base64
import json
import logging
import os
import sys
import warnings
from pathlib import Path

from discord_bot import BlockBrainBot
import config

# ── Silence noisy third-party DEBUG output ──────────────────────────────────
# twikit emits DEBUG: lines via print() in transaction.py — patch_twikit.py removes them.
# These logging lines cover anything routed through Python logging.
logging.getLogger("twikit").setLevel(logging.WARNING)
logging.getLogger("discord.http").setLevel(logging.WARNING)
logging.getLogger("twikit.client.client").setLevel(logging.WARNING)
logging.getLogger("twikit.utils").setLevel(logging.WARNING)

# Suppress twikit's "Quality Filter" UserWarning (informational, not actionable)
warnings.filterwarnings("ignore", category=UserWarning, module="twikit")


def _materialize_cookies_from_env() -> None:
    """
    If cookies.json is missing but TWIKIT_COOKIES_B64 or TWIKIT_COOKIES_JSON is set,
    write DATA_DIR/cookies.json once. For Render: put JSON in a Secret (env), not in git.
    Never logs cookie contents.
    """
    from app_paths import DATA_DIR, ensure_dirs

    ensure_dirs()
    dest = Path(DATA_DIR) / "cookies.json"
    if dest.is_file():
        return
    raw = ""
    b64 = (os.getenv("TWIKIT_COOKIES_B64") or "").strip()
    if b64:
        try:
            raw = base64.b64decode(b64).decode("utf-8")
        except Exception as e:
            print(f"[Velcor3] TWIKIT_COOKIES_B64 set but decode failed: {e}", flush=True)
            return
    if not raw:
        raw = (os.getenv("TWIKIT_COOKIES_JSON") or "").strip()
    if not raw:
        return
    try:
        json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"[Velcor3] Cookie env payload is not valid JSON: {e}", flush=True)
        return
    try:
        dest.write_text(raw, encoding="utf-8")
        print(
            f"[Velcor3] Wrote {dest} from environment (TWIKIT_COOKIES_B64 / TWIKIT_COOKIES_JSON).",
            flush=True,
        )
    except OSError as e:
        print(f"[Velcor3] Could not write cookies.json: {e}", flush=True)


def _print_startup_paths() -> None:
    from app_paths import BASE_DIR, DATA_DIR, ensure_dirs

    ensure_dirs()
    cookie = Path(DATA_DIR) / "cookies.json"
    db_bb = Path(DATA_DIR) / "block_brain.db"
    print(
        f"[Velcor3] DATA_DIR={DATA_DIR} "
        f"(env DATA_DIR={'set' if (os.environ.get('DATA_DIR') or '').strip() else 'unset'}) "
        f"| cookies.json={'yes' if cookie.is_file() else 'no'} "
        f"| block_brain.db={'yes' if db_bb.is_file() else 'no'} "
        f"| etc_secrets_cookies={'yes' if Path('/etc/secrets/cookies.json').is_file() else 'no'}",
        flush=True,
    )
    if getattr(config, "VELCOR3_VERBOSE_LOGS", False):
        print(f"[Velcor3] BASE_DIR={BASE_DIR} | CWD={os.getcwd()}", flush=True)
        print(
            f"[Velcor3] brain_scan_interval_s={getattr(config, 'CHECK_INTERVAL_SECONDS', '?')} "
            f"| DISCORD_CHANNEL_ID={getattr(config, 'DISCORD_CHANNEL_ID', 0)}",
            flush=True,
        )
        logging.getLogger("discord").setLevel(logging.INFO)
        logging.getLogger("discord.gateway").setLevel(logging.INFO)


def main():
    # Windows consoles often default to cp1252, which crashes on emoji logs.
    # Force UTF-8 with replacement to keep the bot running.
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
            # Render/containers: default full buffering hides prints until buffer fills; line-buffer for logs.
            try:
                sys.stdout.reconfigure(line_buffering=True)
            except Exception:
                pass
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
            try:
                sys.stderr.reconfigure(line_buffering=True)
            except Exception:
                pass
    except Exception:
        pass

    if not config.DISCORD_TOKEN:
        print("Error: DISCORD_TOKEN not found in environment variables.", flush=True)
        sys.exit(1)
    
    if config.DISCORD_CHANNEL_ID == 0:
        print(
            "Warning: DISCORD_CHANNEL_ID is not set. The bot will not be able to send alerts.",
            flush=True,
        )

    _materialize_cookies_from_env()
    _print_startup_paths()

    bot = BlockBrainBot()
    
    try:
        print("Starting Velcor3...", flush=True)
        bot.run(config.DISCORD_TOKEN)
    except Exception as e:
        print(f"Fatal error running bot: {e}", flush=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
