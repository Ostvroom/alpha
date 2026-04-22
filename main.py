import sys
import logging
import warnings
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

def main():
    if not config.DISCORD_TOKEN:
        print("Error: DISCORD_TOKEN not found in environment variables.")
        sys.exit(1)
    
    if config.DISCORD_CHANNEL_ID == 0:
        print("Warning: DISCORD_CHANNEL_ID is not set. The bot will not be able to send alerts.")

    bot = BlockBrainBot()
    
    try:
        print("Starting Velcor3...")
        bot.run(config.DISCORD_TOKEN)
    except Exception as e:
        print(f"Fatal error running bot: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
