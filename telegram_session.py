"""
Generate a Telethon StringSession for your Telegram user account.

Run once locally (interactive):
  python telegram_session.py

Then copy the printed TELEGRAM_SESSION string into your .env:
  TELEGRAM_API_ID=...
  TELEGRAM_API_HASH=...
  TELEGRAM_SESSION=...

Notes:
- This is NOT a bot. It logs in as your Telegram user.
- You must create an app at https://my.telegram.org to get API_ID/API_HASH.
"""

import os
import asyncio

from telethon import TelegramClient
from telethon.sessions import StringSession
from dotenv import load_dotenv

load_dotenv(override=True)


async def main():
    api_id = (os.getenv("TELEGRAM_API_ID") or "").strip()
    api_hash = (os.getenv("TELEGRAM_API_HASH") or "").strip()
    if not api_id.isdigit() or not api_hash:
        raise SystemExit("Missing TELEGRAM_API_ID or TELEGRAM_API_HASH in .env")

    async with TelegramClient(StringSession(), int(api_id), api_hash) as client:
        # This will prompt for phone + code (and 2FA password if enabled)
        await client.start()
        s = client.session.save()
        print("\n=== TELEGRAM_SESSION (StringSession) ===\n")
        print(s)
        print("\n======================================\n")


if __name__ == "__main__":
    asyncio.run(main())

