"""
Post one ETH NFT wallet-tracker embed (same builder as live alerts) to a channel.

Prerequisites:
  - Same .env as the bot: DISCORD_BOT_TOKEN
  - Channel the bot can write to

Usage:
  python scripts/send_test_wallet_embed.py <DISCORD_CHANNEL_ID>

Optional env:
  TEST_WALLET        — tracked address shown as "trader" (default: Vitalik)
  TEST_CONTRACT      — ERC-721 contract (default: BAYC)
  TEST_TX_HASH       — mainnet tx hash (default: a real BAYC transfer; replace anytime)
  TEST_TOKEN_ID      — integer (default: 1)

Example:
  python scripts/send_test_wallet_embed.py 123456789012345678
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

# Allow `from trackers import ...` when run as: python scripts/send_test_wallet_embed.py
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env", override=True)


async def _run(channel_id: int) -> None:
    import aiohttp
    import discord

    from trackers import eth_tracker

    eth_tracker.connect_web3()

    token = (os.getenv("DISCORD_TOKEN") or os.getenv("DISCORD_BOT_TOKEN") or "").strip()
    if not token:
        raise SystemExit("Set DISCORD_TOKEN (or DISCORD_BOT_TOKEN) in .env")

    wallet = (os.getenv("TEST_WALLET") or "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045").strip()
    contract = (os.getenv("TEST_CONTRACT") or "0xBC4CA0Eda7647A8Ab7c2061c2E118A18a936f13D").strip()
    tx_hash = (
        os.getenv("TEST_TX_HASH") or "0xeb89af798293422caef87503463226472f226cb87266483229692be82778f65"
    ).strip()
    token_id = int(os.getenv("TEST_TOKEN_ID") or "1")

    # Optional display label (same map live alerts use)
    eth_tracker.tracked_eth_wallets[wallet.lower()] = os.getenv("TEST_WALLET_LABEL") or "Test wallet"

    from_addr = "0x0000000000000000000000000000000000000000"
    to_addr = wallet

    intents = discord.Intents.default()
    client = discord.Client(intents=intents)

    connector = aiohttp.TCPConnector(force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        embed, content, view, files = await eth_tracker.create_eth_nft_embed(
            action_type="Received",
            wallet=wallet,
            contract=contract,
            session=session,
            tx_hash=tx_hash,
            token_id=token_id,
            from_addr=from_addr,
            to_addr=to_addr,
        )

    @client.event
    async def on_ready():
        try:
            ch = client.get_channel(channel_id) or await client.fetch_channel(channel_id)
            if ch is None:
                print(f"Channel {channel_id} not found.", flush=True)
                return
            kwargs = {"embed": embed}
            if content:
                kwargs["content"] = content
            if view is not None:
                kwargs["view"] = view
            if files:
                kwargs["files"] = eth_tracker._fresh_discord_files(files)
            await ch.send(**kwargs)
            print(f"Sent test embed → #{getattr(ch, 'name', channel_id)}", flush=True)
        finally:
            await client.close()

    try:
        await client.start(token)
    except KeyboardInterrupt:
        await client.close()


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit(
            "Usage: python scripts/send_test_wallet_embed.py <DISCORD_CHANNEL_ID>\n"
            "Optional: TEST_WALLET, TEST_CONTRACT, TEST_TX_HASH, TEST_TOKEN_ID, TEST_WALLET_LABEL"
        )
    channel_id = int(sys.argv[1].strip())
    asyncio.run(_run(channel_id))


if __name__ == "__main__":
    main()
