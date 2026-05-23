"""
Send the last 5 real wallet-tracker alerts as test embeds,
using the NEW Alchemy-enhanced image fetcher + correct collection names.

Usage:
  python scripts/send_last_5_wallet_tests.py [DISCORD_CHANNEL_ID]
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=True)

import aiohttp
import discord
import config
from trackers import eth_tracker

eth_tracker.connect_web3()

import feed_events
import wallet_database


ETHSCAN_KEY = (config.ETHSCAN_API_KEY or "").strip()


def _resolve_tx_details_web3(tx_hash: str, contract: str, wallet: str):
    """Parse ERC-721 Transfer event logs from the tx receipt using Web3."""
    try:
        w3 = eth_tracker.w3
        if not w3 or not w3.is_connected():
            eth_tracker.connect_web3()
            w3 = eth_tracker.w3
        if not w3:
            return None

        receipt = w3.eth.get_transaction_receipt(tx_hash)
        if not receipt:
            return None

        transfer_topic = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        contract_l = contract.lower()
        wallet_l = wallet.lower()

        for log in receipt.logs:
            if log.address.lower() != contract_l:
                continue
            topics = [t.hex() if hasattr(t, "hex") else str(t) for t in log.topics]
            if not topics or topics[0] != transfer_topic:
                continue
            if len(topics) >= 4:
                from_addr = "0x" + topics[1][-40:]
                to_addr = "0x" + topics[2][-40:]
                token_id = int(topics[3], 16)
                # Only return if this transfer involves the tracked wallet
                if from_addr.lower() == wallet_l or to_addr.lower() == wallet_l:
                    return {
                        "token_id": token_id,
                        "from": from_addr,
                        "to": to_addr,
                    }
    except Exception as exc:
        print(f"  Web3 receipt parse failed: {exc}")
    return None


async def _run(channel_id: int) -> None:
    token = (os.getenv("DISCORD_TOKEN") or os.getenv("DISCORD_BOT_TOKEN") or "").strip()
    if not token:
        raise SystemExit("Set DISCORD_TOKEN (or DISCORD_BOT_TOKEN) in .env")

    # Load wallet labels so they resolve correctly in embeds
    wallet_database.init_db()
    eth_tracker.tracked_eth_wallets.update(wallet_database.get_wallets_by_chain("ETH"))

    # Fetch last 5 wallet_nft events
    events = feed_events.list_events(kinds=["wallet_nft"], limit=5)
    if not events:
        raise SystemExit("No wallet_nft events found in feed_events.db")

    print(f"Found {len(events)} recent wallet alert(s). Resolving details...\n")

    connector = aiohttp.TCPConnector(force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        embeds_to_send = []
        for ev in events:
            extra = ev.get("extra") or {}
            wallet = extra.get("wallet", "").strip()
            contract = extra.get("contract", "").strip()
            tx_hash = extra.get("tx", "").strip()
            if not wallet or not contract or not tx_hash:
                continue

            details = _resolve_tx_details_web3(tx_hash, contract, wallet)
            if not details:
                print(f"  [SKIP] {tx_hash[:20]}... — could not resolve tx details")
                continue

            token_id = details["token_id"]
            from_addr = details["from"]
            to_addr = details["to"]
            action_type = "Received" if to_addr.lower() == wallet.lower() else "Sent"

            embed, content, view, files = await eth_tracker.create_eth_nft_embed(
                action_type=action_type,
                wallet=wallet,
                contract=contract,
                session=session,
                tx_hash=tx_hash,
                token_id=token_id,
                from_addr=from_addr,
                to_addr=to_addr,
            )
            embeds_to_send.append((embed, content, view, files))
            print(f"  [OK] {tx_hash[:20]}...  Token #{token_id}  Action: {action_type}")

    if not embeds_to_send:
        raise SystemExit("No embeds could be built.")

    print(f"\nSending {len(embeds_to_send)} embed(s) to channel {channel_id}...")

    intents = discord.Intents.default()
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        try:
            ch = client.get_channel(channel_id) or await client.fetch_channel(channel_id)
            if ch is None:
                print(f"Channel {channel_id} not found.", flush=True)
                return

            for embed, content, view, files in embeds_to_send:
                kwargs: dict = {"embed": embed}
                if content:
                    kwargs["content"] = content
                if view is not None:
                    kwargs["view"] = view
                if files:
                    kwargs["files"] = eth_tracker._fresh_discord_files(files)
                await ch.send(**kwargs)
                await asyncio.sleep(0.7)

            print(
                f"\n✅ Sent {len(embeds_to_send)} test embed(s) → #{getattr(ch, 'name', channel_id)}",
                flush=True,
            )
        finally:
            await client.close()

    try:
        await client.start(token)
    except KeyboardInterrupt:
        await client.close()


def main() -> None:
    if len(sys.argv) >= 2:
        channel_id = int(sys.argv[1].strip())
    else:
        channel_id = int(getattr(config, "DISCORD_NFT_CHANNEL_ID", 0) or 0)
        if not channel_id:
            raise SystemExit(
                "Usage: python scripts/send_last_5_wallet_tests.py <DISCORD_CHANNEL_ID>\n"
                "Or set DISCORD_NFT_CHANNEL_ID in .env"
            )
    asyncio.run(_run(channel_id))


if __name__ == "__main__":
    main()
