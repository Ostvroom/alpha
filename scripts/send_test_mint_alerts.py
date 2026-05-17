"""
Send preview live / hot / X-alpha mint embeds (tests smart-wallet merge + branding).

Usage:
  python scripts/send_test_mint_alerts.py
  python scripts/send_test_mint_alerts.py --live-only
  python scripts/send_test_mint_alerts.py --channel 1505177985026756628

Uses .env: DISCORD_TOKEN, LIVE_MINT_CHANNEL_ID, HOT_MINT_CHANNEL_ID, MINT_X_ALPHA_CHANNEL_ID
Optional: TEST_MINT_CONTRACT, TEST_COLLECTION_NAME, TEST_WALLET, TEST_WALLET_LABEL, TEST_TOKEN_ID
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env", override=True)


def _fresh_files(files):
    import discord

    out = []
    for f in files or []:
        path = getattr(f, "fp", None) or getattr(f, "_fp", None) or getattr(f, "filename", None)
        name = getattr(f, "filename", None)
        if path and name and os.path.isfile(str(path)):
            out.append(discord.File(str(path), filename=name))
    return out


async def _send_embed(ch, embed, files=None):
    import discord

    kwargs = {"embed": embed}
    if files:
        kwargs["files"] = _fresh_files(files)
    await ch.send(**kwargs)


async def _run(args) -> None:
    import aiohttp
    import discord

    import config
    import wallet_database
    from brand_assets import collect_embed_attachment_files
    from trackers import eth_tracker
    from trackers.collection_image import COLLECTION_FALLBACK_FILENAME, ensure_black_collection_fallback_path
    from trackers.eth_address import ZERO_ADDRESS, normalize_eth_address
    from trackers.mint_embeds import build_hot_mint_embed, build_live_mint_embeds, build_mint_x_alpha_embed
    from trackers.mint_wallet_intel import (
        attach_collection_wallet_intel,
        attach_hot_mint_wallet_intel,
        get_contract_activity,
        record_tracked_buy,
        register_wallet_tracker_mint,
    )
    from trackers.mint_x_alpha import compute_mint_x_alpha

    token = (os.getenv("DISCORD_TOKEN") or os.getenv("DISCORD_BOT_TOKEN") or "").strip()
    if not token:
        raise SystemExit("Set DISCORD_TOKEN or DISCORD_BOT_TOKEN in .env")

    wallet_database.init_db()
    eth_tracker.tracked_eth_wallets.update(wallet_database.get_wallets_by_chain("ETH"))

    wallet = normalize_eth_address(
        os.getenv("TEST_WALLET", "0xaf29ab7418516cc3f22e609dc783d75864ab545a")
    )
    label = os.getenv("TEST_WALLET_LABEL", "ctrlplus.eth").strip()
    if wallet and label:
        eth_tracker.tracked_eth_wallets[wallet] = label

    contract = normalize_eth_address(
        os.getenv("TEST_MINT_CONTRACT", "0x0000000000000000000000000000000000000001")
    )
    name = os.getenv("TEST_COLLECTION_NAME", "Nomadies").strip()
    token_id = os.getenv("TEST_TOKEN_ID", "1896").strip()
    tx_hash = os.getenv(
        "TEST_TX_HASH",
        f"0x{'a' * 62}{int(time.time()) % 10}",
    ).strip()

    # Simulate padded topic (old bug) — enrichment should still resolve trader
    padded_to = "0x000000000000000000000000" + wallet[2:]

    mint = {
        "type": "erc721",
        "contract_address": contract,
        "contract_name": name,
        "token_id": token_id,
        "to": padded_to,
        "tx_hash": tx_hash,
        "block_number": 25108806,
        "mint_cost": 0,
        "gas_fee": 0.00002,
        "gas_fee_usd": 0.0766,
        "mint_cost_usd": 0,
        "total_supply": 1640,
        "max_supply": 7777,
        "opensea_url": f"https://opensea.io/assets/ethereum/{contract}/{token_id}",
        "etherscan_url": f"https://etherscan.io/address/{contract}",
        "social_links": {},
    }

    register_wallet_tracker_mint(tx_hash, wallet, label, contract=contract, token_id=token_id)

    connector = aiohttp.TCPConnector(force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        from trackers.mint_contract_enricher import MintContractEnricher
        from trackers.mint_social_fetcher import MintSocialFetcher

        enricher = MintContractEnricher()
        social = MintSocialFetcher()
        try:
            mint = await enricher.enrich(mint)
            socials = await social.fetch(contract, name)
            if socials:
                mint["social_links"] = socials
        except Exception as e:
            print(f"[warn] enrich skipped: {e}")
        finally:
            await enricher.close()
            await social.close()

    store = get_contract_activity()
    record_tracked_buy(
        store,
        {
            "to": wallet,
            "contract_address": contract,
            "token_id": "1793",
            "tx_hash": "0x" + "b" * 64,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )
    win = int(getattr(config, "MINT_SMART_ENGAGEMENT_WINDOW_SEC", 1800) or 1800)
    attach_collection_wallet_intel(mint, store, win)
    attach_hot_mint_wallet_intel(mint, store, win)
    compute_mint_x_alpha(mint)

    print(f"Trader: {mint.get('tracked_trader')} | smart={mint.get('is_smart_wallet_event')}")
    print(f"to normalized: {mint.get('to')} | x_alpha score: {mint.get('x_alpha_score')}")

    live_embeds = build_live_mint_embeds([mint])
    hot_embed_test = build_hot_mint_embed(
        {**mint, "hot_mint_count": 12, "hot_mint_window": config.HOT_MINT_WINDOW},
        count=12,
        window_seconds=config.HOT_MINT_WINDOW,
    )
    alpha_embed = build_mint_x_alpha_embed(dict(mint), "live")
    alpha_hot = build_mint_x_alpha_embed(
        {**mint, "hot_mint_count": 12, "hot_mint_window": config.HOT_MINT_WINDOW},
        "hot",
    )

    targets = []
    if args.channel:
        targets.append(("custom", int(args.channel)))
    else:
        if not args.hot_only and not args.alpha_only:
            if config.LIVE_MINT_CHANNEL_ID:
                targets.append(("live", int(config.LIVE_MINT_CHANNEL_ID)))
        if not args.live_only and not args.alpha_only:
            if config.HOT_MINT_CHANNEL_ID:
                targets.append(("hot", int(config.HOT_MINT_CHANNEL_ID)))
        if not args.live_only and not args.hot_only:
            if getattr(config, "ENABLE_MINT_X_ALPHA", True) and config.MINT_X_ALPHA_CHANNEL_ID:
                targets.append(("x-alpha-live", int(config.MINT_X_ALPHA_CHANNEL_ID)))
                targets.append(("x-alpha-hot", int(config.MINT_X_ALPHA_CHANNEL_ID)))

    if not targets:
        raise SystemExit("No target channels. Set LIVE_MINT_CHANNEL_ID in .env or pass --channel ID.")

    intents = discord.Intents.default()
    client = discord.Client(intents=intents)
    ensure_black_collection_fallback_path()

    async def _post_all():
        await client.wait_until_ready()
        for kind, cid in targets:
            ch = client.get_channel(cid) or await client.fetch_channel(cid)
            if not ch:
                print(f"[skip] channel {cid} not found / no access", flush=True)
                continue
            if kind == "live":
                emb = live_embeds[0]
            elif kind == "hot":
                emb = hot_embed_test
            elif kind == "x-alpha-live":
                emb = alpha_embed
            else:
                emb = alpha_hot
            files = []
            thumb = emb.to_dict().get("thumbnail", {}).get("url", "")
            if thumb == f"attachment://{COLLECTION_FALLBACK_FILENAME}":
                p = ensure_black_collection_fallback_path()
                files.append(discord.File(str(p), filename=COLLECTION_FALLBACK_FILENAME))
            for bf in collect_embed_attachment_files([emb]):
                if not any(f.filename == bf.filename for f in files):
                    files.append(bf)
            await _send_embed(ch, emb, files or None)
            print(f"Sent [{kind}] → #{getattr(ch, 'name', cid)} ({cid})", flush=True)

    try:
        async with client:
            await client.login(token)
            task = asyncio.create_task(_post_all())
            await asyncio.wait_for(task, timeout=90)
    except asyncio.TimeoutError:
        raise SystemExit("Discord login/post timed out (90s). Check token and channel IDs.")


def main():
    p = argparse.ArgumentParser(description="Send test mint feed embeds")
    p.add_argument("--channel", type=int, help="Single channel ID (overrides config targets)")
    p.add_argument("--live-only", action="store_true")
    p.add_argument("--hot-only", action="store_true")
    p.add_argument("--alpha-only", action="store_true")
    args = p.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
