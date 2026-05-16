"""
NFTScan-style live + hot mint feed for v3 (WebSocket listener + enrich + embeds).
Ports workflow from nftscan-discord-bot: per-mint live channel, spike hot channel.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set

import discord

import config
from app_paths import DATA_DIR, ensure_dirs
from brand_assets import collect_embed_attachment_files
from trackers.collection_image import COLLECTION_FALLBACK_FILENAME, ensure_black_collection_fallback_path
from trackers.eth_ws_mint_listener import SPAM_NAME_PATTERNS, EthMintListener
from trackers.mint_contract_enricher import MintContractEnricher
from trackers.mint_embeds import build_hot_mint_embed, build_live_mint_embeds
from trackers.mint_social_fetcher import MintSocialFetcher

logger = logging.getLogger(__name__)

MUTED_FILE = os.path.join(DATA_DIR, "muted_mint_contracts.json")


class NftscanLiveFeed:
    def __init__(self, bot: discord.Client) -> None:
        self.bot = bot
        self.listener: Optional[EthMintListener] = None
        self.enricher: Optional[MintContractEnricher] = None
        self.social_fetcher: Optional[MintSocialFetcher] = None
        self._seen_mint_txs: Set[str] = set()
        self._mint_tracker: Dict[str, List[datetime]] = {}
        self._hot_alert_cooldown: Dict[str, datetime] = {}
        self._spam_contracts: Set[str] = set()
        self._spam_contract_expiry: Dict[str, datetime] = {}
        self._muted_contracts: Set[str] = set()
        self._contract_mint_count: Dict[str, int] = {}
        self._task: Optional[asyncio.Task] = None
        self._running = False
        ensure_dirs()
        self._load_muted()

    def _load_muted(self) -> None:
        if os.path.exists(MUTED_FILE):
            try:
                with open(MUTED_FILE, "r", encoding="utf-8") as f:
                    self._muted_contracts = set(json.load(f))
            except Exception:
                self._muted_contracts = set()

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self.enricher = MintContractEnricher()
        self.social_fetcher = MintSocialFetcher()
        self.listener = EthMintListener(max_queue=200)
        await self.listener.start()
        self._task = asyncio.create_task(self._loop())
        live = config.LIVE_MINT_CHANNEL_ID
        hot = config.HOT_MINT_CHANNEL_ID
        logger.info(
            "NftscanLiveFeed started (live=%s hot=%s interval=%ss)",
            live,
            hot,
            config.LIVE_MINT_INTERVAL,
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self.listener:
            await self.listener.stop()
        if self.enricher:
            await self.enricher.close()
        if self.social_fetcher:
            await self.social_fetcher.close()

    async def _loop(self) -> None:
        await self.bot.wait_until_ready()
        interval = max(15, int(config.LIVE_MINT_INTERVAL))
        while self._running:
            try:
                await self._tick()
            except Exception as e:
                logger.warning("Live mint tick: %s", e)
            await asyncio.sleep(interval)

    async def _tick(self) -> None:
        if not self.listener:
            return
        raw_mints = await self.listener.flush_recent_mints(50)

        for m in raw_mints:
            contract = m.get("contract_address", "").lower()
            if contract:
                self._contract_mint_count[contract] = self._contract_mint_count.get(contract, 0) + 1

        new_mints: List[Dict] = []
        for m in raw_mints:
            tx = m.get("tx_hash", "") or m.get("hash", "")
            if tx and tx not in self._seen_mint_txs:
                self._seen_mint_txs.add(tx)
                new_mints.append(m)
        if len(self._seen_mint_txs) > 5000:
            self._seen_mint_txs.clear()

        new_hot_mints = list(raw_mints)

        if new_mints and self.enricher:
            new_mints = await self.enricher.batch_enrich(new_mints)

        self._clean_spam_cache()
        filtered: List[Dict] = []
        for m in new_mints:
            contract = m.get("contract_address", "").lower()
            name = m.get("contract_name", "")
            if contract in self._muted_contracts:
                continue
            if contract in self._spam_contracts:
                continue
            if self._is_spam_name(name):
                self._spam_contracts.add(contract)
                self._spam_contract_expiry[contract] = datetime.now(timezone.utc) + timedelta(hours=48)
                continue
            if not name or name.startswith("0x") or name.replace(".", "").isdigit():
                continue
            if config.SUPPRESS_UNNAMED:
                is_unnamed = not name or "..." in name
                is_free = float(m.get("mint_cost", 0) or 0) == 0
                if is_unnamed and is_free:
                    continue
            filtered.append(m)
        new_mints = filtered

        if new_mints and self.social_fetcher:
            for m in new_mints:
                contract = m.get("contract_address", "").lower()
                if not contract:
                    continue
                socials = await self.social_fetcher.fetch(contract, m.get("contract_name", ""))
                if socials:
                    m["social_links"] = socials
                    social_name = socials.get("collection_name")
                    current_name = m.get("contract_name", "")
                    if social_name and ("..." in current_name or not current_name):
                        m["contract_name"] = social_name

        for m in new_mints:
            if m.get("total_supply") is None:
                contract = m.get("contract_address", "").lower()
                if contract and contract in self._contract_mint_count:
                    m["total_supply"] = self._contract_mint_count[contract]

        if new_mints and config.LIVE_MINT_CHANNEL_ID:
            embeds = build_live_mint_embeds(new_mints)
            await self._send_embeds(config.LIVE_MINT_CHANNEL_ID, embeds)

        await self._check_hot_mints(new_hot_mints)

    async def _check_hot_mints(self, new_mints: List[Dict]) -> None:
        ch = config.HOT_MINT_CHANNEL_ID
        if not ch or not new_mints:
            return

        now = datetime.now(timezone.utc)
        window = timedelta(seconds=config.HOT_MINT_WINDOW)
        cooldown = timedelta(seconds=config.HOT_MINT_COOLDOWN)
        hot_collections: List[Dict] = []

        for m in new_mints:
            contract = m.get("contract_address", "").lower()
            if not contract or contract in self._muted_contracts:
                continue
            if contract not in self._mint_tracker:
                self._mint_tracker[contract] = []
            self._mint_tracker[contract].append(now)
            self._mint_tracker[contract] = [
                t for t in self._mint_tracker[contract] if now - t <= window
            ]
            count = len(self._mint_tracker[contract])
            if count >= config.HOT_MINT_THRESHOLD:
                last_alert = self._hot_alert_cooldown.get(contract)
                if not last_alert or (now - last_alert) > cooldown:
                    self._hot_alert_cooldown[contract] = now
                    hot_collections.append({"contract": contract, "count": count, "mint": m})

        if not hot_collections:
            return

        if self.enricher:
            to_enrich = [h["mint"] for h in hot_collections]
            enriched = await self.enricher.batch_enrich(to_enrich)
            for hot, e in zip(hot_collections, enriched):
                hot["mint"] = e

        for hot in hot_collections:
            mint = hot["mint"]
            if mint.get("total_supply") is None and hot["contract"] in self._contract_mint_count:
                mint["total_supply"] = self._contract_mint_count[hot["contract"]]
            embed = build_hot_mint_embed(mint, hot["count"], config.HOT_MINT_WINDOW)
            await self._send_embeds(ch, [embed])

    def _is_spam_name(self, name: str) -> bool:
        if not name:
            return True
        n = name.lower()
        if n.startswith("0x") and len(n) >= 40:
            return True
        if n.replace(".", "").replace(",", "").isdigit():
            return True
        for pattern in SPAM_NAME_PATTERNS:
            if pattern in n:
                return True
        return False

    def _clean_spam_cache(self) -> None:
        now = datetime.now(timezone.utc)
        expired = [c for c, exp in self._spam_contract_expiry.items() if now > exp]
        for c in expired:
            self._spam_contracts.discard(c)
            self._spam_contract_expiry.pop(c, None)

    async def _send_embeds(self, channel_id: int, embeds: List[discord.Embed]) -> None:
        if not channel_id or not embeds:
            return
        channel = self.bot.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(channel_id)
            except Exception:
                logger.warning("Mint feed channel %s not found", channel_id)
                return

        valid = [e for e in embeds if self._validate_embed(e)]
        if not valid:
            return

        fallback_name = COLLECTION_FALLBACK_FILENAME
        ensure_black_collection_fallback_path()

        chunks: List[List[discord.Embed]] = []
        current: List[discord.Embed] = []
        size = 0
        for e in valid:
            es = self._embed_size(e)
            if current and (size + es > 5500 or len(current) >= 10):
                chunks.append(current)
                current = [e]
                size = es
            else:
                current.append(e)
                size += es
        if current:
            chunks.append(current)

        for chunk in chunks:
            needs_fallback = any(
                e.to_dict().get("thumbnail", {}).get("url") == f"attachment://{fallback_name}"
                for e in chunk
            )
            files: List[discord.File] = []
            if needs_fallback:
                path = ensure_black_collection_fallback_path()
                files.append(discord.File(str(path), filename=fallback_name))
            for bf in collect_embed_attachment_files(chunk):
                if not any(getattr(f, "filename", None) == bf.filename for f in files):
                    files.append(bf)
            kwargs: Dict = {}
            if files:
                kwargs["files"] = files
            try:
                await channel.send(embeds=chunk, **kwargs)
            except discord.HTTPException as e:
                logger.error("Discord HTTP error mint feed %s: %s", channel_id, e)
            except Exception as e:
                logger.error("Failed to send mint feed to %s: %s", channel_id, e)

    def _embed_size(self, embed: discord.Embed) -> int:
        total = 0
        if embed.title:
            total += len(embed.title)
        if embed.description:
            total += len(embed.description)
        if embed.footer and embed.footer.text:
            total += len(embed.footer.text)
        if embed.author and embed.author.name:
            total += len(embed.author.name)
        for field in embed.fields:
            total += len(field.name or "") + len(field.value or "")
        return total

    def _validate_embed(self, embed: discord.Embed) -> bool:
        try:
            if embed.title and len(embed.title) > 250:
                embed.title = embed.title[:247] + "..."
            if embed.description and len(embed.description) > 4000:
                embed.description = embed.description[:3997] + "..."
            if self._embed_size(embed) > 5900:
                return False
            return True
        except Exception:
            return False


_feed: Optional[NftscanLiveFeed] = None


async def start_nftscan_live_feed(bot: discord.Client) -> NftscanLiveFeed:
    global _feed
    if _feed is None:
        _feed = NftscanLiveFeed(bot)
    await _feed.start()
    return _feed
