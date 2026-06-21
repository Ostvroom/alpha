"""
NFTScan-style live + hot mint feed for v3 (WebSocket listener + enrich + embeds).
Ports workflow from nftscan-discord-bot: per-mint live channel, spike hot channel.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import discord

import config
from app_paths import DATA_DIR, ensure_dirs
from brand_assets import collect_embed_attachment_files
from trackers.collection_image import COLLECTION_FALLBACK_FILENAME, ensure_black_collection_fallback_path
from trackers.eth_ws_mint_listener import SPAM_CONTRACTS, SPAM_NAME_PATTERNS, EthMintListener
from trackers.mint_contract_enricher import MintContractEnricher
from trackers.mint_embeds import (
    build_hot_mint_embed,
    build_live_mint_embeds,
    build_mint_x_alpha_embed,
    build_smart_wallet_buy_embed,
)
from trackers.mint_x_alpha import (
    compute_mint_x_alpha,
    mint_qualifies_for_alpha_channel,
)
from trackers.mint_social_fetcher import MintSocialFetcher
from trackers.mint_wallet_intel import (
    attach_collection_wallet_intel,
    attach_hot_mint_wallet_intel,
    enrich_mint_with_wallet_intel,
    get_contract_activity,
    get_tracked_wallet_map,
    prune_contract_activity,
    record_tracked_buy,
    trader_display,
)

logger = logging.getLogger(__name__)

MUTED_FILE = os.path.join(DATA_DIR, "muted_mint_contracts.json")
BANNED_FILE = os.path.join(DATA_DIR, "banned_mint_contracts.json")


class NftscanLiveFeed:
    def __init__(self, bot: discord.Client) -> None:
        self.bot = bot
        self.listener: Optional[EthMintListener] = None
        self.enricher: Optional[MintContractEnricher] = None
        self.social_fetcher: Optional[MintSocialFetcher] = None
        self._seen_mint_txs: Set[str] = set()
        # Per-contract mint events for hot spike detection: {ts, amount, key}
        self._mint_tracker: Dict[str, List[Dict]] = {}
        self._hot_alert_cooldown: Dict[str, datetime] = {}
        self._spam_contracts: Set[str] = set()
        self._spam_contract_expiry: Dict[str, datetime] = {}
        self._muted_contracts: Set[str] = set()
        self._contract_wallet_activity = get_contract_activity()
        self._alpha_alert_cooldown: Dict[str, datetime] = {}
        self._seen_alpha_txs: Set[str] = set()
        self._seen_smart_buy_txs: Set[str] = set()
        self._task: Optional[asyncio.Task] = None
        self._running = False
        ensure_dirs()
        self._load_muted()
        self._load_banned()

    def _load_muted(self) -> None:
        if os.path.exists(MUTED_FILE):
            try:
                with open(MUTED_FILE, "r", encoding="utf-8") as f:
                    self._muted_contracts = set(json.load(f))
            except Exception:
                self._muted_contracts = set()

    def _save_muted(self) -> None:
        try:
            with open(MUTED_FILE, "w", encoding="utf-8") as f:
                json.dump(sorted(self._muted_contracts), f, indent=2)
        except Exception as e:
            logger.warning("Failed to save muted contracts: %s", e)

    def mute_contract(self, address: str) -> None:
        addr = address.lower()
        self._muted_contracts.add(addr)
        self._mint_tracker.pop(addr, None)
        self._hot_alert_cooldown.pop(addr, None)
        self._save_muted()

    def unmute_contract(self, address: str) -> None:
        self._muted_contracts.discard(address.lower())
        self._save_muted()

    def ban_contract(self, address: str, hours: int = 48) -> None:
        from trackers.eth_ws_mint_listener import SPAM_CONTRACTS

        addr = address.lower()
        SPAM_CONTRACTS.add(addr)
        self._spam_contracts.add(addr)
        self._spam_contract_expiry[addr] = datetime.now(timezone.utc) + timedelta(hours=hours)
        self._save_banned()

    def unban_contract(self, address: str) -> None:
        from trackers.eth_ws_mint_listener import SPAM_CONTRACTS

        addr = address.lower()
        SPAM_CONTRACTS.discard(addr)
        self._spam_contracts.discard(addr)
        self._spam_contract_expiry.pop(addr, None)
        self._save_banned()

    def _save_banned(self) -> None:
        try:
            payload = {
                "contracts": sorted(self._spam_contracts),
                "expiry": {
                    k: v.isoformat()
                    for k, v in self._spam_contract_expiry.items()
                },
            }
            with open(BANNED_FILE, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
        except Exception as e:
            logger.warning("Failed to save banned contracts: %s", e)

    def _load_banned(self) -> None:
        from trackers.eth_ws_mint_listener import SPAM_CONTRACTS

        if not os.path.exists(BANNED_FILE):
            return
        try:
            with open(BANNED_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            now = datetime.now(timezone.utc)
            for addr in data.get("contracts", []):
                a = str(addr).lower()
                if a:
                    self._spam_contracts.add(a)
                    SPAM_CONTRACTS.add(a)
            for addr, exp_s in (data.get("expiry") or {}).items():
                try:
                    exp = datetime.fromisoformat(exp_s)
                    if exp.tzinfo is None:
                        exp = exp.replace(tzinfo=timezone.utc)
                    if exp > now:
                        self._spam_contract_expiry[addr.lower()] = exp
                    else:
                        self._spam_contracts.discard(addr.lower())
                        SPAM_CONTRACTS.discard(addr.lower())
                except Exception:
                    pass
        except Exception:
            pass

    @staticmethod
    def _mint_event_key(mint: Dict) -> tuple:
        return (
            (mint.get("tx_hash") or mint.get("hash") or "").strip().lower(),
            str(mint.get("token_id", "")),
        )

    def _record_hot_mint_event(self, contract: str, mint: Dict) -> None:
        """Track mint volume for hot alerts (dedupe by tx+token, count ERC1155 amount)."""
        now = datetime.now(timezone.utc)
        try:
            amount = max(1, int(mint.get("amount") or 1))
        except (TypeError, ValueError):
            amount = 1
        self._mint_tracker.setdefault(contract, []).append(
            {"ts": now, "amount": amount, "key": self._mint_event_key(mint)}
        )
        keep = timedelta(seconds=max(config.HOT_MINT_WINDOW * 2, 120))
        self._mint_tracker[contract] = [
            e for e in self._mint_tracker[contract] if now - e["ts"] <= keep
        ]

    def _hot_mint_volume_in_window(self, contract: str) -> int:
        """NFT mint units in the hot window (deduped, ERC1155 amounts included)."""
        now = datetime.now(timezone.utc)
        window = timedelta(seconds=config.HOT_MINT_WINDOW)
        seen: Set[tuple] = set()
        total = 0
        for e in self._mint_tracker.get(contract, []):
            if now - e["ts"] > window:
                continue
            key = e.get("key")
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            total += int(e.get("amount") or 1)
        return total

    def active_hot_trackers(self) -> List[tuple]:
        """Contracts with mint activity in the hot-mint window: (address, count)."""
        active: List[tuple] = []
        for contract in list(self._mint_tracker.keys()):
            if contract in self._muted_contracts:
                continue
            count = self._hot_mint_volume_in_window(contract)
            if count > 0:
                active.append((contract, count))
            else:
                self._mint_tracker.pop(contract, None)
        active.sort(key=lambda x: x[1], reverse=True)
        return active

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self.enricher = MintContractEnricher()
        self.social_fetcher = MintSocialFetcher()
        tracked = set(get_tracked_wallet_map().keys())
        self.listener = EthMintListener(max_queue=200, tracked_addresses=tracked)
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

    def _sync_tracked_addresses(self) -> None:
        if self.listener:
            self.listener.update_tracked_addresses(set(get_tracked_wallet_map().keys()))

    async def _tick(self) -> None:
        if not self.listener:
            return
        intel_on = getattr(config, "ENABLE_MINT_SMART_WALLET_INTEL", True)
        engagement_window = int(
            getattr(config, "MINT_SMART_ENGAGEMENT_WINDOW_SEC", config.HOT_MINT_WINDOW) or 1800
        )
        if intel_on:
            self._sync_tracked_addresses()
        raw_mints = await self.listener.flush_recent_mints(50)
        if raw_mints:
            print(f"[LiveMints] Received {len(raw_mints)} raw mint event(s) from listener")
        if intel_on:
            tracked_buys = await self.listener.flush_recent_tracked_activity(50)
            wallets = get_tracked_wallet_map()
            store = self._contract_wallet_activity
            for ev in tracked_buys:
                record_tracked_buy(store, ev, wallets)
            await self._process_smart_buy_alerts(tracked_buys, wallets)
            prune_contract_activity(store, engagement_window)

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
            if contract in SPAM_CONTRACTS or contract in self._spam_contracts:
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
        if raw_mints or new_mints:
            logger.info("[LiveMints] raw=%s -> dedup=%s -> filtered=%s", len(raw_mints), len(new_hot_mints), len(new_mints))

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
            if intel_on:
                attach_collection_wallet_intel(
                    m, self._contract_wallet_activity, engagement_window
                )

        if new_mints and config.LIVE_MINT_CHANNEL_ID:
            embeds = build_live_mint_embeds(new_mints)
            logger.info("[LiveMints] Sending %s live mint embed(s) to channel %s", len(embeds), config.LIVE_MINT_CHANNEL_ID)
            if getattr(config, "ENABLE_COOK_SCORE", True):
                await self._send_live_mints_with_cook(new_mints, embeds)
            else:
                await self._send_embeds(config.LIVE_MINT_CHANNEL_ID, embeds)
            for m in new_mints:
                c = (m.get("contract_address") or "").lower()
                if c:
                    self._schedule_watchlist_notify(c, m, "live")

        if new_mints:
            await self._process_x_alpha_alerts(new_mints, "live", intel_on)

        await self._check_hot_mints(new_hot_mints, intel_on, engagement_window)

    def _engagement_window(self) -> int:
        return int(
            getattr(config, "MINT_SMART_ENGAGEMENT_WINDOW_SEC", config.HOT_MINT_WINDOW) or 1800
        )

    async def _process_smart_buy_alerts(
        self,
        buy_events: List[Dict],
        wallets: Dict[str, str],
    ) -> None:
        """Post immediate smart-wallet buy cards to SMART_WALLET_BUY_CHANNEL_ID."""
        if not getattr(config, "ENABLE_MINT_SMART_WALLET_INTEL", True):
            return
        ch = int(getattr(config, "SMART_WALLET_BUY_CHANNEL_ID", 0) or 0)
        if not ch or not buy_events:
            return

        to_send: List[Dict] = []
        for ev in buy_events:
            tx = (ev.get("tx_hash") or ev.get("hash") or "").strip().lower()
            if tx and tx in self._seen_smart_buy_txs:
                continue
            to_addr = (ev.get("to") or "").lower()
            if not to_addr or to_addr not in wallets:
                continue
            payload = dict(ev)
            payload["wallet"] = to_addr
            payload["label"] = trader_display(wallets[to_addr], to_addr)
            to_send.append(payload)
            if tx:
                self._seen_smart_buy_txs.add(tx)
        if len(self._seen_smart_buy_txs) > 50000:
            self._seen_smart_buy_txs = set(random.sample(list(self._seen_smart_buy_txs), 25000))
        if not to_send:
            return

        if self.enricher:
            try:
                to_send = await self.enricher.batch_enrich(to_send)
            except Exception as e:
                logger.debug("smart buy enrich failed: %s", e)

        embeds = [build_smart_wallet_buy_embed(ev) for ev in to_send]
        await self._send_embeds(ch, embeds)
        for ev in to_send:
            c = (ev.get("contract_address") or "").lower()
            if c:
                self._schedule_watchlist_notify(c, ev, "smart_buy")

    async def _process_x_alpha_alerts(
        self,
        mints: List[Dict],
        alert_type: str,
        intel_on: bool,
    ) -> None:
        """Post enriched X/HVA mint cards to MINT_X_ALPHA_CHANNEL_ID only (optional)."""
        if not getattr(config, "ENABLE_MINT_X_ALPHA", True):
            return
        ch = int(getattr(config, "MINT_X_ALPHA_CHANNEL_ID", 0) or 0)
        if not ch:
            return
        min_score = int(getattr(config, "MINT_X_ALPHA_MIN_SCORE", 20) or 20)
        cooldown = timedelta(seconds=int(getattr(config, "MINT_X_ALPHA_COOLDOWN_SEC", 600) or 600))
        now = datetime.now(timezone.utc)

        for m in mints:
            contract = (m.get("contract_address") or "").lower()
            if not contract or contract in self._muted_contracts:
                continue
            tx = m.get("tx_hash", "") or m.get("hash", "")

            if alert_type == "live" and tx and tx in self._seen_alpha_txs:
                continue
            last = self._alpha_alert_cooldown.get(contract)
            if last and (now - last) < cooldown:
                continue

            if self.social_fetcher and not m.get("social_links"):
                try:
                    socials = await self.social_fetcher.fetch(
                        contract, m.get("contract_name", "")
                    )
                    if socials:
                        m["social_links"] = socials
                except Exception:
                    pass

            if intel_on:
                win = self._engagement_window()
                if alert_type == "hot":
                    attach_hot_mint_wallet_intel(m, self._contract_wallet_activity, win)
                else:
                    attach_collection_wallet_intel(m, self._contract_wallet_activity, win)

            try:
                signals = compute_mint_x_alpha(m)
            except Exception as e:
                logger.debug("mint x alpha compute failed: %s", e)
                continue

            if not mint_qualifies_for_alpha_channel(m, signals, min_score=min_score):
                continue

            try:
                embed = build_mint_x_alpha_embed(m, alert_type)
                await self._send_embeds(ch, [embed])
                self._alpha_alert_cooldown[contract] = now
                if tx:
                    self._seen_alpha_txs.add(tx)
                if len(self._seen_alpha_txs) > 50000:
                    self._seen_alpha_txs = set(random.sample(list(self._seen_alpha_txs), 25000))
            except Exception as e:
                logger.warning("X alpha alert send failed: %s", e)

    async def _check_hot_mints(
        self,
        new_mints: List[Dict],
        intel_on: bool = True,
        engagement_window: Optional[int] = None,
    ) -> None:
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
            self._record_hot_mint_event(contract, m)
            count = self._hot_mint_volume_in_window(contract)
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

        logger.info("[HotMints] Sending %s hot mint alert(s) to channel %s", len(hot_collections), ch)
        for hot in hot_collections:
            mint = hot["mint"]
            if intel_on:
                win = engagement_window or self._engagement_window()
                attach_hot_mint_wallet_intel(mint, self._contract_wallet_activity, win)
            embed = build_hot_mint_embed(mint, hot["count"], config.HOT_MINT_WINDOW)
            await self._send_embeds(ch, [embed])
            try:
                import alert_snapshots

                c = (mint.get("contract_address") or "").lower()
                if c:
                    alert_snapshots.record_snapshot(
                        kind="hot_mint",
                        ref=c,
                        ref_label=(mint.get("contract_name") or c)[:200],
                        supply_at=mint.get("total_supply"),
                        max_supply_at=mint.get("max_supply"),
                        extra={"contract": c, "hot_count": hot["count"]},
                    )
            except Exception:
                pass
            mint["hot_mint_count"] = hot["count"]
            c = (mint.get("contract_address") or "").lower()
            if c:
                self._schedule_watchlist_notify(c, mint, "hot")
            mint["hot_mint_window"] = config.HOT_MINT_WINDOW
            await self._process_x_alpha_alerts([mint], "hot", intel_on)

    def _schedule_watchlist_notify(self, contract: str, mint: Dict, alert_type: str) -> None:
        if not getattr(config, "ENABLE_USER_WATCHLIST", True):
            return
        try:
            from trackers.watchlist_notify import notify_watchlist_users

            asyncio.create_task(notify_watchlist_users(self.bot, contract, mint, alert_type))
        except Exception as e:
            logger.debug("watchlist notify schedule failed: %s", e)

    async def post_community_hot_pick(self, contract: str, fire_count: int) -> None:
        """Hot-channel alert when live mint message hits cook-score threshold."""
        ch = int(getattr(config, "HOT_MINT_CHANNEL_ID", 0) or 0)
        if not ch:
            return
        contract = (contract or "").lower()
        if not contract or contract in self._muted_contracts:
            return

        mint: Dict = {
            "contract_address": contract,
            "token_id": "1",
            "community_fire": fire_count,
        }
        if self.enricher:
            try:
                mint = await self.enricher.enrich(mint)
            except Exception as e:
                logger.debug("community pick enrich: %s", e)
        if self.social_fetcher and not mint.get("social_links"):
            try:
                socials = await self.social_fetcher.fetch(contract, mint.get("contract_name", ""))
                if socials:
                    mint["social_links"] = socials
            except Exception:
                pass

        win = self._engagement_window()
        attach_hot_mint_wallet_intel(mint, self._contract_wallet_activity, win)
        count = max(self._hot_mint_volume_in_window(contract), 1)
        embed = build_hot_mint_embed(mint, count, config.HOT_MINT_WINDOW)
        embed.insert_field_at(
            0,
            name="🔥 Community pick",
            value=f"**{fire_count}** {getattr(config, 'COOK_SCORE_FIRE_EMOJI', '🔥')} reactions on live mint alerts",
            inline=False,
        )
        await self._send_embeds(ch, [embed])
        self._schedule_watchlist_notify(contract, mint, "community_hot")
        logger.info("[CookScore] Community hot pick for %s (%s fires)", contract[:10], fire_count)

    async def _send_live_mints_with_cook(self, mints: List[Dict], embeds: List[discord.Embed]) -> None:
        """One message per live mint so cook-score reactions map to a single contract."""
        from trackers import cook_score

        ch_id = config.LIVE_MINT_CHANNEL_ID
        if not ch_id:
            return
        pairs = list(zip(embeds, mints[: len(embeds)]))
        for embed, mint in pairs:
            msg = await self._send_single_embed(ch_id, embed)
            if msg is None:
                continue
            contract = (mint.get("contract_address") or "").lower()
            guild_id = int(getattr(msg.guild, "id", 0) or 0)
            if contract and guild_id:
                cook_score.register_live_message(msg.id, msg.channel.id, guild_id, contract)
                await cook_score.add_fire_reactions(msg)
            try:
                import alert_snapshots

                alert_snapshots.record_snapshot(
                    kind="live_mint",
                    ref=contract,
                    ref_label=(mint.get("contract_name") or contract)[:200],
                    guild_id=guild_id,
                    channel_id=int(msg.channel.id),
                    message_id=int(msg.id),
                    supply_at=mint.get("total_supply"),
                    max_supply_at=mint.get("max_supply"),
                    extra={
                        "contract": contract,
                        "name": mint.get("contract_name"),
                        "tx": mint.get("tx_hash") or mint.get("hash"),
                    },
                )
            except Exception:
                pass

    async def _send_single_embed(self, channel_id: int, embed: discord.Embed) -> Optional[discord.Message]:
        if not self._validate_embed(embed):
            return None
        channel = self.bot.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(channel_id)
            except Exception:
                return None
        fallback_name = COLLECTION_FALLBACK_FILENAME
        ensure_black_collection_fallback_path()
        needs_fallback = (
            embed.to_dict().get("thumbnail", {}).get("url") == f"attachment://{fallback_name}"
        )
        file_specs: List[Tuple[str, str]] = []
        if needs_fallback:
            file_specs.append((str(ensure_black_collection_fallback_path()), fallback_name))
        for bf in collect_embed_attachment_files([embed]):
            if not any(fname == bf.filename for _, fname in file_specs):
                if hasattr(bf, "_filename") and isinstance(bf._filename, str):
                    file_specs.append((bf._filename, bf.filename))
        for attempt in range(3):
            files: List[discord.File] = []
            for path, fname in file_specs:
                if Path(path).is_file():
                    files.append(discord.File(path, filename=fname))
            kwargs: Dict = {}
            if files:
                kwargs["files"] = files
            try:
                return await channel.send(embed=embed, **kwargs)
            except discord.HTTPException as e:
                status = int(getattr(e, "status", 0) or 0)
                if status == 429:
                    await asyncio.sleep(float(getattr(e, "retry_after", 1.0) or 1.0) + 0.5)
                    continue
                if status >= 500:
                    await asyncio.sleep(2.0 * (2**attempt))
                    continue
                logger.error("send single embed %s: %s", channel_id, e)
                break
            except Exception as e:
                logger.error("send single embed %s: %s", channel_id, e)
                break
        return None

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
            # Build list of (path, filename) tuples so we can recreate discord.File on each retry
            file_specs: List[Tuple[str, str]] = []
            if needs_fallback:
                file_specs.append((str(ensure_black_collection_fallback_path()), fallback_name))
            for bf in collect_embed_attachment_files(chunk):
                if not any(fname == bf.filename for _, fname in file_specs):
                    # discord.File from path can be recreated; from buffer we need to re-read
                    if hasattr(bf, '_fp') and hasattr(bf._fp, 'name') and isinstance(bf._fp.name, str):
                        file_specs.append((bf._fp.name, bf.filename))
                    elif hasattr(bf, '_filename') and isinstance(bf._filename, str):
                        file_specs.append((bf._filename, bf.filename))
                    else:
                        # Buffer-based files can't be recreated easily; skip on retry
                        pass

            for d_attempt in range(3):
                files: List[discord.File] = []
                for path, fname in file_specs:
                    if Path(path).is_file():
                        files.append(discord.File(path, filename=fname))
                kwargs: Dict = {}
                if files:
                    kwargs["files"] = files
                try:
                    await channel.send(embeds=chunk, **kwargs)
                    break
                except discord.HTTPException as e:
                    status = int(getattr(e, "status", 0) or 0)
                    if status == 429:
                        retry_after = float(getattr(e, "retry_after", 1.0) or 1.0)
                        logger.warning("Discord 429 mint feed %s; retry after %.1fs (attempt %d/3)", channel_id, retry_after, d_attempt + 1)
                        await asyncio.sleep(retry_after + 0.5)
                        continue
                    if status >= 500:
                        wait = 2.0 * (2 ** d_attempt)
                        logger.warning("Discord %s mint feed %s; retrying in %.1fs (attempt %d/3)", status, channel_id, wait, d_attempt + 1)
                        await asyncio.sleep(wait)
                        continue
                    logger.error("Discord HTTP error mint feed %s: %s", channel_id, e)
                    break
                except Exception as e:
                    logger.error("Failed to send mint feed to %s: %s", channel_id, e)
                    break

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


def get_nftscan_live_feed() -> Optional[NftscanLiveFeed]:
    return _feed


def normalize_eth_contract(address: str) -> Optional[str]:
    addr = (address or "").strip().lower()
    if not addr.startswith("0x"):
        addr = "0x" + addr
    if len(addr) != 42:
        return None
    try:
        int(addr[2:], 16)
    except ValueError:
        return None
    return addr


def record_smart_wallet_buy_from_tracker(event: Dict) -> None:
    """Bridge eth wallet tracker buys into mint-feed collection memory + optional alert."""
    if not getattr(config, "ENABLE_MINT_SMART_WALLET_INTEL", True):
        return
    wallets = get_tracked_wallet_map()
    store = get_contract_activity()
    if _feed is not None:
        store = _feed._contract_wallet_activity
    record_tracked_buy(store, event, wallets)
    if _feed is not None:
        asyncio.create_task(_feed._process_smart_buy_alerts([event], wallets))


async def start_nftscan_live_feed(bot: discord.Client) -> NftscanLiveFeed:
    global _feed
    if _feed is None:
        _feed = NftscanLiveFeed(bot)
    await _feed.start()
    return _feed
