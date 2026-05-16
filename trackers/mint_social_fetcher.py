"""
Social links for live/hot mint embeds (nftscan-discord-bot workflow).
Cache under DATA_DIR/social_links.json.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Dict, Optional

import aiohttp

from app_paths import DATA_DIR, ensure_dirs

logger = logging.getLogger(__name__)

CACHE_FILE = os.path.join(DATA_DIR, "social_links.json")


class MintSocialFetcher:
    def __init__(self) -> None:
        ensure_dirs()
        self._cache: Dict[str, Dict] = {}
        self._load_cache()
        self._session: Optional[aiohttp.ClientSession] = None

    def _load_cache(self) -> None:
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    self._cache = json.load(f)
            except Exception:
                self._cache = {}

    def _save_cache(self) -> None:
        try:
            with open(CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(self._cache, f, indent=2)
        except Exception as e:
            logger.warning("Failed to save social cache: %s", e)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def fetch(self, contract: str, name: str = "") -> Dict:
        contract = contract.lower()
        if contract in self._cache:
            return self._cache[contract]

        links: Dict = {}
        try:
            links = await self._try_opensea(contract)
        except Exception:
            pass

        if not links and name:
            guess = self._guess_twitter(name)
            if guess:
                links["twitter"] = guess

        if links:
            self._cache[contract] = links
            self._save_cache()

        return links

    async def _try_opensea(self, contract: str) -> Dict:
        session = await self._get_session()
        url = f"https://api.opensea.io/api/v2/chain/ethereum/contract/{contract}"
        headers = {"Accept": "application/json", "User-Agent": "Velcor3MintFeed/1.0"}
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                return {}
            data = await resp.json()
            coll = data.get("collection", "")
            if not coll:
                return {}
            url2 = f"https://api.opensea.io/api/v2/collections/{coll}"
            async with session.get(url2, headers=headers) as r2:
                if r2.status == 200:
                    return self._extract_opensea_links(await r2.json())
        return {}

    def _extract_opensea_links(self, data: Dict) -> Dict:
        result: Dict = {}
        if not data:
            return result
        twitter = data.get("twitter_username") or data.get("twitter")
        discord = data.get("discord_url") or data.get("discord")
        website = data.get("external_url") or data.get("website")
        name = data.get("name") or data.get("collection_name")
        if twitter:
            result["twitter"] = f"https://twitter.com/{str(twitter).lstrip('@')}"
        if discord:
            result["discord"] = discord
        if website:
            result["website"] = website
        if name:
            result["collection_name"] = name
        return result

    def _guess_twitter(self, name: str) -> Optional[str]:
        clean = name.lower()
        for suffix in ("nft", "official", "eth", "collection", "project"):
            clean = clean.replace(suffix, "")
        clean = clean.strip().replace(" ", "")
        if len(clean) >= 3:
            return f"https://twitter.com/{clean}"
        return None
