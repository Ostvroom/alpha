"""
Active Mints Tracker - Discord embeds (ETH + Solana marketplaces).
Format: FREE MINT - WARM - Name (SYMBOL), Supply, Mint Price, Unique Minters, Socials, Links.
"""
import asyncio
import time
from datetime import datetime
from typing import Set, Tuple
import aiohttp
from discord import Embed, Color
from trackers.mint_sources import ActiveMint, fetch_active_mints

# Cooldown: don't re-post same (contract, chain) within this many seconds
COOLDOWN_SECONDS = 3600  # 1 hour
_last_sent: Set[Tuple[str, str]] = set()
_sent_at: dict = {}  # (contract, chain) -> timestamp

# Unified alert level tracking: (contract, chain) -> {"tier": 1|2, "last_count": int, "time": float}
# Tier 1 = Radar (first detection), Tier 2 = Trending (high velocity)
_alert_state: dict = {}

def should_alert_radar(contract: str, chain: str) -> bool:
    """Should we fire a Tier 1 (radar) alert? Only fires ONCE per collection."""
    key = (contract.lower(), chain.lower())
    return key not in _alert_state

def should_alert_trending(contract: str, chain: str, current_count: int) -> bool:
    """Should we fire a Tier 2 (trending) alert? Fires at thresholds: first time, then at 2x."""
    key = (contract.lower(), chain.lower())
    state = _alert_state.get(key)
    if not state:
        return True  # Never alerted
    if state.get("tier", 0) < 2:
        return True  # Was only radar, first trending alert
    last_count = state.get("last_count", 0)
    # Re-alert at 2x velocity thresholds
    if last_count > 0 and current_count >= last_count * 2:
        return True
    return False

def mark_alerted(contract: str, chain: str, tier: int, count: int):
    """Mark a collection as alerted at a specific tier."""
    key = (contract.lower(), chain.lower())
    _alert_state[key] = {"tier": tier, "last_count": count, "time": time.time()}
    _sent_at[key] = time.time()
    _last_sent.add(key)


import os
import openai

openai_client = None
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if OPENAI_API_KEY:
    openai_client = openai.AsyncOpenAI(api_key=OPENAI_API_KEY)


async def build_radar_embed(m: ActiveMint) -> Embed:
    """Tier 1: Compact 'New Mint Detected' embed — fires once when first seen."""
    BRAND = "Velcor3"
    embed = Embed(color=Color(0xF5A623))
    collection_name = m.name if m.name else "Unknown"
    chain_emoji = "⟠" if m.chain == "ethereum" else "◎"

    embed.set_author(name=f"⚡ {BRAND}  ·  New Mint  ·  {chain_emoji} {m.chain.capitalize()}")

    if m.image_url and isinstance(m.image_url, str) and m.image_url.startswith("http") and len(m.image_url) < 2000:
        embed.set_thumbnail(url=m.image_url)

    # ── Description ───────────────────────────────────────────────────────────
    desc = f"## {collection_name}\nFirst mints detected on-chain"
    if m.tracked_minters and m.tracked_minters > 0:
        desc += f"\n\n🚨 **{m.tracked_minters} tracked wallet(s)** already minting"
    embed.description = desc

    # ── Fields ────────────────────────────────────────────────────────────────
    embed.add_field(name="💰 Mint Price", value=m.mint_price or "Unknown", inline=True)
    embed.add_field(name="👥 Early Minters", value=str(m.unique_minters or m.recent_mints_count or "?"), inline=True)
    embed.add_field(name="⛓️ Chain", value=f"{chain_emoji} {m.chain.capitalize()}", inline=True)

    # ── Links ─────────────────────────────────────────────────────────────────
    link_parts: list = []
    if m.chain == "ethereum":
        if m.opensea_url: link_parts.append(f"[OpenSea]({m.opensea_url})")
        if m.etherscan_url: link_parts.append(f"[Etherscan]({m.etherscan_url})")
    else:
        if m.magiceden_url: link_parts.append(f"[Magic Eden]({m.magiceden_url})")
        if m.solscan_url: link_parts.append(f"[Solscan]({m.solscan_url})")
    if m.twitter_url: link_parts.append(f"[𝕏]({m.twitter_url})")
    if m.website_url: link_parts.append(f"[Website]({m.website_url})")
    if link_parts:
        embed.add_field(name="🔗 Links", value="  ·  ".join(link_parts), inline=False)

    from datetime import datetime
    embed.set_footer(text=f"{BRAND}  ·  Live Mint Radar  ·  {datetime.utcnow().strftime('%I:%M %p')} UTC")
    return embed

async def build_active_mint_embed(m: ActiveMint) -> Embed:
    """Tier 2: Full 'Trending / Hot Mint' embed — fires when velocity is high."""
    BRAND = "Velcor3"
    embed = Embed(color=Color(0x00C896))
    collection_name = m.name if m.name else "Unknown"
    chain_emoji = "⟠" if m.chain == "ethereum" else "◎"

    embed.set_author(name=f"⚡ {BRAND}  ·  Trending Mint  ·  {chain_emoji} {m.chain.capitalize()}")

    if m.image_url and isinstance(m.image_url, str) and m.image_url.startswith("http") and len(m.image_url) < 2000:
        embed.set_thumbnail(url=m.image_url)

    # ── Progress bar ──────────────────────────────────────────────────────────
    has_supply = m.total_supply and 0 < m.total_supply < 1_000_000_000
    if has_supply:
        pct = (m.minted_count / m.total_supply * 100) if m.total_supply else 0
        supply_val = f"{m.minted_count:,} / {m.total_supply:,}"
    else:
        supply_val = f"{m.minted_count:,}"
        pct = min(50.0, (m.minted_count / max(1, m.minted_count * 2)) * 100)

    filled = max(0, min(20, int((pct / 100) * 20)))
    bar_str = "█" * filled + "░" * (20 - filled)

    if pct >= 100 or (not has_supply and "ended" in str(m.mint_price).lower()):
        status = "🔴 Ended"
    elif pct >= 85:
        status = "🟡 Almost sold out"
    else:
        status = "🟢 Live"

    vol = m.recent_mints_count or 0

    # ── Description ───────────────────────────────────────────────────────────
    desc = f"## {collection_name}\n`{bar_str}` **{pct:.1f}%**  ·  {status}"
    if m.tracked_minters and m.tracked_minters > 0:
        desc += f"\n\n🚨 **{m.tracked_minters} tracked wallet(s)** minting"
    embed.description = desc

    # ── Fields grid ───────────────────────────────────────────────────────────
    embed.add_field(name="💰 Mint Price", value=m.mint_price or "Unknown", inline=True)
    embed.add_field(name="📦 Supply", value=supply_val, inline=True)
    embed.add_field(name="🔥 Recent Mints", value=str(vol), inline=True)

    # ── AI analysis (optional) ────────────────────────────────────────────────
    if m.website_url:
        analysis_text = ""
        if openai_client:
            try:
                prompt = (
                    f"Write a 1-sentence alpha research analysis on this NFT mint. "
                    f"Name: {collection_name}, Chain: {m.chain}, Recent Mints: {m.recent_mints_count}. "
                    f"Website: {m.website_url}."
                )
                if m.tracked_minters > 0:
                    prompt += f" {m.tracked_minters} known high-value wallets have minted."
                prompt += " Sound like an insider alpha group. Be brief."
                resp = await openai_client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=60,
                )
                analysis_text = resp.choices[0].message.content.strip()
            except Exception as e:
                print(f"[OpenAI] Error: {e}")

        if not analysis_text:
            if m.tracked_minters and m.tracked_minters > 0:
                analysis_text = f"Smart money signal: {m.tracked_minters} tracked whale(s) active on {collection_name}."
            elif vol > 15:
                analysis_text = f"High velocity detected — {vol} mints recently on {collection_name}."
            else:
                analysis_text = f"{collection_name} showing steady organic accumulation."

        embed.add_field(name="🧠 Alpha", value=f"_{analysis_text}_", inline=False)

    # ── Socials & links ───────────────────────────────────────────────────────
    link_parts: list = []
    if m.chain == "ethereum" or m.etherscan_url:
        if m.opensea_url: link_parts.append(f"[OpenSea]({m.opensea_url})")
        if m.blur_url: link_parts.append(f"[Blur]({m.blur_url})")
        if m.etherscan_url: link_parts.append(f"[Etherscan]({m.etherscan_url})")
    if m.magiceden_url: link_parts.append(f"[Magic Eden]({m.magiceden_url})")
    if m.tensor_url: link_parts.append(f"[Tensor]({m.tensor_url})")
    if m.solscan_url: link_parts.append(f"[Solscan]({m.solscan_url})")
    if m.twitter_url: link_parts.append(f"[𝕏]({m.twitter_url})")
    if m.discord_url: link_parts.append(f"[Discord]({m.discord_url})")
    if m.website_url: link_parts.append(f"[Website]({m.website_url})")
    if link_parts:
        embed.add_field(name="🔗 Links", value="  ·  ".join(link_parts), inline=False)

    now = datetime.utcnow()
    embed.set_footer(text=f"{BRAND}  ·  Live Mint Tracker  ·  {now.strftime('%I:%M %p')} UTC")
    return embed
