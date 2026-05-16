import discord
from typing import List, Dict, Any
from datetime import datetime, timezone


def _eth_value(value: Any) -> str:
    try:
        v = float(value) if value is not None else 0
        if v == 0:
            return "0 Ξ"
        if v < 0.0001:
            return f"{v:.8f} Ξ"
        if v < 0.01:
            return f"{v:.6f} Ξ"
        return f"{v:.4f} Ξ"
    except (ValueError, TypeError):
        return f"{value} Ξ"


def _safe_int(value: Any) -> str:
    try:
        return f"{int(float(value)):,}"
    except (ValueError, TypeError):
        return str(value)


def _square_thumbnail(image_url: str) -> str:
    """Return a square-cropped thumbnail URL. Uses a proxy for external images."""
    if not image_url:
        return ""
    if image_url.startswith("attachment://"):
        return image_url
    # wsrv.nl is a free image proxy that crops to square
    return f"https://wsrv.nl/?url={image_url}&w=256&h=256&fit=cover&output=png"


def _safe_str(value: Any, default: str = "N/A", max_len: int = 0) -> str:
    if value is None or value == "":
        return default
    s = str(value)
    if max_len > 0 and len(s) > max_len:
        return s[:max_len-3] + "..."
    return s


def _build_progress_bar(current: int, total: int, length: int = 10) -> str:
    try:
        pct = min(100, int((current / total) * 100)) if total > 0 else 0
        filled = int((pct / 100) * length)
        bar = "█" * filled + "░" * (length - filled)
        return f"{bar}  {pct}%"
    except:
        return "░░░░░░░░░░  0%"


# ═══════════════════════════════════════════════════════════════════════════════
# LIVE MINT EMBEDS
# ═══════════════════════════════════════════════════════════════════════════════

def build_live_mint_embeds(collections: List[Dict]) -> List[discord.Embed]:
    """Build one rich embed per live mint — luxury card style."""
    embeds = []
    for col in collections[:10]:
        name = _safe_str(col.get("contract_name") or col.get("name"), "", max_len=200)
        contract_addr = col.get("contract_address", "").lower()
        token_id = _safe_str(col.get("token_id", "?"))
        cost_eth = _eth_value(col.get("mint_cost") or col.get("trade_price") or col.get("price", 0))
        gas_eth = _eth_value(col.get("gas_fee", 0))
        cost_usd = col.get("mint_cost_usd", 0)
        gas_usd = col.get("gas_fee_usd", 0)
        try:
            gas_used = int(float(col.get("gas_used", 0) or 0))
        except (ValueError, TypeError):
            gas_used = 0
        time_ago = _safe_str(col.get("time_ago", "Just now"))
        tx_hash = col.get("tx_hash", "") or col.get("hash", "")
        mint_type = col.get("type", "")
        block_num = col.get("block_number", "")
        to_addr = col.get("to", "")
        image_url = col.get("image_url", "")
        if image_url == "https://www.nftscan.com/images/og-img/home.png":
            image_url = ""
        etherscan_url = col.get("etherscan_url", "")
        opensea_url = col.get("opensea_url", "")
        social_links = col.get("social_links", {})
        total_supply = col.get("total_supply")
        max_supply = col.get("max_supply")

        if not name and contract_addr:
            name = f"{contract_addr[:6]}...{contract_addr[-4:]}"
        elif not name:
            name = "Unknown Collection"

        display_token_id = token_id
        if len(str(token_id)) > 12:
            display_token_id = f"{str(token_id)[:8]}...{str(token_id)[-4:]}"

        amount = col.get("amount", 1)
        if mint_type == "erc1155_batch":
            title = f"{name} (Batch)"
        elif mint_type == "erc1155" and int(str(amount)) > 1:
            title = f"{name} #{display_token_id} × {amount}"
        else:
            title = f"{name} #{display_token_id}"

        url = opensea_url or etherscan_url or "https://opensea.io"

        # Color logic
        try:
            cost_val = float(col.get("mint_cost", 0) or col.get("trade_price", 0) or 0)
            gas_val = float(col.get("gas_fee", 0) or 0)
            if cost_val == 0:
                color = 0x2ECC71
            elif gas_val > 0.01:
                color = 0x9B59B6
            else:
                color = 0xE74C3C
        except:
            color = 0xE74C3C

        # Build description
        lines = []
        meta_parts = []
        if block_num:
            meta_parts.append(f"⬛ `{block_num}`")
        if to_addr and len(to_addr) >= 42:
            meta_parts.append(f"👤 `{to_addr[:6]}...{to_addr[-4:]}`")
        if time_ago != "Just now":
            meta_parts.append(f"🕒 {time_ago}")
        if meta_parts:
            lines.append("  ·  ".join(meta_parts))
            lines.append("")

        cost_display = f"${cost_usd:,.2f}" if cost_usd else cost_eth
        gas_display = f"${gas_usd:,.4f}" if gas_usd else gas_eth
        lines.append(f"💰 **{cost_display}**          ⛽ **{gas_display}**")
        lines.append("")

        if total_supply is not None:
            if max_supply and max_supply > 0:
                bar = _build_progress_bar(total_supply, max_supply)
                lines.append(f"📦 **{total_supply:,}** / **{max_supply:,}**    `{bar}`")
            else:
                lines.append(f"📦 **{total_supply:,}** minted")
            lines.append("")
        else:
            lines.append("📦 Minting active")
            lines.append("")

        description = "\n".join(lines).strip() or None

        embed = discord.Embed(
            title=title,
            url=url,
            description=description,
            color=color,
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_author(
            name="⚡  Live Mint",
            icon_url="https://www.nftscan.com/favicon.png"
        )

        links = []
        # Mint link priority: website → OpenSea collection → Etherscan contract
        if social_links.get("website"):
            links.append(f"[Mint ↗]({social_links['website']})")
        elif opensea_url and "/collection/" in opensea_url:
            links.append(f"[Mint ↗]({opensea_url})")
        elif contract_addr:
            links.append(f"[Contract ↗](https://etherscan.io/address/{contract_addr})")
        if opensea_url:
            links.append(f"[OpenSea]({opensea_url})")
        if tx_hash:
            links.append(f"[Tx ↗](https://etherscan.io/tx/{tx_hash})")
        if social_links.get("twitter"):
            links.append(f"[Twitter]({social_links['twitter']})")
        if social_links.get("discord"):
            links.append(f"[Discord]({social_links['discord']})")
        links.append("[Made by sultan](https://x.com/Degensultan)")

        if links:
            embed.add_field(name="", value=" · ".join(links), inline=False)

        from trackers.collection_image import COLLECTION_FALLBACK_FILENAME

        thumb = _square_thumbnail(image_url) or f"attachment://{COLLECTION_FALLBACK_FILENAME}"
        embed.set_thumbnail(url=thumb)
        embed.set_footer(text="On-chain mint")
        embeds.append(embed)

    return embeds


# ═══════════════════════════════════════════════════════════════════════════════
# WHALE WATCH EMBEDS
# ═══════════════════════════════════════════════════════════════════════════════

def build_whale_embeds(activities: List[Dict]) -> List[discord.Embed]:
    """Build wallet tracker embeds — standard layout matching live mints.
    Shows mints, buys, and sells for tracked wallets."""
    embeds = []
    for act in activities[:10]:
        name = _safe_str(act.get("contract_name") or act.get("name"), "", max_len=200)
        contract_addr = act.get("contract_address", "").lower()
        token_id = _safe_str(act.get("token_id", "?"))
        to_addr = act.get("to", "").lower()
        from_addr = act.get("from", "").lower()
        tx_hash = act.get("tx_hash", "")
        event_type = act.get("type", "wallet_buy")
        image_url = act.get("image_url", "")
        if image_url == "https://www.nftscan.com/images/og-img/home.png":
            image_url = ""
        opensea_url = act.get("opensea_url", "")
        social_links = act.get("social_links", {})
        cost_eth = _eth_value(act.get("mint_cost") or act.get("trade_price") or act.get("price", 0))
        cost_usd = act.get("mint_cost_usd", 0)
        gas_usd = act.get("gas_fee_usd", 0)
        total_supply = act.get("total_supply")
        max_supply = act.get("max_supply")

        # Wallet metadata
        whale_label = act.get("whale_label")
        whale_x_url = act.get("whale_x_url")
        total_amount = act.get("amount", 1)

        # Determine wallet address to display and direction
        if event_type == "wallet_sell":
            wallet_addr = from_addr
            direction = "📤 Sell"
            color = 0xE74C3C  # red for sell
        elif event_type == "wallet_mint":
            wallet_addr = to_addr
            direction = "🌱 Mint"
            color = 0x00D084  # green for mint
        else:  # wallet_buy
            wallet_addr = to_addr
            direction = "📥 Buy"
            color = 0x3498DB  # blue for buy

        # Fallback name
        if not name and contract_addr:
            name = f"{contract_addr[:6]}...{contract_addr[-4:]}"
        elif not name:
            name = "Unknown"

        url = opensea_url or (f"https://etherscan.io/address/{contract_addr}" if contract_addr else "https://opensea.io")

        # Build description — same structure as live mints
        lines = []

        # Wallet line (clickable to Etherscan)
        wallet_display = whale_label if whale_label else f"{wallet_addr[:6]}...{wallet_addr[-4:]}"
        lines.append(f"👤 [{wallet_display}](https://etherscan.io/address/{wallet_addr})")
        lines.append("")

        # Direction + amount
        if total_amount > 1:
            lines.append(f"{direction} — **{total_amount}** NFTs")
        else:
            lines.append(f"{direction} — #{token_id}")
        lines.append("")

        # Price line
        cost_display = f"${cost_usd:,.2f}" if cost_usd else cost_eth
        gas_display = f"${gas_usd:,.4f}" if gas_usd else ""
        price_line = f"💰 **{cost_display}**"
        if gas_display:
            price_line += f"          ⛽ **{gas_display}**"
        lines.append(price_line)
        lines.append("")

        # Progress line — always show something
        if total_supply is not None:
            if max_supply and max_supply > 0:
                bar = _build_progress_bar(total_supply, max_supply)
                lines.append(f"📦 **{total_supply:,}** / **{max_supply:,}**    `{bar}`")
            else:
                lines.append(f"📦 **{total_supply:,}** minted")
        else:
            lines.append("📦 Minting active")
        lines.append("")

        description = "\n".join(lines).strip()

        embed = discord.Embed(
            title=name,
            url=url,
            description=description,
            color=color,
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_author(name="👛  Wallet Tracker", icon_url="https://www.nftscan.com/favicon.png")

        links = []
        if social_links.get("website"):
            links.append(f"[Mint ↗]({social_links['website']})")
        elif opensea_url and "/collection/" in opensea_url:
            links.append(f"[Mint ↗]({opensea_url})")
        elif contract_addr:
            links.append(f"[Contract ↗](https://etherscan.io/address/{contract_addr})")
        if opensea_url:
            links.append(f"[OpenSea]({opensea_url})")
        if tx_hash:
            links.append(f"[Tx ↗](https://etherscan.io/tx/{tx_hash})")
        if whale_x_url:
            links.append(f"[Wallet X]({whale_x_url})")
        links.append("[Made by sultan](https://x.com/Degensultan)")

        if links:
            embed.add_field(name="", value=" · ".join(links), inline=False)

        from trackers.collection_image import COLLECTION_FALLBACK_FILENAME

        thumb = _square_thumbnail(image_url) or f"attachment://{COLLECTION_FALLBACK_FILENAME}"
        embed.set_thumbnail(url=thumb)
        embed.set_footer(text="Wallet Tracker · Ethereum")
        embeds.append(embed)

    return embeds


# ═══════════════════════════════════════════════════════════════════════════════
# HOT MINT ALERT EMBED
# ═══════════════════════════════════════════════════════════════════════════════

def build_hot_mint_embed(mint: Dict, count: int, window_seconds: int) -> discord.Embed:
    """Build a hot mint alert embed — same layout as live mints for consistency."""
    name = _safe_str(mint.get("contract_name") or mint.get("name"), "Unknown", max_len=200)
    contract_addr = mint.get("contract_address", "").lower()
    token_id = _safe_str(mint.get("token_id", "?"))
    cost_eth = _eth_value(mint.get("mint_cost") or mint.get("trade_price") or mint.get("price", 0))
    gas_eth = _eth_value(mint.get("gas_fee", 0))
    cost_usd = mint.get("mint_cost_usd", 0)
    gas_usd = mint.get("gas_fee_usd", 0)
    tx_hash = mint.get("tx_hash", "") or mint.get("hash", "")
    image_url = mint.get("image_url", "")
    if image_url == "https://www.nftscan.com/images/og-img/home.png":
        image_url = ""
    opensea_url = mint.get("opensea_url", "")
    etherscan_url = mint.get("etherscan_url", "")
    social_links = mint.get("social_links", {})
    total_supply = mint.get("total_supply")
    max_supply = mint.get("max_supply")

    if not name and contract_addr:
        name = f"{contract_addr[:6]}...{contract_addr[-4:]}"

    url = opensea_url or etherscan_url or "https://opensea.io"
    window_min = window_seconds // 60

    # Build description — same structure as live mints
    lines = []
    lines.append(f"🔥 **{count}** mints in the last **{window_min}m** — trending!")
    lines.append("")

    cost_display = f"${cost_usd:,.2f}" if cost_usd else cost_eth
    gas_display = f"${gas_usd:,.4f}" if gas_usd else gas_eth
    lines.append(f"💰 **{cost_display}**          ⛽ **{gas_display}**")
    lines.append("")

    # Progress line — always show something
    if total_supply is not None:
        if max_supply and max_supply > 0:
            bar = _build_progress_bar(total_supply, max_supply)
            lines.append(f"📦 **{total_supply:,}** / **{max_supply:,}**    `{bar}`")
        else:
            lines.append(f"📦 **{total_supply:,}** minted")
    else:
        lines.append("📦 Minting active")
    lines.append("")

    description = "\n".join(lines).strip()

    embed = discord.Embed(
        title=f"🔥  {name}",
        url=url,
        description=description,
        color=0xFF4500,
        timestamp=datetime.now(timezone.utc)
    )
    embed.set_author(
        name="🔥  Hot Mint Alert",
        icon_url="https://www.nftscan.com/favicon.png"
    )

    links = []
    if social_links.get("website"):
        links.append(f"[Mint ↗]({social_links['website']})")
    elif opensea_url and "/collection/" in opensea_url:
        links.append(f"[Mint ↗]({opensea_url})")
    elif contract_addr:
        links.append(f"[Contract ↗](https://etherscan.io/address/{contract_addr})")
    if opensea_url:
        links.append(f"[OpenSea]({opensea_url})")
    if tx_hash:
        links.append(f"[Tx ↗](https://etherscan.io/tx/{tx_hash})")
    if social_links.get("twitter"):
        links.append(f"[Twitter]({social_links['twitter']})")
    if social_links.get("discord"):
        links.append(f"[Discord]({social_links['discord']})")
    links.append("[Made by sultan](https://x.com/Degensultan)")

    if links:
        embed.add_field(name="", value=" · ".join(links), inline=False)

    from trackers.collection_image import COLLECTION_FALLBACK_FILENAME

    thumb = _square_thumbnail(image_url) or f"attachment://{COLLECTION_FALLBACK_FILENAME}"
    embed.set_thumbnail(url=thumb)
    embed.set_footer(text="Hot Mint Detection · Ethereum")
    return embed
