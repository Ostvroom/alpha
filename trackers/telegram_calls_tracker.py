from __future__ import annotations

import asyncio
import json
import os
import re
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import aiohttp
import discord
from discord import Color, Embed

import config
from trackers.kolfi_market_enrichment import enrich_solana_mint

try:
    import feed_events
except Exception:
    feed_events = None

# Persist last seen message ids to avoid reposts across restarts
from app_paths import DATA_DIR, ensure_dirs

ensure_dirs()
STATE_PATH = os.path.join(DATA_DIR, "telegram_calls_state.json")

DEX_TOKEN_URL = "https://api.dexscreener.com/tokens/v1/{chain}/{address}"
DEX_ICON_URL = "https://dd.dexscreener.com/ds-data/tokens/{chain}/{address}.png"

_BASE58 = r"[1-9A-HJ-NP-Za-km-z]"
# Solana mint (usually 32-44 base58 chars). Some callers paste pump.fun suffix like "...pump".
MINT_RE = re.compile(rf"(?<!{_BASE58})({_BASE58}{{32,44}})(pump)?(?!{_BASE58})")
EVM_RE = re.compile(r"(?i)\b0x[a-f0-9]{40}\b")
TICKER_RE = re.compile(r"(?im)^\s*\$?([A-Z0-9]{2,12})\s*$")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _format_compact_usd(v: Optional[float]) -> str:
    if v is None:
        return "—"
    try:
        x = float(v)
    except (TypeError, ValueError):
        return "—"
    if x >= 1e9:
        return f"${x/1e9:.2f}B"
    if x >= 1e6:
        return f"${x/1e6:.2f}M"
    if x >= 1e3:
        return f"${x/1e3:.1f}K"
    return f"${x:.0f}"


def _clamp_text(s: str, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[: max(0, n - 1)].rstrip() + "…"


def _normalize_source(s: str) -> str:
    """
    Accept: '@foo', 'foo', 't.me/foo', 'https://t.me/foo', 'https://t.me/s/foo'
    Return: 'foo'
    """
    s = (s or "").strip()
    if not s:
        return ""
    s = s.replace("https://", "").replace("http://", "")
    s = s.replace("t.me/s/", "").replace("t.me/", "")
    if s.startswith("@"):
        s = s[1:]
    return s.strip().strip("/")


def _pick_best_pair(pairs: list[dict]) -> Optional[dict]:
    best = None
    best_liq = -1.0
    for p in pairs:
        if not isinstance(p, dict):
            continue
        liq = _safe_float((p.get("liquidity") or {}).get("usd")) or 0.0
        if liq > best_liq:
            best_liq = liq
            best = p
    return best


def _pair_age_days(pair: dict) -> Optional[float]:
    pc = pair.get("pairCreatedAt")
    if pc is None:
        return None
    try:
        ts = float(pc)
        if ts > 1e12:
            ts /= 1000.0
        now = datetime.now(timezone.utc).timestamp()
        return max(0.0, (now - ts) / 86400.0)
    except (TypeError, ValueError):
        return None


async def fetch_dexscreener_token(
    session: aiohttp.ClientSession, chain: str, address: str
) -> dict[str, Any]:
    """
    Dexscreener token endpoint for any supported chain.
    Returns a normalized dict similar to kolfi_market_enrichment.fetch_dexscreener_solana output.
    """
    out: dict[str, Any] = {
        "ok": False,
        "chain": chain,
        "address": address,
        "liquidity_usd": None,
        "pair_age_days": None,
        "fdv_usd": None,
        "market_cap_usd": None,
        "price_usd": None,
        "price_change_h24_pct": None,
        "volume_h24_usd": None,
        "txns_h24": None,
        "dex_id": None,
        "pair_address": None,
        "pair_url": None,
        "quote_symbol": None,
        "base_symbol": None,
        "image_url": None,
    }
    if not chain or not address:
        return out
    url = DEX_TOKEN_URL.format(chain=chain.strip().lower(), address=address.strip())
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=12),
            headers={"Accept": "application/json"},
        ) as r:
            if r.status != 200:
                return out
            data = await r.json()
    except Exception:
        return out
    if not isinstance(data, list) or not data:
        return out
    pair = _pick_best_pair(data)
    if not pair:
        return out
    out["ok"] = True
    liq = pair.get("liquidity") or {}
    out["liquidity_usd"] = _safe_float(liq.get("usd"))
    out["fdv_usd"] = _safe_float(pair.get("fdv"))
    out["market_cap_usd"] = _safe_float(pair.get("marketCap")) or _safe_float(pair.get("fdv"))
    out["price_usd"] = _safe_float(pair.get("priceUsd"))
    pc = pair.get("priceChange") or {}
    if isinstance(pc, dict):
        out["price_change_h24_pct"] = _safe_float(pc.get("h24"))
    out["volume_h24_usd"] = _safe_float((pair.get("volume") or {}).get("h24"))
    tx = pair.get("txns") or {}
    h24 = tx.get("h24") or {}
    bu = h24.get("buys")
    se = h24.get("sells")
    if bu is not None or se is not None:
        out["txns_h24"] = {"buys": bu, "sells": se}
    out["dex_id"] = pair.get("dexId")
    out["pair_address"] = pair.get("pairAddress")
    out["pair_url"] = pair.get("url")
    out["pair_age_days"] = _pair_age_days(pair)
    qt = pair.get("quoteToken") or {}
    bt = pair.get("baseToken") or {}
    out["quote_symbol"] = qt.get("symbol")
    out["base_symbol"] = bt.get("symbol")
    out["base_name"] = bt.get("name")
    info = pair.get("info") or {}
    if isinstance(info, dict):
        iu = info.get("imageUrl") or info.get("imageURL") or info.get("icon") or info.get("logo")
        if isinstance(iu, str) and iu.strip().startswith("http"):
            out["image_url"] = iu.strip()
    return out


def _normalize_chain_list(chains: list[str]) -> list[str]:
    """
    Allow friendly names; normalize to Dexscreener chain slugs.
    """
    m = {
        "eth": "ethereum",
        "ethereum": "ethereum",
        "mainnet": "ethereum",
        "base": "base",
        "sol": "solana",
        "solana": "solana",
        "bnb": "bsc",
        "bsc": "bsc",
        "binance": "bsc",
        "binance smart chain": "bsc",
    }
    out: list[str] = []
    for c in chains or []:
        k = (c or "").strip().lower()
        if not k:
            continue
        out.append(m.get(k, k))
    # de-dup preserving order
    seen = set()
    final = []
    for c in out:
        if c in seen:
            continue
        seen.add(c)
        final.append(c)
    return final


async def resolve_token_icon_url(
    session: aiohttp.ClientSession, *, chain: str, address: str
) -> Optional[str]:
    c = (chain or "").strip().lower()
    a = (address or "").strip()
    if not c or not a:
        return None
    url = DEX_ICON_URL.format(chain=c, address=a)
    try:
        async with session.head(
            url,
            headers={"Accept": "*/*"},
            timeout=aiohttp.ClientTimeout(total=6),
        ) as r:
            if r.status == 200:
                ct = (r.headers.get("Content-Type") or "").lower()
                if "image" in ct:
                    return url
    except Exception:
        return None
    return None


def _extract_mint_and_ticker(text: str) -> Tuple[Optional[str], Optional[str], Optional[str], bool]:
    if not text:
        return None, None, None, False

    pump_hint = "pump.fun" in text.lower()

    # EVM check FIRST — 0x addresses look like valid base58 to MINT_RE so must be guarded
    em = EVM_RE.search(text)
    evm = (em.group(0) or "").strip() if em else None

    # Solana mint — only if no EVM address found in the same message
    mint = None
    if not evm:
        m = MINT_RE.search(text)
        if m:
            mint = (m.group(1) or "").strip()
            if m.group(2):
                pump_hint = True

    # Ticker: first matching ALL-CAPS line in the first 6 lines
    ticker = None
    for line in (text.splitlines()[:6] if text else []):
        mm = TICKER_RE.match(line or "")
        if mm:
            ticker = (mm.group(1) or "").strip()
            break

    if evm:
        return evm, ticker, "evm", False
    if mint:
        return mint, ticker, "solana", pump_hint
    return None, ticker, None, False


def _load_state() -> Dict[str, Any]:
    if not os.path.isfile(STATE_PATH):
        return {"version": 1, "by_chat": {}, "updated_at": _now_iso()}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if isinstance(raw, dict) and raw.get("version") == 1 and isinstance(raw.get("by_chat"), dict):
            return raw
    except Exception:
        pass
    return {"version": 1, "by_chat": {}, "updated_at": _now_iso()}


def _save_state(state: Dict[str, Any]) -> None:
    try:
        state["version"] = 1
        state["updated_at"] = _now_iso()
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
    except Exception:
        return


@dataclass
class TgCall:
    source: str
    chat_id: int
    message_id: int
    date_iso: str
    text: str
    mint: Optional[str]
    ticker: Optional[str]
    chain_hint: Optional[str]
    pump_hint: bool
    link: Optional[str]


def _make_public_message_link(source_username: str, msg_id: int) -> Optional[str]:
    u = _normalize_source(source_username)
    if not u or not msg_id:
        return None
    return f"https://t.me/{u}/{int(msg_id)}"


async def _build_embed_for_call(session: aiohttp.ClientSession, call: TgCall) -> Embed:
    brand = "Velcor3"
    chain_color = 0x202025  # default — overridden after chain detection
    embed = Embed(color=Color(chain_color), timestamp=datetime.now(timezone.utc))

    # Enrich with Dexscreener (Solana/EVM) — use fetch_dexscreener_token for all chains
    # so base_symbol is always populated correctly.
    dex: Optional[Dict[str, Any]] = None
    detected_chain: Optional[str] = None
    chains = _normalize_chain_list(list(getattr(config, "TELEGRAM_CALLS_CHAINS", []) or []))
    if not chains:
        chains = ["solana", "bsc", "base", "ethereum"]

    if call.mint and call.chain_hint == "solana":
        d = await fetch_dexscreener_token(session, "solana", call.mint)
        if isinstance(d, dict) and d.get("ok"):
            dex = d
            detected_chain = "solana"
        else:
            # Fallback: try birdeye enrichment for extra price data
            try:
                enr = await enrich_solana_mint(session, call.mint, birdeye_api_key=config.BIRDEYE_API_KEY or "")
                raw_dex = (enr.get("dexscreener") or {}) if isinstance(enr, dict) else {}
                # Only accept if it has meaningful data
                if raw_dex and raw_dex.get("price_usd") is not None:
                    dex = raw_dex
                    detected_chain = "solana"
            except Exception:
                pass
        if not detected_chain:
            detected_chain = "solana"
    elif call.mint and call.chain_hint == "evm":
        # Always probe the full EVM chain list so chain is auto-detected correctly.
        # Merge config chains with the hardcoded fallback list, preserving order.
        evm_probe = [ch for ch in chains if ch != "solana"]
        for must_have in ("bsc", "base", "ethereum"):
            if must_have not in evm_probe:
                evm_probe.append(must_have)
        for ch in evm_probe:
            d = await fetch_dexscreener_token(session, ch, call.mint)
            if isinstance(d, dict) and d.get("ok"):
                dex = d
                detected_chain = ch
                break
        # detected_chain stays None if no chain has a live pair — links will still render

    dex_ok = isinstance(dex, dict) and dex.get("ok")

    # Title: prefer base token name from Dex, else ticker from message, else generic
    title = None
    base_name = None
    if dex_ok:
        title = dex.get("base_symbol")
        base_name = dex.get("base_name") or dex.get("base_symbol")
    if not title:
        title = call.ticker
    if not title:
        title = "New Token Alert"

    # Chain label for embed
    chain_label = ""
    if detected_chain == "solana":
        chain_label = "Solana"
    elif detected_chain == "base":
        chain_label = "Base"
    elif detected_chain == "ethereum":
        chain_label = "Ethereum"
    elif detected_chain == "bsc":
        chain_label = "BNB Chain"
    elif detected_chain:
        chain_label = detected_chain.capitalize()

    # Dynamic color: green=Solana, gold=BNB, blurple=EVM
    if detected_chain == "solana":
        chain_color = 0x14F195
    elif detected_chain == "bsc":
        chain_color = 0xF3BA2F  # BNB yellow
    elif call.chain_hint == "evm" or detected_chain in ("base", "ethereum"):
        chain_color = 0x5865F2
    embed.color = Color(chain_color)

    embed.set_author(
        name=f"⚡ {brand}  ·  New Token Alert" + (f"  ·  {chain_label}" if chain_label else ""),
        url=call.link or None,
    )

    # Token images: use Dex image when available; fall back to Dex CDN icon.
    thumb: Optional[str] = None
    banner: Optional[str] = None
    if dex_ok and isinstance(dex.get("image_url"), str) and str(dex.get("image_url")).startswith("http"):
        thumb = str(dex.get("image_url"))
        banner = thumb
    if not thumb and call.mint:
        icon_chain = detected_chain or ("solana" if call.chain_hint == "solana" else "base")
        thumb = await resolve_token_icon_url(session, chain=icon_chain, address=call.mint)
        banner = thumb

    if thumb:
        embed.set_thumbnail(url=thumb)
    if banner:
        embed.set_image(url=banner)

    # Description: token name/symbol, mint address, raw text excerpt
    heading = f"## {title}"
    if base_name and base_name != title:
        heading += f"  ·  {base_name}"
    desc_bits = [heading]
    if call.mint:
        desc_bits.append(f"`{call.mint}`")
    if config.TELEGRAM_CALLS_INCLUDE_RAW_TEXT:
        desc_bits.append(_clamp_text(call.text, int(getattr(config, "TELEGRAM_CALLS_MAX_TEXT_CHARS", 900))))
    embed.description = "\n\n".join([b for b in desc_bits if b])[:4096]

    # Snapshot: keep ONLY MC at call (per request)
    if dex_ok:
        mc = _safe_float(dex.get("market_cap_usd"))
        embed.add_field(name="Snapshot", value=f"**MC at call** {_format_compact_usd(mc)}", inline=False)
    elif call.mint:
        embed.add_field(name="Snapshot", value="**MC at call** —", inline=False)

    # Links: always show relevant explorers even if Dex has no pair
    link_parts = []
    if call.mint:
        mint = call.mint
        if call.chain_hint == "solana" or detected_chain == "solana":
            if call.pump_hint or "pump" in (call.text or "").lower():
                link_parts.append(f"[Pump.fun](https://pump.fun/{mint})")
            link_parts.append(f"[GMGN](https://gmgn.ai/sol/token/{mint})")
            link_parts.append(f"[Dex](https://dexscreener.com/solana/{mint})")
        elif call.chain_hint == "evm" or detected_chain in ("base", "ethereum", "bsc"):
            if detected_chain == "bsc":
                link_parts.append(f"[BscScan](https://bscscan.com/address/{mint})")
                link_parts.append(f"[Dex](https://dexscreener.com/bsc/{mint})")
            elif detected_chain == "base":
                link_parts.append(f"[Basescan](https://basescan.org/address/{mint})")
                link_parts.append(f"[Dex](https://dexscreener.com/base/{mint})")
            else:
                link_parts.append(f"[Etherscan](https://etherscan.io/address/{mint})")
                link_parts.append(f"[Dex](https://dexscreener.com/ethereum/{mint})")
    # If Dex returned a specific pair URL, prefer that over generic search
    if dex_ok and dex.get("pair_url"):
        for i, lp in enumerate(link_parts):
            if lp.startswith("[Dex]"):
                link_parts[i] = f"[Dex]({dex.get('pair_url')})"
                break
    if link_parts:
        embed.add_field(name="Links", value=" · ".join(link_parts)[:1024], inline=False)

    embed.set_footer(text=f"{brand}  ·  source {call.source}  ·  DYOR")
    return embed


async def run_telegram_calls_bridge(discord_client: discord.Client) -> None:
    """
    Listen to Telegram public channels/groups and forward call-like posts to Discord.
    Requires TELEGRAM_API_ID/TELEGRAM_API_HASH/TELEGRAM_SESSION.
    """
    try:
        if not getattr(config, "ENABLE_TELEGRAM_CALLS", False):
            return
        if not getattr(config, "TELEGRAM_CALLS_SOURCES", None):
            print("[TelegramCalls] No TELEGRAM_CALLS_SOURCES configured.")
            return
        if not getattr(config, "TELEGRAM_CALLS_DISCORD_CHANNEL_ID", 0):
            print("[TelegramCalls] Missing TELEGRAM_CALLS_DISCORD_CHANNEL_ID.")
            return

        api_id = (getattr(config, "TELEGRAM_API_ID", "") or "").strip()
        api_hash = (getattr(config, "TELEGRAM_API_HASH", "") or "").strip()
        sess = (getattr(config, "TELEGRAM_SESSION", "") or "").strip()
        if not api_id.isdigit() or not api_hash or not sess:
            print("[TelegramCalls] Missing TELEGRAM_API_ID / TELEGRAM_API_HASH / TELEGRAM_SESSION — bridge not started.")
            return

        try:
            from telethon import TelegramClient, events
            from telethon.sessions import StringSession
        except Exception as e:
            print(f"[TelegramCalls] Telethon import failed: {e}")
            return

        state = _load_state()
        by_chat: Dict[str, Any] = state.setdefault("by_chat", {})
        dedup_sec = int(getattr(config, "TELEGRAM_CALLS_DEDUP_SECONDS", 180))
        recent: Dict[str, float] = {}  # key -> unix ts

        # Pre-resolve Discord channel once
        dst_id = int(getattr(config, "TELEGRAM_CALLS_DISCORD_CHANNEL_ID", 0) or 0)
        try:
            dst = discord_client.get_channel(dst_id) or await discord_client.fetch_channel(dst_id)
        except Exception as e:
            dst = None
            print(f"[TelegramCalls] Discord channel resolve failed ({dst_id}): {e}")
        if not dst:
            print(f"[TelegramCalls] Discord channel {dst_id} not found / no access.")
            return

        sources_raw = (getattr(config, "TELEGRAM_CALLS_SOURCES", []) or [])
        sources = [_normalize_source(s) for s in sources_raw]
        sources = [s for s in sources if s]
        if not sources:
            print(f"[TelegramCalls] TELEGRAM_CALLS_SOURCES empty after normalization: {sources_raw!r}")
            return

        async with aiohttp.ClientSession() as http:
            tg = TelegramClient(StringSession(sess), int(api_id), api_hash)
            print(f"[TelegramCalls] Connecting Telegram (sources={len(sources)})…")
            await tg.connect()
            if not await tg.is_user_authorized():
                print("[TelegramCalls] Session not authorized. Regenerate TELEGRAM_SESSION with telegram_session.py")
                await tg.disconnect()
                return

            # Resolve sources to entities and subscribe
            entities = []
            for s in sources:
                try:
                    ent = await tg.get_entity(s)
                    entities.append(ent)
                except Exception as e:
                    print(f"[TelegramCalls] Could not resolve '{s}': {e}")

            if not entities:
                print("[TelegramCalls] No Telegram sources resolved — bridge not started.")
                await tg.disconnect()
                return

            print(f"[TelegramCalls] Listening to {len(entities)} source(s) → Discord {dst_id}")

            async def _handle_event(event, *, edited: bool = False) -> None:
                try:
                    msg = event.message
                    if not msg:
                        return
                    text = (msg.message or msg.raw_text or "").strip()
                    if not text:
                        return

                    chat = await event.get_chat()
                    src_username = getattr(chat, "username", "") or ""
                    src_title = getattr(chat, "title", "") or src_username or "telegram"
                    chat_id = int(getattr(event, "chat_id", 0) or 0)
                    message_id = int(getattr(msg, "id", 0) or 0)

                    # Basic state-based dedup (monotonic per chat)
                    prev = by_chat.get(str(chat_id)) or {}
                    last_id = int(prev.get("last_id", 0) or 0)
                    if not edited and message_id and last_id and message_id <= last_id:
                        return

                    mint, ticker, chain_hint, pump_hint = _extract_mint_and_ticker(text)
                    # Only forward if message contains an actual token contract (skip ticker-only)
                    if not mint:
                        return

                    # Time-based dedup for identical (chat + mint/ticker) bursts
                    k = f"{chat_id}|{message_id}|{mint or ''}|{ticker or ''}|{'e' if edited else 'n'}"
                    now = time.time()
                    if dedup_sec > 0:
                        t0 = recent.get(k)
                        if t0 and now - t0 < dedup_sec:
                            # still advance last_id so we don't get stuck
                            by_chat[str(chat_id)] = {"last_id": message_id, "last_posted_at": _now_iso()}
                            _save_state(state)
                            return
                    recent[k] = now

                    link = _make_public_message_link(src_username, message_id)
                    call = TgCall(
                        source=src_title,
                        chat_id=chat_id,
                        message_id=message_id,
                        date_iso=_now_iso(),
                        text=text,
                        mint=mint,
                        ticker=ticker,
                        chain_hint=chain_hint,
                        pump_hint=bool(pump_hint),
                        link=link,
                    )
                    emb = await _build_embed_for_call(http, call)
                    await dst.send(embed=emb)
                    if feed_events is not None:
                        try:
                            thumb = ""
                            try:
                                thumb = str(getattr(getattr(emb, "thumbnail", None), "url", "") or "")
                            except Exception:
                                thumb = ""
                            feed_events.add_event(
                                kind="telegram_call",
                                guild_id=int(getattr(getattr(dst, "guild", None), "id", 0) or 0),
                                channel_id=int(getattr(dst, "id", 0) or 0),
                                title=f"{call.ticker or 'Token'} · {call.chain_hint or 'telegram'}",
                                body=(call.text or "")[:1500],
                                url=call.link or "",
                                extra={
                                    "source": call.source,
                                    "mint": call.mint,
                                    "ticker": call.ticker,
                                    "chain": call.chain_hint,
                                    "thumb_url": thumb,
                                },
                            )
                        except Exception:
                            pass

                    by_chat[str(chat_id)] = {"last_id": message_id, "last_posted_at": _now_iso()}
                    _save_state(state)
                except Exception as e:
                    print(f"[TelegramCalls] Handler error: {e}")
                    print(traceback.format_exc())

            @tg.on(events.NewMessage(chats=entities))
            async def _on_new_message(event):  # type: ignore
                await _handle_event(event, edited=False)

            # Some channels "update" calls by editing a message instead of sending new ones.
            @tg.on(events.MessageEdited(chats=entities))
            async def _on_edited_message(event):  # type: ignore
                await _handle_event(event, edited=True)

            try:
                await tg.run_until_disconnected()
            finally:
                try:
                    await tg.disconnect()
                except Exception:
                    pass
    except Exception as e:
        print(f"[TelegramCalls] Bridge crashed: {e}")
        print(traceback.format_exc())

