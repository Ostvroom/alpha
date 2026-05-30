"""
Daily Velcor Performance Recap — one Discord message with sections (24h outcomes).
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import discord
from discord import Color, Embed

import alert_snapshots
import config
import database
import feed_events
from app_paths import DATA_DIR, ensure_dirs
from brand_assets import brand_name

logger = logging.getLogger(__name__)

ensure_dirs()
_STATE_PATH = Path(DATA_DIR) / "performance_recap_state.json"
_post_lock = asyncio.Lock()


def _fmt_followers(n: Optional[int]) -> str:
    if n is None:
        return "—"
    try:
        v = int(n)
    except (TypeError, ValueError):
        return "—"
    if v >= 1_000_000:
        return f"{v / 1_000_000:.1f}M".replace(".0M", "M")
    if v >= 1_000:
        return f"{v / 1_000:.1f}K".replace(".0K", "K")
    return str(v)


def _fmt_delta(n: Optional[int], *, pct: Optional[float] = None) -> str:
    if n is None:
        return "—"
    try:
        v = int(n)
    except (TypeError, ValueError):
        return "—"
    sign = "+" if v >= 0 else ""
    base = f"{sign}{v:,}"
    if pct is not None and abs(pct) >= 1:
        base += f" ({sign}{pct:.0f}%)"
    return base


def _classify_project(delta: Optional[int], base: Optional[int]) -> Tuple[str, str]:
    if delta is None or base is None or base <= 0:
        return "flat", "No follower baseline"
    pct = 100.0 * delta / base
    if delta >= 500 or pct >= 15:
        return "winner", f"{_fmt_delta(delta, pct=pct)} followers"
    if delta >= 50 or pct >= 3:
        return "flat", f"{_fmt_delta(delta, pct=pct)} followers"
    return "miss", f"{_fmt_delta(delta, pct=pct)} followers"


def _classify_mint(supply_delta: Optional[int], supply_at: Optional[int], detail: str) -> Tuple[str, str]:
    if supply_at and supply_at > 0 and supply_delta is not None:
        pct = 100.0 * supply_delta / supply_at
        if supply_delta >= 20 or pct >= 25:
            return "winner", detail or f"supply {_fmt_delta(supply_delta, pct=pct)}"
        if supply_delta >= 5 or pct >= 5:
            return "flat", detail or f"supply {_fmt_delta(supply_delta, pct=pct)}"
    if detail and "hot" in detail.lower():
        return "winner", detail
    return "flat", detail or "minimal mint activity"


def _classify_token(mc_delta_pct: Optional[float]) -> Tuple[str, str]:
    if mc_delta_pct is None:
        return "flat", "MC change unknown"
    if mc_delta_pct >= 50:
        return "winner", f"MC {mc_delta_pct:+.0f}% since alert"
    if mc_delta_pct >= 10:
        return "flat", f"MC {mc_delta_pct:+.0f}% since alert"
    return "miss", f"MC {mc_delta_pct:+.0f}% since alert"


async def measure_pending_snapshots(bot, twitter_client=None) -> int:
    """Refresh metrics for alerts older than 24h; returns count measured."""
    pending = alert_snapshots.list_pending_measurement(
        min_age_hours=float(getattr(config, "PERFORMANCE_RECAP_MIN_AGE_HOURS", 24) or 24),
        limit=int(getattr(config, "PERFORMANCE_RECAP_MEASURE_LIMIT", 60) or 60),
    )
    if not pending:
        return 0

    enricher = None
    feed = None
    try:
        from trackers.nftscan_live_feed import get_nftscan_live_feed

        feed = get_nftscan_live_feed()
        if feed and feed.enricher:
            enricher = feed.enricher
    except Exception:
        pass
    if enricher is None:
        try:
            from trackers.mint_contract_enricher import MintContractEnricher

            enricher = MintContractEnricher()
        except Exception:
            pass

    measured = 0
    x_fetches = 0
    x_cap = int(getattr(config, "PERFORMANCE_RECAP_X_FETCH_CAP", 35) or 35)

    for snap in pending:
        kind = snap["kind"]
        ref = snap["ref"]
        try:
            if kind in ("discovery", "escalation"):
                handle = ref.lstrip("@")
                followers_now = None
                user_id = None
                try:
                    row = database.get_project_by_handle(handle)
                    if row:
                        user_id = str(row[0])
                except Exception:
                    pass
                if twitter_client and user_id and x_fetches < x_cap:
                    x_fetches += 1
                    try:
                        acc = await twitter_client.get_user_info(user_id, handle=handle)
                        if acc is not None:
                            followers_now = getattr(acc, "followers_count", None)
                            try:
                                followers_now = int(followers_now) if followers_now is not None else None
                            except (TypeError, ValueError):
                                followers_now = None
                    except Exception as e:
                        logger.debug("recap X fetch @%s: %s", handle, e)
                    await asyncio.sleep(0.4)

                base = snap.get("followers_at")
                delta = None
                if followers_now is not None and base is not None:
                    delta = followers_now - int(base)
                outcome, detail = _classify_project(delta, base)
                alert_snapshots.save_measurement(
                    snap["id"],
                    followers_now=followers_now,
                    followers_delta=delta,
                    outcome=outcome,
                    outcome_detail=detail,
                )
                measured += 1

            elif kind in ("live_mint", "hot_mint"):
                supply_now = None
                max_s = snap.get("max_supply_at")
                if enricher:
                    try:
                        data = await enricher._enrich_contract(ref, "1")
                        supply_now = data.get("total_supply")
                        if max_s is None:
                            max_s = data.get("max_supply")
                    except Exception as e:
                        logger.debug("recap supply %s: %s", ref[:10], e)

                supply_at = snap.get("supply_at")
                supply_delta = None
                if supply_now is not None and supply_at is not None:
                    supply_delta = int(supply_now) - int(supply_at)

                hot_n = 0
                if feed:
                    hot_n = feed._hot_mint_volume_in_window(ref)
                detail_parts = []
                if supply_delta is not None:
                    detail_parts.append(f"supply +{supply_delta}" if supply_delta >= 0 else f"supply {supply_delta}")
                if hot_n:
                    detail_parts.append(f"{hot_n} mints in hot window")
                outcome, detail = _classify_mint(supply_delta, supply_at, " · ".join(detail_parts))
                alert_snapshots.save_measurement(
                    snap["id"],
                    supply_now=supply_now,
                    supply_delta=supply_delta,
                    outcome=outcome,
                    outcome_detail=detail,
                )
                measured += 1

            elif kind == "token_alert":
                mc_at = snap.get("mc_at")
                mc_now = None
                mint = ref
                if mint and mc_at and float(mc_at) > 0:
                    try:
                        import aiohttp
                        from trackers.kolfi_market_enrichment import fetch_dexscreener_solana

                        async with aiohttp.ClientSession() as session:
                            dex = await fetch_dexscreener_solana(session, mint)
                        mc_now = (dex or {}).get("market_cap_usd") or (dex or {}).get("fdv_usd")
                        if mc_now is not None:
                            mc_now = float(mc_now)
                    except Exception:
                        pass
                mc_pct = None
                if mc_now is not None and mc_at:
                    mc_pct = 100.0 * (mc_now - float(mc_at)) / float(mc_at)
                outcome, detail = _classify_token(mc_pct)
                alert_snapshots.save_measurement(
                    snap["id"],
                    outcome=outcome,
                    outcome_detail=detail,
                )
                measured += 1

            else:
                alert_snapshots.save_measurement(
                    snap["id"],
                    outcome="flat",
                    outcome_detail="Tracked (no 24h metric)",
                )
                measured += 1
        except Exception as e:
            logger.warning("measure snapshot %s %s: %s", kind, ref, e)

    return measured


def _load_recap_state() -> Dict[str, Any]:
    try:
        if _STATE_PATH.is_file():
            return json.loads(_STATE_PATH.read_text(encoding="utf-8")) or {}
    except Exception:
        pass
    return {}


def _save_recap_state(state: Dict[str, Any]) -> None:
    try:
        _STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except Exception as e:
        logger.debug("recap state save: %s", e)


def _parse_recap_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        s = str(ts).replace("Z", "+00:00")
        if "T" not in s and " " in s:
            s = s.replace(" ", "T", 1)
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def recap_posted_within_cooldown() -> bool:
    """True if we already posted a recap within the cooldown window."""
    last = _parse_recap_ts(_load_recap_state().get("last_posted_at"))
    if not last:
        return False
    hours = float(getattr(config, "PERFORMANCE_RECAP_COOLDOWN_HOURS", 23) or 23)
    return datetime.now(timezone.utc) - last < timedelta(hours=hours)


def mark_recap_posted(*, channel_id: int = 0, message_id: int = 0) -> None:
    st = _load_recap_state()
    st["last_posted_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    st["channel_id"] = int(channel_id or 0)
    st["message_id"] = int(message_id or 0)
    _save_recap_state(st)


def _feed_counts_24h() -> Dict[str, int]:
    feed_events.init_db()
    events = feed_events.list_events(limit=500)
    since = datetime.now(timezone.utc) - timedelta(hours=24)
    counts: Dict[str, int] = {}
    for ev in events:
        ts = ev.get("ts")
        try:
            s = str(ts).replace("Z", "+00:00")
            if "T" not in s and " " in s:
                s = s.replace(" ", "T", 1)
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if dt < since:
            continue
        k = str(ev.get("kind") or "other")
        counts[k] = counts.get(k, 0) + 1
    return counts


def _wallet_performance_section(
    events: List[Dict[str, Any]],
    measured: List[Dict[str, Any]],
    *,
    limit: int = 6,
) -> Tuple[str, int]:
    """
    Wallet recap focused on what performed / stood out — not a raw alert dump.
    """
    wallet_events = [e for e in events if e.get("kind") == "wallet_nft"]
    total = len(wallet_events)

    standout_measured: List[str] = []
    for m in measured:
        if m.get("kind") != "wallet_nft":
            continue
        if m.get("outcome") not in ("winner", "flat"):
            continue
        label = (m.get("ref_label") or m.get("ref") or "Wallet")[:48]
        detail = (m.get("outcome_detail") or "").strip()
        if m.get("outcome") == "winner" or (detail and detail != "Tracked (no 24h metric)"):
            line = f"• **{label}**"
            if detail and detail != "Tracked (no 24h metric)":
                line += f" — {detail[:72]}"
            standout_measured.append(line)
        if len(standout_measured) >= limit:
            break

    if standout_measured:
        head = f"**{total}** wallet alerts (24h) · **highlights after our ping:**\n"
        return (head + "\n".join(standout_measured))[:1024], total

    coll_counter: Counter[str] = Counter()
    buy_lines: List[str] = []
    seen_buy: set[str] = set()

    for ev in wallet_events:
        extra = ev.get("extra") or {}
        title = (ev.get("title") or "").strip()
        title_l = title.lower()
        contract = (extra.get("contract") or "").lower()
        if contract:
            coll_counter[contract] += 1

        is_buy = any(
            w in title_l
            for w in (" buy", " bought", " mint", " snipe", " sweep", " picked up")
        ) or title_l.startswith("buy ")
        is_bulk = "bulk" in title_l or " x" in title_l
        if not is_buy and not is_bulk:
            continue
        short = title[:78] if title else "Wallet buy"
        key = f"{contract}:{short[:40]}"
        if key in seen_buy:
            continue
        seen_buy.add(key)
        buy_lines.append(f"• {short}")
        if len(buy_lines) >= limit:
            break

    lines: List[str] = [f"**{total}** wallet alerts in the last 24h."]
    top_coll = coll_counter.most_common(3)
    if top_coll:
        lines.append("**Collections whales hit most:**")
        for contract, n in top_coll:
            lines.append(f"• `{contract[:10]}…` — **{n}** alerts")
    if buy_lines:
        lines.append("**Notable buys (24h):**")
        lines.extend(buy_lines)
    elif total:
        lines.append(
            "_Whale moves logged — we highlight **buys** and **repeat collection hits** here "
            "(not every raw ping)._"
        )
    else:
        lines.append("_No wallet alerts in this window._")

    return "\n".join(lines)[:1024], total


def build_performance_recap_embed(
    measured: List[Dict[str, Any]],
    feed_counts: Dict[str, int],
    snap_counts: Dict[str, int],
    wallet_section: str,
    wallet_total: int,
) -> Embed:
    brand = brand_name()
    ts = datetime.now(timezone.utc)
    embed = Embed(
        title=f"📊 {brand} · 24h Performance Recap",
        description=(
            "How alerts from **~24h ago** performed, plus **last 24h** activity totals. "
            "Projects show **follower change since our alert**."
        ),
        color=Color.gold(),
        timestamp=ts,
    )

    winners: List[str] = []
    for m in measured:
        if m.get("outcome") != "winner":
            continue
        label = (m.get("ref_label") or m.get("ref") or "")[:60]
        detail = (m.get("outcome_detail") or "")[:80]
        kind = m.get("kind", "")
        if kind in ("discovery", "escalation"):
            winners.append(f"• **{label}** — {detail}")
        elif kind in ("live_mint", "hot_mint"):
            winners.append(f"• **{label}** — {detail}")
        elif kind == "token_alert":
            winners.append(f"• **{label}** — {detail}")
        elif kind == "wallet_nft" and m.get("outcome") == "winner":
            winners.append(f"• **{label}** — {detail}")
        if len(winners) >= 8:
            break

    embed.add_field(
        name="🏆 Top winners (24h after alert)",
        value="\n".join(winners) if winners else "_No big movers in this window yet — check back tomorrow._",
        inline=False,
    )

    proj_lines: List[str] = []
    for m in measured:
        if m.get("kind") not in ("discovery", "escalation"):
            continue
        label = (m.get("ref_label") or m.get("ref") or "")[:40]
        delta = m.get("followers_delta")
        base = m.get("followers_at")
        proj_lines.append(
            f"• {label}: {_fmt_followers(base)} → {_fmt_delta(delta)}"
        )
        if len(proj_lines) >= 12:
            break
    embed.add_field(
        name="📈 Projects (follower growth since alert)",
        value="\n".join(proj_lines) if proj_lines else "_No project snapshots measured this run._",
        inline=False,
    )

    mint_lines: List[str] = []
    for m in measured:
        if m.get("kind") not in ("live_mint", "hot_mint"):
            continue
        label = (m.get("ref_label") or m.get("ref", ""))[:36]
        mint_lines.append(f"• {label}: {m.get('outcome_detail') or '—'}")
        if len(mint_lines) >= 10:
            break
    embed.add_field(
        name="🖼️ Mints we flagged",
        value="\n".join(mint_lines) if mint_lines else "_No mint snapshots in this window._",
        inline=False,
    )

    embed.add_field(
        name=f"💎 Wallet alerts — what performed (24h · {wallet_total})",
        value=wallet_section[:1024],
        inline=False,
    )

    def _c(k: str) -> int:
        return int(feed_counts.get(k, 0) or snap_counts.get(k, 0))

    totals = (
        f"**Discoveries:** {_c('discovery')} · **Escalations:** {_c('escalation')}\n"
        f"**Live mints:** {_c('live_mint')} · **Hot mints:** {_c('hot_mint')}\n"
        f"**Wallet NFT:** {_c('wallet_nft')} · **Token calls:** {_c('token_alert')}\n"
        f"**Telegram:** {_c('telegram_call')}"
    )
    embed.add_field(name="📢 All alerts (last 24h)", value=totals[:1024], inline=False)

    embed.set_footer(
        text=f"{brand} · Baselines snapshotted at alert time · Not financial advice"
    )
    return embed


async def run_daily_performance_recap(bot) -> Tuple[bool, str]:
    """Measure pending snapshots and post recap embed (at most once per cooldown window)."""
    if not getattr(config, "ENABLE_PERFORMANCE_RECAP", True):
        return False, "disabled"

    if recap_posted_within_cooldown():
        return False, "already posted within cooldown (once per ~24h)"

    ch_id = int(getattr(config, "PERFORMANCE_RECAP_CHANNEL_ID", 0) or 0)
    if not ch_id:
        ch_id = int(getattr(config, "ESCALATION_DAILY_TOP_MOVERS_CHANNEL_ID", 0) or 0)
    if not ch_id:
        return False, "no channel configured"

    alert_snapshots.init_db()
    feed_events.init_db()

    twitter = None
    try:
        twitter = getattr(bot, "twitter", None)
    except Exception:
        pass

    n_meas = await measure_pending_snapshots(bot, twitter_client=twitter)
    measured = alert_snapshots.list_measured_since_hours(
        hours=float(getattr(config, "PERFORMANCE_RECAP_LOOKBACK_HOURS", 30) or 30),
        limit=120,
    )
    feed_counts = _feed_counts_24h()
    snap_counts = alert_snapshots.count_snapshots_alerted_since_hours(24.0)

    events = feed_events.list_events(limit=400)
    since = datetime.now(timezone.utc) - timedelta(hours=24)
    wallet_events = []
    for ev in events:
        try:
            s = str(ev.get("ts", "")).replace(" ", "T", 1)
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if dt >= since:
                wallet_events.append(ev)
        except Exception:
            continue
    wallet_section, wallet_total = _wallet_performance_section(wallet_events, measured)

    embed = build_performance_recap_embed(
        measured, feed_counts, snap_counts, wallet_section, wallet_total
    )

    ch = bot.get_channel(ch_id)
    if ch is None:
        try:
            ch = await bot.fetch_channel(ch_id)
        except Exception as e:
            return False, f"channel {ch_id}: {e}"

    try:
        async with _post_lock:
            if recap_posted_within_cooldown():
                return False, "already posted within cooldown (race)"
            if hasattr(bot, "safe_send"):
                msg = await bot.safe_send(ch, embed=embed)
            else:
                msg = await ch.send(embed=embed)
            mid = int(getattr(msg, "id", 0) or 0)
            mark_recap_posted(channel_id=ch_id, message_id=mid)
    except Exception as e:
        return False, str(e)

    return True, f"posted to {ch_id} (measured {n_meas}, winners {sum(1 for m in measured if m.get('outcome')=='winner')})"
