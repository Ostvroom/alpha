"""
Solana token alerts feed (token overview API; KOLFI_API_KEY in .env).

Alerts only on **new calls** or **material MC / new ATH** updates — not when a coin
first appears on the board (those are seeded silently).
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp
import discord
from discord import Color, Embed

import config as _kolfi_config

from trackers.kolfi_market_enrichment import enrich_solana_mint

try:
    import feed_events
except Exception:
    feed_events = None

API_BASE = "https://api.kolfi.com"
OVERVIEW_PATH = "/tokens/overview"
DEX_ICON_URL = "https://dd.dexscreener.com/ds-data/tokens/solana/{mint}.png"

MC_LOW_MAX = 100_000
MC_100K_MAX = 1_000_000
# paths (persisted state)
from app_paths import DATA_DIR, ensure_dirs

ensure_dirs()
STATE_PATH = os.path.join(DATA_DIR, "kolfi_feed_state.json")
CALLERS_REGISTRY_PATH = os.path.join(DATA_DIR, "kolfi_callers_registry.json")
# Watchlist built from tokens we actually alerted on (persistent performance tracking)
ALERT_WATCHLIST_PATH = os.path.join(DATA_DIR, "kolfi_alert_watchlist.json")

# Prevents concurrent leaderboard post+edit (boot task vs daily loop)
_leaderboard_send_lock = asyncio.Lock()

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

_last_error: Optional[str] = None

BUCKET_LABEL = {"low": "Low cap", "100k": "$100K+", "1m": "$1M+"}


def get_last_kolfi_error() -> Optional[str]:
    return _last_error


def _mc_bucket(last_market_cap: Optional[float]) -> str:
    if last_market_cap is None:
        return "low"
    if last_market_cap < MC_LOW_MAX:
        return "low"
    if last_market_cap < MC_100K_MAX:
        return "100k"
    return "1m"


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _item_mint(item: Dict[str, Any]) -> str:
    m = item.get("mint") or item.get("address") or ""
    return str(m).strip()


def _item_ticker(item: Dict[str, Any]) -> str:
    t = item.get("ticker") or item.get("symbol") or item.get("name") or ""
    return str(t).strip() or "—"


def _format_mc(mc: Optional[float]) -> str:
    if mc is None:
        return "—"
    if mc >= 1_000_000_000:
        return f"${mc / 1_000_000_000:.2f}B"
    if mc >= 1_000_000:
        return f"${mc / 1_000_000:.2f}M"
    if mc >= 1_000:
        return f"${mc / 1_000:.1f}K"
    return f"${mc:.0f}"


def _format_compact_vol(v: Optional[float]) -> str:
    if v is None or v <= 0:
        return "—"
    if v >= 1e9:
        return f"${v / 1e9:.2f}B"
    if v >= 1e6:
        return f"${v / 1e6:.1f}M"
    if v >= 1e3:
        return f"${v / 1e3:.1f}K"
    return f"${v:.0f}"


def _rel_time(iso_ts: Optional[str]) -> str:
    if not iso_ts:
        return "—"
    try:
        ts = str(iso_ts).replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        sec = int((datetime.now(timezone.utc) - dt).total_seconds())
        if sec < 0:
            return "0s"
        if sec < 60:
            return f"{sec}s"
        if sec < 3600:
            return f"{sec // 60}m"
        if sec < 86400:
            return f"{sec // 3600}h"
        return f"{sec // 86400}d"
    except Exception:
        return str(iso_ts)[:19]

def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_alert_watchlist() -> Dict[str, Any]:
    if not os.path.isfile(ALERT_WATCHLIST_PATH):
        return {"version": 1, "by_mint": {}}
    try:
        with open(ALERT_WATCHLIST_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if isinstance(raw, dict) and raw.get("version") == 1 and isinstance(raw.get("by_mint"), dict):
            return raw
    except Exception:
        pass
    return {"version": 1, "by_mint": {}}


def _save_alert_watchlist(data: Dict[str, Any], max_mints: int = 2500) -> None:
    try:
        by_mint = data.get("by_mint") if isinstance(data, dict) else None
        if not isinstance(by_mint, dict):
            return
        if len(by_mint) > max_mints:
            # drop oldest by last_alert_ts
            items = sorted(
                by_mint.items(),
                key=lambda kv: str((kv[1] or {}).get("last_alert_ts") or ""),
            )
            for k, _ in items[: len(by_mint) - max_mints]:
                by_mint.pop(k, None)
        data["version"] = 1
        data["by_mint"] = by_mint
        data["updated_at"] = _iso_now()
        with open(ALERT_WATCHLIST_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"[Velcor3] WARNING: could not save watchlist: {e}")


def register_alerted_mint(
    item: Dict[str, Any],
    alert_lines: List[str],
    *,
    at_iso: Optional[str] = None,
) -> None:
    """
    Persist a mint into the "alert watchlist" the first time we post an alert for it.
    Stores baseline MC at first alert so we can track performance since *our* alert.
    """
    try:
        mint = _item_mint(item)
        if not mint:
            return
        tick = _item_ticker(item)
        mc = _safe_float(item.get("last_market_cap"))
        ath = _safe_float(item.get("ath_market_cap"))
        now = (str(at_iso).strip() if at_iso else "") or _iso_now()

        calls = item.get("callsPreview") or item.get("calls") or []
        best_call = None
        if isinstance(calls, list) and calls:
            # store the most recent call row for context
            best_call = _best_recent_call(item, max_age_days=30)

        data = _load_alert_watchlist()
        by_mint: Dict[str, Any] = data.setdefault("by_mint", {})
        ent = by_mint.get(mint) or {}

        if not ent:
            ent = {
                "mint": mint,
                "ticker": tick,
                "first_alert_ts": now,
                "baseline_mc_usd": mc,
                "baseline_ath_usd": ath,
            }
        # always update last seen + last snapshot
        ent["ticker"] = tick
        ent["last_alert_ts"] = now
        ent["last_alert_lines"] = alert_lines[:6]
        ent["last_kolfi_mc_usd"] = mc
        ent["last_kolfi_ath_usd"] = ath
        if best_call:
            ent["last_call"] = {
                "who": _call_label(best_call),
                "kolXId": best_call.get("kolXId") or best_call.get("kol_x_id"),
                "callMarketCap": _safe_float(best_call.get("callMarketCap")),
                "messageTs": best_call.get("messageTs"),
            }
        by_mint[mint] = ent
        data["by_mint"] = by_mint
        _save_alert_watchlist(data)
    except Exception:
        return


def _call_multiplier(call: Dict[str, Any]) -> Optional[float]:
    m = _safe_float(call.get("multiplier"))
    if m is not None and m > 0:
        return m
    peak = _safe_float(call.get("peakMarketCap"))
    base = _safe_float(call.get("callMarketCap"))
    if peak and base and base > 0:
        return peak / base
    return None


def _fmt_mult(x: Optional[float]) -> str:
    if x is None or x <= 0:
        return "—"
    if x >= 10:
        return f"{int(x)}x"
    t = f"{x:.1f}"
    if t.endswith(".0"):
        return f"{int(x)}x"
    return f"{t}x"


def _call_label(call: Dict[str, Any]) -> str:
    for k in ("kolUsername", "channelName", "kol_name", "channel_name"):
        v = call.get(k)
        if v:
            return str(v)[:40]
    kid = call.get("kolXId") or call.get("kol_x_id")
    if kid:
        return str(kid)[:40]
    return "caller"


def _call_identity_set(calls: List[Dict[str, Any]]) -> Set[str]:
    """Stable ids for calls (API callId, else fingerprint)."""
    out: Set[str] = set()
    for i, c in enumerate(calls):
        cid = c.get("callId")
        if cid is not None and str(cid).strip() != "":
            out.add(f"id:{cid}")
        else:
            out.add(
                "fp:"
                + "|".join(
                    str(x)
                    for x in (
                        c.get("messageTs"),
                        c.get("kolXId"),
                        c.get("callMarketCap"),
                        i,
                    )
                )
            )
    return out


def bucket_items(items: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {"low": [], "100k": [], "1m": []}
    for it in items:
        mc = _safe_float(it.get("last_market_cap"))
        out[_mc_bucket(mc)].append(it)
    return out


def _load_by_mint() -> Dict[str, Dict[str, Any]]:
    if not os.path.isfile(STATE_PATH):
        return {}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if raw.get("version") == 2 and isinstance(raw.get("by_mint"), dict):
            return dict(raw["by_mint"])
        # Legacy v1: drop — avoids mass “new mint” spam when upgrading
        return {}
    except Exception:
        return {}


def _save_by_mint(by_mint: Dict[str, Dict[str, Any]], max_mints: int = 1500) -> None:
    if len(by_mint) > max_mints:
        keys = list(by_mint.keys())[-max_mints:]
        by_mint = {k: by_mint[k] for k in keys}
    try:
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump({"version": 2, "by_mint": by_mint}, f, indent=0)
    except Exception as e:
        print(f"[Token feed] WARNING: could not save state to {STATE_PATH}: {e}")


def _caller_stable_key(c: Dict[str, Any]) -> str:
    kid = c.get("kolXId") or c.get("kol_x_id")
    if kid is not None and str(kid).strip():
        return f"x:{str(kid).lstrip('@').split('/')[0]}"
    lab = _call_label(c)
    if lab and lab != "caller":
        slug = "".join(ch if ch.isalnum() else "_" for ch in lab.lower())[:80].strip("_")
        if slug:
            return f"label:{slug}"
    return ""


def _call_links_from_dict(c: Dict[str, Any]) -> List[str]:
    """Extract profile / message / channel URLs from a call object (for local registry)."""
    links: List[str] = []
    kid = c.get("kolXId") or c.get("kol_x_id")
    if kid:
        h = str(kid).lstrip("@").split("/")[0]
        if h:
            links.append(f"https://x.com/{h}")
    for _k, v in c.items():
        if not isinstance(v, str):
            continue
        v = v.strip()
        if v.startswith("http://") or v.startswith("https://"):
            if v not in links:
                links.append(v)
    return links[:24]


def _load_callers_registry() -> Dict[str, Any]:
    if not os.path.isfile(CALLERS_REGISTRY_PATH):
        return {"version": 1, "callers": {}}
    try:
        with open(CALLERS_REGISTRY_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if isinstance(raw, dict) and raw.get("version") == 1 and isinstance(raw.get("callers"), dict):
            return raw
    except Exception:
        pass
    return {"version": 1, "callers": {}}


def _trim_callers_registry(callers: Dict[str, Any], max_entries: int = 2000) -> None:
    if len(callers) <= max_entries:
        return
    items = sorted(
        callers.items(),
        key=lambda kv: str(kv[1].get("last_seen") or ""),
    )
    for k, _ in items[: len(callers) - max_entries]:
        del callers[k]


def merge_callers_from_items(items: List[Dict[str, Any]]) -> None:
    """
    Merge caller labels + links from API items into kolfi_callers_registry.json.
    Use this file to migrate off the third-party API later (your own index of callers).
    """
    if not items:
        return
    data = _load_callers_registry()
    callers: Dict[str, Any] = data.setdefault("callers", {})
    now = datetime.now(timezone.utc).isoformat()
    for item in items:
        mint = _item_mint(item)
        tick = _item_ticker(item)
        raw = item.get("callsPreview") or item.get("calls") or []
        if not isinstance(raw, list):
            continue
        for c in raw:
            if not isinstance(c, dict):
                continue
            key = _caller_stable_key(c)
            if not key:
                continue
            links = _call_links_from_dict(c)
            ent = callers.get(key)
            if not ent:
                ent = {
                    "label": _call_label(c),
                    "links": [],
                    "mints_seen": [],
                    "first_seen": now,
                }
            ent["label"] = _call_label(c)
            kid = c.get("kolXId") or c.get("kol_x_id")
            if kid is not None:
                ent["kol_x_id"] = str(kid).lstrip("@")
            for u in links:
                if u not in ent["links"]:
                    ent["links"].append(u)
            ent["links"] = ent["links"][:40]
            if mint:
                ms = ent.setdefault("mints_seen", [])
                entry = {"mint": mint, "ticker": tick, "ts": now}
                replaced = False
                for i, x in enumerate(ms):
                    if x.get("mint") == mint:
                        ms[i] = entry
                        replaced = True
                        break
                if not replaced:
                    ms.append(entry)
                ent["mints_seen"] = ms[-40:]
            ent["last_seen"] = now
            callers[key] = ent
    _trim_callers_registry(callers)
    data["callers"] = callers
    data["updated_at"] = now
    data["version"] = 1
    try:
        with open(CALLERS_REGISTRY_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"[Token feed] WARNING: could not save callers registry: {e}")


def _classify_alert_kinds(alert_lines: List[str]) -> List[str]:
    kinds: List[str] = []
    blob = " ".join(alert_lines).lower()
    if "new callers" in blob or "fresh signal" in blob:
        kinds.append("new_callers")
    if "market cap" in blob or "mc" in blob:
        kinds.append("mc_move")
    if "all-time high" in blob or "ath" in blob:
        kinds.append("ath_break")
    return kinds or ["update"]


def _heuristic_tape_signals(item: Dict[str, Any]) -> str:
    """Deterministic hints the model can weigh (not predictions)."""
    mc = _safe_float(item.get("last_market_cap"))
    ath = _safe_float(item.get("ath_market_cap"))
    vol = _safe_float(item.get("last_volume"))
    chg = _safe_float(item.get("change_5m"))
    parts: List[str] = []
    if mc and ath and ath > 0:
        if mc >= ath * 0.98:
            parts.append("MC near tracked ATH (limited upside vs prior peak in feed)")
        elif mc < ath:
            dd = (ath - mc) / ath * 100.0
            parts.append(f"MC ~{dd:.0f}% below ATH in snapshot")
    if mc and vol and mc > 0:
        r = vol / mc
        if r >= 2.0:
            parts.append(f"High turnover (vol/MC ≈ {r:.1f}×)")
        elif r >= 0.5:
            parts.append(f"Moderate turnover (vol/MC ≈ {r:.1f}×)")
        else:
            parts.append(f"Lower turnover (vol/MC ≈ {r:.1f}×)")
    if chg is not None:
        parts.append(f"5m move {chg:+.1f}%")
    return " · ".join(parts) if parts else "Limited tape context in feed."


def _summarize_calls_for_prompt(item: Dict[str, Any], max_rows: int = 12) -> str:
    calls = item.get("callsPreview") or []
    if not isinstance(calls, list) or not calls:
        return "(no caller rows in API response)"
    lines: List[str] = []
    for c in calls[:max_rows]:
        who = _call_label(c)
        cmc = _format_mc(_safe_float(c.get("callMarketCap")))
        mult = _fmt_mult(_call_multiplier(c))
        rel = _rel_time(c.get("messageTs"))
        peak_mc = _safe_float(c.get("peakMarketCap"))
        peak = _format_mc(peak_mc)
        peak_s = f" · ATH/peak {peak}" if peak_mc else ""
        lines.append(f"• {who} — call at {cmc} → best multiple {mult}{peak_s} · {rel} ago")
    if len(calls) > max_rows:
        lines.append(f"… +{len(calls) - max_rows} more callers in feed")
    return "\n".join(lines)


def _social_urls_line(item: Dict[str, Any]) -> str:
    tw = item.get("twitter_url") or item.get("twitterUrl")
    web = item.get("website_url") or item.get("websiteUrl")
    bits = []
    if tw:
        bits.append(f"X linked in feed")
    if web:
        bits.append(f"site linked in feed")
    return " · ".join(bits) if bits else "No X/site URL in feed"


def compile_alert_facts(
    item: Dict[str, Any],
    alert_lines: List[str],
    enrichment: Dict[str, Any],
) -> Dict[str, Any]:
    """Deterministic JSON: token snapshot + market enrichment (ground truth for LLMs)."""
    mc = _safe_float(item.get("last_market_cap"))
    ath = _safe_float(item.get("ath_market_cap"))
    vol = _safe_float(item.get("last_volume"))
    chg = _safe_float(item.get("change_5m"))
    price = _safe_float(item.get("price"))
    supply = _safe_float(item.get("supply"))
    calls = item.get("callsPreview") or []
    n_callers = len(calls) if isinstance(calls, list) else 0

    dex = enrichment.get("dexscreener") or {}
    be = enrichment.get("birdeye") or {}

    liq_mc = None
    dex_liq = dex.get("liquidity_usd")
    if dex_liq is not None and mc and mc > 0:
        liq_mc = dex_liq / mc

    vol_mc = None
    if vol is not None and mc and mc > 0:
        vol_mc = vol / mc

    mcap_diff_pct = None
    dex_mcap = dex.get("market_cap_usd")
    if dex.get("ok") and dex_mcap is not None and mc and mc > 0:
        mcap_diff_pct = abs(dex_mcap - mc) / mc * 100.0

    heur_flags: List[str] = []
    if dex.get("ok"):
        if (dex_liq or 0) < 15_000:
            heur_flags.append("low_dex_liquidity_vs_typical_meme")
        pad = dex.get("pair_age_days")
        if pad is not None and pad < 1.5:
            heur_flags.append("very_new_pair")
        if liq_mc is not None and liq_mc < 0.03:
            heur_flags.append("liquidity_small_vs_mcap")
    if be.get("ok") and be.get("holder_count") is not None:
        if be["holder_count"] < 150:
            heur_flags.append("relatively_few_on_chain_holders")
    if vol_mc is not None and vol_mc > 3:
        heur_flags.append("high_turnover_vs_mcap")

    return {
        "version": 2,
        "ticker": _item_ticker(item),
        "mint": _item_mint(item),
        "kolfi": {
            "mc_usd": mc,
            "ath_usd": ath,
            "vol_usd": vol,
            "change_5m_pct": chg,
            "price_usd": price,
            "supply": supply,
            "caller_rows": n_callers,
        },
        "alert": {
            "lines": alert_lines,
            "kinds": _classify_alert_kinds(alert_lines),
        },
        "dexscreener": dex,
        "birdeye": be,
        "derived": {
            "liquidity_to_mcap_ratio": liq_mc,
            "vol_to_mcap_ratio": vol_mc,
            "dex_vs_kolfi_mcap_diff_pct": mcap_diff_pct,
            "heuristic_risk_flags": heur_flags,
        },
        "caller_digest": _summarize_calls_for_prompt(item),
        "socials": _social_urls_line(item),
        "tape_hints": _heuristic_tape_signals(item),
    }


async def _llm_step1_analyze_facts(
    client: Any,
    model: str,
    facts: Dict[str, Any],
    max_tokens: int,
) -> Dict[str, Any]:
    """Structured risk / alignment — must not invent numbers outside `facts`."""
    payload = json.dumps(facts, ensure_ascii=False, indent=0)[:14_000]
    try:
        resp = await client.chat.completions.create(
            model=model,
            temperature=0.15,
            max_tokens=min(450, max_tokens),
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You receive `facts`, verified JSON from our servers (liquidity, pair age, holders when available). "
                        "Output a SINGLE JSON object with keys only: "
                        "risk_flags (string[], max 6, short), "
                        "alignment_note (string, one sentence comparing sources when numbers differ), "
                        "liquidity_comment (string, one sentence about pool depth vs mcap), "
                        "holder_comment (string, one sentence about holder count or 'unknown'). "
                        "Rules: never invent FDV, holders, or liquidity. If a source is missing, say so. "
                        "You may restate numbers ONLY if they appear in `facts`."
                    ),
                },
                {"role": "user", "content": payload},
            ],
        )
        raw = (resp.choices[0].message.content or "").strip()
        out = json.loads(raw)
        return out if isinstance(out, dict) else {}
    except Exception as e:
        print(f"[Velcor3] AI step1 skipped: {e}")
        return {}


async def _llm_step2_render_brief(
    client: Any,
    model: str,
    facts: Dict[str, Any],
    analysis: Dict[str, Any],
    depth: str,
    max_tokens: int,
) -> str:
    """Discord copy from facts + step-1 analysis."""
    blob = {
        "facts": facts,
        "analysis": analysis,
    }
    user = json.dumps(blob, ensure_ascii=False, indent=0)[:14_000]
    if depth == "standard":
        sys = (
            "You write 2–3 short neutral sentences for a Discord crypto alert. "
            "Use ONLY `facts` and `analysis`. No financial advice, no buy/sell. End with DYOR."
        )
        usr = user + "\nWrite the blurb only, plain text."
        temp = 0.38
    else:
        sys = (
            "You compose the visible Discord 'AI brief' using ONLY `facts` and `analysis`. "
            "No new statistics. No financial advice. "
            "Exact format — 4 lines, each starts with:\n"
            "⚡ Trigger —\n📊 Tape —\n👥 Callers / flow —\n⚠️ Watch —\n"
            "Reference liquidity, pair age, and holder counts when present in facts; else say missing."
        )
        usr = user
        temp = 0.42
    resp = await client.chat.completions.create(
        model=model,
        temperature=temp,
        max_tokens=max_tokens,
        messages=[{"role": "system", "content": sys}, {"role": "user", "content": usr}],
    )
    return (resp.choices[0].message.content or "").strip()


async def _llm_single_shot_brief(
    client: Any,
    model: str,
    facts: Dict[str, Any],
    depth: str,
    max_tokens: int,
) -> str:
    """One call: facts JSON in, brief out (standard or deep format)."""
    user = json.dumps(facts, ensure_ascii=False, indent=0)[:14_000]
    if depth == "standard":
        sys = (
            "You write 2–3 short neutral sentences for a Discord embed. "
            "Use ONLY the JSON facts. No financial advice. Mention DYOR."
        )
        usr = user + "\nWrite plain text blurb only."
        temp = 0.38
    else:
        sys = (
            "You compose a Discord brief using ONLY the JSON. No new numbers. "
            "4 lines: ⚡ Trigger — / 📊 Tape — / 👥 Callers / flow — / ⚠️ Watch — "
            "Cite liquidity, pair age, and holder counts from facts when present."
        )
        usr = user
        temp = 0.42
    resp = await client.chat.completions.create(
        model=model,
        temperature=temp,
        max_tokens=max_tokens,
        messages=[{"role": "system", "content": sys}, {"role": "user", "content": usr}],
    )
    return (resp.choices[0].message.content or "").strip()


async def generate_kolfi_alert_review(
    item: Dict[str, Any],
    alert_lines: List[str],
    session: Optional[aiohttp.ClientSession] = None,
) -> Optional[str]:
    """
    AI brief: optional market enrichment, deterministic `facts` JSON,
    then either two-step (analysis JSON → prose) or single-shot.
    """
    try:
        import config as _cfg
    except Exception:
        return None
    if not getattr(_cfg, "OPENAI_API_KEY", None):
        return None
    try:
        from openai import AsyncOpenAI
    except ImportError:
        return None

    mint = _item_mint(item)
    enrich_markets = getattr(_cfg, "KOLFI_MARKET_ENRICH", True)
    be_key = getattr(_cfg, "BIRDEYE_API_KEY", "") or ""
    enrichment: Dict[str, Any] = {
        "mint": mint,
        "dexscreener": {"ok": False},
        "birdeye": {"ok": False},
    }
    if enrich_markets and session and mint:
        try:
            enrichment = await enrich_solana_mint(session, mint, birdeye_api_key=be_key)
        except Exception as e:
            print(f"[Velcor3] Market enrichment failed: {e}")

    facts = compile_alert_facts(item, alert_lines, enrichment)

    model = getattr(_cfg, "KOLFI_AI_MODEL", None) or getattr(_cfg, "AI_MODEL", None) or "gpt-4o-mini"
    depth = getattr(_cfg, "KOLFI_AI_REVIEW_DEPTH", "deep")
    max_tok = int(getattr(_cfg, "KOLFI_AI_MAX_TOKENS", 550))
    two_step = getattr(_cfg, "KOLFI_AI_TWO_STEP", True)
    step1_model = getattr(_cfg, "KOLFI_AI_MODEL_STEP1", None) or model

    client = AsyncOpenAI(api_key=_cfg.OPENAI_API_KEY)

    try:
        if two_step and depth == "deep":
            analysis = await _llm_step1_analyze_facts(client, step1_model, facts, max_tok)
            text = await _llm_step2_render_brief(client, model, facts, analysis, depth, max_tok)
        else:
            text = await _llm_single_shot_brief(client, model, facts, depth, max_tok)
        if not text:
            return None
        if text.startswith("```"):
            lines = text.split("\n")
            if len(lines) >= 2:
                text = "\n".join(lines[1:])
            if text.rstrip().endswith("```"):
                text = text.rstrip()[:-3].rstrip()
        return text[:1024]
    except Exception as e:
        print(f"[Velcor3] AI review skipped: {e}")
        return None


async def fetch_dexscreener_icon_url(session: aiohttp.ClientSession, mint: str) -> Optional[str]:
    if not mint:
        return None
    url = DEX_ICON_URL.format(mint=mint)
    try:
        async with session.head(url, headers=UA, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                ct = (r.headers.get("Content-Type") or "").lower()
                if "image" in ct:
                    return url
    except Exception:
        pass
    try:
        async with session.get(
            url,
            headers={**UA, "Range": "bytes=0-0"},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            if r.status in (200, 206):
                ct = (r.headers.get("Content-Type") or "").lower()
                if "image" in ct:
                    return url
    except Exception:
        pass
    return None


async def resolve_token_thumbnail(
    session: aiohttp.ClientSession,
    item: Dict[str, Any],
    mint: str,
) -> Optional[str]:
    for k in ("logo", "iconUrl", "image", "tokenImage", "icon_url", "imageUrl"):
        v = item.get(k)
        if v and isinstance(v, str) and v.startswith("http"):
            return v
    return await fetch_dexscreener_icon_url(session, mint)


async def fetch_tokens_overview(
    session: aiohttp.ClientSession,
    api_key: str,
    *,
    limit: int = 100,
    include_calls: int = 50,
    max_pages: int = 15,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    global _last_error
    _last_error = None
    if not api_key.strip():
        _last_error = "Missing KOLFI_API_KEY"
        return [], _last_error

    headers = {"x-api-key": api_key.strip(), "Accept": "application/json"}
    items: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    cursor_mint: Optional[str] = None

    for _ in range(max(1, max_pages)):
        params: Dict[str, Any] = {
            "limit": min(100, max(1, limit)),
            "includeCalls": min(50, max(1, include_calls)),
        }
        if cursor:
            params["cursor"] = cursor
        if cursor_mint:
            params["cursor_mint"] = cursor_mint

        url = f"{API_BASE}{OVERVIEW_PATH}"
        try:
            async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=45)) as r:
                text = await r.text()
                if r.status != 200:
                    _last_error = f"HTTP {r.status}: {text[:200]}"
                    return items if items else ([], _last_error)
                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    _last_error = "Invalid JSON from token data API"
                    return [], _last_error
        except Exception as e:
            _last_error = str(e)
            return items if items else ([], _last_error)

        chunk = data.get("items")
        if isinstance(chunk, list):
            items.extend(chunk)
        cursor = data.get("nextCursor") or data.get("next_cursor")
        cursor_mint = data.get("nextCursorMint") or data.get("next_cursor_mint")
        if not cursor:
            break

    return items, None


def build_token_embed(
    item: Dict[str, Any],
    bucket_key: str,
    *,
    brand_name: str,
    embed_color: int,
    alert_lines: List[str],
    attachment_banner_name: Optional[str],
    thumb_url: Optional[str],
    ai_review: Optional[str] = None,
    simple_embed: bool = True,
    our_alert_utc_iso: Optional[str] = None,
) -> Embed:
    mint = _item_mint(item)
    tick = _item_ticker(item)
    label = BUCKET_LABEL.get(bucket_key, bucket_key)

    mc = _safe_float(item.get("last_market_cap"))
    vol = _safe_float(item.get("last_volume"))
    ath = _safe_float(item.get("ath_market_cap"))
    chg = _safe_float(item.get("change_5m"))
    dex = item.get("dexscreener_url") or item.get("dexUrl") or item.get("dexscreenerUrl")
    tw = item.get("twitter_url") or item.get("twitterUrl")
    web = item.get("website_url") or item.get("websiteUrl")
    padre = f"https://trade.padre.gg/sol/{mint}" if mint else None
    axiom = f"https://axiom.trade/t/{mint}" if mint else None
    gmgn = f"https://gmgn.ai/sol/token/{mint}" if mint else None
    pump_fun = f"https://pump.fun/{mint}" if mint else None
    chart_url = dex or gmgn

    _color = embed_color
    if alert_lines:
        joined = " ".join(alert_lines).lower()
        if "ath" in joined:
            _color = 0x00C896
        elif "market cap" in joined or "mc" in joined:
            _color = 0x5865F2
        else:
            _color = 0xF5A623

    embed = Embed(color=Color(_color), timestamp=datetime.now(timezone.utc))

    embed.set_author(
        name=f"⚡ {brand_name}  ·  {label}",
        url=chart_url or None,
    )

    if attachment_banner_name:
        embed.set_image(url=f"attachment://{attachment_banner_name}")
    if thumb_url:
        embed.set_thumbnail(url=thumb_url)

    mc_s = _format_mc(mc)
    v_s = _format_compact_vol(vol)
    ath_s = _format_mc(ath)
    chg_s = (f"{'↑' if chg >= 0 else '↓'}{abs(chg):.1f}%" if chg is not None else "—")

    if simple_embed:
        reason = " · ".join(alert_lines) if alert_lines else "New signal"
        if len(reason) > 280:
            reason = reason[:277] + "…"
        desc = f"## {tick}\n{reason}"
        if mint:
            desc += f"\n\n`{mint}`"
        embed.description = desc[:4096]

        snap = f"**MC** {mc_s}  ·  **ATH** {ath_s}  ·  **Vol** {v_s}  ·  **5m** {chg_s}"
        embed.add_field(name="Snapshot", value=snap, inline=False)

        if our_alert_utc_iso:
            dt_alert = _parse_message_ts(our_alert_utc_iso)
            human = (
                dt_alert.strftime("%d/%m/%Y %H:%M:%S UTC")
                if dt_alert
                else str(our_alert_utc_iso)[:32]
            )
            embed.add_field(name="Our alert (UTC)", value=human, inline=False)

        link_parts: List[str] = []
        if tw:
            link_parts.append(f"[𝕏]({tw})")
        if web:
            link_parts.append(f"[Site]({web})")
        if pump_fun:
            link_parts.append(f"[Pump.fun]({pump_fun})")
        if padre:
            link_parts.append(f"[Swap]({padre})")
        if axiom:
            link_parts.append(f"[Swap]({axiom})")
        if gmgn:
            link_parts.append(f"[Chart]({gmgn})")
        if dex:
            link_parts.append(f"[Dex]({dex})")
        if link_parts:
            embed.add_field(name="Links", value=" · ".join(link_parts), inline=False)

        kol_line = _format_earliest_kol_call_line(item)
        embed.add_field(
            name="Earliest caller",
            value=kol_line if kol_line else "_No caller history._",
            inline=False,
        )

        embed.set_footer(text=f"{brand_name}  ·  {label}  ·  DYOR")
        return embed

    # ── Legacy verbose embed (set KOLFI_SIMPLE_ALERT_EMBED=0) ─────────────────
    reason = "  ·  ".join(alert_lines) if alert_lines else "New signal"
    desc = f"## {tick}\n{reason}"
    if mint:
        desc += f"\n\n`{mint}`"
    embed.description = desc[:4096]

    embed.add_field(name="Market Cap", value=mc_s, inline=True)
    embed.add_field(name="ATH", value=ath_s, inline=True)
    embed.add_field(name="Volume", value=v_s, inline=True)
    embed.add_field(name="5m Move", value=chg_s, inline=True)

    price = _safe_float(item.get("price"))
    supply = _safe_float(item.get("supply"))
    if price is not None and price > 0:
        ptxt = f"${price:.8f}".rstrip("0").rstrip(".")
        if supply is not None and supply > 0:
            ptxt += f"  ·  supply {supply:,.0f}".replace(",", " ")
        embed.add_field(name="Price", value=ptxt, inline=True)

    soc_parts: List[str] = []
    if tw:
        soc_parts.append(f"[𝕏]({tw})")
    if web:
        soc_parts.append(f"[Site]({web})")
    if soc_parts:
        embed.add_field(name="Socials", value="  ·  ".join(soc_parts), inline=True)

    trade_links: List[str] = []
    if pump_fun:
        trade_links.append(f"[Pump.fun]({pump_fun})")
    if padre:
        trade_links.append(f"[Swap]({padre})")
    if axiom:
        trade_links.append(f"[Swap]({axiom})")
    if gmgn:
        trade_links.append(f"[Chart]({gmgn})")
    if dex:
        trade_links.append(f"[Dex]({dex})")
    if trade_links:
        embed.add_field(name="Trade", value="  ·  ".join(trade_links), inline=False)

    if ai_review:
        embed.add_field(name="🧠 AI brief", value=ai_review[:1024], inline=False)

    calls = item.get("callsPreview") or []
    if isinstance(calls, list) and calls:
        ts_list = [c.get("messageTs") for c in calls if c.get("messageTs")]
        rows: List[str] = []
        if ts_list:
            rows.append(f"_First call {_rel_time(min(ts_list))} ago_\n")
        for c in calls[:8]:
            cmc = _format_mc(_safe_float(c.get("callMarketCap")))
            mult = _fmt_mult(_call_multiplier(c))
            rel = _rel_time(c.get("messageTs"))
            who = _call_label(c)
            kid = c.get("kolXId")
            who_link = f"[{who}](https://x.com/{str(kid).lstrip('@')})" if kid else who
            rows.append(f"▸ {who_link}  ·  **{cmc}** at call  →  **{mult}**  ·  _{rel}_")
        body = "\n".join(rows)
        if len(calls) > 8:
            body += f"\n_+{len(calls) - 8} more_"
        if len(body) > 1024:
            body = body[:1020] + "…"
        embed.add_field(name="Callers", value=body, inline=False)
    else:
        embed.add_field(name="Callers", value="_No caller history._", inline=False)

    embed.set_footer(text=f"{brand_name}  ·  {label}  ·  DYOR")
    return embed


def _snapshot(
    item: Dict[str, Any],
    call_ids: Set[str],
    *,
    ref_mc: Optional[float],
    ref_ath: Optional[float],
) -> Dict[str, Any]:
    """ref_* = baseline for % move / new ATH since last alert (or seed), not last poll."""
    cur_mc = _safe_float(item.get("last_market_cap"))
    cur_ath = _safe_float(item.get("ath_market_cap"))
    return {
        "call_ids": sorted(call_ids),
        "last_mc": cur_mc,
        "last_ath": cur_ath,
        "last_vol": _safe_float(item.get("last_volume")),
        "ref_mc": ref_mc if ref_mc is not None else cur_mc,
        "ref_ath": ref_ath if ref_ath is not None else cur_ath,
    }


def _prev_ref_mc(prev: Dict[str, Any]) -> Optional[float]:
    v = prev.get("ref_mc")
    if v is not None:
        return _safe_float(v)
    return _safe_float(prev.get("last_mc"))


def _prev_ref_ath(prev: Dict[str, Any]) -> Optional[float]:
    v = prev.get("ref_ath")
    if v is not None:
        return _safe_float(v)
    return _safe_float(prev.get("last_ath"))


async def run_kolfi_feed_once(
    client: discord.Client,
    session: aiohttp.ClientSession,
    api_key: str,
    channel_ids: Dict[str, int],
    *,
    send_delay_sec: float = 1.25,
    max_alerts_per_bucket: int = 0,
    brand_name: str = "Velcor3",
    embed_color: int = 0x202025,
    banner_path: Optional[str] = None,
    banner_filename: Optional[str] = None,
    mc_move_pct: float = 15.0,
    ath_break_pct: float = 2.0,
    enable_ai_review: bool = True,
) -> Tuple[int, Optional[str], Dict[str, Any]]:
    """
    Alert only if:
    - new caller row(s) vs last poll, or
    - MC moved by >= mc_move_pct vs ref baseline (since last alert), or
    - ATH increased by >= ath_break_pct vs ref baseline.

    First time seeing a mint: seed snapshot, no Discord post.
    """
    global _last_error
    items, err = await fetch_tokens_overview(session, api_key)
    if err and not items:
        return 0, err, {"items": 0, "queued": 0, "sent": 0}

    merge_callers_from_items(items)

    buckets = bucket_items(items)
    by_mint = _load_by_mint()
    sent = 0
    queued_total = 0
    use_banner = bool(banner_path and banner_filename and os.path.isfile(banner_path))
    simple_alert = getattr(_kolfi_config, "KOLFI_SIMPLE_ALERT_EMBED", True)

    for key, ch_id in channel_ids.items():
        if not ch_id:
            continue
        bucket_list = buckets.get(key, [])
        pending: List[Tuple[Dict[str, Any], List[str], str, Set[str]]] = []

        for item in bucket_list:
            mint = _item_mint(item)
            if not mint:
                continue
            calls = item.get("callsPreview") or []
            if not isinstance(calls, list):
                calls = []
            cur_ids = _call_identity_set(calls)
            cur_mc = _safe_float(item.get("last_market_cap"))
            cur_ath = _safe_float(item.get("ath_market_cap"))

            prev = by_mint.get(mint)
            if prev is None:
                by_mint[mint] = _snapshot(
                    item,
                    cur_ids,
                    ref_mc=cur_mc,
                    ref_ath=cur_ath,
                )
                continue

            prev_ids = set(prev.get("call_ids") or [])
            new_ids = cur_ids - prev_ids
            alert_lines: List[str] = []

            if not prev_ids and cur_ids:
                alert_lines.append("**First call** — KOL signal on this token")
            elif new_ids:
                n = len(new_ids)
                alert_lines.append(
                    f"**New callers** — {n} fresh signal{'s' if n != 1 else ''}"
                )

            pm = _prev_ref_mc(prev)
            if pm and cur_mc and pm > 0:
                move = abs(cur_mc - pm) / pm * 100.0
                if move >= mc_move_pct:
                    if cur_mc > pm:
                        alert_lines.append(
                            f"**Market cap** ↑ **{move:.1f}%** — {_format_mc(pm)} → {_format_mc(cur_mc)}"
                        )
                    else:
                        alert_lines.append(
                            f"**Market cap** ↓ **{move:.1f}%** — {_format_mc(pm)} → {_format_mc(cur_mc)}"
                        )

            pa = _prev_ref_ath(prev)
            if pa and cur_ath and pa > 0 and cur_ath > pa * (1.0 + ath_break_pct / 100.0):
                alert_lines.append(
                    f"**All-time high** — {_format_mc(cur_ath)} (was {_format_mc(pa)})"
                )

            if alert_lines:
                pending.append((item, alert_lines, mint, cur_ids))
            else:
                by_mint[mint] = _snapshot(
                    item,
                    cur_ids,
                    ref_mc=_prev_ref_mc(prev),
                    ref_ath=_prev_ref_ath(prev),
                )

        if max_alerts_per_bucket > 0 and len(pending) > max_alerts_per_bucket:
            print(
                f"[Velcor3] Bucket {key}: cap {max_alerts_per_bucket} alerts "
                f"(had {len(pending)}); raise KOLFI_MAX_ALERTS_PER_BUCKET or set 0"
            )
            pending = pending[:max_alerts_per_bucket]

        if not pending:
            continue

        ch = client.get_channel(ch_id) or await client.fetch_channel(ch_id)
        if not ch:
            _last_error = f"Channel {ch_id} not found"
            continue

        queued_total += len(pending)

        for item, alert_lines, mint, cur_ids in pending:
            thumb = await resolve_token_thumbnail(session, item, mint)
            ai_rev = None
            if enable_ai_review and not simple_alert:
                ai_rev = await generate_kolfi_alert_review(item, alert_lines, session)
            alert_at = _iso_now()
            embed = build_token_embed(
                item,
                key,
                brand_name=brand_name,
                embed_color=embed_color,
                alert_lines=alert_lines,
                attachment_banner_name=banner_filename if use_banner else None,
                thumb_url=thumb,
                ai_review=ai_rev,
                simple_embed=simple_alert,
                our_alert_utc_iso=alert_at,
            )
            files: List[discord.File] = []
            if use_banner and banner_path and banner_filename:
                files.append(discord.File(banner_path, filename=banner_filename))
            if files:
                await ch.send(embed=embed, files=files)
            else:
                await ch.send(embed=embed)
            if feed_events is not None:
                try:
                    embed_payload = {}
                    try:
                        embed_payload = embed.to_dict() if embed else {}
                    except Exception:
                        embed_payload = {}
                    sym = (item.get("symbol") or item.get("ticker") or "").strip()
                    name = (item.get("name") or "").strip()
                    title = f"{name or sym or 'Token'} · {key}"
                    body = " | ".join(alert_lines or [])[:1500]
                    feed_events.add_event(
                        kind="token_alert",
                        guild_id=int(getattr(getattr(ch, "guild", None), "id", 0) or 0),
                        channel_id=int(getattr(ch, "id", 0) or 0),
                        title=title[:200],
                        body=body,
                        url=str(item.get("url") or item.get("dexscreener_url") or item.get("twitter") or ""),
                        extra={
                            "bucket": key,
                            "mint": mint,
                            "symbol": sym,
                            "name": name,
                            "thumb_url": str(thumb or ""),
                            "embed": embed_payload,
                        },
                    )
                except Exception:
                    pass
            # Add this mint to the "alert watchlist" for daily performance tracking.
            register_alerted_mint(item, alert_lines, at_iso=alert_at)
            cm = _safe_float(item.get("last_market_cap"))
            ca = _safe_float(item.get("ath_market_cap"))
            by_mint[mint] = _snapshot(item, cur_ids, ref_mc=cm, ref_ath=ca)
            sent += 1
            if send_delay_sec > 0:
                await asyncio.sleep(send_delay_sec)

    _save_by_mint(by_mint)
    stats = {"items": len(items), "queued": queued_total, "sent": sent}
    return sent, None, stats


# --- Call leaderboard (best call multiples across the overview snapshot) ---


def _parse_message_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        s = str(ts).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _call_within_age_days(c: Dict[str, Any], max_days: int) -> bool:
    """Call must have messageTs within the last max_days (UTC)."""
    dt = _parse_message_ts(c.get("messageTs"))
    if dt is None:
        return False
    age_sec = (datetime.now(timezone.utc) - dt).total_seconds()
    return age_sec >= 0 and age_sec <= max_days * 86400


def _call_within_last_hours(ts: Optional[str], hours: float) -> bool:
    """True if messageTs is within the last `hours` (rolling window, UTC)."""
    if hours <= 0:
        return True
    dt = _parse_message_ts(ts)
    if dt is None:
        return False
    age_sec = (datetime.now(timezone.utc) - dt).total_seconds()
    return age_sec >= 0 and age_sec <= hours * 3600.0


def _first_call_with_mc(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Chronologically first KOL call with a valid call MC (true first call on the token in the payload)."""
    calls = item.get("callsPreview") or item.get("calls") or []
    if not isinstance(calls, list) or not calls:
        return None
    dated: List[Tuple[datetime, Dict[str, Any]]] = []
    for c in calls:
        if not isinstance(c, dict):
            continue
        cmc = _safe_float(c.get("callMarketCap"))
        if cmc is None or cmc <= 0:
            continue
        ts = _parse_message_ts(c.get("messageTs"))
        if ts is None:
            continue
        dated.append((ts, c))
    if not dated:
        return None
    dated.sort(key=lambda x: x[0])
    return dated[0][1]


def _any_kol_call_within_last_hours(item: Dict[str, Any], hours: float) -> bool:
    """True if any caller row has messageTs within the rolling window (e.g. top movers gate)."""
    if hours <= 0:
        return True
    calls = item.get("callsPreview") or item.get("calls") or []
    if not isinstance(calls, list) or not calls:
        return False
    for c in calls:
        if isinstance(c, dict) and _call_within_last_hours(c.get("messageTs"), hours):
            return True
    return False


def _call_age_days(ts: Optional[str]) -> Optional[int]:
    dt = _parse_message_ts(ts)
    if dt is None:
        return None
    sec = int((datetime.now(timezone.utc) - dt).total_seconds())
    if sec < 0:
        return 0
    return sec // 86400


def _entry_for_leaderboard(
    item: Dict[str, Any],
    *,
    max_call_age_hours: float = 24.0,
) -> Optional[Dict[str, Any]]:
    """
    One row per token: **first** KOL call with MC must fall within the age window;
    ranked by **ATH** (market cap) reached. Uses true first call in the payload, not best multiple.
    """
    mint = _item_mint(item)
    if not mint:
        return None

    fc = _first_call_with_mc(item)
    if fc is None:
        return None
    if max_call_age_hours > 0 and not _call_within_last_hours(fc.get("messageTs"), max_call_age_hours):
        return None

    cur_mc = _safe_float(item.get("last_market_cap"))
    ath_usd = _safe_float(item.get("ath_market_cap"))
    if ath_usd is None or ath_usd <= 0:
        return None

    cmc = _safe_float(fc.get("callMarketCap"))
    peak = _safe_float(fc.get("peakMarketCap"))
    best_mult = 0.0
    if cmc and cmc > 0 and cur_mc and cur_mc > 0:
        best_mult = cur_mc / cmc
    else:
        m = _call_multiplier(fc)
        if m is not None and m > 0:
            best_mult = float(m)

    return {
        "sort_key": ath_usd,
        "ath_usd": ath_usd,
        "ticker": _item_ticker(item),
        "mint": mint,
        "best_mult": best_mult,
        "best_call": fc,
        "cur_mc": cur_mc,
        "call_mc": cmc,
        "peak_mc": peak,
        "item": item,
    }


def format_kolfi_leaderboard_window(hours: float) -> str:
    """Short label for embed + logs: 24h, 7d, 30d, or raw hours."""
    if hours <= 0:
        return "all-time"
    h = float(hours)
    if h >= 24:
        days = h / 24.0
        nd = int(round(days))
        if abs(days - float(nd)) < 0.02 and nd >= 1:
            return "24h" if nd == 1 else f"{nd}d"
    return f"{h:.0f}h"


def build_kolfi_leaderboard_embed(
    entries: List[Dict[str, Any]],
    *,
    brand_name: str = "Velcor3",
    embed_color: int = 0x5865F2,
    thumb_url: Optional[str] = None,
    max_call_age_hours: float = 24.0,
) -> Embed:
    """First KOL call within the window · ranked by ATH reached."""
    now = datetime.now(timezone.utc)
    embed = Embed(color=Color(embed_color), timestamp=now)
    win_label = format_kolfi_leaderboard_window(max_call_age_hours)
    embed.set_author(name=f"⚡ {brand_name}  ·  Top ATH · first KOL call · {win_label}")

    if not entries:
        win = win_label if max_call_age_hours > 0 else "any time"
        embed.description = (
            f"_No tokens whose **first KOL call** falls in the last **{win}** — check back on the next run._"
        )
        embed.set_footer(text=f"{brand_name}  ·  DYOR")
        return embed

    win = win_label if max_call_age_hours > 0 else "∞"
    intro = f"*First KOL call in last **{win}** · ranked by **ATH** (peak MC)*\n\u200b\n"

    lines: List[str] = []
    for rank, e in enumerate(entries, 1):
        tick = str(e.get("ticker") or "—")[:40]
        mult_s = _fmt_mult(e.get("best_mult"))
        bc = e.get("best_call") or {}
        who = _call_label(bc)
        kid = bc.get("kolXId")
        who_disp = f"[{who}](https://x.com/{str(kid).lstrip('@')})" if kid else who

        cmc = _format_mc(e.get("call_mc"))
        cur = _format_mc(e.get("cur_mc"))
        peak = _format_mc(e.get("peak_mc"))
        ath_s = _format_mc(e.get("ath_usd"))
        rel = _rel_time(bc.get("messageTs"))
        age_d = _call_age_days(bc.get("messageTs"))
        age_s = f"  ·  **{age_d}d** ago" if age_d is not None else ""

        mint = e.get("mint") or ""
        gmgn = f"https://gmgn.ai/sol/token/{mint}" if mint else None
        dex = (e.get("item") or {}).get("dexscreener_url") or (e.get("item") or {}).get("dexUrl")

        medal = ("🥇", "🥈", "🥉")[rank - 1] if rank <= 3 else f"`{rank}.`"
        peak_s = f"  ·  call peak **{peak}**" if peak else ""
        chart_link = f"[{tick}]({gmgn})" if gmgn else tick
        dex_link = f"  ·  [Dex]({dex})" if dex else ""

        line = (
            f"{medal} **{chart_link}**  ·  ATH **{ath_s}**{dex_link}\n"
            f"   {who_disp}  ·  first call **{cmc}**  →  now **{cur}**  ·  **{mult_s}** since call{peak_s}{age_s}  ·  _{rel}_"
        )
        lines.append(line)

    embed.description = (intro + "\n\n".join(lines))[:4096]

    if thumb_url:
        embed.set_thumbnail(url=thumb_url)

    tot = len(entries)
    embed.set_footer(
        text=f"{brand_name}  ·  top {tot} by ATH · first call in window  ·  Not financial advice"
    )
    return embed


async def run_kolfi_leaderboard_once(
    client: discord.Client,
    session: aiohttp.ClientSession,
    api_key: str,
    channel_id: int,
    *,
    brand_name: str = "Velcor3",
    embed_color: int = 0x202025,
    top_n: int = 10,
    max_call_age_hours: Optional[float] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Fetch full overview: **first** KOL call within the age window, ranked by **ATH** reached.
    Post a new embed each run (so role pings and history work; no in-place edit).
    """
    global _last_error
    hours = max_call_age_hours
    if hours is None:
        hours = float(getattr(_kolfi_config, "KOLFI_LEADERBOARD_MAX_CALL_AGE_HOURS", 24.0))

    async with _leaderboard_send_lock:
        items, err = await fetch_tokens_overview(
            session,
            api_key,
            limit=100,
            include_calls=50,
            max_pages=15,
        )
        if err and not items:
            return False, err or _last_error

        merge_callers_from_items(items)

        scored: List[Dict[str, Any]] = []
        for it in items:
            row = _entry_for_leaderboard(it, max_call_age_hours=float(hours))
            if row:
                scored.append(row)

        scored.sort(key=lambda x: float(x.get("sort_key") or 0), reverse=True)
        scored = scored[: max(1, top_n)]

        thumb: Optional[str] = None
        if scored:
            first_item = scored[0].get("item") or {}
            m0 = scored[0].get("mint") or ""
            if m0:
                thumb = await resolve_token_thumbnail(session, first_item, m0)

        embed = build_kolfi_leaderboard_embed(
            scored,
            brand_name=brand_name,
            embed_color=embed_color,
            thumb_url=thumb,
            max_call_age_hours=float(hours),
        )

        try:
            ch = client.get_channel(channel_id) or await client.fetch_channel(channel_id)
        except Exception as e:
            return False, f"Channel {channel_id}: {e}"
        if not ch:
            return False, f"Channel {channel_id} not found"

        await ch.send(embed=embed)
        return True, None


# --- Daily top movers (24h) ---


def _fmt_pct(x: Optional[float]) -> str:
    if x is None:
        return "—"
    try:
        return f"{x:+.1f}%"
    except Exception:
        return "—"


def _kolfi_h24_safety() -> Tuple[float, float]:
    """(max_abs_pct_to_show_numeric, min_liq_usd_to_trust_pct)."""
    cap = float(getattr(_kolfi_config, "KOLFI_H24_PCT_DISPLAY_CAP", 2500.0))
    min_liq = float(getattr(_kolfi_config, "KOLFI_H24_MIN_LIQ_USD_FOR_PCT", 25_000.0))
    return max(0.0, cap), max(0.0, min_liq)


def fmt_dex_24h_pct_display(
    pct: Optional[float],
    liquidity_usd: Optional[float] = None,
) -> str:
    """
    Dexscreener 24h %% — safe for user-facing copy.
    Hides or caps absurd values on thin pools (raw feed can show millions %%).
    """
    if pct is None:
        return "—"
    try:
        p = float(pct)
    except (TypeError, ValueError):
        return "—"
    if p != p:  # NaN
        return "—"
    cap, min_liq = _kolfi_h24_safety()
    if min_liq > 0 and liquidity_usd is not None and liquidity_usd < min_liq:
        return f"n/a (thin pool · liq under {_format_compact_vol(min_liq)})"
    if cap > 0 and abs(p) > cap:
        s = "+" if p > 0 else ""
        return f"{s}≥{cap:,.0f}% (volatile — check Dex chart)"
    return f"{p:+.1f}%"


def _h24_sort_key_movers(r: Dict[str, Any]) -> float:
    """Rank by trustworthy 24h move; thin / absurd %% pairs sink."""
    pct = _safe_float(r.get("h24_change_pct"))
    liq = _safe_float(r.get("liq_usd"))
    cap, min_liq = _kolfi_h24_safety()
    if pct is None:
        return -1e18
    if min_liq > 0 and (liq is None or liq < min_liq):
        return -1e12
    out = float(pct)
    if cap > 0 and abs(out) > cap:
        out = cap if out > 0 else -cap
    return out


def _best_recent_call(
    item: Dict[str, Any],
    *,
    max_age_days: int = 7,
) -> Optional[Dict[str, Any]]:
    calls = item.get("callsPreview") or item.get("calls") or []
    if not isinstance(calls, list) or not calls:
        return None
    # Keep only recent calls with a usable call MC
    recent: List[Dict[str, Any]] = []
    for c in calls:
        if not isinstance(c, dict):
            continue
        if not _call_within_age_days(c, max_age_days):
            continue
        cmc = _safe_float(c.get("callMarketCap"))
        if cmc is None or cmc <= 0:
            continue
        recent.append(c)
    if not recent:
        return None
    # Pick the most recent call (messageTs)
    recent.sort(
        key=lambda c: _parse_message_ts(c.get("messageTs"))
        or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    return recent[0]


def _earliest_call_with_mc(
    item: Dict[str, Any],
    *,
    max_age_days: int = 30,
) -> Optional[Dict[str, Any]]:
    """Chronologically first KOL call with a valid call MC (for recap multiplier vs current MC)."""
    calls = item.get("callsPreview") or item.get("calls") or []
    if not isinstance(calls, list) or not calls:
        return None
    dated: List[Tuple[datetime, Dict[str, Any]]] = []
    for c in calls:
        if not isinstance(c, dict):
            continue
        if not _call_within_age_days(c, max_age_days):
            continue
        cmc = _safe_float(c.get("callMarketCap"))
        if cmc is None or cmc <= 0:
            continue
        ts = _parse_message_ts(c.get("messageTs"))
        if ts is None:
            continue
        dated.append((ts, c))
    if not dated:
        return None
    dated.sort(key=lambda x: x[0])
    return dated[0][1]


def _format_earliest_kol_call_line(item: Dict[str, Any]) -> Optional[str]:
    """One markdown line: earliest KOL, entry MC, x vs now MC, time ago."""
    ec = _earliest_call_with_mc(item, max_age_days=30)
    if not ec:
        return None
    who = _call_label(ec)
    kid = ec.get("kolXId") or ec.get("kol_x_id")
    who_link = f"[{who}](https://x.com/{str(kid).lstrip('@')})" if kid else who
    cmc_f = _safe_float(ec.get("callMarketCap"))
    cmc = _format_mc(cmc_f) if cmc_f else "—"
    now_mc = _safe_float(item.get("last_market_cap"))
    mult_s = "—"
    if cmc_f and cmc_f > 0 and now_mc and now_mc > 0:
        mult_s = _fmt_mult(now_mc / cmc_f)
    rel = _rel_time(ec.get("messageTs"))
    ago = f"{rel} ago" if rel else "—"
    return f"{who_link} · entry **{cmc}** · **{mult_s}** · _{ago}_"


def _call_display(call: Dict[str, Any]) -> str:
    who = _call_label(call)
    kid = call.get("kolXId") or call.get("kol_x_id")
    if kid:
        return f"[{who}](https://x.com/{str(kid).lstrip('@')})"
    return who


def build_kolfi_top_movers_embed(
    rows: List[Dict[str, Any]],
    *,
    brand_name: str = "Velcor3",
    embed_color: int = 0x202025,
    top_n: int = 10,
) -> Embed:
    now = datetime.now(timezone.utc)
    embed = Embed(color=Color(embed_color), timestamp=now)
    embed.set_author(name=f"⚡ {brand_name}  ·  Top movers 24h  ·  KOL call in window  ·  daily")

    if not rows:
        embed.description = (
            "_No movers (need Dex data + liq floor + at least one KOL call in the configured window)._"
        )
        embed.set_footer(text=f"{brand_name}  ·  DYOR")
        return embed

    top_n = max(1, min(25, int(top_n)))
    shown = rows[:top_n]

    lines: List[str] = []
    for rank, r in enumerate(shown, 1):
        tick = str(r.get("ticker") or "—")[:40]
        mint = str(r.get("mint") or "")
        chg = r.get("h24_change_pct")
        liq = r.get("liq_usd")
        vol = r.get("vol_h24_usd")
        age = r.get("pair_age_days")
        dex = r.get("pair_url") or r.get("dex_url")
        now_mc = _safe_float(r.get("now_mc_usd"))
        call_mc = _safe_float(r.get("call_mc_usd"))
        call_x = _safe_float(r.get("call_x"))
        caller = str(r.get("caller") or "").strip()
        call_age = str(r.get("call_age") or "").strip()
        gmgn = f"https://gmgn.ai/sol/token/{mint}" if mint else None

        medal = ("🥇", "🥈", "🥉")[rank - 1] if rank <= 3 else f"`{rank}.`"
        name = f"[{tick}]({gmgn})" if gmgn else tick
        dex_link = f" · [Dex]({dex})" if dex else ""

        bits = []
        if liq is not None:
            bits.append(f"liq {_format_compact_vol(liq)}")
        if vol is not None:
            bits.append(f"vol {_format_compact_vol(vol)}")
        if age is not None:
            try:
                bits.append(f"pair {age:.1f}d")
            except Exception:
                pass
        if caller and call_mc is not None:
            x_part = f"{_fmt_mult(call_x)}" if call_x is not None else "—"
            age_part = f" · {call_age} ago" if call_age else ""
            now_part = f" → now {_format_mc(now_mc)}" if now_mc is not None else ""
            bits.append(f"caller {caller} · called at {_format_mc(call_mc)}{now_part} · {x_part}{age_part}")
        meta = " · ".join(bits) if bits else "—"

        chg_s = fmt_dex_24h_pct_display(_safe_float(chg), _safe_float(liq))
        lines.append(f"{medal} **{name}**{dex_link}\n   **{chg_s}**  ·  {meta}")

    embed.description = ("\n\n".join(lines))[:4096]
    embed.set_footer(
        text=f"{brand_name}  ·  24h %% from Dexscreener (capped / thin pools flagged)  ·  Not financial advice"
    )
    return embed


def build_kolfi_alert_watchlist_embed(
    rows: List[Dict[str, Any]],
    *,
    brand_name: str = "Velcor3",
    embed_color: int = 0x202025,
    top_n: int = 10,
) -> Embed:
    now = datetime.now(timezone.utc)
    embed = Embed(color=Color(embed_color), timestamp=now)
    embed.set_author(name=f"⚡ {brand_name}  ·  Token alerts  ·  top performers (since alert)  ·  daily")

    if not rows:
        embed.description = "_No alerted coins in watchlist yet (or could not fetch market data)._"[:4096]
        embed.set_footer(text=f"{brand_name}  ·  DYOR")
        return embed

    top_n = max(1, min(25, int(top_n)))
    shown = rows[:top_n]
    lines: List[str] = []
    for rank, r in enumerate(shown, 1):
        tick = str(r.get("ticker") or "—")[:40]
        mint = str(r.get("mint") or "")
        gmgn = f"https://gmgn.ai/sol/token/{mint}" if mint else None
        name = f"[{tick}]({gmgn})" if gmgn else tick

        mult = _safe_float(r.get("since_alert_x"))
        base_mc = _safe_float(r.get("baseline_mc_usd"))
        cur_mc = _safe_float(r.get("now_mc_usd"))
        age = str(r.get("alert_age") or "").strip()
        chg24 = _safe_float(r.get("h24_change_pct"))
        dex = r.get("pair_url") or r.get("dex_url")
        dex_link = f" · [Dex]({dex})" if dex else ""

        caller = str(r.get("caller") or "").strip()
        call_mc = _safe_float(r.get("call_mc_usd"))
        call_age = str(r.get("call_age") or "").strip()
        call_x = _safe_float(r.get("call_x"))

        medal = ("🥇", "🥈", "🥉")[rank - 1] if rank <= 3 else f"`{rank}.`"

        sub: List[str] = []
        if mult is not None:
            sub.append(f"**Since alert:** **{_fmt_mult(mult)}**")
        if age:
            sub.append(f"**Alert:** {age} ago")
        liq_w = _safe_float(r.get("liquidity_usd"))
        sub.append(f"**24h:** {fmt_dex_24h_pct_display(chg24, liq_w)}")
        if base_mc is not None and cur_mc is not None:
            sub.append(f"**MC:** {_format_mc(base_mc)} → {_format_mc(cur_mc)}")

        if caller and call_mc is not None:
            cx = _fmt_mult(call_x) if call_x is not None else "—"
            ca = f" · {call_age} ago" if call_age else ""
            sub.append(f"**Call:** {caller} · entry {_format_mc(call_mc)} · **{cx}**{ca}")

        header = f"{medal} **{name}**{dex_link}"
        if sub:
            body = "\n".join(f"   {s}" for s in sub)
            lines.append(f"{header}\n{body}")
        else:
            lines.append(header)

    embed.description = ("\n\n".join(lines))[:4096]
    embed.set_footer(text=f"{brand_name}  ·  watchlist = coins alerted by our bot  ·  Not financial advice")
    return embed


def build_kolfi_new_alerts_recap_embed(
    rows: List[Dict[str, Any]],
    *,
    brand_name: str = "Velcor3",
    embed_color: int = 0x202025,
    top_n: int = 10,
) -> Embed:
    now = datetime.now(timezone.utc)
    embed = Embed(color=Color(embed_color), timestamp=now)
    embed.set_author(name=f"⚡ {brand_name}  ·  Our alerts  ·  top 24h %%  ·  daily")

    if not rows:
        embed.description = (
            "_No qualifying alerts: none in the last **24h** with Dex data, or first KOL call filter excluded them._"
        )
        embed.set_footer(text=f"{brand_name}  ·  DYOR")
        return embed

    top_n = max(1, min(25, int(top_n)))
    shown = rows[:top_n]

    lines: List[str] = []
    for rank, r in enumerate(shown, 1):
        tick = str(r.get("ticker") or "—")[:40]
        mint = str(r.get("mint") or "")
        gmgn = f"https://gmgn.ai/sol/token/{mint}" if mint else None
        name = f"[{tick}]({gmgn})" if gmgn else tick

        base_ath = _safe_float(r.get("baseline_ath_usd"))
        cur_ath = _safe_float(r.get("now_ath_usd"))
        chg24 = _safe_float(r.get("h24_change_pct"))
        dex = r.get("pair_url") or r.get("dex_url")
        dex_link = f" · [Dex]({dex})" if dex else ""

        caller = str(r.get("caller") or "").strip()
        call_mc = _safe_float(r.get("call_mc_usd"))
        call_age = str(r.get("call_age") or "").strip()

        sub: List[str] = []
        peak_ath: Optional[float] = None
        if base_ath is not None and cur_ath is not None:
            peak_ath = max(base_ath, cur_ath)
        elif cur_ath is not None:
            peak_ath = cur_ath
        elif base_ath is not None:
            peak_ath = base_ath

        ca = f" · {call_age} ago" if call_age else ""
        if caller and call_mc is not None:
            sub.append(f"**First call:** {caller} · entry **{_format_mc(call_mc)}**{ca}")

        if peak_ath is not None and peak_ath > 0:
            x_ath = (
                _fmt_mult(peak_ath / call_mc)
                if call_mc is not None and call_mc > 0
                else "—"
            )
            x_note = (
                f" · **{x_ath}** from first-call entry → ATH"
                if x_ath != "—"
                else ""
            )
            sub.append(f"**ATH (market cap):** **{_format_mc(peak_ath)}**{x_note}")

        liq_w = _safe_float(r.get("liquidity_usd"))
        sub.append(f"**24h:** {fmt_dex_24h_pct_display(chg24, liq_w)}")

        header = f"`{rank}.` **{name}**{dex_link}"
        if sub:
            body = "\n".join(f"   {s}" for s in sub)
            lines.append(f"{header}\n{body}")
        else:
            lines.append(header)

    embed.description = (
        "*Ranked by **24h %%** (Dexscreener; thin pools flagged). Mints = tokens **we first alerted** in the last **24h**; "
        "**First call** is the earliest KOL row with MC (same rolling hour cap as the bot config, default 24h).*"
        "\n\u200b\n"
        + "\n\n".join(lines)
    )[:4096]
    embed.set_footer(
        text=f"{brand_name}  ·  our alerts · 24h %% · Not financial advice"
    )
    return embed


async def run_kolfi_alert_watchlist_daily_once(
    client: discord.Client,
    session: aiohttp.ClientSession,
    api_key: str,
    channel_id: int,
    *,
    brand_name: str = "Velcor3",
    embed_color: int = 0x202025,
    top_n: int = 10,
    concurrency: int = 12,
    max_watchlist: int = 500,
    max_entry_age_days: int = 30,
) -> Tuple[bool, Optional[str]]:
    """
    Daily recap: mints **we first alerted** in the last 24h (watchlist `first_alert_ts`), Dex-enriched 24h %%,
    ranked by **24h performance** (same safety sort as Kolfi movers). Rows require KOL **first call**
    `messageTs` within `KOLFI_ALERT_RECAP_FIRST_CALL_MAX_HOURS` when that value is > 0 (default 24).
    """
    if not channel_id:
        return False, "Missing channel_id"

    data = _load_alert_watchlist()
    by_mint = data.get("by_mint") if isinstance(data, dict) else None
    if not isinstance(by_mint, dict) or not by_mint:
        embed = build_kolfi_alert_watchlist_embed([], brand_name=brand_name, embed_color=embed_color, top_n=top_n)
        ch = client.get_channel(channel_id) or await client.fetch_channel(channel_id)
        await ch.send(embed=embed)
        return True, None

    # Keep only mints **we first alerted** in the last 24h (recap).
    # Using last_alert_ts wrongly re-included old coins after any re-fire (MC / new callers).
    now = datetime.now(timezone.utc)
    window_sec = 24 * 3600
    entries: List[Dict[str, Any]] = []
    for mint, ent in by_mint.items():
        if not isinstance(ent, dict):
            continue
        ts = ent.get("first_alert_ts") or ent.get("last_alert_ts")
        dt = None
        if ts:
            try:
                dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
            except Exception:
                dt = None
        if dt is None:
            continue
        if (now - dt).total_seconds() > window_sec:
            continue
        entries.append(ent)

    # Most recent first, cap list size
    entries.sort(key=lambda e: str(e.get("first_alert_ts") or e.get("last_alert_ts") or ""), reverse=True)
    entries = entries[: max(1, int(max_watchlist))]

    # Pull current token snapshot once so we can show current ATH (pair API doesn't expose ATH).
    kolfi_items, _ = await fetch_tokens_overview(
        session,
        api_key,
        limit=100,
        include_calls=30,
        max_pages=15,
    )
    kolfi_by_mint: Dict[str, Dict[str, Any]] = {}
    for it in kolfi_items or []:
        if not isinstance(it, dict):
            continue
        m = _item_mint(it)
        if m:
            kolfi_by_mint[m] = it

    sem = asyncio.Semaphore(max(1, min(30, int(concurrency))))

    async def _row(ent: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        mint = str(ent.get("mint") or "").strip()
        if not mint:
            return None
        base_ath = _safe_float(ent.get("baseline_ath_usd"))
        if base_ath is None or base_ath <= 0:
            base_ath = _safe_float(ent.get("last_kolfi_ath_usd"))
        if base_ath is None or base_ath <= 0:
            return None
        first_ts = ent.get("first_alert_ts")
        alert_age = _rel_time(first_ts) if first_ts else ""
        last_ts = ent.get("last_alert_ts")
        alerted = _rel_time(last_ts) if last_ts else ""

        async with sem:
            try:
                enr = await enrich_solana_mint(session, mint, birdeye_api_key="")
            except Exception:
                return None
        dex = (enr.get("dexscreener") or {}) if isinstance(enr, dict) else {}
        if not dex or not dex.get("ok"):
            return None
        k_it = kolfi_by_mint.get(mint) or {}
        now_ath = _safe_float(k_it.get("ath_market_cap"))
        if now_ath is None or now_ath <= 0:
            # fallback to last known ATH in watchlist
            now_ath = _safe_float(ent.get("last_kolfi_ath_usd"))
        if now_ath is None or now_ath <= 0:
            return None
        since_ath_x = now_ath / base_ath if base_ath > 0 else None

        caller = ""
        call_mc = None
        call_age = ""
        call_max_h = float(getattr(_kolfi_config, "KOLFI_ALERT_RECAP_FIRST_CALL_MAX_HOURS", 0.0))
        use_call = _first_call_with_mc(k_it) if k_it else None
        if use_call is None and isinstance(ent.get("last_call"), dict):
            lc = ent["last_call"]
            use_call = {
                "kolUsername": lc.get("who"),
                "kolXId": lc.get("kolXId"),
                "callMarketCap": lc.get("callMarketCap"),
                "messageTs": lc.get("messageTs"),
            }
        if use_call is None:
            return None
        if call_max_h > 0 and not _call_within_last_hours(use_call.get("messageTs"), call_max_h):
            return None
        who = _call_label(use_call)
        kid = use_call.get("kolXId") or use_call.get("kol_x_id")
        if who:
            caller = _call_display({"kolUsername": who, "kolXId": kid})
        call_mc = _safe_float(use_call.get("callMarketCap"))
        call_age = _rel_time(use_call.get("messageTs"))

        if not caller or call_mc is None:
            return None

        liq_u = _safe_float(dex.get("liquidity_usd"))
        return {
            "mint": mint,
            "ticker": ent.get("ticker") or "—",
            "baseline_ath_usd": base_ath,
            "now_ath_usd": now_ath,
            "since_alert_ath_x": since_ath_x,
            "alert_age": alert_age,
            "alerted": alerted,
            "h24_change_pct": _safe_float(dex.get("price_change_h24_pct")),
            "liquidity_usd": liq_u,
            "liq_usd": liq_u,
            "pair_url": dex.get("pair_url"),
            "dex_url": dex.get("pair_url") or dex.get("pair_url"),
            "caller": caller,
            "call_mc_usd": call_mc,
            "call_age": call_age,
        }

    rows_raw = await asyncio.gather(*[_row(e) for e in entries])
    rows = [r for r in rows_raw if isinstance(r, dict)]
    # Rank by trustworthy 24h %% (same key as global Kolfi movers feed)
    rows.sort(key=_h24_sort_key_movers, reverse=True)

    embed = build_kolfi_new_alerts_recap_embed(
        rows,
        brand_name=brand_name,
        embed_color=embed_color,
        top_n=top_n,
    )

    ch = client.get_channel(channel_id) or await client.fetch_channel(channel_id)
    if not ch:
        return False, f"Channel {channel_id} not found"
    await ch.send(embed=embed)
    return True, None


async def fetch_kolfi_top_movers_rows(
    session: aiohttp.ClientSession,
    api_key: str,
    *,
    max_pages: int = 10,
    concurrency: int = 10,
    min_liquidity_usd: float = 25_000.0,
    require_kol_call_within_hours: Optional[float] = None,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Top 24h %% among overview mints that pass liquidity + Dex enrichment.
    Optionally require at least one KOL call within `require_kol_call_within_hours` (default from config).
    """
    global _last_error

    kol_hours = require_kol_call_within_hours
    if kol_hours is None:
        kol_hours = float(
            getattr(_kolfi_config, "KOLFI_TOP_MOVERS_KOL_CALL_MAX_HOURS", 24.0)
        )

    items, err = await fetch_tokens_overview(
        session,
        api_key,
        limit=100,
        include_calls=30,
        max_pages=max(1, int(max_pages)),
    )
    if err and not items:
        return [], err or _last_error

    merge_callers_from_items(items)

    seen: Set[str] = set()
    base: List[Dict[str, Any]] = []
    for it in items:
        mint = _item_mint(it)
        if not mint or mint in seen:
            continue
        seen.add(mint)
        base.append(it)

    sem = asyncio.Semaphore(max(1, min(30, int(concurrency))))

    async def _enrich_one(it: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        mint = _item_mint(it)
        if not mint:
            return None
        if kol_hours > 0 and not _any_kol_call_within_last_hours(it, float(kol_hours)):
            return None
        async with sem:
            try:
                enr = await enrich_solana_mint(session, mint, birdeye_api_key="")
            except Exception:
                return None
        dex = (enr.get("dexscreener") or {}) if isinstance(enr, dict) else {}
        if not dex or not dex.get("ok"):
            return None
        liq = _safe_float(dex.get("liquidity_usd"))
        if liq is not None and liq < float(min_liquidity_usd):
            return None
        chg = _safe_float(dex.get("price_change_h24_pct"))
        if chg is None:
            return None
        now_mc = _safe_float(it.get("last_market_cap"))
        bc = _best_recent_call(it, max_age_days=7)
        caller = ""
        call_mc = None
        call_x = None
        call_age = ""
        if bc:
            caller = _call_display(bc)
            call_mc = _safe_float(bc.get("callMarketCap"))
            if call_mc and call_mc > 0 and now_mc and now_mc > 0:
                call_x = now_mc / call_mc
            call_age = _rel_time(bc.get("messageTs"))
        return {
            "mint": mint,
            "ticker": _item_ticker(it),
            "h24_change_pct": chg,
            "liq_usd": liq,
            "vol_h24_usd": _safe_float(dex.get("volume_h24_usd")),
            "pair_age_days": _safe_float(dex.get("pair_age_days")),
            "pair_url": dex.get("pair_url"),
            "dex_url": it.get("dexscreener_url") or it.get("dexUrl") or it.get("dexscreenerUrl"),
            "now_mc_usd": now_mc,
            "caller": caller,
            "call_mc_usd": call_mc,
            "call_x": call_x,
            "call_age": call_age,
        }

    enriched = await asyncio.gather(*[_enrich_one(it) for it in base])
    rows = [r for r in enriched if isinstance(r, dict)]
    rows.sort(key=_h24_sort_key_movers, reverse=True)
    return rows, None


async def run_kolfi_top_movers_daily_once(
    client: discord.Client,
    session: aiohttp.ClientSession,
    api_key: str,
    channel_id: int,
    *,
    brand_name: str = "Velcor3",
    embed_color: int = 0x202025,
    top_n: int = 10,
    max_pages: int = 10,
    concurrency: int = 10,
    min_liquidity_usd: float = 25_000.0,
) -> Tuple[bool, Optional[str]]:
    """
    Daily leaderboard: top performing coins over 24h, based on 24h % change
    for the best-liquidity pair of each Solana mint in the overview snapshot.
    """
    if not channel_id:
        return False, "Missing channel_id"

    rows, err = await fetch_kolfi_top_movers_rows(
        session,
        api_key,
        max_pages=max_pages,
        concurrency=concurrency,
        min_liquidity_usd=min_liquidity_usd,
    )
    if err and not rows:
        return False, err

    embed = build_kolfi_top_movers_embed(
        rows,
        brand_name=brand_name,
        embed_color=embed_color,
        top_n=top_n,
    )

    try:
        ch = client.get_channel(channel_id) or await client.fetch_channel(channel_id)
    except Exception as e:
        return False, f"Channel {channel_id}: {e}"
    if not ch:
        return False, f"Channel {channel_id} not found"

    await ch.send(embed=embed)
    return True, None
