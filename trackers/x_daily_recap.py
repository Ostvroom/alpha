"""
Plain-text bodies for daily X recaps (posted via twikit / TwitterClient.create_tweet).
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

from trackers.kolfi_tokens_client import fmt_dex_24h_pct_display


def _truncate(s: str, max_len: int = 280) -> str:
    s = s.strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 1].rstrip() + "…"


def format_x_top_movers_tweet(
    rows: List[Dict[str, Any]],
    *,
    brand_name: str = "Velcor3",
    top_n: int = 3,
) -> str:
    """Solana token movers (same data as Discord daily top movers embed)."""
    top_n = max(1, min(5, int(top_n)))
    lines: List[str] = [
        f"⚡ {brand_name} · Solana 24h movers (top {top_n})",
        "",
    ]
    if not rows:
        lines.append("No qualifying tokens today (liquidity / data).")
        lines.append("")
        lines.append("DYOR · NFA · #Velcor3")
        return _truncate("\n".join(lines))

    for i, r in enumerate(rows[:top_n], 1):
        tick = str(r.get("ticker") or "?").strip()[:16]
        pct = fmt_dex_24h_pct_display(r.get("h24_change_pct"), r.get("liq_usd"))
        lines.append(f"{i}) ${tick} {pct}")

    lines.append("")
    lines.append("DYOR · NFA · #Velcor3 #Solana")
    return _truncate("\n".join(lines))


def format_x_daily_finds_tweet(
    rows: List[Tuple],
    *,
    brand_name: str = "Velcor3",
    top_n: int = 3,
) -> str:
    """
    Rows from database.get_projects_finds_24h:
    (twitter_id, handle, name, description, created_at, alerted_at, ai_category, ai_summary, followers_count)
    """
    top_n = max(1, min(5, int(top_n)))
    lines: List[str] = [
        f"🔍 {brand_name} · New finds (24h) · top {top_n}",
        "",
    ]
    if not rows:
        lines.append("No new Velcor3 finds in the last 24h.")
        lines.append("")
        lines.append("#Velcor3 #NFT #Web3")
        return _truncate("\n".join(lines))

    for i, row in enumerate(rows[:top_n], 1):
        _pid, handle, name, _desc, _created_at, _alerted_at, ai_cat, _ai_sum, followers = row
        handle = str(handle or "").strip()
        nm = (name or "").strip()
        cat = (str(ai_cat).strip() if ai_cat else "") or "—"
        try:
            fc = int(followers) if followers is not None else None
        except (TypeError, ValueError):
            fc = None
        if fc is None:
            fol = "—"
        elif fc >= 1_000_000:
            fol = f"{fc / 1_000_000:.1f}M".replace(".0M", "M")
        elif fc >= 1_000:
            fol = f"{fc / 1_000:.1f}k".replace(".0k", "k")
        else:
            fol = str(fc)
        tail = f" · {cat} · {fol} flw"
        if nm and nm.lower() != handle.lower():
            lines.append(f"{i}. @{handle} — {nm[:28]}{tail}")
        else:
            lines.append(f"{i}. @{handle}{tail}")

    lines.append("")
    lines.append("#Velcor3 #NFT #Web3")
    return _truncate("\n".join(lines))
