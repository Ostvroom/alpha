"""
Fetch & parse https://daily-mints.com mint calendar (public HTML + JSON-LD).
The /api/mints endpoint returns 401 without credentials; we scrape SSR-visible HTML instead.
"""
from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import discord

BASE = "https://daily-mints.com"
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Nav / filter links on the site (not individual mint pages)
_EXCLUDE_MINTS_PATHS = frozenset(
    {
        "/mints/today",
        "/mints/free",
        "/mints/trending",
        "/mints/this-week",
        "/mints/ethereum",
        "/mints/base",
        "/mints/solana",
        "/mints/abs",
        "/mints/eth",
    }
)

# SSR may use absolute https://daily-mints.com/... hrefs — normalize to path for fetch_mint_detail
_DM_ORIGIN_RE = re.compile(r"^https?://(?:www\.)?daily-mints\.com", re.I)


def _normalize_dm_path(href: str) -> Optional[str]:
    """Turn a mint listing href into a path like /mint/123 or /mints/some-slug."""
    if not href or not href.strip():
        return None
    h = href.strip().split("?")[0].split("#")[0].rstrip("/")
    if h.startswith("/"):
        path = h
    else:
        m = _DM_ORIGIN_RE.sub("", h)
        if not m.startswith("/"):
            return None
        path = m
    if path.startswith("/mint/") and re.match(r"^/mint/\d+$", path):
        return path
    if path.startswith("/mints/"):
        if path in _EXCLUDE_MINTS_PATHS:
            return None
        if path.startswith("/mints/eth/") or path.startswith("/mints/sol"):
            return None
        if re.match(r"^/mints/[a-zA-Z0-9_-]+$", path):
            return path
    return None

_last_error: Optional[str] = None


def get_last_daily_mints_error() -> Optional[str]:
    return _last_error


# Month name → number (case-insensitive)
_MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _mint_date_is_today(date_str: str) -> bool:
    """
    Return True if `date_str` (as scraped from daily-mints.com) matches today (UTC).
    Handles formats:
      "Monday, April 6, 2026"  ← actual site format
      "April 7, 2026" / "Apr 7, 2026" / "April 7" / "Apr 7"
      "2026-04-07"
      "7 April 2026" / "7 Apr 2026"
    Returns True when unparseable (fail-open = don't suppress).
    """
    if not date_str or not date_str.strip():
        return True  # no date info → don't suppress
    s = date_str.strip()
    today = datetime.now(timezone.utc).date()

    # Strip leading day-of-week: "Monday, April 6, 2026" → "April 6, 2026"
    s = re.sub(r"^[A-Za-z]+,\s*", "", s)

    # ISO: 2026-04-07
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", s)
    if m:
        try:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            return datetime(y, mo, d).date() == today
        except Exception:
            return True

    # "April 7, 2026" / "Apr 7, 2026" / "April 7" / "Apr 7"
    m = re.match(r"^([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?(?:,\s*(\d{4}))?", s)
    if m:
        mon_name = m.group(1).lower()
        day = int(m.group(2))
        year = int(m.group(3)) if m.group(3) else today.year
        mo = _MONTH_MAP.get(mon_name)
        if mo:
            try:
                return datetime(year, mo, day).date() == today
            except Exception:
                return True

    # "7 April 2026" / "7 Apr 2026"
    m = re.match(r"^(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)(?:\s+(\d{4}))?", s)
    if m:
        day = int(m.group(1))
        mon_name = m.group(2).lower()
        year = int(m.group(3)) if m.group(3) else today.year
        mo = _MONTH_MAP.get(mon_name)
        if mo:
            try:
                return datetime(year, mo, day).date() == today
            except Exception:
                return True

    return True  # unknown format → don't suppress


def twitter_handle_from_url(url: Optional[str]) -> Optional[str]:
    """Extract screen name from x.com / twitter.com URL."""
    if not url:
        return None
    m = re.search(r"(?:twitter\.com|x\.com)/@?([^/?#]+)", url, re.I)
    if not m:
        return None
    h = m.group(1).strip()
    if h.lower() in ("intent", "share", "home", "search"):
        return None
    return h.lstrip("@") or None


@dataclass
class DailyMintDetail:
    """One mint — fields may be missing on sparse pages."""

    source_url: str
    name: str = "Unknown"
    chain: str = ""
    risk_label: str = ""  # Low / Medium / High + Risk
    mint_date: str = ""
    price: str = ""
    supply: str = ""
    ai_score: Optional[int] = None
    ai_verdict: str = ""
    recommendation: str = ""  # WATCH, etc.
    analyzed_at: str = ""
    green_flags: List[str] = field(default_factory=list)
    red_flags: List[str] = field(default_factory=list)
    twitter_url: Optional[str] = None
    discord_url: Optional[str] = None
    website_url: Optional[str] = None
    json_ld: Optional[Dict[str, Any]] = None


def _extract_paths_from_index(html: str, today_only: bool = False) -> List[str]:
    """
    Preserve first-seen order; dedupe.
    today_only=True: extract ONLY links found inside the 'Minting Today' section of /mints.
    This is the most reliable filter — the site's own section grouping is correct even
    when individual detail pages haven't been updated yet.
    """
    seen = set()
    ordered: List[str] = []

    def push(p: str) -> None:
        if p in seen:
            return
        seen.add(p)
        ordered.append(p)

    def _collect(chunk: str) -> None:
        # Relative paths (legacy SSR)
        for m in re.finditer(r'href="(/mint/\d+)"', chunk):
            push(m.group(1))
        for m in re.finditer(r'href="(/mints/[a-zA-Z0-9_-]+)"', chunk):
            path = m.group(1)
            if path in _EXCLUDE_MINTS_PATHS:
                continue
            if path.startswith("/mints/eth/") or path.startswith("/mints/sol"):
                continue
            push(path)
        # Absolute URLs (current site often emits full daily-mints.com links)
        for m in re.finditer(
            r'href="(https?://(?:www\.)?daily-mints\.com/mint/\d+[^"]*)"',
            chunk,
            re.I,
        ):
            p = _normalize_dm_path(m.group(1))
            if p:
                push(p)
        for m in re.finditer(
            r'href="(https?://(?:www\.)?daily-mints\.com/mints/[a-zA-Z0-9_-]+[^"]*)"',
            chunk,
            re.I,
        ):
            p = _normalize_dm_path(m.group(1))
            if p:
                push(p)

    if today_only:
        # The full /mints page sections: "Minting Today(6)", "Tomorrow(2)", "Upcoming(16)"
        # Strategy: find where "Minting Today" heading ENDS (closing tag), then grab
        # everything up to the next section heading. This avoids the regex eating links.
        today_m = re.search(r"Minting\s+Today", html, re.I)
        if today_m:
            # Advance past the closing tag of this heading (</h1>, </h2>, </h3>, </div>, etc.)
            after_heading_start = today_m.end()
            closing = re.search(r"</h[123]>|</div>", html[after_heading_start:])
            if closing:
                section_start = after_heading_start + closing.end()
            else:
                section_start = after_heading_start

            remainder = html[section_start:]
            # Stop at the next major calendar section only. Do NOT use a bare <h3> match —
            # each mint card has an <h3> title and would truncate after the first mint.
            next_section = re.search(
                r"Tomorrow|Upcoming|This\s+Week|Free\s+Only",
                remainder,
                re.I,
            )
            chunk = remainder[: next_section.start()] if next_section else remainder
            _collect(chunk)
            if ordered:
                return ordered
        # Fallback: couldn't isolate section → collect everything
        _collect(html)
        return ordered

    # Normal (non-today) extraction: entire page
    _collect(html)
    return ordered


async def fetch_text(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    global _last_error
    try:
        async with session.get(
            url, headers=DEFAULT_HEADERS, timeout=aiohttp.ClientTimeout(total=25)
        ) as resp:
            if resp.status != 200:
                _last_error = f"{url} -> HTTP {resp.status}"
                return None
            return await resp.text()
    except Exception as e:
        _last_error = f"{url} -> {e}"
        return None


async def fetch_index_paths(
    session: aiohttp.ClientSession,
    scope: str = "all",
) -> List[str]:
    """
    scope='today':
      PRIMARY → fetch /mints (full listing) and extract ONLY the "Minting Today" section.
                The full listing updates faster than /mints/today and groups correctly even
                when individual detail pages still show yesterday's date.
      FALLBACK → /mints/today if the primary yields nothing.

    scope='all':
      → fetch /mints and return all links.
    """
    if scope.lower() == "today":
        # Primary: full listing, section-isolated
        full_html = await fetch_text(session, f"{BASE}/mints")
        if full_html:
            paths = _extract_paths_from_index(full_html, today_only=True)
            if paths:
                return paths
        # Fallback: dedicated today page
        today_html = await fetch_text(session, f"{BASE}/mints/today")
        if today_html:
            return _extract_paths_from_index(today_html, today_only=False)
        return []

    # scope='all'
    html = await fetch_text(session, f"{BASE}/mints")
    if not html:
        return []
    return _extract_paths_from_index(html, today_only=False)


def _parse_json_ld_event(html: str) -> Optional[Dict[str, Any]]:
    for m in re.finditer(
        r'<script type="application/ld\+json"[^>]*>(.*?)</script>',
        html,
        re.DOTALL,
    ):
        raw = m.group(1).strip()
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if data.get("@type") == "Event":
            return data
    return None


def _parse_ai_score(html: str) -> Optional[int]:
    m = re.search(r'text-5xl font-bold[^>]*>(\d+)</div>', html)
    if m:
        return int(m.group(1))
    m = re.search(r"AI Score (\d+)/100", html)
    if m:
        return int(m.group(1))
    return None


def _parse_meta_block(html: str) -> Tuple[str, str, str, str, str, str]:
    """Name, chain, risk, date, price, supply from visible HTML."""
    name = ""
    nm = re.search(r'<h1[^>]*class="[^"]*text-2xl[^"]*"[^>]*>([^<]+)</h1>', html)
    if not nm:
        nm = re.search(r"<h1[^>]*>([^<]+)</h1>", html)
    if nm:
        name = nm.group(1).strip()

    chain = ""
    cm = re.search(
        r'<span[^>]*>(ETH|BASE|SOL|Abstract)</span>',
        html,
    )
    if cm:
        chain = cm.group(1)

    risk = ""
    rm = re.search(
        r"text-(?:yellow|green|red)-400[^>]*>(Low|Medium|High)(?:<!--[^>]*-->\s*)?Risk",
        html,
    )
    if rm:
        risk = f"{rm.group(1)} Risk"
    if not risk:
        # e.g. <span class="...text-yellow-400">Medium<!-- --> Risk</span>
        rm2 = re.search(
            r"(Low|Medium|High)\s*(?:<!--[^>]*-->\s*)?Risk",
            html,
            re.I,
        )
        if rm2:
            risk = f"{rm2.group(1).title()} Risk"
    if not risk:
        # Title / meta sometimes includes "High Risk" etc.
        rm3 = re.search(
            r"\b(Low|Medium|High)\s+Risk\b",
            html,
            re.I,
        )
        if rm3:
            risk = f"{rm3.group(1).title()} Risk"

    date_s, price_s, supply_s = "", "", ""
    for label in ("Date", "Price", "Supply"):
        xm = re.search(
            rf'{label}</span><p class="text-white font-medium">([^<]*)</p>',
            html,
        )
        if xm:
            val = xm.group(1).strip()
            if label == "Date":
                date_s = val
            elif label == "Price":
                price_s = val
            else:
                supply_s = val

    return name, chain, risk, date_s, price_s, supply_s


def _parse_verdict_block(html: str) -> Tuple[str, str, str]:
    """Verdict text, 💡 line, analyzed at."""
    vm = re.search(
        r'AI Verdict</h2><p class="text-white leading-relaxed">([^<]+)</p>',
        html,
        re.DOTALL,
    )
    verdict = vm.group(1).strip() if vm else ""

    rec = ""
    rec_m = re.search(
        r'italic">💡[^<]*<!--[^>]*-->([^<]+)</p>',
        html,
    )
    if rec_m:
        rec = rec_m.group(1).strip()

    an = ""
    an_m = re.search(r'Analyzed <!--[^>]*-->([^<]+)</p>', html)
    if an_m:
        an = an_m.group(1).strip()

    return verdict, rec, an


def _parse_flag_lists(html: str) -> Tuple[List[str], List[str]]:
    green: List[str] = []
    red: List[str] = []

    def grab_spans(chunk: str) -> List[str]:
        return [
            x.strip()
            for x in re.findall(r'<span class="text-white">([^<]+)</span>', chunk)
            if x.strip()
        ]

    gm = re.search(r"✅ Green Flags.*?(?=🚩 Red Flags)", html, re.DOTALL)
    if gm:
        green = grab_spans(gm.group(0))

    rm = re.search(
        r"🚩 Red Flags.*?(?=← Back|mt-8 flex justify-between|More <!--)",
        html,
        re.DOTALL,
    )
    if rm:
        red = grab_spans(rm.group(0))

    return green, red


def _parse_social_links(html: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    tw = None
    m = re.search(r'href="(https://(?:twitter|x)\.com/[^"]+)"', html, re.I)
    if m:
        tw = m.group(1)
    dc = None
    m = re.search(r'href="(https://discord\.(?:gg|com)/[^"]+)"', html, re.I)
    if m:
        dc = m.group(1)
    web = None
    # website links: any href that isn't Twitter/X/Discord/daily-mints itself
    for m in re.finditer(r'href="(https?://[^"]+)"', html, re.I):
        u = m.group(1)
        if any(s in u.lower() for s in ("twitter.com", "x.com", "discord.gg", "discord.com", "daily-mints.com")):
            continue
        if u.startswith("https://") and "." in u.split("//", 1)[-1].split("/")[0]:
            web = u
            break
    return tw, dc, web


def parse_mint_detail_html(html: str, page_url: str) -> DailyMintDetail:
    ev = _parse_json_ld_event(html)
    h1_name, chain, risk, date_s, price_s, supply_s = _parse_meta_block(html)
    verdict, rec, an = _parse_verdict_block(html)
    green, red = _parse_flag_lists(html)
    tw, dc, web = _parse_social_links(html)

    score = _parse_ai_score(html)

    name = h1_name or "Unknown"
    if name == "Unknown" and ev:
        name = (ev.get("name") or "Unknown").replace(" NFT Mint", "").strip()

    # Fallback: when the site's visible meta block changes, try to fill basics from JSON-LD Event.
    # This keeps the "details grid" populated even if regexes miss.
    if ev:
        try:
            if not date_s:
                sd = (ev.get("startDate") or "").strip()
                if sd:
                    date_s = sd
            if not price_s:
                offers = ev.get("offers")
                if isinstance(offers, dict):
                    p = offers.get("price")
                    cur = offers.get("priceCurrency")
                    if p is not None and str(p).strip():
                        price_s = f"{p} {cur}".strip() if cur else str(p).strip()
            if not supply_s:
                # Some pages use maximumAttendeeCapacity or a custom "supply" field.
                for k in ("supply", "totalSupply", "maximumAttendeeCapacity", "maximumAttendees"):
                    v = ev.get(k)
                    if v is not None and str(v).strip():
                        supply_s = str(v).strip()
                        break
        except Exception:
            pass

    return DailyMintDetail(
        source_url=page_url,
        name=name,
        chain=chain,
        risk_label=risk,
        mint_date=date_s,
        price=price_s,
        supply=supply_s,
        ai_score=score,
        ai_verdict=verdict,
        recommendation=rec,
        analyzed_at=an,
        green_flags=green,
        red_flags=red,
        twitter_url=tw,
        discord_url=dc,
        website_url=web,
        json_ld=ev,
    )


async def fetch_mint_detail(
    session: aiohttp.ClientSession, path: str
) -> Optional[DailyMintDetail]:
    url = f"{BASE}{path}" if path.startswith("/") else f"{BASE}/{path}"
    html = await fetch_text(session, url)
    if not html:
        return None
    return parse_mint_detail_html(html, url)


async def fetch_mint_details(
    session: aiohttp.ClientSession,
    paths: List[str],
    limit: int = 5,
    concurrency: int = 4,
    filter_today: bool = True,
) -> List[DailyMintDetail]:
    """Fetch first `limit` paths with bounded concurrency.
    When filter_today=True (default), results whose mint_date is a past date are dropped.
    """
    paths = paths[: max(1, min(limit, 25))]
    sem = asyncio.Semaphore(concurrency)

    async def one(p: str) -> Optional[DailyMintDetail]:
        async with sem:
            return await fetch_mint_detail(session, p)

    results = await asyncio.gather(*[one(p) for p in paths])
    mints = [r for r in results if r is not None]

    if filter_today:
        today_mints = [m for m in mints if _mint_date_is_today(m.mint_date)]
        if today_mints:
            # Only apply the filter when it actually found some valid entries
            return today_mints
        # If filter drops everything (e.g. all dates unparseable), return all
        return mints

    return mints


def daily_mint_embed_color(score: Optional[int]) -> int:
    if score is None:
        return 0x5865F2
    if score >= 70:
        return 0x2ECC71
    if score >= 40:
        return 0xF39C12
    return 0xE74C3C


def _score_emoji(score: Optional[int]) -> str:
    if score is None:
        return "📊"
    if score >= 70:
        return "🟢"
    if score >= 40:
        return "🟡"
    return "🔴"


def _risk_display(risk: str) -> str:
    if not (risk or "").strip():
        return "—"
    rl = risk.lower()
    if "low" in rl:
        return f"🟢 {risk}"
    if "medium" in rl:
        return f"🟡 {risk}"
    if "high" in rl:
        return f"🔴 {risk}"
    return f"⚡ {risk}"


def _format_signal_block(recommendation: str) -> str:
    """Rich text for WATCH / SKIP / etc."""
    r = (recommendation or "").strip().upper()
    if not r:
        return "—"
    if "SKIP" in r or "PASS" in r or "AVOID" in r:
        return (
            "🛑 **SKIP**\n"
            "└ *Analyst suggests passing — weak signals or elevated risk.*"
        )
    if "WATCH" in r:
        return (
            "👀 **WATCH**\n"
            "└ *Notable but unconfirmed — review closer to mint.*"
        )
    if any(k in r for k in ("STRONG", "GEM", "CONVICTION", "BUY")):
        return (
            "✨ **HIGH INTEREST**\n"
            "└ *Stronger quality signals — still verify yourself (DYOR).*"
        )
    return f"💡 **{recommendation.strip()}**\n└ *See AI verdict below.*"


def chunk_text(s: str, max_len: int = 1024) -> List[str]:
    s = (s or "").strip()
    if not s:
        return []
    if len(s) <= max_len:
        return [s]
    chunks: List[str] = []
    while s:
        chunks.append(s[:max_len])
        s = s[max_len:]
    return chunks


def build_daily_mint_embeds(
    detail: DailyMintDetail,
    brand_name: str = "Velcor3",
    footer_icon_attachment: Optional[str] = None,
    x_pfp_url: Optional[str] = None,
    x_banner_url: Optional[str] = None,
    x_handle: Optional[str] = None,
) -> List[discord.Embed]:
    """Return one or more discord.Embed objects (split if verdict is huge)."""

    se = _score_emoji(detail.ai_score)
    score_bar = ""
    if detail.ai_score is not None:
        filled = round(detail.ai_score / 10)
        score_bar = "█" * filled + "░" * (10 - filled)
        title = f"{se} {detail.name}  ·  {detail.ai_score}/100"
    else:
        title = f"{se} {detail.name}"

    embed_url = detail.twitter_url if detail.twitter_url else None
    embed = discord.Embed(
        title=title[:256],
        color=daily_mint_embed_color(detail.ai_score),
        url=embed_url,
    )

    # Author: X handle if known, otherwise brand
    if detail.twitter_url and x_handle:
        embed.set_author(
            name=f"⚡ {brand_name}  ·  Mint Calendar  ·  @{x_handle}",
            url=detail.twitter_url,
        )
    else:
        embed.set_author(name=f"⚡ {brand_name}  ·  Mint Calendar")

    if x_pfp_url:
        embed.set_thumbnail(url=x_pfp_url)
    if x_banner_url:
        embed.set_image(url=x_banner_url)

    # ── Description: score bar + signal ──────────────────────────────────────
    desc_parts: List[str] = []
    if score_bar:
        desc_parts.append(f"`{score_bar}` **{detail.ai_score}/100**")
    if detail.recommendation:
        desc_parts.append(_format_signal_block(detail.recommendation))
    embed.description = "\n".join(desc_parts) if desc_parts else "\u200b"

    # ── Mint details grid (3 × 2) ─────────────────────────────────────────────
    embed.add_field(name="⛓️ Chain", value=detail.chain or "—", inline=True)
    embed.add_field(name="📅 Mint Date", value=detail.mint_date or "—", inline=True)
    embed.add_field(name="💰 Price", value=detail.price or "—", inline=True)
    embed.add_field(name="📦 Supply", value=detail.supply or "—", inline=True)
    embed.add_field(
        name="⚠️ Risk",
        value=_risk_display(detail.risk_label) if detail.risk_label else "—",
        inline=True,
    )

    social_links: List[str] = []
    if detail.twitter_url and x_handle:
        social_links.append(f"[@{x_handle}]({detail.twitter_url})")
    elif detail.twitter_url:
        social_links.append(f"[𝕏]({detail.twitter_url})")
    if detail.discord_url:
        social_links.append(f"[Discord]({detail.discord_url})")
    if detail.website_url:
        social_links.append(f"[Website]({detail.website_url})")
    embed.add_field(
        name="🔗 Links",
        value="  ·  ".join(social_links) if social_links else "—",
        inline=True,
    )

    # ── AI verdict ────────────────────────────────────────────────────────────
    if detail.ai_verdict:
        for i, chunk in enumerate(chunk_text(detail.ai_verdict, 1024)):
            name = "🤖 AI Verdict" if i == 0 else "🤖 AI Verdict (cont.)"
            embed.add_field(name=name, value=chunk, inline=False)

    if detail.analyzed_at:
        embed.add_field(name="🕒 Scored At", value=detail.analyzed_at, inline=True)

    # ── Flags ─────────────────────────────────────────────────────────────────
    if detail.green_flags:
        gv = "\n".join(f"✅ {g}" for g in detail.green_flags[:20])
        embed.add_field(name=f"🌿 Green Flags  ·  {len(detail.green_flags)}", value=gv[:1024], inline=False)
    if detail.red_flags:
        rv = "\n".join(f"🚩 {r}" for r in detail.red_flags[:20])
        embed.add_field(name=f"⚠️ Red Flags  ·  {len(detail.red_flags)}", value=rv[:1024], inline=False)

    foot = f"{brand_name}  ·  Mint Calendar  ·  Not financial advice"
    if footer_icon_attachment:
        embed.set_footer(text=foot, icon_url=f"attachment://{footer_icon_attachment}")
    else:
        embed.set_footer(text=foot)
    return [embed]
