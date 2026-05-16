"""
X / HVA alpha signals for mint alerts (read-only block_brain.db — no Twikit calls).
Used only for the dedicated MINT_X_ALPHA channel; does not touch brain scan loops.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import database

logger = logging.getLogger(__name__)

_INTERACTION_LABELS = {
    "follow": "Followed",
    "mention": "Mentioned",
    "retweet": "Retweeted",
    "quote": "Quoted",
    "reply": "Replied",
    "like": "Liked",
    "keyword_search": "Found via search",
    "ct_domain": "CT domain",
}

_TWITTER_HOSTS = frozenset(
    {"twitter.com", "www.twitter.com", "mobile.twitter.com", "x.com", "www.x.com"}
)


def _normalize_handle(raw: str) -> str:
    s = (raw or "").strip().lstrip("@").lower()
    return s if s and re.match(r"^[a-z0-9_]{1,15}$", s) else ""


def twitter_handle_from_socials(social_links: Optional[Dict]) -> str:
    if not social_links:
        return ""
    url = (social_links.get("twitter") or "").strip()
    if not url:
        return ""
    if url.startswith("@"):
        return _normalize_handle(url)
    try:
        host = (urlparse(url).netloc or "").lower()
        if host not in _TWITTER_HOSTS:
            return ""
        path = (urlparse(url).path or "").strip("/")
        if not path:
            return ""
        return _normalize_handle(path.split("/")[0])
    except Exception:
        return ""


def resolve_project_for_mint(mint: Dict[str, Any]) -> Optional[Tuple[str, str, str]]:
    """
    Return (twitter_id, handle, display_name) from OpenSea/social links + projects DB.
    """
    socials = mint.get("social_links") or {}
    handle = twitter_handle_from_socials(socials)
    if not handle:
        return None
    row = database.get_project_by_handle(handle)
    if not row:
        return None
    tid, db_handle, name = row[0], row[1], row[2]
    return str(tid), _normalize_handle(db_handle or handle), (name or mint.get("contract_name") or handle)


def _recent_hva_lines(project_id: str, limit: int = 8) -> List[str]:
    events = database.get_project_follow_events(project_id, limit=40)
    lines: List[str] = []
    seen_hva: set = set()
    for row in events:
        if not row or len(row) < 2:
            continue
        hva = str(row[0] or "").strip().lstrip("@")
        itype = str(row[1] or "follow").strip().lower()
        if not hva or hva.lower() in seen_hva:
            continue
        seen_hva.add(hva.lower())
        label = _INTERACTION_LABELS.get(itype, itype.replace("_", " ").title())
        lines.append(f"• [@{hva}](https://x.com/{hva}) — **{label}**")
        if len(lines) >= limit:
            break
    return lines


def compute_mint_x_alpha(mint: Dict[str, Any]) -> Dict[str, Any]:
    """
    Attach x_alpha_* fields to mint dict (in-place) and return signal summary.
    Pure SQLite reads — safe alongside HVA brain scan.
    """
    out: Dict[str, Any] = {
        "score": 0,
        "qualifies": False,
        "twitter_handle": "",
        "project_id": "",
        "hvas_24h": 0,
        "hvas_7d": 0,
        "unique_hvas": 0,
        "ai_alpha": 0,
        "activity_lines": [],
        "smart_followers": [],
    }

    socials = mint.get("social_links") or {}
    handle = twitter_handle_from_socials(socials)
    out["twitter_handle"] = handle

    proj = resolve_project_for_mint(mint)
    sf = {"raw_score": 0.0, "unique_hvas": 0, "hvas_24h": 0, "hvas_7d": 0, "hvas_30d": 0}
    ai_alpha = 0
    activity_lines: List[str] = []
    smart_names: List[str] = []

    if proj:
        tid, handle, _ = proj
        out["project_id"] = tid
        out["twitter_handle"] = handle
        try:
            sf = database.calculate_project_smart_followers_v2(tid) or sf
        except Exception as e:
            logger.debug("sf_v2 failed: %s", e)
        try:
            ai = database.get_project_ai_data(tid) or {}
            ai_alpha = int(ai.get("alpha_score") or 0)
        except Exception:
            pass
        try:
            activity_lines = _recent_hva_lines(tid)
        except Exception:
            pass
        try:
            smart_names = database.get_project_smart_followers(tid, limit=12)
        except Exception:
            pass

    out["hvas_24h"] = int(sf.get("hvas_24h") or 0)
    out["hvas_7d"] = int(sf.get("hvas_7d") or 0)
    out["unique_hvas"] = int(sf.get("unique_hvas") or 0)
    out["ai_alpha"] = ai_alpha
    out["activity_lines"] = activity_lines
    out["smart_followers"] = smart_names

    score = 0
    score += min(35, int(sf.get("raw_score", 0) * 3))
    score += min(25, ai_alpha // 4)
    score += min(20, out["hvas_24h"] * 8)
    score += min(10, out["hvas_7d"] * 2)
    if mint.get("is_smart_wallet_event"):
        score += 25
    if activity_lines:
        score += min(15, len(activity_lines) * 3)

    score = min(100, score)
    out["score"] = score

    mint["x_alpha_score"] = score
    mint["x_alpha_handle"] = out["twitter_handle"]
    mint["x_alpha_hvas_24h"] = out["hvas_24h"]
    mint["x_alpha_hvas_7d"] = out["hvas_7d"]
    mint["x_alpha_unique_hvas"] = out["unique_hvas"]
    mint["x_alpha_ai"] = ai_alpha
    mint["x_alpha_activity_lines"] = activity_lines
    mint["x_alpha_smart_followers"] = smart_names
    return out


def mint_qualifies_for_alpha_channel(
    mint: Dict[str, Any],
    signals: Dict[str, Any],
    *,
    min_score: int,
    allow_smart_wallet_only: bool = True,
) -> bool:
    """Whether to post an enriched alert to MINT_X_ALPHA_CHANNEL_ID."""
    score = int(signals.get("score") or 0)
    if score >= min_score:
        return True
    if allow_smart_wallet_only and mint.get("is_smart_wallet_event"):
        return True
    if int(signals.get("hvas_24h") or 0) >= 1:
        return True
    if int(signals.get("unique_hvas") or 0) >= 2:
        return True
    if signals.get("activity_lines"):
        return True
    return False
