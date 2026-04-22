"""
Resolve Telegram message links found in kolfi_callers_registry.json into joinable channels.

Inputs:
  - kolfi_callers_registry.json (produced by kolfi feed)

Outputs:
  - kolfi_callers_telegram_channels.txt
    - public channels/groups: title + @username + https://t.me/<username>
    - private/inaccessible chats: listed with numeric id when resolvable

Requires:
  TELEGRAM_API_ID / TELEGRAM_API_HASH / TELEGRAM_SESSION in .env (Telethon user session)
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(override=True)

REG_PATH = Path("kolfi_callers_registry.json")
OUT_PATH = Path("kolfi_callers_telegram_channels.txt")

TME_USER_RE = re.compile(r"(?i)https?://t\.me/(?!c/)([A-Za-z0-9_]{4,})")
TME_C_RE = re.compile(r"(?i)https?://t\.me/c/(\d+)(?:/\d+)?")


def _norm_user(u: str) -> str:
    u = (u or "").strip().lstrip("@")
    return u


def _load_links() -> tuple[dict[str, list[str]], set[str], set[int]]:
    raw = json.loads(REG_PATH.read_text(encoding="utf-8"))
    callers = raw.get("callers", {}) if isinstance(raw, dict) else {}
    by_label: dict[str, list[str]] = {}
    usernames: set[str] = set()
    c_ids: set[int] = set()
    for _k, ent in (callers or {}).items():
        if not isinstance(ent, dict):
            continue
        label = str(ent.get("label") or _k)
        links = ent.get("links") or []
        if not isinstance(links, list):
            continue
        tg_links = []
        for u in links:
            if not isinstance(u, str):
                continue
            uu = u.strip()
            if "t.me/" not in uu and "telegram.me/" not in uu:
                continue
            tg_links.append(uu)
            m = TME_USER_RE.search(uu)
            if m:
                usernames.add(_norm_user(m.group(1)))
            m2 = TME_C_RE.search(uu)
            if m2:
                try:
                    c_ids.add(int(m2.group(1)))
                except ValueError:
                    pass
        if tg_links:
            by_label[label] = tg_links
    return by_label, usernames, c_ids


async def main() -> None:
    api_id = (os.getenv("TELEGRAM_API_ID") or "").strip()
    api_hash = (os.getenv("TELEGRAM_API_HASH") or "").strip()
    sess = (os.getenv("TELEGRAM_SESSION") or "").strip()
    if not api_id.isdigit() or not api_hash or not sess:
        raise SystemExit("Missing TELEGRAM_API_ID / TELEGRAM_API_HASH / TELEGRAM_SESSION in .env")

    if not REG_PATH.exists():
        raise SystemExit("Missing kolfi_callers_registry.json")

    by_label, usernames, c_ids = _load_links()

    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from telethon.tl.types import PeerChannel

    tg = TelegramClient(StringSession(sess), int(api_id), api_hash)
    await tg.connect()
    if not await tg.is_user_authorized():
        raise SystemExit("Telegram session not authorized. Re-generate TELEGRAM_SESSION.")

    # Resolve public usernames to entities (best effort)
    resolved_public: dict[str, dict] = {}
    for u in sorted(usernames):
        try:
            ent = await tg.get_entity(u)
            title = getattr(ent, "title", "") or getattr(ent, "first_name", "") or u
            username = getattr(ent, "username", "") or u
            resolved_public[u] = {
                "title": str(title),
                "username": str(username),
                "id": int(getattr(ent, "id", 0) or 0),
            }
        except Exception:
            continue

    # Resolve /c/<id> numeric chats (only works if accessible in your account)
    resolved_c: dict[int, dict] = {}
    for cid in sorted(c_ids):
        try:
            ent = await tg.get_entity(PeerChannel(cid))
            title = getattr(ent, "title", "") or str(cid)
            username = getattr(ent, "username", "") or ""
            resolved_c[cid] = {
                "title": str(title),
                "username": str(username),
                "id": int(getattr(ent, "id", 0) or 0),
            }
        except Exception:
            # inaccessible/private/not joined
            continue

    await tg.disconnect()

    # Build recommended TELEGRAM_CALLS_SOURCES (joinable usernames)
    sources: list[str] = []
    seen: set[str] = set()
    for v in resolved_public.values():
        un = _norm_user(v.get("username", ""))
        if un and un.lower() not in seen:
            seen.add(un.lower())
            sources.append("@" + un)
    for v in resolved_c.values():
        un = _norm_user(v.get("username", ""))
        if un and un.lower() not in seen:
            seen.add(un.lower())
            sources.append("@" + un)

    # Output file
    lines: list[str] = []
    lines.append(f"callers_in_registry={len(by_label)}")
    lines.append(f"unique_usernames_in_links={len(usernames)}")
    lines.append(f'unique_c_ids_in_links={len(c_ids)}')
    lines.append(f"resolved_public_usernames={len(resolved_public)}")
    lines.append(f"resolved_c_chats={len(resolved_c)}")
    lines.append("")
    lines.append("## Joinable channels (resolved)")
    lines.append("(Only channels with @username are joinable by link. Private groups need an invite link.)")
    lines.append("")
    # Combine and list unique joinable usernames with titles
    join_rows: list[tuple[str, str]] = []
    for v in list(resolved_public.values()) + list(resolved_c.values()):
        un = _norm_user(v.get("username", ""))
        if not un:
            continue
        join_rows.append((str(v.get("title", un)), un))
    join_rows.sort(key=lambda x: x[0].lower())
    # de-dup by username
    seen2: set[str] = set()
    for title, un in join_rows:
        if un.lower() in seen2:
            continue
        seen2.add(un.lower())
        lines.append(f"- {title} — @{un} — https://t.me/{un}")
    if not join_rows:
        lines.append("- (none resolved to public usernames)")
    lines.append("")
    lines.append("## TELEGRAM_CALLS_SOURCES suggestion")
    if sources:
        lines.append("TELEGRAM_CALLS_SOURCES=" + ", ".join(sources[:250]))
        if len(sources) > 250:
            lines.append(f"(+{len(sources)-250} more; split across lines in .env)")
    else:
        lines.append("TELEGRAM_CALLS_SOURCES=")
    lines.append("")
    lines.append("## Notes")
    lines.append("- If a link is like https://t.me/c/<id>/<msg>, it often points to a private channel/group.")
    lines.append("- To resolve those, your Telegram account must be able to access the chat (joined / has invite).")

    OUT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    asyncio.run(main())

