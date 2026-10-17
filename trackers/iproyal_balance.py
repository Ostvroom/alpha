"""IPRoyal residential-traffic monitoring with deduplicated Discord DMs."""
from __future__ import annotations

from typing import Any, Optional

import aiohttp
import discord

import config
import database


API_URL = "https://resi-api.iproyal.com/v1/me"
PROVIDER = "iproyal"


def balance_level(available_gb: float) -> str:
    available = max(0.0, float(available_gb))
    critical = float(getattr(config, "IPROYAL_CRITICAL_GB", 0.5) or 0.0)
    warning = float(getattr(config, "IPROYAL_WARNING_GB", 2.0) or 0.0)
    if available <= critical:
        return "critical"
    if available <= warning:
        return "warning"
    return "healthy"


async def fetch_available_traffic(session: Optional[aiohttp.ClientSession] = None) -> float:
    token = str(getattr(config, "IPROYAL_API_TOKEN", "") or "").strip()
    if not token:
        raise RuntimeError("IPROYAL_API_TOKEN is not configured")

    owns_session = session is None
    http = session or aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
    try:
        async with http.get(
            API_URL,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        ) as response:
            if response.status != 200:
                # Do not include the response body: providers sometimes echo
                # request details, and logs must never expose API credentials.
                raise RuntimeError(f"IPRoyal API returned HTTP {response.status}")
            payload: Any = await response.json(content_type=None)
    finally:
        if owns_session:
            await http.close()

    if not isinstance(payload, dict) or "available_traffic" not in payload:
        raise RuntimeError("IPRoyal API response omitted available_traffic")
    try:
        return max(0.0, float(payload["available_traffic"]))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("IPRoyal API returned an invalid available_traffic value") from exc


def _alert_embed(available_gb: float, level: str) -> discord.Embed:
    critical = level == "critical"
    embed = discord.Embed(
        title="🚨 IPRoyal proxy traffic critical" if critical else "⚠️ IPRoyal proxy traffic low",
        description=(
            f"Only **{available_gb:.2f} GB** of residential proxy traffic remains.\n\n"
            "Top up IPRoyal soon to keep BrainScan and TweetWatcher online."
        ),
        color=0xED4245 if critical else 0xFEE75C,
        url="https://dashboard.iproyal.com/",
    )
    embed.add_field(
        name="Alert threshold",
        value=(
            f"Critical: ≤ {float(config.IPROYAL_CRITICAL_GB):.2f} GB"
            if critical
            else f"Warning: ≤ {float(config.IPROYAL_WARNING_GB):.2f} GB"
        ),
        inline=False,
    )
    embed.set_footer(text="This DM is sent once per threshold crossing.")
    return embed


async def check_and_notify(bot) -> float:
    """Check balance and DM configured users once per warning-level transition."""
    available = await fetch_available_traffic()
    level = balance_level(available)
    previous = database.get_proxy_balance_alert_state(PROVIDER) or {}
    previous_level = str(previous.get("level") or "")

    should_alert = level in {"warning", "critical"} and level != previous_level
    delivered = False
    if should_alert:
        embed = _alert_embed(available, level)
        for user_id in list(getattr(config, "IPROYAL_ALERT_USER_IDS", []) or []):
            try:
                user = bot.get_user(int(user_id)) or await bot.fetch_user(int(user_id))
                await user.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
                delivered = True
            except Exception as exc:
                print(f"[IPRoyal] Could not DM configured user {user_id}: {type(exc).__name__}: {exc}")

    # Save healthy checks immediately. For a low balance, only advance the alert
    # level after at least one DM succeeds so a temporary Discord failure retries.
    if level == "healthy" or not should_alert or delivered:
        database.upsert_proxy_balance_alert_state(
            PROVIDER,
            level,
            available,
            alerted=bool(should_alert and delivered),
        )

    print(f"[IPRoyal] Residential traffic remaining: {available:.2f} GB ({level})")
    return available
