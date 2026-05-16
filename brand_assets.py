"""
Velcor brand logo + name for Discord embeds (author/footer icons).
"""
from __future__ import annotations

import os
from typing import List, Optional, Tuple

import discord

_ROOT = os.path.dirname(os.path.abspath(__file__))


def brand_name() -> str:
    return (
        (
            os.getenv("VELOCR3_BRAND_NAME")
            or os.getenv("VELCOR3_BRAND_NAME")
            or os.getenv("NERDS_BRAND_NAME")
            or "Velcor3"
        ).strip()
        or "Velcor3"
    )


def _optional_http_url(key: str) -> str:
    raw = (os.getenv(key) or "").strip()
    if raw.lower() in ("0", "false", "none", "off", ""):
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    return ""


def resolve_brand_logo() -> Tuple[Optional[str], Optional[str]]:
    """Local logo file path + attachment filename for Discord."""
    for name in (
        "velcor3_logo.png",
        "velcor3_logo.jpg",
        "velcor_logo.png",
        "velcor_logo.jpg",
        "alpha_logo.png",
        "alpha_logo.jpg",
        "logo.png",
        "block_brain_logo.png",
    ):
        path = os.path.join(_ROOT, name)
        if os.path.isfile(path):
            ext = name.rsplit(".", 1)[-1].lower()
            return path, f"logo.{ext}"
    assets = os.path.join(_ROOT, "assets", "velcor3_logo.png")
    if os.path.isfile(assets):
        return assets, "velcor3_logo.png"
    return None, None


def resolve_brand_assets() -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Logo + banner paths (same discovery as discord_bot)."""
    logo_path, logo_file = resolve_brand_logo()
    banner_path, banner_file = None, None
    vdir = os.path.join(_ROOT, "v")
    p_root_banner = os.path.join(_ROOT, "banner.jpg")
    if os.path.isfile(p_root_banner):
        banner_path = p_root_banner
        banner_file = "banner.jpg"
    else:
        for name in (
            "banner.png",
            "banner.jpg",
            "banner.jpeg",
            "velcor3_banner.png",
            "velcor3_banner.jpg",
            "alpha_banner.jpg",
            "alpha_banner.png",
        ):
            for base in (vdir, _ROOT):
                p = os.path.join(base, name)
                if os.path.isfile(p):
                    banner_path = p
                    banner_file = "banner." + name.rsplit(".", 1)[-1].lower()
                    break
            if banner_path:
                break
    return logo_path, logo_file, banner_path, banner_file


def brand_logo_embed_icon() -> str:
    """HTTPS URL or attachment:// filename for embed author/footer icons."""
    for key in ("VELOCR3_BRAND_LOGO_URL", "VELCOR3_BRAND_LOGO_URL"):
        url = _optional_http_url(key)
        if url:
            return url
    _path, fname = resolve_brand_logo()
    if fname:
        return f"attachment://{fname}"
    site = (os.getenv("VELOCR3_PUBLIC_URL") or os.getenv("WEBSITE_PUBLIC_URL") or "").strip().rstrip("/")
    if site:
        return f"{site}/logo.png"
    return ""


def append_brand_logo_file(files: List[discord.File]) -> List[discord.File]:
    path, fname = resolve_brand_logo()
    if not path or not fname:
        return files
    if any(getattr(f, "filename", None) == fname for f in files):
        return files
    files.append(discord.File(path, filename=fname))
    return files


def embed_references_attachment(embed: discord.Embed, filename: str) -> bool:
    ref = f"attachment://{filename}"
    data = embed.to_dict()
    if data.get("author", {}).get("icon_url") == ref:
        return True
    if data.get("footer", {}).get("icon_url") == ref:
        return True
    if data.get("thumbnail", {}).get("url") == ref:
        return True
    return False


def apply_live_mint_branding(embed: discord.Embed) -> None:
    brand = brand_name()
    icon = brand_logo_embed_icon()
    author_name = f"⚡  {brand} · Live Mint"
    footer_text = f"{brand} · On-chain mint"
    if icon:
        embed.set_author(name=author_name, icon_url=icon)
        embed.set_footer(text=footer_text, icon_url=icon)
    else:
        embed.set_author(name=author_name)
        embed.set_footer(text=footer_text)


def apply_hot_mint_branding(embed: discord.Embed) -> None:
    brand = brand_name()
    icon = brand_logo_embed_icon()
    author_name = f"🔥  {brand} · Hot Mint"
    footer_text = f"{brand} · Hot mint · Ethereum"
    if icon:
        embed.set_author(name=author_name, icon_url=icon)
        embed.set_footer(text=footer_text, icon_url=icon)
    else:
        embed.set_author(name=author_name)
        embed.set_footer(text=footer_text)


def collect_embed_attachment_files(embeds: List[discord.Embed]) -> List[discord.File]:
    """Attach local files referenced by embed attachment:// URLs."""
    files: List[discord.File] = []
    _path, brand_fname = resolve_brand_logo()
    if brand_fname and any(embed_references_attachment(e, brand_fname) for e in embeds):
        files = append_brand_logo_file(files)
    return files
