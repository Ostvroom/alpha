"""Normalize Ethereum addresses from RPC logs (32-byte topics) and APIs (20-byte)."""
from __future__ import annotations


def normalize_eth_address(value: str) -> str:
    """
    Accept 0x + 40 hex (EOA) or padded 32-byte topic (66 hex after 0x).
    Returns lowercase 0x + 40 hex, or "" if invalid.
    """
    if not value:
        return ""
    raw = str(value).strip().lower()
    if raw.startswith("0x"):
        hex_body = raw[2:]
    else:
        hex_body = "".join(c for c in raw if c in "0123456789abcdef")
    if len(hex_body) < 40:
        return ""
    if len(hex_body) > 40:
        hex_body = hex_body[-40:]
    return "0x" + hex_body


ZERO_ADDRESS = "0x" + "0" * 40
