"""Non-blocking, signed synchronization to the Velcorians staking website.

The local engagement and alpha SQLite databases remain the source of truth.
This module only mirrors absolute totals, coalescing rapid updates per member
and retrying temporary HTTP failures on a daemon worker thread.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Dict

import requests

logger = logging.getLogger(__name__)

SYNC_URL = (os.getenv("STAKING_SYNC_URL") or "").strip()
SYNC_SECRET = (os.getenv("DISCORD_SYNC_SECRET") or "").strip()

_PENDING: Dict[int, dict] = {}
_LOCK = threading.Lock()
_WAKE = threading.Event()
_WORKER_STARTED = False


def enabled() -> bool:
    return SYNC_URL.startswith("https://") and len(SYNC_SECRET) >= 32


def sign_body(body: str, timestamp: str, secret: str) -> str:
    message = f"{timestamp}.{body}".encode("utf-8")
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


def _post(payload: dict) -> bool:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    for attempt in range(3):
        timestamp = str(int(time.time()))
        headers = {
            "Content-Type": "application/json",
            "X-Velcorians-Timestamp": timestamp,
            "X-Velcorians-Signature": sign_body(raw, timestamp, SYNC_SECRET),
        }
        try:
            response = requests.post(SYNC_URL, headers=headers, data=raw, timeout=10)
            if response.status_code in (200, 201, 204):
                return True
            # Authentication and payload failures require configuration/code
            # changes; retrying them would only add traffic.
            if response.status_code in (400, 401, 403):
                logger.warning(
                    "[StakingSync] rejected HTTP %s: %s",
                    response.status_code,
                    (response.text or "")[:180],
                )
                return False
            logger.warning("[StakingSync] HTTP %s (attempt %s/3)", response.status_code, attempt + 1)
        except Exception as error:
            logger.warning("[StakingSync] request failed (attempt %s/3): %s", attempt + 1, error)
        if attempt < 2:
            time.sleep(0.75 * (2 ** attempt))
    return False


def _worker() -> None:
    while True:
        _WAKE.wait(timeout=30)
        _WAKE.clear()
        while True:
            with _LOCK:
                if not _PENDING:
                    break
                _user_id, payload = _PENDING.popitem()
            _post(payload)


def _ensure_worker() -> None:
    global _WORKER_STARTED
    with _LOCK:
        if _WORKER_STARTED:
            return
        thread = threading.Thread(target=_worker, name="staking-score-sync", daemon=True)
        thread.start()
        _WORKER_STARTED = True


def queue_score_sync(discord_user_id: int, engagement_points: int, alpha_score: int) -> bool:
    if not enabled():
        return False
    uid = int(discord_user_id or 0)
    if uid <= 0:
        return False

    payload = {
        # Discord snowflakes must cross JSON as strings; JavaScript numbers
        # cannot represent them safely.
        "discordUserId": str(uid),
        "engagementPoints": int(engagement_points),
        "alphaScore": int(alpha_score),
        "sourceUpdatedAt": datetime.now(timezone.utc).isoformat(),
    }
    _ensure_worker()
    with _LOCK:
        _PENDING[uid] = payload
    _WAKE.set()
    return True
