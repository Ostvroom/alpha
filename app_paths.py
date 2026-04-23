from __future__ import annotations

import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent

# Render / Docker: mount a persistent disk and set DATA_DIR to that path (e.g. /var/data).
_data_override = (os.environ.get("DATA_DIR") or "").strip()
if _data_override:
    DATA_DIR = Path(_data_override).expanduser().resolve()
else:
    DATA_DIR = (BASE_DIR / "data").resolve()

ASSETS_DIR = BASE_DIR / "assets"


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ASSETS_DIR.mkdir(parents=True, exist_ok=True)

