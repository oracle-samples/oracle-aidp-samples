"""Minimal .env loader. No dependency on python-dotenv."""
from __future__ import annotations

import os
from pathlib import Path

# Only this tool's own settings. The file is read from whatever directory the
# CLI runs in, and an arbitrary key there -- NODE_OPTIONS=--require ./x.js --
# would run code inside the Node M parser; PATH could swap `node` or `aidp`.
KEY_PREFIXES = ("AIDP_", "OCI_", "FABRIC_")


def load_dotenv(path=".env") -> None:
    candidate = Path(path)
    if not candidate.is_file():
        return
    try:
        lines = candidate.read_text(encoding="utf-8-sig").splitlines()
    except (OSError, UnicodeError):
        return
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip().strip("'\"")
        if key.startswith(KEY_PREFIXES) and key not in os.environ:
            os.environ[key] = value
