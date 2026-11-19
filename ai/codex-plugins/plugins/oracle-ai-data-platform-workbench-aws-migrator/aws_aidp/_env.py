"""Tiny .env loader + required-var check. Stdlib only."""
from __future__ import annotations

import os
from pathlib import Path


def load_dotenv(path: str | Path | None = None) -> None:
    """Load KEY=VALUE pairs from .env into os.environ if not already set."""
    p = Path(path or ".env")
    if not p.exists():
        return
    for raw in p.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def require(*names: str) -> dict[str, str]:
    """Return dict of required env vars, raising with a clean message if any missing."""
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        raise RuntimeError(
            "missing required env var(s): " + ", ".join(missing)
            + "  (set in .env or shell; see .env.example)"
        )
    return {n: os.environ[n] for n in names}
