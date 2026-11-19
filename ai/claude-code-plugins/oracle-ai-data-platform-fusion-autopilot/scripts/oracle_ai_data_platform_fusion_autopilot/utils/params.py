"""Parameter resolution helper.

Wraps AIDP's notebook-native ``oci_util.parameters.getParameter()`` (per pdf2 p2)
when running inside an AIDP notebook. Falls back to environment variables for
local dev / CI runs where ``oci_util`` is not importable.
"""

from __future__ import annotations

import os
from typing import Any


def get(key: str, default: str | None = None) -> str | None:
    """Resolve a parameter against AIDP notebook params first, then env.

    Args:
        key: The parameter name (e.g. ``"FUSION_BICC_PVO"``).
        default: Value to return when neither source has the key. ``None`` if absent.

    Returns:
        The resolved string, or ``default`` if not found in either source.

    Notes:
        - Inside an AIDP notebook with ``oci_util`` injected, this calls
          ``oci_util.parameters.getParameter(key, default)`` per pdf2 p2.
        - Outside AIDP (local dev, CI), falls back to ``os.environ.get(key, default)``.
        - Never logs the resolved value — callers handle secret-safe display.
    """
    aidp_value = _from_aidp_oci_util(key, default)
    if aidp_value is not None:
        return aidp_value
    return os.environ.get(key, default)


def _from_aidp_oci_util(key: str, default: str | None) -> str | None:
    """Try ``oci_util.parameters.getParameter`` if available; else None.

    Returns ``None`` (not ``default``) when ``oci_util`` is unavailable, so the
    caller can fall back to env. Returns the resolved value (which may equal
    ``default``) when the AIDP path succeeds.
    """
    try:
        import oci_util  # type: ignore[import-not-found]  # injected by AIDP notebook runtime
    except ImportError:
        return None

    parameters: Any = getattr(oci_util, "parameters", None)
    if parameters is None:
        return None

    fn = getattr(parameters, "getParameter", None)
    if fn is None:
        return None

    return fn(key, default)
