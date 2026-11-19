"""Fusion REST paged extractor — fallback path for tiny dimensions only.

Hard-capped at 499 rows/page per MOS Doc ID 2429019.1; for bulk extracts
(>5k rows) always prefer :mod:`bicc`.

Mirrors :func:`oracle_ai_data_platform_connectors.rest.fusion.fetch_paged`
in the existing connectors plugin; included here as a vendored copy so the
bundle has no hard dependency on the sibling plugin's helpers being on
``sys.path``.
"""

from __future__ import annotations

import time
from typing import Any, Iterator

# Per Oracle MOS Doc ID 2429019.1 — Fusion silently truncates limit > 499.
FUSION_PAGE_LIMIT_HARD_CAP: int = 499


class DeadlineExceeded(Exception):
    """An absolute deadline expired before (another) request could be issued.

    Raised by :func:`fetch_paged` / :func:`fetch_first` when the caller passed
    ``deadline`` and it has passed. The deadline is checked BEFORE every
    request, and each request's timeout is clamped to the remaining budget, so
    a multi-page response has a real global bound and total overrun is capped
    by the last granted slice (feature: metadata-driven-coa-resolution).
    """


def _remaining_or_raise(deadline: float | None, timeout: float) -> float:
    """Clamp ``timeout`` to the remaining budget; raise when it's spent."""
    if deadline is None:
        return timeout
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise DeadlineExceeded(
            f"deadline expired {abs(remaining):.1f}s ago; no further requests"
        )
    return min(timeout, remaining)


def fetch_paged(
    session: Any,
    base_url: str,
    path: str,
    *,
    limit: int = FUSION_PAGE_LIMIT_HARD_CAP,
    fields: str | None = None,
    extra_params: dict[str, str] | None = None,
    timeout: int = 120,
    deadline: float | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield rows from a Fusion REST resource, page by page.

    Args:
        session: A ``requests.Session`` with HTTP Basic auth set.
        base_url: Fusion pod base URL (e.g.
            ``https://my-pod.fa.us-phoenix-1.oraclecloud.com``).
        path: Resource path (e.g.
            ``/fscmRestApi/resources/11.13.18.05/invoices``).
        limit: Page size. Hard-capped at 499 (Fusion silently truncates higher).
        fields: Comma-separated field projection.
        extra_params: Additional query params (e.g. ``q=...`` filters).
        timeout: Per-request timeout in seconds.
        deadline: Optional ABSOLUTE ``time.monotonic()`` timestamp. When set,
            it is checked before every page request and each request's timeout
            is ``min(timeout, remaining)``; an expired deadline raises
            :class:`DeadlineExceeded`. ``None`` preserves the historical
            unbounded-pages behaviour.

    Yields:
        One dict per row (Fusion's ``items`` array element).

    Raises:
        DeadlineExceeded: ``deadline`` passed before a (further) page could
            be requested.
    """
    if limit > FUSION_PAGE_LIMIT_HARD_CAP:
        limit = FUSION_PAGE_LIMIT_HARD_CAP

    base_url = base_url.rstrip("/")
    offset = 0

    while True:
        params: dict[str, str | int] = {
            "limit": limit,
            "offset": offset,
            "onlyData": "true",
        }
        if fields:
            params["fields"] = fields
        if extra_params:
            params.update(extra_params)

        url = f"{base_url}{path}"
        effective_timeout = _remaining_or_raise(deadline, timeout)
        response = session.get(url, params=params, timeout=effective_timeout)
        response.raise_for_status()
        payload = response.json()

        items = payload.get("items", [])
        if not items:
            return
        yield from items

        if not payload.get("hasMore", False):
            return
        offset += limit


def fetch_first(
    session: Any,
    base_url: str,
    path: str,
    *,
    fields: str | None = None,
    extra_params: dict[str, str] | None = None,
    timeout: int = 120,
    deadline: float | None = None,
) -> dict[str, Any] | None:
    """Return the FIRST row of a Fusion REST resource — one HTTP request,
    by construction.

    Exists because :func:`fetch_paged` is a ``while hasMore`` generator:
    materialising it at ``limit=1`` issues one request per ROW (potentially
    tens of thousands), and relying on callers to only ``next()`` it is
    one careless ``list()`` away from reintroducing that bug (plan-review
    finding, metadata-driven-coa-resolution). This helper issues exactly one
    GET with ``limit=1`` and never follows ``hasMore``.

    Args:
        session / base_url / path / fields / extra_params / timeout / deadline:
            as :func:`fetch_paged`.

    Returns:
        The first item dict, or ``None`` when the (possibly filtered) resource
        returned no rows.

    Raises:
        DeadlineExceeded: ``deadline`` passed before the request could be
            issued.
    """
    base_url = base_url.rstrip("/")
    params: dict[str, str | int] = {"limit": 1, "offset": 0, "onlyData": "true"}
    if fields:
        params["fields"] = fields
    if extra_params:
        params.update(extra_params)

    effective_timeout = _remaining_or_raise(deadline, timeout)
    response = session.get(f"{base_url}{path}", params=params, timeout=effective_timeout)
    response.raise_for_status()
    payload = response.json()
    items = payload.get("items", [])
    return items[0] if items else None
