"""Fusion REST paged extractor — fallback path for tiny dimensions only.

Hard-capped at 499 rows/page per MOS Doc ID 2429019.1; for bulk extracts
(>5k rows) always prefer :mod:`bicc`.

Mirrors :func:`oracle_ai_data_platform_connectors.rest.fusion.fetch_paged`
in the existing connectors plugin; included here as a vendored copy so the
bundle has no hard dependency on the sibling plugin's helpers being on
``sys.path``.
"""

from __future__ import annotations

from typing import Any, Iterator

# Per Oracle MOS Doc ID 2429019.1 — Fusion silently truncates limit > 499.
FUSION_PAGE_LIMIT_HARD_CAP: int = 499


def fetch_paged(
    session: Any,
    base_url: str,
    path: str,
    *,
    limit: int = FUSION_PAGE_LIMIT_HARD_CAP,
    fields: str | None = None,
    extra_params: dict[str, str] | None = None,
    timeout: int = 120,
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

    Yields:
        One dict per row (Fusion's ``items`` array element).
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
        response = session.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        payload = response.json()

        items = payload.get("items", [])
        if not items:
            return
        yield from items

        if not payload.get("hasMore", False):
            return
        offset += limit
