"""Unit tests for the Fusion REST paged extractor (499-row hard cap)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from oracle_ai_data_platform_fusion_bundle.extractors.rest import (
    FUSION_PAGE_LIMIT_HARD_CAP,
    fetch_paged,
)


def _mock_session(pages: list[dict[str, Any]]) -> Any:
    """Build a mock requests.Session whose .get() returns successive pages."""
    session = MagicMock()
    responses = []
    for p in pages:
        r = MagicMock()
        r.json.return_value = p
        r.raise_for_status = MagicMock(return_value=None)
        responses.append(r)
    session.get.side_effect = responses
    return session


class TestFetchPaged:
    def test_499_row_cap_enforced(self) -> None:
        session = _mock_session([{"items": [], "hasMore": False}])
        list(fetch_paged(session, "https://x", "/api", limit=10_000))
        # Verify the call used 499, not 10_000:
        call_kwargs = session.get.call_args
        assert call_kwargs.kwargs["params"]["limit"] == FUSION_PAGE_LIMIT_HARD_CAP

    def test_paginates_until_hasmore_false(self) -> None:
        session = _mock_session(
            [
                {"items": [{"id": 1}, {"id": 2}], "hasMore": True},
                {"items": [{"id": 3}], "hasMore": False},
            ]
        )
        rows = list(fetch_paged(session, "https://x", "/api", limit=2))
        assert [r["id"] for r in rows] == [1, 2, 3]
        assert session.get.call_count == 2

    def test_stops_on_empty_items_even_if_hasmore_true(self) -> None:
        # Defensive: if Fusion lies about hasMore, empty items terminates the loop.
        session = _mock_session([{"items": [], "hasMore": True}])
        rows = list(fetch_paged(session, "https://x", "/api"))
        assert rows == []

    def test_only_data_param_set(self) -> None:
        session = _mock_session([{"items": [], "hasMore": False}])
        list(fetch_paged(session, "https://x", "/api"))
        params = session.get.call_args.kwargs["params"]
        # Per the connector skill: onlyData=true keeps Fusion HATEOAS noise out.
        assert params["onlyData"] == "true"

    def test_fields_projection(self) -> None:
        session = _mock_session([{"items": [], "hasMore": False}])
        list(fetch_paged(session, "https://x", "/api", fields="A,B"))
        assert session.get.call_args.kwargs["params"]["fields"] == "A,B"

    def test_extra_params_q_filter(self) -> None:
        session = _mock_session([{"items": [], "hasMore": False}])
        list(
            fetch_paged(
                session,
                "https://x",
                "/api",
                extra_params={"q": "InvoiceDate >= '2026-01-01'"},
            )
        )
        assert session.get.call_args.kwargs["params"]["q"] == "InvoiceDate >= '2026-01-01'"

    def test_offset_increments_by_limit(self) -> None:
        session = _mock_session(
            [
                {"items": [{"id": 1}], "hasMore": True},
                {"items": [{"id": 2}], "hasMore": True},
                {"items": [], "hasMore": False},
            ]
        )
        list(fetch_paged(session, "https://x", "/api", limit=1))
        # Offsets should be 0, 1, 2:
        offsets = [c.kwargs["params"]["offset"] for c in session.get.call_args_list]
        assert offsets == [0, 1, 2]
