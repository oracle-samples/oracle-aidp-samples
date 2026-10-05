"""Unit tests for the BICC offering-schema auto-discovery helper (P1.5α-fix19).

Walker pinned against the anonymized fixture in
``docs/features/p1-5a-fix19-bicc-schema-auto-discovery/plan.md`` Step 3 —
which is the canonical shape ``/biacm/rest/meta/datastores`` returns on
demo-pod and similar tenants.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from oracle_ai_data_platform_fusion_autopilot.orchestrator.discovery import (
    discover_pvo_schemas,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.errors import (
    DiscoveryProbeError,
)


# Anonymized BICC response fixture — the canonical shape the walker must
# handle. Matches the plan.md Step 3 fixture verbatim so the contract is
# pinned in code.
_ANON_RESPONSE = {
    "items": [
        {
            "offeringName": "Financial",
            "datastores": [
                {
                    "name": "FscmTopModelAM.FinExtractAM.GlBiccExtractAM.JournalHeaderExtractPVO",
                    "viewObjectName": "JournalHeaderExtractPVO",
                },
                {
                    "name": "FscmTopModelAM.ScmExtractAM.EgpBiccExtractAM.ItemExtractPVO",
                },
            ],
        },
        {
            "offeringName": "Procurement",
            "datastores": [
                {
                    "name": "FscmTopModelAM.PrcExtractAM.PozBiccExtractAM.SupplierExtractPVO",
                },
            ],
        },
    ],
}


def _fake_response(status_code: int = 200, body=None, text: str = "") -> MagicMock:
    r = MagicMock()
    r.status_code = status_code
    r.text = text
    if body is not None:
        r.json.return_value = body
    else:
        r.json.side_effect = ValueError("no body")
    return r


def test_discover_returns_pairs_from_anonymized_bicc_response():
    """Walker yields every (datastore, schema) pair from the anonymized
    fixture — including the duplicate viewObjectName entry that pdf shapes
    sometimes carry alongside the full name."""
    with patch(
        "oracle_ai_data_platform_fusion_autopilot.orchestrator.discovery.requests.get",
        return_value=_fake_response(200, _ANON_RESPONSE),
    ):
        mapping = discover_pvo_schemas("https://x", "u", "p")

    # Convert sets to sorted lists for stable comparison
    actual = {k: sorted(v) for k, v in mapping.items()}
    expected = {
        "FscmTopModelAM.FinExtractAM.GlBiccExtractAM.JournalHeaderExtractPVO": ["Financial"],
        "JournalHeaderExtractPVO": ["Financial"],  # viewObjectName duplicate
        "FscmTopModelAM.ScmExtractAM.EgpBiccExtractAM.ItemExtractPVO": ["Financial"],
        "FscmTopModelAM.PrcExtractAM.PozBiccExtractAM.SupplierExtractPVO": ["Procurement"],
    }
    assert actual == expected


def test_discover_walker_silently_skips_datastores_with_no_ancestor_schema():
    """A datastore-name found outside any ``offeringName`` wrapper is silently
    skipped — yielding ``(None, name)`` would inflate the unique-match count
    and risk a wrong auto-correct."""
    orphan_response = {
        "items": [
            {"name": "OrphanDatastoreNoSchema"},  # no offeringName anywhere up the tree
            {
                "offeringName": "Financial",
                "datastores": [{"name": "RealDatastore"}],
            },
        ],
    }
    with patch(
        "oracle_ai_data_platform_fusion_autopilot.orchestrator.discovery.requests.get",
        return_value=_fake_response(200, orphan_response),
    ):
        mapping = discover_pvo_schemas("https://x", "u", "p")

    assert "OrphanDatastoreNoSchema" not in mapping, (
        "orphan datastores (no enclosing schema) must NOT appear in the mapping"
    )
    assert mapping == {"RealDatastore": {"Financial"}}


def test_discover_walker_handles_inline_schema_form():
    """Alternative response shape: each datastore inlines its own
    ``offeringName`` (no parent wrapper). Walker must accept this too."""
    inline_response = [
        {"name": "X", "offeringName": "Y"},
        {"name": "A", "schemaName": "B"},  # alternative schema key
    ]
    with patch(
        "oracle_ai_data_platform_fusion_autopilot.orchestrator.discovery.requests.get",
        return_value=_fake_response(200, inline_response),
    ):
        mapping = discover_pvo_schemas("https://x", "u", "p")

    assert mapping == {"X": {"Y"}, "A": {"B"}}


def test_discover_raises_DiscoveryProbeError_on_http_error():
    """Non-200 response → DiscoveryProbeError with the status code surfaced."""
    with patch(
        "oracle_ai_data_platform_fusion_autopilot.orchestrator.discovery.requests.get",
        return_value=_fake_response(500, text="Internal Server Error"),
    ):
        with pytest.raises(DiscoveryProbeError) as exc_info:
            discover_pvo_schemas("https://x", "u", "p")
    assert "500" in str(exc_info.value)
    # Also covers requests.RequestException raising at the helper layer
    with patch(
        "oracle_ai_data_platform_fusion_autopilot.orchestrator.discovery.requests.get",
        side_effect=requests.ConnectionError("connection refused"),
    ):
        with pytest.raises(DiscoveryProbeError) as exc_info:
            discover_pvo_schemas("https://x", "u", "p")
    assert "connection refused" in str(exc_info.value).lower()
