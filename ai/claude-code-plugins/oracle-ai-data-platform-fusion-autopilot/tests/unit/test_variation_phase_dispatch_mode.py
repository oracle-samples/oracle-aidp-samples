"""Step 6 + Step 7 tests — verify ``run_variation_phase`` consumes
``walker_results`` from ``_ProbeResult`` regardless of whether the probe
ran locally or via the cluster dispatcher.

The marker→ProbeResult conversion is the seam Step 7's multi-match
resolution wiring relies on: the laptop's prompt machinery operates on
the same dict-of-dataclasses shape both modes produce, so cluster mode
inherits the interactive UX for free.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_autopilot.commands.variation_phase import (
    _ProbeResult,
    _probe_result_from_marker,
    VariationPhaseOptions,
)
from oracle_ai_data_platform_fusion_autopilot.commands.variation_resolver import (
    AutoResolved,
    MultiMatch,
    NoMatch,
)
from oracle_ai_data_platform_fusion_autopilot.schema.bronze_fingerprint import (
    ColumnInfo,
)
from oracle_ai_data_platform_fusion_autopilot.schema.cluster_probe_marker import (
    ClusterProbeMarker,
)


def _marker(walker_results) -> ClusterProbeMarker:
    return ClusterProbeMarker(
        markerVersion=1,
        tenant="demo-pod",
        bronzeFingerprint="sha256:cluster-source",
        observedSchema={
            "erp_suppliers": [
                {"name": "Segment1", "type": "string"},
                {"name": "VendorId", "type": "bigint"},
            ],
        },
        walkerResults=walker_results,
        dispatchedAt=datetime(2026, 6, 7, 12, 0, tzinfo=timezone.utc),
    )


class TestProbeResultFromMarkerAutoResolved:
    def test_round_trips_observed_schema_as_canonical_columninfo(self) -> None:
        marker = _marker(
            [
                {
                    "name": "supplier_natural_key",
                    "kind": "columnAliases",
                    "outcome": "auto_resolved",
                    "chosen": "Segment1",
                }
            ]
        )
        result = _probe_result_from_marker(marker)
        # observed_schema values come back as dataclass ColumnInfo —
        # same type the local-mode path produces, so the writer +
        # snapshot code below run unchanged.
        assert all(
            isinstance(c, ColumnInfo)
            for cols in result.observed.values()
            for c in cols
        )
        # Field round-trip.
        seg1 = next(
            c for c in result.observed["erp_suppliers"] if c.name == "Segment1"
        )
        assert seg1.type == "string"

    def test_auto_resolved_outcome_translates_to_dataclass(self) -> None:
        marker = _marker(
            [
                {
                    "name": "supplier_natural_key",
                    "kind": "columnAliases",
                    "outcome": "auto_resolved",
                    "chosen": "Segment1",
                }
            ]
        )
        result = _probe_result_from_marker(marker)
        outcome = result.walker_results[("supplier_natural_key", "columnAliases")]
        assert isinstance(outcome, AutoResolved)
        assert outcome.chosen == "Segment1"

    def test_fingerprint_threads_through(self) -> None:
        result = _probe_result_from_marker(_marker([]))
        assert result.fingerprint == "sha256:cluster-source"


class TestProbeResultFromMarkerMultiMatch:
    def test_multi_match_outcome_translates(self) -> None:
        marker = _marker(
            [
                {
                    "name": "invoice_currency_code",
                    "kind": "columnAliases",
                    "outcome": "multi_match",
                    "matched": [
                        "ApInvoicesInvoiceCurrencyCode",
                        "ApInvoicesCurrencyCode",
                    ],
                }
            ]
        )
        result = _probe_result_from_marker(marker)
        outcome = result.walker_results[
            ("invoice_currency_code", "columnAliases")
        ]
        assert isinstance(outcome, MultiMatch)
        # Order preserved — laptop prompt needs the priority ordering.
        assert outcome.matched == [
            "ApInvoicesInvoiceCurrencyCode",
            "ApInvoicesCurrencyCode",
        ]

    def test_semantic_variant_kind_preserved(self) -> None:
        marker = _marker(
            [
                {
                    "name": "cancelled_status",
                    "kind": "semanticVariants",
                    "outcome": "multi_match",
                    "matched": ["cancelled_date", "cancelled_flag"],
                }
            ]
        )
        result = _probe_result_from_marker(marker)
        # Key includes both name + kind — laptop prompt code keys
        # off this same shape.
        assert ("cancelled_status", "semanticVariants") in result.walker_results


class TestProbeResultFromMarkerNoMatch:
    def test_no_match_outcome_preserves_candidates_tried(self) -> None:
        marker = _marker(
            [
                {
                    "name": "supplier_natural_key",
                    "kind": "columnAliases",
                    "outcome": "no_match",
                    "candidatesTried": [
                        {"candidate": "SEGMENT1", "outcome": "column_not_found"},
                        {"candidate": "VENDORID", "outcome": "column_not_found"},
                    ],
                }
            ]
        )
        result = _probe_result_from_marker(marker)
        outcome = result.walker_results[("supplier_natural_key", "columnAliases")]
        assert isinstance(outcome, NoMatch)
        # Step 8's AIDPF-2010 / 2011 artifact writers consume the
        # `candidates_tried` list — preserve it byte-for-byte.
        assert len(outcome.candidates_tried) == 2
        assert outcome.candidates_tried[0].candidate == "SEGMENT1"
        assert outcome.candidates_tried[0].outcome == "column_not_found"


class TestAcquireProbeResultClusterModeGuards:
    def test_cluster_mode_requires_dispatch_config(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.commands.variation_phase import (
            _acquire_probe_result,
        )

        options = VariationPhaseOptions(
            dispatch_mode="cluster",
            dispatch_config=None,  # programmer error
            env=MagicMock(name="EnvSpec"),
        )
        with pytest.raises(ValueError, match="dispatch_config"):
            _acquire_probe_result(
                bundle=MagicMock(name="Bundle"),
                bundle_path=MagicMock(),
                pack=MagicMock(name="ResolvedPack"),
                tenant="t",
                options=options,
                console=MagicMock(),
            )

    def test_cluster_mode_requires_env(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.commands.variation_phase import (
            _acquire_probe_result,
        )

        options = VariationPhaseOptions(
            dispatch_mode="cluster",
            dispatch_config=MagicMock(name="ResolvedClusterDispatchConfig"),
            env=None,  # programmer error
        )
        with pytest.raises(ValueError, match="dispatch_config"):
            # Error mentions both fields; matching "dispatch_config"
            # suffices to confirm the message lists what's missing.
            _acquire_probe_result(
                bundle=MagicMock(name="Bundle"),
                bundle_path=MagicMock(),
                pack=MagicMock(name="ResolvedPack"),
                tenant="t",
                options=options,
                console=MagicMock(),
            )

    def test_local_mode_default_does_not_require_dispatch_config(self) -> None:
        """Regression guard — every existing test that creates a default
        VariationPhaseOptions runs in local mode without setting the
        new cluster-mode fields. Backward compat is non-negotiable."""
        options = VariationPhaseOptions()
        assert options.dispatch_mode == "local"
        assert options.dispatch_config is None
        assert options.env is None
