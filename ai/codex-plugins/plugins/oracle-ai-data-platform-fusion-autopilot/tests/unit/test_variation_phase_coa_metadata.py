"""FR-14a outcome semantics in the variation phase — the marker-consuming
half (`_precompute_coa_metadata` / `_merge_coa_metadata_into_profile` /
`_report_coa_metadata_outcome`), driven by literal marker sections.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from rich.console import Console

from oracle_ai_data_platform_fusion_autopilot.commands.variation_phase import (
    VariationPhaseOptions,
    _merge_coa_metadata_into_profile,
    _precompute_coa_metadata,
    _report_coa_metadata_outcome,
)
from oracle_ai_data_platform_fusion_autopilot.schema.cluster_probe_marker import (
    CoaCandidateArmMarker,
    CoaChartProbeMarker,
    CoaMetadataMarker,
)


def _console():
    return Console(record=True, width=120)


def _marker(charts: dict[str, tuple[int, int, int]], *,
            active: dict[str, int] | None = None,
            probe_note: str | None = None,
            coverage: str = "complete") -> CoaMetadataMarker:
    """charts: chart_id -> (active_rows, na_distinct, na_ambiguous)."""
    return CoaMetadataMarker(
        source="bicc-pvo", coverage=coverage, probeNote=probe_note,
        candidates=[
            CoaCandidateArmMarker(
                chartId=c, balancingSegment="CodeCombinationSegment1",
                costCenterSegment="CodeCombinationSegment2",
                naturalAccountSegment="CodeCombinationSegment3",
            ) for c in charts
        ],
        probes=[
            CoaChartProbeMarker(chartId=c, activeRows=a, naDistinct=d,
                                naAmbiguous=m)
            for c, (a, d, m) in charts.items()
        ],
        activeCharts=active if active is not None else {
            c: a for c, (a, _, _) in charts.items()
        },
    )


OPTS = VariationPhaseOptions(resolve_coa_from_metadata=True)


def _profile(coa=None):
    prof = MagicMock()
    prof.profile = {"chartOfAccounts": coa or {}}
    prof.provenance = {}
    return prof


class TestFR14aRows:
    def test_s6_flag_off_is_none(self) -> None:
        assert _precompute_coa_metadata(None, VariationPhaseOptions()) is None

    def test_s3_local_mode_no_marker_is_2021_exit_1(self, tmp_path: Path) -> None:
        state = _precompute_coa_metadata(None, OPTS)
        assert state is not None and state.skip_reason is not None
        assert "AIDPF-2021" in state.skip_reason
        exit_code, diags = _report_coa_metadata_outcome(
            state, chart_of_accounts=None, workdir=tmp_path, run_id="r1",
            console=_console(),
        )
        assert exit_code == 1
        assert any("AIDPF-2021__coa-metadata.json" in str(d) for d in diags)

    def test_s3_cluster_fetch_skipped_is_2021(self, tmp_path: Path) -> None:
        marker = CoaMetadataMarker(coverage="skipped", skipReason="boom")
        state = _precompute_coa_metadata(marker, OPTS)
        assert state.skip_reason and "boom" in state.skip_reason
        exit_code, _ = _report_coa_metadata_outcome(
            state, chart_of_accounts=None, workdir=tmp_path, run_id="r1",
            console=_console(),
        )
        assert exit_code == 1

    def test_s1_all_active_resolved_exit_0_and_merged(self, tmp_path: Path) -> None:
        state = _precompute_coa_metadata(
            _marker({"138": (5000, 400, 0), "22625": (900, 200, 0)}), OPTS,
        )
        assert state.skip_reason is None
        assert set(state.outcome.persistable) == {"138", "22625"}
        assert state.metadata_default is not None  # rung 2.5 feed
        profile = _profile()
        report = _merge_coa_metadata_into_profile(
            profile, state, run_id="r1", operator="op",
            now=__import__("datetime").datetime.now(
                __import__("datetime").timezone.utc),
        )
        assert set(report.added) == {"138", "22625"}
        merged = profile.profile["chartOfAccounts"]["byChart"]
        assert merged["138"]["naturalAccountSegment"] == "CodeCombinationSegment3"
        audit = profile.provenance["chartOfAccounts"]["metadataResolution"]
        assert audit["chartsAdded"] == ["138", "22625"]
        assert audit["verification"]["perChart"]["138"]["verdict"] == "verified"
        exit_code, diags = _report_coa_metadata_outcome(
            state, chart_of_accounts=profile.profile["chartOfAccounts"],
            workdir=tmp_path, run_id="r1", console=_console(),
        )
        assert exit_code == 0 and diags == []

    def test_s2_rejected_chart_leaves_unresolved_exit_1(self, tmp_path: Path) -> None:
        state = _precompute_coa_metadata(
            _marker({"138": (5000, 400, 0), "9": (402, 402, 212)}), OPTS,
        )
        assert [v.arm.chart_id for v in state.outcome.rejected] == ["9"]
        profile = _profile()
        _merge_coa_metadata_into_profile(
            profile, state, run_id="r1", operator="op",
            now=__import__("datetime").datetime.now(
                __import__("datetime").timezone.utc),
        )
        exit_code, diags = _report_coa_metadata_outcome(
            state, chart_of_accounts=profile.profile["chartOfAccounts"],
            workdir=tmp_path, run_id="r1", console=_console(),
        )
        assert exit_code == 1  # chart 9 active but unmapped → 2023
        names = [str(d) for d in diags]
        assert any("AIDPF-2022__coa-metadata.json" in n for n in names)
        assert any("AIDPF-2023__coa-metadata.json" in n for n in names)
        # Monotonic: the verified arm IS persisted despite the verdict.
        assert "138" in profile.profile["chartOfAccounts"]["byChart"]

    def test_s5_gl_coa_unavailable_exit_0_nothing_persisted(
        self, tmp_path: Path,
    ) -> None:
        marker = _marker({}, active={}, probe_note="gl_coa probe unavailable: X")
        marker = CoaMetadataMarker(
            source="bicc-pvo", coverage="complete",
            probeNote="gl_coa probe unavailable: AnalysisException",
            candidates=[CoaCandidateArmMarker(
                chartId="138", balancingSegment="CodeCombinationSegment1",
                costCenterSegment="CodeCombinationSegment2",
                naturalAccountSegment="CodeCombinationSegment3")],
            probes=[], activeCharts={},
        )
        state = _precompute_coa_metadata(marker, OPTS)
        assert state.outcome.persistable == {}          # all UNVERIFIED
        assert state.outcome.unverified == (("138", "probe_not_run"),)
        assert state.metadata_default is None            # ladder untouched (S5b safe)
        exit_code, diags = _report_coa_metadata_outcome(
            state, chart_of_accounts={}, workdir=tmp_path, run_id="r1",
            console=_console(),
        )
        assert exit_code == 0 and diags == []            # S5a: honest two-pass

    def test_existing_arm_wins_on_merge(self) -> None:
        state = _precompute_coa_metadata(_marker({"138": (5000, 400, 0)}), OPTS)
        existing = {"byChart": {"138": {
            "balancingSegment": "CodeCombinationSegment9",
            "costCenterSegment": "CodeCombinationSegment8",
            "naturalAccountSegment": "CodeCombinationSegment7"}}}
        profile = _profile(existing)
        report = _merge_coa_metadata_into_profile(
            profile, state, run_id="r1", operator="op",
            now=__import__("datetime").datetime.now(
                __import__("datetime").timezone.utc),
        )
        assert report.kept == ("138",)
        assert profile.profile["chartOfAccounts"]["byChart"]["138"][
            "naturalAccountSegment"] == "CodeCombinationSegment7"
        assert len(report.disagreements) == 3


class TestRoleDomainAtPrecompute:
    """§7.3 wiring: `_precompute_coa_metadata(role_domains=...)` threads the
    pack's semanticRole candidate domain into `verify_arms`, so a derived arm
    binding outside the contract (live incident: chart 41627 → Segment9 vs a
    Segment1-6 starter domain) is REJECTED — never persistable, never the
    rung-2.5 default."""

    DOMAIN = {
        role: frozenset(f"CodeCombinationSegment{i}" for i in range(1, 7))
        for role in ("coa.balancing", "coa.cost_center", "coa.natural_account")
    }

    def _deep_marker(self) -> CoaMetadataMarker:
        return CoaMetadataMarker(
            source="bicc-pvo", coverage="complete",
            candidates=[
                CoaCandidateArmMarker(
                    chartId="41627",
                    balancingSegment="CodeCombinationSegment1",
                    costCenterSegment="CodeCombinationSegment9",
                    naturalAccountSegment="CodeCombinationSegment3",
                ),
                CoaCandidateArmMarker(
                    chartId="101",
                    balancingSegment="CodeCombinationSegment1",
                    costCenterSegment="CodeCombinationSegment2",
                    naturalAccountSegment="CodeCombinationSegment3",
                ),
            ],
            probes=[
                CoaChartProbeMarker(chartId="41627", activeRows=9000,
                                    naDistinct=200, naAmbiguous=0),
                CoaChartProbeMarker(chartId="101", activeRows=1500,
                                    naDistinct=120, naAmbiguous=0),
            ],
            activeCharts={"41627": 9000, "101": 1500},
        )

    def test_deep_arm_rejected_in_domain_but_in_domain_chart_persists(self) -> None:
        state = _precompute_coa_metadata(
            self._deep_marker(), OPTS, role_domains=self.DOMAIN
        )
        assert set(state.outcome.persistable) == {"101"}
        assert len(state.outcome.rejected) == 1
        assert "AIDPF-2042" in state.outcome.rejected[0].detail
        # rung-2.5 default never selects the rejected deep chart (even though
        # it has the most active rows).
        assert state.metadata_default_source == "fusion_metadata:chart=101"

    def test_without_domains_deep_arm_would_persist(self) -> None:
        """Guard for the wiring itself: omitting role_domains reproduces the
        pre-fix behaviour (arm verifies on its clean natural-account probe) —
        proving the call sites MUST pass the pack domain."""
        state = _precompute_coa_metadata(self._deep_marker(), OPTS)
        assert "41627" in state.outcome.persistable


class TestRepinConfirmationFlow:
    """§10.1 repin rules at the merge helper: --non-interactive with a
    disagreement REFUSES (RefreshRequiresConfirmation); interactive accept
    applies the repin (audit block gains `repinned`); decline keeps the
    existing arm (recorded, not applied)."""

    def _profile_obj(self):
        class _P:
            profile = {
                "chartOfAccounts": {
                    "default": {
                        "balancingSegment": "CodeCombinationSegment1",
                        "costCenterSegment": "CodeCombinationSegment2",
                        "naturalAccountSegment": "CodeCombinationSegment3",
                    },
                    "byChart": {
                        "138": {
                            "balancingSegment": "CodeCombinationSegment1",
                            "costCenterSegment": "CodeCombinationSegment2",
                            "naturalAccountSegment": "CodeCombinationSegment3",
                        },
                    },
                },
            }
            provenance: dict = {}
        p = _P()
        p.profile = {k: (dict(v) if isinstance(v, dict) else v)
                     for k, v in p.profile.items()}
        p.provenance = {}
        return p

    def _state(self):
        marker = CoaMetadataMarker(
            source="bicc-pvo", coverage="complete",
            candidates=[CoaCandidateArmMarker(
                chartId="138",
                balancingSegment="CodeCombinationSegment1",
                costCenterSegment="CodeCombinationSegment2",
                naturalAccountSegment="CodeCombinationSegment4",  # disagrees
            )],
            probes=[CoaChartProbeMarker(chartId="138", activeRows=5000,
                                        naDistinct=300, naAmbiguous=0)],
            activeCharts={"138": 5000},
        )
        return _precompute_coa_metadata(marker, OPTS)

    def _merge(self, profile, *, repin: bool, non_interactive: bool,
               answers: list[str] | None = None):
        from datetime import datetime, timezone

        from oracle_ai_data_platform_fusion_autopilot.commands.variation_phase import (
            VariationPhaseOptions,
            _merge_coa_metadata_into_profile,
        )

        opts = VariationPhaseOptions(
            resolve_coa_from_metadata=True,
            repin_coa_from_metadata=repin,
            non_interactive=non_interactive,
            input_fn=(lambda _prompt: answers.pop(0)) if answers else None,
        )
        return _merge_coa_metadata_into_profile(
            profile, self._state(), run_id="r-1", operator="op",
            now=datetime(2026, 8, 6, tzinfo=timezone.utc),
            options=opts, console=_console(),
        )

    def test_non_interactive_repin_refuses(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.commands.variation_phase import (
            RefreshRequiresConfirmation,
        )

        profile = self._profile_obj()
        with pytest.raises(RefreshRequiresConfirmation) as exc:
            self._merge(profile, repin=True, non_interactive=True)
        assert "MUTATING" in str(exc.value)
        # Nothing applied.
        assert profile.profile["chartOfAccounts"]["byChart"]["138"][
            "naturalAccountSegment"
        ] == "CodeCombinationSegment3"

    def test_interactive_accept_applies_repin(self) -> None:
        profile = self._profile_obj()
        report = self._merge(
            profile, repin=True, non_interactive=False, answers=["y"],
        )
        assert profile.profile["chartOfAccounts"]["byChart"]["138"][
            "naturalAccountSegment"
        ] == "CodeCombinationSegment4"
        assert len(report.repinned) == 1
        audit = profile.provenance["chartOfAccounts"]["metadataResolution"]
        assert audit["repinned"][0]["chart"] == "138"
        prov = profile.provenance["chartOfAccounts"]["byChart"]["138"]
        assert prov["natural_account"]["repinnedFrom"] == "CodeCombinationSegment3"

    def test_interactive_decline_keeps_existing(self) -> None:
        profile = self._profile_obj()
        report = self._merge(
            profile, repin=True, non_interactive=False, answers=["n"],
        )
        assert profile.profile["chartOfAccounts"]["byChart"]["138"][
            "naturalAccountSegment"
        ] == "CodeCombinationSegment3"
        assert report.repinned == ()
        assert len(report.disagreements) == 1

    def test_without_repin_flag_disagreement_is_additive(self) -> None:
        profile = self._profile_obj()
        report = self._merge(profile, repin=False, non_interactive=True)
        assert profile.profile["chartOfAccounts"]["byChart"]["138"][
            "naturalAccountSegment"
        ] == "CodeCombinationSegment3"
        assert len(report.disagreements) == 1
