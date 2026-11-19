"""P2 verification slice — ``verify_arms`` verdict mapping (design §7.2),
zero-evidence handling, ``select_default_arm`` determinism, the BICC
three-PVO join, and the ledger. Verdicts come EXCLUSIVELY from the shared
gate result plus probe evidence fields — no threshold re-derivation.
"""

from __future__ import annotations

from oracle_ai_data_platform_fusion_autopilot.commands.coa_metadata_resolution import (
    CandidateArm,
    KFF_METADATA_PVOS,
    ResourceCandidate,
    bicc_rows_to_qualifier_rows,
    derive_arms,
    render_ledger,
    select_default_arm,
    verify_arms,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.coa_gate import (
    SAMPLE_FLOOR_DISTINCT,
    SAMPLE_FLOOR_ROWS,
    ChartProbe,
)


def _arm(chart: str) -> CandidateArm:
    return CandidateArm(
        chart_id=chart,
        balancing_segment="CodeCombinationSegment1",
        cost_center_segment="CodeCombinationSegment2",
        natural_account_segment="CodeCombinationSegment3",
    )


def _probe(chart: str, *, active: int, distinct: int, ambiguous: int = 0):
    return ChartProbe(
        chart_id=chart, active_row_count=active,
        natural_account_distinct=distinct, natural_account_ambiguous=ambiguous,
    )


class TestVerifyArmsVerdictMapping:
    def test_clean_sufficient_is_verified(self) -> None:
        out = verify_arms(
            {"138": _arm("138")},
            {"138": _probe("138", active=SAMPLE_FLOOR_ROWS,
                           distinct=SAMPLE_FLOOR_DISTINCT)},
        )
        assert out.persistable["138"].verdict == "verified"
        assert out.persistable["138"].active_rows == SAMPLE_FLOOR_ROWS

    def test_strong_contradiction_is_rejected_never_persisted(self) -> None:
        out = verify_arms(
            {"138": _arm("138")},
            {"138": _probe("138", active=402, distinct=402, ambiguous=212)},
        )
        assert out.persistable == {}
        assert len(out.rejected) == 1
        assert out.rejected[0].verdict == "rejected"
        assert "AIDPF-2017" in out.rejected[0].detail

    def test_clean_below_floor_is_verified_weak_not_verified(self) -> None:
        out = verify_arms(
            {"138": _arm("138")},
            {"138": _probe("138", active=SAMPLE_FLOOR_ROWS - 1,
                           distinct=SAMPLE_FLOOR_DISTINCT)},
        )
        assert out.persistable["138"].verdict == "verified_weak"

    def test_zero_evidence_is_unverified_before_below_floor(self) -> None:
        # n == 0 (all-null natural account) with active rows: NO empirical
        # backing — never persisted, never "weak".
        out = verify_arms(
            {"138": _arm("138")},
            {"138": _probe("138", active=500, distinct=0)},
        )
        assert out.persistable == {}
        assert out.unverified == (("138", "zero_evidence"),)

    def test_zero_active_rows_is_unverified(self) -> None:
        out = verify_arms(
            {"138": _arm("138")},
            {"138": _probe("138", active=0, distinct=0)},
        )
        assert out.unverified == (("138", "zero_evidence"),)

    def test_missing_probe_is_unverified_probe_not_run(self) -> None:
        out = verify_arms({"138": _arm("138")}, {})
        assert out.unverified == (("138", "probe_not_run"),)

    def test_warning_above_floor_is_verified_weak(self) -> None:
        out = verify_arms(
            {"138": _arm("138")},
            {"138": _probe("138", active=500, distinct=100, ambiguous=25)},
        )
        assert out.persistable["138"].verdict == "verified_weak"
        assert out.persistable["138"].detail  # the gate's warning text


class TestSelectDefaultArm:
    def test_highest_active_verified_wins(self) -> None:
        out = verify_arms(
            {"1": _arm("1"), "2": _arm("2")},
            {"1": _probe("1", active=100, distinct=50),
             "2": _probe("2", active=9000, distinct=500)},
        )
        selected = select_default_arm(out.persistable)
        assert selected is not None and selected[0] == "2"

    def test_strong_verified_preferred_over_weak(self) -> None:
        out = verify_arms(
            {"1": _arm("1"), "2": _arm("2")},
            {"1": _probe("1", active=100, distinct=50),          # verified
             "2": _probe("2", active=50, distinct=10)},           # weak
        )
        selected = select_default_arm(out.persistable)
        assert selected is not None and selected[0] == "1"

    def test_tie_breaks_on_lowest_numeric_chart_id(self) -> None:
        out = verify_arms(
            {"22625": _arm("22625"), "138": _arm("138")},
            {"22625": _probe("22625", active=100, distinct=50),
             "138": _probe("138", active=100, distinct=50)},
        )
        selected = select_default_arm(out.persistable)
        assert selected is not None and selected[0] == "138"

    def test_empty_is_none(self) -> None:
        assert select_default_arm({}) is None


class TestBiccJoin:
    SI = [
        {"StructureInstanceNumber": "138", "StructureId": "9001",
         "KeyFlexfieldCode": "GL#", "EnabledFlag": "Y"},
        {"StructureInstanceNumber": "22625", "StructureId": "9002",
         "KeyFlexfieldCode": "GL#", "EnabledFlag": True},
        # non-GL flexfield — filtered out
        {"StructureInstanceNumber": "77", "StructureId": "9100",
         "KeyFlexfieldCode": "CAT#", "EnabledFlag": "Y"},
        # disabled — filtered out
        {"StructureInstanceNumber": "88", "StructureId": "9001",
         "KeyFlexfieldCode": "GL#", "EnabledFlag": "N"},
    ]
    SEG = [
        {"StructureId": "9001", "SegmentCode": "CO", "ColumnName": "SEGMENT1"},
        {"StructureId": "9001", "SegmentCode": "CC", "ColumnName": "SEGMENT4"},
        {"StructureId": "9001", "SegmentCode": "ACC", "ColumnName": "SEGMENT6"},
        {"StructureId": "9002", "SegmentCode": "CO", "ColumnName": "SEGMENT1"},
        {"StructureId": "9002", "SegmentCode": "CC", "ColumnName": "SEGMENT2"},
        {"StructureId": "9002", "SegmentCode": "ACC", "ColumnName": "SEGMENT3"},
    ]
    LAB = [
        {"StructureId": "9001", "SegmentCode": "CO", "SegmentLabelCode": "GL_BALANCING"},
        {"StructureId": "9001", "SegmentCode": "CC", "SegmentLabelCode": "FA_COST_CTR"},
        {"StructureId": "9001", "SegmentCode": "ACC", "SegmentLabelCode": "GL_ACCOUNT"},
        {"StructureId": "9002", "SegmentCode": "CO", "SegmentLabelCode": "GL_BALANCING"},
        {"StructureId": "9002", "SegmentCode": "CC", "SegmentLabelCode": "FA_COST_CTR"},
        {"StructureId": "9002", "SegmentCode": "ACC", "SegmentLabelCode": "GL_ACCOUNT"},
    ]

    def test_join_feeds_the_shared_derivation(self) -> None:
        rows = bicc_rows_to_qualifier_rows(self.SI, self.SEG, self.LAB)
        arms, rejects = derive_arms(rows, candidate=ResourceCandidate(path="/x"))
        assert rejects == ()
        assert arms["138"].as_by_chart_block() == {
            "balancingSegment": "CodeCombinationSegment1",
            "costCenterSegment": "CodeCombinationSegment4",
            "naturalAccountSegment": "CodeCombinationSegment6",
        }
        assert arms["22625"].natural_account_segment == "CodeCombinationSegment3"
        assert set(arms) == {"138", "22625"}  # CAT# and disabled excluded

    def test_pvo_names_are_the_smoke_pinned_triplet(self) -> None:
        assert set(KFF_METADATA_PVOS) == {
            "structure_instances", "segments", "labeled_segments",
        }
        for name in KFF_METADATA_PVOS.values():
            assert name.startswith(
                "FscmTopModelAM.FinExtractAM.AnalyticsExtractServiceAM."
            )


class TestLedger:
    def test_ledger_names_rejects_unverified_and_unresolved(self) -> None:
        out = verify_arms(
            {"1": _arm("1"), "2": _arm("2"), "3": _arm("3")},
            {"1": _probe("1", active=500, distinct=100),
             "2": _probe("2", active=402, distinct=402, ambiguous=212),
             "3": _probe("3", active=500, distinct=0)},
        )
        lines = render_ledger(out, unresolved_active=["2", "3", "9"])
        text = "\n".join(lines)
        assert "1 chart(s) verified" in text
        assert "AIDPF-2022 chart '2' REJECTED" in text
        assert "chart '3' UNVERIFIED (zero_evidence)" in text
        assert "AIDPF-2023 3 active chart(s) remain unmapped" in text


# ── merge + ladder rung 2.5 (coa_resolution) ────────────────────────────────

from oracle_ai_data_platform_fusion_autopilot.commands.coa_resolution import (  # noqa: E402
    CoaResolutionInput,
    merge_metadata_arms,
    resolve_coa_roles,
)

ALIASES = {
    "coa_balancing_segment": "coa.balancing",
    "coa_cost_center_segment": "coa.cost_center",
    "coa_natural_account_segment": "coa.natural_account",
}
BLOCK_138 = {
    "balancingSegment": "CodeCombinationSegment1",
    "costCenterSegment": "CodeCombinationSegment4",
    "naturalAccountSegment": "CodeCombinationSegment6",
}


class TestMergeMetadataArms:
    def test_adds_missing_arm_with_verified_provenance(self) -> None:
        coa = {"default": {"balancingSegment": "CodeCombinationSegment1",
                           "costCenterSegment": "CodeCombinationSegment2",
                           "naturalAccountSegment": "CodeCombinationSegment3"}}
        new_coa, new_prov, report = merge_metadata_arms(
            coa, {}, verified_arms={"138": BLOCK_138},
            verdicts={"138": "verified_weak"},
        )
        assert report.added == ("138",)
        assert new_coa["byChart"]["138"] == BLOCK_138
        prov = new_prov["byChart"]["138"]["natural_account"]
        assert prov["mechanism"] == "metadata_resolved"
        assert prov["verification"] == "verified_weak"
        assert coa.get("byChart") is None  # pure — input unmutated

    def test_existing_arm_wins_disagreement_recorded_not_applied(self) -> None:
        existing_arm = {"balancingSegment": "CodeCombinationSegment1",
                        "costCenterSegment": "CodeCombinationSegment2",
                        "naturalAccountSegment": "CodeCombinationSegment3"}
        coa = {"byChart": {"138": dict(existing_arm)}}
        new_coa, _, report = merge_metadata_arms(
            coa, {}, verified_arms={"138": BLOCK_138},
            verdicts={"138": "verified"},
        )
        assert report.kept == ("138",)
        assert new_coa["byChart"]["138"] == existing_arm  # untouched
        assert {(d["role"], d["existing"], d["metadata"])
                for d in report.disagreements} == {
            ("cost_center", "CodeCombinationSegment2", "CodeCombinationSegment4"),
            ("natural_account", "CodeCombinationSegment3", "CodeCombinationSegment6"),
        }

    def test_no_arm_without_proof_invariant(self) -> None:
        _, new_prov, _ = merge_metadata_arms(
            {}, {}, verified_arms={"138": BLOCK_138, "9": BLOCK_138},
            verdicts={"138": "verified", "9": "verified_weak"},
        )
        for chart, roles in new_prov["byChart"].items():
            for role, entry in roles.items():
                assert entry["verification"] in ("verified", "verified_weak")
                assert entry["mechanism"] == "metadata_resolved"
                assert "auto_resolve" not in str(entry)


class TestLadderRung25:
    def test_metadata_default_used_when_no_config_or_pin(self) -> None:
        res = resolve_coa_roles(CoaResolutionInput(
            semantic_role_aliases=ALIASES,
            metadata_default={
                "coa.balancing": "CodeCombinationSegment1",
                "coa.cost_center": "CodeCombinationSegment4",
                "coa.natural_account": "CodeCombinationSegment6",
            },
            metadata_default_source="fusion_metadata:chart=138",
        ))
        assert res.role_provenance["natural_account"]["mechanism"] == (
            "metadata_resolved"
        )
        assert res.role_provenance["natural_account"]["source"] == (
            "fusion_metadata:chart=138"
        )
        assert res.chart_of_accounts["default"]["naturalAccountSegment"] == (
            "CodeCombinationSegment6"
        )

    def test_explicit_config_still_beats_metadata(self) -> None:
        res = resolve_coa_roles(CoaResolutionInput(
            semantic_role_aliases=ALIASES,
            explicit_config={"balancingSegment": "CodeCombinationSegment1",
                             "costCenterSegment": "CodeCombinationSegment2",
                             "naturalAccountSegment": "CodeCombinationSegment3"},
            metadata_default={
                "coa.balancing": "CodeCombinationSegment1",
                "coa.cost_center": "CodeCombinationSegment4",
                "coa.natural_account": "CodeCombinationSegment6",
            },
        ))
        assert res.role_provenance["natural_account"]["mechanism"] == (
            "config_resolved"
        )

    def test_existing_pin_still_beats_metadata_on_refresh(self) -> None:
        res = resolve_coa_roles(CoaResolutionInput(
            semantic_role_aliases=ALIASES,
            is_refresh=True,
            existing_chart_of_accounts={
                "default": {"balancingSegment": "CodeCombinationSegment1",
                            "costCenterSegment": "CodeCombinationSegment2",
                            "naturalAccountSegment": "CodeCombinationSegment3"}},
            metadata_default={
                "coa.balancing": "CodeCombinationSegment1",
                "coa.cost_center": "CodeCombinationSegment4",
                "coa.natural_account": "CodeCombinationSegment6",
            },
        ))
        assert res.chart_of_accounts["default"]["naturalAccountSegment"] == (
            "CodeCombinationSegment3"
        )


class TestVerifyArmsRoleDomain:
    """Design §7.3 (live incident 2026-08-06, chart 41627): metadata may
    truthfully bind a segment the pack contract does not carry — the arm must
    be REJECTED at derivation, not persisted and left to explode at render."""

    DOMAIN = {
        role: frozenset(f"CodeCombinationSegment{i}" for i in range(1, 7))
        for role in ("coa.balancing", "coa.cost_center", "coa.natural_account")
    }

    def _deep_arm(self, chart: str) -> CandidateArm:
        return CandidateArm(
            chart_id=chart,
            balancing_segment="CodeCombinationSegment1",
            cost_center_segment="CodeCombinationSegment9",  # out of domain
            natural_account_segment="CodeCombinationSegment3",
        )

    def test_out_of_domain_arm_rejected_with_2042_detail(self) -> None:
        out = verify_arms(
            {"41627": self._deep_arm("41627")},
            {"41627": _probe("41627", active=9000, distinct=200)},
            role_domains=self.DOMAIN,
        )
        assert out.persistable == {}
        assert len(out.rejected) == 1
        assert out.rejected[0].verdict == "rejected"
        assert "AIDPF-2042" in out.rejected[0].detail
        assert "CodeCombinationSegment9" in out.rejected[0].detail

    def test_domain_reject_wins_over_clean_probe(self) -> None:
        """A clean Tier-B probe must NOT rescue an out-of-domain arm — the
        natural-account column can be right while the cost-center binding is
        unrenderable (exactly the live incident)."""
        out = verify_arms(
            {"41627": self._deep_arm("41627")},
            {"41627": _probe("41627", active=SAMPLE_FLOOR_ROWS,
                             distinct=SAMPLE_FLOOR_DISTINCT)},
            role_domains=self.DOMAIN,
        )
        assert out.persistable == {}
        assert out.rejected and "AIDPF-2042" in out.rejected[0].detail

    def test_rejected_arm_excluded_from_default_selection(self) -> None:
        out = verify_arms(
            {
                "41627": self._deep_arm("41627"),   # deep, most active rows
                "101": _arm("101"),                  # in-domain
            },
            {
                "41627": _probe("41627", active=90000, distinct=400),
                "101": _probe("101", active=1500, distinct=120),
            },
            role_domains=self.DOMAIN,
        )
        selected = select_default_arm(out.persistable)
        assert selected is not None
        assert selected[0] == "101"  # never the rejected deep chart

    def test_in_domain_arms_unaffected_and_none_domain_is_noop(self) -> None:
        probes = {"101": _probe("101", active=1500, distinct=120)}
        with_domain = verify_arms(
            {"101": _arm("101")}, probes, role_domains=self.DOMAIN
        )
        without = verify_arms({"101": _arm("101")}, probes)
        assert with_domain.persistable["101"].verdict == "verified"
        assert without.persistable["101"].verdict == "verified"

    def test_deep_arm_passes_when_domain_extended(self) -> None:
        """The coa-deep-overlay path: domain extended to Segment10 → the same
        arm verifies normally."""
        deep_domain = {
            role: frozenset(f"CodeCombinationSegment{i}" for i in range(1, 11))
            for role in self.DOMAIN
        }
        out = verify_arms(
            {"41627": self._deep_arm("41627")},
            {"41627": _probe("41627", active=9000, distinct=200)},
            role_domains=deep_domain,
        )
        assert out.persistable["41627"].verdict == "verified"
        assert out.rejected == ()


class TestMergeRepin:
    """--repin-coa-from-metadata (design §10.1): a disagreeing arm is
    overwritten ONLY for explicitly confirmed charts (``repin_charts``);
    the prior column lands in per-role ``repinnedFrom`` provenance and the
    change in ``report.repinned``. Default stays strictly additive."""

    EXISTING = {
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
    }
    METADATA = {
        "138": {
            "balancingSegment": "CodeCombinationSegment1",
            "costCenterSegment": "CodeCombinationSegment2",
            "naturalAccountSegment": "CodeCombinationSegment4",  # disagrees
        },
    }

    def _merge(self, repin_charts=frozenset()):
        from oracle_ai_data_platform_fusion_autopilot.commands.coa_resolution import (
            merge_metadata_arms,
        )

        return merge_metadata_arms(
            self.EXISTING, {"byChart": {}},
            verified_arms=self.METADATA, verdicts={"138": "verified"},
            repin_charts=repin_charts,
        )

    def test_default_additive_records_not_applies(self) -> None:
        new_coa, _prov, report = self._merge()
        assert new_coa["byChart"]["138"]["naturalAccountSegment"] == (
            "CodeCombinationSegment3"
        )
        assert report.repinned == ()
        assert len(report.disagreements) == 1

    def test_confirmed_chart_is_repinned_with_provenance(self) -> None:
        new_coa, new_prov, report = self._merge(repin_charts=frozenset({"138"}))
        assert new_coa["byChart"]["138"]["naturalAccountSegment"] == (
            "CodeCombinationSegment4"
        )
        assert report.disagreements == ()
        assert len(report.repinned) == 1
        assert report.repinned[0]["existing"] == "CodeCombinationSegment3"
        na_prov = new_prov["byChart"]["138"]["natural_account"]
        assert na_prov["mechanism"] == "metadata_resolved"
        assert na_prov["repinnedFrom"] == "CodeCombinationSegment3"
        # Unchanged roles carry NO repinnedFrom.
        assert "repinnedFrom" not in new_prov["byChart"]["138"]["balancing"]

    def test_unlisted_chart_never_repinned(self) -> None:
        new_coa, _prov, report = self._merge(repin_charts=frozenset({"999"}))
        assert new_coa["byChart"]["138"]["naturalAccountSegment"] == (
            "CodeCombinationSegment3"
        )
        assert report.repinned == ()
