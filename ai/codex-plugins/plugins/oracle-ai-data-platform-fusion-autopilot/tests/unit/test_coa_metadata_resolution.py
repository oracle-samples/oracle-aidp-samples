"""P1 read-only slice of ``coa_metadata_resolution`` — derivation matrix,
NFR-9 path safety (zero-request assertions), the fail-soft fetch ladder, the
shape assertion, and the round-8 fixture invariant (the probe and a direct
``derive_arms()`` call produce identical ``CandidateArm`` values).
"""

from __future__ import annotations

import pytest

from oracle_ai_data_platform_fusion_autopilot.commands.coa_metadata_resolution import (
    AIDPF_2021_COA_METADATA_UNREACHABLE,
    ArmReject,
    CandidateArm,
    ENV_RESOURCE_PATH,
    ResourceCandidate,
    arm_from_qualifiers,
    assert_same_origin,
    derive_arms,
    fetch_segment_qualifiers,
    resolve_resource_plan,
    run_metadata_probe,
    segment_column,
    shape_assertion,
    validate_resource_path,
)
from oracle_ai_data_platform_fusion_autopilot.commands.coa_advisory import _Skip

SERVICE_URL = "https://fusion.example.com"
CANDIDATE = ResourceCandidate(path="/fscmRestApi/resources/11.13.18.05/kffSegments")


def _row(chart: str, qualifier: str | None, column: str,
         identity_attr: str = "StructureInstanceNumber") -> dict:
    row = {identity_attr: chart, "ApplicationColumnName": column}
    if qualifier is not None:
        row["SegmentQualifier"] = qualifier
    return row


FIXTURE_ROWS = [
    # chart 138: balancing=1, cost centre=4, natural account=6 (deep-ish)
    _row("138", "GL_BALANCING", "SEGMENT1"),
    _row("138", "FA_COST_CTR", "SEGMENT4"),
    _row("138", "GL_ACCOUNT", "SEGMENT6"),
    _row("138", None, "SEGMENT2"),          # unqualified segment — ignored
    # chart 22625: conventional 1/2/3
    _row("22625", "GL_BALANCING", "Segment1"),
    _row("22625", "FA_COST_CTR", "Segment2"),
    _row("22625", "GL_ACCOUNT", "Segment3"),
]


class TestSegmentColumn:
    @pytest.mark.parametrize("raw,expected", [
        ("SEGMENT7", "CodeCombinationSegment7"),
        ("Segment7", "CodeCombinationSegment7"),
        ("segment30", "CodeCombinationSegment30"),
        ("CodeCombinationSegment3", "CodeCombinationSegment3"),
        ("codecombinationsegment12", "CodeCombinationSegment12"),
        ("SEGMENT31", None),
        ("SEGMENT0", None),
        ("GL_ACCOUNT_X", None),
        ("", None),
        (None, None),
    ])
    def test_domain(self, raw, expected) -> None:
        assert segment_column(raw) == expected


class TestArmFromQualifiers:
    def test_complete_arm(self) -> None:
        arm = arm_from_qualifiers(
            "138", [r for r in FIXTURE_ROWS if r["StructureInstanceNumber"] == "138"],
            candidate=CANDIDATE,
        )
        assert isinstance(arm, CandidateArm)
        assert arm.as_by_chart_block() == {
            "balancingSegment": "CodeCombinationSegment1",
            "costCenterSegment": "CodeCombinationSegment4",
            "naturalAccountSegment": "CodeCombinationSegment6",
        }

    def test_missing_role_is_incomplete(self) -> None:
        rows = [_row("9", "GL_BALANCING", "SEGMENT1"),
                _row("9", "GL_ACCOUNT", "SEGMENT3")]
        out = arm_from_qualifiers("9", rows, candidate=CANDIDATE)
        assert isinstance(out, ArmReject) and out.reason == "incomplete"
        assert "costCenter" in out.detail

    def test_same_role_two_segments_is_ambiguous(self) -> None:
        rows = [_row("9", "GL_ACCOUNT", "SEGMENT3"),
                _row("9", "GL_ACCOUNT", "SEGMENT5"),
                _row("9", "GL_BALANCING", "SEGMENT1"),
                _row("9", "FA_COST_CTR", "SEGMENT2")]
        out = arm_from_qualifiers("9", rows, candidate=CANDIDATE)
        assert isinstance(out, ArmReject) and out.reason == "ambiguous"

    def test_deep_segment_beyond_30_rejected(self) -> None:
        rows = [_row("9", "GL_BALANCING", "SEGMENT1"),
                _row("9", "FA_COST_CTR", "SEGMENT2"),
                _row("9", "GL_ACCOUNT", "SEGMENT31")]
        out = arm_from_qualifiers("9", rows, candidate=CANDIDATE)
        assert isinstance(out, ArmReject) and out.reason == "invalid_segment"

    def test_two_roles_one_column_is_2016_precheck(self) -> None:
        rows = [_row("9", "GL_BALANCING", "SEGMENT1"),
                _row("9", "FA_COST_CTR", "SEGMENT1"),
                _row("9", "GL_ACCOUNT", "SEGMENT3")]
        out = arm_from_qualifiers("9", rows, candidate=CANDIDATE)
        assert isinstance(out, ArmReject) and out.reason == "duplicate_column"
        assert "2016" in out.detail

    def test_flags_style_strict_truthiness(self) -> None:
        cand = ResourceCandidate(
            path="/x", qualifier_style="flags",
            flag_attrs={"balancing": "BalFlag", "costCenter": "CcFlag",
                        "naturalAccount": "NaFlag"},
        )
        rows = [
            {"StructureInstanceNumber": "9", "ApplicationColumnName": "SEGMENT1",
             "BalFlag": "Y", "CcFlag": "N", "NaFlag": "N"},
            {"StructureInstanceNumber": "9", "ApplicationColumnName": "SEGMENT2",
             "BalFlag": "N", "CcFlag": True, "NaFlag": False},
            {"StructureInstanceNumber": "9", "ApplicationColumnName": "SEGMENT3",
             "BalFlag": "N", "CcFlag": "N", "NaFlag": "Y"},
        ]
        arm = arm_from_qualifiers("9", rows, candidate=cand)
        assert isinstance(arm, CandidateArm)
        assert arm.natural_account_segment == "CodeCombinationSegment3"


class TestDeriveArms:
    def test_multi_chart_fixture(self) -> None:
        arms, rejects = derive_arms(FIXTURE_ROWS, candidate=CANDIDATE)
        assert set(arms) == {"138", "22625"}
        assert rejects == ()
        assert arms["22625"].natural_account_segment == "CodeCombinationSegment3"

    def test_non_numeric_identity_rejected_before_bychart(self) -> None:
        rows = [_row("GL#CHART", "GL_BALANCING", "SEGMENT1")]
        arms, rejects = derive_arms(rows, candidate=CANDIDATE)
        assert arms == {}
        assert [r.reason for r in rejects] == ["invalid_chart_id"]

    def test_rows_missing_identity_ignored(self) -> None:
        arms, rejects = derive_arms(
            [{"ApplicationColumnName": "SEGMENT1"}], candidate=CANDIDATE,
        )
        assert arms == {} and rejects == ()


class TestPathSafety:
    @pytest.mark.parametrize("bad", [
        "@evil.example/x",
        "//evil.example/x",
        "https://evil.example/x",
        "/fscmRestApi/x?y=1",
        "/fscmRestApi/x#frag",
        "/fscmRestApi/ x",
        "/fscm\\x",
        "",
    ])
    def test_invalid_paths_rejected_statically(self, bad: str) -> None:
        assert validate_resource_path(bad, field_name="resourcePath") is not None

    def test_valid_path_accepted(self) -> None:
        assert validate_resource_path(
            "/fscmRestApi/resources/11.13.18.05/x", field_name="resourcePath",
        ) is None

    def test_origin_assertion_catches_userinfo_redirect(self) -> None:
        # The exact attack: base + "@attacker/x" makes the base host USERINFO.
        composed = f"{SERVICE_URL}" + "@attacker.example/x"
        assert assert_same_origin(composed, SERVICE_URL) is not None
        assert assert_same_origin(f"{SERVICE_URL}/ok", SERVICE_URL) is None

    def test_statically_invalid_config_is_2021_with_zero_io(self) -> None:
        class _Meta:
            resource_path = "@evil.example/x"
            segments_child_path = None
            identity_attr = None
            qualifier_attr = None
            segment_column_attr = None
            qualifier_values: dict = {}

        with pytest.raises(_Skip, match=AIDPF_2021_COA_METADATA_UNREACHABLE):
            resolve_resource_plan(_Meta())

    def test_invalid_child_path_is_2021(self) -> None:
        class _Meta:
            resource_path = "/fscmRestApi/resources/x"
            segments_child_path = "https://evil.example/child"
            identity_attr = None
            qualifier_attr = None
            segment_column_attr = None
            qualifier_values: dict = {}

        with pytest.raises(_Skip, match="segmentsChildPath"):
            resolve_resource_plan(_Meta())

    def test_offorigin_candidate_records_zero_requests(self) -> None:
        calls: list[str] = []

        class _Session:
            auth = ("u", "p")

            def get(self, url, **kwargs):
                calls.append(url)
                raise AssertionError("no request may be issued")

        plan = resolve_resource_plan(None, cli_path="/fscmRestApi/resources/x")
        # Force an off-origin composition by asserting against a different
        # service URL host than the composed one.
        fetch = fetch_segment_qualifiers(
            service_url=SERVICE_URL,
            plan=plan.__class__(
                candidates=(ResourceCandidate(path="@evil.example/x"),),
                source="cli",
            ),
            session=_Session(),
        )
        assert fetch.skip_reason is not None
        assert AIDPF_2021_COA_METADATA_UNREACHABLE in fetch.skip_reason
        assert calls == []  # ZERO network I/O


class TestConfigPrecedence:
    def test_cli_beats_env_beats_bundle(self, monkeypatch) -> None:
        class _Meta:
            resource_path = "/from-bundle"
            segments_child_path = None
            identity_attr = None
            qualifier_attr = None
            segment_column_attr = None
            qualifier_values: dict = {}

        env = {ENV_RESOURCE_PATH: "/from-env"}
        plan = resolve_resource_plan(_Meta(), cli_path="/from-cli", env=env)
        assert plan.source == "cli"
        assert plan.candidates[0].path == "/from-cli"
        plan = resolve_resource_plan(_Meta(), env=env)
        assert plan.source == "env"
        plan = resolve_resource_plan(_Meta(), env={})
        assert plan.source == "bundle"
        assert plan.candidates[0].path == "/from-bundle"
        plan = resolve_resource_plan(None, env={})
        assert plan.source == "ladder"
        assert len(plan.candidates) >= 1

    def test_bundle_child_path_appended(self) -> None:
        class _Meta:
            resource_path = "/parent"
            segments_child_path = "/child/segments"
            identity_attr = None
            qualifier_attr = None
            segment_column_attr = None
            qualifier_values: dict = {}

        plan = resolve_resource_plan(_Meta(), env={})
        assert plan.candidates[0].path == "/parent/child/segments"

    def test_non_identifier_attr_is_2021(self) -> None:
        class _Meta:
            resource_path = None
            segments_child_path = None
            identity_attr = "Bad Attr!"
            qualifier_attr = None
            segment_column_attr = None
            qualifier_values: dict = {}

        with pytest.raises(_Skip, match="identityAttr"):
            resolve_resource_plan(_Meta())


class _ScriptedSession:
    """Routes by URL substring; records every call (coa_advisory pattern)."""

    auth = ("u", "p")

    def __init__(self, pages: dict[str, list[dict] | Exception]):
        self.pages = pages
        self.calls: list[str] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append(url)

        class _Resp:
            def __init__(self, items):
                self._items = items
                self.status_code = 200

            def raise_for_status(self):
                pass

            def json(self):
                return {"items": self._items, "hasMore": False}

        for key, value in self.pages.items():
            if key in url:
                if isinstance(value, Exception):
                    raise value
                return _Resp(value)
        return _Resp([])


class TestFetchLadder:
    def test_first_candidate_with_asserting_shape_wins(self) -> None:
        session = _ScriptedSession({"kffSegments": FIXTURE_ROWS})
        plan = resolve_resource_plan(
            None, cli_path="/fscmRestApi/resources/11.13.18.05/kffSegments",
        )
        fetch = fetch_segment_qualifiers(
            service_url=SERVICE_URL, plan=plan, session=session,
        )
        assert fetch.skip_reason is None
        assert fetch.coverage == "complete"
        assert fetch.candidate is not None
        assert fetch.shape is not None and fetch.shape.ok

    def test_wrong_shape_200_is_skipped_never_guessed(self) -> None:
        session = _ScriptedSession({"kffSegments": [{"SomethingElse": 1}]})
        plan = resolve_resource_plan(
            None, cli_path="/fscmRestApi/resources/11.13.18.05/kffSegments",
        )
        fetch = fetch_segment_qualifiers(
            service_url=SERVICE_URL, plan=plan, session=session,
        )
        assert fetch.skip_reason is not None
        assert "shape assertion" in " ".join(
            outcome for _, outcome in fetch.candidates_tried
        )

    def test_401_names_the_privilege(self) -> None:
        class _Err(Exception):
            def __init__(self):
                self.response = type("R", (), {"status_code": 403})()

        session = _ScriptedSession({"kffSegments": _Err()})
        plan = resolve_resource_plan(
            None, cli_path="/fscmRestApi/resources/11.13.18.05/kffSegments",
        )
        fetch = fetch_segment_qualifiers(
            service_url=SERVICE_URL, plan=plan, session=session,
        )
        assert fetch.skip_reason is not None
        assert "FUN_GET_ENTERPRISE_STRUCTURES_REST_SERVICE_PRIV" in fetch.skip_reason

    def test_missing_creds_skip(self, monkeypatch) -> None:
        monkeypatch.delenv("FUSION_BICC_USER", raising=False)
        monkeypatch.delenv("FUSION_BICC_PASSWORD", raising=False)
        plan = resolve_resource_plan(
            None, cli_path="/fscmRestApi/resources/11.13.18.05/kffSegments",
        )
        fetch = fetch_segment_qualifiers(
            service_url=SERVICE_URL, plan=plan, session=None,
        )
        assert fetch.skip_reason is not None


class TestShapeAssertion:
    def test_detects_attribute_style(self) -> None:
        report = shape_assertion(FIXTURE_ROWS[:3], CANDIDATE)
        assert report.ok
        assert report.qualifier_style_detected == "attribute"
        assert report.identity_attr_present

    def test_empty_first_page_fails(self) -> None:
        report = shape_assertion([], CANDIDATE)
        assert not report.ok


class TestProbeFixtureInvariant:
    """Round-8: the probe and a direct ``derive_arms()`` call must produce
    IDENTICAL CandidateArm values from the same qualifier fixture — one
    shared derivation, no probe-local drift. This test must stay green
    UNMODIFIED through P2."""

    def test_probe_equals_direct_derivation(self) -> None:
        session = _ScriptedSession({"kffSegments": FIXTURE_ROWS})
        report = run_metadata_probe(
            service_url=SERVICE_URL,
            coa_metadata=None,
            cli_resource_path="/fscmRestApi/resources/11.13.18.05/kffSegments",
            limit=99,
            session=session,
        )
        assert report["ok"], report
        direct_arms, _ = derive_arms(FIXTURE_ROWS, candidate=ResourceCandidate(
            path="/fscmRestApi/resources/11.13.18.05/kffSegments",
        ))
        assert report["derivedArms"] == {
            chart_id: arm.as_by_chart_block()
            for chart_id, arm in direct_arms.items()
        }

    def test_probe_is_read_only_report(self) -> None:
        session = _ScriptedSession({"kffSegments": FIXTURE_ROWS})
        report = run_metadata_probe(
            service_url=SERVICE_URL, coa_metadata=None,
            cli_resource_path="/fscmRestApi/resources/11.13.18.05/kffSegments",
            limit=1, session=session,
        )
        assert report["chartsDerived"] == 2
        assert len(report["derivedArms"]) == 1  # --limit honoured
        assert report["byChartYamlFragment"].startswith("byChart:")
        assert report["attribute_names_seen"]


class TestBundleSchema:
    """The documented §6.2 example must PARSE — `extra="forbid"` would
    otherwise reject `segmentsChildPath` (round-7 finding 2)."""

    def test_coa_metadata_spec_parses_the_design_example(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import (
            CoaMetadataSpec,
        )

        spec = CoaMetadataSpec.model_validate({
            "resourcePath": "/fscmRestApi/resources/11.13.18.05/pinned",
            "segmentsChildPath": "/child/segments",
            "identityAttr": "StructureInstanceNumber",
            "qualifierAttr": "SegmentQualifier",
            "segmentColumnAttr": "ApplicationColumnName",
            "qualifierValues": {
                "balancing": "GL_BALANCING",
                "costCenter": "FA_COST_CTR",
                "naturalAccount": "GL_ACCOUNT",
            },
        })
        assert spec.segments_child_path == "/child/segments"
        assert spec.qualifier_values["naturalAccount"] == "GL_ACCOUNT"

    def test_all_fields_optional(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import (
            CoaMetadataSpec,
        )

        spec = CoaMetadataSpec.model_validate({})
        assert spec.resource_path is None

    def test_unknown_field_still_forbidden(self) -> None:
        import pydantic
        import pytest as _pytest

        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import (
            CoaMetadataSpec,
        )

        with _pytest.raises(pydantic.ValidationError):
            CoaMetadataSpec.model_validate({"nonsense": 1})
