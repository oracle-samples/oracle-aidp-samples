"""COA-depth overlay tests (sub-plan D1/D2/D3).

Proves: the shipped `examples/coa-deep-overlay` extends the COA role domain to
Segment1-10 (candidates + gl_coa outputSchema together) and passes validation;
a candidate-only extension (no outputSchema extend) is rejected (AIDPF-2015); a
Segment31 candidate is rejected (AIDPF-2019).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
    load_full_chain,
    load_pack,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_validators import (
    AIDPF_2015_COA_BINDING_OUT_OF_CONTRACT,
    AIDPF_2019_COA_SEGMENT_OUT_OF_RANGE,
    validate_coa_semantic_roles,
)

REPO = Path(__file__).resolve().parents[2]
SHIPPED = (
    REPO / "scripts" / "oracle_ai_data_platform_fusion_autopilot"
    / "content_packs" / "fusion-finance-starter"
)
EXAMPLE_OVERLAY = REPO / "examples" / "coa-deep-overlay"


def _resolver(ref):
    # Map the base ref to the shipped pack root regardless of co-location.
    if ref.name == "fusion-finance-starter":
        return SHIPPED
    raise AssertionError(f"unexpected base ref {ref!r}")


def _codes(errs):
    return {e.code for e in errs}


def test_example_overlay_merges_domain_to_segment10() -> None:
    merged = load_full_chain(EXAMPLE_OVERLAY, base_resolver=_resolver)
    bal = merged.pack.column_aliases["coa_balancing_segment"]
    assert bal.resolution == "semanticRole" and bal.role == "coa.balancing"
    assert "CodeCombinationSegment10" in bal.candidates
    assert "CodeCombinationSegment1" in bal.candidates  # inherited base
    cols = {c.name for c in merged.bronze["gl_coa"].output_schema.columns}
    assert {"CodeCombinationSegment7", "CodeCombinationSegment10"} <= cols


def test_example_overlay_passes_coa_validation() -> None:
    merged = load_full_chain(EXAMPLE_OVERLAY, base_resolver=_resolver)
    errs = validate_coa_semantic_roles(merged)
    assert errs == [], [e.message for e in errs]


def test_candidate_extend_without_outputschema_extend_rejected() -> None:
    """A COA role allowed to bind Segment10 without the gl_coa contract also
    declaring it → AIDPF-2015 (depth gated by the contract, not a hardcoded 6)."""
    pack = load_pack(SHIPPED)  # base only: outputSchema is Segment1-6
    spec = pack.pack.column_aliases["coa_balancing_segment"]
    object.__setattr__(spec, "candidates", [*spec.candidates, "CodeCombinationSegment10"])
    errs = validate_coa_semantic_roles(pack)
    assert AIDPF_2015_COA_BINDING_OUT_OF_CONTRACT in _codes(errs)


def test_segment_out_of_range_rejected() -> None:
    pack = load_pack(SHIPPED)
    spec = pack.pack.column_aliases["coa_balancing_segment"]
    object.__setattr__(spec, "candidates", [*spec.candidates, "CodeCombinationSegment31"])
    errs = validate_coa_semantic_roles(pack)
    assert AIDPF_2019_COA_SEGMENT_OUT_OF_RANGE in _codes(errs)


def test_non_segment_candidate_rejected() -> None:
    pack = load_pack(SHIPPED)
    spec = pack.pack.column_aliases["coa_balancing_segment"]
    object.__setattr__(spec, "candidates", [*spec.candidates, "SomeOtherColumn"])
    errs = validate_coa_semantic_roles(pack)
    assert AIDPF_2019_COA_SEGMENT_OUT_OF_RANGE in _codes(errs)


# --- D5: deep-segment resolver derivation + preflight union -----------------


def test_bootstrap_derives_deep_segment_from_chartofaccounts() -> None:
    """bootstrap --refresh derives resolved.column.coa_* from a deep
    profile.chartOfAccounts (natural account at Segment10)."""
    from oracle_ai_data_platform_fusion_autopilot.commands.coa_resolution import (
        CoaResolutionInput,
        resolve_coa_roles,
    )

    res = resolve_coa_roles(
        CoaResolutionInput(
            semantic_role_aliases={
                "coa_balancing_segment": "coa.balancing",
                "coa_cost_center_segment": "coa.cost_center",
                "coa_natural_account_segment": "coa.natural_account",
            },
            explicit_config={
                "balancingSegment": "CodeCombinationSegment1",
                "costCenterSegment": "CodeCombinationSegment2",
                "naturalAccountSegment": "CodeCombinationSegment10",
            },
        )
    )
    assert res.column_map["coa_natural_account_segment"] == "CodeCombinationSegment10"
    assert res.role_provenance["natural_account"]["mechanism"] == "config_resolved"


def test_deep_segment_union_existence_blocks_when_unlanded() -> None:
    """A deep byChart arm (Segment10) absent from landed gl_coa blocks
    preflight via the $coa.* union (AIDPF-2042)."""
    from unittest.mock import MagicMock

    from oracle_ai_data_platform_fusion_autopilot.orchestrator.node_preflight import (
        preflight_node,
    )

    merged = load_full_chain(EXAMPLE_OVERLAY, base_resolver=_resolver)
    node = merged.silver["dim_account"]

    # gl_coa landed WITHOUT Segment10.
    landed = [
        "CodeCombinationCodeCombinationId",
        "CodeCombinationChartOfAccountsId",
        "CodeCombinationSegment1",
        "CodeCombinationSegment2",
        "CodeCombinationSegment3",
        "CodeCombinationAccountType",
        "CodeCombinationEnabledFlag",
        "_extract_ts",
        "_source_pvo",
    ]
    spark = MagicMock()

    def _sql(q: str):
        df = MagicMock()
        qq = " ".join(q.split())
        if qq.startswith("DESCRIBE TABLE"):
            df.collect.return_value = [(c, "string", None) for c in landed]
        elif "GROUP BY CAST(CodeCombinationChartOfAccountsId AS STRING)" in qq:
            df.collect.return_value = [("101", 15000)]
        else:
            df.collect.return_value = [(500, 0)]
        return df

    spark.sql.side_effect = _sql

    ctx = MagicMock()
    ctx.bronze_table_for_source = {"gl_coa": "cat.bronze.gl_coa"}
    profile = MagicMock()
    profile.resolved.column = {}
    profile.profile = {
        "chartOfAccounts": {
            "default": {
                "balancingSegment": "CodeCombinationSegment1",
                "costCenterSegment": "CodeCombinationSegment2",
                "naturalAccountSegment": "CodeCombinationSegment10",
            }
        }
    }
    report = preflight_node(spark, node, merged, profile, ctx)
    assert not report.ok
    assert any(e.code == "AIDPF-2042" for e in report.errors)


def test_coa_role_domains_base_and_overlay() -> None:
    """`coa_role_domains` returns the per-role candidate domain the runtime
    gate (checkpoint step 1b) and the metadata-arm verifier enforce: starter =
    Segment1-6; deep overlay extends every role to Segment10."""
    from oracle_ai_data_platform_fusion_autopilot.schema.coa_roles import (
        SUPPORTED_COA_ROLES,
        coa_role_domains,
    )

    base = coa_role_domains(load_pack(SHIPPED))
    assert set(base) == set(SUPPORTED_COA_ROLES)
    seg16 = {f"CodeCombinationSegment{i}" for i in range(1, 7)}
    assert base["coa.cost_center"] == frozenset(seg16)

    merged = coa_role_domains(
        load_full_chain(EXAMPLE_OVERLAY, base_resolver=_resolver)
    )
    seg110 = {f"CodeCombinationSegment{i}" for i in range(1, 11)}
    assert merged["coa.cost_center"] == frozenset(seg110)
    assert "CodeCombinationSegment9" in merged["coa.natural_account"]


def test_example_overlay_extends_dim_account_render_scope() -> None:
    """The overlay MUST also replace silver/dim_account (guarded replaceNode):
    candidates + outputSchema alone leave a landmine — a deep arm validates,
    then rendering fails with UNRESOLVED_COLUMN because the starter's inner
    projection stops at Segment6 (live-observed 2026-08-06, chart 41627 →
    Segment9). The reads-more SQL also requires the replacement YAML to
    declare Segment7-10 in requiredColumns (AIDPF-2084)."""
    merged = load_full_chain(EXAMPLE_OVERLAY, base_resolver=_resolver)
    node = merged.silver["dim_account"]
    assert node.implementation.sql == "silver/dim_account.sql"
    root = merged.root_for("silver/dim_account")
    assert root == EXAMPLE_OVERLAY
    sql = (root / node.implementation.sql).read_text()
    inner = sql.split("FROM (", 1)[1]
    for i in range(7, 11):
        assert f"coa.CodeCombinationSegment{i}" in inner, f"Segment{i} missing"
    # The output contract is untouched: no new segment_0N output aliases.
    assert "AS segment_07" not in sql
    # requiredColumns declare the deep reads (AIDPF-2084's demand).
    req = set(node.required_columns["gl_coa"])
    assert {f"CodeCombinationSegment{i}" for i in range(7, 11)} <= req


def test_example_overlay_passes_run_start_full_validation() -> None:
    """The RUN-START gate (AIDPF-1036 wraps `validate_pack_full`) must accept
    the shipped example — this is exactly the gate that rejected the first
    `sql:`-override draft live (AIDPF-5003 token-in-comment + AIDPF-2084
    undeclared deep reads)."""
    from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_validators import (
        validate_pack_full,
    )

    merged = load_full_chain(EXAMPLE_OVERLAY, base_resolver=_resolver)
    report = validate_pack_full(merged)
    assert not report.errors, [e.message for e in report.errors]


def test_template_with_comment_fails_hygiene_offline(tmp_path) -> None:
    """AIDPF-5010's design-time twin: a template containing `--` can never
    render (the renderer rejects comment markers in rendered SQL), so
    validation must fail OFFLINE — not cluster-side mid-run, which is where
    the first draft of this overlay failed live (2026-08-06)."""
    import shutil

    from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_validators import (
        AIDPF_5010_POST_RENDER_REJECTED,
        validate_sql_template_hygiene,
    )

    overlay = tmp_path / "coa-deep-overlay"
    shutil.copytree(EXAMPLE_OVERLAY, overlay)
    sql = overlay / "silver" / "dim_account.sql"
    sql.write_text("-- explanatory header\n" + sql.read_text())

    merged = load_full_chain(overlay, base_resolver=_resolver)
    errs = validate_sql_template_hygiene(merged)
    assert [e.code for e in errs] == [AIDPF_5010_POST_RENDER_REJECTED]
    assert "silver/dim_account" in errs[0].message
    # The shipped (comment-free) example stays clean.
    clean = load_full_chain(EXAMPLE_OVERLAY, base_resolver=_resolver)
    assert validate_sql_template_hygiene(clean) == []
