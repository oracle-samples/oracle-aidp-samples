"""Integration test for the additive-COA per-node prior-equivalent proof.

Feature: incremental-coa-chart-onboarding. Exercises the REAL crux end-to-end
without Spark/Delta/Fusion: a real content-pack silver SQL node is rendered under
the prior vs. incoming chart-of-accounts via the actual `render_node_sql` +
`compute_content_pack_plan_hash`, driven through `CoaIncrementalContext`'s
per-node acceptance exactly as `execute_node` (sql path) wires it.

Covers:
  * additive new chart → accepted (recompute-under-prior-COA reproduces the
    stored prior plan-hash);
  * mutating change → blocked (classifier);
  * additive COA + a SQL edit riding along → blocked (per-node proof: recompute
    diverges from the stored prior hash).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator import plan_hash as ph
from oracle_ai_data_platform_fusion_autopilot.orchestrator.coa_change import (
    coa_projection_of,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.coa_incremental import (
    CoaIncrementalContext,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_renderer import (
    RunContext,
    compute_rendered_sql_hash,
    render_node_sql,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
    _profile_with_prior_coa,
)
from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import (
    NodeYaml,
    PackYaml,
    ResolvedPack,
)
from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
    TenantProfile,
    compute_profile_hash,
)


_SILVER_SQL = (
    "SELECT CodeCombinationChartOfAccountsId AS chart_of_accounts_id, "
    "{{ coa.natural_account }} AS natural_account "
    "FROM {{ catalog }}.{{ bronze_schema }}.gl_coa gc\n"
)
# A SQL edit that rides along (adds a non-COA projected column) — must block.
_SILVER_SQL_EDITED = (
    "SELECT CodeCombinationChartOfAccountsId AS chart_of_accounts_id, "
    "{{ coa.natural_account }} AS natural_account, "
    "CodeCombinationEnabledFlag AS enabled_flag "
    "FROM {{ catalog }}.{{ bronze_schema }}.gl_coa gc\n"
)


def _coa(by_chart: dict[str, str]) -> dict:
    """Build a chartOfAccounts dict (nested default + byChart) for the given
    ``{chart_id: naturalAccountSegment}`` map. All three role segments are set
    (the multi-COA renderer computes the default column even though the SQL only
    reads natural_account)."""
    def _block(na: str) -> dict:
        return {
            "balancingSegment": "SEGMENT1",
            "costCenterSegment": "SEGMENT2",
            "naturalAccountSegment": na,
        }

    return {
        "default": _block("SEGMENT3"),
        "byChart": {cid: _block(na) for cid, na in by_chart.items()},
    }


def _profile(coa: dict) -> TenantProfile:
    return TenantProfile.model_validate(
        {
            "schemaVersion": 1,
            "tenant": "acme",
            "pinnedAt": "2026-01-01T00:00:00+00:00",
            "bronzeSchemaFingerprint": "sha256:fixture",
            "profile": {"chartOfAccounts": coa},
        }
    )


@pytest.fixture
def pack_and_node(tmp_path: Path):
    root = tmp_path / "pack"
    (root / "silver").mkdir(parents=True)
    (root / "pack.yaml").write_text("id: p\nversion: 1.0.0\n", encoding="utf-8")
    sql_file = root / "silver" / "dim_thing.sql"
    sql_file.write_text(_SILVER_SQL, encoding="utf-8")

    pack = PackYaml.model_validate(
        {"id": "p", "version": "1.0.0", "description": "d",
         "compatibility": {"pluginMinVersion": "0.3.0"}}
    )
    node = NodeYaml.model_validate({
        "id": "dim_thing", "layer": "silver",
        "implementation": {"type": "sql", "sql": "silver/dim_thing.sql"},
        "target": "dim_thing",
        "dependsOn": {"bronze": [{"id": "gl_coa"}]},
        "refresh": {"seed": {"strategy": "replace"},
                    "incremental": {"strategy": "merge",
                                    "watermark": {"source": "gl_coa", "column": "_extract_ts"},
                                    "naturalKey": ["chart_of_accounts_id"]}},
        "requiredColumns": {"gl_coa": ["CodeCombinationChartOfAccountsId",
                                       "$coa.natural_account"]},
        "outputSchema": {"columns": [
            {"name": "chart_of_accounts_id", "type": "string", "nullable": True, "pii": "none"},
            {"name": "natural_account", "type": "string", "nullable": True, "pii": "none"},
        ]},
    })
    resolved = ResolvedPack(root=root, pack=pack, silver={node.id: node})
    return resolved, node, sql_file


def _ctx() -> RunContext:
    return RunContext(
        catalog="cat", bronze_schema="bronze", silver_schema="silver",
        gold_schema="gold", run_id="RUN", active_profile_name="finance-default",
    )


def _plan_hash(pack, node, profile, ctx) -> str:
    rendered = render_node_sql(node, pack, profile, ctx)
    return ph.compute_content_pack_plan_hash(
        pack=pack, node=node, profile=profile,
        rendered_sql_hash=compute_rendered_sql_hash(rendered),
        output_schema_hash=ph.compute_output_schema_hash(node),
        profile_hash=compute_profile_hash(profile),
    )


def _make_ctx(prior_profile, incoming_profile, protected=("101",), checkpoint=True):
    return CoaIncrementalContext(
        active=True,
        incoming_coa=coa_projection_of(incoming_profile),
        protected_charts=frozenset(protected) if protected is not None else None,
        coa_source_ids=frozenset(),  # dim_thing is a downstream consumer
        coa_checkpoint_passed=checkpoint,
        manifest_by_run_id=lambda rid: {
            "coa_projection": coa_projection_of(prior_profile),
            "profile_hash": compute_profile_hash(prior_profile),
        } if rid == "R1" else None,
    )


def _sql_recompute(pack, node, incoming_profile, ctx):
    """Mirror execute_node's sql-path recompute closure exactly."""
    def _recompute(prior_coa, prior_profile_hash):
        prior_profile = _profile_with_prior_coa(incoming_profile, prior_coa)
        prior_rendered = render_node_sql(node, pack, prior_profile, ctx)
        return ph.compute_content_pack_plan_hash(
            pack=pack, node=node, profile=incoming_profile,
            rendered_sql_hash=compute_rendered_sql_hash(prior_rendered),
            output_schema_hash=ph.compute_output_schema_hash(node),
            profile_hash=prior_profile_hash,
        )
    return _recompute


class TestAdditiveProofIntegration:
    def test_additive_new_chart_accepted(self, pack_and_node) -> None:
        pack, node, _sql = pack_and_node
        ctx = _ctx()
        prior = _profile(_coa({"101": "SEGMENT3"}))
        incoming = _profile(_coa({"101": "SEGMENT3", "202": "SEGMENT4"}))  # additive

        stored_prior_hash = _plan_hash(pack, node, prior, ctx)
        # Sanity: the incoming profile really does drift the plan-hash.
        assert _plan_hash(pack, node, incoming, ctx) != stored_prior_hash

        cctx = _make_ctx(prior, incoming)
        reason = cctx.coa_accept_reason(
            node=node, prior_run_id="R1", stored_prior_hash=stored_prior_hash,
            recompute_hash=_sql_recompute(pack, node, incoming, ctx),
        )
        assert reason is not None and "additive" in reason

    def test_mutating_existing_chart_blocked(self, pack_and_node) -> None:
        pack, node, _sql = pack_and_node
        ctx = _ctx()
        prior = _profile(_coa({"101": "SEGMENT3"}))
        incoming = _profile(_coa({"101": "SEGMENT4"}))  # existing chart moved

        stored_prior_hash = _plan_hash(pack, node, prior, ctx)
        cctx = _make_ctx(prior, incoming)
        assert cctx.coa_accept_reason(
            node=node, prior_run_id="R1", stored_prior_hash=stored_prior_hash,
            recompute_hash=_sql_recompute(pack, node, incoming, ctx),
        ) is None

    def test_additive_plus_sql_edit_blocked(self, pack_and_node) -> None:
        """Additive COA change AND a SQL edit riding along → the per-node
        prior-equivalent proof diverges from the stored prior hash → blocked."""
        pack, node, sql_file = pack_and_node
        ctx = _ctx()
        prior = _profile(_coa({"101": "SEGMENT3"}))
        incoming = _profile(_coa({"101": "SEGMENT3", "202": "SEGMENT4"}))  # additive COA

        # Stored prior hash captured under the ORIGINAL SQL.
        stored_prior_hash = _plan_hash(pack, node, prior, ctx)
        # Now the SQL template is edited (a non-COA change rides along).
        sql_file.write_text(_SILVER_SQL_EDITED, encoding="utf-8")

        cctx = _make_ctx(prior, incoming)
        # classifier says additive, but recompute (current edited SQL under prior
        # COA) != stored prior hash → fail-closed block.
        assert cctx.coa_accept_reason(
            node=node, prior_run_id="R1", stored_prior_hash=stored_prior_hash,
            recompute_hash=_sql_recompute(pack, node, incoming, ctx),
        ) is None

    def test_unreadable_protected_charts_blocks(self, pack_and_node) -> None:
        pack, node, _sql = pack_and_node
        ctx = _ctx()
        prior = _profile(_coa({"101": "SEGMENT3"}))
        incoming = _profile(_coa({"101": "SEGMENT3", "202": "SEGMENT4"}))
        stored_prior_hash = _plan_hash(pack, node, prior, ctx)
        cctx = _make_ctx(prior, incoming, protected=None)  # fail-closed read
        assert cctx.coa_accept_reason(
            node=node, prior_run_id="R1", stored_prior_hash=stored_prior_hash,
            recompute_hash=_sql_recompute(pack, node, incoming, ctx),
        ) is None
