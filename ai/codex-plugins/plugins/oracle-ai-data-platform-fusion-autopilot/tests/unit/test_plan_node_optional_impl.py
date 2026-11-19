"""Phase 5 Step 6 — Content-pack dry-run plan nodes carry implementation
metadata so the renderer can show operators which backend each node
dispatches through (``sql`` / ``builtin`` / ``bronze_extract``).

The current ``PlanNode`` shape carries ``dataset_id``, ``layer``,
``status``, ``reason``. Phase 5's dry-run path produces PlanNodes
from content-pack ``NodeYaml`` objects; the discrimination
information (``implementation.type``) is recoverable via the pack
itself (``_resolve_node_from_pack(...)``). This test locks the
contract: every dry-run PlanNode is resolvable back to its
NodeYaml + implementation type via the helper.
"""

from __future__ import annotations

import pathlib

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
    _build_content_pack_dry_run_plan,
    _resolve_node_from_pack,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_pack


PACK_YAML = """
id: phase5-plan-node-test
version: 1.0.0
description: Phase 5 plan-node optional impl test pack
compatibility:
  pluginMinVersion: 0.3.0
profiles:
  finance-default:
    chartOfAccounts:
      balancingSegment: segment1
      costCenterSegment: segment2
      naturalAccountSegment: segment3
"""

SQL_NODE = """
id: dim_sql
layer: silver
implementation:
  type: sql
  sql: silver/dim_sql.sql
target: dim_sql
dependsOn:
  bronze: []
refresh:
  seed:
    strategy: replace
outputSchema:
  columns:
    - name: x
      type: bigint
      nullable: false
      pii: none
"""

BUILTIN_NODE = """
id: dim_calendar
layer: silver
implementation:
  type: builtin
  callable: oracle_ai_data_platform_fusion_autopilot.dimensions.dim_calendar:build
target: dim_calendar
dependsOn:
  bronze: []
refresh:
  seed:
    strategy: replace
outputSchema:
  columns:
    - name: calendar_key
      type: bigint
      nullable: false
      pii: none
"""


def _build_pack(tmp_path: pathlib.Path):
    pack_root = tmp_path / "pack"
    pack_root.mkdir(parents=True)
    (pack_root / "pack.yaml").write_text(PACK_YAML, encoding="utf-8")
    silver = pack_root / "silver"
    silver.mkdir()
    (silver / "dim_sql.yaml").write_text(SQL_NODE, encoding="utf-8")
    (silver / "dim_sql.sql").write_text("SELECT 1 AS x\n", encoding="utf-8")
    (silver / "dim_calendar.yaml").write_text(BUILTIN_NODE, encoding="utf-8")
    return load_pack(pack_root)


class TestPlanNodeOptionalImpl:
    def test_dry_run_plan_resolves_back_to_implementation_type(
        self, tmp_path,
    ) -> None:
        pack = _build_pack(tmp_path)
        plan = _build_content_pack_dry_run_plan(
            resolved_pack=pack, datasets=None, layers=None,
        )
        ids = [pn.dataset_id for pn in plan]
        assert set(ids) == {"dim_sql", "dim_calendar"}
        for pn in plan:
            node = _resolve_node_from_pack(pack, pn.layer, pn.dataset_id)
            assert node.implementation.type in {"sql", "builtin", "bronze_extract"}
            if pn.dataset_id == "dim_sql":
                assert node.implementation.type == "sql"
            elif pn.dataset_id == "dim_calendar":
                assert node.implementation.type == "builtin"
