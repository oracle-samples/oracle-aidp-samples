"""Unit tests for the content-pack dry-run plan builder (Phase 5 Step 6).

Verifies :func:`_build_content_pack_dry_run_plan` walks the resolved
pack via the same plan resolver the runtime uses and returns
``PlanNode``-shaped entries for the CLI summary renderer.
"""

from __future__ import annotations

import pathlib

from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
    _build_content_pack_dry_run_plan,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_pack


PACK_YAML = """
id: phase5-dry-run-plan-test
version: 1.0.0
description: Phase 5 dry-run plan renderer test pack
compatibility:
  pluginMinVersion: 0.3.0
profiles:
  finance-default:
    chartOfAccounts:
      balancingSegment: segment1
      costCenterSegment: segment2
      naturalAccountSegment: segment3
"""

SILVER_NODE = """
id: dim_supplier
layer: silver
implementation:
  type: sql
  sql: silver/dim_supplier.sql
target: dim_supplier
dependsOn:
  bronze:
    - id: erp_suppliers
refresh:
  seed:
    strategy: replace
outputSchema:
  columns:
    - name: supplier_key
      type: bigint
      nullable: false
      pii: none
"""

GOLD_NODE = """
id: supplier_spend
layer: gold
implementation:
  type: sql
  sql: gold/supplier_spend.sql
target: supplier_spend
dependsOn:
  silver:
    - id: dim_supplier
refresh:
  seed:
    strategy: replace
outputSchema:
  columns:
    - name: supplier_key
      type: bigint
      nullable: false
      pii: none
"""


def _build_pack(tmp_path: pathlib.Path):
    pack_root = tmp_path / "pack"
    pack_root.mkdir(parents=True, exist_ok=True)
    (pack_root / "pack.yaml").write_text(PACK_YAML, encoding="utf-8")

    silver = pack_root / "silver"
    silver.mkdir()
    (silver / "dim_supplier.yaml").write_text(SILVER_NODE, encoding="utf-8")
    (silver / "dim_supplier.sql").write_text("SELECT 1\n", encoding="utf-8")

    gold = pack_root / "gold"
    gold.mkdir()
    (gold / "supplier_spend.yaml").write_text(GOLD_NODE, encoding="utf-8")
    (gold / "supplier_spend.sql").write_text("SELECT 1\n", encoding="utf-8")

    return load_pack(pack_root)


class TestContentPackDryRunPlan:
    def test_no_filter_returns_both_nodes(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        plan = _build_content_pack_dry_run_plan(
            resolved_pack=pack, datasets=None, layers=None,
        )
        # Silver before gold (topologically ordered).
        ids = [pn.dataset_id for pn in plan]
        assert ids == ["dim_supplier", "supplier_spend"]
        # Every plan node is eligible (no deferred under content-pack).
        assert all(pn.status == "eligible" for pn in plan)
        # Layer tags are present.
        layers = [pn.layer for pn in plan]
        assert layers == ["silver", "gold"]

    def test_layer_filter_silver_only(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        plan = _build_content_pack_dry_run_plan(
            resolved_pack=pack, datasets=None, layers=["silver"],
        )
        assert [pn.dataset_id for pn in plan] == ["dim_supplier"]

    def test_dataset_filter(self, tmp_path: pathlib.Path) -> None:
        # Phase 9 D-1: declaring supplier_spend (gold) auto-includes
        # its transitive silver dim_supplier dep. Topological order:
        # silver first, then gold.
        pack = _build_pack(tmp_path)
        plan = _build_content_pack_dry_run_plan(
            resolved_pack=pack, datasets=["supplier_spend"], layers=None,
        )
        assert [pn.dataset_id for pn in plan] == ["dim_supplier", "supplier_spend"]
