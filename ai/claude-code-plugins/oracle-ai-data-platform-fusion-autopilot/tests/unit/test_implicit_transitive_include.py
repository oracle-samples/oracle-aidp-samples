"""Phase 9 Step 4: D-1 implicit transitive include semantics.

Operator declares high-level intent (a silver/gold node id); the
resolver auto-includes the transitive bronze + silver dependencies.
``--strict-scope`` opts out of that auto-include.
"""

from __future__ import annotations

import pathlib

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_pack
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_plan_resolver import (
    AIDPF_1042_STRICT_SCOPE_MISSING_DEPENDENCY,
    AIDPF_1043_CLI_DATASET_OUTSIDE_BUNDLE_SCOPE,
    AIDPF_1045_LAYER_FILTER_EMPTIED_PLAN,
    CliDatasetOutsideBundleScopeError,
    LayerFilterEmptiedPlanError,
    StrictScopeMissingDependencyError,
    resolve_content_pack_plan,
)


PACK_YAML = """\
id: test-pack
version: 0.1.0
compatibility:
  pluginMinVersion: 0.1.0
"""


def _bronze_yaml(node_id: str) -> str:
    return f"""\
id: {node_id}
layer: bronze
implementation:
  type: bronze_extract
  datastore: {node_id.upper()}_PVO
  biccSchema: Financial
target: {node_id}
dependsOn:
  bronze: []
  silver: []
refresh:
  seed:
    strategy: replace
  incremental:
    strategy: merge
    watermark:
      source: {node_id}
      column: LAST_UPDATE_DATE
    naturalKey: [ID]
outputSchema:
  columns:
    - {{ name: ID, type: long, nullable: false, pii: none }}
    - {{ name: _extract_ts, type: timestamp, nullable: false, pii: none }}
    - {{ name: _source_pvo, type: string, nullable: false, pii: none }}
    - {{ name: _run_id, type: string, nullable: false, pii: none }}
    - {{ name: _watermark_used, type: timestamp, nullable: true, pii: none }}
"""


SILVER_DIM_SUPPLIER = """\
id: dim_supplier
layer: silver
implementation:
  type: sql
  sql: silver/dim_supplier.sql
target: dim_supplier
dependsOn:
  bronze:
    - id: erp_suppliers
      role: primary
      watermark: { column: _extract_ts }
  silver: []
refresh:
  seed:
    strategy: replace
  incremental:
    strategy: merge
    watermark:
      source: erp_suppliers
      column: _extract_ts
    naturalKey: [supplier_id]
requiredColumns:
  erp_suppliers: [ID]
outputSchema:
  columns:
    - { name: supplier_id, type: long, nullable: false, pii: none }
"""


GOLD_SUPPLIER_SPEND = """\
id: supplier_spend
layer: gold
implementation:
  type: sql
  sql: gold/supplier_spend.sql
target: supplier_spend
dependsOn:
  bronze:
    - id: ap_invoices
      role: primary
      watermark: { column: _extract_ts }
  silver:
    - id: dim_supplier
refresh:
  seed:
    strategy: replace
  incremental:
    strategy: merge
    watermark:
      source: ap_invoices
      column: _extract_ts
    naturalKey: [supplier_id, period]
requiredColumns:
  ap_invoices: [ID]
outputSchema:
  columns:
    - { name: supplier_id, type: long, nullable: false, pii: none }
    - { name: spend, type: "decimal(28,8)", nullable: false, pii: low }
"""


@pytest.fixture
def pack(tmp_path: pathlib.Path):
    root = tmp_path / "pack"
    root.mkdir()
    (root / "pack.yaml").write_text(PACK_YAML)
    (root / "bronze").mkdir()
    (root / "bronze" / "erp_suppliers.yaml").write_text(_bronze_yaml("erp_suppliers"))
    (root / "bronze" / "ap_invoices.yaml").write_text(_bronze_yaml("ap_invoices"))
    (root / "silver").mkdir()
    (root / "silver" / "dim_supplier.yaml").write_text(SILVER_DIM_SUPPLIER)
    (root / "silver" / "dim_supplier.sql").write_text("SELECT 1 AS supplier_id")
    (root / "gold").mkdir()
    (root / "gold" / "supplier_spend.yaml").write_text(GOLD_SUPPLIER_SPEND)
    (root / "gold" / "supplier_spend.sql").write_text(
        "SELECT 1 AS supplier_id, 1.0 AS spend"
    )
    return load_pack(root)


class TestImplicitTransitiveInclude:
    def test_gold_only_declaration_pulls_silver_and_bronze(self, pack):
        # Operator says "run supplier_spend"; resolver auto-includes
        # ap_invoices + erp_suppliers + dim_supplier (D-1).
        plan = resolve_content_pack_plan(pack, datasets=["supplier_spend"])
        ids = [n.id for n in plan]
        assert set(ids) == {
            "erp_suppliers", "ap_invoices", "dim_supplier", "supplier_spend"
        }
        # Topological order: bronze → silver → gold.
        layers = [n.layer for n in plan]
        bronze_idx = [i for i, l in enumerate(layers) if l == "bronze"]
        silver_idx = [i for i, l in enumerate(layers) if l == "silver"]
        gold_idx = [i for i, l in enumerate(layers) if l == "gold"]
        assert max(bronze_idx) < min(silver_idx)
        assert max(silver_idx) < min(gold_idx)

    def test_silver_only_declaration_pulls_bronze_dep(self, pack):
        plan = resolve_content_pack_plan(pack, datasets=["dim_supplier"])
        ids = {n.id for n in plan}
        assert ids == {"erp_suppliers", "dim_supplier"}

    def test_strict_scope_requires_explicit_deps(self, pack):
        with pytest.raises(StrictScopeMissingDependencyError) as exc:
            resolve_content_pack_plan(
                pack, datasets=["supplier_spend"], strict_scope=True
            )
        assert AIDPF_1042_STRICT_SCOPE_MISSING_DEPENDENCY in str(exc.value)

    def test_strict_scope_with_explicit_deps_passes(self, pack):
        plan = resolve_content_pack_plan(
            pack,
            datasets=[
                "supplier_spend", "dim_supplier", "ap_invoices", "erp_suppliers",
            ],
            strict_scope=True,
        )
        assert len(plan) == 4

    def test_layer_filter_does_not_clip_transitive_deps(self, pack):
        # --layers gold filters declared root; D-1 still pulls deps.
        plan = resolve_content_pack_plan(
            pack, datasets=["supplier_spend"], layers=["gold"]
        )
        ids = {n.id for n in plan}
        # All deps still pulled in regardless of layer filter.
        assert "supplier_spend" in ids
        assert "ap_invoices" in ids
        assert "erp_suppliers" in ids
        assert "dim_supplier" in ids

    def test_layer_filter_emptying_plan_raises_1045(self, pack):
        with pytest.raises(LayerFilterEmptiedPlanError) as exc:
            resolve_content_pack_plan(
                pack, datasets=["supplier_spend"], layers=["silver"]
            )
        assert AIDPF_1045_LAYER_FILTER_EMPTIED_PLAN in str(exc.value)

    def test_cli_dataset_outside_bundle_scope_raises_1043(self, pack):
        with pytest.raises(CliDatasetOutsideBundleScopeError) as exc:
            resolve_content_pack_plan(
                pack,
                datasets=["supplier_spend"],
                bundle_scope={"ap_invoices"},  # supplier_spend not in scope
            )
        assert AIDPF_1043_CLI_DATASET_OUTSIDE_BUNDLE_SCOPE in str(exc.value)

    def test_bundle_scope_as_implicit_roots(self, pack):
        # No --datasets; bundle_scope is the root set.
        plan = resolve_content_pack_plan(
            pack, bundle_scope={"supplier_spend"}
        )
        ids = {n.id for n in plan}
        assert "supplier_spend" in ids
        assert "ap_invoices" in ids
        assert "erp_suppliers" in ids
        assert "dim_supplier" in ids

    def test_bronze_only_run(self, pack):
        plan = resolve_content_pack_plan(pack, layers=["bronze"])
        ids = {n.id for n in plan}
        assert ids == {"erp_suppliers", "ap_invoices"}
