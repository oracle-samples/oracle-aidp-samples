"""Unit tests for ``orchestrator/content_pack_plan_resolver.py`` (Step 12d)."""

from __future__ import annotations

import pathlib

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_pack
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_plan_resolver import (
    AIDPF_1034_UNKNOWN_DATASET_FILTER,
    UnknownDatasetFilterError,
    resolve_content_pack_plan,
)


PACK_YAML = """
id: phase2-resolver-test
version: 1.0.0
description: Plan resolver test pack
compatibility:
  pluginMinVersion: 0.3.0
"""

# Two silver nodes and one gold node; gold depends on dim_a.
SILVER_DIM_A_YAML = """
id: dim_a
layer: silver
implementation:
  type: sql
  sql: silver/dim_a.sql
target: dim_a
outputSchema:
  columns:
    - name: a_id
      type: string
      nullable: false
      pii: none
dependsOn:
  bronze:
    - id: erp_a
      role: primary
refresh:
  seed:
    strategy: replace
"""

SILVER_DIM_B_YAML = """
id: dim_b
layer: silver
implementation:
  type: sql
  sql: silver/dim_b.sql
target: dim_b
outputSchema:
  columns:
    - name: b_id
      type: string
      nullable: false
      pii: none
dependsOn:
  bronze:
    - id: erp_b
      role: primary
refresh:
  seed:
    strategy: replace
"""

GOLD_MART_YAML = """
id: mart_x
layer: gold
implementation:
  type: sql
  sql: gold/mart_x.sql
target: mart_x
outputSchema:
  columns:
    - name: id
      type: string
      nullable: false
      pii: none
dependsOn:
  silver:
    - id: dim_a
      role: primary
refresh:
  seed:
    strategy: replace
"""


@pytest.fixture
def pack(tmp_path: pathlib.Path):
    root = tmp_path / "pack"
    root.mkdir()
    (root / "pack.yaml").write_text(PACK_YAML)
    (root / "silver").mkdir()
    (root / "silver" / "dim_a.yaml").write_text(SILVER_DIM_A_YAML)
    (root / "silver" / "dim_a.sql").write_text("SELECT 1 AS a_id")
    (root / "silver" / "dim_b.yaml").write_text(SILVER_DIM_B_YAML)
    (root / "silver" / "dim_b.sql").write_text("SELECT 1 AS b_id")
    (root / "gold").mkdir()
    (root / "gold" / "mart_x.yaml").write_text(GOLD_MART_YAML)
    (root / "gold" / "mart_x.sql").write_text("SELECT 1 AS id")
    return load_pack(root)


def test_no_filter_returns_all_nodes_silver_then_gold(pack):
    plan = resolve_content_pack_plan(pack)
    ids = [n.id for n in plan]
    assert "dim_a" in ids and "dim_b" in ids and "mart_x" in ids
    # Silver before gold.
    silver_positions = [i for i, n in enumerate(plan) if n.layer == "silver"]
    gold_positions = [i for i, n in enumerate(plan) if n.layer == "gold"]
    assert max(silver_positions) < min(gold_positions)


def test_silver_dependency_ordered_before_dependent(pack):
    """mart_x depends on dim_a — dim_a must come first."""
    plan = resolve_content_pack_plan(pack)
    a_idx = next(i for i, n in enumerate(plan) if n.id == "dim_a")
    mart_idx = next(i for i, n in enumerate(plan) if n.id == "mart_x")
    assert a_idx < mart_idx


def test_layers_filter_restricts_to_silver(pack):
    plan = resolve_content_pack_plan(pack, layers=["silver"])
    ids = {n.id for n in plan}
    assert ids == {"dim_a", "dim_b"}


def test_layers_filter_restricts_to_gold(pack):
    # Phase 9: --layers filters declared roots only; D-1 still pulls
    # in transitive deps so the plan stays correct. mart_x depends on
    # dim_a, so dim_a appears via D-1.
    plan = resolve_content_pack_plan(pack, layers=["gold"])
    ids = {n.id for n in plan}
    assert ids == {"mart_x", "dim_a"}


def test_layers_filter_gold_strict_scope_excludes_silver(pack):
    # Strict-scope opts out of D-1: only the gold root (plus would
    # raise on missing deps). dim_a is a real dep of mart_x; expect
    # strict-scope to raise.
    from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_plan_resolver import (
        StrictScopeMissingDependencyError,
    )
    with pytest.raises(StrictScopeMissingDependencyError):
        resolve_content_pack_plan(pack, layers=["gold"], strict_scope=True)


def test_datasets_filter_restricts_to_named(pack):
    plan = resolve_content_pack_plan(pack, datasets=["dim_b"])
    ids = {n.id for n in plan}
    assert ids == {"dim_b"}


def test_unknown_dataset_filter_raises_1034(pack):
    with pytest.raises(UnknownDatasetFilterError) as exc_info:
        resolve_content_pack_plan(pack, datasets=["nonexistent_dim"])
    assert AIDPF_1034_UNKNOWN_DATASET_FILTER in str(exc_info.value)


def test_node_not_in_legacy_registry_still_resolves(pack):
    """Phase 2 acceptance test: a content-pack node that does NOT
    exist in v1's SILVER_DIMS / GOLD_MARTS registry still resolves
    correctly under the content-pack plan resolver. Proves the
    resolver drives the loop independently of the legacy registry."""
    # The fixture nodes (dim_a, dim_b, mart_x) are intentionally not in
    # the v1 registry — they're test-only. The resolver returns them
    # without consulting the registry. The fact that this test passes
    # proves the contract.
    plan = resolve_content_pack_plan(pack)
    assert len(plan) == 3


def test_empty_pack_returns_empty_plan(tmp_path: pathlib.Path):
    root = tmp_path / "empty"
    root.mkdir()
    (root / "pack.yaml").write_text(PACK_YAML)
    pack = load_pack(root)
    plan = resolve_content_pack_plan(pack)
    assert plan == []
