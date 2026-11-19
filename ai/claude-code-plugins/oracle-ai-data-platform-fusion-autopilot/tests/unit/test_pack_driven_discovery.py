"""Unit tests for the pack-driven node-discovery surface (Phase 5 Step 2).

Covers :func:`_resolve_node_from_pack` across the ``sql`` / ``builtin``
``implementation.type`` branches plus error paths (unknown layer,
unknown node_id). The Phase 9 ``bronze_extract`` arm has its own
adapter tests.

The dispatch itself is owned by ``sql_runner.execute_node``
(:mod:`tests.unit.test_sql_runner_builtin_dispatch` covers the builtin
arm; :mod:`tests.unit.test_sql_runner` covers the sql arm). This test
only asserts that the discovery helper returns the correct
``NodeYaml`` so the dispatcher sees the right ``implementation.type``.
"""

from __future__ import annotations

import pathlib

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
    PackNodeNotFoundError,
    _resolve_node_from_pack,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_pack


PACK_YAML = """
id: phase5-discovery-test
version: 1.0.0
description: Phase 5 discovery test pack
compatibility:
  pluginMinVersion: 0.3.0
profiles:
  finance-default:
    chartOfAccounts:
      balancingSegment: segment1
      costCenterSegment: segment2
      naturalAccountSegment: segment3
"""

NODE_YAML_SQL = """
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

NODE_YAML_BUILTIN = """
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

NODE_YAML_GOLD_SQL = """
id: gold_mart
layer: gold
implementation:
  type: sql
  sql: gold/gold_mart.sql
target: gold_mart
dependsOn:
  bronze:
    - id: ap_invoices
refresh:
  seed:
    strategy: replace
outputSchema:
  columns:
    - name: amount
      type: double
      nullable: true
      pii: none
"""


def _build_pack(tmp_path: pathlib.Path):
    pack_root = tmp_path / "pack"
    pack_root.mkdir(parents=True, exist_ok=True)
    (pack_root / "pack.yaml").write_text(PACK_YAML, encoding="utf-8")

    silver = pack_root / "silver"
    silver.mkdir()
    (silver / "dim_supplier.yaml").write_text(NODE_YAML_SQL, encoding="utf-8")
    (silver / "dim_calendar.yaml").write_text(NODE_YAML_BUILTIN, encoding="utf-8")
    # SQL template file referenced by NODE_YAML_SQL — needs to exist for
    # the loader's pack validation (silver SQL nodes require the file).
    (silver / "dim_supplier.sql").write_text(
        "SELECT 1 AS supplier_key\n", encoding="utf-8"
    )

    gold = pack_root / "gold"
    gold.mkdir()
    (gold / "gold_mart.yaml").write_text(
        NODE_YAML_GOLD_SQL, encoding="utf-8"
    )
    (gold / "gold_mart.sql").write_text("SELECT 1 AS amount\n", encoding="utf-8")

    return load_pack(pack_root)


class TestResolveNodeFromPack:
    def test_silver_sql_node(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        node = _resolve_node_from_pack(pack, "silver", "dim_supplier")
        assert node.id == "dim_supplier"
        assert node.layer == "silver"
        assert node.implementation.type == "sql"

    def test_silver_builtin_node(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        node = _resolve_node_from_pack(pack, "silver", "dim_calendar")
        assert node.id == "dim_calendar"
        assert node.implementation.type == "builtin"
        assert node.implementation.callable.endswith(":build")

    def test_gold_sql_node(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        node = _resolve_node_from_pack(pack, "gold", "gold_mart")
        assert node.id == "gold_mart"
        assert node.layer == "gold"
        assert node.implementation.type == "sql"

    def test_unknown_layer_raises_valueerror(self, tmp_path: pathlib.Path) -> None:
        # Phase 9: bronze is a valid layer; only truly unknown layer
        # values raise.
        pack = _build_pack(tmp_path)
        with pytest.raises(ValueError) as exc:
            _resolve_node_from_pack(pack, "not_a_real_layer", "anything")
        assert "not in" in str(exc.value)

    def test_unknown_node_id_raises_pack_not_found(
        self, tmp_path: pathlib.Path
    ) -> None:
        pack = _build_pack(tmp_path)
        with pytest.raises(PackNodeNotFoundError) as exc:
            _resolve_node_from_pack(pack, "silver", "dim_nonexistent")
        # Message lists what IS available so operators see the typo.
        assert "dim_supplier" in str(exc.value)
        assert "dim_calendar" in str(exc.value)
        assert "dim_nonexistent" in str(exc.value)

    def test_unknown_gold_id_lists_gold_only(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        with pytest.raises(PackNodeNotFoundError) as exc:
            _resolve_node_from_pack(pack, "gold", "missing_mart")
        # Should list gold nodes only, not silver.
        assert "gold_mart" in str(exc.value)
        # Silver nodes MUST NOT appear in a gold lookup error message.
        assert "dim_supplier" not in str(exc.value)
