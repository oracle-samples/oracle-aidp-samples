"""Round-trip tests for ``stage_pack_files`` + ``materialize_staged_pack`` (Step 12c)."""

from __future__ import annotations

import pathlib
from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
    load_full_chain,
    load_pack,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_staging import (
    AIDPF_1039_PATH_TRAVERSAL,
    ContentPackPathTraversalError,
    materialize_staged_pack,
    stage_pack_files,
)


PACK_YAML = """
id: phase2-staging-test
version: 1.0.0
description: Phase 2 staging round-trip test pack
compatibility:
  pluginMinVersion: 0.3.0
"""

NODE_YAML = """
id: dim_thing
layer: silver
implementation:
  type: sql
  sql: silver/dim_thing.sql
target: dim_thing
outputSchema:
  columns:
    - name: thing_id
      type: string
      nullable: false
      pii: none
dependsOn:
  bronze:
    - id: erp_thing
      role: primary
refresh:
  seed:
    strategy: replace
"""

NODE_YAML_NON_GLOB_SQL = NODE_YAML.replace(
    "  sql: silver/dim_thing.sql", "  sql: sql/shared/dim_thing.sql"
)


def _build_pack(root: Path, *, sql_path: str = "silver/dim_thing.sql", node_yaml: str = NODE_YAML, sql_content: str = "SELECT 1 AS thing_id"):
    root.mkdir(parents=True, exist_ok=True)
    (root / "pack.yaml").write_text(PACK_YAML, encoding="utf-8")
    silver = root / "silver"
    silver.mkdir()
    (silver / "dim_thing.yaml").write_text(node_yaml, encoding="utf-8")
    # Write the SQL at the declared relative path.
    sql_full = root / sql_path
    sql_full.parent.mkdir(parents=True, exist_ok=True)
    sql_full.write_text(sql_content, encoding="utf-8")


# ---------------------------------------------------------------------------
# Basic round trip — single-layer pack
# ---------------------------------------------------------------------------


class TestBasicRoundTrip:
    def test_stage_then_materialize_then_load_yields_equivalent_pack(self, tmp_path):
        pack_root = tmp_path / "src" / "phase2-staging-test"
        _build_pack(pack_root)
        original = load_pack(pack_root)

        files, manifest = stage_pack_files(original)

        # __layer_0__/pack.yaml + __layer_0__/silver/dim_thing.yaml + SQL
        assert "__layer_0__/pack.yaml" in files
        assert "__layer_0__/silver/dim_thing.yaml" in files
        assert "__layer_0__/silver/dim_thing.sql" in files

        # Manifest shape.
        assert manifest["entry_layer_index"] == 0
        assert len(manifest["chain_layers"]) == 1
        assert manifest["chain_layers"][0]["pack_id"] == "phase2-staging-test"

        top_root, base_resolver = materialize_staged_pack(files, manifest)
        rebuilt = load_full_chain(top_root, base_resolver=base_resolver)

        assert rebuilt.pack.id == original.pack.id
        assert set(rebuilt.silver) == set(original.silver)


# ---------------------------------------------------------------------------
# Non-glob SQL path — staging must read from declared path
# ---------------------------------------------------------------------------


class TestNonGlobSqlPath:
    def test_sql_in_non_standard_location_gets_staged(self, tmp_path):
        """A pack that declares ``implementation.sql: sql/shared/foo.sql``
        (not ``silver/<id>.sql``) MUST still have its SQL file staged.
        Globbing only ``silver/*.sql`` would silently omit such files;
        the staging walks the merged ResolvedPack's nodes."""
        pack_root = tmp_path / "src" / "phase2-staging-test"
        marker = "-- NON_GLOB_MARKER"
        _build_pack(
            pack_root,
            sql_path="sql/shared/dim_thing.sql",
            node_yaml=NODE_YAML_NON_GLOB_SQL,
            sql_content=f"{marker}\nSELECT 1 AS thing_id",
        )
        original = load_pack(pack_root)
        files, manifest = stage_pack_files(original)
        # Key has the layer prefix AND the declared relative path.
        assert "__layer_0__/sql/shared/dim_thing.sql" in files
        assert marker in files["__layer_0__/sql/shared/dim_thing.sql"]

    def test_non_glob_round_trip_yields_same_sql_content(self, tmp_path):
        pack_root = tmp_path / "src" / "phase2-staging-test"
        marker = "-- NON_GLOB_MARKER"
        _build_pack(
            pack_root,
            sql_path="sql/shared/dim_thing.sql",
            node_yaml=NODE_YAML_NON_GLOB_SQL,
            sql_content=f"{marker}\nSELECT 1 AS thing_id",
        )
        original = load_pack(pack_root)
        files, manifest = stage_pack_files(original)
        top_root, base_resolver = materialize_staged_pack(files, manifest)
        rebuilt = load_full_chain(top_root, base_resolver=base_resolver)
        # The rebuilt pack's SQL path is still the declared one.
        node = rebuilt.silver["dim_thing"]
        assert node.implementation.sql == "sql/shared/dim_thing.sql"
        # And the SQL is reachable.
        sql_path = rebuilt.root_for("silver/dim_thing") / node.implementation.sql
        assert marker in sql_path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Path-traversal rejection — AIDPF-1039
# ---------------------------------------------------------------------------


class TestPathTraversalRejection:
    def test_double_dot_in_sql_path_rejected(self, tmp_path):
        pack_root = tmp_path / "src" / "phase2-staging-test"
        evil_node_yaml = NODE_YAML.replace(
            "  sql: silver/dim_thing.sql",
            "  sql: ../../etc/passwd",
        )
        # Build the pack — Pydantic allows the path; staging rejects it.
        _build_pack(pack_root, sql_path="silver/dim_thing.sql", node_yaml=evil_node_yaml)
        original = load_pack(pack_root)
        with pytest.raises(ContentPackPathTraversalError) as exc_info:
            stage_pack_files(original)
        assert AIDPF_1039_PATH_TRAVERSAL in str(exc_info.value)

    def test_absolute_sql_path_rejected(self, tmp_path):
        pack_root = tmp_path / "src" / "phase2-staging-test"
        evil_node_yaml = NODE_YAML.replace(
            "  sql: silver/dim_thing.sql",
            "  sql: /etc/passwd",
        )
        _build_pack(pack_root, sql_path="silver/dim_thing.sql", node_yaml=evil_node_yaml)
        original = load_pack(pack_root)
        with pytest.raises(ContentPackPathTraversalError):
            stage_pack_files(original)
