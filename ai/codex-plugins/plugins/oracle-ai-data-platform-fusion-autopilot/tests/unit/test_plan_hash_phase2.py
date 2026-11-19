"""Unit tests for the Phase 2 content-pack plan-hash extension.

The pre-existing v1 plan-hash (``hash_resolved_plan``) must remain
unchanged — Phase 2 adds new functions but doesn't modify the old API.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import yaml

from oracle_ai_data_platform_fusion_autopilot.orchestrator.plan_hash import (
    compute_content_pack_plan_hash,
    compute_output_schema_hash,
)
from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import NodeYaml
from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
    load_tenant_profile_from_string,
)


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
    - name: thing_name
      type: string
      nullable: true
      pii: low
dependsOn:
  bronze:
    - id: erp_thing
      role: primary
      watermark:
        column: _extract_ts
refresh:
  seed:
    strategy: replace
  incremental:
    strategy: merge
    naturalKey: [thing_id]
    watermark:
      source: erp_thing
      column: _extract_ts
"""


PROFILE_YAML = """
schemaVersion: 1
tenant: acme-prod
pinnedAt: 2026-06-01T00:00:00+00:00
bronzeSchemaFingerprint: "sha256:abc"
resolved:
  column: {}
  semantic: {}
"""


def _node(yaml_text: str = NODE_YAML) -> NodeYaml:
    return NodeYaml.model_validate(yaml.safe_load(yaml_text))


def _pack(pack_id: str = "phase2-test", version: str = "1.0.0") -> MagicMock:
    """Stand-in ResolvedPack — the hash function only reads pack.pack.id/version."""
    pack = MagicMock()
    pack.pack.id = pack_id
    pack.pack.version = version
    return pack


def _profile():
    return load_tenant_profile_from_string(PROFILE_YAML)


# ---------------------------------------------------------------------------
# compute_output_schema_hash
# ---------------------------------------------------------------------------


class TestComputeOutputSchemaHash:
    def test_deterministic_across_runs(self) -> None:
        node = _node()
        assert compute_output_schema_hash(node) == compute_output_schema_hash(node)

    def test_adding_column_shifts_hash(self) -> None:
        a = compute_output_schema_hash(_node())
        # Add a column to the YAML.
        modified = NODE_YAML.replace(
            "    - name: thing_name",
            "    - name: extra\n      type: int\n      nullable: true\n      pii: none\n    - name: thing_name",
        )
        b = compute_output_schema_hash(_node(modified))
        assert a != b

    def test_changing_type_shifts_hash(self) -> None:
        a = compute_output_schema_hash(_node())
        b_yaml = NODE_YAML.replace(
            "    - name: thing_id\n      type: string",
            "    - name: thing_id\n      type: bigint",
        )
        b = compute_output_schema_hash(_node(b_yaml))
        assert a != b

    def test_flipping_nullable_shifts_hash(self) -> None:
        a = compute_output_schema_hash(_node())
        b_yaml = NODE_YAML.replace(
            "    - name: thing_name\n      type: string\n      nullable: true",
            "    - name: thing_name\n      type: string\n      nullable: false",
        )
        b = compute_output_schema_hash(_node(b_yaml))
        assert a != b

    def test_changing_pii_shifts_hash(self) -> None:
        a = compute_output_schema_hash(_node())
        b_yaml = NODE_YAML.replace(
            "    - name: thing_name\n      type: string\n      nullable: true\n      pii: low",
            "    - name: thing_name\n      type: string\n      nullable: true\n      pii: high",
        )
        b = compute_output_schema_hash(_node(b_yaml))
        assert a != b

    def test_column_reorder_shifts_hash(self) -> None:
        """Declared column order is significant (downstream consumers
        rely on it). Reordering MUST shift the hash."""
        a = compute_output_schema_hash(_node())
        # Swap thing_id and thing_name positions.
        reordered = """
id: dim_thing
layer: silver
implementation:
  type: sql
  sql: silver/dim_thing.sql
target: dim_thing
outputSchema:
  columns:
    - name: thing_name
      type: string
      nullable: true
      pii: low
    - name: thing_id
      type: string
      nullable: false
      pii: none
dependsOn:
  bronze:
    - id: erp_thing
      role: primary
      watermark:
        column: _extract_ts
refresh:
  seed:
    strategy: replace
"""
        b = compute_output_schema_hash(_node(reordered))
        assert a != b


# ---------------------------------------------------------------------------
# compute_content_pack_plan_hash
# ---------------------------------------------------------------------------


class TestComputeContentPackPlanHash:
    def _baseline(self):
        return compute_content_pack_plan_hash(
            pack=_pack(),
            node=_node(),
            profile=_profile(),
            rendered_sql_hash="sql-hash-1",
            output_schema_hash="schema-hash-1",
            profile_hash="profile-hash-1",
        )

    def test_deterministic_across_runs(self) -> None:
        assert self._baseline() == self._baseline()

    def test_different_pack_version_shifts_hash(self) -> None:
        a = self._baseline()
        b = compute_content_pack_plan_hash(
            pack=_pack(version="1.1.0"),
            node=_node(),
            profile=_profile(),
            rendered_sql_hash="sql-hash-1",
            output_schema_hash="schema-hash-1",
            profile_hash="profile-hash-1",
        )
        assert a != b

    def test_different_rendered_sql_hash_shifts(self) -> None:
        a = self._baseline()
        b = compute_content_pack_plan_hash(
            pack=_pack(),
            node=_node(),
            profile=_profile(),
            rendered_sql_hash="sql-hash-DIFFERENT",
            output_schema_hash="schema-hash-1",
            profile_hash="profile-hash-1",
        )
        assert a != b

    def test_different_output_schema_hash_shifts(self) -> None:
        a = self._baseline()
        b = compute_content_pack_plan_hash(
            pack=_pack(),
            node=_node(),
            profile=_profile(),
            rendered_sql_hash="sql-hash-1",
            output_schema_hash="schema-hash-DIFFERENT",
            profile_hash="profile-hash-1",
        )
        assert a != b

    def test_different_profile_hash_shifts(self) -> None:
        a = self._baseline()
        b = compute_content_pack_plan_hash(
            pack=_pack(),
            node=_node(),
            profile=_profile(),
            rendered_sql_hash="sql-hash-1",
            output_schema_hash="schema-hash-1",
            profile_hash="profile-hash-DIFFERENT",
        )
        assert a != b

    def test_different_bronze_schema_fingerprint_shifts(self) -> None:
        a = self._baseline()
        profile_b = load_tenant_profile_from_string(
            PROFILE_YAML.replace("sha256:abc", "sha256:DIFFERENT")
        )
        b = compute_content_pack_plan_hash(
            pack=_pack(),
            node=_node(),
            profile=profile_b,
            rendered_sql_hash="sql-hash-1",
            output_schema_hash="schema-hash-1",
            profile_hash="profile-hash-1",
        )
        assert a != b

    def test_changing_natural_key_shifts_hash(self) -> None:
        a = self._baseline()
        b_yaml = NODE_YAML.replace(
            "naturalKey: [thing_id]", "naturalKey: [thing_id, thing_name]"
        )
        b = compute_content_pack_plan_hash(
            pack=_pack(),
            node=_node(b_yaml),
            profile=_profile(),
            rendered_sql_hash="sql-hash-1",
            output_schema_hash="schema-hash-1",
            profile_hash="profile-hash-1",
        )
        assert a != b

    def test_changing_watermark_column_shifts_hash(self) -> None:
        """P-incr-L1 lossless guard: the watermark COLUMN is mixed into the
        plan-hash via a dedicated field, so changing it shifts the hash EVEN
        THOUGH Approach-3 mode-normalization strips the watermark predicate
        TEXT (``col > :wm`` → ``1=1``) from ``rendered_sql_hash``. This is the
        invariant that makes normalizing the predicate text away lossless —
        without this field a watermark-column edit could go undetected.

        We hold ``rendered_sql_hash`` constant (simulating the normalized hash
        that no longer reflects the column) and prove the plan-hash still moves
        purely from the dedicated ``watermark_column`` field."""
        a = self._baseline()
        b_yaml = NODE_YAML.replace(
            "      source: erp_thing\n      column: _extract_ts",
            "      source: erp_thing\n      column: _load_ts",
        )
        b = compute_content_pack_plan_hash(
            pack=_pack(),
            node=_node(b_yaml),
            profile=_profile(),
            rendered_sql_hash="sql-hash-1",  # held constant on purpose
            output_schema_hash="schema-hash-1",
            profile_hash="profile-hash-1",
        )
        assert a != b


# Phase 9 follow-up: the ``TestV1PlanHashUnchanged`` class was deleted
# along with the v1 plan-hash entrypoints (``hash_resolved_plan``,
# ``serialize_plan_snapshot``, ``build_current_diagnostics``). The
# content-pack plan-hash (``compute_content_pack_plan_hash``) is the only
# surviving plan-hash surface; its stability properties are covered by
# the ``TestContentPackPlanHash*`` classes above.
