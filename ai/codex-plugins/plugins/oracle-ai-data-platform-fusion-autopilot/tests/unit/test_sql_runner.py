"""Unit tests for ``orchestrator/sql_runner.py``::``execute_node`` (Phase 2 Step 11).

The most important tests here lock the **render-then-gate ordering
invariant**: render runs exactly once BEFORE the plan-hash drift gate,
and a preflight/render/drift failure must never invoke Spark writes.
"""

from __future__ import annotations

import pathlib
from unittest.mock import MagicMock, patch

import pytest
import yaml

from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_pack
from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_renderer import (
    RunContext,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
    AIDPF_4040_PLAN_HASH_DRIFT,
    AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT,
    AIDPF_4071_BRONZE_SOURCE_COLUMN_MISSING,
    NodeExecutionResult,
    _assert_materialized_matches_declared,
    _bronze_source_schema_gate,
    execute_node,
)
from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
    load_tenant_profile_from_string,
)


# ---------------------------------------------------------------------------
# Fixture pack builder + helpers
# ---------------------------------------------------------------------------


PACK_YAML = """
id: phase2-runner-test
version: 1.0.0
description: Phase 2 sql_runner.execute_node test pack
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
      watermark:
        column: _extract_ts
requiredColumns:
  erp_thing:
    - SEGMENT1
    - _extract_ts
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
tenant: phase2-tenant
pinnedAt: 2026-06-01T00:00:00+00:00
bronzeSchemaFingerprint: "sha256:abc"
resolved:
  column: {}
  semantic: {}
profile: {}
"""

SIMPLE_SQL = "SELECT 1 AS thing_id"


def _build_pack(tmp_path: pathlib.Path, sql: str = SIMPLE_SQL):
    pack_root = tmp_path / "pack"
    pack_root.mkdir(parents=True, exist_ok=True)
    (pack_root / "pack.yaml").write_text(PACK_YAML, encoding="utf-8")
    silver = pack_root / "silver"
    silver.mkdir()
    (silver / "dim_thing.yaml").write_text(NODE_YAML, encoding="utf-8")
    (silver / "dim_thing.sql").write_text(sql, encoding="utf-8")
    return load_pack(pack_root)


def _ctx(mode: str = "seed") -> RunContext:
    return RunContext(
        catalog="cat",
        bronze_schema="bronze",
        silver_schema="silver",
        gold_schema="gold",
        run_id="run-phase2",
        active_profile_name="finance-default",
        prior_watermark={},
        mode=mode,
        bronze_table_for_source={"erp_thing": "cat.bronze.erp_thing"},
    )


def _profile():
    return load_tenant_profile_from_string(PROFILE_YAML)


def _paths() -> MagicMock:
    """MagicMock paths whose .bronze/.silver/.gold return string identifiers.

    The Phase 9 follow-up made ``paths`` REQUIRED at every
    ``_build_target_identifier`` call site (``sql_runner.py:290`` /
    ``:537`` / ``:910`` / ``:1078``), so test fixtures can no longer
    pass a bare ``MagicMock()`` — the helper would return a Mock object
    instead of a string, and downstream ``f"... FROM {target}"`` SQL
    composition would produce ``"... FROM <MagicMock name='...'>``.
    Use this helper to keep tests Spark-free while still emitting real
    identifier strings.
    """
    paths = MagicMock()
    paths.bronze.side_effect = lambda t: f"cat.bronze.{t}"
    paths.silver.side_effect = lambda t: f"cat.silver.{t}"
    paths.gold.side_effect = lambda t: f"cat.gold.{t}"
    return paths


def _fake_spark_seed_happy_path(target_row_count: int = 5) -> MagicMock:
    """Fake Spark that lets execute_node complete the full seed-mode
    happy path: preflight DESCRIBE returns required cols; CREATE OR
    REPLACE succeeds; quality DataFrame supports .count(); materialised
    schema DESCRIBE matches declared; max(watermark) returns NULL.
    """
    spark = MagicMock()

    # Default .sql(...) handler. Different statements need different
    # DataFrames; we route based on the SQL text.
    def sql_side_effect(stmt: str, *args, **kwargs):
        df = MagicMock(name="default-df")
        if "DESCRIBE TABLE cat.bronze.erp_thing" in stmt:
            # Preflight DESCRIBE returns the required columns.
            df.collect.return_value = [
                ("SEGMENT1", "string", None),
                ("_extract_ts", "timestamp", None),
            ]
            return df
        if stmt.startswith("CREATE OR REPLACE TABLE"):
            # Strategy executor — no return value needed.
            return df
        if "SELECT COUNT(*)" in stmt:
            df.collect.return_value = [(target_row_count,)]
            return df
        if "DESCRIBE TABLE" in stmt:
            # Materialised-schema assertion DESCRIBE — must match declared.
            df.collect.return_value = [
                ("thing_id", "string", None),
            ]
            return df
        if "SELECT MAX" in stmt:
            df.collect.return_value = [(None,)]
            return df
        df.collect.return_value = []
        return df

    spark.sql.side_effect = sql_side_effect

    # spark.table(target) -> target_df with .count()
    target_df = MagicMock(name="target_df")
    target_df.count.return_value = target_row_count
    spark.table.return_value = target_df

    # spark.createDataFrame for the state-row write.
    state_df = MagicMock(name="state_df")
    spark.createDataFrame.return_value = state_df

    return spark


# ---------------------------------------------------------------------------
# Happy path — seed mode end-to-end
# ---------------------------------------------------------------------------


class TestSeedHappyPath:
    def test_full_flow_returns_success(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        spark = _fake_spark_seed_happy_path()
        result = execute_node(
            spark,
            node=pack.silver["dim_thing"],
            pack=pack,
            profile=_profile(),
            ctx=_ctx("seed"),
            paths=_paths(),
            mode="seed",
            profile_hash="profile-h",
        )
        assert isinstance(result, NodeExecutionResult)
        assert result.status == "success", result.error_message
        assert result.row_count == 5
        assert result.plan_hash  # non-empty

    def test_success_uses_atomic_batch_state_write(self, tmp_path: pathlib.Path) -> None:
        """Exactly ONE createDataFrame call for the success state rows —
        Step 10's atomic batch contract."""
        pack = _build_pack(tmp_path)
        spark = _fake_spark_seed_happy_path()
        execute_node(
            spark,
            node=pack.silver["dim_thing"],
            pack=pack,
            profile=_profile(),
            ctx=_ctx("seed"),
            paths=_paths(),
            mode="seed",
            profile_hash="profile-h",
        )
        # Single createDataFrame call for the state-row batch.
        assert spark.createDataFrame.call_count == 1


# ---------------------------------------------------------------------------
# Render-then-gate ordering — preflight blocks BEFORE render
# ---------------------------------------------------------------------------


class TestRenderThenGateOrdering:
    """These tests lock the Step 11 ordering invariant. A preflight or
    render failure MUST happen before any Spark write occurs."""

    def test_preflight_blocked_does_not_call_renderer(self, tmp_path: pathlib.Path, monkeypatch) -> None:
        pack = _build_pack(tmp_path)
        # Preflight will fail: DESCRIBE returns a column set missing SEGMENT1.
        spark = MagicMock()

        def sql_side_effect(stmt: str, *args, **kwargs):
            df = MagicMock()
            if "DESCRIBE TABLE" in stmt:
                df.collect.return_value = [("_extract_ts", "timestamp", None)]
                # SEGMENT1 is missing -> preflight blocks.
                return df
            df.collect.return_value = []
            return df

        spark.sql.side_effect = sql_side_effect

        # Patch render_node_sql in sql_runner's namespace to detect calls.
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import sql_runner

        render_mock = MagicMock(side_effect=AssertionError("render must not be called"))
        monkeypatch.setattr(sql_runner, "render_node_sql", render_mock)

        result = execute_node(
            spark,
            node=pack.silver["dim_thing"],
            pack=pack,
            profile=_profile(),
            ctx=_ctx("seed"),
            paths=_paths(),
            mode="seed",
            profile_hash="profile-h",
        )
        assert result.status == "preflight_blocked"
        render_mock.assert_not_called()

    def test_render_failed_does_not_call_strategy_executor(self, tmp_path: pathlib.Path, monkeypatch) -> None:
        pack = _build_pack(tmp_path)
        spark = _fake_spark_seed_happy_path()

        # Patch render_node_sql to raise.
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import sql_runner
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_renderer import (
            SqlRendererError,
        )

        monkeypatch.setattr(
            sql_runner, "render_node_sql",
            MagicMock(side_effect=SqlRendererError("simulated render failure")),
        )

        strategy_mock = MagicMock()
        monkeypatch.setattr(sql_runner, "execute_strategy", strategy_mock)

        result = execute_node(
            spark,
            node=pack.silver["dim_thing"],
            pack=pack,
            profile=_profile(),
            ctx=_ctx("seed"),
            paths=_paths(),
            mode="seed",
            profile_hash="profile-h",
        )
        assert result.status == "render_failed"
        strategy_mock.assert_not_called()


# ---------------------------------------------------------------------------
# Plan-hash drift gate — incremental mode only
# ---------------------------------------------------------------------------


class TestPlanHashDriftGate:
    def test_incremental_with_matching_prior_hash_proceeds(
        self, tmp_path: pathlib.Path
    ) -> None:
        """When prior_plan_hash matches the freshly-computed expected
        hash, execution proceeds."""
        pack = _build_pack(tmp_path)
        spark = _fake_spark_seed_happy_path()
        # First run: compute the expected hash to use as prior_plan_hash.
        first = execute_node(
            spark,
            node=pack.silver["dim_thing"],
            pack=pack,
            profile=_profile(),
            ctx=_ctx("seed"),
            paths=_paths(),
            mode="seed",
            profile_hash="profile-h",
        )
        assert first.status == "success"

        # Second run with the SAME inputs in incremental mode + matching prior_hash.
        # We need a fresh fake spark because the first run consumed its side_effects.
        spark2 = _fake_spark_seed_happy_path()
        second = execute_node(
            spark2,
            node=pack.silver["dim_thing"],
            pack=pack,
            profile=_profile(),
            ctx=_ctx("incremental"),
            paths=_paths(),
            mode="incremental",
            profile_hash="profile-h",
            prior_plan_hash=first.plan_hash,
        )
        # Drift gate didn't block — proceeds to execution.
        assert second.status == "success"

    def test_incremental_with_mismatched_prior_hash_blocks_resume(
        self, tmp_path: pathlib.Path
    ) -> None:
        pack = _build_pack(tmp_path)
        spark = _fake_spark_seed_happy_path()

        result = execute_node(
            spark,
            node=pack.silver["dim_thing"],
            pack=pack,
            profile=_profile(),
            ctx=_ctx("incremental"),
            paths=_paths(),
            mode="incremental",
            profile_hash="profile-h",
            prior_plan_hash="some-stale-hash-from-previous-yaml-version",
        )
        assert result.status == "resume_drift_blocked"
        assert AIDPF_4040_PLAN_HASH_DRIFT in result.error_message

    def test_seed_mode_skips_drift_gate(self, tmp_path: pathlib.Path) -> None:
        """Seed mode has no prior state to compare against — drift gate
        is skipped regardless of prior_plan_hash."""
        pack = _build_pack(tmp_path)
        spark = _fake_spark_seed_happy_path()
        result = execute_node(
            spark,
            node=pack.silver["dim_thing"],
            pack=pack,
            profile=_profile(),
            ctx=_ctx("seed"),
            paths=_paths(),
            mode="seed",
            profile_hash="profile-h",
            prior_plan_hash="this-would-cause-drift-but-seed-skips",
        )
        assert result.status == "success"


class TestRepinPlanHashBreakGlass:
    """``--repin-plan-hash`` bypasses the AIDPF-4040 gate on a deliberate
    plan edit (P-incr-L1 escape hatch)."""

    def test_repin_false_still_blocks(self, tmp_path: pathlib.Path) -> None:
        """Default (repin_plan_hash=False) — a diverged hash still blocks."""
        pack = _build_pack(tmp_path)
        spark = _fake_spark_seed_happy_path()
        result = execute_node(
            spark,
            node=pack.silver["dim_thing"],
            pack=pack,
            profile=_profile(),
            ctx=_ctx("incremental"),
            paths=_paths(),
            mode="incremental",
            profile_hash="profile-h",
            prior_plan_hash="stale-hash-from-prior-yaml",
            repin_plan_hash=False,
        )
        assert result.status == "resume_drift_blocked"

    def test_repin_true_bypasses_gate_and_proceeds(self, tmp_path: pathlib.Path) -> None:
        """With the flag set, a diverged hash is repinned and execution
        proceeds to success instead of blocking."""
        pack = _build_pack(tmp_path)
        spark = _fake_spark_seed_happy_path()
        result = execute_node(
            spark,
            node=pack.silver["dim_thing"],
            pack=pack,
            profile=_profile(),
            ctx=_ctx("incremental"),
            paths=_paths(),
            mode="incremental",
            profile_hash="profile-h",
            prior_plan_hash="stale-hash-from-prior-yaml",
            repin_plan_hash=True,
        )
        assert result.status == "success"
        # The success row pins the NEW (freshly-computed) plan-hash.
        assert result.plan_hash and result.plan_hash != "stale-hash-from-prior-yaml"

    def test_repin_writes_audit_row(self, tmp_path: pathlib.Path) -> None:
        """The bypass writes a ``mode='plan_hash_repin'`` audit row to
        fusion_autopilot_state (the SOX trail)."""
        pack = _build_pack(tmp_path)
        spark = _fake_spark_seed_happy_path()
        execute_node(
            spark,
            node=pack.silver["dim_thing"],
            pack=pack,
            profile=_profile(),
            ctx=_ctx("incremental"),
            paths=_paths(),
            mode="incremental",
            profile_hash="profile-h",
            prior_plan_hash="stale-hash-from-prior-yaml",
            repin_plan_hash=True,
        )
        repin_inserts = [
            call.args[0] for call in spark.sql.call_args_list
            if call.args and "INSERT INTO" in call.args[0]
            and "'plan_hash_repin'" in call.args[0]
        ]
        assert len(repin_inserts) == 1, "expected exactly one plan_hash_repin audit row"

    def test_repin_does_not_fire_when_hashes_match(self, tmp_path: pathlib.Path) -> None:
        """If the prior hash already matches (no drift), repin is a no-op —
        no audit row, normal success."""
        pack = _build_pack(tmp_path)
        spark = _fake_spark_seed_happy_path()
        first = execute_node(
            spark, node=pack.silver["dim_thing"], pack=pack, profile=_profile(),
            ctx=_ctx("seed"), paths=_paths(), mode="seed", profile_hash="profile-h",
        )
        spark2 = _fake_spark_seed_happy_path()
        execute_node(
            spark2, node=pack.silver["dim_thing"], pack=pack, profile=_profile(),
            ctx=_ctx("incremental"), paths=_paths(), mode="incremental",
            profile_hash="profile-h", prior_plan_hash=first.plan_hash,
            repin_plan_hash=True,
        )
        repin_inserts = [
            call.args[0] for call in spark2.sql.call_args_list
            if call.args and "'plan_hash_repin'" in call.args[0]
        ]
        assert repin_inserts == []


# ---------------------------------------------------------------------------
# Materialised-schema assertion (Step 11 sub-step 8)
# ---------------------------------------------------------------------------


class TestMaterialisedSchemaAssertion:
    def test_matching_schema_returns_hash(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import NodeYaml

        node = NodeYaml.model_validate(yaml.safe_load(NODE_YAML))
        spark = MagicMock()
        df = MagicMock()
        df.collect.return_value = [("thing_id", "string", None)]
        spark.sql.return_value = df
        h = _assert_materialized_matches_declared(spark, "cat.silver.dim_thing", node)
        assert isinstance(h, str)
        assert len(h) == 64  # sha256 hex length

    def test_extra_materialised_column_raises_4070(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            MaterializedSchemaDriftError,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import NodeYaml

        node = NodeYaml.model_validate(yaml.safe_load(NODE_YAML))
        spark = MagicMock()
        df = MagicMock()
        df.collect.return_value = [
            ("thing_id", "string", None),
            ("extra_column", "string", None),
        ]
        spark.sql.return_value = df
        with pytest.raises(MaterializedSchemaDriftError) as exc_info:
            _assert_materialized_matches_declared(spark, "cat.silver.dim_thing", node)
        assert AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT in str(exc_info.value)

    def test_missing_target_raises_4070_not_raw_exception(self) -> None:
        """A missing target after execution (DESCRIBE TABLE throws
        TABLE_OR_VIEW_NOT_FOUND) must surface as MaterializedSchemaDriftError
        — a graceful per-node failure — not a raw AnalysisException that
        would abort the whole run."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            MaterializedSchemaDriftError,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import NodeYaml

        node = NodeYaml.model_validate(yaml.safe_load(NODE_YAML))
        spark = MagicMock()
        spark.sql.side_effect = RuntimeError(
            "[TABLE_OR_VIEW_NOT_FOUND] cat.bronze.x cannot be found"
        )
        with pytest.raises(MaterializedSchemaDriftError) as exc_info:
            _assert_materialized_matches_declared(spark, "cat.bronze.x", node)
        assert AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT in str(exc_info.value)
        assert "no table" in str(exc_info.value)

    def test_type_mismatch_raises_4070(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            MaterializedSchemaDriftError,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import NodeYaml

        node = NodeYaml.model_validate(yaml.safe_load(NODE_YAML))
        spark = MagicMock()
        df = MagicMock()
        df.collect.return_value = [("thing_id", "bigint", None)]  # declared string
        spark.sql.return_value = df
        with pytest.raises(MaterializedSchemaDriftError):
            _assert_materialized_matches_declared(spark, "cat.silver.dim_thing", node)

    # --- subset semantics (bronze_extract) — full raw PVO vs minimum contract ---

    def test_subset_allows_extra_materialised_columns(self) -> None:
        """Bronze writes the full raw PVO; extra columns beyond the
        declared minimum contract must NOT raise under subset=True."""
        from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import NodeYaml

        node = NodeYaml.model_validate(yaml.safe_load(NODE_YAML))  # declares thing_id:string
        spark = MagicMock()
        df = MagicMock()
        df.collect.return_value = [
            ("thing_id", "string", None),
            ("RawPvoColumnA", "string", None),
            ("RawPvoColumnB", "bigint", None),
        ]
        spark.sql.return_value = df
        h = _assert_materialized_matches_declared(
            spark, "cat.bronze.erp_thing", node, subset=True
        )
        assert isinstance(h, str) and len(h) == 64

    def test_subset_matches_case_insensitively(self) -> None:
        """Fusion bronze write lowercases columns; the pack declares
        Fusion-native PascalCase. Subset match must be case-insensitive
        (Spark/Delta semantics), consistent with preflight."""
        from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import NodeYaml

        # NODE_YAML declares thing_id:string — materialise it as THING_ID.
        node = NodeYaml.model_validate(yaml.safe_load(NODE_YAML))
        spark = MagicMock()
        df = MagicMock()
        df.collect.return_value = [
            ("THING_ID", "string", None),
            ("ExtraRawColumn", "string", None),
        ]
        spark.sql.return_value = df
        h = _assert_materialized_matches_declared(
            spark, "cat.bronze.erp_thing", node, subset=True
        )
        assert isinstance(h, str) and len(h) == 64

    def test_subset_missing_declared_column_raises_4070(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            MaterializedSchemaDriftError,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import NodeYaml

        node = NodeYaml.model_validate(yaml.safe_load(NODE_YAML))
        spark = MagicMock()
        df = MagicMock()
        df.collect.return_value = [("SomeOtherColumn", "string", None)]  # thing_id absent
        spark.sql.return_value = df
        with pytest.raises(MaterializedSchemaDriftError) as exc_info:
            _assert_materialized_matches_declared(
                spark, "cat.bronze.erp_thing", node, subset=True
            )
        assert AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT in str(exc_info.value)

    def test_subset_declared_column_type_mismatch_raises_4070(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            MaterializedSchemaDriftError,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import NodeYaml

        node = NodeYaml.model_validate(yaml.safe_load(NODE_YAML))
        spark = MagicMock()
        df = MagicMock()
        df.collect.return_value = [
            ("thing_id", "bigint", None),  # declared string
            ("ExtraRaw", "string", None),
        ]
        spark.sql.return_value = df
        with pytest.raises(MaterializedSchemaDriftError):
            _assert_materialized_matches_declared(
                spark, "cat.bronze.erp_thing", node, subset=True
            )


# ---------------------------------------------------------------------------
# State-commit failure — preserves prior watermark
# ---------------------------------------------------------------------------


class TestStateCommitFailure:
    def test_state_commit_failure_returns_state_commit_failed(self, tmp_path: pathlib.Path) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state_phase2

        pack = _build_pack(tmp_path)
        spark = _fake_spark_seed_happy_path()
        # Make state write fail.
        spark.createDataFrame.return_value.write.format.return_value.mode.return_value.option.return_value.saveAsTable.side_effect = RuntimeError(
            "simulated state-table write failure"
        )

        result = execute_node(
            spark,
            node=pack.silver["dim_thing"],
            pack=pack,
            profile=_profile(),
            ctx=_ctx("seed"),
            paths=_paths(),
            mode="seed",
            profile_hash="profile-h",
        )
        assert result.status == "state_commit_failed"
        assert "state_commit_failed" in result.error_message


# ---------------------------------------------------------------------------
# Pre-ingest source-schema gate (AIDPF-4071) — bronze Step 3
# ---------------------------------------------------------------------------

_BRONZE_NODE_YAML = """
id: test_bronze
layer: bronze
implementation:
  type: bronze_extract
  datastore: FscmTopModelAM.Test.TestPVO
  pvo_id: FscmTopModelAM.Test.TestPVO
  biccSchema: Financial
  schemaOverride: null
  incrementalCapable: true
  auditColumnsMode: bronze_v1
target: test_bronze
dependsOn:
  bronze: []
  silver: []
refresh:
  seed:
    strategy: replace
  incremental:
    strategy: merge
    watermark:
      source: test_bronze
      column: LASTUPDATEDATE
    naturalKey: [VENDORID]
requiredColumns:
  test_bronze:
    - VENDORID
outputSchema:
  columns:
    - { name: VENDORID, type: "decimal(38,30)", nullable: true, pii: low }
    - { name: SEGMENT1, type: string, nullable: true, pii: low }
    - { name: _extract_ts, type: timestamp, nullable: false, pii: none }
quality:
  tests: []
"""


# Declared types for the _BRONZE_NODE_YAML fixture's non-audit columns.
# A name-only probe (``_fake_struct([...])``) defaults each field to the
# fixture's declared type so the AIDPF-4070 type gate is satisfied and the
# presence-only (AIDPF-4071) tests stay green. Pass ``types={...}`` to force a
# mismatch and exercise the type gate.
_FIXTURE_DECLARED_TYPES = {
    "VENDORID": "decimal(38,30)",
    "SEGMENT1": "string",
    "LASTUPDATEDATE": "timestamp",
}


def _fake_struct(names, types=None):
    from types import SimpleNamespace
    types = types or {}

    def _field(n):
        t = (
            types.get(n)
            or types.get(n.lower())
            or _FIXTURE_DECLARED_TYPES.get(n.upper(), "string")
        )
        return SimpleNamespace(
            name=n, nullable=True,
            # bind ``t`` per-field (avoid the late-binding closure trap).
            dataType=SimpleNamespace(simpleString=(lambda tt: (lambda: tt))(t)),
        )

    return SimpleNamespace(fields=[_field(n) for n in names])


def _bronze_node():
    from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import NodeYaml
    return NodeYaml.model_validate(yaml.safe_load(_BRONZE_NODE_YAML))


def _gate_ctx():
    from types import SimpleNamespace
    # the gate reads ctx.bundle (passed to the — mocked — probe) and
    # ctx.run_id (stamped into the diagnostic).
    return SimpleNamespace(
        run_id="run-gate-test",
        bundle=SimpleNamespace(fusion=SimpleNamespace(password="x")),
    )


def _gate_profile():
    from types import SimpleNamespace
    return SimpleNamespace(tenant="test-tenant")


class TestBronzeSourceSchemaGate:
    """AIDPF-4071: presence-only, case-insensitive PVO probe before extract."""

    def _patch(self, monkeypatch, pvo_field_names):
        import oracle_ai_data_platform_fusion_autopilot.orchestrator.builtins.bronze_extract_adapter as bea
        import oracle_ai_data_platform_fusion_autopilot.orchestrator.runtime as rt
        from types import SimpleNamespace
        monkeypatch.setattr(rt, "_resolve_password",
                            lambda _v: SimpleNamespace(get_secret_value=lambda: "pw"))
        monkeypatch.setattr(bea, "probe_bronze_schemas",
                            lambda *a, **k: {"test_bronze": _fake_struct(pvo_field_names)})

    def test_all_wanted_present_returns_none(self, monkeypatch):
        self._patch(monkeypatch, ["VENDORID", "SEGMENT1", "EXTRA_RAW_COL"])
        assert _bronze_source_schema_gate(
            MagicMock(), node=_bronze_node(), pack=MagicMock(), profile=_gate_profile(), ctx=_gate_ctx()
        ) is None

    def test_case_insensitive_present_returns_none(self, monkeypatch):
        # PVO returns lowercase; pack declares uppercase — must still pass.
        self._patch(monkeypatch, ["vendorid", "segment1", "createddate"])
        assert _bronze_source_schema_gate(
            MagicMock(), node=_bronze_node(), pack=MagicMock(), profile=_gate_profile(), ctx=_gate_ctx()
        ) is None

    def test_missing_column_returns_4071(self, monkeypatch):
        self._patch(monkeypatch, ["vendorid"])  # SEGMENT1 absent
        result = _bronze_source_schema_gate(
            MagicMock(), node=_bronze_node(), pack=MagicMock(), profile=_gate_profile(), ctx=_gate_ctx()
        )
        assert result is not None
        msg, diag = result
        assert AIDPF_4071_BRONZE_SOURCE_COLUMN_MISSING in msg
        assert "SEGMENT1" in msg
        # structured diagnostic for medallion-author
        assert diag["errorCode"] == AIDPF_4071_BRONZE_SOURCE_COLUMN_MISSING
        assert diag["node"] == "test_bronze"
        assert diag["missingColumns"] == ["SEGMENT1"]
        assert {c["name"] for c in diag["pvoColumns"]} == {"vendorid"}
        # the structured payload must validate as the diagnostic model
        from oracle_ai_data_platform_fusion_autopilot.schema.diagnostic_artifact import (
            BronzeSourceColumnMissingV1,
        )
        BronzeSourceColumnMissingV1.model_validate(diag)

    def test_audit_columns_not_gated(self, monkeypatch):
        # _extract_ts is adapter-generated; its absence from the PVO must
        # NOT trip the gate.
        self._patch(monkeypatch, ["vendorid", "segment1"])
        assert _bronze_source_schema_gate(
            MagicMock(), node=_bronze_node(), pack=MagicMock(), profile=_gate_profile(), ctx=_gate_ctx()
        ) is None

    def test_overlay_added_required_column_absent_returns_4071(self, monkeypatch):
        # An overlay added a requiredColumns entry that is NOT in outputSchema
        # (TENANT_ONLY_COL). The live PVO supplies every outputSchema column but
        # not TENANT_ONLY_COL — the 4071 gate must still fire on it, proving the
        # node's own requiredColumns are probed (not just outputSchema).
        node_dict = yaml.safe_load(_BRONZE_NODE_YAML)
        node_dict["requiredColumns"]["test_bronze"].append("TENANT_ONLY_COL")
        from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import NodeYaml
        node = NodeYaml.model_validate(node_dict)
        # PVO has the outputSchema cols but NOT TENANT_ONLY_COL.
        self._patch(monkeypatch, ["VENDORID", "SEGMENT1"])
        result = _bronze_source_schema_gate(
            MagicMock(), node=node, pack=MagicMock(), profile=_gate_profile(), ctx=_gate_ctx()
        )
        assert result is not None
        msg, diag = result
        assert AIDPF_4071_BRONZE_SOURCE_COLUMN_MISSING in msg
        assert "TENANT_ONLY_COL" in msg
        assert diag["missingColumns"] == ["TENANT_ONLY_COL"]

    def test_probe_failure_degrades_to_none(self, monkeypatch):
        import oracle_ai_data_platform_fusion_autopilot.orchestrator.builtins.bronze_extract_adapter as bea
        import oracle_ai_data_platform_fusion_autopilot.orchestrator.runtime as rt
        from types import SimpleNamespace
        monkeypatch.setattr(rt, "_resolve_password",
                            lambda _v: SimpleNamespace(get_secret_value=lambda: "pw"))
        def _boom(*a, **k):
            raise RuntimeError("BICC unreachable")
        monkeypatch.setattr(bea, "probe_bronze_schemas", _boom)
        assert _bronze_source_schema_gate(
            MagicMock(), node=_bronze_node(), pack=MagicMock(), profile=_gate_profile(), ctx=_gate_ctx()
        ) is None

    def test_type_mismatch_returns_4070_pre_extract(self, monkeypatch):
        # Names all present, but VENDORID materialises as decimal(18,0) while
        # the fixture declares decimal(38,30) — the hoisted AIDPF-4070 gate
        # must fire BEFORE extraction.
        self._patch(monkeypatch, ["VENDORID", "SEGMENT1"])
        # override VENDORID's probed type to a mismatch
        import oracle_ai_data_platform_fusion_autopilot.orchestrator.builtins.bronze_extract_adapter as bea
        monkeypatch.setattr(bea, "probe_bronze_schemas", lambda *a, **k: {
            "test_bronze": _fake_struct(
                ["VENDORID", "SEGMENT1"], types={"VENDORID": "decimal(18,0)"}
            )
        })
        result = _bronze_source_schema_gate(
            MagicMock(), node=_bronze_node(), pack=MagicMock(),
            profile=_gate_profile(), ctx=_gate_ctx(),
        )
        assert result is not None
        msg, diag = result
        assert AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT in msg
        assert "VENDORID" in msg
        assert diag["errorCode"] == AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT
        assert diag["typeMismatches"] == [
            {"column": "VENDORID", "declared": "decimal(38,30)",
             "materialised": "decimal(18,0)"}
        ]

    def test_type_match_returns_none(self, monkeypatch):
        # Probed types equal declared types (case-insensitive name) → pass.
        self._patch(monkeypatch, ["vendorid", "segment1"])
        assert _bronze_source_schema_gate(
            MagicMock(), node=_bronze_node(), pack=MagicMock(),
            profile=_gate_profile(), ctx=_gate_ctx(),
        ) is None


class TestBatchBronzeSourceSchemaGate:
    """check_bronze_source_schemas — ONE probe over all in-scope bronze
    nodes, returns a failure list (fail-fast, before any extract)."""

    def _patch(self, monkeypatch, schemas_by_node):
        import oracle_ai_data_platform_fusion_autopilot.orchestrator.builtins.bronze_extract_adapter as bea
        import oracle_ai_data_platform_fusion_autopilot.orchestrator.runtime as rt
        from types import SimpleNamespace
        monkeypatch.setattr(rt, "_resolve_password",
                            lambda _v: SimpleNamespace(get_secret_value=lambda: "pw"))
        monkeypatch.setattr(bea, "probe_bronze_schemas", lambda *a, **k: schemas_by_node)

    def _pack(self):
        from types import SimpleNamespace
        return SimpleNamespace(bronze={"test_bronze": _bronze_node()})

    def _call(self, **kw):
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            check_bronze_source_schemas,
        )
        from unittest.mock import MagicMock
        from types import SimpleNamespace
        return check_bronze_source_schemas(
            MagicMock(), pack=self._pack(),
            bundle=SimpleNamespace(fusion=SimpleNamespace(password="x")),
            profile=SimpleNamespace(tenant="t"),
            bronze_node_ids=["test_bronze"], run_id="run-batch", **kw,
        )

    def test_all_present_returns_empty(self, monkeypatch):
        self._patch(monkeypatch, {"test_bronze": _fake_struct(["VENDORID", "SEGMENT1", "x"])})
        assert self._call() == []

    def test_missing_returns_failure_with_diagnostic(self, monkeypatch):
        self._patch(monkeypatch, {"test_bronze": _fake_struct(["vendorid"])})  # SEGMENT1 absent
        out = self._call()
        assert len(out) == 1
        assert out[0]["node"] == "test_bronze"
        assert AIDPF_4071_BRONZE_SOURCE_COLUMN_MISSING in out[0]["message"]
        assert out[0]["diagnostic"]["missingColumns"] == ["SEGMENT1"]

    def test_type_mismatch_returns_4070_failure(self, monkeypatch):
        # All names present but VENDORID type drifts → batch gate fails the
        # node with AIDPF-4070 BEFORE any extract.
        self._patch(monkeypatch, {"test_bronze": _fake_struct(
            ["VENDORID", "SEGMENT1"], types={"VENDORID": "decimal(18,0)"}
        )})
        out = self._call()
        assert len(out) == 1
        assert out[0]["node"] == "test_bronze"
        assert AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT in out[0]["message"]
        assert out[0]["diagnostic"]["errorCode"] == AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT
        assert out[0]["diagnostic"]["typeMismatches"][0]["column"] == "VENDORID"

    def test_probe_failure_degrades_to_empty(self, monkeypatch):
        import oracle_ai_data_platform_fusion_autopilot.orchestrator.builtins.bronze_extract_adapter as bea
        import oracle_ai_data_platform_fusion_autopilot.orchestrator.runtime as rt
        from types import SimpleNamespace
        monkeypatch.setattr(rt, "_resolve_password",
                            lambda _v: SimpleNamespace(get_secret_value=lambda: "pw"))
        def _boom(*a, **k): raise RuntimeError("BICC down")
        monkeypatch.setattr(bea, "probe_bronze_schemas", _boom)
        assert self._call() == []


class TestBatchGateDownstreamNeeds:
    """The batch gate also fails when an in-scope silver/gold node needs a
    column the bronze source's PVO can't supply — caught BEFORE extraction."""

    def _patch(self, monkeypatch, schemas_by_node):
        import oracle_ai_data_platform_fusion_autopilot.orchestrator.builtins.bronze_extract_adapter as bea
        import oracle_ai_data_platform_fusion_autopilot.orchestrator.runtime as rt
        from types import SimpleNamespace
        monkeypatch.setattr(rt, "_resolve_password",
                            lambda _v: SimpleNamespace(get_secret_value=lambda: "pw"))
        monkeypatch.setattr(bea, "probe_bronze_schemas", lambda *a, **k: schemas_by_node)

    def _call(self, **kw):
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            check_bronze_source_schemas,
        )
        from unittest.mock import MagicMock
        from types import SimpleNamespace
        return check_bronze_source_schemas(
            MagicMock(), pack=SimpleNamespace(bronze={"test_bronze": _bronze_node()}),
            bundle=SimpleNamespace(fusion=SimpleNamespace(password="x")),
            profile=SimpleNamespace(tenant="t"),
            bronze_node_ids=["test_bronze"], run_id="run-ds", **kw,
        )

    def test_downstream_need_missing_from_pvo_fails(self, monkeypatch):
        # declared (VENDORID, SEGMENT1) all present; silver needs a col the PVO lacks.
        self._patch(monkeypatch, {"test_bronze": _fake_struct(["vendorid", "segment1"])})
        out = self._call(downstream_required={"test_bronze": {"SILVER_NEEDS_THIS"}})
        assert len(out) == 1
        assert "SILVER_NEEDS_THIS" in out[0]["diagnostic"]["missingColumns"]

    def test_downstream_need_present_passes(self, monkeypatch):
        self._patch(monkeypatch, {"test_bronze": _fake_struct(["vendorid", "segment1"])})
        out = self._call(downstream_required={"test_bronze": {"VENDORID"}})  # present (ci)
        assert out == []

    def test_downstream_audit_columns_ignored(self, monkeypatch):
        # _extract_ts is adapter-generated; a silver dep on it must NOT trip
        # the gate even though it's absent from the PVO.
        self._patch(monkeypatch, {"test_bronze": _fake_struct(["vendorid", "segment1"])})
        out = self._call(downstream_required={"test_bronze": {"_extract_ts"}})
        assert out == []
