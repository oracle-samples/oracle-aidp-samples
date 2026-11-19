"""Unit tests for the content-pack builtin dispatch path (Phase 3 Step 3).

Covers:

* Happy-path dispatch through ``dim_calendar`` adapter → success state
  row with ``node_implementation_type='builtin'``.
* Unknown builtin id → AIDPF-5014; ``render_failed`` state row written;
  ``NodeExecutionResult.status == 'render_failed'``.
* Plan-hash drift on builtin (incremental) → ``resume_drift_blocked``.
* Adapter: tenant id ≠ active profile name → reads
  ``pack.profiles[active_profile_name]``, not ``pack.profiles[tenant]``.
* Adapter: profile-level ``calendar`` block override wins over pack
  defaults.

The first three tests exercise the dispatcher via mocked Spark + mocked
adapter; the last two exercise the adapter directly because they're
about precedence logic, not dispatcher wiring.
"""

from __future__ import annotations

import pathlib
import textwrap
from unittest.mock import MagicMock, patch

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator import sql_runner
from oracle_ai_data_platform_fusion_autopilot.orchestrator.builtins import (
    dim_calendar_adapter,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_pack
from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_renderer import RunContext
from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
    AIDPF_5014_UNKNOWN_BUILTIN_DISPATCH,
    execute_node,
)
from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
    load_tenant_profile_from_string,
)


# ---------------------------------------------------------------------------
# Pack + profile fixtures
# ---------------------------------------------------------------------------


PACK_YAML = """
id: phase3-builtin-test
version: 1.0.0
description: Phase 3 builtin-dispatch test pack
compatibility:
  pluginMinVersion: 0.3.0
profiles:
  finance-default:
    calendar:
      startDate: '2020-01-01'
      endDate: '2030-12-31'
      fiscalStartMonth: 1
    chartOfAccounts:
      balancingSegment: segment1
      costCenterSegment: segment2
      naturalAccountSegment: segment3
"""

NODE_YAML_DIM_CALENDAR = """
id: dim_calendar
layer: silver
implementation:
  type: builtin
  callable: oracle_ai_data_platform_fusion_autopilot.dimensions.dim_calendar:build
target: dim_calendar
dependsOn:
  bronze: []
  silver: []
refresh:
  seed:
    strategy: replace
outputSchema:
  columns:
    - name: calendar_key
      type: bigint
      nullable: false
      pii: none
    - name: calendar_date
      type: date
      nullable: false
      pii: none
"""

NODE_YAML_UNKNOWN_BUILTIN = """
id: dim_widget
layer: silver
implementation:
  type: builtin
  callable: nonexistent.module:run
target: dim_widget
dependsOn:
  bronze: []
  silver: []
refresh:
  seed:
    strategy: replace
outputSchema:
  columns:
    - name: widget_id
      type: bigint
      nullable: false
      pii: none
"""

PROFILE_YAML = """
schemaVersion: 1
tenant: acme-corp
pinnedAt: 2026-06-05T00:00:00+00:00
bronzeSchemaFingerprint: "sha256:builtin-test"
"""


def _build_pack(tmp_path: pathlib.Path, node_yaml: str = NODE_YAML_DIM_CALENDAR, node_id: str = "dim_calendar"):
    pack_root = tmp_path / "pack"
    pack_root.mkdir(parents=True, exist_ok=True)
    (pack_root / "pack.yaml").write_text(PACK_YAML, encoding="utf-8")
    silver = pack_root / "silver"
    silver.mkdir()
    (silver / f"{node_id}.yaml").write_text(node_yaml, encoding="utf-8")
    return load_pack(pack_root)


def _profile():
    return load_tenant_profile_from_string(PROFILE_YAML)


def _ctx(mode: str = "seed", run_id: str = "builtin-test-run") -> RunContext:
    return RunContext(
        catalog="cat",
        bronze_schema="bronze",
        silver_schema="silver",
        gold_schema="gold",
        run_id=run_id,
        active_profile_name="finance-default",
        prior_watermark={},
        mode=mode,
        bronze_table_for_source={},
    )


def _paths() -> MagicMock:
    """MagicMock paths whose .bronze/.silver/.gold return string identifiers.

    Required after the Phase 9 follow-up made ``paths`` a required
    positional arg at every ``_build_target_identifier`` call site;
    bare ``MagicMock()`` returns Mock objects which propagate as
    ``f"... FROM {target}"`` strings containing ``<MagicMock ...>``.
    """
    paths = MagicMock()
    paths.bronze.side_effect = lambda t: f"cat.bronze.{t}"
    paths.silver.side_effect = lambda t: f"cat.silver.{t}"
    paths.gold.side_effect = lambda t: f"cat.gold.{t}"
    return paths


def _fake_spark_for_builtin(materialized_cols: list[tuple[str, str]] | None = None) -> MagicMock:
    """Fake Spark for the builtin path.

    The adapter under test is mocked separately — the real adapter would
    call ``spark.sql(CREATE OR REPLACE ...)`` against the v1 builtin's
    full 16-column SELECT. For the dispatcher tests we mock the adapter
    so this fixture only needs to cover:

    * Preflight DESCRIBE (no required cols — dim_calendar has empty
      ``required.bronze.columns`` so preflight is trivially OK).
    * ``spark.table(target).count()`` for the post-execute row count.
    * Materialised-schema DESCRIBE → matches the (small) declared schema.
    * ``spark.createDataFrame(...)`` for the state-row write.
    """
    if materialized_cols is None:
        materialized_cols = [("calendar_key", "bigint"), ("calendar_date", "date")]

    spark = MagicMock()

    def sql_side_effect(stmt: str, *args, **kwargs):
        df = MagicMock(name="default-df")
        upper_stmt = stmt.upper()
        if "DESCRIBE TABLE" in upper_stmt:
            df.collect.return_value = [(name, dtype, None) for name, dtype in materialized_cols]
            return df
        df.collect.return_value = []
        return df

    spark.sql.side_effect = sql_side_effect

    target_df = MagicMock(name="target_df")
    target_df.count.return_value = 4018  # ~10 years of daily rows
    spark.table.return_value = target_df

    spark.createDataFrame.return_value = MagicMock(name="state_df")
    return spark


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestBuiltinDispatchHappyPath:
    def test_dim_calendar_dispatches_through_adapter(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        spark = _fake_spark_for_builtin()
        node = pack.silver["dim_calendar"]

        with patch.object(
            dim_calendar_adapter, "run", return_value=MagicMock(name="cal_df")
        ) as mock_adapter:
            result = execute_node(
                spark,
                node=node,
                pack=pack,
                profile=_profile(),
                ctx=_ctx("seed"),
                paths=_paths(),
                mode="seed",
                profile_hash="profile-hash-test",
                prior_plan_hash=None,
            )

        assert result.status == "success", (
            f"expected success, got {result.status}: {result.error_message}"
        )
        # Adapter received uniform signature
        assert mock_adapter.called
        call_kwargs = mock_adapter.call_args.kwargs
        assert call_kwargs["node"] is node
        assert call_kwargs["ctx"].active_profile_name == "finance-default"
        # State row was written (createDataFrame called at least once for
        # the state batch).
        assert spark.createDataFrame.called

    def test_registry_populated_lazily(self) -> None:
        """First dispatch populates _BUILTIN_REGISTRY; subsequent calls reuse."""
        # Clear the registry to simulate fresh-process state.
        sql_runner._BUILTIN_REGISTRY.clear()
        sql_runner._ensure_registry_populated()
        assert (
            "oracle_ai_data_platform_fusion_autopilot.dimensions.dim_calendar:build"
            in sql_runner._BUILTIN_REGISTRY
        )
        # Second call is idempotent.
        sql_runner._ensure_registry_populated()
        assert len(sql_runner._BUILTIN_REGISTRY) == 1


# ---------------------------------------------------------------------------
# Unknown builtin id → AIDPF-5014
# ---------------------------------------------------------------------------


class TestUnknownBuiltinDispatch:
    def test_unknown_callable_returns_render_failed(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path, NODE_YAML_UNKNOWN_BUILTIN, node_id="dim_widget")
        spark = _fake_spark_for_builtin()
        node = pack.silver["dim_widget"]

        result = execute_node(
            spark,
            node=node,
            pack=pack,
            profile=_profile(),
            ctx=_ctx("seed"),
            paths=_paths(),
            mode="seed",
            profile_hash="profile-hash-test",
            prior_plan_hash=None,
        )

        assert result.status == "render_failed"
        assert AIDPF_5014_UNKNOWN_BUILTIN_DISPATCH in result.error_message
        # Message names the offending callable AND lists what IS registered.
        assert "nonexistent.module:run" in result.error_message


# ---------------------------------------------------------------------------
# Plan-hash drift on builtin → resume_drift_blocked
# ---------------------------------------------------------------------------


class TestBuiltinPlanHashDrift:
    def test_incremental_drift_blocks_dispatch(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        spark = _fake_spark_for_builtin()
        node = pack.silver["dim_calendar"]

        # Force a known-stale prior plan-hash.
        with patch.object(dim_calendar_adapter, "run") as mock_adapter:
            result = execute_node(
                spark,
                node=node,
                pack=pack,
                profile=_profile(),
                ctx=_ctx("incremental"),
                paths=_paths(),
                mode="incremental",
                profile_hash="profile-hash-test",
                prior_plan_hash="0" * 64,  # known stale
            )

        assert result.status == "resume_drift_blocked"
        assert "AIDPF-4040" in result.error_message
        # Adapter MUST NOT have been called — drift gate fires before dispatch.
        assert not mock_adapter.called


# ---------------------------------------------------------------------------
# Adapter precedence — tenant id ≠ active profile name
# ---------------------------------------------------------------------------


def _profile_with_calendar_override(**calendar_block) -> "object":
    lines = [
        'schemaVersion: 1',
        'tenant: acme-corp',  # tenant id explicitly DIFFERENT from profile name
        'pinnedAt: 2026-06-05T00:00:00+00:00',
        'bronzeSchemaFingerprint: "sha256:builtin-test"',
    ]
    if calendar_block:
        lines.append('profile:')
        lines.append('  calendar:')
        for k, v in calendar_block.items():
            lines.append(f'    {k}: {v!r}')
    return load_tenant_profile_from_string("\n".join(lines) + "\n")


class TestAdapterPrecedence:
    def test_tenant_id_differs_from_active_profile_name(self, tmp_path: pathlib.Path) -> None:
        """Adapter MUST key off ctx.active_profile_name, NOT profile.tenant."""
        pack = _build_pack(tmp_path)
        node = pack.silver["dim_calendar"]
        # Tenant is 'acme-corp'; active profile is 'finance-default'.
        # If the adapter wrongly used profile.tenant, pack.profiles['acme-corp']
        # would KeyError — so the adapter must look up 'finance-default'.
        profile = _profile_with_calendar_override()  # no override → use pack default

        captured = {}

        def fake_build(spark, **kwargs):
            captured.update(kwargs)
            return MagicMock()

        with patch.object(
            dim_calendar_adapter._dim_calendar, "build", side_effect=fake_build
        ):
            dim_calendar_adapter.run(
                spark=MagicMock(),
                node=node,
                pack=pack,
                profile=profile,
                ctx=_ctx(),
            )

        # Adapter used pack.profiles['finance-default'] defaults (the pack
        # YAML above declares startDate '2020-01-01', fiscalStartMonth 1).
        assert captured["start_date"] == "2020-01-01"
        assert captured["end_date"] == "2030-12-31"
        assert captured["fiscal_start_month"] == 1
        assert captured["silver_table"] == "cat.silver.dim_calendar"
        assert captured["run_id"] == "builtin-test-run"

    def test_profile_override_wins_over_pack_default(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        node = pack.silver["dim_calendar"]
        # Profile-level override declares a non-default fiscalStartMonth.
        profile = _profile_with_calendar_override(fiscalStartMonth=7)

        captured = {}

        def fake_build(spark, **kwargs):
            captured.update(kwargs)
            return MagicMock()

        with patch.object(
            dim_calendar_adapter._dim_calendar, "build", side_effect=fake_build
        ):
            dim_calendar_adapter.run(
                spark=MagicMock(),
                node=node,
                pack=pack,
                profile=profile,
                ctx=_ctx(),
            )

        # Tenant override wins; pack default was 1.
        assert captured["fiscal_start_month"] == 7
        # Untouched keys fall back to pack defaults.
        assert captured["start_date"] == "2020-01-01"

    def test_profile_override_wins_for_dates_too(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        node = pack.silver["dim_calendar"]
        profile = _profile_with_calendar_override(
            startDate="2022-01-01",
            endDate="2025-12-31",
        )

        captured = {}

        def fake_build(spark, **kwargs):
            captured.update(kwargs)
            return MagicMock()

        with patch.object(
            dim_calendar_adapter._dim_calendar, "build", side_effect=fake_build
        ):
            dim_calendar_adapter.run(
                spark=MagicMock(),
                node=node,
                pack=pack,
                profile=profile,
                ctx=_ctx(),
            )

        assert captured["start_date"] == "2022-01-01"
        assert captured["end_date"] == "2025-12-31"
