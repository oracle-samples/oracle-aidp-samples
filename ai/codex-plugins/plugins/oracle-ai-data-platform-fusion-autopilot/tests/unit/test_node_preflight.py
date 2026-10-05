"""Unit tests for ``orchestrator/node_preflight.py`` (Phase 2 Step 7).

Tests verify the **ordering invariant**: preflight does NOT render SQL.
This is what enables Step 11's execute_node assertion that the renderer
is never invoked when preflight blocks.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import yaml

from oracle_ai_data_platform_fusion_autopilot.orchestrator.node_preflight import (
    AIDPF_2042_REQUIRED_COLUMN_MISSING,
    AIDPF_2043_WATERMARK_COLUMN_MISSING,
    AIDPF_2046_REQUIRED_COLUMN_UNRESOLVED_REF,
    PreflightError,
    PreflightReport,
    preflight_node,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_renderer import RunContext
from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import NodeYaml


NODE_YAML_REQUIRED_COLS = """
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
    - VENDORID
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


def _load_node(yaml_text: str = NODE_YAML_REQUIRED_COLS) -> NodeYaml:
    return NodeYaml.model_validate(yaml.safe_load(yaml_text))


def _ctx() -> RunContext:
    return RunContext(
        catalog="cat",
        bronze_schema="bronze",
        silver_schema="silver",
        gold_schema="gold",
        run_id="r",
        active_profile_name="finance-default",
        bronze_table_for_source={"erp_thing": "cat.bronze.erp_thing"},
    )


def _fake_describe_spark(columns: list[str]) -> MagicMock:
    """Fake Spark whose DESCRIBE TABLE returns Row-like tuples for ``columns``."""
    spark = MagicMock()
    df = MagicMock()
    df.collect.return_value = [(c, "string", None) for c in columns]
    spark.sql.return_value = df
    return spark


def _pack(
    alias_keys: tuple[str, ...] = (),
    bronze_ids: tuple[str, ...] = ("erp_thing",),
    silver_ids: tuple[str, ...] = (),
) -> MagicMock:
    """Minimal ResolvedPack-shaped mock.

    Fields preflight reads that must be real:
    * ``pack.column_aliases`` — `_resolve_required_column_entry`'s key check.
    * ``pack.bronze`` — the live-DESCRIBE gate is bronze-only; preflight skips a
      ``requiredColumns`` source whose id is not in ``pack.bronze`` (silver/gold
      deps are gated statically by AIDPF-2045 + the producer's 4070/4071). Real
      dicts so the ``in`` membership test behaves (a bare MagicMock would make
      every source look non-bronze).
    """
    m = MagicMock()
    m.pack.column_aliases = {k: MagicMock() for k in alias_keys}
    m.bronze = {bid: MagicMock() for bid in bronze_ids}
    m.silver = {sid: MagicMock() for sid in silver_ids}
    m.gold = {}
    return m


def _profile(resolved_column: dict[str, str] | None = None) -> MagicMock:
    """Minimal TenantProfile-shaped mock with a real dict at ``resolved.column``."""
    m = MagicMock()
    m.resolved.column = dict(resolved_column or {})
    return m


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestPreflightHappyPath:
    def test_all_required_columns_present(self) -> None:
        spark = _fake_describe_spark(["SEGMENT1", "VENDORID", "_extract_ts"])
        report = preflight_node(spark, _load_node(), pack=_pack(), profile=_profile(), ctx=_ctx())
        assert report.ok
        assert report.errors == ()

    def test_returns_preflight_report(self) -> None:
        spark = _fake_describe_spark(["SEGMENT1", "VENDORID", "_extract_ts"])
        report = preflight_node(spark, _load_node(), pack=_pack(), profile=_profile(), ctx=_ctx())
        assert isinstance(report, PreflightReport)


# ---------------------------------------------------------------------------
# Required column missing
# ---------------------------------------------------------------------------


class TestRequiredColumnMissing:
    def test_missing_required_column_raises_2042(self) -> None:
        spark = _fake_describe_spark(["VENDORID", "_extract_ts"])  # SEGMENT1 missing
        report = preflight_node(spark, _load_node(), pack=_pack(), profile=_profile(), ctx=_ctx())
        assert not report.ok
        codes = [e.code for e in report.errors]
        assert AIDPF_2042_REQUIRED_COLUMN_MISSING in codes
        # Message names the column.
        assert any("SEGMENT1" in e.message for e in report.errors)

    def test_required_column_pascalcase_live_uppercase_pack_passes(self) -> None:
        # Pack declares SEGMENT1 / VENDORID (UPPERCASE); live tenant
        # (demo-pod D1 evidence) emits PascalCase. Spark resolves the
        # SQL case-insensitively at query time, so preflight must not
        # over-reject what the engine would accept. Regression for
        # docs/v2-phase-4-live-defects.md D1 Layer A.
        spark = _fake_describe_spark(["Segment1", "VendorId", "_extract_ts"])
        report = preflight_node(spark, _load_node(), pack=_pack(), profile=_profile(), ctx=_ctx())
        assert report.ok, [e.message for e in report.errors]


_NODE_YAML_SEMANTIC = """
id: dim_thing
layer: silver
implementation: { type: sql, sql: silver/dim_thing.sql }
target: dim_thing
outputSchema:
  columns:
    - { name: thing_id, type: string, nullable: false, pii: none }
dependsOn:
  bronze:
    - id: erp_thing
      role: primary
      watermark: { column: _extract_ts }
requiredColumns:
  erp_thing:
    - SEGMENT1
    - $semantic.cancelled_status
refresh:
  seed: { strategy: replace }
  incremental:
    strategy: merge
    naturalKey: [thing_id]
    watermark: { source: erp_thing, column: _extract_ts }
"""


def _semantic_pack() -> MagicMock:
    """Pack mock with a `cancelled_status` semanticVariant whose active candidate
    `cancelled_date` detects `ApInvoicesCancelledDate`."""
    from types import SimpleNamespace
    m = MagicMock()
    m.pack.column_aliases = {}
    m.bronze = {"erp_thing": MagicMock()}
    m.silver = {}
    m.gold = {}
    cand = SimpleNamespace(id="cancelled_date",
                           detect=SimpleNamespace(column_exists="ApInvoicesCancelledDate"))
    m.pack.semantic_variants = {"cancelled_status": SimpleNamespace(candidates=[cand])}
    return m


def _semantic_profile() -> MagicMock:
    m = MagicMock()
    m.resolved.column = {}
    m.resolved.semantic = {"cancelled_status": "cancelled_date"}
    return m


class TestSemanticRequiredColumn:
    def test_semantic_ref_resolves_and_passes(self) -> None:
        # $semantic.cancelled_status → ApInvoicesCancelledDate; present in live
        # schema → preflight passes (regression: it used to false-fail AIDPF-2042
        # treating the $semantic.* entry as a literal column name).
        spark = _fake_describe_spark(["SEGMENT1", "ApInvoicesCancelledDate", "_extract_ts"])
        report = preflight_node(
            spark, _load_node(_NODE_YAML_SEMANTIC),
            pack=_semantic_pack(), profile=_semantic_profile(), ctx=_ctx(),
        )
        assert report.ok, [e.message for e in report.errors]

    def test_semantic_resolved_column_missing_raises_2042(self) -> None:
        spark = _fake_describe_spark(["SEGMENT1", "_extract_ts"])  # cancelled date absent
        report = preflight_node(
            spark, _load_node(_NODE_YAML_SEMANTIC),
            pack=_semantic_pack(), profile=_semantic_profile(), ctx=_ctx(),
        )
        assert not report.ok
        assert any(
            e.code == AIDPF_2042_REQUIRED_COLUMN_MISSING and "ApInvoicesCancelledDate" in e.message
            for e in report.errors
        )

    def test_unknown_source_id_yields_2042(self) -> None:
        spark = _fake_describe_spark(["SEGMENT1", "VENDORID", "_extract_ts"])
        ctx = RunContext(
            catalog="cat",
            bronze_schema="bronze",
            silver_schema="silver",
            gold_schema="gold",
            run_id="r",
            active_profile_name="finance-default",
            bronze_table_for_source={},  # NO entry for erp_thing
        )
        report = preflight_node(spark, _load_node(), pack=_pack(), profile=_profile(), ctx=ctx)
        assert any(e.code == AIDPF_2042_REQUIRED_COLUMN_MISSING for e in report.errors)


# ---------------------------------------------------------------------------
# Gold node with a SILVER dependency in requiredColumns — the live-DESCRIBE
# gate is bronze-only (silver sources are pack-built; gated by AIDPF-2045 +
# the producer's 4070/4071). Regression for the silver-source preflight gap.
# ---------------------------------------------------------------------------


_NODE_YAML_GOLD_SILVER_DEP = """
id: supplier_spend
layer: gold
implementation:
  type: sql
  sql: gold/supplier_spend.sql
target: supplier_spend
outputSchema:
  columns:
    - name: vendor_id
      type: bigint
      nullable: true
      pii: none
dependsOn:
  bronze:
    - id: erp_thing
      role: primary
  silver:
    - id: dim_supplier
requiredColumns:
  erp_thing:
    - SEGMENT1
  dim_supplier:
    - supplier_name
    - vendor_id
refresh:
  seed:
    strategy: replace
"""


class TestGoldNodeWithSilverDependency:
    """A gold node may declare ``requiredColumns`` on a silver source. The
    source→table map (``ctx.bronze_table_for_source``) only carries bronze
    sources, so the live-DESCRIBE gate MUST skip silver deps rather than
    false-fail AIDPF-2042 (which would block the node before render)."""

    def _pack(self):
        return _pack(bronze_ids=("erp_thing",), silver_ids=("dim_supplier",))

    def test_silver_dependency_does_not_block_preflight(self) -> None:
        # Bronze SEGMENT1 present; dim_supplier absent from the source→table map
        # (mirrors the real orchestrator map, which is bronze-only). Preflight
        # must pass — the silver columns are owned by AIDPF-2045 + 4070/4071.
        spark = _fake_describe_spark(["SEGMENT1", "_extract_ts"])
        report = preflight_node(
            spark, _load_node(_NODE_YAML_GOLD_SILVER_DEP),
            pack=self._pack(), profile=_profile(), ctx=_ctx(),
        )
        assert report.ok, [e.message for e in report.errors]
        # Specifically: no AIDPF-2042 naming the silver source.
        assert not any(
            "dim_supplier" in (e.message or "") for e in report.errors
        ), [e.message for e in report.errors]

    def test_bronze_source_still_checked_alongside_silver_dep(self) -> None:
        # The skip is silver-only — a genuinely missing BRONZE column must still
        # fail AIDPF-2042 (proves the guard doesn't over-skip).
        spark = _fake_describe_spark(["_extract_ts"])  # SEGMENT1 missing
        report = preflight_node(
            spark, _load_node(_NODE_YAML_GOLD_SILVER_DEP),
            pack=self._pack(), profile=_profile(), ctx=_ctx(),
        )
        assert not report.ok
        assert any(
            e.code == AIDPF_2042_REQUIRED_COLUMN_MISSING and "SEGMENT1" in (e.message or "")
            for e in report.errors
        )
        # Still no false-positive on the silver source.
        assert not any("dim_supplier" in (e.message or "") for e in report.errors)


# ---------------------------------------------------------------------------
# requiredColumns `$column.<key>` reference resolution (AIDPF-2046)
# ---------------------------------------------------------------------------


NODE_YAML_WITH_REFS = """
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
    - $column.supplier_natural_key
    - PARTYID
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


class TestRequiredColumnRefResolution:
    """B-2 layer: `$column.<key>` references resolve through pack aliases + profile."""

    def test_column_ref_resolves_through_profile(self) -> None:
        # Pack declares the alias; profile pins it to "Segment1" (PascalCase
        # — same demo-pod evidence). Live bronze has Segment1. Should pass.
        spark = _fake_describe_spark(["Segment1", "PARTYID", "_extract_ts"])
        node = _load_node(NODE_YAML_WITH_REFS)
        pack = _pack(alias_keys=("supplier_natural_key",))
        profile = _profile(resolved_column={"supplier_natural_key": "Segment1"})
        report = preflight_node(spark, node, pack=pack, profile=profile, ctx=_ctx())
        assert report.ok, [e.message for e in report.errors]

    def test_column_ref_resolves_with_word_different_tenant(self) -> None:
        # Hypothetical overlay-driven tenant where the natural key column is
        # actually "SupplierNumber" — not just case-different. The skill's
        # overlay adds SupplierNumber to the candidate list; bootstrap pins
        # it; preflight must follow the profile, not the pack's literal.
        spark = _fake_describe_spark(["SupplierNumber", "PARTYID", "_extract_ts"])
        node = _load_node(NODE_YAML_WITH_REFS)
        pack = _pack(alias_keys=("supplier_natural_key",))
        profile = _profile(resolved_column={"supplier_natural_key": "SupplierNumber"})
        report = preflight_node(spark, node, pack=pack, profile=profile, ctx=_ctx())
        assert report.ok, [e.message for e in report.errors]

    def test_column_ref_unknown_alias_key_raises_2046(self) -> None:
        # Pack doesn't declare the alias (typo in node YAML or stale ref).
        spark = _fake_describe_spark(["Segment1", "PARTYID", "_extract_ts"])
        node = _load_node(NODE_YAML_WITH_REFS)
        pack = _pack(alias_keys=())  # no aliases declared
        profile = _profile(resolved_column={})
        report = preflight_node(spark, node, pack=pack, profile=profile, ctx=_ctx())
        codes = [e.code for e in report.errors]
        assert AIDPF_2046_REQUIRED_COLUMN_UNRESOLVED_REF in codes
        # Message names the unresolved key.
        assert any("supplier_natural_key" in e.message for e in report.errors)

    def test_column_ref_alias_declared_but_profile_unpinned_raises_2046(self) -> None:
        # Bootstrap was never run (or alias was added post-bootstrap).
        spark = _fake_describe_spark(["Segment1", "PARTYID", "_extract_ts"])
        node = _load_node(NODE_YAML_WITH_REFS)
        pack = _pack(alias_keys=("supplier_natural_key",))
        profile = _profile(resolved_column={})  # alias known but unpinned
        report = preflight_node(spark, node, pack=pack, profile=profile, ctx=_ctx())
        codes = [e.code for e in report.errors]
        assert AIDPF_2046_REQUIRED_COLUMN_UNRESOLVED_REF in codes
        # Message hints at re-running bootstrap.
        assert any("bootstrap" in e.message for e in report.errors)

    def test_literal_entry_still_works_alongside_refs(self) -> None:
        # Backward-compat: the YAML mixes a ref ($column.X) with a literal
        # (PARTYID). Both must pass when present in live bronze.
        spark = _fake_describe_spark(["Segment1", "PARTYID", "_extract_ts"])
        node = _load_node(NODE_YAML_WITH_REFS)
        pack = _pack(alias_keys=("supplier_natural_key",))
        profile = _profile(resolved_column={"supplier_natural_key": "Segment1"})
        report = preflight_node(spark, node, pack=pack, profile=profile, ctx=_ctx())
        assert report.ok, [e.message for e in report.errors]


# ---------------------------------------------------------------------------
# Watermark column missing (AIDPF-2043)
# ---------------------------------------------------------------------------


class TestWatermarkColumnMissing:
    def test_watermark_column_absent_raises_2043(self) -> None:
        # DESCRIBE returns required cols but not _extract_ts.
        spark = _fake_describe_spark(["SEGMENT1", "VENDORID"])
        report = preflight_node(spark, _load_node(), pack=_pack(), profile=_profile(), ctx=_ctx())
        codes = [e.code for e in report.errors]
        assert AIDPF_2043_WATERMARK_COLUMN_MISSING in codes

    def test_watermark_column_case_insensitive(self) -> None:
        # Live bronze names the watermark column in a different case than
        # the pack literal. Spark would resolve it; preflight must too.
        # Regression for docs/v2-phase-4-live-defects.md D1 Layer A.
        spark = _fake_describe_spark(["SEGMENT1", "VENDORID", "_Extract_Ts"])
        report = preflight_node(spark, _load_node(), pack=_pack(), profile=_profile(), ctx=_ctx())
        assert all(e.code != AIDPF_2043_WATERMARK_COLUMN_MISSING for e in report.errors), \
            [e.message for e in report.errors]


# ---------------------------------------------------------------------------
# CRITICAL: preflight never renders SQL
# ---------------------------------------------------------------------------


class TestPreflightDoesNotRender:
    """Locks the Step 11 ordering invariant: preflight runs BEFORE render,
    and a preflight failure must never trigger the renderer.

    If a future change accidentally invokes the renderer inside preflight,
    Step 11's render-then-gate ordering tests would start passing for the
    wrong reason. This test catches that regression."""

    def test_renderer_not_called_on_preflight_blocked(self, monkeypatch) -> None:
        spark = _fake_describe_spark(["VENDORID"])  # SEGMENT1 missing → preflight blocks

        # Patch the renderer so any accidental invocation raises.
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import sql_renderer

        renderer_mock = MagicMock(side_effect=AssertionError(
            "render_node_sql MUST NOT be called from preflight_node"
        ))
        monkeypatch.setattr(sql_renderer, "render_node_sql", renderer_mock)

        report = preflight_node(spark, _load_node(), pack=_pack(), profile=_profile(), ctx=_ctx())
        # Preflight blocked, renderer mock never invoked.
        assert not report.ok
        renderer_mock.assert_not_called()

    def test_renderer_not_called_on_preflight_success_either(self, monkeypatch) -> None:
        """Even on the happy path preflight doesn't render — render happens
        in execute_node Step 3, AFTER preflight returns ok."""
        spark = _fake_describe_spark(["SEGMENT1", "VENDORID", "_extract_ts"])

        from oracle_ai_data_platform_fusion_autopilot.orchestrator import sql_renderer
        renderer_mock = MagicMock(side_effect=AssertionError(
            "render_node_sql MUST NOT be called from preflight_node"
        ))
        monkeypatch.setattr(sql_renderer, "render_node_sql", renderer_mock)

        report = preflight_node(spark, _load_node(), pack=_pack(), profile=_profile(), ctx=_ctx())
        assert report.ok
        renderer_mock.assert_not_called()


# ---------------------------------------------------------------------------
# Non-merge-strategy nodes skip watermark check
# ---------------------------------------------------------------------------


class TestNonMergeStrategySkipsWatermarkCheck:
    def test_seed_only_node_skips_watermark_check(self) -> None:
        seed_only_yaml = """
id: replace_only
layer: silver
implementation:
  type: sql
  sql: silver/replace_only.sql
target: replace_only
outputSchema:
  columns:
    - name: x
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
        node = _load_node(seed_only_yaml)
        spark = _fake_describe_spark(["x"])  # no _extract_ts but seed-only node doesn't need it
        report = preflight_node(spark, node, pack=_pack(), profile=_profile(), ctx=_ctx())
        assert report.ok  # No watermark check because there's no incremental.merge.


# ---------------------------------------------------------------------------
# bronze_extract nodes skip table-introspection preflight (first-seed safety)
# ---------------------------------------------------------------------------

BRONZE_EXTRACT_NODE_YAML = """
id: erp_thing
layer: bronze
implementation:
  type: bronze_extract
  datastore: FscmTopModelAM.Test.TestPVO
  pvo_id: FscmTopModelAM.Test.TestPVO
  biccSchema: Financial
  incrementalCapable: true
target: erp_thing
dependsOn:
  bronze: []
  silver: []
requiredColumns:
  erp_thing:
    - SEGMENT1
    - VENDORID
refresh:
  seed:
    strategy: replace
  incremental:
    strategy: merge
    watermark:
      source: erp_thing
      column: LASTUPDATEDATE
    naturalKey: [SEGMENT1]
outputSchema:
  columns:
    - { name: SEGMENT1, type: string, nullable: true, pii: low }
    - { name: _extract_ts, type: timestamp, nullable: false, pii: none }
quality:
  tests: []
"""


class TestBronzeExtractSkipsTableChecks:
    def test_bronze_extract_preflight_does_not_describe_table(self) -> None:
        """A bronze_extract node CREATES its target from the live PVO; the
        table doesn't exist yet on a first-ever seed (or after a drop).
        preflight MUST NOT DESCRIBE it (that raised an uncaught
        AnalysisException and blocked the seed). Even with a spark whose
        every sql() call raises, preflight returns ok without touching it."""
        node = NodeYaml.model_validate(yaml.safe_load(BRONZE_EXTRACT_NODE_YAML))
        spark = MagicMock()
        spark.sql.side_effect = AssertionError(
            "DESCRIBE must not be called during bronze_extract preflight"
        )
        report = preflight_node(spark, node, pack=_pack(), profile=_profile(), ctx=_ctx())
        assert report.ok
        spark.sql.assert_not_called()
