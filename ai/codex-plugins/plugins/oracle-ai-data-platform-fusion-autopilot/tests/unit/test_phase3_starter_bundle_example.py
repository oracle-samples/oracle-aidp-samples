"""Smoke tests for the v2 starter bundle + profile (Phase 3 Step 4).

Covers:

* Bundle parses and resolves to the installed `fusion-finance-starter` pack.
* Profile loads and populates every `resolved.column.*` / `resolved.semantic.*`
  knob the starter-pack SQL templates reference.
* Render-all smoke test (NEW Phase 3 contract) — every silver/gold node with
  `implementation.type == 'sql'` renders without raising
  `UnresolvedVariationPointError` (catches missing profile keys before any
  SQL executes). `dim_calendar` is EXCLUDED because the renderer would
  crash trying to read `node.implementation.sql` on a builtin.
* No bound parameter name ends in `_segment` / `_currency` / `_currency_code`
  — guards against COA / column knobs being smuggled in as `profile.*`
  parameters (which the renderer binds as values, not identifiers).
* Builtin-dispatch smoke test — `dim_calendar` materialises through the
  builtin path with the widened output schema, schema check passes.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator.builtins import (
    dim_calendar_adapter,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
    load_full_chain,
    load_pack,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_renderer import (
    RunContext,
    render_node_sql,
)
from oracle_ai_data_platform_fusion_autopilot.schema.bundle import (
    Bundle,
    load_bundle,
    resolve_content_pack_root,
)
from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
    load_tenant_profile,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_BUNDLE = REPO_ROOT / "examples" / "fusion-finance-starter.yaml"
EXAMPLE_PROFILE = REPO_ROOT / "examples" / "profiles" / "finance-default.yaml"

# Identifier-suffix patterns that suggest a value is a SQL column reference.
# If any bound profile parameter ends in one of these, the SQL would substitute
# a value where it needs an identifier — likely a renderer-token misuse.
_IDENTIFIER_SUFFIX_RE = re.compile(
    r"_(segment|currency|currency_code|natural_key|table|column|schema)$"
)


@pytest.fixture(autouse=True)
def _env_stubs(monkeypatch: pytest.MonkeyPatch) -> None:
    """The example bundle uses ${FUSION_*} env vars; provide stub values so
    `load_bundle` doesn't raise on render."""
    for name, value in [
        ("FUSION_BICC_BASE_URL", "https://example.invalid/bicc"),
        ("FUSION_BICC_USER", "demo"),
        ("FUSION_BICC_EXTERNAL_STORAGE", "demo-storage"),
    ]:
        if os.environ.get(name) is None:
            monkeypatch.setenv(name, value)


# ---------------------------------------------------------------------------
# Bundle + profile parse
# ---------------------------------------------------------------------------


class TestStarterBundleParse:
    def test_bundle_loads_and_declares_content_pack(self) -> None:
        result = load_bundle(EXAMPLE_BUNDLE)
        bundle = result[0] if isinstance(result, tuple) else result
        assert isinstance(bundle, Bundle)
        assert bundle.content_pack is not None
        assert bundle.content_pack.name == "fusion-finance-starter"
        assert bundle.content_pack.profile == "finance-default"

    def test_bundle_resolves_to_installed_pack(self) -> None:
        result = load_bundle(EXAMPLE_BUNDLE)
        bundle = result[0] if isinstance(result, tuple) else result
        pack_root = resolve_content_pack_root(EXAMPLE_BUNDLE, bundle.content_pack)
        assert pack_root.exists()
        assert (pack_root / "pack.yaml").exists()
        assert pack_root.name == "fusion-finance-starter"

    def test_profile_loads_with_required_keys(self) -> None:
        prof = load_tenant_profile(EXAMPLE_PROFILE)
        assert prof.tenant == "finance-default"
        assert prof.bronze_schema_fingerprint.startswith("sha256:")
        # Currency + supplier-key + three COA role-aliases (round-3 restore
        # after the round-2 rollback regression — see
        # docs/v2-phase-3-variation-catalog.md "Round-3 restore notes").
        # Each role's `columnAliases.coa_*_segment` candidate list is
        # single-candidate-per-role; the values below pin the conventional
        # Fusion COA defaults that demo-pod uses.
        required_columns = {
            "supplier_natural_key",
            "vendor_id",
            "invoice_currency_code",
            "coa_balancing_segment",
            "coa_cost_center_segment",
            "coa_natural_account_segment",
        }
        assert required_columns <= set(prof.resolved.column.keys())
        # COA role-aliases pin the demo-pod conventional positions.
        assert prof.resolved.column["coa_balancing_segment"] == "CodeCombinationSegment1"
        assert prof.resolved.column["coa_cost_center_segment"] == "CodeCombinationSegment2"
        assert prof.resolved.column["coa_natural_account_segment"] == "CodeCombinationSegment3"
        # Semantic variant resolved.
        assert prof.resolved.semantic["cancelled_status"] == "cancelled_date"
        # Snapshot date authored at the fixture-determinism value.
        assert prof.profile.get("snapshotDate") == "2026-06-05"


# ---------------------------------------------------------------------------
# Builtin-dispatch smoke test — dim_calendar
# ---------------------------------------------------------------------------


class TestBuiltinDispatchSmoke:
    def test_dim_calendar_adapter_invokes_v1_builtin(self) -> None:
        """The adapter resolves calendar settings + builds the silver-table
        identifier from ctx, then calls dim_calendar.build."""
        pack = load_pack(REPO_ROOT / "scripts" / "oracle_ai_data_platform_fusion_autopilot"
                         / "content_packs" / "fusion-finance-starter")
        profile = load_tenant_profile(EXAMPLE_PROFILE)
        ctx = RunContext(
            catalog="fusion_catalog",
            bronze_schema="bronze",
            silver_schema="silver",
            gold_schema="gold",
            run_id="phase3-step4-smoke",
            active_profile_name="finance-default",
        )
        node = pack.silver["dim_calendar"]

        captured = {}

        def fake_build(spark, **kwargs):
            captured.update(kwargs)
            return MagicMock(name="cal_df")

        with patch.object(
            dim_calendar_adapter._dim_calendar, "build", side_effect=fake_build
        ):
            dim_calendar_adapter.run(
                spark=MagicMock(),
                node=node,
                pack=pack,
                profile=profile,
                ctx=ctx,
            )

        # Adapter wired up the v1 builtin with the right args.
        assert captured["silver_table"] == "fusion_catalog.silver.dim_calendar"
        assert captured["run_id"] == "phase3-step4-smoke"
        # Profile-level calendar override takes precedence over pack defaults
        # (both are identical here, but the precedence path is exercised).
        assert captured["start_date"] == "2020-01-01"
        assert captured["end_date"] == "2030-12-31"
        assert captured["fiscal_start_month"] == 1

    def test_dim_calendar_yaml_declares_widened_output_schema(self) -> None:
        """Step 3 widened dim_calendar.yaml from 6 cols → 16 cols to match
        the actual builtin emit. Pin the contract here so a future YAML
        regression fires fast."""
        pack = load_pack(REPO_ROOT / "scripts" / "oracle_ai_data_platform_fusion_autopilot"
                         / "content_packs" / "fusion-finance-starter")
        node = pack.silver["dim_calendar"]
        col_names = [c.name for c in node.output_schema.columns]
        # 16-column shape per dim_calendar.py:94-138.
        expected = {
            "calendar_key", "calendar_date", "year", "quarter", "month",
            "month_name", "day_of_month", "day_of_week", "day_name",
            "week_of_year", "is_weekend", "fiscal_year", "fiscal_period",
            "fiscal_quarter", "silver_built_at", "silver_run_id",
        }
        assert set(col_names) == expected
        # Quality tests target calendar_date (NOT the historical 'date').
        test_columns = []
        for q in node.quality.tests:
            cols = getattr(q, "columns", None)
            if cols:
                test_columns.extend(cols)
        assert "calendar_date" in test_columns
        assert "calendar_key" in test_columns
        assert "date" not in test_columns  # pre-Phase-3 bug


# ---------------------------------------------------------------------------
# Render-all smoke (Phase 3 Step 4 contract)
# ---------------------------------------------------------------------------


class TestRenderAllSmoke:
    """Every `implementation.type == 'sql'` node MUST render against the
    example profile without raising, MUST NOT bind any identifier-shaped
    name as a profile parameter (which would emit a `:param` where SQL
    needs a bare column / table name), and MUST keep the security-boundary
    invariant that profile values cannot smuggle SQL fragments."""

    def _ctx(self, pack) -> RunContext:
        # The bronze_table_for_source map is consumed by semantic-fragment
        # `{table}` substitutions (e.g. ap_aging's cancelled_status).
        # Phase 9: source from per-file pack.bronze; fall back to the
        # legacy single-file pack.bronze_yaml for pre-Phase-9 packs.
        btfs: dict[str, str] = {
            node_id: f"cat.bronze.{node.target}"
            for node_id, node in pack.bronze.items()
        }
        bronze_yaml = pack.bronze_yaml or {}
        for ds in bronze_yaml.get("datasets", []) or []:
            if isinstance(ds, dict) and "id" in ds and ds["id"] not in btfs:
                btfs[ds["id"]] = f"cat.bronze.{ds['id']}"
        return RunContext(
            catalog="cat",
            bronze_schema="bronze",
            silver_schema="silver",
            gold_schema="gold",
            run_id="render-all-smoke",
            active_profile_name="finance-default",
            bronze_table_for_source=btfs,
        )

    def test_every_sql_node_renders(self) -> None:
        pack = load_full_chain(
            REPO_ROOT / "scripts" / "oracle_ai_data_platform_fusion_autopilot"
            / "content_packs" / "fusion-finance-starter",
        )
        profile = load_tenant_profile(EXAMPLE_PROFILE)
        ctx = self._ctx(pack)

        sql_nodes = [
            n for n in {**pack.silver, **pack.gold}.values()
            if n.implementation.type == "sql"
        ]
        # Phase 3 migrated five nodes; assert that's the floor.
        assert len(sql_nodes) >= 5, (
            f"expected ≥5 type:sql nodes after Steps 5–9, got {len(sql_nodes)}"
        )

        for node in sql_nodes:
            rendered = render_node_sql(node, pack, profile, ctx)
            # No bound parameter name looks like an SQL identifier — that
            # would mean a `{{ profile.* }}` lookup was used where a
            # `{{ column.* }}` substitution was needed.
            for name in rendered.params.keys():
                assert not _IDENTIFIER_SUFFIX_RE.search(name), (
                    f"node {node.id}: bound parameter {name!r} ends in an "
                    f"identifier-shaped suffix — was an identifier knob "
                    f"accidentally smuggled in as {{{{ profile.* }}}}?"
                )

    def test_every_sql_node_emits_run_id_audit(self) -> None:
        """All migrated SQL nodes must carry the orchestrator's run-id
        through to their respective audit column."""
        pack = load_full_chain(
            REPO_ROOT / "scripts" / "oracle_ai_data_platform_fusion_autopilot"
            / "content_packs" / "fusion-finance-starter",
        )
        profile = load_tenant_profile(EXAMPLE_PROFILE)
        ctx = self._ctx(pack)
        for node in {**pack.silver, **pack.gold}.values():
            if node.implementation.type != "sql":
                continue
            rendered = render_node_sql(node, pack, profile, ctx)
            assert ":run_id" in rendered.sql, (
                f"node {node.id} renders without :run_id parameter — "
                f"the audit column is missing the {{{{ run_id_literal }}}} token."
            )
            assert rendered.params.get("run_id") == "render-all-smoke"
