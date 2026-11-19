"""Unit tests for ``orchestrator/sql_renderer.py`` (Phase 2 Step 3).

The renderer is the security boundary between untrusted profile values and
Spark SQL execution. Tests cover:

* Happy path with every substitution kind.
* Identifier allowlist (AIDPF-5001).
* Profile-string injection contained inside parameter markers (no raw
  ``"DROP TABLE"`` ever appears in ``rendered.sql``).
* Semantic-fragment grammar rejection (AIDPF-5010).
* Dotted profile lookups across depth.
* Unknown token (AIDPF-5002).
* Unresolved variation point (AIDPF-5003).
* Hash determinism — cosmetic whitespace changes don't shift the hash;
  profile-value flips do.
* Disallowed param value type (AIDPF-5011).
"""

from __future__ import annotations

import pathlib

import pytest

import oracle_ai_data_platform_fusion_autopilot as _pkg
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_pack
from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_renderer import (
    AIDPF_5001_IDENTIFIER_ALLOWLIST,
    AIDPF_5002_UNKNOWN_TOKEN,
    AIDPF_5003_UNRESOLVED_VARIATION,
    AIDPF_5010_POST_RENDER_REJECTED,
    AIDPF_5011_DISALLOWED_PARAM_TYPE,
    DisallowedParamTypeError,
    IdentifierAllowlistError,
    PostRenderRejectedError,
    RenderedSql,
    RunContext,
    UnknownTokenError,
    UnresolvedVariationPointError,
    _format_profile_value_for_params,
    compute_rendered_sql_hash,
    render_node_sql,
)
from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
    load_tenant_profile_from_string,
)


# ---------------------------------------------------------------------------
# Fixture pack builder — minimal pack on disk that the renderer can read.
# ---------------------------------------------------------------------------


PACK_YAML_BASE = """
id: phase2-renderer-test
version: 1.0.0
description: Phase 2 renderer test pack (no variants)
compatibility:
  pluginMinVersion: 0.3.0
"""


PACK_YAML_WITH_VARIANTS = """
id: phase2-renderer-test
version: 1.0.0
description: Phase 2 renderer test pack with variation points
compatibility:
  pluginMinVersion: 0.3.0
columnAliases:
  invoice_currency:
    appliesTo: silver.dim_thing
    required: true
    candidates:
      - ApInvoicesInvoiceCurrencyCode
      - ApInvoicesCurrencyCode
semanticVariants:
  cancelled_status:
    appliesTo: silver.dim_thing
    required: true
    candidates:
      - id: cancelled_date
        detect:
          columnExists: ApInvoicesCancelledDate
        fragment: "{table}.ApInvoicesCancelledDate IS NULL"
      - id: cancelled_flag
        detect:
          columnExists: CancelledFlag
        fragment: "COALESCE({table}.CancelledFlag, 'N') != 'Y'"
"""


NODE_YAML_NO_VARIANTS = """
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


NODE_YAML_WITH_VARIANTS = """
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
tenant: test-tenant
pinnedAt: 2026-06-01T00:00:00+00:00
bronzeSchemaFingerprint: "sha256:abc"
resolved:
  column:
    invoice_currency: ApInvoicesInvoiceCurrencyCode
  semantic:
    cancelled_status: cancelled_date
profile:
  calendar:
    fiscalStartMonth: 4
    startDate: "2024-01-01"
  chartOfAccounts:
    balancingSegment: segment1
"""


def _build_pack(tmp_path: pathlib.Path, *, with_variants: bool, sql_template: str):
    """Materialise a minimal content pack on disk and return the ResolvedPack."""
    pack_root = tmp_path / "pack"
    pack_root.mkdir(parents=True, exist_ok=True)
    pack_yaml = PACK_YAML_WITH_VARIANTS if with_variants else PACK_YAML_BASE
    (pack_root / "pack.yaml").write_text(pack_yaml, encoding="utf-8")

    silver = pack_root / "silver"
    silver.mkdir()
    node_yaml = NODE_YAML_WITH_VARIANTS if with_variants else NODE_YAML_NO_VARIANTS
    (silver / "dim_thing.yaml").write_text(node_yaml, encoding="utf-8")
    (silver / "dim_thing.sql").write_text(sql_template, encoding="utf-8")

    return load_pack(pack_root)


def _default_ctx(mode: str = "seed", **overrides) -> RunContext:
    base = dict(
        catalog="fusion_catalog",
        bronze_schema="bronze",
        silver_schema="silver",
        gold_schema="gold",
        run_id="run-2026-06-06-001",
        active_profile_name="finance-default",
        prior_watermark={},
        mode=mode,
        bronze_table_for_source={"erp_thing": "fusion_catalog.bronze.erp_thing"},
    )
    base.update(overrides)
    return RunContext(**base)


def _profile():
    return load_tenant_profile_from_string(PROFILE_YAML)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_simple_template_renders(self, tmp_path: pathlib.Path) -> None:
        sql_template = """
        SELECT {{ column.invoice_currency }} AS currency
        FROM {{ catalog }}.{{ bronze_schema }}.erp_thing
        WHERE {{ watermark_predicate }}
        """
        pack = _build_pack(tmp_path, with_variants=True, sql_template=sql_template)
        rendered = render_node_sql(
            pack.silver["dim_thing"], pack, _profile(), _default_ctx()
        )
        # Identifier substitutions inlined.
        assert "fusion_catalog" in rendered.sql
        assert "bronze.erp_thing" in rendered.sql
        assert "ApInvoicesInvoiceCurrencyCode" in rendered.sql
        # Seed-mode watermark renders as always-true.
        assert "1=1" in rendered.sql

    def test_run_id_uses_parameter_marker(self, tmp_path: pathlib.Path) -> None:
        sql_template = "SELECT '{{ run_id_literal }}' AS run_id FROM {{ catalog }}.t"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        rendered = render_node_sql(
            pack.silver["dim_thing"], pack, _profile(), _default_ctx()
        )
        # Marker used (not the raw value).
        assert ":run_id" in rendered.sql
        assert "run-2026-06-06-001" not in rendered.sql
        assert rendered.params["run_id"] == "run-2026-06-06-001"

    def test_incremental_watermark_uses_parameter_marker(self, tmp_path: pathlib.Path) -> None:
        sql_template = "SELECT * FROM t WHERE {{ watermark_predicate }}"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        ctx = _default_ctx(mode="incremental", prior_watermark={"erp_thing": "2026-06-01T00:00:00"})
        rendered = render_node_sql(pack.silver["dim_thing"], pack, _profile(), ctx)
        # Column inlined; value via marker.
        assert "_extract_ts > :watermark_erp_thing" in rendered.sql
        assert rendered.params["watermark_erp_thing"] == "2026-06-01T00:00:00"
        # The raw watermark value must not appear next to the column in the SQL.
        assert "_extract_ts > '2026-06-01" not in rendered.sql

    def test_semantic_variant_renders_with_table_substitution(self, tmp_path: pathlib.Path) -> None:
        sql_template = "SELECT * FROM t WHERE {{ semantic.cancelled_status }}"
        pack = _build_pack(tmp_path, with_variants=True, sql_template=sql_template)
        rendered = render_node_sql(
            pack.silver["dim_thing"], pack, _profile(), _default_ctx()
        )
        # cancelled_date fragment selected; {table} substituted with the
        # bronze table identifier.
        assert "fusion_catalog.bronze.erp_thing.ApInvoicesCancelledDate IS NULL" in rendered.sql

    def test_profile_lookup_dotted_key(self, tmp_path: pathlib.Path) -> None:
        sql_template = "SELECT {{ profile.calendar.fiscalStartMonth }} AS m FROM t"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        rendered = render_node_sql(
            pack.silver["dim_thing"], pack, _profile(), _default_ctx()
        )
        assert ":profile_calendar__fiscalStartMonth" in rendered.sql
        assert rendered.params["profile_calendar__fiscalStartMonth"] == 4

    def test_returns_rendered_sql_dataclass(self, tmp_path: pathlib.Path) -> None:
        sql_template = "SELECT 1 AS x FROM {{ catalog }}.t"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        rendered = render_node_sql(
            pack.silver["dim_thing"], pack, _profile(), _default_ctx()
        )
        assert isinstance(rendered, RenderedSql)
        assert rendered.sql
        assert rendered.hash_input


# ---------------------------------------------------------------------------
# Identifier allowlist (AIDPF-5001)
# ---------------------------------------------------------------------------


class TestIdentifierAllowlist:
    def test_catalog_with_unsafe_chars_rejected(self, tmp_path: pathlib.Path) -> None:
        sql_template = "SELECT * FROM {{ catalog }}.t"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        ctx = _default_ctx(catalog='evil"; DROP TABLE')
        with pytest.raises(IdentifierAllowlistError) as exc_info:
            render_node_sql(pack.silver["dim_thing"], pack, _profile(), ctx)
        assert AIDPF_5001_IDENTIFIER_ALLOWLIST in str(exc_info.value)

    def test_column_resolution_to_unsafe_identifier_rejected(
        self, tmp_path: pathlib.Path
    ) -> None:
        sql_template = "SELECT {{ column.invoice_currency }} FROM t"
        pack = _build_pack(tmp_path, with_variants=True, sql_template=sql_template)
        # Override the profile to resolve to an unsafe value.
        evil_profile_yaml = PROFILE_YAML.replace(
            "ApInvoicesInvoiceCurrencyCode",
            'evil"; DROP TABLE; --',
        )
        profile = load_tenant_profile_from_string(evil_profile_yaml)
        with pytest.raises(IdentifierAllowlistError):
            render_node_sql(pack.silver["dim_thing"], pack, profile, _default_ctx())


# ---------------------------------------------------------------------------
# Profile injection contained inside parameter markers
# ---------------------------------------------------------------------------


class TestProfileInjectionContainment:
    def test_drop_table_string_never_inlined_into_sql(self, tmp_path: pathlib.Path) -> None:
        """A profile-string value containing SQL injection MUST land in
        ``params`` only — never as a substring of ``rendered.sql``.
        """
        sql_template = "SELECT {{ profile.calendar.startDate }} FROM t"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        malicious_profile_yaml = PROFILE_YAML.replace(
            'startDate: "2024-01-01"',
            "startDate: \"'; DROP TABLE x; --\"",
        )
        profile = load_tenant_profile_from_string(malicious_profile_yaml)
        rendered = render_node_sql(pack.silver["dim_thing"], pack, profile, _default_ctx())

        # The malicious string must NOT appear in the SQL.
        assert "DROP TABLE" not in rendered.sql
        # It MUST appear in params (where Spark binds it as a literal).
        assert any("DROP TABLE" in str(v) for v in rendered.params.values())


# ---------------------------------------------------------------------------
# Semantic-fragment grammar (AIDPF-5010)
# ---------------------------------------------------------------------------


class TestSemanticFragmentGrammar:
    def test_subquery_in_fragment_rejected(self, tmp_path: pathlib.Path) -> None:
        bad_pack_yaml = PACK_YAML_WITH_VARIANTS.replace(
            'fragment: "{table}.ApInvoicesCancelledDate IS NULL"',
            'fragment: "(SELECT 1 FROM other_table)"',
        )
        pack_root = tmp_path / "pack"
        pack_root.mkdir()
        (pack_root / "pack.yaml").write_text(bad_pack_yaml, encoding="utf-8")
        silver = pack_root / "silver"
        silver.mkdir()
        (silver / "dim_thing.yaml").write_text(NODE_YAML_WITH_VARIANTS, encoding="utf-8")
        (silver / "dim_thing.sql").write_text(
            "SELECT * FROM t WHERE {{ semantic.cancelled_status }}", encoding="utf-8"
        )
        pack = load_pack(pack_root)
        with pytest.raises(PostRenderRejectedError) as exc_info:
            render_node_sql(pack.silver["dim_thing"], pack, _profile(), _default_ctx())
        assert AIDPF_5010_POST_RENDER_REJECTED in str(exc_info.value)

    def test_semicolon_in_fragment_rejected(self, tmp_path: pathlib.Path) -> None:
        bad_pack_yaml = PACK_YAML_WITH_VARIANTS.replace(
            'fragment: "{table}.ApInvoicesCancelledDate IS NULL"',
            'fragment: "1=1; DROP TABLE x"',
        )
        pack_root = tmp_path / "pack"
        pack_root.mkdir()
        (pack_root / "pack.yaml").write_text(bad_pack_yaml, encoding="utf-8")
        silver = pack_root / "silver"
        silver.mkdir()
        (silver / "dim_thing.yaml").write_text(NODE_YAML_WITH_VARIANTS, encoding="utf-8")
        (silver / "dim_thing.sql").write_text(
            "SELECT * FROM t WHERE {{ semantic.cancelled_status }}", encoding="utf-8"
        )
        pack = load_pack(pack_root)
        with pytest.raises(PostRenderRejectedError):
            render_node_sql(pack.silver["dim_thing"], pack, _profile(), _default_ctx())

    def test_comment_marker_in_fragment_rejected(self, tmp_path: pathlib.Path) -> None:
        bad_pack_yaml = PACK_YAML_WITH_VARIANTS.replace(
            'fragment: "{table}.ApInvoicesCancelledDate IS NULL"',
            'fragment: "1=1 -- comment"',
        )
        pack_root = tmp_path / "pack"
        pack_root.mkdir()
        (pack_root / "pack.yaml").write_text(bad_pack_yaml, encoding="utf-8")
        silver = pack_root / "silver"
        silver.mkdir()
        (silver / "dim_thing.yaml").write_text(NODE_YAML_WITH_VARIANTS, encoding="utf-8")
        (silver / "dim_thing.sql").write_text(
            "SELECT * FROM t WHERE {{ semantic.cancelled_status }}", encoding="utf-8"
        )
        pack = load_pack(pack_root)
        with pytest.raises(PostRenderRejectedError):
            render_node_sql(pack.silver["dim_thing"], pack, _profile(), _default_ctx())


# ---------------------------------------------------------------------------
# Unknown / unresolved tokens
# ---------------------------------------------------------------------------


class TestTokenResolutionFailures:
    def test_unknown_token_raises_5002(self, tmp_path: pathlib.Path) -> None:
        sql_template = "SELECT {{ frobnicate }} FROM t"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        with pytest.raises(UnknownTokenError) as exc_info:
            render_node_sql(pack.silver["dim_thing"], pack, _profile(), _default_ctx())
        assert AIDPF_5002_UNKNOWN_TOKEN in str(exc_info.value)

    def test_undeclared_column_variation_raises_5003(self, tmp_path: pathlib.Path) -> None:
        sql_template = "SELECT {{ column.undeclared }} FROM t"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        with pytest.raises(UnresolvedVariationPointError) as exc_info:
            render_node_sql(pack.silver["dim_thing"], pack, _profile(), _default_ctx())
        assert AIDPF_5003_UNRESOLVED_VARIATION in str(exc_info.value)

    def test_undeclared_semantic_variation_raises_5003(self, tmp_path: pathlib.Path) -> None:
        sql_template = "SELECT * FROM t WHERE {{ semantic.undeclared }}"
        pack = _build_pack(tmp_path, with_variants=True, sql_template=sql_template)
        with pytest.raises(UnresolvedVariationPointError):
            render_node_sql(pack.silver["dim_thing"], pack, _profile(), _default_ctx())

    def test_profile_dotted_lookup_to_dict_rejected(self, tmp_path: pathlib.Path) -> None:
        """``{{ profile.calendar }}`` resolves to a nested dict, not a leaf
        scalar — AIDPF-5011 (disallowed type)."""
        sql_template = "SELECT {{ profile.calendar }} FROM t"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        with pytest.raises(DisallowedParamTypeError):
            render_node_sql(pack.silver["dim_thing"], pack, _profile(), _default_ctx())


# ---------------------------------------------------------------------------
# Hash determinism
# ---------------------------------------------------------------------------


class TestHashDeterminism:
    def test_cosmetic_whitespace_does_not_shift_hash(self, tmp_path: pathlib.Path) -> None:
        sql_template_a = "SELECT 1 FROM {{ catalog }}.t WHERE {{ watermark_predicate }}"
        sql_template_b = "SELECT   1\nFROM   {{ catalog }}.t   WHERE   {{ watermark_predicate }}"
        pack_a = _build_pack(tmp_path / "a", with_variants=False, sql_template=sql_template_a)
        pack_b = _build_pack(tmp_path / "b", with_variants=False, sql_template=sql_template_b)
        r_a = render_node_sql(pack_a.silver["dim_thing"], pack_a, _profile(), _default_ctx())
        r_b = render_node_sql(pack_b.silver["dim_thing"], pack_b, _profile(), _default_ctx())
        assert compute_rendered_sql_hash(r_a) == compute_rendered_sql_hash(r_b)

    def test_profile_value_change_shifts_hash(self, tmp_path: pathlib.Path) -> None:
        sql_template = "SELECT {{ profile.calendar.fiscalStartMonth }} FROM t"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        r_a = render_node_sql(pack.silver["dim_thing"], pack, _profile(), _default_ctx())

        # Profile with a different fiscal start month.
        b_profile = load_tenant_profile_from_string(
            PROFILE_YAML.replace("fiscalStartMonth: 4", "fiscalStartMonth: 7")
        )
        r_b = render_node_sql(pack.silver["dim_thing"], pack, b_profile, _default_ctx())
        assert compute_rendered_sql_hash(r_a) != compute_rendered_sql_hash(r_b)

    def test_run_id_value_does_not_shift_hash(self, tmp_path: pathlib.Path) -> None:
        """REGRESSION (AIDPF-4040): the per-run ``run_id`` param VALUE must not
        enter the plan-hash. The §11.9 hash is a plan-*shape* fingerprint;
        ``run_id`` is run identity, not plan shape. Before the fix it leaked in,
        so the continuity gate fired on every incremental-after-seed (the run_id
        always differs). The ``:run_id`` marker still appears in the SQL, so a
        template change is still caught."""
        sql_template = "SELECT '{{ run_id_literal }}' AS run_id FROM {{ catalog }}.t"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        r_a = render_node_sql(
            pack.silver["dim_thing"], pack, _profile(),
            _default_ctx(run_id="run-2026-06-06-001"),
        )
        r_b = render_node_sql(
            pack.silver["dim_thing"], pack, _profile(),
            _default_ctx(run_id="run-2026-06-15-999"),
        )
        # Different run_ids -> identical plan-hash (the value is excluded).
        assert r_a.params["run_id"] != r_b.params["run_id"]
        assert compute_rendered_sql_hash(r_a) == compute_rendered_sql_hash(r_b)

    def test_watermark_cursor_value_does_not_shift_hash(self, tmp_path: pathlib.Path) -> None:
        """REGRESSION (AIDPF-4040): the per-run ``watermark_<source>`` cursor
        VALUE advances every run; it must not enter the plan-hash or no
        incremental run could ever match the prior run's hash."""
        sql_template = "SELECT * FROM t WHERE {{ watermark_predicate }}"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        r_a = render_node_sql(
            pack.silver["dim_thing"], pack, _profile(),
            _default_ctx(mode="incremental",
                         prior_watermark={"erp_thing": "2026-06-01T00:00:00"}),
        )
        r_b = render_node_sql(
            pack.silver["dim_thing"], pack, _profile(),
            _default_ctx(mode="incremental",
                         prior_watermark={"erp_thing": "2026-06-14T23:59:59"}),
        )
        assert r_a.params["watermark_erp_thing"] != r_b.params["watermark_erp_thing"]
        assert compute_rendered_sql_hash(r_a) == compute_rendered_sql_hash(r_b)

    # ------ Mode-normalization (P-incr-L1 / Approach 3) -------------------

    def test_merge_node_seed_and_incremental_hash_equal(self, tmp_path: pathlib.Path) -> None:
        """P-incr-L1: a MERGE node's seed and (cursor-populated) incremental
        renders must produce the SAME plan-hash, even though their EXECUTABLE
        SQL differs on the watermark predicate (``1=1`` vs ``col > :wm``).

        This is the core of the fix: the hash is computed from a mode-normalized
        render (watermark predicate forced to ``1=1``), so the AIDPF-4040
        continuity gate doesn't false-positive on the first incremental after a
        seed. Pre-fix this asserted ``!=`` (the reviewer's ``hashes_equal=False``
        probe)."""
        sql_template = (
            "SELECT * FROM {{ catalog }}.{{ bronze_schema }}.erp_thing "
            "WHERE {{ watermark_predicate }}"
        )
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        node = pack.silver["dim_thing"]

        r_seed = render_node_sql(node, pack, _profile(), _default_ctx(mode="seed"))
        r_inc = render_node_sql(
            node, pack, _profile(),
            _default_ctx(mode="incremental",
                         prior_watermark={"erp_thing": "2026-06-01T00:00:00"}),
        )

        # Executable SQL is mode-correct (NOT normalized).
        assert "1=1" in r_seed.sql
        assert "_extract_ts > :watermark_erp_thing" in r_inc.sql
        # ...but the plan-hash is identical across modes.
        assert compute_rendered_sql_hash(r_seed) == compute_rendered_sql_hash(r_inc)

    def test_first_incremental_no_cursor_hash_equals_seed(self, tmp_path: pathlib.Path) -> None:
        """A first incremental with no prior cursor renders ``1=1`` in BOTH the
        executable and hash passes — its hash must equal the seed's too."""
        sql_template = "SELECT * FROM t WHERE {{ watermark_predicate }}"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        node = pack.silver["dim_thing"]
        r_seed = render_node_sql(node, pack, _profile(), _default_ctx(mode="seed"))
        r_inc = render_node_sql(
            node, pack, _profile(),
            _default_ctx(mode="incremental", prior_watermark={}),
        )
        assert compute_rendered_sql_hash(r_seed) == compute_rendered_sql_hash(r_inc)

    def test_sql_body_edit_still_shifts_hash_across_modes(self, tmp_path: pathlib.Path) -> None:
        """Detection preserved: a genuine SQL-template body edit must shift the
        hash even with mode-normalization in place (the non-watermark SQL stays
        inlined in the hash input)."""
        base = "SELECT a FROM t WHERE {{ watermark_predicate }}"
        edited = "SELECT a, b FROM t WHERE {{ watermark_predicate }}"
        pack_a = _build_pack(tmp_path / "a", with_variants=False, sql_template=base)
        pack_b = _build_pack(tmp_path / "b", with_variants=False, sql_template=edited)
        # seed-vs-seed and incr-vs-incr both differ.
        r_a = render_node_sql(pack_a.silver["dim_thing"], pack_a, _profile(),
                              _default_ctx(mode="incremental",
                                           prior_watermark={"erp_thing": "2026-06-01T00:00:00"}))
        r_b = render_node_sql(pack_b.silver["dim_thing"], pack_b, _profile(),
                              _default_ctx(mode="incremental",
                                           prior_watermark={"erp_thing": "2026-06-01T00:00:00"}))
        assert compute_rendered_sql_hash(r_a) != compute_rendered_sql_hash(r_b)

    def test_snapshot_date_is_mode_invariant(self, tmp_path: pathlib.Path) -> None:
        """``{{ snapshot_date }}`` is per-PROFILE, not per-mode: it renders
        identically (and hashes identically) in seed and incremental. Guards the
        token-audit claim that watermark_predicate is the ONLY per-mode token."""
        sql_template = (
            "SELECT * FROM t WHERE d <= {{ snapshot_date }} "
            "AND {{ watermark_predicate }}"
        )
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        node = pack.silver["dim_thing"]
        r_seed = render_node_sql(node, pack, _profile(), _default_ctx(mode="seed"))
        r_inc = render_node_sql(
            node, pack, _profile(),
            _default_ctx(mode="incremental",
                         prior_watermark={"erp_thing": "2026-06-01T00:00:00"}),
        )
        assert compute_rendered_sql_hash(r_seed) == compute_rendered_sql_hash(r_inc)


# ---------------------------------------------------------------------------
# Disallowed param value types
# ---------------------------------------------------------------------------


class TestDisallowedParamTypes:
    def test_list_param_value_rejected(self) -> None:
        with pytest.raises(DisallowedParamTypeError) as exc_info:
            _format_profile_value_for_params([1, 2, 3])
        assert AIDPF_5011_DISALLOWED_PARAM_TYPE in str(exc_info.value)

    def test_none_param_value_rejected(self) -> None:
        with pytest.raises(DisallowedParamTypeError):
            _format_profile_value_for_params(None)

    def test_allowed_scalar_types_pass(self) -> None:
        # str / int / float / bool / date / datetime all pass.
        from datetime import date, datetime
        for v in ("x", 1, 1.5, True, date(2026, 1, 1), datetime(2026, 1, 1)):
            assert _format_profile_value_for_params(v) == v


# ---------------------------------------------------------------------------
# Real shipped ap_aging.sql — semantic-source aliasing regression
# ---------------------------------------------------------------------------


class TestRealApAgingSemanticSourceShape:
    """Regression for the `{{ semantic.cancelled_status }}` / FROM-alias clash.

    The semantic fragment substitutes `{table}` with the FULL bronze identifier
    (catalog.schema.ap_invoices). If the SQL aliases ap_invoices with a
    correlation name, that full-path qualifier in the rendered predicate becomes
    unresolvable at execution. The shipped ap_aging.sql therefore keeps
    ap_invoices UNALIASED (reads qualified by the table name). This test renders
    the REAL shipped node and asserts that invariant holds end-to-end.
    """

    STARTER_PACK_ROOT = (
        pathlib.Path(_pkg.__file__).parent
        / "content_packs"
        / "fusion-finance-starter"
    )

    _PROFILE_YAML = """
schemaVersion: 1
tenant: ap-aging-render-test
pinnedAt: 2026-06-01T00:00:00+00:00
bronzeSchemaFingerprint: "sha256:abc"
resolved:
  column:
    invoice_currency_code: ApInvoicesInvoiceCurrencyCode
  semantic:
    cancelled_status: cancelled_date
profile: {}
"""

    def _render(self):
        pack = load_pack(self.STARTER_PACK_ROOT)
        profile = load_tenant_profile_from_string(self._PROFILE_YAML)
        ctx = RunContext(
            catalog="fusion_catalog",
            bronze_schema="bronze",
            silver_schema="silver",
            gold_schema="gold",
            run_id="run-test-001",
            active_profile_name="finance-default",
            mode="seed",
            bronze_table_for_source={
                "ap_invoices": "fusion_catalog.bronze.ap_invoices",
            },
        )
        return render_node_sql(pack.gold["ap_aging"], pack, profile, ctx).sql

    def test_semantic_predicate_uses_full_path_qualifier(self) -> None:
        sql = self._render()
        # {table} → full bronze identifier inside the cancelled-date predicate.
        assert (
            "fusion_catalog.bronze.ap_invoices.ApInvoicesCancelledDate IS NULL"
            in sql
        )

    def test_ap_invoices_source_is_unaliased(self) -> None:
        # The semantic-consuming source must NOT carry a correlation name, or the
        # full-path qualifier above cannot resolve. Unaliased → the FROM clause
        # goes straight from the table identifier to the next keyword (WHERE).
        sql = self._render()
        import re

        assert re.search(
            r"FROM\s+fusion_catalog\.bronze\.ap_invoices\s+WHERE", sql
        ), "ap_invoices must be referenced unaliased in ap_aging.sql"
        # And specifically not re-aliased back to `ai` (the broken shape).
        assert not re.search(
            r"fusion_catalog\.bronze\.ap_invoices\s+(?:AS\s+)?ai\b", sql, re.IGNORECASE
        )

    def test_full_path_qualifier_is_in_scope(self) -> None:
        # The exact table identifier used to qualify the predicate must appear as
        # a FROM source (so Spark can resolve it) — the core invariant the alias
        # rewrite violated.
        sql = self._render()
        qualifier = "fusion_catalog.bronze.ap_invoices"
        assert f"{qualifier}.ApInvoicesCancelledDate" in sql
        assert f"FROM {qualifier}" in sql
