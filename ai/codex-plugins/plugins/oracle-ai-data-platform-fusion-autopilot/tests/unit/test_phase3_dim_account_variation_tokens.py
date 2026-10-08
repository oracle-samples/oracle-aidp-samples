"""Regression-guard tests for the `dim_account` variation-point machinery.

The Phase 3 parity harness alone CANNOT detect a regression that
re-removes the `{{ column.coa_*_segment }}` tokens from
`dim_account.sql` — both the variation-token shape (substituting to
`CodeCombinationSegment1/2/3` on demo-pod) and the hardcoded
shape (literal `CodeCombinationSegment1/2/3`) render to byte-identical
output on the conventional COA. That ambiguity is exactly what
allowed the Phase 3 round-2 rollback to slip through. This module
adds two test cases that fail loudly on either regression path:

* **Test A — raw-text token-presence assertion.** Reads
  `dim_account.sql` from disk; asserts the three
  `{{ column.coa_*_segment }}` tokens are present on the
  role-aliased emit lines AND the three lines do NOT re-hardcode
  the v1 `CodeCombinationSegmentN AS <role>` literal.

* **Test B — alternate-profile render check.** Constructs an
  in-memory `TenantProfile` whose `resolved.column.coa_*_segment`
  maps the three roles to non-default source columns
  (`CodeCombinationSegment4/5/6`). Renders `dim_account.sql`
  through `sql_renderer.render_node_sql`; asserts the rendered SQL
  contains the non-default columns AND does NOT contain the
  conventional `Segment1/2/3 AS <role>` defaults. Proves the
  renderer's `{{ column.X }}` lookup follows the profile's resolved
  values rather than any hardcoded fallback.

Together these guard both regression vectors: re-hardcoded SQL
(Test A) and a broken renderer substitution (Test B).
"""

from __future__ import annotations

import re
import textwrap
from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_pack
from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_renderer import (
    RunContext,
    render_node_sql,
)
from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
    load_tenant_profile_from_string,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
DIM_ACCOUNT_SQL = (
    REPO_ROOT / "scripts" / "oracle_ai_data_platform_fusion_autopilot"
    / "content_packs" / "fusion-finance-starter" / "silver" / "dim_account.sql"
)
PACK_ROOT = (
    REPO_ROOT / "scripts" / "oracle_ai_data_platform_fusion_autopilot"
    / "content_packs" / "fusion-finance-starter"
)


# ---------------------------------------------------------------------------
# Test A — raw-text token presence (no Spark / no renderer)
# ---------------------------------------------------------------------------


class TestDimAccountVariationTokensPresent:
    """The three role-aliased emit lines (`AS company` / `AS cost_center`
    / `AS account`) MUST reference `{{ column.coa_*_segment }}` tokens,
    NOT literal `CodeCombinationSegmentN`. The positional emit
    (`segment_01..06`) and the CONCAT_WS code_combination block are
    allowed to reference `CodeCombinationSegmentN` literally — those
    are stable across tenants per Fusion's BICC schema."""

    def test_three_coa_segment_tokens_present(self) -> None:
        sql = DIM_ACCOUNT_SQL.read_text()
        for token in (
            "{{ coa.balancing }}",
            "{{ coa.cost_center }}",
            "{{ coa.natural_account }}",
        ):
            assert token in sql, (
                f"dim_account.sql is missing the {token!r} substitution. "
                f"This is the round-2 regression — the role-aliased COA "
                f"columns must be variation-resolved via the renderer, not "
                f"hardcoded. See `docs/v2-phase-3-variation-catalog.md` "
                f"\"Round-3 restore notes\"."
            )

    def test_role_aliased_lines_not_rehardcoded(self) -> None:
        """Catches any future edit that drops the tokens and pastes the
        v1 literal `CodeCombinationSegmentN AS <role>` form back in.

        The check matches `CodeCombinationSegment1 AS company` /
        `…2 AS cost_center` / `…3 AS account` with arbitrary intervening
        whitespace; it does NOT match the positional emit
        `CodeCombinationSegment1 AS segment_01` (the role alias is what
        we're guarding)."""
        sql = DIM_ACCOUNT_SQL.read_text()
        forbidden_patterns = [
            r"CodeCombinationSegment1\s+AS\s+company",
            r"CodeCombinationSegment2\s+AS\s+cost_center",
            r"CodeCombinationSegment3\s+AS\s+account\b",
        ]
        for pattern in forbidden_patterns:
            assert not re.search(pattern, sql), (
                f"dim_account.sql contains the v1 hardcoded form "
                f"matching {pattern!r}. This is the round-2 regression — "
                f"the three role-aliased COA emit lines must use "
                f"`{{{{ column.coa_*_segment }}}}` substitution, not "
                f"literal CodeCombinationSegmentN. See "
                f"`tests/unit/test_phase3_dim_account_variation_tokens.py` "
                f"docstring for the rationale."
            )

    def test_positional_emit_unchanged(self) -> None:
        """The positional `segment_01..06` emit is allowed to hardcode
        `CodeCombinationSegmentN`. Confirm those lines are still present
        so this regression guard doesn't accidentally fire on the
        positional shape."""
        sql = DIM_ACCOUNT_SQL.read_text()
        for n in range(1, 7):
            pattern = rf"CodeCombinationSegment{n}\s+AS\s+segment_0{n}"
            assert re.search(pattern, sql), (
                f"dim_account.sql is missing the positional emit "
                f"matching {pattern!r}; this guard test assumes the "
                f"positional `segment_01..06` columns are stable. If "
                f"the positional shape was intentionally removed, "
                f"update this test."
            )


# ---------------------------------------------------------------------------
# Test B — alternate-profile render proves substitution follows profile
# ---------------------------------------------------------------------------


_ALT_PROFILE_YAML = textwrap.dedent(
    """
    schemaVersion: 1
    tenant: alt-coa-positioning-test
    pinnedAt: 2026-06-06T00:00:00+00:00
    bronzeSchemaFingerprint: "sha256:alt-fixture"
    resolved:
      column:
        # Variation points referenced by other starter-pack templates.
        # dim_account itself only needs the three coa_*_segment keys
        # below, but the renderer validates the full required-keys set
        # at load time.
        supplier_natural_key: SEGMENT1
        vendor_id: VENDORID
        invoice_currency_code: ApInvoicesInvoiceCurrencyCode
      semantic:
        cancelled_status: cancelled_date
    profile:
      snapshotDate: '2026-06-06'
      # The deliberately non-conventional COA mapping — this is what
      # makes Test B load-bearing: rendered SQL MUST follow these
      # values via {{ coa.* }}, not v1's conventional Segment1/2/3.
      chartOfAccounts:
        default:
          balancingSegment: CodeCombinationSegment4
          costCenterSegment: CodeCombinationSegment5
          naturalAccountSegment: CodeCombinationSegment6
    """
).strip()


class TestDimAccountRendersFromResolvedProfile:
    """Render `dim_account.sql` against an in-memory `TenantProfile`
    whose `resolved.column.coa_*_segment` maps the three role aliases to
    the non-conventional `CodeCombinationSegment4/5/6`. The rendered
    SQL must follow the profile's resolved values. If anyone re-hardcodes
    the SQL or breaks the renderer's `{{ column.X }}` lookup, the
    rendered string will still contain `Segment1/2/3 AS company/…` —
    which this test rejects."""

    def _render(self) -> str:
        pack = load_pack(PACK_ROOT)
        profile = load_tenant_profile_from_string(_ALT_PROFILE_YAML)
        ctx = RunContext(
            catalog="cat",
            bronze_schema="bronze",
            silver_schema="silver",
            gold_schema="gold",
            run_id="alt-coa-positioning-test",
            active_profile_name="finance-default",
            bronze_table_for_source={
                "gl_coa": "cat.bronze.gl_coa",
            },
        )
        rendered = render_node_sql(pack.silver["dim_account"], pack, profile, ctx)
        return rendered.sql

    def test_rendered_sql_uses_resolved_non_default_columns(self) -> None:
        sql = self._render()
        for pattern in (
            r"CodeCombinationSegment4\s+AS\s+company",
            r"CodeCombinationSegment5\s+AS\s+cost_center",
            r"CodeCombinationSegment6\s+AS\s+account\b",
        ):
            assert re.search(pattern, sql), (
                f"Rendered dim_account.sql is missing the alternate-profile "
                f"substitution matching {pattern!r}. Either the renderer's "
                f"`{{{{ column.X }}}}` lookup is broken, or the SQL "
                f"re-hardcodes the v1 default columns regardless of the "
                f"resolved profile values.\n\nRendered SQL:\n{sql}"
            )

    def test_rendered_sql_does_not_carry_default_role_emits(self) -> None:
        sql = self._render()
        for pattern in (
            r"CodeCombinationSegment1\s+AS\s+company",
            r"CodeCombinationSegment2\s+AS\s+cost_center",
            r"CodeCombinationSegment3\s+AS\s+account\b",
        ):
            assert not re.search(pattern, sql), (
                f"Rendered dim_account.sql carries the v1 default "
                f"hardcoded form matching {pattern!r} despite the "
                f"alternate profile mapping the three roles to "
                f"`Segment4/5/6`. This indicates either a re-hardcoded "
                f"SQL or a renderer bug that ignores the profile's "
                f"resolved values.\n\nRendered SQL:\n{sql}"
            )

    def test_positional_segments_unaffected_by_profile(self) -> None:
        """The positional `segment_01..06` emit is hardcoded and MUST
        NOT vary by profile. Confirm the alternate profile leaves the
        positional shape untouched — `CodeCombinationSegment1` still
        emits as `segment_01`, etc."""
        sql = self._render()
        for n in range(1, 7):
            pattern = rf"CodeCombinationSegment{n}\s+AS\s+segment_0{n}"
            assert re.search(pattern, sql), (
                f"Rendered dim_account.sql is missing the positional "
                f"emit matching {pattern!r}. The positional shape "
                f"should NOT be affected by `resolved.column.coa_*_segment`."
            )
