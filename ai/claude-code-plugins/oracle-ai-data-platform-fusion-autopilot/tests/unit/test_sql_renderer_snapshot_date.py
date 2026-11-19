"""Unit tests for the ``{{ snapshot_date }}`` renderer token (Phase 3 Step 2).

The token differs from ``{{ profile.<key> }}`` in three important ways:

* Absent / empty profile value emits the literal SQL expression
  ``CURRENT_DATE()`` (no parameter binding). This is the only place in
  the renderer where a token expands to raw SQL.
* Present value MUST be an ISO-8601 date string; binds as
  ``:snapshot_date`` parameter.
* Anything else (non-string, malformed date, embedded SQL) raises
  :class:`InvalidSnapshotDateError` (AIDPF-5013).

The contrast with ``{{ profile.snapshotDate }}`` (which would force the
value to bind as a parameter, breaking the literal-fallback need) is
documented in the variation_catalog.md.
"""

from __future__ import annotations

import pathlib
import textwrap

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_pack
from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_renderer import (
    AIDPF_5013_INVALID_SNAPSHOT_DATE,
    InvalidSnapshotDateError,
    RunContext,
    render_node_sql,
)
from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
    load_tenant_profile_from_string,
)


PACK_YAML = """
id: phase3-snapshot-test
version: 1.0.0
description: Snapshot-date renderer token test pack
compatibility:
  pluginMinVersion: 0.3.0
"""

NODE_YAML = """
id: ap_aging
layer: gold
implementation:
  type: sql
  sql: gold/ap_aging.sql
target: ap_aging
outputSchema:
  columns:
    - name: as_of_date
      type: date
      nullable: false
      pii: none
refresh:
  seed:
    strategy: replace
"""

# Minimal template — the snapshot_date token in isolation lets us assert
# its rendered form precisely.
SQL_TEMPLATE = "SELECT {{ snapshot_date }} AS as_of_date FROM dual\n"


def _build_pack(tmp_path: pathlib.Path) -> "object":
    pack_root = tmp_path / "pack"
    pack_root.mkdir(parents=True, exist_ok=True)
    (pack_root / "pack.yaml").write_text(PACK_YAML, encoding="utf-8")
    gold = pack_root / "gold"
    gold.mkdir()
    (gold / "ap_aging.yaml").write_text(NODE_YAML, encoding="utf-8")
    (gold / "ap_aging.sql").write_text(SQL_TEMPLATE, encoding="utf-8")
    return load_pack(pack_root)


_BASE_PROFILE_LINES = [
    'schemaVersion: 1',
    'tenant: snapshot-date-test',
    'pinnedAt: 2026-06-05T00:00:00+00:00',
    'bronzeSchemaFingerprint: "sha256:placeholder"',
]


def _profile_with(**profile_block) -> "object":
    """Build a TenantProfile with the given ``profile:`` block contents.

    Values are emitted via ``repr()`` to keep YAML quoting reliable across
    strings, ints, and ``None``-becomes-``~`` cases. Indentation is
    hand-controlled (no ``textwrap.dedent``) so f-string interpolation
    doesn't accidentally break the YAML layout.
    """
    lines = list(_BASE_PROFILE_LINES)
    if profile_block:
        lines.append("profile:")
        for k, v in profile_block.items():
            lines.append(f"  {k}: {v!r}")
    yaml_text = "\n".join(lines) + "\n"
    return load_tenant_profile_from_string(yaml_text)


def _ctx() -> RunContext:
    return RunContext(
        catalog="cat",
        bronze_schema="bronze",
        silver_schema="silver",
        gold_schema="gold",
        run_id="snapshot-test-run",
        active_profile_name="finance-default",
    )


def _node(pack):
    return pack.gold["ap_aging"]


# ---------------------------------------------------------------------------
# Absent value → literal CURRENT_DATE()
# ---------------------------------------------------------------------------


class TestSnapshotDateAbsent:
    def test_no_profile_block_renders_current_date(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        profile = _profile_with()  # no profile: block
        rendered = render_node_sql(_node(pack), pack, profile, _ctx())
        assert "CURRENT_DATE()" in rendered.sql
        assert "snapshot_date" not in rendered.params

    def test_explicit_null_renders_current_date(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        # Use ~ (YAML null) — load_tenant_profile_from_string parses it as None.
        yaml_text = textwrap.dedent(
            """
            schemaVersion: 1
            tenant: snapshot-date-test
            pinnedAt: 2026-06-05T00:00:00+00:00
            bronzeSchemaFingerprint: "sha256:placeholder"
            profile:
              snapshotDate: ~
            """
        ).strip()
        profile = load_tenant_profile_from_string(yaml_text)
        rendered = render_node_sql(_node(pack), pack, profile, _ctx())
        assert "CURRENT_DATE()" in rendered.sql
        assert "snapshot_date" not in rendered.params

    def test_empty_string_renders_current_date(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        profile = _profile_with(snapshotDate="")
        rendered = render_node_sql(_node(pack), pack, profile, _ctx())
        assert "CURRENT_DATE()" in rendered.sql
        assert "snapshot_date" not in rendered.params

    def test_whitespace_string_renders_current_date(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        profile = _profile_with(snapshotDate="   ")
        rendered = render_node_sql(_node(pack), pack, profile, _ctx())
        assert "CURRENT_DATE()" in rendered.sql
        assert "snapshot_date" not in rendered.params


# ---------------------------------------------------------------------------
# Present + valid → bound :snapshot_date
# ---------------------------------------------------------------------------


class TestSnapshotDatePresent:
    def test_iso_date_binds_as_parameter(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        profile = _profile_with(snapshotDate="2026-06-05")
        rendered = render_node_sql(_node(pack), pack, profile, _ctx())
        assert ":snapshot_date" in rendered.sql
        assert "CURRENT_DATE()" not in rendered.sql
        assert rendered.params["snapshot_date"] == "2026-06-05"


# ---------------------------------------------------------------------------
# Invalid values → AIDPF-5013
# ---------------------------------------------------------------------------


class TestSnapshotDateInvalid:
    def test_slash_separated_date_rejected(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        profile = _profile_with(snapshotDate="2026/06/05")
        with pytest.raises(InvalidSnapshotDateError) as exc_info:
            render_node_sql(_node(pack), pack, profile, _ctx())
        assert AIDPF_5013_INVALID_SNAPSHOT_DATE in str(exc_info.value)

    def test_sql_injection_attempt_rejected(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        profile = _profile_with(snapshotDate="CURRENT_DATE()")
        with pytest.raises(InvalidSnapshotDateError) as exc_info:
            render_node_sql(_node(pack), pack, profile, _ctx())
        # The rejection message names the offending value so debugging is fast.
        assert "CURRENT_DATE()" in str(exc_info.value)

    def test_integer_value_rejected(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        profile = _profile_with(snapshotDate=20260605)
        with pytest.raises(InvalidSnapshotDateError):
            render_node_sql(_node(pack), pack, profile, _ctx())

    def test_impossible_date_round_trip_rejected(self, tmp_path: pathlib.Path) -> None:
        """Matches the regex shape but isn't a real calendar date."""
        pack = _build_pack(tmp_path)
        profile = _profile_with(snapshotDate="2026-13-05")  # month 13
        with pytest.raises(InvalidSnapshotDateError) as exc_info:
            render_node_sql(_node(pack), pack, profile, _ctx())
        assert "2026-13-05" in str(exc_info.value)

    def test_appended_sql_rejected(self, tmp_path: pathlib.Path) -> None:
        """ISO date followed by an SQL fragment — must not match the regex."""
        pack = _build_pack(tmp_path)
        profile = _profile_with(snapshotDate="2026-06-05'; DROP TABLE x --")
        with pytest.raises(InvalidSnapshotDateError):
            render_node_sql(_node(pack), pack, profile, _ctx())
