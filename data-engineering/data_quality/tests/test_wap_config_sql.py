"""Tests for the WAP config rename + ``sql_file`` field
(sub-feature B / Step 2).

Covers:
- ``WAPConfig.sql`` (renamed from ``write_sql``).
- ``WAPConfig.sql_file`` reads file at config-load time, populates
  ``_resolved_sql`` PrivateAttr.
- ``effective_sql`` property: returns ``_resolved_sql or sql``.
- Three-way mutual exclusion (``df`` × ``sql`` × ``sql_file``).
- File-read errors map to ``QualifireConfigError`` with chained
  cause; absolute / env-var / CWD path resolution policy.
- ``model_dump()`` preserves ``sql_file``, omits ``_resolved_sql``.
- Two configs with the same ``sql_file:`` field but different
  on-disk contents compare equal at the schema level (PrivateAttr).
- Round-trip rename: existing tests using ``write_sql=`` were
  migrated; this file exercises only the ``sql=`` / ``sql_file=``
  surface.
"""

from __future__ import annotations

import os

import pytest
from pydantic import ValidationError

from qualifire.core.config import WAPConfig
from qualifire.core.exceptions import QualifireConfigError


class TestSqlField:
    def test_sql_assignment_round_trip(self):
        c = WAPConfig(target_table="cat.t", sql="SELECT 1")
        assert c.sql == "SELECT 1"
        assert c.effective_sql == "SELECT 1"
        assert c._resolved_sql is None  # only sql_file populates this


class TestSqlFile:
    def test_reads_file_at_construction(self, tmp_path):
        p = tmp_path / "q.sql"
        p.write_text("SELECT a, b FROM raw.t", encoding="utf-8")
        c = WAPConfig(target_table="cat.t", sql_file=str(p))
        assert c._resolved_sql == "SELECT a, b FROM raw.t"
        assert c.effective_sql == "SELECT a, b FROM raw.t"

    def test_missing_file_raises_qualifire_config_error(self, tmp_path):
        p = tmp_path / "missing.sql"
        with pytest.raises(QualifireConfigError, match="not found"):
            WAPConfig(target_table="cat.t", sql_file=str(p))

    def test_non_utf8_file_raises(self, tmp_path):
        p = tmp_path / "bin.sql"
        p.write_bytes(b"\xff\xfeBAD")
        with pytest.raises(QualifireConfigError, match="not valid UTF-8"):
            WAPConfig(target_table="cat.t", sql_file=str(p))

    def test_absolute_path_used_as_is(self, tmp_path):
        p = tmp_path / "q.sql"
        p.write_text("SELECT 1", encoding="utf-8")
        c = WAPConfig(target_table="cat.t", sql_file=str(p))
        assert c.effective_sql == "SELECT 1"

    def test_env_var_resolves_relative_paths(self, tmp_path, monkeypatch):
        sub = tmp_path / "configs"
        sub.mkdir()
        (sub / "q.sql").write_text("SELECT 'env-var'", encoding="utf-8")
        monkeypatch.setenv("QUALIFIRE_CONFIG_BASE_DIR", str(sub))
        c = WAPConfig(target_table="cat.t", sql_file="q.sql")
        assert c.effective_sql == "SELECT 'env-var'"

    def test_relative_falls_back_to_cwd_when_env_unset(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("QUALIFIRE_CONFIG_BASE_DIR", raising=False)
        (tmp_path / "q.sql").write_text("SELECT 'cwd'", encoding="utf-8")
        c = WAPConfig(target_table="cat.t", sql_file="q.sql")
        assert c.effective_sql == "SELECT 'cwd'"


class TestThreeWayMutualExclusion:
    def test_df_and_sql_rejected(self):
        with pytest.raises(ValidationError, match="mutually exclusive"):
            WAPConfig(target_table="cat.t", sql="X", df=object())

    def test_df_and_sql_file_rejected(self, tmp_path):
        p = tmp_path / "q.sql"
        p.write_text("X", encoding="utf-8")
        with pytest.raises(ValidationError, match="mutually exclusive"):
            WAPConfig(
                target_table="cat.t", sql_file=str(p), df=object(),
            )

    def test_sql_and_sql_file_rejected(self, tmp_path):
        p = tmp_path / "q.sql"
        p.write_text("X", encoding="utf-8")
        with pytest.raises(ValidationError, match="mutually exclusive"):
            WAPConfig(target_table="cat.t", sql="Y", sql_file=str(p))

    def test_validate_assignment_catches_post_construction_violation(self):
        c = WAPConfig(target_table="cat.t", sql="SELECT 1")
        with pytest.raises(ValidationError, match="mutually exclusive"):
            c.df = object()


class TestSerialization:
    def test_model_dump_preserves_sql_file_omits_resolved(self, tmp_path):
        p = tmp_path / "q.sql"
        p.write_text("SELECT 1", encoding="utf-8")
        c = WAPConfig(target_table="cat.t", sql_file=str(p))
        d = c.model_dump()
        assert d["sql_file"] == str(p)
        assert "_resolved_sql" not in d

    def test_resolved_sql_reflects_disk_state_at_construction(
        self, tmp_path,
    ):
        """Two configs constructed against different on-disk states
        of the same path each capture the file content at their
        construction time. Pydantic's default ``__eq__`` includes
        the PrivateAttr, so they compare unequal — useful for
        operators who want to detect a config-load against stale
        files."""
        p = tmp_path / "q.sql"
        p.write_text("SELECT 1", encoding="utf-8")
        c1 = WAPConfig(target_table="cat.t", sql_file=str(p))
        p.write_text("SELECT 2", encoding="utf-8")
        c2 = WAPConfig(target_table="cat.t", sql_file=str(p))
        assert c1.effective_sql == "SELECT 1"
        assert c2.effective_sql == "SELECT 2"
        # model_dump reflects the schema-level fields only — those
        # are equal because both reference the same path.
        assert c1.model_dump() == c2.model_dump()
