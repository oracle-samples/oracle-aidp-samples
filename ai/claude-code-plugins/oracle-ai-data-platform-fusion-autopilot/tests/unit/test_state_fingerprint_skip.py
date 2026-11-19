"""Tests for :func:`state.write_fingerprint_skip_row` (Phase 3c).

The helper writes a Phase 3c ``--force-fingerprint-skip`` audit row.
Tests cover:

* INSERT is issued with the expected column values.
* SQL-identifier safety: single-quote escaping per the existing
  ``state.py`` pattern (no SQL-injection surface).
* Fingerprint truncation: 24 chars + ``...``.
* Reserved sentinel ``dataset_id="__fingerprint_skip__"`` so the resume reader
  and bootstrap watermark query exclude it (reserved ``__*__`` namespace).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_autopilot.config.paths import TablePaths
from oracle_ai_data_platform_fusion_autopilot.orchestrator import state


def _mock_paths() -> TablePaths:
    """A minimal TablePaths fixture for tests that just need the
    fully-qualified state-table path string."""
    return TablePaths(catalog="cat", bronze_schema="bronze", silver_schema="silver", gold_schema="gold")


class TestWriteFingerprintSkipRow:
    def test_insert_carries_expected_columns(self) -> None:
        spark = MagicMock(name="spark")
        paths = _mock_paths()
        state.write_fingerprint_skip_row(
            spark,
            paths,
            run_id="cp-test-abc",
            prior_fingerprint="sha256:" + "a" * 64,
            current_fingerprint="sha256:" + "b" * 64,
        )
        spark.sql.assert_called_once()
        sql = spark.sql.call_args[0][0]
        # 3-part path used.
        assert "cat.bronze.fusion_autopilot_state" in sql
        # Sentinel + mode + status.
        assert "'__fingerprint_skip__'" in sql
        assert "'fingerprint_skip'" in sql
        assert "'success'" in sql
        # Run id.
        assert "'cp-test-abc'" in sql
        # NULL columns are explicitly cast.
        assert "CAST(NULL AS TIMESTAMP)" in sql
        assert "CAST(NULL AS STRING)" in sql

    def test_skip_reason_carries_truncated_fingerprints(self) -> None:
        spark = MagicMock(name="spark")
        prior = "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
        current = "sha256:fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"
        state.write_fingerprint_skip_row(
            spark, _mock_paths(),
            run_id="r1",
            prior_fingerprint=prior,
            current_fingerprint=current,
        )
        sql = spark.sql.call_args[0][0]
        # Exactly 24-char truncation + ellipsis.
        assert f"prior={prior[:24]}..." in sql
        assert f"current={current[:24]}..." in sql

    def test_single_quote_escaped_run_id(self) -> None:
        """Defensive — run_id should never have a single quote (the
        cp-prefix format is timestamp + uuid hex), but the helper
        MUST still escape if one slipped through to prevent SQL
        injection in the INSERT statement."""
        spark = MagicMock(name="spark")
        state.write_fingerprint_skip_row(
            spark, _mock_paths(),
            run_id="cp-test'; DROP TABLE x;--",
            prior_fingerprint="p",
            current_fingerprint="c",
        )
        sql = spark.sql.call_args[0][0]
        # Doubled single quotes (Delta's escape style).
        assert "cp-test''; DROP TABLE x;--" in sql
        # No raw "; DROP TABLE — the doubled escape contains it.
        assert "'cp-test'; DROP TABLE x;--'" not in sql

    def test_sentinel_dataset_id_distinct_from_real_datasets(self) -> None:
        """Bootstrap's ``read_last_watermark`` and the resume reader filter out
        the reserved ``__fingerprint_skip__`` sentinel; it must never collide
        with a real dataset. Confirm the literal we emit is in the reserved
        ``__*__`` namespace."""
        spark = MagicMock(name="spark")
        state.write_fingerprint_skip_row(
            spark, _mock_paths(),
            run_id="r",
            prior_fingerprint="p",
            current_fingerprint="c",
        )
        sql = spark.sql.call_args[0][0]
        # Reserved __*__ namespace is the discriminator.
        assert "'__fingerprint_skip__'" in sql
        assert "'_fingerprint_skip'" not in sql.replace("'__fingerprint_skip__'", "")
