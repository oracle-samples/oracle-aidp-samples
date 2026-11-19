"""Regression tests for the PR #4 maintainer review hardening.

These lock the security / correctness fixes applied during review so they
can't silently regress:

* SQL-identifier allowlisting of incremental key/column names (AIDPF-2082)
  and calendar dates (AIDPF-2083) at pack-load, plus the builder-level and
  MERGE-helper defense-in-depth checks.
* The resume identity-drift gate failing CLOSED (raising) on a corrupt plan
  snapshot rather than silently skipping the credential-endpoint safety check.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from oracle_ai_data_platform_fusion_autopilot.dimensions.dim_calendar import (
    build_dim_calendar_sql,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator import _natural_key_join_sql
from oracle_ai_data_platform_fusion_autopilot.orchestrator.errors import (
    ResumeRunNotResumableError,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.resume import (
    check_identity_drift,
)
from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import (
    AIDPF_2082_INVALID_SQL_IDENTIFIER,
    AIDPF_2083_INVALID_CALENDAR_DATE,
    CalendarProfile,
    IncrementalWatermark,
    RefreshIncremental,
)


def _err_text(exc: ValidationError) -> str:
    return " ".join(str(e.get("msg", "")) for e in exc.errors())


# ---------------------------------------------------------------------------
# Schema-layer identifier allowlisting (AIDPF-2082)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad",
    ["1=1 OR x", "id) WHEN MATCHED THEN DELETE --", "col-with-hyphen", "a.b", ""],
)
def test_refresh_incremental_rejects_bad_natural_key(bad: str) -> None:
    with pytest.raises(ValidationError) as ei:
        RefreshIncremental(strategy="merge", naturalKey=[bad])
    assert AIDPF_2082_INVALID_SQL_IDENTIFIER in _err_text(ei.value)


def test_refresh_incremental_rejects_bad_partition_and_tracked_columns() -> None:
    with pytest.raises(ValidationError) as ei:
        RefreshIncremental(strategy="merge", partitionColumns=["bad-col"])
    assert AIDPF_2082_INVALID_SQL_IDENTIFIER in _err_text(ei.value)
    with pytest.raises(ValidationError) as ei:
        RefreshIncremental(strategy="scd2", trackedColumns=["bad;col"])
    assert AIDPF_2082_INVALID_SQL_IDENTIFIER in _err_text(ei.value)


def test_incremental_watermark_rejects_bad_column() -> None:
    with pytest.raises(ValidationError) as ei:
        IncrementalWatermark(source="src", column="ts; DROP TABLE x")
    assert AIDPF_2082_INVALID_SQL_IDENTIFIER in _err_text(ei.value)


def test_refresh_incremental_accepts_clean_identifiers() -> None:
    # Real shipped values must still validate (no false positives).
    spec = RefreshIncremental(
        strategy="merge", naturalKey=["ApInvoicesInvoiceId", "SEGMENT1", "_extract_ts"]
    )
    assert spec.natural_key == ["ApInvoicesInvoiceId", "SEGMENT1", "_extract_ts"]


# ---------------------------------------------------------------------------
# Calendar-date validation (AIDPF-2083)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad", ["2020-13-40", "not-a-date", "2020-01-01'; DROP", "20200101"]
)
def test_calendar_profile_rejects_bad_dates(bad: str) -> None:
    with pytest.raises(ValidationError) as ei:
        CalendarProfile(startDate=bad, endDate="2030-12-31")
    assert AIDPF_2083_INVALID_CALENDAR_DATE in _err_text(ei.value)


def test_calendar_profile_accepts_iso_dates() -> None:
    prof = CalendarProfile(startDate="2020-01-01", endDate="2030-12-31")
    assert prof.start_date == "2020-01-01"


# ---------------------------------------------------------------------------
# Builder-level defense (tenant-override path bypasses Pydantic)
# ---------------------------------------------------------------------------


def test_dim_calendar_builder_rejects_injection_date() -> None:
    with pytest.raises(ValueError):
        build_dim_calendar_sql(
            start_date="2020-01-01', DATE'2020-01-02', INTERVAL 1 DAY)) AS d --",
            end_date="2030-12-31",
        )


def test_dim_calendar_builder_accepts_iso_dates() -> None:
    sql = build_dim_calendar_sql(start_date="2020-01-01", end_date="2030-12-31")
    assert "sequence(DATE'2020-01-01', DATE'2030-12-31'" in sql


# ---------------------------------------------------------------------------
# MERGE join helper defense-in-depth
# ---------------------------------------------------------------------------


def test_natural_key_join_sql_rejects_unsafe_column() -> None:
    with pytest.raises(ValueError):
        _natural_key_join_sql(["id) WHEN MATCHED THEN DELETE"])


def test_natural_key_join_sql_accepts_clean_columns() -> None:
    out = _natural_key_join_sql(("a", "b"))
    assert out == "target.a <=> src.a AND target.b <=> src.b"


# ---------------------------------------------------------------------------
# Resume identity-drift gate fails CLOSED on a corrupt snapshot
# ---------------------------------------------------------------------------


def test_check_identity_drift_raises_on_corrupt_snapshot() -> None:
    # A non-JSON snapshot must raise (non-resumable), NOT silently return and
    # skip the identity-drift gate that runs before credential unwrap.
    with pytest.raises(ResumeRunNotResumableError):
        check_identity_drift(
            "{ this is not valid json",
            bundle=None,
            paths=None,
            plugin_version="0.1.0",
            run_id="run-1",
        )
