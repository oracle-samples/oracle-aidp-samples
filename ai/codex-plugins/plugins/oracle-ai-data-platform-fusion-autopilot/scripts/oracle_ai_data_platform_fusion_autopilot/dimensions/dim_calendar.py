"""silver.dim_calendar — system-generated calendar dimension.

Unlike :mod:`.dim_supplier`, this dim has **no bronze source** — every column
is derived from a date sequence. Generates Gregorian + Fiscal calendars for
the configured range; default 2020-01-01 → 2030-12-31 (4,018 days).

Required by `gold.gl_balance` and `gold.po_backlog`.

Design notes
------------

* **Surrogate key strategy** — `calendar_key = YYYYMMDD as BIGINT`. This is
  *deterministic from the date*, unlike `dim_supplier`'s
  `monotonically_increasing_id()`. Stable across rebuilds; downstream marts
  can safely cache joins.

* **Fiscal calendar** — configurable via ``fiscal_start_month`` (1..12).
  Default 1 (calendar year = fiscal year). Common Fusion conventions:
  ``7`` (Jul-Jun: FY26 spans Jul 2025 - Jun 2026), ``10`` (Oct-Sep: US federal).
  Fiscal year is named by the calendar year **the fiscal year ends in** when
  ``fiscal_start_month > 1`` (so Jul 2025 → FY 2026 with ``fiscal_start_month=7``).

* **No source watermark** — the dim is fully regenerated each build. Cheap
  (~few thousand rows for a decade) and avoids drift.

* **Date functions used** — `sequence(DATE, DATE, INTERVAL 1 DAY)` for the
  range, `date_format(..., 'yyyyMMdd')` for the surrogate, `YEAR/MONTH/DAY/
  QUARTER/DAYOFWEEK/WEEKOFYEAR` for parts. Spark's `DAYOFWEEK` returns 1=Sun
  through 7=Sat; we treat 1 and 7 as `is_weekend = TRUE`.
"""

from __future__ import annotations

import re
from datetime import date
from typing import TYPE_CHECKING, Final

from oracle_ai_data_platform_fusion_autopilot.config.paths import DEFAULT_PATHS, TablePaths

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession


TARGET_SILVER_TABLE: Final[str] = DEFAULT_PATHS.silver("dim_calendar")
DEFAULT_START_DATE: Final[str]  = "2020-01-01"
DEFAULT_END_DATE:   Final[str]  = "2030-12-31"
DEFAULT_FISCAL_START_MONTH: Final[int] = 1   # calendar year = fiscal year

_ISO_DATE_RE: Final[re.Pattern[str]] = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _validate_calendar_date(field_name: str, value: str) -> None:
    """Reject any value that isn't a real ISO-8601 (YYYY-MM-DD) date.

    ``start_date`` / ``end_date`` interpolate into ``sequence(DATE'...')``;
    this guards the tenant-override path that bypasses the Pydantic
    CalendarProfile validator. Mirrors AIDPF-2083 at the SQL-builder layer.
    """
    if not isinstance(value, str) or not _ISO_DATE_RE.match(value):
        raise ValueError(
            f"{field_name}={value!r} is not an ISO-8601 date (YYYY-MM-DD)."
        )
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"{field_name}={value!r} is not a valid calendar date ({exc})."
        ) from exc


def _run_id_audit_sql(run_id: str | None) -> str:
    """SQL fragment for the silver_run_id audit column."""
    if run_id is None:
        return "NULL"
    escaped = run_id.replace("'", "''")
    return f"'{escaped}'"


def build_dim_calendar_sql(
    *,
    paths:      TablePaths | None = None,
    start_date: str = DEFAULT_START_DATE,
    end_date:   str = DEFAULT_END_DATE,
    fiscal_start_month: int = DEFAULT_FISCAL_START_MONTH,
    silver_table: str | None = None,
    run_id:     str | None = None,
) -> str:
    """Return the CREATE-OR-REPLACE Delta SQL that produces ``silver.dim_calendar``.

    Args:
        start_date: ISO ``YYYY-MM-DD`` string (inclusive).
        end_date:   ISO ``YYYY-MM-DD`` string (inclusive).
        fiscal_start_month: 1..12 — the calendar month that opens each fiscal
            year. ``1`` = calendar = fiscal (default). ``7`` = Jul-Jun.
        silver_table: 3-part target table name.

    Raises:
        ValueError: if ``fiscal_start_month`` is not in [1..12].
    """
    if paths is None:
        paths = DEFAULT_PATHS
    if silver_table is None:
        silver_table = paths.silver("dim_calendar")
    if not (1 <= fiscal_start_month <= 12):
        raise ValueError(
            f"fiscal_start_month must be in [1, 12]; got {fiscal_start_month}"
        )
    # start_date / end_date interpolate into `sequence(DATE'...')` below. The
    # pack-load CalendarProfile validator (AIDPF-2083) covers the typed path,
    # but the tenant-override adapter reads the un-modelled profile dict and
    # passes raw strings here — so validate at the builder too (defense in
    # depth). Reject anything that isn't a real ISO-8601 (YYYY-MM-DD) date.
    _validate_calendar_date("start_date", start_date)
    _validate_calendar_date("end_date", end_date)

    # The +1 to fiscal_year (when fiscal_start_month > 1) names the FY by its
    # ending calendar year — Fusion convention. For fiscal_start_month == 1
    # (calendar = fiscal), the "+1" is a no-op since the WHEN branch never fires
    # under standard SQL semantics. We still emit the same expression for
    # consistency; the math holds.
    fy_advance = 0 if fiscal_start_month == 1 else 1
    run_id_sql = _run_id_audit_sql(run_id)

    return f"""\
CREATE OR REPLACE TABLE {silver_table}
USING DELTA
AS
WITH dates AS (
  SELECT EXPLODE(
    sequence(DATE'{start_date}', DATE'{end_date}', INTERVAL 1 DAY)
  ) AS calendar_date
)
SELECT
  CAST(date_format(calendar_date, 'yyyyMMdd') AS BIGINT)            AS calendar_key,
  calendar_date                                                     AS calendar_date,
  YEAR(calendar_date)                                               AS year,
  QUARTER(calendar_date)                                            AS quarter,
  MONTH(calendar_date)                                              AS month,
  date_format(calendar_date, 'MMMM')                                AS month_name,
  DAY(calendar_date)                                                AS day_of_month,
  DAYOFWEEK(calendar_date)                                          AS day_of_week,
  date_format(calendar_date, 'EEEE')                                AS day_name,
  WEEKOFYEAR(calendar_date)                                         AS week_of_year,
  CASE WHEN DAYOFWEEK(calendar_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
  -- Fiscal year: named by the calendar year the FY ends in (when fiscal_start > 1)
  CASE
    WHEN MONTH(calendar_date) >= {fiscal_start_month}
    THEN YEAR(calendar_date) + {fy_advance}
    ELSE YEAR(calendar_date)
  END                                                               AS fiscal_year,
  -- Fiscal period 1..12 (period 1 = fiscal_start_month)
  CASE
    WHEN MONTH(calendar_date) >= {fiscal_start_month}
    THEN MONTH(calendar_date) - {fiscal_start_month} + 1
    ELSE MONTH(calendar_date) + 12 - {fiscal_start_month} + 1
  END                                                               AS fiscal_period,
  -- Fiscal quarter from the fiscal period
  CAST(
    CEIL(
      (CASE
         WHEN MONTH(calendar_date) >= {fiscal_start_month}
         THEN MONTH(calendar_date) - {fiscal_start_month} + 1
         ELSE MONTH(calendar_date) + 12 - {fiscal_start_month} + 1
       END) / 3.0
    ) AS INT
  )                                                                 AS fiscal_quarter,
  current_timestamp()                                               AS silver_built_at,
  {run_id_sql}                                                      AS silver_run_id
FROM dates
"""


def build(
    spark: SparkSession,
    *,
    paths:      TablePaths | None = None,
    start_date: str = DEFAULT_START_DATE,
    end_date:   str = DEFAULT_END_DATE,
    fiscal_start_month: int = DEFAULT_FISCAL_START_MONTH,
    silver_table: str | None = None,
    run_id:     str | None = None,
) -> DataFrame:
    """Materialize ``silver.dim_calendar``; returns a DataFrame backed by it.

    ``paths`` (defaults to ``DEFAULT_PATHS``) resolves the silver table
    identifier from the tenant's ``bundle.yaml.aidp.*`` config. Explicit
    ``silver_table=`` wins over ``paths``. ``run_id`` threads the
    orchestrator's run identifier into the ``silver_run_id`` audit column.
    """
    if paths is None:
        paths = DEFAULT_PATHS
    if silver_table is None:
        silver_table = paths.silver("dim_calendar")
    sql = build_dim_calendar_sql(
        start_date=start_date,
        end_date=end_date,
        fiscal_start_month=fiscal_start_month,
        silver_table=silver_table,
        run_id=run_id,
    )
    spark.sql(sql)
    return spark.table(silver_table)


__all__ = [
    "DEFAULT_END_DATE",
    "DEFAULT_FISCAL_START_MONTH",
    "DEFAULT_START_DATE",
    "TARGET_SILVER_TABLE",
    "build",
    "build_dim_calendar_sql",
]
