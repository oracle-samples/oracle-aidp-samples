---
started: 2026-04-01
---

# Implementation Plan: Data Quality Health Reports

## Overview

Add a health report system that queries the system table to produce aggregate dashboards covering: performance summary (pass/warn/error counts), check type distribution, worst offenders (most-failing datasets), and pass rate trends over time. Output as structured data and standalone HTML.

## Implementation Steps

- [x] Step 1: Add `read_health_data()` method to storage protocol and backends
  - Add `read_health_data(days: int = 30) -> list[dict]` to `SystemTableStorage` protocol in `storage/base.py`
  - Implement in `SQLiteStorage` — query recent validation rows within date range
  - Implement in `ExternalCatalogStorage` — same via Spark SQL
  - Implement in `DeltaStorage` — same via Spark SQL
  - Returns: list of dicts with `dataset_name`, `validation_type`, `validation_status`, `run_timestamp`, `validation_name`

- [x] Step 2: Create `reporting/health.py` with `HealthReporter` class
  - `__init__(storage)` — takes a storage instance
  - `generate(days: int = 30) -> HealthReport` — queries data and computes all metrics
  - `HealthReport` dataclass with:
    - `summary`: total runs, pass/warning/error counts and percentages
    - `by_dataset`: per-dataset pass/warning/error counts, sorted by error count desc
    - `by_check_type`: per-validation-type counts and pass rates
    - `trend`: daily pass rate over the date range (list of date + pass_rate)
    - `worst_offenders`: top 10 datasets by error count

- [x] Step 3: Create `generate_health_html()` in `reporting/html_report.py`
  - Renders a `HealthReport` as a standalone HTML page with:
    - Summary cards (total checks, pass rate, warning rate, error rate)
    - Worst offenders table
    - Check type distribution table
    - Daily pass rate trend (inline SVG or CSS bar chart)
  - Reuses existing styling patterns from `generate_html_report()`

- [x] Step 4: Add `health_report()` method to `Qualifire` API
  - `health_report(days: int = 30, output_path: str | None = None) -> HealthReport`
  - Queries storage, generates report, optionally writes HTML

- [x] Step 5: Wire up CLI `report` command
  - Implement the stubbed `report` command in `cli.py`
  - `qualifire report --config config.yaml --output health.html --days 30`

- [x] Step 6: Write tests
  - `HealthReporter` with SQLite storage: summary counts, worst offenders, check type dist, trend
  - HTML generation: produces valid HTML with expected sections
  - API method: returns HealthReport
  - Edge cases: empty system table, single run, all passing

- [x] Step 7: Update docs
  - Add health reports section to `docs/configuration.md` or create `docs/health_reports.md`

## Technical Decisions

1. **Storage-level read**: A single `read_health_data(days)` method returns raw validation rows. All aggregation happens in Python in `HealthReporter`. This avoids writing complex aggregate SQL across 4 backends.

2. **`HealthReport` dataclass**: Structured output enables both programmatic access and HTML rendering. Users can consume the data however they want.

3. **HTML-only visualization**: No matplotlib dependency for health reports. Uses CSS-based charts and tables. Keeps it lightweight and portable.

4. **CLI integration**: The existing stub in `cli.py` becomes functional.

## Files to Modify

| File | Change |
|------|--------|
| `qualifire/storage/base.py` | Add `read_health_data()` to protocol |
| `qualifire/storage/sqlite_storage.py` | Implement `read_health_data()` |
| `qualifire/storage/external_catalog.py` | Implement `read_health_data()` |
| `qualifire/storage/delta_storage.py` | Implement `read_health_data()` |
| `qualifire/reporting/health.py` | New: `HealthReporter` + `HealthReport` |
| `qualifire/reporting/html_report.py` | Add `generate_health_html()` |
| `qualifire/api.py` | Add `health_report()` method |
| `qualifire/cli.py` | Implement `report` command |
| `tests/test_reporting/test_health.py` | New: health report tests |

## Testing Strategy

- Seed an in-memory SQLite system table with synthetic validation rows spanning 7 days
- Verify summary counts, pass rates, worst offenders ordering
- Verify HTML output contains expected sections
- Edge case: empty table returns zeroed-out report

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Large system tables slow down `read_health_data()` | Date filter limits scope; `days` parameter controls range |
| JDBC storage not implemented | Defer JDBC to follow-up; document as known gap |
