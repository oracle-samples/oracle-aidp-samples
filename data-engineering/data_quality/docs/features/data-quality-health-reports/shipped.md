---
shipped: 2026-04-01
---

# Shipped: Data Quality Health Reports

## Summary
Added a health report system that queries the system table to produce aggregate dashboards covering performance summary, check type distribution, worst offenders, and pass rate trends. Outputs as structured `HealthReport` dataclass and standalone HTML. Wired up the previously-stubbed CLI `report` command.

## Key Changes
- Added `read_health_data(days)` to storage protocol (SQLite, ExternalCatalog, Delta backends)
- Created `HealthReporter` class with `generate(days)` method producing `HealthReport` dataclass
- Created `generate_health_html()` for standalone HTML dashboard with summary cards, worst offenders table, check type distribution, and daily pass rate trend chart
- Added `health_report()` method to `Qualifire` API
- Implemented CLI `report` command: `qualifire report -c config.yaml -o health.html --days 30`

## Files Changed
- `qualifire/storage/base.py` — Protocol extension
- `qualifire/storage/sqlite_storage.py` — `read_health_data()` implementation
- `qualifire/storage/external_catalog.py` — `read_health_data()` implementation
- `qualifire/storage/delta_storage.py` — `read_health_data()` implementation
- `qualifire/reporting/health.py` — New: `HealthReporter` + `HealthReport`
- `qualifire/reporting/html_report.py` — `generate_health_html()`
- `qualifire/api.py` — `health_report()` method
- `qualifire/cli.py` — `report` command implementation
- `tests/test_reporting/test_health.py` — 10 new tests
- `tests/test_cli_run.py` — Updated CLI test

## Testing
- 419 total tests pass (10 new for health reports + 1 updated CLI test)
- Verified summary counts, pass rates, worst offenders ordering
- Verified check type distribution and daily trend computation
- Verified HTML output contains all expected sections
- Edge cases: empty system table, all-passing results

## Notes
- JDBC storage `read_health_data()` not yet implemented — deferred to follow-up
- Security review recommended adding `days` bounds validation and LIMIT clauses (non-blocking)
- Pre-existing Spark SQL injection in storage backends flagged but not introduced by this feature
