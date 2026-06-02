"""Tests for health report generation."""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest

from qualifire.reporting.health import HealthReport, HealthReporter
from qualifire.reporting.html_report import generate_health_html
from qualifire.storage.base import SYSTEM_TABLE_COLUMNS
from qualifire.storage.sqlite_storage import SQLiteStorage


def _seed_storage(rows: list[dict]) -> SQLiteStorage:
    """Create an in-memory SQLite storage and seed it with rows."""
    storage = SQLiteStorage(db_path=":memory:")
    storage.initialize()
    if rows:
        full_rows = []
        for r in rows:
            row = {col: None for col in SYSTEM_TABLE_COLUMNS}
            row.update(r)
            row["record_type"] = "validation"
            full_rows.append(row)
        storage.write_results(full_rows)
    return storage


_ROW_COUNTER = [0]


def _make_row(dataset: str, vtype: str, status: str, days_ago: int = 0) -> dict:
    ts = (datetime.now() - timedelta(days=days_ago)).isoformat()
    # Plan H1/H2: read_health_data dedups per
    # (dataset, validation_name, metric_name, partition_ts, dim).
    # Give each seeded row a unique partition_ts so the test's
    # raw-count assertions still hold (was implicit pre-H1).
    _ROW_COUNTER[0] += 1
    return {
        "run_id": "run-1",
        "run_timestamp": ts,
        "dataset_name": dataset,
        "validation_type": vtype,
        "validation_status": status,
        "validation_name": f"{vtype}.{dataset}",
        "partition_ts": f"{ts}-{_ROW_COUNTER[0]}",
    }


class TestHealthReporter:
    def test_summary_counts(self):
        storage = _seed_storage([
            _make_row("ds1", "threshold", "PASS"),
            _make_row("ds1", "threshold", "WARNING"),
            _make_row("ds2", "slo", "ERROR"),
            _make_row("ds2", "slo", "PASS"),
        ])
        report = HealthReporter(storage).generate(days=7)

        assert report.total_checks == 4
        assert report.pass_count == 2
        assert report.warning_count == 1
        assert report.error_count == 1
        assert report.pass_rate == 50.0

    def test_internal_error_count_segregates_qualifire_internal_failures(self):
        """Phase 2 of external-catalog-system-table-hardening:
        rows tagged with ``details_json['qualifire_internal_failure']
        =True`` are counted in ``error_count`` for backward compat
        but ALSO surface in a separate ``internal_error_count`` so
        dashboards can render them with a distinct visual treatment.
        """
        # Two ERRORs: one real data finding, one qualifire-internal.
        rows = [
            {
                "run_id": "r1",
                "run_timestamp": datetime.now().isoformat(),
                "dataset_name": "ds1",
                "validation_type": "threshold",
                "validation_status": "ERROR",
                "validation_name": "threshold.ds1",
                "partition_ts": datetime.now().isoformat() + "-real",
                # No internal-failure marker — genuine data-quality finding.
                "details_json": json.dumps({"actual_value": 42}),
            },
            {
                "run_id": "r1",
                "run_timestamp": datetime.now().isoformat(),
                "dataset_name": "ds1",
                "validation_type": "engine",
                "validation_status": "ERROR",
                "validation_name": "qualifire.persistence",
                "partition_ts": datetime.now().isoformat() + "-internal",
                # Marker present — qualifire-internal failure.
                "details_json": json.dumps({
                    "qualifire_internal_failure": True,
                    "error": "system-table outage",
                }),
            },
        ]
        full_rows = [
            {col: None for col in SYSTEM_TABLE_COLUMNS} | r | {"record_type": "validation"}
            for r in rows
        ]
        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()
        storage.write_results(full_rows)

        report = HealthReporter(storage).generate(days=7)

        # Both rows count toward error_count (back-compat).
        assert report.error_count == 2
        # Only one is the qualifire-internal subset.
        assert report.internal_error_count == 1

    def test_internal_error_count_handles_string_details_json(self):
        """Round-7 codex review RISK fix: details_json arrives as a
        JSON STRING after re-read from the persisted system table
        (SQLite stores it as TEXT). The predicate must JSON-decode
        and still classify correctly."""
        rows = [{
            "run_id": "r1",
            "run_timestamp": datetime.now().isoformat(),
            "dataset_name": "ds1",
            "validation_type": "engine",
            "validation_status": "ERROR",
            "validation_name": "qualifire.persistence",
            "partition_ts": datetime.now().isoformat() + "-string-payload",
            # String form — what SQLite TEXT column actually stores.
            "details_json": '{"qualifire_internal_failure": true}',
        }]
        full_rows = [
            {col: None for col in SYSTEM_TABLE_COLUMNS} | r | {"record_type": "validation"}
            for r in rows
        ]
        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()
        storage.write_results(full_rows)

        report = HealthReporter(storage).generate(days=7)
        assert report.internal_error_count == 1

    def test_worst_offenders_sorted_by_errors(self):
        storage = _seed_storage([
            _make_row("bad_ds", "threshold", "ERROR"),
            _make_row("bad_ds", "slo", "ERROR"),
            _make_row("ok_ds", "threshold", "PASS"),
            _make_row("ok_ds", "slo", "WARNING"),
        ])
        report = HealthReporter(storage).generate(days=7)

        assert len(report.worst_offenders) == 1
        assert report.worst_offenders[0]["dataset"] == "bad_ds"
        assert report.worst_offenders[0]["error"] == 2

    def test_check_type_distribution(self):
        storage = _seed_storage([
            _make_row("ds1", "threshold", "PASS"),
            _make_row("ds1", "threshold", "PASS"),
            _make_row("ds1", "slo", "ERROR"),
        ])
        report = HealthReporter(storage).generate(days=7)

        by_type = {t["type"]: t for t in report.by_check_type}
        assert "threshold" in by_type
        assert by_type["threshold"]["pass_rate"] == 100.0
        assert "slo" in by_type
        assert by_type["slo"]["pass_rate"] == 0.0

    def test_daily_trend(self):
        storage = _seed_storage([
            _make_row("ds1", "threshold", "PASS", days_ago=1),
            _make_row("ds1", "threshold", "PASS", days_ago=1),
            _make_row("ds1", "threshold", "ERROR", days_ago=0),
        ])
        report = HealthReporter(storage).generate(days=7)

        assert len(report.trend) >= 1
        # Each trend entry has date, pass_rate, total
        for t in report.trend:
            assert "date" in t
            assert "pass_rate" in t
            assert "total" in t

    def test_empty_system_table(self):
        storage = _seed_storage([])
        report = HealthReporter(storage).generate(days=7)

        assert report.total_checks == 0
        assert report.pass_rate == 0.0
        assert report.worst_offenders == []
        assert report.by_check_type == []
        assert report.trend == []

    def test_all_passing(self):
        storage = _seed_storage([
            _make_row("ds1", "threshold", "PASS"),
            _make_row("ds2", "slo", "PASS"),
        ])
        report = HealthReporter(storage).generate(days=7)

        assert report.pass_rate == 100.0
        assert report.error_count == 0
        assert report.worst_offenders == []

    def test_by_dataset_ordering(self):
        storage = _seed_storage([
            _make_row("alpha", "threshold", "PASS"),
            _make_row("beta", "threshold", "ERROR"),
            _make_row("beta", "slo", "ERROR"),
            _make_row("gamma", "threshold", "ERROR"),
        ])
        report = HealthReporter(storage).generate(days=7)

        # Sorted by error count descending
        datasets = [d["dataset"] for d in report.by_dataset]
        assert datasets[0] == "beta"  # 2 errors
        assert datasets[1] == "gamma"  # 1 error


class TestHealthHtml:
    def test_html_contains_sections(self):
        storage = _seed_storage([
            _make_row("ds1", "threshold", "PASS"),
            _make_row("ds2", "slo", "ERROR"),
        ])
        report = HealthReporter(storage).generate(days=7)
        html = generate_health_html(report)

        assert "Health Report" in html
        assert "Total Checks" in html
        assert "Pass Rate" in html
        assert "Worst Offenders" in html
        assert "Check Type Distribution" in html
        assert "Daily Pass Rate Trend" in html

    def test_html_writes_to_file(self, tmp_path):
        storage = _seed_storage([_make_row("ds1", "threshold", "PASS")])
        report = HealthReporter(storage).generate(days=7)
        out = tmp_path / "report.html"
        generate_health_html(report, output_path=str(out))

        assert out.exists()
        content = out.read_text()
        assert "Health Report" in content

    def test_empty_report_html(self):
        report = HealthReport()
        html = generate_health_html(report)

        assert "Total Checks" in html
        assert "No errors in this period" in html
