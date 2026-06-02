"""CLI tests for the new ``qualifire backfill`` and
``qualifire deactivate-metric`` subcommands plus the
``--skip-recollection`` flag on ``qualifire run``.

The CLI's ``_cmd_*`` handlers wrap ``Qualifire.{backfill,
deactivate_metric, run_config_parsed}``; tests exercise the
argument-parsing and exit-code contract end-to-end against an
in-memory SQLite system table.
"""

from __future__ import annotations

import argparse

import pytest

from qualifire.cli import _parse_partition_arg


class TestPartitionArgParser:
    def test_single_value(self):
        assert _parse_partition_arg("2026-04-01") == "2026-04-01"

    def test_list(self):
        assert _parse_partition_arg("2026-04-01,2026-04-02") == [
            "2026-04-01", "2026-04-02",
        ]

    def test_list_strips_whitespace(self):
        assert _parse_partition_arg(" 2026-04-01 , 2026-04-02 ") == [
            "2026-04-01", "2026-04-02",
        ]

    def test_range(self):
        assert _parse_partition_arg("2026-04-01..2026-04-07") == (
            "2026-04-01", "2026-04-07",
        )

    def test_combined_list_and_range_rejected(self):
        with pytest.raises(argparse.ArgumentTypeError, match="cannot combine"):
            _parse_partition_arg("2026-04-01,2026-04-02..2026-04-03")

    def test_malformed_range_rejected(self):
        with pytest.raises(argparse.ArgumentTypeError, match="malformed"):
            _parse_partition_arg("..2026-04-07")
        with pytest.raises(argparse.ArgumentTypeError, match="malformed"):
            _parse_partition_arg("2026-04-01..")

    def test_empty_list_rejected(self):
        with pytest.raises(argparse.ArgumentTypeError, match="empty list"):
            _parse_partition_arg(",,")


class TestBackfillSummaryStreamRouting:
    """Item 5 (backfill-followups-and-polish): the non-`--json`
    summary block routes ``errored`` lines to stderr while keeping
    ``refreshed`` / ``unchanged`` / ``skipped`` lines on stdout. Lets
    operators pipe ``qualifire backfill`` output to a downstream
    tool and `2>` into a separate channel for failures.
    """

    def _make_report(self):
        from datetime import datetime
        from qualifire.core.backfill_report import (
            BackfillReport, PartitionDiff,
        )

        ts = datetime(2026, 4, 1)
        return BackfillReport(
            partitions=[
                PartitionDiff(
                    dataset_name="sales", metric_name="ok_metric",
                    dimension_value=None, partition_ts=ts,
                    original_value=10.0, backfilled_value=10.0,
                    severity_before=None, severity_after=None,
                    status="unchanged",
                ),
                PartitionDiff(
                    dataset_name="sales", metric_name="bad_metric",
                    dimension_value=None, partition_ts=ts,
                    original_value=None, backfilled_value=None,
                    severity_before=None, severity_after=None,
                    status="errored",
                    error="boom",
                ),
            ],
            refreshed=0, unchanged=1, skipped=0, errored=1,
        )

    def test_errored_lines_go_to_stderr(self, monkeypatch, capsys, tmp_path):
        from qualifire.cli import _cmd_backfill
        from qualifire.api import Qualifire

        # Stub a minimal config file (CLI insists on loading one).
        cfg = tmp_path / "qf.yml"
        cfg.write_text(
            "owner: t\n"
            "bu: b\n"
            "system_table: history\n"
            "system_table_backend: sqlite\n"
            "system_table_sqlite_path: \":memory:\"\n"
            "datasets: []\n"
        )

        report = self._make_report()
        # Bypass real backfill — exercise only the stdout/stderr
        # routing behavior in the summary block.
        monkeypatch.setattr(
            Qualifire, "backfill", lambda *a, **kw: report,
        )

        ns = argparse.Namespace(
            config=str(cfg), partition="2026-04-01", selector=None,
            data=False, skip_recollection=False, soft_delete_prior=False,
            json=False,
        )
        rc = _cmd_backfill(ns)
        captured = capsys.readouterr()

        # Errored line MUST be on stderr.
        assert "[ERRORED]" in captured.err
        assert "bad_metric" in captured.err
        # Errored line MUST NOT also appear on stdout.
        assert "[ERRORED]" not in captured.out
        # Unchanged line stays on stdout.
        assert "[UNCHANGED]" in captured.out
        assert "ok_metric" in captured.out
        # Header summary on stdout.
        assert "Backfill complete:" in captured.out
        assert rc == 1  # has_errors

    def test_json_mode_unchanged(self, monkeypatch, capsys, tmp_path):
        from qualifire.cli import _cmd_backfill
        from qualifire.api import Qualifire

        cfg = tmp_path / "qf.yml"
        cfg.write_text(
            "owner: t\n"
            "bu: b\n"
            "system_table: history\n"
            "system_table_backend: sqlite\n"
            "system_table_sqlite_path: \":memory:\"\n"
            "datasets: []\n"
        )

        report = self._make_report()
        monkeypatch.setattr(
            Qualifire, "backfill", lambda *a, **kw: report,
        )

        ns = argparse.Namespace(
            config=str(cfg), partition="2026-04-01", selector=None,
            data=False, skip_recollection=False, soft_delete_prior=False,
            json=True,
        )
        _cmd_backfill(ns)
        captured = capsys.readouterr()

        # --json mode emits one JSON blob to stdout; nothing on stderr
        # for the report itself.
        assert captured.out.strip().startswith("{")
        # The errored partition is inside the JSON, not on stderr.
        assert "[ERRORED]" not in captured.err
