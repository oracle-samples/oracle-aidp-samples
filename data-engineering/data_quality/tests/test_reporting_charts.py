"""Tests for the public ``qualifire.reporting`` surface.

Covers the helpers added by the partition-tracking-and-dashboards
work: the backend-agnostic storage factory, the coerced
DataFrame loader, the QualifireResult reconstructor, and the new
aggregate dashboard plots.

These tests are deliberately light on visual assertions — matplotlib
/ Plotly figure objects are large and snapshotting them is brittle.
We assert the **shape** of the return values (Figure vs None,
file written, key fields present) and the failure modes of the
storage factory.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def seeded_storage(tmp_path):
    """Sqlite storage seeded with a 6-row mixed-severity history.

    Two datasets (``ds_a`` / ``ds_b``), three days, mix of PASS /
    WARNING / ERROR. Used by the aggregate-plot tests below — the
    plots refuse to draw without data, so this is the minimum
    realistic input.
    """
    from datetime import datetime, timedelta

    from qualifire.reporting import make_storage

    # Anchor timestamps to ``datetime.now()`` so the fixture stays
    # inside the ``read_health_data(days=30)`` window indefinitely.
    # Rows are 0..5 days old.
    now = datetime.now()
    db = tmp_path / "qf.db"
    storage = make_storage("sqlite", str(db))
    rows = [
        {
            "run_id": f"r{i}",
            "run_timestamp": (now - timedelta(days=5 - i)).isoformat(),
            "owner": "o", "bu": "b",
            "dataset_name": f"ds_{'ab'[i % 2]}",
            "table_name": "t",
            "metric_name": "row_count",
            "metric_value": 100.0 + i,
            "collection_type": "aggregation",
            "validation_name": "row_floor",
            "validation_type": "threshold",
            "validation_status": ["PASS", "WARNING", "ERROR",
                                  "PASS", "PASS", "PASS"][i],
            "validation_message": "ok",
            "notification_channel": None,
            "notification_status": None,
            "details_json": "{}",
            "record_type": "validation",
            "is_active": "true",
            "collector_name": None,
            "dimension_value": None,
            "collected_at": None,
            "validated_at": None,
            "notified_at": None,
            "partition_ts": (now - timedelta(days=5 - i)).date().isoformat(),
            "dataset_description": "demo dataset",
            "validation_description": "demo validation",
        }
        for i in range(6)
    ]
    storage.write_results(rows)
    return storage


# ---------------------------------------------------------------------------
# Storage factory
# ---------------------------------------------------------------------------


class TestMakeStorageFactory:
    def test_sqlite_roundtrip_returns_initialized_storage(self, tmp_path):
        from qualifire.reporting import make_storage
        from qualifire.storage.sqlite_storage import SQLiteStorage

        db = tmp_path / "qf.db"
        storage = make_storage("sqlite", str(db))
        assert isinstance(storage, SQLiteStorage)
        # initialize() should have created the schema; read should not raise.
        assert storage.read_health_data(days=1) == []

    def test_unknown_backend_raises_with_pointer(self, tmp_path):
        from qualifire.reporting import make_storage

        with pytest.raises(ValueError, match="Unknown system_table_backend"):
            make_storage("unknown", str(tmp_path / "x"))

    def test_jdbc_without_url_raises(self):
        from qualifire.reporting import make_storage

        with pytest.raises(ValueError, match="requires a jdbc connection"):
            make_storage("jdbc", "schema.table", jdbc_config={})


# ---------------------------------------------------------------------------
# load_health_dataframe — coercion contract
# ---------------------------------------------------------------------------


class TestLoadHealthDataframe:
    def test_returns_empty_df_when_no_rows(self, tmp_path):
        from qualifire.reporting import load_health_dataframe, make_storage

        storage = make_storage("sqlite", str(tmp_path / "qf.db"))
        df = load_health_dataframe(storage, days=30)
        assert df.empty

    def test_coerces_columns_from_seeded_rows(self, seeded_storage):
        import pandas as pd

        from qualifire.reporting import load_health_dataframe

        df = load_health_dataframe(seeded_storage, days=30)
        # Dtype contract — chart helpers depend on these.
        assert pd.api.types.is_datetime64_any_dtype(df["run_timestamp"])
        assert pd.api.types.is_datetime64_any_dtype(df["partition_ts"])
        assert pd.api.types.is_numeric_dtype(df["metric_value"])
        # severity is uppercase + has no missing values.
        assert (df["severity"] == df["severity"].str.upper()).all()
        assert set(df["severity"]).issubset({"PASS", "WARNING", "ERROR"})
        # day is a date object derived from run_timestamp.
        assert {type(v).__name__ for v in df["day"]} == {"date"}


# ---------------------------------------------------------------------------
# build_result_from_system_table
# ---------------------------------------------------------------------------


class TestBuildResultFromSystemTable:
    def test_picks_latest_run_when_run_id_omitted(self, seeded_storage):
        from qualifire.reporting import build_result_from_system_table

        qr = build_result_from_system_table(seeded_storage, days=30)
        # Pin the *property* (max run_timestamp), not the seeded
        # run_id literal — a future change to the SELECT ORDER BY or
        # to the seed naming should not break this test.
        rows = seeded_storage.read_health_data(days=30)
        latest_run_id = max(
            (r.get("run_id") for r in rows if r.get("run_id")),
            key=lambda rid: max(
                (r.get("run_timestamp") or "")
                for r in rows
                if r.get("run_id") == rid
            ),
        )
        assert qr.run_id == latest_run_id
        assert sum(len(d.validation_results) for d in qr.datasets) == 1

    def test_specific_run_id_returns_just_that_run(self, seeded_storage):
        from qualifire.reporting import build_result_from_system_table

        qr = build_result_from_system_table(seeded_storage, run_id="r2", days=30)
        assert qr.run_id == "r2"
        statuses = [
            vr.severity.value
            for d in qr.datasets for vr in d.validation_results
        ]
        assert statuses == ["ERROR"]

    def test_unknown_run_id_raises(self, seeded_storage):
        from qualifire.reporting import build_result_from_system_table

        with pytest.raises(ValueError, match="not found"):
            build_result_from_system_table(seeded_storage, run_id="missing", days=30)

    def test_owner_and_bu_propagate(self, seeded_storage):
        """``read_health_data`` returns ``owner`` / ``bu``; the
        reconstruction must surface them on the rebuilt
        ``QualifireResult`` so the static HTML report doesn't show
        blank Owner/BU."""
        from qualifire.reporting import build_result_from_system_table

        qr = build_result_from_system_table(seeded_storage, days=30)
        assert qr.owner == "o"
        assert qr.bu == "b"

    def test_empty_storage_raises_with_window_hint(self, tmp_path):
        from qualifire.reporting import build_result_from_system_table, make_storage

        storage = make_storage("sqlite", str(tmp_path / "qf.db"))
        with pytest.raises(ValueError, match="no rows in the last 30 days"):
            build_result_from_system_table(storage, days=30)


# ---------------------------------------------------------------------------
# Aggregate dashboard plots — shape only
# ---------------------------------------------------------------------------


class TestAggregateDashboardPlots:
    def test_executive_summary_returns_figure_and_writes_png(
        self, seeded_storage, tmp_path
    ):
        from qualifire.reporting import (
            load_health_dataframe,
            plot_executive_summary,
        )

        df = load_health_dataframe(seeded_storage, days=30)
        out = tmp_path / "charts"
        fig = plot_executive_summary(df, output_dir=out)
        assert fig is not None
        png = out / "executive_summary.png"
        # ≥ 5 KB is a sane floor for a real chart with cards + trend
        # line + worst-offenders bar; a blank figure would be far
        # smaller.
        assert png.exists()
        assert png.stat().st_size >= 5_000, (
            f"executive_summary.png is suspiciously small "
            f"({png.stat().st_size} bytes) — chart may be blank"
        )

    def test_executive_summary_interactive_returns_plotly_figure(
        self, seeded_storage
    ):
        from qualifire.reporting import (
            load_health_dataframe,
            plot_executive_summary_interactive,
        )

        df = load_health_dataframe(seeded_storage, days=30)
        fig = plot_executive_summary_interactive(df, days=30)
        assert fig is not None
        # Pie + Scatter pair, with non-empty payloads.
        traces_by_type = {t.type: t for t in fig.data}
        assert set(traces_by_type) == {"pie", "scatter"}
        pie = traces_by_type["pie"]
        scatter = traces_by_type["scatter"]
        # Pie sums to the row count (3 PASS + 1 WARNING + 1 ERROR + ...).
        assert sum(pie.values) == len(df)
        # Scatter has at least one daily point.
        assert len(scatter.x) >= 1
        assert len(scatter.y) == len(scatter.x)

    def test_dataset_day_heatmap_renders_with_single_dataset(
        self, seeded_storage, tmp_path
    ):
        from qualifire.reporting import (
            load_health_dataframe,
            plot_dataset_day_heatmap,
        )

        df = load_health_dataframe(seeded_storage, days=30)
        # Filter to one dataset to exercise the small-input path.
        df_one = df[df["dataset_name"] == "ds_a"].copy()
        fig = plot_dataset_day_heatmap(df_one, output_dir=tmp_path)
        assert fig is not None
        png = tmp_path / "dataset_day_heatmap.png"
        assert png.exists()
        assert png.stat().st_size >= 3_000

    def test_validation_history_interactive_picks_error_pair_by_default(
        self, seeded_storage
    ):
        from qualifire.reporting import (
            load_health_dataframe,
            plot_validation_history_interactive,
        )

        df = load_health_dataframe(seeded_storage, days=30)
        fig = plot_validation_history_interactive(df)
        assert fig is not None
        # The "ERROR" row is for the row_floor validation → title
        # contains it, and the figure carries at least one scatter
        # trace with non-empty x/y.
        assert "row_floor" in fig.layout.title.text
        assert len(fig.data) >= 1
        assert len(fig.data[0].x) >= 1
        assert len(fig.data[0].y) == len(fig.data[0].x)

    def test_severity_hierarchy_returns_sunburst_and_treemap(
        self, seeded_storage
    ):
        from qualifire.reporting import (
            load_health_dataframe,
            plot_severity_hierarchy,
        )

        df = load_health_dataframe(seeded_storage, days=30)
        sun, tree = plot_severity_hierarchy(df)
        assert sun is not None and tree is not None
        assert sun.data[0].type == "sunburst"
        assert tree.data[0].type == "treemap"
        # Both expose dataset/validation_type/severity hierarchy levels —
        # at least one entry per leaf, totalling the row count.
        assert sum(sun.data[0].values) >= len(seeded_storage.read_health_data(days=30))

    def test_severity_by_type_writes_png(self, seeded_storage, tmp_path):
        from qualifire.reporting import (
            load_health_dataframe,
            plot_severity_by_type,
        )

        df = load_health_dataframe(seeded_storage, days=30)
        fig = plot_severity_by_type(df, output_dir=tmp_path)
        assert fig is not None
        png = tmp_path / "severity_by_type.png"
        assert png.exists()
        assert png.stat().st_size >= 3_000

    def test_metric_distribution_by_severity_returns_violin(self, seeded_storage):
        from qualifire.reporting import (
            load_health_dataframe,
            plot_metric_distribution_by_severity,
        )

        df = load_health_dataframe(seeded_storage, days=30)
        fig = plot_metric_distribution_by_severity(df)
        assert fig is not None
        # Plotly violin traces have type "violin", and each one
        # carries at least one numeric point (a blank violin would
        # have an empty `y` array).
        assert all(t.type == "violin" for t in fig.data)
        assert any(len(t.y) >= 1 for t in fig.data), (
            "expected at least one violin trace with numeric data"
        )

    def test_validation_history_handles_missing_dimension_column(self):
        """Old persisted data via a custom storage might omit
        ``dimension_value`` from the read payload. The chart helper
        must not KeyError on such input — it falls back to a single
        ``"(no dimension)"`` group."""
        import pandas as pd

        from qualifire.reporting import plot_validation_history_interactive

        df = pd.DataFrame({
            "dataset_name": ["ds"] * 3,
            "validation_name": ["v"] * 3,
            "metric_value": [1.0, 2.0, 3.0],
            "severity": ["PASS"] * 3,
            "partition_ts": pd.to_datetime(["2026-05-01", "2026-05-02", "2026-05-03"]),
            "run_timestamp": pd.to_datetime(["2026-05-01", "2026-05-02", "2026-05-03"]),
            # NOTE: no `dimension_value` column.
        })
        fig = plot_validation_history_interactive(
            df, dataset_name="ds", validation_name="v"
        )
        assert fig is not None
        assert len(fig.data) >= 1

    @pytest.mark.parametrize("plot_name", [
        "plot_executive_summary",
        "plot_dataset_day_heatmap",
        "plot_severity_by_type",
    ])
    def test_matplotlib_plots_handle_empty_df(self, plot_name, tmp_path):
        """Empty input must not crash the chart helpers — they should
        return a Figure carrying a 'no data' message so callers can
        render unconditionally."""
        import pandas as pd
        from qualifire import reporting

        fig = getattr(reporting, plot_name)(pd.DataFrame())
        assert fig is not None
        # Matplotlib Figure has axes even on the no-data path.
        assert len(fig.axes) >= 1

    @pytest.mark.parametrize("plot_name", [
        "plot_executive_summary_interactive",
        "plot_validation_history_interactive",
        "plot_metric_distribution_by_severity",
    ])
    def test_plotly_plots_return_none_on_empty_df(self, plot_name):
        """Plotly helpers return ``None`` so callers can ``if fig:
        fig.show()`` cleanly without an empty-figure flash."""
        import pandas as pd
        from qualifire import reporting

        assert getattr(reporting, plot_name)(pd.DataFrame()) is None

    def test_severity_hierarchy_returns_pair_of_none_on_empty(self):
        import pandas as pd
        from qualifire.reporting import plot_severity_hierarchy

        assert plot_severity_hierarchy(pd.DataFrame()) == (None, None)


# ---------------------------------------------------------------------------
# read_health_data shape — backwards-compat regression for the run_id column
# ---------------------------------------------------------------------------


class TestReadHealthDataShape:
    """The dashboards added a dependency on ``run_id`` in the read
    payload (so ``build_result_from_system_table`` can group by run).
    Pin the column so we don't silently drop it from the SELECT."""

    def test_run_id_present_when_persisted(self, seeded_storage):
        rows = seeded_storage.read_health_data(days=30)
        assert rows
        assert all("run_id" in r for r in rows)
        assert {r["run_id"] for r in rows} == {f"r{i}" for i in range(6)}

    def test_partition_ts_present_in_payload(self, seeded_storage):
        rows = seeded_storage.read_health_data(days=30)
        assert all(r.get("partition_ts") for r in rows)


# ---------------------------------------------------------------------------
# Integration with HealthReporter — the existing public class still works
# against the wider read_health_data shape.
# ---------------------------------------------------------------------------


class TestHealthReporterAfterSchemaChange:
    def test_health_reporter_still_aggregates(self, seeded_storage):
        from qualifire.reporting import HealthReporter

        report = HealthReporter(seeded_storage).generate(days=30)
        assert report.total_checks == 6
        assert report.pass_count == 4
        assert report.warning_count == 1
        assert report.error_count == 1
