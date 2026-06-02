"""Tests for the Qualifire programmatic API and builder methods."""

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from tests.conftest import MockDataFrame
from qualifire.api import Qualifire
from qualifire.core.config import (
    AnomalyDetectionValidationConfig,
    ForecastValidationConfig,
    HistoricalValidationConfig,
    SLOValidationConfig,
    ThresholdValidationConfig,
)
from qualifire.core.exceptions import QualifireValidationError
from qualifire.core.models import Severity


def _make_qf(agg_return=None):
    """Create a Qualifire instance with a mocked Spark backend."""
    backend = MagicMock()
    backend.spark = MagicMock()
    backend.execute_sql.return_value = MockDataFrame(
        [{"max_ts": datetime(2024, 6, 15, 10, 0, 0)}]
    )
    backend.get_aggregations.return_value = agg_return or {"row_count": 5000}
    backend.sample_records.return_value = MockDataFrame([{"a": 1}])
    return Qualifire(
        backend=backend,
        system_table=":memory:",
        system_table_backend="sqlite",
        owner="test",
        bu="test",
    )


class TestBuilderMethods:
    def test_slo_check_returns_config(self):
        cfg = Qualifire.slo_check(column="ts", warning="PT4H", error="PT8H")
        assert isinstance(cfg, SLOValidationConfig)
        assert cfg.recency.strategy == "max_column"
        assert cfg.recency.column == "ts"
        assert cfg.thresholds == {"warning": "PT4H", "error": "PT8H"}

    def test_slo_check_custom_sql(self):
        cfg = Qualifire.slo_check(strategy="custom_sql", sql="SELECT MAX(ts) FROM t")
        assert cfg.recency.strategy == "custom_sql"
        assert cfg.recency.sql == "SELECT MAX(ts) FROM t"

    def test_threshold_check_returns_config(self):
        cfg = Qualifire.threshold_check(
            aggregations={"cnt": "COUNT(*)"},
            rules=[{"metric": "cnt", "thresholds": {"error": {"min": 100}}}],
        )
        assert isinstance(cfg, ThresholdValidationConfig)
        assert len(cfg.rules) == 1
        assert cfg.rules[0].metric == "cnt"

    def test_threshold_check_metrics_shorthand(self):
        """``metrics=[...]`` is the passthrough shorthand for the
        ``validate_query`` flow where the inner SELECT has already
        aggregated. Each name expands to ``MAX(<col>) AS <col>`` —
        a no-op on a single-row query but keeps the collector
        contract uniform.
        """
        cfg = Qualifire.threshold_check(
            metrics=["row_count", "null_pid_pct"],
            rules=[
                {"metric": "row_count",   "thresholds": {"error": {"min": 100}}},
                {"metric": "null_pid_pct","thresholds": {"error": {"max": 1}}},
            ],
        )
        assert isinstance(cfg, ThresholdValidationConfig)
        # Sub-feature A: dict form. Keys are the metric names; values
        # the SQL expressions (here ``MAX(<name>)`` shorthand wraps
        # the name as a passthrough aggregation).
        assert cfg.collection.expressions == {
            "row_count": "MAX(row_count)",
            "null_pid_pct": "MAX(null_pid_pct)",
        }

    def test_threshold_check_rejects_both_metrics_and_aggregations(self):
        """The two parameters describe the same collection step; mixing
        them is ambiguous so the builder rejects it loudly rather than
        silently picking one."""
        import pytest
        with pytest.raises(ValueError, match="not both"):
            Qualifire.threshold_check(
                aggregations={"a": "COUNT(*)"},
                metrics=["b"],
                rules=[{"metric": "a", "thresholds": {"error": {"min": 1}}}],
            )

    def test_drift_check_metrics_shorthand(self):
        cfg = Qualifire.drift_check(
            metrics=["avg_amount"],
            rules=[{
                "metric": "avg_amount",
                "compare": {"past_values": 3, "step": "P1D"},
                "thresholds": {"error": {"deviation_pct": {"min": -25, "max": 25}}},
            }],
        )
        assert cfg.collection.expressions == {"avg_amount": "MAX(avg_amount)"}

    def test_threshold_check_auto_derives_metrics_from_rules(self):
        """When neither ``aggregations`` nor ``metrics`` is given, the
        builder reads the metric names off the rules. The common
        ``validate_query`` pattern (already-aggregated source query,
        rules referencing source columns) drops the redundant
        ``metrics=[...]`` listing entirely."""
        cfg = Qualifire.threshold_check(
            rules=[
                {"metric": "row_count",   "thresholds": {"error": {"min": 100}}},
                {"metric": "null_pid_pct","thresholds": {"error": {"max": 0.5}}},
            ],
        )
        assert cfg.collection.expressions == {
            "row_count": "MAX(row_count)",
            "null_pid_pct": "MAX(null_pid_pct)",
        }

    def test_drift_check_auto_derives_metrics_from_rules(self):
        """Same single-source-of-truth — ``rules`` carries the metric
        names, so a separate ``metrics=[...]`` listing is redundant."""
        cfg = Qualifire.drift_check(
            rules=[{
                "metric": "avg_amount",
                "compare": {"past_values": 3, "step": "P1D"},
                "thresholds": {"error": {"deviation_pct": {"min": -25, "max": 25}}},
            }],
        )
        assert cfg.collection.expressions == {"avg_amount": "MAX(avg_amount)"}

    def test_trend_check_defaults_aggregation_to_max(self):
        """Same simplification — when ``aggregation`` is omitted the
        builder defaults to ``MAX(<metric>)``, which is a no-op on a
        single-row aggregate query (the typical ``validate_query``
        shape). Explicit aggregations (e.g. ``"AVG(amount)"``) still
        work for raw fact-table sources."""
        cfg = Qualifire.trend_check(metric="avg_amount", step="P1D")
        assert cfg.collection.expressions == {"avg_amount": "MAX(avg_amount)"}
        # Explicit aggregation still wins.
        cfg2 = Qualifire.trend_check(
            metric="m", aggregation="AVG(amount)", step="P1D",
        )
        assert cfg2.collection.expressions == {"m": "AVG(amount)"}

    def test_drift_check_returns_config(self):
        cfg = Qualifire.drift_check(
            aggregations={"avg": "AVG(x)"},
            rules=[{
                "metric": "avg",
                "compare": {"past_values": 3, "step": "P7D"},
                "thresholds": {"warning": {"deviation_pct": {"min": -20, "max": 20}}},
            }],
        )
        assert isinstance(cfg, HistoricalValidationConfig)
        assert cfg.rules[0].compare.past_values == 3

    def test_trend_check_returns_config(self):
        cfg = Qualifire.trend_check(
            metric="m", aggregation="AVG(x)", history_count=60, step="P1D"
        )
        assert isinstance(cfg, ForecastValidationConfig)
        assert cfg.rules[0].metric == "m"
        assert cfg.rules[0].model.history_count == 60

    def test_trend_check_omitted_step_rejected_by_pydantic(self):
        """``step`` is required for forecast — anchors the
        partition-anchored history read at
        ``cr.partition_ts − k·step``. Pydantic rejects a builder
        call that omits ``step`` at config-construction time so
        the misconfiguration is loud."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="step"):
            Qualifire.trend_check(metric="m", aggregation="AVG(x)")


    def test_trend_check_explicit_step_passed_through(self):
        """Opt-in works: callers who *do* want bucketing can still
        pass a duration string and get it back unchanged."""
        cfg = Qualifire.trend_check(
            metric="m", aggregation="AVG(x)", step="P7D"
        )
        assert cfg.rules[0].model.step == "P7D"

    def test_shape_check_returns_config(self):
        cfg = Qualifire.shape_check(n_records=5000, past_dates=2, step="P14D")
        assert isinstance(cfg, AnomalyDetectionValidationConfig)
        assert cfg.collection.n_records == 5000
        assert cfg.collection.history.past_dates == 2

    def test_builder_with_notify(self):
        cfg = Qualifire.slo_check(
            column="ts", warning="PT2H",
            notify={"warning": ["email"], "error": ["email", "slack"]},
        )
        assert cfg.notify.warning == ["email"]
        assert cfg.notify.error == ["email", "slack"]


class TestValidateMethod:
    def test_validate_pass(self):
        qf = _make_qf(agg_return={"row_count": 5000})
        result = qf.validate(
            table="db.t",
            validations=[
                Qualifire.threshold_check(
                    aggregations={"row_count": "COUNT(*)"},
                    rules=[{"metric": "row_count", "thresholds": {"error": {"min": 100}}}],
                ),
            ],
        )
        assert result.overall_severity == Severity.PASS
        assert result.datasets[0].dataset_name == "db.t"

    def test_validate_error_raises(self):
        qf = _make_qf(agg_return={"row_count": 5})
        with pytest.raises(QualifireValidationError):
            qf.validate(
                table="db.t",
                validations=[
                    Qualifire.threshold_check(
                        aggregations={"row_count": "COUNT(*)"},
                        rules=[{"metric": "row_count", "thresholds": {"error": {"min": 100}}}],
                    ),
                ],
            )

    def test_validate_custom_name(self):
        qf = _make_qf()
        result = qf.validate(
            table="db.t",
            name="my_check",
            validations=[
                Qualifire.threshold_check(
                    aggregations={"row_count": "COUNT(*)"},
                    rules=[{"metric": "row_count", "thresholds": {}}],
                ),
            ],
        )
        assert result.datasets[0].dataset_name == "my_check"

    def test_validate_with_context(self):
        qf = _make_qf()
        result = qf.validate(
            table="db.t",
            filter_expr="date = '{{ ds }}'",
            validations=[
                Qualifire.threshold_check(
                    aggregations={"row_count": "COUNT(*)"},
                    rules=[{"metric": "row_count", "thresholds": {}}],
                ),
            ],
            context={"ds": "2024-06-15"},
        )
        assert result.overall_severity == Severity.PASS


class TestInitStorage:
    def test_sqlite_storage_init(self):
        backend = MagicMock()
        backend.spark = MagicMock()
        qf = Qualifire(
            backend=backend,
            system_table=":memory:",
            system_table_backend="sqlite",
            owner="o", bu="b",
        )
        assert qf._storage is not None

    def test_no_storage_when_table_none(self):
        backend = MagicMock()
        qf = Qualifire(backend=backend, owner="o", bu="b")
        assert qf._storage is None

    def test_unknown_storage_backend_raises(self):
        backend = MagicMock()
        backend.spark = MagicMock()
        with pytest.raises(ValueError, match="Unknown system_table_backend"):
            Qualifire(
                backend=backend,
                system_table="t",
                system_table_backend="unknown",
                owner="o", bu="b",
            )


class TestNotifierRegistration:
    def test_register_notifier(self):
        qf = _make_qf()
        mock_notifier = MagicMock()
        mock_notifier.channel_name = "test"
        qf.register_notifier("test", mock_notifier)
        assert "test" in qf._notifiers

    def test_default_console_notifier_auto_registered(self):
        """``Qualifire.__init__`` must auto-register a ``console``
        notifier so YAML configs can route via ``notify: ["console"]``
        without an explicit ``notifications.console:`` block.

        This is the contract that lets notebook workflows drop the
        old programmatic-``inbox`` boilerplate.
        """
        from qualifire.notification.console_notifier import ConsoleNotifier

        qf = _make_qf()
        assert "console" in qf._notifiers
        assert isinstance(qf._notifiers["console"], ConsoleNotifier)

    def test_caller_supplied_console_wins_over_default(self):
        """If the caller passes their own ``console`` instance via
        ``notifiers={...}``, the default must not overwrite it —
        programmatic always wins."""
        backend = MagicMock()
        backend.spark = MagicMock()
        custom_console = MagicMock()
        custom_console.channel_name = "console"
        qf = Qualifire(
            backend=backend, owner="o", bu="b",
            notifiers={"console": custom_console},
        )
        assert qf._notifiers["console"] is custom_console

    def test_notifiers_via_init(self):
        backend = MagicMock()
        backend.spark = MagicMock()
        mock_notifier = MagicMock()
        qf = Qualifire(
            backend=backend, owner="o", bu="b",
            notifiers={"slack": mock_notifier},
        )
        assert "slack" in qf._notifiers

    def test_notifiers_passed_to_engine(self):
        """Registered notifiers are invoked when notify param is set on a validation."""
        mock_notifier = MagicMock()
        mock_notifier.send.return_value = MagicMock(
            channel="slack", severity=Severity.WARNING, status="sent", message=""
        )

        backend = MagicMock()
        backend.spark = MagicMock()
        backend.get_aggregations.return_value = {"cnt": 50}

        qf = Qualifire(
            backend=backend, owner="o", bu="b",
            notifiers={"slack": mock_notifier},
        )
        result = qf.validate(
            table="t",
            validations=[
                Qualifire.threshold_check(
                    aggregations={"cnt": "COUNT(*)"},
                    rules=[{"metric": "cnt", "thresholds": {"warning": {"min": 100}}}],
                    notify={"warning": ["slack"]},
                ),
            ],
        )
        mock_notifier.send.assert_called()
        assert result.has_warnings


class TestValidateQueryMethod:
    def test_validate_query_pass(self):
        """validate_query() runs validations against a SQL query result."""
        backend = MagicMock()
        backend.spark = MagicMock()
        backend.execute_sql.return_value = MockDataFrame(
            [{"total": 5000}]
        )
        backend.get_aggregations.return_value = {"total": 5000}
        # NB: pre-findings-sweep this simulated a Spark backend; engine now uses Backend Protocol unconditionally.

        qf = Qualifire(
            backend=backend,
            system_table=":memory:",
            system_table_backend="sqlite",
            owner="test", bu="test",
        )
        result = qf.validate_query(
            query="SELECT SUM(amount) AS total FROM orders",
            name="order_totals",
            validations=[
                Qualifire.threshold_check(
                    aggregations={"total": "SUM(total)"},
                    rules=[{"metric": "total", "thresholds": {"error": {"min": 100}}}],
                ),
            ],
        )
        assert result.overall_severity == Severity.PASS
        assert result.datasets[0].dataset_name == "order_totals"

    def test_validate_query_with_partition_step_and_ts(self):
        """Plan H3: validate_query(partition_ts=..., partition_step=...)
        constructs and runs without tripping the H3 invariant. Closes
        Codex impl-R1 finding on validate_query() missing partition_step
        kwarg.
        """
        backend = MagicMock()
        backend.spark = MagicMock()
        backend.execute_sql.return_value = MockDataFrame([{"total": 5000}])
        backend.get_aggregations.return_value = {"total": 5000}

        qf = Qualifire(
            backend=backend,
            system_table=":memory:",
            system_table_backend="sqlite",
            owner="test", bu="test",
        )
        result = qf.validate_query(
            query="SELECT SUM(amount) AS total FROM orders",
            name="order_totals",
            partition_ts="'2026-05-08'",
            partition_step="P1D",
            validations=[
                Qualifire.threshold_check(
                    aggregations={"total": "SUM(total)"},
                    rules=[{"metric": "total", "thresholds": {"error": {"min": 100}}}],
                ),
            ],
        )
        assert result.overall_severity == Severity.PASS

    def test_validate_query_default_name(self):
        """validate_query() defaults dataset name to 'custom_query'."""
        backend = MagicMock()
        backend.spark = MagicMock()
        backend.execute_sql.return_value = MockDataFrame([{"val": 1}])
        backend.get_aggregations.return_value = {"val": 1}
        # NB: pre-findings-sweep this simulated a Spark backend; engine now uses Backend Protocol unconditionally.

        qf = Qualifire(
            backend=backend,
            system_table=":memory:",
            system_table_backend="sqlite",
            owner="o", bu="b",
        )
        result = qf.validate_query(
            query="SELECT 1 AS val",
            validations=[
                Qualifire.threshold_check(
                    aggregations={"val": "SUM(val)"},
                    rules=[{"metric": "val", "thresholds": {}}],
                ),
            ],
        )
        assert result.datasets[0].dataset_name == "custom_query"

    def test_validate_query_with_dimensions_and_measures(self):
        """validate_query() passes dimensions/measures for schema validation."""
        backend = MagicMock()
        backend.spark = MagicMock()
        backend.execute_sql.return_value = MockDataFrame(
            [{"region": "US", "total": 500}]
        )
        backend.get_aggregations.return_value = {"total": 500}
        # NB: pre-findings-sweep this simulated a Spark backend; engine now uses Backend Protocol unconditionally.

        qf = Qualifire(
            backend=backend,
            system_table=":memory:",
            system_table_backend="sqlite",
            owner="o", bu="b",
        )
        result = qf.validate_query(
            query="SELECT region, SUM(amount) AS total FROM orders GROUP BY region",
            dimensions=["region"],
            measures=["total"],
            validations=[
                Qualifire.threshold_check(
                    aggregations={"total": "SUM(total)"},
                    rules=[{"metric": "total", "thresholds": {}}],
                ),
            ],
        )
        assert result.overall_severity == Severity.PASS
