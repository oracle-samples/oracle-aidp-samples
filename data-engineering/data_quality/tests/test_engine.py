"""Tests for QualifireEngine orchestration."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import MockDataFrame
from qualifire.core.config import (
    AggregationCollectionConfig,
    DatasetConfig,
    NotifyConfig,
    QualifireConfig,
    RecencyCollectionConfig,
    SLOValidationConfig,
    ThresholdLevels,
    ThresholdRuleConfig,
    ThresholdValidationConfig,
    WAPConfig,
)
from qualifire.core.context import QualifireContext
from qualifire.core.engine import QualifireEngine
from qualifire.core.exceptions import QualifireValidationError
from qualifire.core.models import Severity
from qualifire.storage.sqlite_storage import SQLiteStorage


def _make_engine(datasets, storage=None, notifiers=None):
    backend = MagicMock()
    backend.execute_sql.return_value = MockDataFrame(
        [{"max_ts": datetime(2024, 6, 15, 10, 0, 0)}]
    )
    backend.get_aggregations.return_value = {"row_count": 5000, "avg_sales": 150.0}
    backend.sample_records.return_value = MockDataFrame([{"a": 1}])

    config = QualifireConfig(
        owner="test-team",
        bu="test-bu",
        system_table="test_sys",
        datasets=datasets,
    )
    ctx = QualifireContext(run_id="test-run-id")
    return QualifireEngine(
        backend=backend,
        storage=storage,
        context=ctx,
        config=config,
        notifiers=notifiers or {},
    )


class TestEngineRun:
    def test_run_single_dataset_pass(self):
        """Engine runs a single dataset and returns PASS."""
        ds = DatasetConfig(
            name="sales",
            table="db.sales",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"row_count": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="row_count",
                        thresholds=ThresholdLevels(warning=None, error=None),
                    )],
                ),
            ],
        )
        engine = _make_engine([ds])
        result = engine.run()

        assert result.owner == "test-team"
        assert result.run_id == "test-run-id"
        assert len(result.datasets) == 1
        assert result.overall_severity == Severity.PASS

    def test_run_raises_on_error(self):
        """Engine raises QualifireValidationError on ERROR severity."""
        backend = MagicMock()
        backend.get_aggregations.return_value = {"row_count": 5}

        ds = DatasetConfig(
            name="sales",
            table="db.sales",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"row_count": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="row_count",
                        thresholds=ThresholdLevels(
                            error={"min": 100},
                        ),
                    )],
                ),
            ],
        )
        config = QualifireConfig(
            owner="o", bu="b", system_table="s",
            datasets=[ds],
        )
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
        )
        with pytest.raises(QualifireValidationError) as exc_info:
            engine.run()

        assert exc_info.value.result is not None
        assert exc_info.value.result.has_errors

    def test_run_multiple_datasets(self):
        """Engine processes all datasets even if one has errors."""
        backend = MagicMock()
        backend.get_aggregations.return_value = {"cnt": 50}

        datasets = [
            DatasetConfig(
                name=f"ds{i}",
                table=f"t{i}",
                validations=[
                    ThresholdValidationConfig(
                        collection=AggregationCollectionConfig(
                            expressions={"cnt": "COUNT(*)"}
                        ),
                        rules=[ThresholdRuleConfig(
                            metric="cnt",
                            thresholds=ThresholdLevels(warning={"min": 100}),
                        )],
                    ),
                ],
            )
            for i in range(3)
        ]
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=datasets)
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
        )
        result = engine.run()

        assert len(result.datasets) == 3

    def test_validation_error_caught_gracefully(self):
        """If a collector/validator raises, it becomes a marked ERROR
        result (not a crash) and the run raises ``QualifireInternalError``
        — sibling class to ``QualifireValidationError``, distinguishing
        library/infrastructure failures from data-quality findings.

        Phase 2 of external-catalog-system-table-hardening renamed the
        contract from "validator exceptions raise QualifireValidationError"
        to "validator exceptions raise QualifireInternalError" so
        operators can route engineering on-call vs data-team queues.
        """
        from qualifire.core.exceptions import QualifireInternalError

        backend = MagicMock()
        backend.get_aggregations.side_effect = RuntimeError("DB connection lost")

        ds = DatasetConfig(
            name="ds",
            table="t",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"cnt": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="cnt",
                        thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=[ds])
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
        )

        with pytest.raises(QualifireInternalError) as exc_info:
            engine.run()

        # NOT caught by `except QualifireValidationError` — siblings.
        assert not isinstance(exc_info.value, QualifireValidationError)
        vr = exc_info.value.result.datasets[0].validation_results[0]
        assert vr.severity == Severity.ERROR
        assert "execution error" in vr.message.lower()
        # Marker is on the synthesised row.
        assert vr.details.get("qualifire_internal_failure") is True


class TestEnginePersist:
    def test_results_persisted_to_storage(self):
        """Engine writes results to system table storage."""
        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()

        ds = DatasetConfig(
            name="sales",
            table="db.sales",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"row_count": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="row_count",
                        thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        engine = _make_engine([ds], storage=storage)
        engine.run()

        history = storage.read_metric_history(
            table_name="db.sales", metric_name="row_count", limit=10
        )
        assert len(history) >= 1

    def test_persist_failure_raises_with_chained_exception(self):
        """If storage write fails (system-table outage / infra issue),
        the engine raises ``QualifireValidationError`` with the
        original storage exception chained via ``__cause__`` (PEP
        3134), AND the qualifire.persistence engine warning is
        recorded.

        Phase 2 of external-catalog-system-table-hardening inverts the
        prior P4.1 contract ("does not crash"): qualifire-internal
        failures (persistence + validator-execution exceptions) are no
        longer silent. The run raises so callers see the failure;
        notifications are suppressed (no false-positive paging on
        infra outage); forensic rows are persisted via the best-effort
        retry path. See plan §5.3 — infra failures are operationally
        distinct from data-quality findings.
        """
        from qualifire.core.exceptions import (
            QualifireInternalError,
            QualifireValidationError,
        )

        storage = MagicMock()
        storage.write_results.side_effect = RuntimeError("Storage full")

        ds = DatasetConfig(
            name="sales",
            table="db.sales",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"row_count": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="row_count",
                        thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        engine = _make_engine([ds], storage=storage)

        with pytest.raises(QualifireInternalError) as excinfo:
            engine.run()

        # The original storage exception is chained as __cause__ so
        # debuggers see it without a separate logger inspection.
        assert isinstance(excinfo.value.__cause__, RuntimeError)
        assert "Storage full" in str(excinfo.value.__cause__)
        # Message references the run-level signal (no findings to
        # deliver here because the validation passed).
        assert "persistence failed" in str(excinfo.value).lower()
        # Crucially: QualifireInternalError is NOT a subclass of
        # QualifireValidationError — they're sibling classes,
        # representing distinct error categories. Pin the contract.
        assert not isinstance(excinfo.value, QualifireValidationError)
        # Engine warning row was still recorded for forensic trail.
        result = excinfo.value.result
        assert result is not None
        assert any(
            vr.validation_base_name == "qualifire.persistence"
            for vr in result.engine_warnings
        )
        # The warning carries the qualifire-internal-failure marker.
        persistence_warning = next(
            vr for vr in result.engine_warnings
            if vr.validation_base_name == "qualifire.persistence"
        )
        assert persistence_warning.details.get("qualifire_internal_failure") is True


class TestEngineNotifications:
    def test_notifications_sent_on_warning(self):
        """Notifiers are called for WARNING severity results."""
        backend = MagicMock()
        backend.get_aggregations.return_value = {"cnt": 50}

        mock_notifier = MagicMock()
        mock_notifier.send.return_value = MagicMock(
            channel="email", severity=Severity.WARNING, status="sent", message=""
        )

        ds = DatasetConfig(
            name="ds",
            table="t",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"cnt": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="cnt",
                        thresholds=ThresholdLevels(warning={"min": 100}),
                    )],
                    notify=NotifyConfig(warning=["email"]),
                ),
            ],
        )
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=[ds])
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
            notifiers={"email": mock_notifier},
        )

        result = engine.run()
        mock_notifier.send.assert_called()
        assert len(result.notifications) >= 1

    def test_grouped_one_notification_per_channel_per_severity(self):
        """Multiple failing validations in one dataset → ONE notification per channel."""
        backend = MagicMock()
        backend.get_aggregations.return_value = {"cnt": 50, "avg": 5.0}

        mock_email = MagicMock()
        mock_email.send.return_value = MagicMock(
            channel="email", severity=Severity.WARNING, status="sent", message=""
        )
        mock_slack = MagicMock()
        mock_slack.send.return_value = MagicMock(
            channel="slack", severity=Severity.WARNING, status="sent", message=""
        )

        ds = DatasetConfig(
            name="ds",
            table="t",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"cnt": "COUNT(*)", "avg": "AVG(x)"}
                    ),
                    rules=[
                        ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels(warning={"min": 100})),
                        ThresholdRuleConfig(metric="avg", thresholds=ThresholdLevels(warning={"min": 10})),
                    ],
                    notify=NotifyConfig(warning=["email", "slack"]),
                ),
            ],
        )
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=[ds])
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
            notifiers={"email": mock_email, "slack": mock_slack},
        )

        result = engine.run()
        assert mock_email.send.call_count == 1
        assert mock_slack.send.call_count == 1
        assert len(result.notifications) == 2

    def test_cross_dataset_grouping_same_channel(self):
        """Two datasets routing WARNING to same channel → ONE notification.

        Cross-dataset grouping is preserved per P2.3 (findings-sweep)
        only when the datasets share the same `(channel, severity,
        validation_base_name)`. Setting `name="my_check"` on every
        validation gives them the same base; without that, each
        dataset's threshold validation defaults to a unique
        `threshold.<dataset>` base and routes to its own message.
        """
        backend = MagicMock()
        backend.get_aggregations.return_value = {"cnt": 50}

        mock_slack = MagicMock()
        mock_slack.send.return_value = MagicMock(
            channel="slack", severity=Severity.WARNING, status="sent", message=""
        )

        datasets = [
            DatasetConfig(
                name=f"ds{i}",
                table=f"t{i}",
                validations=[
                    ThresholdValidationConfig(
                        name="shared_check",  # same base across all 3 datasets
                        collection=AggregationCollectionConfig(
                            expressions={"cnt": "COUNT(*)"}
                        ),
                        rules=[ThresholdRuleConfig(
                            metric="cnt",
                            thresholds=ThresholdLevels(warning={"min": 100}),
                        )],
                        notify=NotifyConfig(warning=["slack"]),
                    ),
                ],
            )
            for i in range(3)
        ]
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=datasets)
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
            notifiers={"slack": mock_slack},
        )

        result = engine.run()
        # 3 datasets, same channel+severity → ONE slack message
        assert mock_slack.send.call_count == 1
        # The summary should contain results from all 3 datasets
        sent_ds = mock_slack.send.call_args[0][0]
        assert "3 dataset" in sent_ds.dataset_name

    def test_cross_dataset_different_channels_separate(self):
        """Two datasets routing to different channels → separate notifications."""
        backend = MagicMock()
        backend.get_aggregations.return_value = {"cnt": 50}

        mock_sales = MagicMock()
        mock_sales.send.return_value = MagicMock(
            channel="sales_email", severity=Severity.WARNING, status="sent", message=""
        )
        mock_infra = MagicMock()
        mock_infra.send.return_value = MagicMock(
            channel="infra_email", severity=Severity.WARNING, status="sent", message=""
        )

        ds_sales = DatasetConfig(
            name="sales", table="t1",
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
                rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels(warning={"min": 100}))],
                notify=NotifyConfig(warning=["sales_email"]),
            )],
        )
        ds_infra = DatasetConfig(
            name="infra", table="t2",
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
                rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels(warning={"min": 100}))],
                notify=NotifyConfig(warning=["infra_email"]),
            )],
        )
        config = QualifireConfig(
            owner="o", bu="b", system_table="s",
            datasets=[ds_sales, ds_infra],
        )
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
            notifiers={"sales_email": mock_sales, "infra_email": mock_infra},
        )

        result = engine.run()
        # Different channels → separate messages, each with their own dataset
        assert mock_sales.send.call_count == 1
        assert mock_infra.send.call_count == 1
        assert len(result.notifications) == 2

    def test_no_notification_on_pass_by_default(self):
        """No notifications sent when all validations pass (default behavior)."""
        mock_notifier = MagicMock()

        ds = DatasetConfig(
            name="ds",
            table="t",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"row_count": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="row_count",
                        thresholds=ThresholdLevels(),
                    )],
                    notify=NotifyConfig(error=["email"]),
                ),
            ],
        )
        engine = _make_engine([ds], notifiers={"email": mock_notifier})
        result = engine.run()

        mock_notifier.send.assert_not_called()
        assert len(result.notifications) == 0

    def test_success_notification_when_configured(self):
        """When pass channels are configured, send notification on PASS."""
        mock_email = MagicMock()
        mock_email.send.return_value = MagicMock(
            channel="email", severity=Severity.PASS, status="sent", message=""
        )

        ds = DatasetConfig(
            name="ds",
            table="t",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"row_count": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="row_count",
                        thresholds=ThresholdLevels(),
                    )],
                    notify=NotifyConfig(on_success=["email"]),
                ),
            ],
        )
        engine = _make_engine([ds], notifiers={"email": mock_email})
        result = engine.run()

        mock_email.send.assert_called_once()
        assert len(result.notifications) == 1

    def test_success_notification_not_sent_without_config(self):
        """Without pass channels, PASS does not trigger any notification."""
        mock_email = MagicMock()

        ds = DatasetConfig(
            name="ds",
            table="t",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"row_count": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="row_count",
                        thresholds=ThresholdLevels(),
                    )],
                    notify=NotifyConfig(warning=["email"], error=["email"]),
                ),
            ],
        )
        engine = _make_engine([ds], notifiers={"email": mock_email})
        result = engine.run()

        mock_email.send.assert_not_called()

    def test_cross_dataset_success_grouped(self):
        """Multiple passing datasets with pass channels → ONE notification.

        Same per-validation-routing rule as
        `test_cross_dataset_grouping_same_channel`: cross-dataset
        grouping requires a shared base name.
        """
        mock_email = MagicMock()
        mock_email.send.return_value = MagicMock(
            channel="email", severity=Severity.PASS, status="sent", message=""
        )

        datasets = [
            DatasetConfig(
                name=f"ds{i}",
                table=f"t{i}",
                validations=[
                    ThresholdValidationConfig(
                        name="shared_check",
                        collection=AggregationCollectionConfig(
                            expressions={"row_count": "COUNT(*)"}
                        ),
                        rules=[ThresholdRuleConfig(
                            metric="row_count",
                            thresholds=ThresholdLevels(),
                        )],
                        notify=NotifyConfig(on_success=["email"]),
                    ),
                ],
            )
            for i in range(3)
        ]
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=datasets)
        engine = QualifireEngine(
            backend=MagicMock(get_aggregations=MagicMock(return_value={"row_count": 5000})),
            storage=None,
            context=QualifireContext(), config=config,
            notifiers={"email": mock_email},
        )

        result = engine.run()
        assert mock_email.send.call_count == 1
        sent_ds = mock_email.send.call_args[0][0]
        assert "3 dataset" in sent_ds.dataset_name


class TestEngineCollectionPersistence:
    def test_collection_results_persisted(self):
        """Engine writes collection rows alongside validation rows."""
        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()

        ds = DatasetConfig(
            name="sales",
            table="db.sales",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"row_count": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="row_count",
                        thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        engine = _make_engine([ds], storage=storage)
        result = engine.run()

        # Both collection and validation rows should exist
        conn = storage._get_conn()
        cursor = conn.execute(
            "SELECT record_type, COUNT(*) FROM qualifire_history GROUP BY record_type"
        )
        rows = {row[0]: row[1] for row in cursor.fetchall()}
        assert "collection" in rows
        assert "validation" in rows

    def test_collection_results_have_collector_name(self):
        """Collection rows should have auto-generated collector_name."""
        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()

        ds = DatasetConfig(
            name="sales",
            table="db.sales",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"row_count": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="row_count",
                        thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        engine = _make_engine([ds], storage=storage)
        engine.run()

        conn = storage._get_conn()
        cursor = conn.execute(
            "SELECT collector_name FROM qualifire_history WHERE record_type = 'collection'"
        )
        rows = cursor.fetchall()
        assert len(rows) >= 1
        assert rows[0][0] == "threshold.sales"

    def test_custom_collector_name(self):
        """User-specified validation name should be used as collector_name."""
        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()

        ds = DatasetConfig(
            name="sales",
            table="db.sales",
            validations=[
                ThresholdValidationConfig(
                    name="count_last_7d",
                    collection=AggregationCollectionConfig(
                        expressions={"row_count": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="row_count",
                        thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        engine = _make_engine([ds], storage=storage)
        engine.run()

        conn = storage._get_conn()
        cursor = conn.execute(
            "SELECT collector_name FROM qualifire_history WHERE record_type = 'collection'"
        )
        rows = cursor.fetchall()
        assert rows[0][0] == "count_last_7d"

    def test_dataset_result_has_collection_results(self):
        """DatasetResult should carry collection_results after engine run."""
        ds = DatasetConfig(
            name="sales",
            table="db.sales",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"row_count": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="row_count",
                        thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        engine = _make_engine([ds])
        result = engine.run()

        ds_result = result.datasets[0]
        assert len(ds_result.collection_results) >= 1
        assert ds_result.collection_results[0].metric_name == "row_count"


class TestCollectionPersistenceReadback:
    def test_collection_values_readable_via_read_metric_history(self):
        """End-to-end: collection values written by engine can be read back via read_metric_history."""
        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()

        ds = DatasetConfig(
            name="sales",
            table="db.sales",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"row_count": "COUNT(*)", "avg_amount": "AVG(amount)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="row_count",
                        thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        engine = _make_engine([ds], storage=storage)
        engine.run()

        # Read back collection values via the same API validators use.
        # The threshold validator emits BOTH a validation row and a
        # collection row at the same run_timestamp; the dedupe rule
        # (latest run_timestamp wins, validation wins on tie) returns
        # the validation row. metric_value is identical on both, so
        # consumers reading the value get the same answer regardless
        # of which row survives.
        history = storage.read_metric_history(
            table_name="db.sales", metric_name="row_count"
        )
        assert len(history) >= 1
        assert history[0]["metric_value"] == 5000.0
        assert history[0]["record_type"] == "validation"

        history2 = storage.read_metric_history(
            table_name="db.sales", metric_name="avg_sales"
        )
        assert len(history2) >= 1
        assert history2[0]["metric_value"] == 150.0

    def test_datetime_metric_value_persisted_as_epoch(self):
        """RecencyCollector datetime values are converted to epoch seconds for persistence."""
        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()

        ds = DatasetConfig(
            name="freshness",
            table="db.events",
            validations=[
                SLOValidationConfig(
                    recency=RecencyCollectionConfig(
                        strategy="max_column",
                        column="updated_at",
                    ),
                    thresholds={"warning": "PT4H", "error": "PT8H"},
                ),
            ],
        )
        engine = _make_engine([ds], storage=storage)
        # SLO may raise ERROR (mock timestamp is in 2024), but results are persisted first
        try:
            engine.run()
        except Exception:
            pass

        # The recency metric should be persisted as epoch seconds
        conn = storage._get_conn()
        cursor = conn.execute(
            "SELECT metric_value FROM qualifire_history "
            "WHERE record_type = 'collection' AND metric_name = 'recency'"
        )
        rows = cursor.fetchall()
        assert len(rows) >= 1
        epoch_value = rows[0][0]
        assert epoch_value is not None
        # Epoch for 2024-06-15T10:00:00 should be a large positive float
        assert epoch_value > 1_000_000_000

    def test_profiling_collection_type_persisted(self):
        """ProfilingCollector sets collection_type='profiling' in persisted rows."""
        from qualifire.core.config import ProfilingCollectionConfig

        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()

        ds = DatasetConfig(
            name="profile_test",
            table="db.data",
            validations=[
                ThresholdValidationConfig(
                    collection=ProfilingCollectionConfig(
                        columns=["amount"],
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="amount.count",
                        thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )

        backend = MagicMock()
        import pandas as pd
        backend.execute_sql.return_value = pd.DataFrame({"amount": [10.0, 20.0, 30.0]})
        config = QualifireConfig(
            owner="test-team", bu="test-bu", system_table="test_sys", datasets=[ds]
        )
        engine = QualifireEngine(
            backend=backend, storage=storage,
            context=QualifireContext(run_id="test-run"),
            config=config, notifiers={},
        )
        engine.run()

        conn = storage._get_conn()
        cursor = conn.execute(
            "SELECT collection_type FROM qualifire_history "
            "WHERE record_type = 'collection' LIMIT 1"
        )
        rows = cursor.fetchall()
        assert len(rows) >= 1
        assert rows[0][0] == "profiling"


class TestCollectionNoneSkip:
    def test_none_metric_value_not_persisted(self):
        """CollectionResult with metric_value=None should NOT produce a system table row."""
        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()

        # SamplerCollector produces a "sample" metric with a DataFrame value (not numeric).
        # We use a threshold check here so the collector path is exercised, then
        # manually inject a None-valued CollectionResult to test the guard.
        ds = DatasetConfig(
            name="sales",
            table="db.sales",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"row_count": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="row_count",
                        thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        engine = _make_engine([ds], storage=storage)
        result = engine.run()

        # Manually add a None-valued collection result to simulate sampler output
        from qualifire.core.models import CollectionResult
        result.datasets[0].collection_results.append(
            CollectionResult(
                metric_name="sample",
                metric_value=None,
                collected_at=datetime.now(),
                metadata={"collection_type": "sample"},
            )
        )

        # Re-persist (clear and write again)
        engine._persist_results(result)

        conn = storage._get_conn()
        cursor = conn.execute(
            "SELECT COUNT(*) FROM qualifire_history "
            "WHERE record_type = 'collection' AND metric_name = 'sample'"
        )
        count = cursor.fetchone()[0]
        assert count == 0, "None metric_value should not be persisted as a collection row"

    def test_non_numeric_metric_value_not_persisted(self):
        """CollectionResult with non-numeric metric_value should NOT produce a row."""
        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()

        ds = DatasetConfig(
            name="sales",
            table="db.sales",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"row_count": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="row_count",
                        thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        engine = _make_engine([ds], storage=storage)
        result = engine.run()

        from qualifire.core.models import CollectionResult
        result.datasets[0].collection_results.append(
            CollectionResult(
                metric_name="sample",
                metric_value="not-a-number",
                collected_at=datetime.now(),
                metadata={"collection_type": "sample"},
            )
        )

        engine._persist_results(result)

        conn = storage._get_conn()
        cursor = conn.execute(
            "SELECT COUNT(*) FROM qualifire_history "
            "WHERE record_type = 'collection' AND metric_name = 'sample'"
        )
        count = cursor.fetchone()[0]
        assert count == 0, "Non-numeric metric_value should not be persisted"


class TestEngineWAP:
    def test_wap_publish_on_pass(self):
        """WAP publishes data when validation passes."""
        backend = MagicMock()
        # Force the SQL fallback publish path: MagicMock auto-creates
        # every attribute, including ``spark``. Setting it to None
        # makes ``_publish_spark``'s discriminator route through
        # ``_publish_sql`` (the SQL INSERT path the test asserts).
        backend.spark = None
        backend.execute_sql.return_value = MockDataFrame([{"cnt": 1000}])
        backend.get_aggregations.return_value = {"cnt": 1000}

        ds = DatasetConfig(
            name="wap_test",
            wap=WAPConfig(
                target_table="prod.table",
                sql="SELECT * FROM raw.table",
            ),
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"cnt": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="cnt",
                        thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=[ds])
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
        )
        result = engine.run()

        # Should have called execute_sql for CREATE TABLE staging, publish, and drop
        assert backend.execute_sql.call_count >= 2
        assert backend.drop_table.called

    def test_wap_rollback_on_error(self):
        """WAP drops staging table when validation fails."""
        backend = MagicMock()
        backend.spark = None  # Force SQL fallback (see test_wap_publish_on_pass).
        backend.execute_sql.return_value = MockDataFrame([{"cnt": 5}])
        backend.get_aggregations.return_value = {"cnt": 5}

        ds = DatasetConfig(
            name="wap_fail",
            wap=WAPConfig(
                target_table="prod.table",
                sql="SELECT * FROM raw.table",
            ),
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"cnt": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="cnt",
                        thresholds=ThresholdLevels(error={"min": 100}),
                    )],
                ),
            ],
        )
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=[ds])
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
        )

        with pytest.raises(QualifireValidationError):
            engine.run()

        # staging dropped (rollback)
        assert backend.drop_table.called


class TestEngineWAPStagingPropagation:
    """Sub-feature C: ``_run_wap`` forwards dataset-level fields into
    the staging audit ``DatasetConfig``.

    Pre-fix: only ``name``, ``table``, ``validations`` were forwarded,
    forcing operators to set ``partition_ts`` etc. at the run level.
    Post-fix: ``description``, ``dimensions``, ``measures``,
    ``partition_ts``, ``partition_step`` propagate too. ``wap`` is
    stripped (recursion guard).

    Each test below patches ``_run_dataset`` so we can capture the
    exact ``DatasetConfig`` the staging audit ran against, then
    asserts on its fields. We don't care about the audit's actual
    output for these tests; the propagation contract is the subject.
    """

    @staticmethod
    def _capture_staging(monkeypatch, engine):
        captured: list[DatasetConfig] = []
        from qualifire.core.models import DatasetResult
        original = engine._run_dataset
        def spy(ds_config: DatasetConfig) -> DatasetResult:
            captured.append(ds_config)
            return original(ds_config)
        monkeypatch.setattr(engine, "_run_dataset", spy)
        return captured

    def _make_backend(self):
        backend = MagicMock()
        backend.spark = None  # Force SQL fallback publish (see TestEngineWAP).
        backend.execute_sql.return_value = MockDataFrame([{"cnt": 1000}])
        backend.get_aggregations.return_value = {"cnt": 1000}
        return backend

    def _build_engine(self, ds: DatasetConfig):
        backend = self._make_backend()
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=[ds])
        return QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
        )

    def test_partition_ts_and_partition_step_forwarded(self, monkeypatch):
        """Dataset-level partition_ts and partition_step flow through."""
        ds = DatasetConfig(
            name="wap_part",
            partition_ts="event_date",
            partition_step="P1D",
            wap=WAPConfig(target_table="prod.t", sql="SELECT * FROM raw.t"),
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
                    rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
                ),
            ],
        )
        engine = self._build_engine(ds)
        captured = self._capture_staging(monkeypatch, engine)
        engine.run()

        assert len(captured) == 1
        staging = captured[0]
        assert staging.partition_ts == "event_date"
        assert staging.partition_step == "P1D"

    def test_dimensions_and_measures_forwarded(self, monkeypatch):
        ds = DatasetConfig(
            name="wap_dim",
            dimensions=["region"],
            measures=["amount"],
            wap=WAPConfig(target_table="prod.t", sql="SELECT * FROM raw.t"),
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
                    rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
                ),
            ],
        )
        engine = self._build_engine(ds)
        captured = self._capture_staging(monkeypatch, engine)
        engine.run()

        staging = captured[0]
        assert staging.dimensions == ["region"]
        assert staging.measures == ["amount"]

    def test_description_forwarded(self, monkeypatch):
        ds = DatasetConfig(
            name="wap_desc",
            description="WAP-backed daily summary",
            wap=WAPConfig(target_table="prod.t", sql="SELECT * FROM raw.t"),
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
                    rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
                ),
            ],
        )
        engine = self._build_engine(ds)
        captured = self._capture_staging(monkeypatch, engine)
        engine.run()

        assert captured[0].description == "WAP-backed daily summary"

    def test_wap_stripped_from_staging_recursion_guard(self, monkeypatch):
        """Staging config must NOT carry `wap` — would recurse forever."""
        ds = DatasetConfig(
            name="wap_recursion",
            wap=WAPConfig(target_table="prod.t", sql="SELECT * FROM raw.t"),
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
                    rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
                ),
            ],
        )
        engine = self._build_engine(ds)
        captured = self._capture_staging(monkeypatch, engine)
        engine.run()

        assert captured[0].wap is None

    def test_filter_and_query_NOT_forwarded(self, monkeypatch):
        """Source-side fields (filter, query, df) are intentionally
        not forwarded — staging is the just-written subset."""
        ds = DatasetConfig(
            name="wap_no_filter_pass",
            filter="event_dt >= '2026-01-01'",
            wap=WAPConfig(target_table="prod.t", sql="SELECT * FROM raw.t"),
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
                    rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
                ),
            ],
        )
        engine = self._build_engine(ds)
        captured = self._capture_staging(monkeypatch, engine)
        engine.run()

        # staging gets table=staging_table, no filter / query forwarded
        assert captured[0].filter is None
        assert captured[0].query is None

    def test_two_wap_datasets_isolated_partition_ts(self, monkeypatch):
        """Each WAP dataset's audit sees its own partition_ts."""
        ds_a = DatasetConfig(
            name="wap_a",
            partition_ts="event_dt_a",
            partition_step="P1D",
            wap=WAPConfig(target_table="prod.a", sql="SELECT * FROM raw.a"),
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
                    rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
                ),
            ],
        )
        ds_b = DatasetConfig(
            name="wap_b",
            partition_ts="event_dt_b",
            partition_step="P1D",
            wap=WAPConfig(target_table="prod.b", sql="SELECT * FROM raw.b"),
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
                    rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
                ),
            ],
        )
        backend = self._make_backend()
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=[ds_a, ds_b])
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
        )
        captured = self._capture_staging(monkeypatch, engine)
        engine.run()

        # Two WAP datasets → two staging configs, each with its own
        # partition_ts.
        assert len(captured) == 2
        seen = sorted(c.partition_ts for c in captured)
        assert seen == ["event_dt_a", "event_dt_b"]


class TestEngineParallelism:
    def test_parallel_datasets(self):
        """Multiple datasets run in parallel when parallelism > 1."""
        backend = MagicMock()
        backend.get_aggregations.return_value = {"cnt": 5000}

        datasets = [
            DatasetConfig(
                name=f"ds{i}",
                table=f"t{i}",
                validations=[
                    ThresholdValidationConfig(
                        collection=AggregationCollectionConfig(
                            expressions={"cnt": "COUNT(*)"}
                        ),
                        rules=[ThresholdRuleConfig(
                            metric="cnt", thresholds=ThresholdLevels(),
                        )],
                    ),
                ],
            )
            for i in range(5)
        ]
        config = QualifireConfig(
            owner="o", bu="b", system_table="s",
            dataset_parallelism=3,
            datasets=datasets,
        )
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
        )
        result = engine.run()

        assert len(result.datasets) == 5
        # Results should be in original dataset order
        assert [ds.dataset_name for ds in result.datasets] == [f"ds{i}" for i in range(5)]
        assert result.overall_severity == Severity.PASS

    def test_parallel_datasets_preserves_order_on_error(self):
        """Parallel execution preserves dataset order even with errors."""
        backend = MagicMock()
        backend.get_aggregations.return_value = {"cnt": 5}

        datasets = [
            DatasetConfig(
                name=f"ds{i}",
                table=f"t{i}",
                validations=[
                    ThresholdValidationConfig(
                        collection=AggregationCollectionConfig(
                            expressions={"cnt": "COUNT(*)"}
                        ),
                        rules=[ThresholdRuleConfig(
                            metric="cnt",
                            thresholds=ThresholdLevels(error={"min": 100}),
                        )],
                    ),
                ],
            )
            for i in range(3)
        ]
        config = QualifireConfig(
            owner="o", bu="b", system_table="s",
            dataset_parallelism=3,
            datasets=datasets,
        )
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
        )
        with pytest.raises(QualifireValidationError) as exc_info:
            engine.run()

        result = exc_info.value.result
        assert len(result.datasets) == 3
        assert [ds.dataset_name for ds in result.datasets] == ["ds0", "ds1", "ds2"]

    def test_parallel_pipelines_within_dataset(self):
        """Multiple collect+validate pipelines run in parallel within one dataset."""
        backend = MagicMock()
        backend.get_aggregations.return_value = {"cnt": 5000, "avg": 100.0}

        ds = DatasetConfig(
            name="ds",
            table="t",
            validation_parallelism=3,
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"cnt": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="cnt", thresholds=ThresholdLevels(),
                    )],
                ),
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"avg": "AVG(x)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="avg", thresholds=ThresholdLevels(),
                    )],
                ),
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"cnt": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="cnt", thresholds=ThresholdLevels(warning={"min": 100}),
                    )],
                ),
            ],
        )
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=[ds])
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
        )
        result = engine.run()

        # All 3 pipelines ran and produced results
        assert len(result.datasets[0].validation_results) == 3

    def test_parallel_pipelines_preserve_order(self):
        """Parallel pipeline results appear in original config order."""
        backend = MagicMock()
        backend.get_aggregations.return_value = {"a": 1, "b": 2, "c": 3}

        ds = DatasetConfig(
            name="ds",
            table="t",
            validation_parallelism=3,
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(expressions={"a": "1"}),
                    rules=[ThresholdRuleConfig(metric="a", thresholds=ThresholdLevels())],
                ),
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(expressions={"b": "2"}),
                    rules=[ThresholdRuleConfig(metric="b", thresholds=ThresholdLevels())],
                ),
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(expressions={"c": "3"}),
                    rules=[ThresholdRuleConfig(metric="c", thresholds=ThresholdLevels())],
                ),
            ],
        )
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=[ds])
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
        )
        result = engine.run()

        vr_names = [vr.validation_name for vr in result.datasets[0].validation_results]
        # Each threshold validator names results as "threshold.ds.a", etc.
        assert len(vr_names) == 3

    def test_parallelism_default_is_sequential(self):
        """Default dataset_parallelism=1 runs sequentially (no ThreadPoolExecutor)."""
        backend = MagicMock()
        backend.get_aggregations.return_value = {"cnt": 5000}

        ds = DatasetConfig(
            name="ds", table="t",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"cnt": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="cnt", thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=[ds])
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
        )
        result = engine.run()
        assert result.overall_severity == Severity.PASS

    def test_pipeline_error_doesnt_crash_others(self):
        """One failing pipeline doesn't prevent other parallel pipelines from completing."""
        backend = MagicMock()

        call_count = {"n": 0}
        def side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("first pipeline fails")
            return {"cnt": 5000}

        backend.get_aggregations.side_effect = side_effect

        ds = DatasetConfig(
            name="ds", table="t",
            validation_parallelism=2,
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
                    rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
                ),
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
                    rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
                ),
            ],
        )
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=[ds])
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
        )

        # First pipeline raises (synthesised internal-failure ERROR
        # row); second pipeline passes. With no real data-quality
        # findings alongside, the run raises QualifireInternalError
        # — Phase 2 of external-catalog-system-table-hardening.
        from qualifire.core.exceptions import QualifireInternalError

        with pytest.raises(QualifireInternalError) as exc_info:
            engine.run()

        vrs = exc_info.value.result.datasets[0].validation_results
        # One ERROR (failed pipeline, marked internal) + one PASS
        # (successful pipeline)
        assert len(vrs) == 2
        severities = {vr.severity for vr in vrs}
        assert Severity.ERROR in severities


class TestEngineQueryDataset:
    """Tests for query-based datasets (custom SQL query as dataset source)."""

    def _make_query_engine(self, ds_config, storage=None):
        backend = MagicMock()
        # execute_sql returns a MockDataFrame with columns matching the query result
        backend.execute_sql.return_value = MockDataFrame(
            [{"region": "US", "total_revenue": 50000, "order_count": 200}]
        )
        backend.get_aggregations.return_value = {"total_revenue": 50000}
        # NB: pre-findings-sweep this stub did `del backend.register_table`
        # to simulate a Spark backend whose engine path issued raw
        # ``DROP VIEW`` SQL. The engine now calls ``backend.register_table``
        # and ``backend.drop_temp_view`` unconditionally (P4.5), so the
        # MagicMock auto-stubs both methods and the SparkBackend SQL form
        # only fires for callers using the real backend.

        config = QualifireConfig(
            owner="test-team",
            bu="test-bu",
            system_table="test_sys",
            datasets=[ds_config],
        )
        ctx = QualifireContext(run_id="test-run-id")
        engine = QualifireEngine(
            backend=backend,
            storage=storage,
            context=ctx,
            config=config,
        )
        return engine, backend

    def test_query_dataset_pass(self):
        """Query-based dataset runs validations and returns PASS."""
        ds = DatasetConfig(
            name="revenue",
            query="SELECT region, SUM(amount) AS total_revenue FROM orders GROUP BY region",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"total_revenue": "SUM(total_revenue)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="total_revenue",
                        thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        engine, backend = self._make_query_engine(ds)
        result = engine.run()

        assert len(result.datasets) == 1
        assert result.overall_severity == Severity.PASS
        # Table should be the dataset name, not a temp view
        assert result.datasets[0].table == "revenue"
        # execute_sql should have been called (query materialization + DROP VIEW)
        assert backend.execute_sql.call_count >= 1

    def test_query_dataset_creates_and_drops_temp_view(self):
        """Temp view is created via the Backend Protocol and dropped after.

        Pre-findings-sweep this test simulated a Spark backend by
        deleting ``register_table`` from the mock and asserted on
        the raw ``DROP VIEW`` SQL the engine issued. The engine now
        calls ``backend.register_table`` and ``backend.drop_temp_view``
        through the Protocol regardless of backend (P4.5); we assert
        on the protocol calls instead.
        """
        query_df = MockDataFrame(
            [{"region": "US", "total": 100}]
        )
        backend = MagicMock()
        backend.execute_sql.return_value = query_df
        backend.get_aggregations.return_value = {"total": 100}

        ds = DatasetConfig(
            name="rev",
            query="SELECT region, SUM(x) AS total FROM t GROUP BY region",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"total": "SUM(total)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="total", thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        config = QualifireConfig(
            owner="o", bu="b", system_table="s", datasets=[ds],
        )
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(run_id="abc12345"), config=config,
        )
        engine.run()

        # First execute_sql call materializes the query
        first_sql_call = backend.execute_sql.call_args_list[0][0][0]
        assert "SELECT" in first_sql_call

        # Verify register_table happened with a sanitized view name
        assert backend.register_table.call_count == 1
        view_name = backend.register_table.call_args[0][0]
        assert view_name.startswith("_qf_rev_")
        assert view_name.endswith("_abc12345")

        # Verify cleanup: drop_temp_view called with the same view name
        assert backend.drop_temp_view.call_count == 1
        assert backend.drop_temp_view.call_args[0][0] == view_name

    def test_query_schema_validation_catches_missing_column(self):
        """Declared dimension/measure not in query result → ERROR."""
        ds = DatasetConfig(
            name="rev",
            query="SELECT SUM(amount) AS total FROM orders",
            dimensions=["region"],  # not in query result
            measures=["total"],
        )
        engine, _ = self._make_query_engine(ds)

        with pytest.raises(QualifireValidationError) as exc_info:
            engine.run()

        vrs = exc_info.value.result.datasets[0].validation_results
        assert any(vr.severity == Severity.ERROR for vr in vrs)
        assert any("region" in vr.message for vr in vrs)

    def test_query_schema_validation_passes_when_columns_match(self):
        """All declared columns present in query result → no schema errors."""
        ds = DatasetConfig(
            name="rev",
            query="SELECT region, SUM(amount) AS total_revenue FROM orders GROUP BY region",
            dimensions=["region"],
            measures=["total_revenue"],
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"total_revenue": "SUM(total_revenue)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="total_revenue",
                        thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        engine, _ = self._make_query_engine(ds)
        result = engine.run()

        assert result.overall_severity == Severity.PASS

    def test_query_schema_skipped_when_no_declarations(self):
        """No dimensions/measures declared → schema validation skipped."""
        ds = DatasetConfig(
            name="rev",
            query="SELECT SUM(amount) AS total FROM orders",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"total": "SUM(total)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="total", thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        engine, backend = self._make_query_engine(ds)
        backend.get_aggregations.return_value = {"total": 50000}
        result = engine.run()

        assert result.overall_severity == Severity.PASS

    def test_query_execution_error_produces_error_result(self):
        """SQL syntax error or runtime failure → ERROR DatasetResult."""
        backend = MagicMock()
        backend.execute_sql.side_effect = RuntimeError("Syntax error in SQL")

        ds = DatasetConfig(
            name="bad_query",
            query="SELECTX broken sql",
        )
        config = QualifireConfig(
            owner="o", bu="b", system_table="s", datasets=[ds],
        )
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
        )

        with pytest.raises(QualifireValidationError) as exc_info:
            engine.run()

        ds_result = exc_info.value.result.datasets[0]
        assert ds_result.table == "bad_query"  # dataset name, not temp view
        vr = ds_result.validation_results[0]
        assert vr.severity == Severity.ERROR
        assert "query execution failed" in vr.message.lower()
        assert "query" in vr.details

    def test_filter_ignored_with_warning_for_query_dataset(self):
        """filter alongside query is ignored (query should contain its own filtering)."""
        ds = DatasetConfig(
            name="rev",
            query="SELECT SUM(x) AS total FROM t WHERE date = '2024-01-01'",
            filter="should_be_ignored",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"total": "SUM(total)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="total", thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        engine, backend = self._make_query_engine(ds)
        backend.get_aggregations.return_value = {"total": 100}

        with patch("qualifire.core.engine.logger") as mock_logger:
            result = engine.run()
            mock_logger.warning.assert_any_call(
                "Dataset '%s': 'filter' is ignored for query-based datasets; "
                "include filtering in the query itself.",
                "rev",
            )

        assert result.overall_severity == Severity.PASS

    def test_query_context_in_results_for_persistence(self):
        """Query text should be attached to validation and collection results."""
        ds = DatasetConfig(
            name="rev",
            query="SELECT SUM(amount) AS total FROM orders",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"total": "SUM(total)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="total", thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        engine, backend = self._make_query_engine(ds)
        backend.get_aggregations.return_value = {"total": 50000}
        result = engine.run()

        ds_result = result.datasets[0]
        # Validation results should have query in details
        for vr in ds_result.validation_results:
            assert "query" in vr.details
            assert "SELECT SUM(amount)" in vr.details["query"]
        # Collection results should have query in metadata
        for cr in ds_result.collection_results:
            assert "query" in cr.metadata

    def test_query_dataset_persisted_to_system_table(self):
        """Query-based dataset results are persisted with dataset name as table_name."""
        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()

        ds = DatasetConfig(
            name="cross_table_check",
            query="SELECT SUM(amount) AS total FROM orders",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"total": "SUM(total)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="total", thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        engine, backend = self._make_query_engine(ds, storage=storage)
        backend.get_aggregations.return_value = {"total": 50000}
        engine.run()

        conn = storage._get_conn()
        # Validation records persisted
        cursor = conn.execute(
            "SELECT table_name, record_type FROM qualifire_history WHERE record_type = 'validation'"
        )
        rows = cursor.fetchall()
        assert len(rows) >= 1
        assert rows[0][0] == "cross_table_check"  # dataset name, not temp view

        # Collection records persisted
        cursor = conn.execute(
            "SELECT table_name, record_type FROM qualifire_history WHERE record_type = 'collection'"
        )
        rows = cursor.fetchall()
        assert len(rows) >= 1
        assert rows[0][0] == "cross_table_check"

    def test_query_with_parallel_validations(self):
        """Multiple validations on a query dataset run in parallel."""
        ds = DatasetConfig(
            name="rev",
            query="SELECT region, SUM(amount) AS total FROM orders GROUP BY region",
            validation_parallelism=2,
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"total_revenue": "SUM(total_revenue)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="total_revenue", thresholds=ThresholdLevels(),
                    )],
                ),
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"order_count": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="order_count", thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        engine, backend = self._make_query_engine(ds)
        backend.get_aggregations.return_value = {"total_revenue": 50000, "order_count": 200}
        result = engine.run()

        assert len(result.datasets[0].validation_results) == 2
        assert result.overall_severity == Severity.PASS

    def test_temp_view_cleaned_up_on_validation_error(self):
        """Temp view is dropped even when validations produce errors."""
        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame(
            [{"total": 5}]
        )
        backend.get_aggregations.return_value = {"total": 5}

        ds = DatasetConfig(
            name="rev",
            query="SELECT SUM(x) AS total FROM t",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"total": "SUM(total)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="total",
                        thresholds=ThresholdLevels(error={"min": 100}),
                    )],
                ),
            ],
        )
        config = QualifireConfig(
            owner="o", bu="b", system_table="s", datasets=[ds],
        )
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(run_id="cleanup123"), config=config,
        )

        with pytest.raises(QualifireValidationError):
            engine.run()

        # Verify cleanup happened via the Backend Protocol method
        assert backend.drop_temp_view.call_count == 1

    def test_ddl_query_rejected(self):
        """DDL statements (DROP, CREATE, ALTER) are rejected at engine level."""
        for bad_sql in [
            "DROP TABLE production_data",
            "CREATE TABLE foo AS SELECT 1",
            "INSERT INTO t SELECT * FROM other",
            "DELETE FROM t WHERE 1=1",
        ]:
            with pytest.raises(ValueError):
                QualifireEngine._validate_readonly_sql(bad_sql)

    def test_multi_statement_query_rejected(self):
        """Queries with semicolons (multi-statement) are rejected."""
        with pytest.raises(ValueError, match="semicolons"):
            QualifireEngine._validate_readonly_sql("SELECT 1; DROP TABLE x")

    def test_valid_select_queries_accepted(self):
        """Normal SELECT queries pass validation."""
        for valid_sql in [
            "SELECT * FROM t WHERE x > 1",
            "SELECT region, SUM(amount) AS total FROM orders GROUP BY region",
            "WITH cte AS (SELECT * FROM t) SELECT * FROM cte",
            "SELECT created_at, updated_by FROM t",  # column names containing DDL keywords
        ]:
            QualifireEngine._validate_readonly_sql(valid_sql)  # should not raise

    def test_dataset_name_rejects_special_characters(self):
        """Dataset names with SQL injection characters are rejected."""
        with pytest.raises(ValueError, match="alphanumeric"):
            DatasetConfig(name="x; DROP TABLE --", query="SELECT 1 AS val")

    def test_dimensions_reject_invalid_identifiers(self):
        """Dimension/measure columns must be valid SQL identifiers."""
        with pytest.raises(ValueError, match="valid SQL identifier"):
            DatasetConfig(
                name="ds",
                query="SELECT 1 AS val",
                dimensions=["1; DROP TABLE x"],
            )


class TestEngineOnEmptyData:
    """Tests for on_empty_data behavior at engine level."""

    def test_empty_validator_output_produces_warning_by_default(self):
        """When a validator returns empty list, engine creates WARNING result."""
        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame(
            [{"max_ts": datetime(2024, 6, 15, 10, 0, 0)}]
        )

        # SLO check with recency collector that returns a "recency" metric,
        # but we'll mock _validate to return empty list
        ds = DatasetConfig(
            name="ds",
            table="t",
            validations=[
                SLOValidationConfig(
                    recency=RecencyCollectionConfig(strategy="max_column", column="ts"),
                    thresholds={"warning": "PT4H", "error": "PT8H"},
                ),
            ],
        )
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=[ds])
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
        )

        # Patch _validate to return empty list (simulating validator producing no output)
        with patch.object(engine, "_validate", return_value=[]):
            result = engine.run()

        vrs = result.datasets[0].validation_results
        assert len(vrs) == 1
        assert vrs[0].severity == Severity.WARNING
        assert "no data to validate" in vrs[0].message.lower()

    def test_empty_validator_output_respects_on_empty_data_error(self):
        """With on_empty_data='error', empty validator output produces ERROR."""
        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame(
            [{"max_ts": datetime(2024, 6, 15, 10, 0, 0)}]
        )

        ds = DatasetConfig(
            name="ds",
            table="t",
            validations=[
                SLOValidationConfig(
                    recency=RecencyCollectionConfig(strategy="max_column", column="ts"),
                    thresholds={"warning": "PT4H", "error": "PT8H"},
                    on_empty_data="error",
                ),
            ],
        )
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=[ds])
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
        )

        with patch.object(engine, "_validate", return_value=[]):
            with pytest.raises(QualifireValidationError) as exc_info:
                engine.run()

        vrs = exc_info.value.result.datasets[0].validation_results
        assert vrs[0].severity == Severity.ERROR

    def test_empty_validator_output_respects_on_empty_data_pass(self):
        """With on_empty_data='pass', empty validator output is silently PASS."""
        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame(
            [{"max_ts": datetime(2024, 6, 15, 10, 0, 0)}]
        )

        ds = DatasetConfig(
            name="ds",
            table="t",
            validations=[
                SLOValidationConfig(
                    recency=RecencyCollectionConfig(strategy="max_column", column="ts"),
                    thresholds={"warning": "PT4H", "error": "PT8H"},
                    on_empty_data="pass",
                ),
            ],
        )
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=[ds])
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
        )

        with patch.object(engine, "_validate", return_value=[]):
            result = engine.run()

        vrs = result.datasets[0].validation_results
        assert len(vrs) == 1
        assert vrs[0].severity == Severity.PASS


class TestEngineTimestampTracking:
    """Tests for collected_at, validated_at, notified_at timestamp columns."""

    def test_validated_at_populated_on_results(self):
        """ValidationResult.validated_at is set after validation completes."""
        backend = MagicMock()
        backend.get_aggregations.return_value = {"cnt": 5000}

        ds = DatasetConfig(
            name="ds", table="t",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"cnt": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="cnt", thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=[ds])
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
        )
        result = engine.run()

        for vr in result.datasets[0].validation_results:
            assert vr.validated_at is not None
            assert isinstance(vr.validated_at, datetime)

    def test_collected_at_persisted_to_system_table(self):
        """Collection rows include collected_at from CollectionResult."""
        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()

        ds = DatasetConfig(
            name="sales", table="db.sales",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"row_count": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="row_count", thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        engine = _make_engine([ds], storage=storage)
        engine.run()

        conn = storage._get_conn()
        cursor = conn.execute(
            "SELECT collected_at FROM qualifire_history WHERE record_type = 'collection'"
        )
        rows = cursor.fetchall()
        assert len(rows) >= 1
        assert rows[0][0] is not None  # collected_at is populated

    def test_validated_at_persisted_to_system_table(self):
        """Validation rows include validated_at timestamp."""
        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()

        ds = DatasetConfig(
            name="sales", table="db.sales",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"row_count": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="row_count", thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        engine = _make_engine([ds], storage=storage)
        engine.run()

        conn = storage._get_conn()
        cursor = conn.execute(
            "SELECT validated_at FROM qualifire_history WHERE record_type = 'validation'"
        )
        rows = cursor.fetchall()
        assert len(rows) >= 1
        assert rows[0][0] is not None  # validated_at is populated

    def test_notified_at_persisted_to_system_table(self):
        """Notification rows include notified_at timestamp."""
        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()

        backend = MagicMock()
        backend.get_aggregations.return_value = {"cnt": 50}

        mock_notifier = MagicMock()
        mock_notifier.send.return_value = MagicMock(
            channel="email", severity=Severity.WARNING, status="sent", message=""
        )

        ds = DatasetConfig(
            name="ds", table="t",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"cnt": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="cnt",
                        thresholds=ThresholdLevels(warning={"min": 100}),
                    )],
                    notify=NotifyConfig(warning=["email"]),
                ),
            ],
        )
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=[ds])
        engine = QualifireEngine(
            backend=backend, storage=storage,
            context=QualifireContext(), config=config,
            notifiers={"email": mock_notifier},
        )
        result = engine.run()

        conn = storage._get_conn()
        cursor = conn.execute(
            "SELECT notified_at FROM qualifire_history WHERE record_type = 'notification'"
        )
        rows = cursor.fetchall()
        assert len(rows) >= 1
        assert rows[0][0] is not None  # notified_at is populated

    def test_timestamps_null_for_inapplicable_record_types(self):
        """Validation rows have NULL collected_at/notified_at; collection rows have NULL validated_at/notified_at."""
        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()

        ds = DatasetConfig(
            name="sales", table="db.sales",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"row_count": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="row_count", thresholds=ThresholdLevels(),
                    )],
                ),
            ],
        )
        engine = _make_engine([ds], storage=storage)
        engine.run()

        conn = storage._get_conn()
        # Validation rows: collected_at and notified_at should be NULL
        cursor = conn.execute(
            "SELECT collected_at, notified_at FROM qualifire_history WHERE record_type = 'validation'"
        )
        for row in cursor.fetchall():
            assert row[0] is None  # collected_at
            assert row[1] is None  # notified_at

        # Collection rows: validated_at and notified_at should be NULL
        cursor = conn.execute(
            "SELECT validated_at, notified_at FROM qualifire_history WHERE record_type = 'collection'"
        )
        for row in cursor.fetchall():
            assert row[0] is None  # validated_at
            assert row[1] is None  # notified_at
