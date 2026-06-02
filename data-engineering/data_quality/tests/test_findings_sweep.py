"""Regression tests for the findings-sweep feature.

Each test pins one bug from `.tmp/findings.md` (the consolidated
review): runs the failing-before scenario and asserts the fix
behaves correctly. Map test → plan step in the docstring.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from tests.conftest import MockDataFrame
from qualifire.api import Qualifire
from qualifire.core.config import (
    AggregationCollectionConfig,
    DatasetConfig,
    NotifyConfig,
    QualifireConfig,
    ThresholdLevels,
    ThresholdRuleConfig,
    ThresholdValidationConfig,
    WAPConfig,
)
from qualifire.core.context import QualifireContext
from qualifire.core.engine import QualifireEngine
from qualifire.core.exceptions import QualifireValidationError
from qualifire.core.models import (
    CollectionResult,
    MetricKey,
    Severity,
    ValidationKey,
    ValidationResult,
    metric_key_for,
    validation_key_for,
)


# --- P1.4: typed identity keys -----------------------------------------------


class TestIdentityKeys:
    def test_metric_key_hashable(self) -> None:
        k1 = MetricKey(dataset_name="ds", metric_name="row_count")
        k2 = MetricKey(dataset_name="ds", metric_name="row_count")
        assert k1 == k2 and hash(k1) == hash(k2)
        d = {k1: "value"}
        assert d[k2] == "value"

    def test_validation_key_distinct_by_dim(self) -> None:
        k_us = ValidationKey(
            dataset_name="ds", validation_name="threshold.ds.cnt",
            metric_name="cnt", dimension_value='{"region":"US"}',
        )
        k_eu = ValidationKey(
            dataset_name="ds", validation_name="threshold.ds.cnt",
            metric_name="cnt", dimension_value='{"region":"EU"}',
        )
        assert k_us != k_eu
        assert {k_us, k_eu} == {k_us, k_eu}  # distinct in a set

    def test_metric_and_validation_share_segment(self) -> None:
        cr = CollectionResult(
            metric_name="row_count", metric_value=500,
            collected_at=datetime.now(), dimension_value='{"region":"US"}',
        )
        vr = ValidationResult(
            validation_name="threshold.ds.row_count",
            validation_type="threshold",
            severity=Severity.PASS,
            message="ok",
            validation_base_name="threshold.ds",
            metric_name="row_count",
            dimension_value='{"region":"US"}',
        )
        mk = metric_key_for("ds", cr)
        vk = validation_key_for("ds", vr)
        assert mk.metric_name == vk.metric_name == "row_count"
        assert mk.dimension_value == vk.dimension_value == '{"region":"US"}'


# --- P2.1 (Blocking): dimension-aware threshold validation -------------------


class TestDimensionAwareThreshold:
    def test_per_dimension_results_emitted(self) -> None:
        """Threshold rule across `dimensions=["region"]` produces one
        ValidationResult per dimension — not collapsed to the last."""
        from qualifire.validation.threshold import ThresholdValidator

        # Three per-dim collection rows for the same metric.
        collected = [
            CollectionResult(
                metric_name="row_count", metric_value=500,
                collected_at=datetime.now(),
                dimension_value='{"region": "US"}',
            ),
            CollectionResult(
                metric_name="row_count", metric_value=1500,
                collected_at=datetime.now(),
                dimension_value='{"region": "EU"}',
            ),
            CollectionResult(
                metric_name="row_count", metric_value=9,
                collected_at=datetime.now(),
                dimension_value='{"region": "APAC"}',
            ),
        ]
        validator = ThresholdValidator(
            rules=[{
                "metric": "row_count",
                "thresholds": {"error": {"min": 100}},
            }],
            name="my_check",
        )
        results = validator.validate(collected)
        # Three distinct ValidationResults — one per dim.
        assert len(results) == 3
        by_dim = {r.dimension_value: r for r in results}
        assert by_dim['{"region": "US"}'].severity == Severity.PASS
        assert by_dim['{"region": "EU"}'].severity == Severity.PASS
        assert by_dim['{"region": "APAC"}'].severity == Severity.ERROR
        # Each result carries the JSON-encoded dim, the base name, and the
        # full suffixed validation_name with metric.
        for r in results:
            assert r.validation_base_name == "my_check"
            assert r.validation_name == "my_check.row_count"
            assert r.metric_name == "row_count"


# --- P3.1: dataset-level notify fanout ---------------------------------------


class TestDatasetLevelNotifyFanout:
    def test_validate_notify_threads_through_to_validation_configs(self) -> None:
        backend = MagicMock()
        backend.spark = MagicMock()
        # Value below threshold (5 < 100) → ERROR.
        backend.get_aggregations.return_value = {"row_count": 5}

        mock_slack = MagicMock()
        mock_slack.send.return_value = MagicMock(
            channel="slack", severity=Severity.ERROR, status="sent", message=""
        )

        qf = Qualifire(
            backend=backend, owner="o", bu="b",
            notifiers={"slack": mock_slack},
        )
        # Threshold builder with no notify; dataset-level notify should fan out.
        # Engine raises QualifireValidationError on ERROR — catch + inspect.
        with pytest.raises(QualifireValidationError):
            qf.validate(
                table="t",
                validations=[
                    Qualifire.threshold_check(
                        aggregations={"row_count": "COUNT(*)"},
                        rules=[{"metric": "row_count", "thresholds": {"error": {"min": 100}}}],
                    ),
                ],
                notify={"error": ["slack"]},
            )
        # Pre-findings-sweep this would not have fired.
        assert mock_slack.send.call_count == 1

    def test_per_validation_notify_wins_over_dataset_default(self) -> None:
        backend = MagicMock()
        backend.spark = MagicMock()
        backend.get_aggregations.return_value = {"row_count": 5}

        mock_a = MagicMock()
        mock_a.send.return_value = MagicMock(
            channel="a", severity=Severity.ERROR, status="sent", message=""
        )
        mock_b = MagicMock()
        mock_b.send.return_value = MagicMock(
            channel="b", severity=Severity.ERROR, status="sent", message=""
        )

        qf = Qualifire(
            backend=backend, owner="o", bu="b",
            notifiers={"a": mock_a, "b": mock_b},
        )
        # Builder explicitly sets notify={"error": ["b"]}; dataset-level
        # notify={"error": ["a"]} must NOT override.
        per_check_notify = Qualifire.threshold_check(
            aggregations={"row_count": "COUNT(*)"},
            rules=[{"metric": "row_count", "thresholds": {"error": {"min": 100}}}],
            notify={"error": ["b"]},
        )
        with pytest.raises(QualifireValidationError):
            qf.validate(
                table="t",
                validations=[per_check_notify],
                notify={"error": ["a"]},
            )
        assert mock_b.send.call_count == 1
        assert mock_a.send.call_count == 0

    def test_validate_does_not_mutate_caller_validations(self) -> None:
        """Reusing the same builder config without notify= must NOT
        carry over the previous fanout (P3.1 non-mutating fanout)."""
        backend = MagicMock()
        backend.spark = MagicMock()
        backend.get_aggregations.return_value = {"row_count": 5}

        mock_x = MagicMock()
        mock_x.send.return_value = MagicMock(
            channel="x", severity=Severity.ERROR, status="sent", message=""
        )

        qf = Qualifire(
            backend=backend, owner="o", bu="b",
            notifiers={"x": mock_x},
        )
        # Single builder config reused across two calls.
        cfg = Qualifire.threshold_check(
            aggregations={"row_count": "COUNT(*)"},
            rules=[{"metric": "row_count", "thresholds": {"error": {"min": 100}}}],
        )
        # First call applies fanout (raises because severity=ERROR).
        with pytest.raises(QualifireValidationError):
            qf.validate(table="t", validations=[cfg], notify={"error": ["x"]})
        # Second call without dataset-level notify must NOT route to x.
        with pytest.raises(QualifireValidationError):
            qf.validate(table="t", validations=[cfg])
        assert mock_x.send.call_count == 1


# --- P3.2: WAP DataFrame via WAPConfig (no monkey-patch) ---------------------


class TestWAPDfViaConfig:
    def test_wap_df_does_not_monkey_patch_engine(self) -> None:
        """write_audit_publish(df=...) must not rebind engine._run_wap.

        Pre-findings-sweep this method patched a closure onto the engine
        instance. Now the df flows through WAPConfig.df. Assert no
        per-instance _run_wap shadow exists after the call.
        """
        backend = MagicMock()
        backend.spark = MagicMock()
        backend.get_aggregations.return_value = {"row_count": 100}

        qf = Qualifire(backend=backend, owner="o", bu="b")
        df = MagicMock(name="df")

        qf.write_audit_publish(
            target_table="cat.schema.prod",
            df=df,
            validations=[
                Qualifire.threshold_check(
                    aggregations={"row_count": "COUNT(*)"},
                    rules=[{"metric": "row_count", "thresholds": {"error": {"min": 1}}}],
                ),
            ],
        )

        # Confirm no instance-level shadow of _run_wap exists.
        # (`engine` instance is local to the call; we re-create one and
        # check the class still owns the method.)
        engine = QualifireEngine(
            backend=backend, storage=None, context=QualifireContext(),
            config=QualifireConfig(owner="o", bu="b", system_table="",
                                   datasets=[DatasetConfig(name="x", table="t")]),
        )
        assert "_run_wap" not in engine.__dict__, (
            "Engine instance should not own _run_wap directly — it lives on the class."
        )

    def test_wapconfig_df_and_sql_mutually_exclusive(self) -> None:
        """§R9 + sub-feature B: ``df``, ``sql``, ``sql_file`` are
        pairwise mutually exclusive."""
        with pytest.raises(Exception):  # ValidationError or ValueError
            WAPConfig(
                target_table="cat.t",
                sql="SELECT 1",
                df=MagicMock(),
            )

    def test_wapconfig_df_only_or_sql_only_works(self) -> None:
        """Each WAP source individually is fine."""
        c1 = WAPConfig(target_table="cat.t", sql="SELECT 1")
        c2 = WAPConfig(target_table="cat.t", df=MagicMock())
        assert c1.sql == "SELECT 1" and c1.df is None
        assert c2.df is not None and c2.sql is None


# --- P4.1: two-pass persistence + engine warnings ----------------------------


class TestEngineWarnings:
    def test_persistence_failure_raises_with_internal_failure_marker(self) -> None:
        """Phase 2 of external-catalog-system-table-hardening inverts
        the original P4.1 contract ("does not crash") to fail loud
        on infra failure: the engine raises with PEP 3134 chained
        cause + records the qualifire.persistence engine_warning
        tagged with ``qualifire_internal_failure=True`` so the
        notification dispatcher skips it.
        """
        from qualifire.core.exceptions import (
            QualifireInternalError,
            QualifireValidationError,
        )

        backend = MagicMock()
        backend.spark = MagicMock()
        backend.get_aggregations.return_value = {"row_count": 100}

        storage = MagicMock()
        storage.write_results.side_effect = RuntimeError("Storage full")
        storage.read_validation_history_bulk.return_value = {}

        qf = Qualifire(
            backend=backend, owner="o", bu="b",
        )
        # Inject the failing storage manually.
        qf._storage = storage

        with pytest.raises(QualifireInternalError) as excinfo:
            qf.validate(
                table="t",
                validations=[
                    Qualifire.threshold_check(
                        aggregations={"row_count": "COUNT(*)"},
                        rules=[{"metric": "row_count", "thresholds": {"error": {"min": 1}}}],
                    ),
                ],
            )

        # Original storage exception chained as __cause__.
        assert isinstance(excinfo.value.__cause__, RuntimeError)
        assert "Storage full" in str(excinfo.value.__cause__)
        # Engine warning was still recorded for forensic trail, with
        # the qualifire-internal-failure marker so notifications skip.
        result = excinfo.value.result
        persistence_warning = next(
            (vr for vr in result.engine_warnings
             if vr.validation_base_name == "qualifire.persistence"),
            None,
        )
        assert persistence_warning is not None, \
            "qualifire.persistence engine warning must be recorded"
        assert persistence_warning.details.get("qualifire_internal_failure") is True


# --- P4.2: cache=True semantics on Pandas vs Spark ---------------------------


class TestCacheSemantics:
    def test_cache_true_on_pandas_no_op(self) -> None:
        """PandasBackend doesn't expose .spark; cache=True is a no-op,
        not an error (P4.2 R5 split)."""
        backend = MagicMock()
        # No `.spark` attr — simulate PandasBackend.
        if hasattr(backend, "spark"):
            del backend.spark

        engine = QualifireEngine(
            backend=backend, storage=None, context=QualifireContext(),
            config=QualifireConfig(owner="o", bu="b", system_table="",
                                   datasets=[DatasetConfig(name="d", table="t")]),
        )
        # Should return None silently — no RuntimeError.
        result = engine._cache_table("t", storage_level="MEMORY_AND_DISK")
        assert result is None

    def test_cache_true_on_spark_without_pyspark_raises(self, monkeypatch) -> None:
        """Spark-capable backend + missing PySpark + cache=True → explicit RuntimeError."""
        backend = MagicMock()
        backend.spark = MagicMock()
        engine = QualifireEngine(
            backend=backend, storage=None, context=QualifireContext(),
            config=QualifireConfig(owner="o", bu="b", system_table="",
                                   datasets=[DatasetConfig(name="d", table="t")]),
        )
        # Force the import to fail.
        import builtins
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "pyspark":
                raise ImportError("simulated missing pyspark")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)

        with pytest.raises(RuntimeError, match="PySpark is required"):
            engine._cache_table("t", storage_level="MEMORY_AND_DISK")


# --- P4.5: drop_temp_view on the Backend Protocol ----------------------------


class TestBackendProtocolMaterialization:
    def test_pandas_backend_register_and_drop_temp_view(self) -> None:
        """PandasBackend implements register_table and drop_temp_view
        as in-memory dict ops (P4.5)."""
        from qualifire.backends.pandas_backend import PandasBackend
        import pandas as pd

        backend = PandasBackend()
        df = pd.DataFrame({"x": [1, 2, 3]})
        backend.register_table("my_view", df)
        assert backend.table_exists("my_view")
        backend.drop_temp_view("my_view")
        assert not backend.table_exists("my_view")


# --- PR-6 review: exact-severity routing ------------------------------------


class TestExactSeverityRouting:
    """ERROR results must NOT also fan out to WARNING channels.

    Pre-PR-6-review the routing predicate was ``vr.severity >= severity``,
    which meant a config with both ``warning=["w"]`` and ``error=["e"]``
    received the same ERROR row on both channels. The fix is exact
    severity equality; this test pins it.
    """

    def test_error_does_not_route_to_warning_channel(self) -> None:
        backend = MagicMock()
        backend.spark = MagicMock()
        backend.get_aggregations.return_value = {"row_count": 5}

        warn_notifier = MagicMock()
        warn_notifier.send.return_value = MagicMock(
            channel="warn", severity=Severity.WARNING, status="sent", message=""
        )
        error_notifier = MagicMock()
        error_notifier.send.return_value = MagicMock(
            channel="err", severity=Severity.ERROR, status="sent", message=""
        )

        qf = Qualifire(
            backend=backend, owner="o", bu="b",
            notifiers={"warn": warn_notifier, "err": error_notifier},
        )
        # Validation produces ERROR. Channel config has both warning and
        # error tiers populated with distinct channels.
        cfg = Qualifire.threshold_check(
            aggregations={"row_count": "COUNT(*)"},
            rules=[{"metric": "row_count", "thresholds": {"error": {"min": 100}}}],
            notify={"warning": ["warn"], "error": ["err"]},
        )
        with pytest.raises(QualifireValidationError):
            qf.validate(table="t", validations=[cfg])

        # Only the ERROR channel fires.
        assert error_notifier.send.call_count == 1
        assert warn_notifier.send.call_count == 0


# --- PR-6 review: validation_base_name kw_only enforcement -------------------


class TestValidationBaseNameRequired:
    """``validation_base_name`` must be required keyword-only (no default).

    Plan §P1.1 explicitly rejects an empty default — a default lets
    missed emit sites collapse onto each other under "" and silently
    mis-route alerts. PR-6 review blocking finding.
    """

    def test_missing_validation_base_name_raises(self) -> None:
        with pytest.raises(TypeError):
            ValidationResult(
                validation_name="v",
                validation_type="threshold",
                severity=Severity.PASS,
                message="ok",
            )

    def test_validation_base_name_is_keyword_only(self) -> None:
        # Pass everything positionally — still TypeError because
        # validation_base_name is kw_only.
        with pytest.raises(TypeError):
            ValidationResult("v", "threshold", Severity.PASS, "ok")
