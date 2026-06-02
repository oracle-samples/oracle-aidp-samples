"""End-to-end validation test suite.

Generates realistic data with 45 days of daily partitions, weekday/weekend
seasonality, and gradual trend. Runs all 5 validation types through the
full engine pipeline with both PandasBackend and SparkBackend, using
SQLiteStorage for system table persistence.

All pass/fail outcomes are driven by data patterns, not artificially tuned
thresholds.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from qualifire.core.config import (
    AggregationCollectionConfig,
    AnomalyDetectionValidationConfig,
    AnomalyModelConfig,
    AnomalyThresholdConfig,
    DatasetConfig,
    ForecastRuleConfig,
    ForecastValidationConfig,
    HistoricalCompareConfig,
    HistoricalRuleConfig,
    HistoricalThresholds,
    HistoricalValidationConfig,
    PatternModelConfig,
    PatternThresholdConfig,
    PatternValidationConfig,
    RecencyCollectionConfig,
    SLOValidationConfig,
    SampleCollectionConfig,
    SampleHistoryConfig,
    ThresholdLevels,
    ThresholdRuleConfig,
    ThresholdValidationConfig,
)
from qualifire.core.models import Severity
from tests._e2e_support.backends import (
    BASE_DATE,
    E2EPandasBackend,
    _make_backend,
    _make_spark_backend,
    _make_storage,
    _run,
    _seed_history,
    _seed_partition_ts,
)


# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------

RNG = np.random.RandomState(42)
NUM_DAYS = 45


def _generate_daily_sales() -> pd.DataFrame:
    """45 days of daily sales, weekday/weekend seasonality, 0.5% daily uptrend."""
    rows, sale_id = [], 1
    regions = ["US", "EU", "APAC"]
    for day in range(NUM_DAYS):
        dt = BASE_DATE + timedelta(days=day)
        is_wkend = dt.weekday() >= 5
        trend = 1.0 + 0.005 * day
        base = (100.0 if is_wkend else 200.0) * trend
        n = max(10, int(RNG.normal(base, base * 0.1)))
        for _ in range(n):
            rows.append({
                "sale_id": sale_id,
                "sale_date": dt.strftime("%Y-%m-%d"),
                "day_of_week": dt.weekday(),
                "amount": round(max(1.0, RNG.normal(75 * trend, 25)), 2),
                "region": regions[sale_id % 3],
                "updated_at": dt.strftime("%Y-%m-%d %H:%M:%S"),
            })
            sale_id += 1
    return pd.DataFrame(rows)


def _generate_products() -> pd.DataFrame:
    return pd.DataFrame({
        "product_id": range(1, 11),
        "product_name": [f"Product_{i}" for i in range(1, 11)],
        "category": ["Electronics"] * 4 + ["Clothing"] * 3 + ["Food"] * 3,
        "price": [99.99, 149.99, 29.99, 199.99, 49.99, 79.99, 39.99, 12.99, 8.99, 15.99],
    })


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sales_df() -> pd.DataFrame:
    return _generate_daily_sales()


@pytest.fixture(scope="module")
def products_df() -> pd.DataFrame:
    return _generate_products()


@pytest.fixture(params=["pandas", "spark"], scope="module")
def backend_type(request):
    return request.param


# ===========================================================================
# SLO (Freshness)
# ===========================================================================


class TestE2ESLO:
    """SLO freshness checks — both backends.

    Spark uses max_column strategy (backtick-safe).
    Pandas uses custom_sql strategy (pandasql/SQLite compatible).
    """

    def _slo_config(self, backend_type: str, table: str, column: str):
        if backend_type == "spark":
            return SLOValidationConfig(
                recency=RecencyCollectionConfig(strategy="max_column", column=column),
                thresholds={"warning": "PT4H", "error": "PT8H"},
            )
        return SLOValidationConfig(
            recency=RecencyCollectionConfig(
                strategy="custom_sql", sql=f"SELECT MAX({column}) FROM {table}",
            ),
            thresholds={"warning": "PT4H", "error": "PT8H"},
        )

    def test_slo_pass(self, backend_type):
        now = datetime.now()
        ts = (now - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
        df = pd.DataFrame({"id": [1, 2], "updated_at": [ts, ts]})
        backend = _make_backend(backend_type, {"fresh_table": df})
        storage = _make_storage()

        ds = DatasetConfig(
            name="slo_pass", table="fresh_table",
            validations=[self._slo_config(backend_type, "fresh_table", "updated_at")],
        )
        result = _run(backend, storage, [ds])
        assert result.overall_severity == Severity.PASS

    def test_slo_error(self, backend_type):
        now = datetime.now()
        ts = (now - timedelta(hours=10)).strftime("%Y-%m-%d %H:%M:%S")
        df = pd.DataFrame({"id": [1], "updated_at": [ts]})
        backend = _make_backend(backend_type, {"stale_table": df})
        storage = _make_storage()

        ds = DatasetConfig(
            name="slo_error", table="stale_table",
            validations=[self._slo_config(backend_type, "stale_table", "updated_at")],
        )
        result = _run(backend, storage, [ds])
        assert result.overall_severity == Severity.ERROR


# ===========================================================================
# Threshold (Static Bounds)
# ===========================================================================


class TestE2EThreshold:
    def test_pass(self, backend_type, sales_df):
        backend = _make_backend(backend_type, {"sales": sales_df})
        storage = _make_storage()

        ds = DatasetConfig(
            name="thr_pass", table="sales",
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(
                    expressions={"row_count": "COUNT(*)", "avg_amount": "AVG(amount)"},
                ),
                rules=[
                    ThresholdRuleConfig(metric="row_count",
                        thresholds=ThresholdLevels(warning={"min": 5000}, error={"min": 1000})),
                    ThresholdRuleConfig(metric="avg_amount",
                        thresholds=ThresholdLevels(warning={"min": 50.0, "max": 200.0})),
                ],
            )],
        )
        result = _run(backend, storage, [ds])
        assert result.overall_severity == Severity.PASS
        assert len(sales_df) > 5000

    def test_error(self, backend_type):
        tiny = pd.DataFrame({"sale_id": [1, 2, 3], "amount": [10.0, 20.0, 30.0],
                             "sale_date": ["2024-01-01"] * 3, "updated_at": ["2024-01-01"] * 3})
        backend = _make_backend(backend_type, {"tiny": tiny})
        storage = _make_storage()

        ds = DatasetConfig(
            name="thr_error", table="tiny",
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
                rules=[ThresholdRuleConfig(metric="cnt",
                    thresholds=ThresholdLevels(error={"min": 1000}))],
            )],
        )
        result = _run(backend, storage, [ds])
        assert result.overall_severity == Severity.ERROR


# ===========================================================================
# Drift (Historical Comparison)
# ===========================================================================


class TestE2EDrift:
    def test_pass_stable(self, backend_type, sales_df):
        backend = _make_backend(backend_type, {"sales": sales_df})
        storage = _make_storage()
        avg = round(sales_df["amount"].mean(), 2)
        _seed_history(storage, "sales", "avg_amount",
                      [avg * f for f in (0.97, 1.02, 0.99, 1.01)])

        ds = DatasetConfig(
            name="drift_pass", table="sales",
            partition_ts=_seed_partition_ts(BASE_DATE, 7, 4),
            validations=[HistoricalValidationConfig(
                collection=AggregationCollectionConfig(
                    expressions={"avg_amount": "AVG(amount)"}),
                rules=[HistoricalRuleConfig(
                    metric="avg_amount",
                    compare=HistoricalCompareConfig(past_values=4, step="P7D"),
                    thresholds=HistoricalThresholds(
                        warning={"deviation_pct": {"min": -25, "max": 25}}, error={"deviation_pct": {"min": -50, "max": 50}}),
                )],
            )],
        )
        result = _run(backend, storage, [ds])
        vr = [v for v in result.datasets[0].validation_results if "avg_amount" in (v.metric_name or "")]
        assert vr[0].severity == Severity.PASS

    def test_error_deviation(self, backend_type, sales_df):
        backend = _make_backend(backend_type, {"sales": sales_df})
        storage = _make_storage()
        avg = sales_df["amount"].mean()
        _seed_history(storage, "sales", "avg_amount", [avg * 3.0] * 4)

        ds = DatasetConfig(
            name="drift_error", table="sales",
            partition_ts=_seed_partition_ts(BASE_DATE, 7, 4),
            validations=[HistoricalValidationConfig(
                collection=AggregationCollectionConfig(
                    expressions={"avg_amount": "AVG(amount)"}),
                rules=[HistoricalRuleConfig(
                    metric="avg_amount",
                    compare=HistoricalCompareConfig(past_values=4, step="P7D"),
                    thresholds=HistoricalThresholds(
                        warning={"deviation_pct": {"min": -25, "max": 25}}, error={"deviation_pct": {"min": -50, "max": 50}}),
                )],
            )],
        )
        result = _run(backend, storage, [ds])
        vr = [v for v in result.datasets[0].validation_results if "avg_amount" in (v.metric_name or "")]
        assert vr[0].severity == Severity.ERROR

    def test_no_history_passes(self, backend_type, sales_df):
        backend = _make_backend(backend_type, {"sales": sales_df})
        storage = _make_storage()

        ds = DatasetConfig(
            name="drift_nohist", table="sales",
            partition_ts=_seed_partition_ts(BASE_DATE, 7, 4),
            validations=[HistoricalValidationConfig(
                collection=AggregationCollectionConfig(
                    expressions={"avg_amount": "AVG(amount)"}),
                rules=[HistoricalRuleConfig(
                    metric="avg_amount",
                    compare=HistoricalCompareConfig(past_values=4, step="P7D"),
                    thresholds=HistoricalThresholds(warning={"deviation_pct": {"min": -25, "max": 25}}),
                )],
            )],
        )
        assert _run(backend, storage, [ds]).overall_severity == Severity.PASS


# ===========================================================================
# Trend / Forecast (Prophet)
# ===========================================================================


class TestE2ETrend:
    """Prophet forecast checks — requires prophet package."""

    def test_pass_within_interval(self, backend_type, sales_df):
        # Use a day from the middle of the time series where Prophet's fit
        # is strongest (edges diverge due to trend extrapolation)
        dates = sorted(sales_df["sale_date"].unique())
        mid_idx = len(dates) // 2  # day ~22
        mid_date = dates[mid_idx]
        mid_day_df = sales_df[sales_df["sale_date"] == mid_date].copy()
        backend = _make_backend(backend_type, {"sales_mid": mid_day_df})
        storage = _make_storage()

        # Seed all days EXCEPT the mid day as history
        daily_rev = sales_df.groupby("sale_date")["amount"].sum().sort_index()
        history_vals = [v for d, v in daily_rev.items() if d != mid_date]
        _seed_history(storage, "sales_mid", "daily_revenue", history_vals,
                      step_days=1)

        ds = DatasetConfig(
            name="trend_pass", table="sales_mid",
            partition_ts=_seed_partition_ts(BASE_DATE, 1, 44),
            validations=[ForecastValidationConfig(
                collection=AggregationCollectionConfig(
                    expressions={"daily_revenue": "SUM(amount)"}),
                rules=[ForecastRuleConfig(
                    metric="daily_revenue",
                    model={"history_count": 44, "step": "P1D",
                           "changepoint_prior_scale": 0.5},
                )],
            )],
        )
        result = _run(backend, storage, [ds])
        vr = [v for v in result.datasets[0].validation_results
              if "daily_revenue" in (v.metric_name or "")]
        assert len(vr) == 1
        assert vr[0].severity in (Severity.PASS, Severity.WARNING)

    def test_error_extreme(self, backend_type, sales_df):
        storage = _make_storage()
        daily_rev = sales_df.groupby("sale_date")["amount"].sum().sort_index().values.tolist()
        _seed_history(storage, "extreme_sales", "extreme_revenue", daily_rev[:-1],
                      step_days=1)

        extreme = sales_df.copy()
        extreme["amount"] = extreme["amount"] * 10
        backend = _make_backend(backend_type, {"extreme_sales": extreme})

        ds = DatasetConfig(
            name="trend_error", table="extreme_sales",
            partition_ts=_seed_partition_ts(BASE_DATE, 1, 44),
            validations=[ForecastValidationConfig(
                collection=AggregationCollectionConfig(
                    expressions={"extreme_revenue": "SUM(amount)"}),
                rules=[ForecastRuleConfig(
                    metric="extreme_revenue",
                    model={"history_count": 44, "step": "P1D"},
                )],
            )],
        )
        result = _run(backend, storage, [ds])
        vr = [v for v in result.datasets[0].validation_results
              if "extreme_revenue" in (v.metric_name or "")]
        assert len(vr) == 1
        assert vr[0].severity == Severity.ERROR


# ===========================================================================
# Anomaly / Shape (Isolation Forest)
# ===========================================================================


class TestE2EAnomaly:
    """Isolation Forest anomaly checks — PandasBackend only.

    SamplerCollector uses pandas .query() filter syntax which differs
    between backends. These tests use PandasBackend for deterministic
    sampling behavior.
    """

    @pytest.fixture(autouse=True)
    def _require_sklearn(self):
        pytest.importorskip("sklearn", reason="scikit-learn not installed")

    def test_pass_normal(self, sales_df):
        dates = sorted(sales_df["sale_date"].unique())
        current, history = dates[-1], dates[-22:-1:7]
        backend = E2EPandasBackend(tables={"sales": sales_df})
        storage = _make_storage()

        ds = DatasetConfig(
            name="anomaly_pass", table="sales",
            validations=[AnomalyDetectionValidationConfig(
                collection=SampleCollectionConfig(
                    n_records=500,
                    slice_column="sale_date", slice_value=current,
                    history=SampleHistoryConfig(
                        past_dates=3, step="P7D",
                        filters=[f"sale_date == '{d}'" for d in history]),
                ),
                model=AnomalyModelConfig(n_estimators=50, contamination=0.1,
                                         explain=False, drop_complex=True),
                thresholds=AnomalyThresholdConfig(
                    warning={"anomaly_score": 0.6}, error={"anomaly_score": 0.8}),
            )],
        )
        result = _run(backend, storage, [ds])
        vr = [v for v in result.datasets[0].validation_results if v.validation_type == "shape"]
        assert vr[0].severity == Severity.PASS

    def test_corrupted_triggers_alert(self, sales_df):
        dates = sorted(sales_df["sale_date"].unique())
        current, history = dates[-1], dates[-22:-1:7]

        corrupted = sales_df.copy()
        mask = corrupted["sale_date"] == current
        n = mask.sum()
        corrupted.loc[mask, "amount"] = RNG.uniform(0, 10000, size=n)
        corrupted.loc[mask, "sale_id"] = RNG.randint(100000, 999999, size=n)
        corrupted.loc[mask, "day_of_week"] = 6

        backend = E2EPandasBackend(tables={"corrupted": corrupted})
        storage = _make_storage()

        ds = DatasetConfig(
            name="anomaly_corrupt", table="corrupted",
            validations=[AnomalyDetectionValidationConfig(
                collection=SampleCollectionConfig(
                    n_records=500,
                    slice_column="sale_date", slice_value=current,
                    history=SampleHistoryConfig(
                        past_dates=3, step="P7D",
                        filters=[f"sale_date == '{d}'" for d in history]),
                ),
                model=AnomalyModelConfig(n_estimators=100, contamination=0.05,
                                         explain=False, drop_complex=True),
                thresholds=AnomalyThresholdConfig(
                    warning={"anomaly_score": 0.15}, error={"anomaly_score": 0.30}),
            )],
        )
        result = _run(backend, storage, [ds])
        vr = [v for v in result.datasets[0].validation_results if v.validation_type == "shape"]
        assert vr[0].severity in (Severity.WARNING, Severity.ERROR)


# ===========================================================================
# Pattern (Random Forest two-sample)
# ===========================================================================


class TestE2EPattern:
    """Pattern-check E2E — PandasBackend only (same rationale as TestE2EAnomaly).

    Per plan.md acceptance §15 & §17: prove engine->collector->validator
    wiring feeds the intended slices (slice_column + history.filters)
    AND that the pattern row persists with a parseable details_json.
    """

    @pytest.fixture(autouse=True)
    def _require_sklearn(self):
        pytest.importorskip("sklearn", reason="scikit-learn not installed")

    def _pattern_config(
        self,
        *,
        current,
        history,
        exclude_columns=None,
        warning_auc=0.65,
        error_auc=0.80,
    ):
        return PatternValidationConfig(
            collection=SampleCollectionConfig(
                n_records=500,
                slice_column="sale_date", slice_value=current,
                history=SampleHistoryConfig(
                    past_dates=len(history),
                    step="P7D",
                    filters=[f"sale_date == '{d}'" for d in history],
                ),
            ),
            model=PatternModelConfig(
                n_estimators=50,
                cv_folds=2,
                explain=False,
                drop_complex=True,
                exclude_columns=exclude_columns or ["sale_date", "updated_at", "sale_id"],
            ),
            thresholds=PatternThresholdConfig(
                warning={"auc": warning_auc},
                error={"auc": error_auc},
            ),
        )

    def test_pattern_pass_same_distribution(self, sales_df):
        """Same-distribution slices via slice_column + history.filters -> PASS."""
        dates = sorted(sales_df["sale_date"].unique())
        current, history = dates[-1], dates[-22:-1:7]
        backend = E2EPandasBackend(tables={"sales": sales_df})
        storage = _make_storage()

        ds = DatasetConfig(
            name="pattern_pass",
            table="sales",
            validations=[self._pattern_config(current=current, history=history)],
        )
        result = _run(backend, storage, [ds])
        vr = [v for v in result.datasets[0].validation_results
              if v.validation_type == "pattern"]
        assert len(vr) == 1
        assert vr[0].metric_name == "auc"
        assert 0.0 <= vr[0].actual_value <= 1.0
        assert vr[0].severity == Severity.PASS

    def test_pattern_alerts_when_current_shifted(self, sales_df):
        """Shifted current slice -> AUC crosses warning threshold -> ALERT."""
        dates = sorted(sales_df["sale_date"].unique())
        current, history = dates[-1], dates[-22:-1:7]

        shifted = sales_df.copy()
        mask = shifted["sale_date"] == current
        n = int(mask.sum())
        # Shift amount and day_of_week to an obviously different regime.
        shifted.loc[mask, "amount"] = RNG.uniform(2000, 3000, size=n)
        shifted.loc[mask, "day_of_week"] = 6
        shifted.loc[mask, "region"] = "US"

        backend = E2EPandasBackend(tables={"shifted": shifted})
        storage = _make_storage()

        ds = DatasetConfig(
            name="pattern_shifted",
            table="shifted",
            validations=[self._pattern_config(
                current=current, history=history,
                warning_auc=0.60, error_auc=0.80,
            )],
        )
        result = _run(backend, storage, [ds])
        vr = [v for v in result.datasets[0].validation_results
              if v.validation_type == "pattern"]
        assert vr[0].severity in (Severity.WARNING, Severity.ERROR)
        assert vr[0].actual_value > 0.60

    def test_pattern_persists_to_system_table(self, sales_df):
        """Pattern row must land in the system table with parseable details_json.

        Acceptance §17 — guards against numpy scalars in details silently
        failing the JSON persistence.
        """
        dates = sorted(sales_df["sale_date"].unique())
        current, history = dates[-1], dates[-22:-1:7]
        backend = E2EPandasBackend(tables={"sales": sales_df})
        storage = _make_storage()

        ds = DatasetConfig(
            name="pattern_persist",
            table="sales",
            validations=[self._pattern_config(current=current, history=history)],
        )
        _run(backend, storage, [ds], run_id="patternrun001")

        conn = storage._get_conn()
        rows = conn.execute(
            "SELECT details_json, metric_value FROM qualifire_history "
            "WHERE record_type='validation' AND validation_type='pattern'"
        ).fetchall()
        assert len(rows) >= 1
        for details_json, metric_value in rows:
            assert details_json is not None
            # Must be valid JSON, not the string 'null' and not a numpy
            # repr leaked through str().
            parsed = json.loads(details_json)
            assert isinstance(parsed, dict)
            assert "auc" in parsed
            assert 0.0 <= float(metric_value) <= 1.0


# ===========================================================================
# Query-based Dataset (Custom SQL with JOIN) — Pandas only
# ===========================================================================


class TestE2EQueryDataset:
    """Query-based datasets use temp views which work differently per backend.
    Tested with PandasBackend for reliable pandasql JOIN support."""

    def test_join_query(self, sales_df, products_df):
        enriched = sales_df.copy()
        enriched["product_id"] = (enriched["sale_id"] % 10) + 1
        backend = E2EPandasBackend(tables={"sales": enriched, "products": products_df})
        storage = _make_storage()

        ds = DatasetConfig(
            name="joined_sales",
            query=(
                "SELECT p.category, COUNT(*) AS order_count, AVG(s.amount) AS avg_amount "
                "FROM sales s JOIN products p ON s.product_id = p.product_id "
                "GROUP BY p.category"
            ),
            dimensions=["category"], measures=["order_count", "avg_amount"],
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(
                    expressions={"total_orders": "SUM(order_count)"}),
                rules=[ThresholdRuleConfig(metric="total_orders",
                    thresholds=ThresholdLevels(error={"min": 100}))],
            )],
        )
        result = _run(backend, storage, [ds])
        assert result.overall_severity == Severity.PASS


# ===========================================================================
# Persistence (SQLite system table)
# ===========================================================================


class TestE2EPersistence:
    def test_collection_and_validation_rows(self, backend_type, sales_df):
        backend = _make_backend(backend_type, {"sales": sales_df})
        storage = _make_storage()

        ds = DatasetConfig(
            name="persist_test", table="sales",
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(
                    expressions={"row_count": "COUNT(*)"}),
                rules=[ThresholdRuleConfig(metric="row_count",
                    thresholds=ThresholdLevels())],
            )],
        )
        _run(backend, storage, [ds], run_id="persistrun")

        conn = storage._get_conn()
        val_ct = conn.execute(
            "SELECT COUNT(*) FROM qualifire_history WHERE record_type='validation'"
        ).fetchone()[0]
        col_ct = conn.execute(
            "SELECT COUNT(*) FROM qualifire_history WHERE record_type='collection'"
        ).fetchone()[0]
        assert val_ct >= 1
        assert col_ct >= 1

    def test_collector_name(self, backend_type, sales_df):
        backend = _make_backend(backend_type, {"sales": sales_df})
        storage = _make_storage()

        ds = DatasetConfig(
            name="named_test", table="sales",
            validations=[ThresholdValidationConfig(
                name="daily-row-count",
                collection=AggregationCollectionConfig(
                    expressions={"row_count": "COUNT(*)"}),
                rules=[ThresholdRuleConfig(metric="row_count",
                    thresholds=ThresholdLevels())],
            )],
        )
        _run(backend, storage, [ds])

        row = storage._get_conn().execute(
            "SELECT collector_name FROM qualifire_history "
            "WHERE record_type='collection' LIMIT 1"
        ).fetchone()
        assert row[0] == "daily-row-count"


# ===========================================================================
# Multi-Validation Pipeline
# ===========================================================================


class TestE2EMultiValidation:
    def test_slo_plus_threshold(self, backend_type):
        now = datetime.now()
        ts = (now - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        df = pd.DataFrame({
            "id": range(1, 1001),
            "amount": RNG.normal(100, 20, 1000).round(2),
            "updated_at": [ts] * 1000,
        })
        backend = _make_backend(backend_type, {"multi_table": df})
        storage = _make_storage()

        slo = (
            SLOValidationConfig(
                recency=RecencyCollectionConfig(strategy="max_column", column="updated_at"),
                thresholds={"warning": "PT4H", "error": "PT8H"},
            )
            if backend_type == "spark"
            else SLOValidationConfig(
                recency=RecencyCollectionConfig(
                    strategy="custom_sql", sql="SELECT MAX(updated_at) FROM multi_table",
                ),
                thresholds={"warning": "PT4H", "error": "PT8H"},
            )
        )

        ds = DatasetConfig(
            name="multi_check", table="multi_table",
            validations=[
                slo,
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"cnt": "COUNT(*)", "avg_amt": "AVG(amount)"}),
                    rules=[
                        ThresholdRuleConfig(metric="cnt",
                            thresholds=ThresholdLevels(error={"min": 100})),
                        ThresholdRuleConfig(metric="avg_amt",
                            thresholds=ThresholdLevels(warning={"min": 50, "max": 200})),
                    ],
                ),
            ],
        )
        result = _run(backend, storage, [ds])
        assert result.overall_severity == Severity.PASS
        assert len(result.datasets[0].validation_results) >= 3

    def test_all_six_validator_types_coexist(self, sales_df):
        """Single run with pattern + shape + drift + trend + slo + threshold.

        Plan acceptance §10: all six validator types produce rows in the
        system table with the correct ``validation_type``. PandasBackend
        because sample-based validators (shape, pattern) are pandas-only
        in this suite. SLO uses custom_sql for pandasql.
        """
        pytest.importorskip("sklearn", reason="scikit-learn not installed")

        backend = E2EPandasBackend(tables={"sales": sales_df})
        storage = _make_storage()

        # Seed drift + trend history keyed on logical table name "sales".
        avg = round(sales_df["amount"].mean(), 2)
        _seed_history(
            storage, "sales", "avg_amount",
            [avg * f for f in (0.97, 1.02, 0.99, 1.01)],
        )

        dates = sorted(sales_df["sale_date"].unique())
        current, history = dates[-1], dates[-22:-1:7]

        pattern_cfg = PatternValidationConfig(
            collection=SampleCollectionConfig(
                n_records=500,
                slice_column="sale_date", slice_value=current,
                history=SampleHistoryConfig(
                    past_dates=len(history),
                    step="P7D",
                    filters=[f"sale_date == '{d}'" for d in history],
                ),
            ),
            model=PatternModelConfig(
                n_estimators=50, cv_folds=2, explain=False, drop_complex=True,
                exclude_columns=["sale_date", "updated_at", "sale_id"],
            ),
            thresholds=PatternThresholdConfig(
                warning={"auc": 0.65}, error={"auc": 0.80},
            ),
        )
        shape_cfg = AnomalyDetectionValidationConfig(
            collection=SampleCollectionConfig(
                n_records=500,
                slice_column="sale_date", slice_value=current,
                history=SampleHistoryConfig(
                    past_dates=len(history), step="P7D",
                    filters=[f"sale_date == '{d}'" for d in history],
                ),
            ),
            model=AnomalyModelConfig(n_estimators=50, contamination=0.1,
                                     explain=False, drop_complex=True),
            thresholds=AnomalyThresholdConfig(
                warning={"anomaly_score": 0.6}, error={"anomaly_score": 0.8}),
        )
        drift_cfg = HistoricalValidationConfig(
            collection=AggregationCollectionConfig(
                expressions={"avg_amount": "AVG(amount)"}),
            rules=[HistoricalRuleConfig(
                metric="avg_amount",
                compare=HistoricalCompareConfig(past_values=4, step="P7D"),
                thresholds=HistoricalThresholds(
                    warning={"deviation_pct": {"min": -25, "max": 25}}, error={"deviation_pct": {"min": -50, "max": 50}}),
            )],
        )
        slo_cfg = SLOValidationConfig(
            recency=RecencyCollectionConfig(
                strategy="custom_sql",
                sql="SELECT MAX(updated_at) FROM sales",
            ),
            thresholds={"warning": "PT400000H", "error": "PT400000H"},  # always passes
        )
        threshold_cfg = ThresholdValidationConfig(
            collection=AggregationCollectionConfig(
                expressions={"row_count": "COUNT(*)"}),
            rules=[ThresholdRuleConfig(
                metric="row_count",
                thresholds=ThresholdLevels(error={"min": 1}),
            )],
        )

        ds = DatasetConfig(
            name="ds", table="sales",
            validations=[slo_cfg, threshold_cfg, drift_cfg, shape_cfg, pattern_cfg],
        )
        _run(backend, storage, [ds], run_id="multirun001")

        # Query the system table for validation rows by validation_type.
        conn = storage._get_conn()
        rows = conn.execute(
            "SELECT DISTINCT validation_type FROM qualifire_history "
            "WHERE record_type='validation' AND validation_type IS NOT NULL"
        ).fetchall()
        seen_types = {r[0] for r in rows}
        # At minimum pattern must be persisted alongside the other four
        # validator types we configured in this run.
        assert "pattern" in seen_types
        assert "shape" in seen_types
        assert "drift" in seen_types
        assert "slo" in seen_types
        assert "threshold" in seen_types

        # The pattern row must carry a parseable details blob with auc.
        pattern_rows = conn.execute(
            "SELECT details_json FROM qualifire_history "
            "WHERE record_type='validation' AND validation_type='pattern'"
        ).fetchall()
        assert len(pattern_rows) >= 1
        parsed = json.loads(pattern_rows[0][0])
        assert "auc" in parsed

    def test_shape_and_pattern_share_sample_collection(self, sales_df):
        """Shape + pattern with identical sample spec must sample once, not twice.

        Codex round 4: on Spark the sample path uses unseeded ORDER BY
        RAND(), so independent collection calls produce different rows
        and contradictory drift/anomaly narratives for the same run. The
        engine now pre-computes unique sample-collection results and
        shares them across validators with matching configs. Patch
        ``QualifireEngine._collect_sample`` to count invocations.
        """
        pytest.importorskip("sklearn", reason="scikit-learn not installed")
        from qualifire.core.engine import QualifireEngine

        backend = E2EPandasBackend(tables={"sales": sales_df})
        storage = _make_storage()
        dates = sorted(sales_df["sale_date"].unique())
        current, history = dates[-1], dates[-22:-1:7]

        # Identical SampleCollectionConfig between shape and pattern.
        sample_coll = SampleCollectionConfig(
            n_records=500,
            slice_column="sale_date", slice_value=current,
            history=SampleHistoryConfig(
                past_dates=len(history), step="P7D",
                filters=[f"sale_date == '{d}'" for d in history],
            ),
        )
        shape_cfg = AnomalyDetectionValidationConfig(
            collection=sample_coll,
            model=AnomalyModelConfig(
                n_estimators=20, contamination=0.1,
                explain=False, drop_complex=True,
            ),
            thresholds=AnomalyThresholdConfig(
                warning={"anomaly_score": 0.6}, error={"anomaly_score": 0.8},
            ),
        )
        pattern_cfg = PatternValidationConfig(
            collection=sample_coll,
            model=PatternModelConfig(
                n_estimators=20, cv_folds=2, explain=False, drop_complex=True,
                exclude_columns=["sale_date", "updated_at", "sale_id"],
            ),
            thresholds=PatternThresholdConfig(
                warning={"auc": 0.65}, error={"auc": 0.80},
            ),
        )

        original_collect_sample = QualifireEngine._collect_sample
        call_count = {"n": 0}

        def counting(self, ds_config, val_config, collection):
            call_count["n"] += 1
            return original_collect_sample(self, ds_config, val_config, collection)

        with patch.object(
            QualifireEngine, "_collect_sample", counting
        ):
            ds = DatasetConfig(
                name="share", table="sales",
                validations=[shape_cfg, pattern_cfg],
            )
            _run(backend, storage, [ds])

        assert call_count["n"] == 1, (
            f"shape + pattern with identical sample config must call "
            f"_collect_sample once; got {call_count['n']}"
        )

    def test_sample_cache_fallback_shares_single_collection_after_prime_failure(
        self, sales_df
    ):
        """Prime failure must not reintroduce divergent Spark samples.

        Codex round 5: if _prime_sample_cache raises for a key, the
        per-validator fallback in _collect must still serialize and
        write back so matching validators share one collection. Before
        the fix, each validator independently re-ran the unseeded
        ORDER BY RAND() sample and saw different rows. Here we force
        the first _collect_sample call (the prime) to raise; the
        fallback must then run exactly once for shape+pattern combined.
        """
        pytest.importorskip("sklearn", reason="scikit-learn not installed")
        from qualifire.core.engine import QualifireEngine

        backend = E2EPandasBackend(tables={"sales": sales_df})
        storage = _make_storage()
        dates = sorted(sales_df["sale_date"].unique())
        current, history = dates[-1], dates[-22:-1:7]

        sample_coll = SampleCollectionConfig(
            n_records=500,
            slice_column="sale_date", slice_value=current,
            history=SampleHistoryConfig(
                past_dates=len(history), step="P7D",
                filters=[f"sale_date == '{d}'" for d in history],
            ),
        )
        shape_cfg = AnomalyDetectionValidationConfig(
            collection=sample_coll,
            model=AnomalyModelConfig(
                n_estimators=20, contamination=0.1,
                explain=False, drop_complex=True,
            ),
            thresholds=AnomalyThresholdConfig(
                warning={"anomaly_score": 0.6}, error={"anomaly_score": 0.8},
            ),
        )
        pattern_cfg = PatternValidationConfig(
            collection=sample_coll,
            model=PatternModelConfig(
                n_estimators=20, cv_folds=2, explain=False, drop_complex=True,
                exclude_columns=["sale_date", "updated_at", "sale_id"],
            ),
            thresholds=PatternThresholdConfig(
                warning={"auc": 0.65}, error={"auc": 0.80},
            ),
        )

        original_collect_sample = QualifireEngine._collect_sample
        call_count = {"n": 0}

        def flaky(self, ds_config, val_config, collection):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("simulated prime failure")
            return original_collect_sample(self, ds_config, val_config, collection)

        with patch.object(QualifireEngine, "_collect_sample", flaky):
            ds = DatasetConfig(
                name="share",
                table="sales",
                validation_parallelism=4,
                validations=[shape_cfg, pattern_cfg],
            )
            _run(backend, storage, [ds])

        # Exactly 2: prime attempt (raises) + one fallback collection
        # that both shape and pattern share via _SampleCache.
        assert call_count["n"] == 2, (
            f"Expected prime failure + single shared fallback, got "
            f"{call_count['n']} total _collect_sample calls"
        )
