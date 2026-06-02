"""Retail industry E2E pack — 13 scenarios across the 6 validation types.

Each scenario name matches the ``scenario_key`` in
``tests/test_e2e_industries/_skip_manifest.py``; the in-process
collection hook in the parent ``conftest.py`` asserts this module
contains every expected ``(retail, scenario_key, backend)`` tuple.

All thirteen scenarios run on both the pandas and Spark backends.
The ``skip_pandas_only`` status and ``sanctioned_skip_reason`` helper
in ``tests/test_e2e_industries/_skip_manifest.py`` are retained as a
sanctioned escape hatch for future per-scenario regressions but are
not currently used by any Retail scenario; see
``docs/features/spark-primary-industry-packs/plan.md`` §Step 5.

Each scenario registers its data under a unique table name
(``retail_<scenario_key>``) so:
1. ``_seed_history`` rows don't bleed between scenarios in the
   shared ``industry_storage``.
2. Spark ``createOrReplaceTempView`` never aliases the same name
   across tests.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
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
    SampleCollectionConfig,
    SampleHistoryConfig,
    SLOValidationConfig,
    ThresholdLevels,
    ThresholdRuleConfig,
    ThresholdValidationConfig,
)
from qualifire.core.models import Severity
from tests._e2e_support.backends import (
    _make_backend, _run, _seed_history, _seed_partition_ts,
)
from tests._e2e_support.retail.generate import generate


# Pattern/Shape leakage control (plan §Coverage matrices).
PATTERN_EXCLUDE_COLUMNS = ["sale_id", "sale_date", "updated_at"]


@pytest.fixture(scope="package")
def retail_data():
    """One call per pack run (plan §B7 — generator lifecycle)."""
    return generate()


@pytest.fixture(params=["pandas", "spark"])
def backend_type(request):
    return request.param


def _require_sklearn():
    pytest.importorskip("sklearn", reason="scikit-learn not installed")


# ===========================================================================
# SLO — max_column freshness
# ===========================================================================


def _slo_config(backend_type: str, table: str, column: str, *,
                warning: str, error: str) -> SLOValidationConfig:
    if backend_type == "spark":
        return SLOValidationConfig(
            recency=RecencyCollectionConfig(strategy="max_column", column=column),
            thresholds={"warning": warning, "error": error},
        )
    return SLOValidationConfig(
        recency=RecencyCollectionConfig(
            strategy="custom_sql",
            sql=f"SELECT MAX({column}) FROM {table}",
        ),
        thresholds={"warning": warning, "error": error},
    )


def test_slo_pass(retail_data, industry_storage, backend_type):
    # Plan §Retail #1: warn=4H err=8H. The generator stamps the final
    # partition's ``updated_at`` at ``now()-10min`` so this budget
    # holds even if the SLO test runs late in the pack.
    table = "retail_slo_pass"
    backend = _make_backend(backend_type, {table: retail_data.sales_fact})
    ds = DatasetConfig(
        name=table, table=table,
        validations=[_slo_config(backend_type, table, "updated_at",
                                 warning="PT4H", error="PT8H")],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    assert result.overall_severity == Severity.PASS


def test_slo_fail(retail_data, industry_storage, backend_type):
    table = "retail_slo_fail"
    stale = retail_data.sales_fact.copy()
    shift = (datetime.now() - timedelta(hours=30)).strftime("%Y-%m-%d %H:%M:%S")
    stale["updated_at"] = shift
    backend = _make_backend(backend_type, {table: stale})
    ds = DatasetConfig(
        name=table, table=table,
        validations=[_slo_config(backend_type, table, "updated_at",
                                 warning="PT4H", error="PT8H")],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    assert result.overall_severity == Severity.ERROR


# ===========================================================================
# Threshold — static bounds
# ===========================================================================


def test_threshold_pass(retail_data, industry_storage, backend_type):
    # Plan §Retail #3: "`COUNT(*)` per day on `sales_fact` ≥ 100". The
    # collection filter pins the aggregation to the current partition;
    # generator produces ~150–210 sales/day so 100 is a comfortable
    # PASS floor.
    table = "retail_threshold_pass"
    backend = _make_backend(backend_type, {table: retail_data.sales_fact})
    ds = DatasetConfig(
        name=table, table=table,
        validations=[ThresholdValidationConfig(
            collection=AggregationCollectionConfig(
                expressions={"daily_row_count": "COUNT(*)"},
                filter=f"sale_date = '{retail_data.current_date}'",
            ),
            rules=[ThresholdRuleConfig(
                metric="daily_row_count",
                thresholds=ThresholdLevels(error={"min": 100}),
            )],
        )],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    assert result.overall_severity == Severity.PASS


def test_threshold_fail(retail_data, industry_storage, backend_type):
    """Plan §Retail #4: ``SUM(CASE WHEN product_id IS NULL ...)`` null
    rate ≤ 0.5%. The generator injects 1% null ``product_id``; the
    backend NaN→None normalization in ``_make_spark_backend`` keeps
    the ``IS NULL`` predicate consistent between pandas and Spark.
    """
    table = "retail_threshold_fail"
    backend = _make_backend(backend_type, {table: retail_data.sales_fact})
    ds = DatasetConfig(
        name=table, table=table,
        validations=[ThresholdValidationConfig(
            collection=AggregationCollectionConfig(
                expressions={
                    "null_pid_pct": (
                        "SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) "
                        "* 100.0 / COUNT(*)"
                    ),
                }),
            rules=[ThresholdRuleConfig(
                metric="null_pid_pct",
                thresholds=ThresholdLevels(error={"max": 0.5}),
            )],
        )],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    assert result.overall_severity == Severity.ERROR


# ===========================================================================
# Drift — z_score vs 3 past 7-day windows
# ===========================================================================
# Seeds are written against the per-scenario table name so one drift
# test's history doesn't pollute another's lookup. Timestamps anchor
# at now-21d so 7-day buckets align with a distinct bucket per seed.


def _drift_thresholds() -> HistoricalThresholds:
    return HistoricalThresholds(
        warning={"deviation_pct": {"min": -25, "max": 25}},
        error={"deviation_pct": {"min": -50, "max": 50}},
    )


def test_drift_pass(retail_data, industry_storage, backend_type):
    ds_name = "retail_drift_pass"
    table = f"{ds_name}_{backend_type}"  # see trend notes on per-backend history
    backend = _make_backend(backend_type, {table: retail_data.sales_fact})
    avg = float(retail_data.sales_fact["amount"].mean())
    base = (datetime.now() - timedelta(days=21)).replace(
        hour=0, minute=0, second=0, microsecond=0)
    _seed_history(industry_storage, table, "avg_amount",
                  [avg * 0.98, avg * 1.01, avg * 0.99],
                  step_days=7, base_date=base)
    ds = DatasetConfig(
        name=ds_name, table=table,
        partition_ts=_seed_partition_ts(base, 7, 3),
        validations=[HistoricalValidationConfig(
            collection=AggregationCollectionConfig(
                expressions={"avg_amount": "AVG(amount)"}),
            rules=[HistoricalRuleConfig(
                metric="avg_amount",
                compare=HistoricalCompareConfig(past_values=3, step="P7D"),
                thresholds=_drift_thresholds(),
            )],
        )],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    vr = [v for v in result.datasets[0].validation_results
          if "avg_amount" in (v.metric_name or "")]
    assert vr and vr[0].severity == Severity.PASS


def test_drift_fail(retail_data, industry_storage, backend_type):
    ds_name = "retail_drift_fail"
    table = f"{ds_name}_{backend_type}"
    backend = _make_backend(backend_type, {table: retail_data.sales_fact})
    avg = float(retail_data.sales_fact["amount"].mean())
    base = (datetime.now() - timedelta(days=21)).replace(
        hour=0, minute=0, second=0, microsecond=0)
    # Past at 3× current → deviation_pct ≈ 67%, ERROR.
    _seed_history(industry_storage, table, "avg_amount",
                  [avg * 3.0, avg * 3.0, avg * 3.0],
                  step_days=7, base_date=base)
    ds = DatasetConfig(
        name=ds_name, table=table,
        partition_ts=_seed_partition_ts(base, 7, 3),
        validations=[HistoricalValidationConfig(
            collection=AggregationCollectionConfig(
                expressions={"avg_amount": "AVG(amount)"}),
            rules=[HistoricalRuleConfig(
                metric="avg_amount",
                compare=HistoricalCompareConfig(past_values=3, step="P7D"),
                thresholds=_drift_thresholds(),
            )],
        )],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    vr = [v for v in result.datasets[0].validation_results
          if "avg_amount" in (v.metric_name or "")]
    assert vr and vr[0].severity == Severity.ERROR


# ===========================================================================
# Trend — Prophet with 30-day seeded history (plan §Config shape conventions)
# ===========================================================================


def _trend_fixture_slice(retail_data) -> tuple[str, list[float], "pd.DataFrame"]:
    """Return (target_date, flat_history_values, target_day_slice).

    Plan §417 locks ``history_count: 30`` in the trend config. We seed
    a **flat** history of 30 points centered on the target partition's
    observed ``SUM(amount)`` with mean-subtracted N(0, 2%) noise, so
    Prophet's one-step forecast stays inside the 80% warning band
    regardless of the generator's per-day revenue tail. Harmonizes
    with the Life Sciences and Financial Services trend fixtures —
    seeding from the observed daily-SUM series destabilizes PASS on
    some RNG realizations because Prophet's auto-enabled weekly
    seasonality latches onto end-of-series patterns. The flat prior
    still exercises the Prophet fit + band-computation + severity
    path, which is the part of the validator under test.

    ``trend_fail`` uses the same fixture; its slice has ``amount``
    amplified 10× so the actual far exceeds the forecast band
    regardless of the flat-history predicted value.
    """
    import numpy as np
    import pandas as pd  # noqa: F401 — kept for type annotation readability
    daily_rev = retail_data.sales_fact.groupby("sale_date")["amount"].sum().sort_index()
    target_date = str(daily_rev.index[-1])
    target_sum = float(daily_rev.iloc[-1])
    # Noise seed ≠ generator SEED=2026. Seed 42 is verified to yield a
    # predicted value inside the forecast band for PASS.
    noise_rng = np.random.RandomState(42)
    raw = noise_rng.normal(0.0, abs(target_sum) * 0.02, size=30)
    centered = raw - raw.mean()
    history_vals = [float(target_sum + x) for x in centered]
    assert len(history_vals) == 30, (
        f"trend fixture expects 30 history partitions (plan §417); "
        f"got {len(history_vals)} — generator NUM_DAYS has drifted"
    )
    target_slice = retail_data.sales_fact[
        retail_data.sales_fact["sale_date"] == target_date
    ].copy()
    return target_date, history_vals, target_slice


def test_trend_pass(retail_data, industry_storage, backend_type):
    pytest.importorskip("prophet", reason="prophet not installed")
    _, history_vals, mid_slice = _trend_fixture_slice(retail_data)
    # Physical table is backend-suffixed so the two parametrizations
    # cannot read each other's seeded-history rows. ``logical_table``
    # (per engine.py:240) is ``ds.table``, so this also keys the
    # forecast validator's history read to a per-backend namespace.
    ds_name = "retail_trend_pass"
    table = f"{ds_name}_{backend_type}"
    backend = _make_backend(backend_type, {table: mid_slice})
    base = (datetime.now() - timedelta(days=len(history_vals))).replace(
        hour=0, minute=0, second=0, microsecond=0)
    _seed_history(industry_storage, table, "daily_revenue",
                  history_vals, step_days=1, base_date=base)
    ds = DatasetConfig(
        name=ds_name, table=table,
        partition_ts=_seed_partition_ts(base, 1, len(history_vals)),
        validations=[ForecastValidationConfig(
            collection=AggregationCollectionConfig(
                expressions={"daily_revenue": "SUM(amount)"}),
            rules=[ForecastRuleConfig(
                metric="daily_revenue",
                model={
                    # Plan §417: literal `history_count: 30` to match
                    # the seeded 30-day training window. Generator
                    # NUM_DAYS=31 yields exactly 30 history values
                    # when mid_date is excluded; assert invariant so
                    # future generator changes don't silently drift.
                    "history_count": 30,
                    "step": "P1D",
                    # Harmonized with LS/FS trend tests — tight CPS
                    # keeps the forecast stable across seed variance.
                    "changepoint_prior_scale": 0.05,
                },
            )],
        )],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    vr = [v for v in result.datasets[0].validation_results
          if "daily_revenue" in (v.metric_name or "")]
    assert vr and vr[0].severity == Severity.PASS


def test_trend_fail(retail_data, industry_storage, backend_type):
    pytest.importorskip("prophet", reason="prophet not installed")
    _, history_vals, mid_slice = _trend_fixture_slice(retail_data)
    mid_slice["amount"] = mid_slice["amount"] * 10.0  # force outside 95% band
    ds_name = "retail_trend_fail"
    table = f"{ds_name}_{backend_type}"
    backend = _make_backend(backend_type, {table: mid_slice})
    base = (datetime.now() - timedelta(days=len(history_vals))).replace(
        hour=0, minute=0, second=0, microsecond=0)
    _seed_history(industry_storage, table, "daily_revenue",
                  history_vals, step_days=1, base_date=base)
    ds = DatasetConfig(
        name=ds_name, table=table,
        partition_ts=_seed_partition_ts(base, 1, len(history_vals)),
        validations=[ForecastValidationConfig(
            collection=AggregationCollectionConfig(
                expressions={"daily_revenue": "SUM(amount)"}),
            rules=[ForecastRuleConfig(
                metric="daily_revenue",
                model={
                    # Plan §417: literal `history_count: 30` to match
                    # the seeded 30-day training window. Generator
                    # NUM_DAYS=31 yields exactly 30 history values
                    # when mid_date is excluded; assert invariant so
                    # future generator changes don't silently drift.
                    "history_count": 30,
                    "step": "P1D",
                    # Harmonized with LS/FS trend tests — tight CPS
                    # keeps the forecast stable across seed variance.
                    "changepoint_prior_scale": 0.05,
                },
            )],
        )],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    vr = [v for v in result.datasets[0].validation_results
          if "daily_revenue" in (v.metric_name or "")]
    assert vr and vr[0].severity in (Severity.WARNING, Severity.ERROR)


# ===========================================================================
# Shape — Isolation Forest
# ===========================================================================


def _shape_config(current: str, history: tuple[str, ...],
                  *, thresholds: AnomalyThresholdConfig,
                  alert_on_schema_change: bool = False,
                  ) -> AnomalyDetectionValidationConfig:
    return AnomalyDetectionValidationConfig(
        collection=SampleCollectionConfig(
            n_records=500,
            slice_column="sale_date", slice_value=current,
            history=SampleHistoryConfig(
                past_dates=len(history),
                step="P7D",
                filters=[f"sale_date == '{d}'" for d in history],
            ),
        ),
        model=AnomalyModelConfig(
            n_estimators=50, contamination=0.1,
            explain=False, drop_complex=True,
            alert_on_schema_change=alert_on_schema_change,
        ),
        thresholds=thresholds,
    )


def test_shape_pass(retail_data, industry_storage, backend_type):
    _require_sklearn()
    table = "retail_shape_pass"
    backend = _make_backend(backend_type, {table: retail_data.sales_fact})
    ds = DatasetConfig(
        name=table, table=table,
        validations=[_shape_config(
            retail_data.current_date, retail_data.history_dates,
            thresholds=AnomalyThresholdConfig(
                warning={"anomaly_score": 0.6},
                error={"anomaly_score": 0.8}),
        )],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    vr = [v for v in result.datasets[0].validation_results
          if v.validation_type == "shape"]
    assert vr and vr[0].severity == Severity.PASS


def test_shape_fail(retail_data, industry_storage, backend_type):
    """Plan §Retail #10 — distributional shift on the current day
    (``currency`` nulled + ``amount`` amplified 10×) drives the
    IsolationForest anomaly_ratio above the warning threshold.

    **Why no ``alert_on_schema_change``**: literal schema drift
    (current and past samples with different column sets) is
    architecturally impossible through ``SampleCollectionConfig``
    because the sampler pulls both current and past partitions
    from the same physical table via
    ``backend.sample_records(table, n, filter)`` — a pandas
    DataFrame has one column set regardless of the row filter.
    The nulled ``currency`` values and the amplified ``amount``
    still produce a distributional drift the validator catches
    via encoded-column anomaly scores. The schema-change alert
    branch itself is covered by
    ``tests/test_validation/test_isolation_forest.py::test_alert_on_schema_change``.
    """
    _require_sklearn()
    table = "retail_shape_fail"
    corrupted = retail_data.sales_fact.copy()
    mask = corrupted["sale_date"] == retail_data.current_date
    corrupted.loc[mask, "currency"] = None
    corrupted.loc[mask, "amount"] = corrupted.loc[mask, "amount"] * 10.0

    backend = _make_backend(backend_type, {table: corrupted})
    ds = DatasetConfig(
        name=table, table=table,
        validations=[_shape_config(
            retail_data.current_date, retail_data.history_dates,
            # IsolationForest with contamination=0.1 caps flags at ~10%
            # of current rows. The corruption pushes anomaly_ratio to
            # ~7%, so the warning threshold must sit well below that.
            thresholds=AnomalyThresholdConfig(
                warning={"anomaly_score": 0.03},
                error={"anomaly_score": 0.06}),
        )],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    vr = [v for v in result.datasets[0].validation_results
          if v.validation_type == "shape"]
    assert vr
    print(
        f"[SEVERITY-GATE] retail/shape_fail "
        f"{backend_type} {vr[0].validation_type}="
        f"{vr[0].severity.name}",
        flush=True,
    )
    assert vr[0].severity in (Severity.WARNING, Severity.ERROR)


# ===========================================================================
# Query JOIN
# ===========================================================================


def test_query_join(retail_data, industry_storage, backend_type):
    """Plan §Retail #11 — ``sales_fact × products_dim`` on ``product_id``,
    aggregate revenue by ``category``, threshold ≥ €X. The threshold
    gates the revenue roll-up (``SUM(amount)``) rather than the row
    count so the JOIN's output measure is what triggers PASS/FAIL,
    matching the plan's revenue framing."""
    backend = _make_backend(backend_type, {
        "sales_fact_join": retail_data.sales_fact,
        "products_dim_join": retail_data.products_dim,
    })
    ds = DatasetConfig(
        name="retail_query_join",
        query=(
            "SELECT p.category, SUM(s.amount) AS category_revenue "
            "FROM sales_fact_join s JOIN products_dim_join p "
            "ON s.product_id = p.product_id "
            "GROUP BY p.category"
        ),
        dimensions=["category"], measures=["category_revenue"],
        validations=[ThresholdValidationConfig(
            collection=AggregationCollectionConfig(
                expressions={"total_revenue": "SUM(category_revenue)"}),
            rules=[ThresholdRuleConfig(
                metric="total_revenue",
                # Plan §Retail #11: revenue threshold ≥ €X. The generator
                # produces ~200+ orders/day × ~€25 avg amount × ≥30
                # days ≈ €150k+ total; €10k is a comfortable PASS
                # floor that still catches a collapsed JOIN (empty
                # or single-category result).
                thresholds=ThresholdLevels(error={"min": 10000.0}),
            )],
        )],
    )
    result = _run(backend, industry_storage, [ds], run_id="retail_query_join")
    assert result.overall_severity == Severity.PASS


# ===========================================================================
# Pattern
# ===========================================================================


def _pattern_config(current: str, history: tuple[str, ...], *,
                    warning_auc: float = 0.65, error_auc: float = 0.80,
                    explain: bool = False) -> PatternValidationConfig:
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
            n_estimators=50, cv_folds=2,
            explain=explain, drop_complex=True,
            exclude_columns=PATTERN_EXCLUDE_COLUMNS,
        ),
        thresholds=PatternThresholdConfig(
            warning={"auc": warning_auc}, error={"auc": error_auc},
        ),
    )


def test_pattern_pass(retail_data, industry_storage, backend_type):
    _require_sklearn()
    table = "retail_pattern_pass"
    backend = _make_backend(backend_type, {table: retail_data.sales_fact})
    ds = DatasetConfig(
        name=table, table=table,
        validations=[_pattern_config(
            retail_data.current_date, retail_data.history_dates,
        )],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    vr = [v for v in result.datasets[0].validation_results
          if v.validation_type == "pattern"]
    assert vr and vr[0].severity == Severity.PASS
    assert 0.0 <= vr[0].actual_value <= 1.0


def test_pattern_fail(retail_data, industry_storage, backend_type):
    _require_sklearn()
    pytest.importorskip("shap", reason="shap not installed")
    # Inflate amount tail + shift quantity upward on the current day.
    shifted = retail_data.sales_fact.copy()
    mask = shifted["sale_date"] == retail_data.current_date
    n = int(mask.sum())
    rng = np.random.RandomState(2026)
    shifted.loc[mask, "amount"] = rng.uniform(500, 2000, size=n)
    shifted.loc[mask, "quantity"] = rng.randint(10, 30, size=n)

    table = "retail_pattern_fail"
    backend = _make_backend(backend_type, {table: shifted})
    ds = DatasetConfig(
        name=table, table=table,
        validations=[_pattern_config(
            retail_data.current_date, retail_data.history_dates,
            warning_auc=0.60, error_auc=0.80, explain=True,
        )],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    vr = [v for v in result.datasets[0].validation_results
          if v.validation_type == "pattern"]
    assert vr, "pattern validation did not produce a result"
    print(
        f"[SEVERITY-GATE] retail/pattern_fail "
        f"{backend_type} {vr[0].validation_type}="
        f"{vr[0].severity.name}",
        flush=True,
    )
    assert vr[0].severity in (Severity.WARNING, Severity.ERROR)
    assert vr[0].actual_value > 0.60

    # Plan §B8: SHAP drivers assertion is "not None", not just "present".
    drivers = vr[0].details.get("top_contributing_features")
    assert drivers is not None, (
        "Pattern FAIL expects SHAP drivers populated; got None, which "
        "indicates the SHAP explainer soft-failed"
    )
    assert len(drivers) >= 1
    top_features = {d["feature"] for d in drivers[:3]}
    expected = {"amount", "quantity"}
    assert top_features & expected, (
        f"Expected top-3 SHAP features to include one of {expected}, "
        f"got {top_features}"
    )


# ===========================================================================
# YAML ↔ pytest inventory parity (plan §B3 — inventory-only)
# ===========================================================================


def test_config_yaml_inventory_matches_pytest_scenarios():
    """Asserts tracked YAML and pytest scenarios share the same
    ``(dataset, validation.type, validation.name)`` inventory triples.
    Inventory only — thresholds / filters / hyperparameters not compared.
    """
    import yaml

    from qualifire.core.config import QualifireConfig

    yaml_path = Path("examples/industries/retail/config.yaml")
    with yaml_path.open() as fh:
        raw = yaml.safe_load(fh)
    loaded = QualifireConfig.model_validate(raw)

    yaml_inventory: set[tuple[str, str, str]] = set()
    for ds in loaded.datasets:
        for val in ds.validations:
            vtype = getattr(val, "type", None)
            vname = getattr(val, "name", None) or ""
            yaml_inventory.add((ds.name, str(vtype), vname))

    pytest_inventory = {
        ("retail_slo_pass", "slo", ""),
        ("retail_slo_fail", "slo", ""),
        ("retail_threshold_pass", "threshold", ""),
        ("retail_threshold_fail", "threshold", ""),
        ("retail_drift_pass", "drift", ""),
        ("retail_drift_fail", "drift", ""),
        ("retail_trend_pass", "trend", ""),
        ("retail_trend_fail", "trend", ""),
        ("retail_shape_pass", "shape", ""),
        ("retail_shape_fail", "shape", ""),
        ("retail_query_join", "threshold", ""),
        ("retail_pattern_pass", "pattern", ""),
        ("retail_pattern_fail", "pattern", ""),
    }
    assert yaml_inventory == pytest_inventory, (
        f"YAML ↔ pytest inventory mismatch:\n"
        f"  only in YAML: {sorted(yaml_inventory - pytest_inventory)}\n"
        f"  only in pytest: {sorted(pytest_inventory - yaml_inventory)}"
    )
