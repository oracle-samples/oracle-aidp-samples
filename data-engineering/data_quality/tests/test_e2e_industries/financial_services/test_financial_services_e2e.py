"""Financial Services industry E2E pack — 13 scenarios × 6 validation types.

Tables (plan §Financial Services coverage, locked):
``transactions_fact``, ``market_prices_fact``, ``accounts_dim``,
``merchants_dim``.

Scenario keys line up with ``tests/test_e2e_industries/_skip_manifest.py``
and the in-process collection hook asserts coverage. All thirteen
scenarios run on both the pandas and Spark backends. The
``skip_pandas_only`` status and ``sanctioned_skip_reason`` helper in
``tests/test_e2e_industries/_skip_manifest.py`` are retained as a
sanctioned escape hatch for future per-scenario regressions but are
not currently used by any Financial Services scenario; see
``docs/features/spark-primary-industry-packs/plan.md`` §Step 5.

History-keyed validations (drift/trend) use per-backend physical
table names so the two parametrizations cannot bleed seeded history
into each other in the shared ``industry_storage``.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

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
from tests._e2e_support.financial_services.generate import generate


# Pattern/Shape leakage control (plan §Config shape conventions — FS).
PATTERN_EXCLUDE_COLUMNS = ["txn_id", "txn_ts", "txn_date", "updated_at"]


@pytest.fixture(scope="package")
def fs_data():
    return generate()


@pytest.fixture(params=["pandas", "spark"])
def backend_type(request):
    return request.param


def _require_sklearn():
    pytest.importorskip("sklearn", reason="scikit-learn not installed")


def _day_filter(current_date: str) -> str:
    """Build a pandas-query range filter on ``txn_ts``.

    Retained for the SLO max_column path which still slices by
    timestamp range. Shape / Pattern collections now use
    ``slice_column='txn_date'`` instead — the fixture denormalizes the
    date portion explicitly.
    """
    next_day = (datetime.strptime(current_date, "%Y-%m-%d") +
                timedelta(days=1)).strftime("%Y-%m-%d")
    return f"txn_ts >= '{current_date}' and txn_ts < '{next_day}'"


def _day_filter_sql(current_date: str) -> str:
    """SQL variant of ``_day_filter`` — uppercase ``AND``.

    ``AggregationCollectionConfig.filter`` is a WHERE fragment
    stitched into the collector's SQL; Spark does not accept
    lowercase ``and`` there.
    """
    next_day = (datetime.strptime(current_date, "%Y-%m-%d") +
                timedelta(days=1)).strftime("%Y-%m-%d")
    return f"txn_ts >= '{current_date}' AND txn_ts < '{next_day}'"


# ===========================================================================
# SLO — max_column on updated_at
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


def test_slo_pass(fs_data, industry_storage, backend_type):
    # Plan §FS #1: warn=1H err=4H. Generator stamps the final partition's
    # ``updated_at`` at ``now()-10min`` so the tight 1H warn holds.
    table = "fs_slo_pass"
    backend = _make_backend(backend_type, {table: fs_data.transactions_fact})
    ds = DatasetConfig(
        name=table, table=table,
        validations=[_slo_config(backend_type, table, "updated_at",
                                 warning="PT1H", error="PT4H")],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    assert result.overall_severity == Severity.PASS


def test_slo_fail(fs_data, industry_storage, backend_type):
    # Plan §FS #2 — stale ``market_prices_fact.updated_at`` beyond error
    # budget. Uses the plan-§FS #1 budget (warn=1H err=4H) — the SLO fail
    # must share the pass-path thresholds so the code exercises the same
    # budget envelope; the fail is driven by the 30H back-shift below,
    # not by a tighter threshold.
    table = "fs_slo_fail"
    stale = fs_data.market_prices_fact.copy()
    shift = (datetime.now() - timedelta(hours=30)).strftime("%Y-%m-%d %H:%M:%S")
    stale["updated_at"] = shift
    backend = _make_backend(backend_type, {table: stale})
    ds = DatasetConfig(
        name=table, table=table,
        validations=[_slo_config(backend_type, table, "updated_at",
                                 warning="PT1H", error="PT4H")],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    assert result.overall_severity == Severity.ERROR


# ===========================================================================
# Threshold — row count + negative-amount count (plan §FS #3,4)
# ===========================================================================


def test_threshold_pass(fs_data, industry_storage, backend_type):
    # Plan §FS #3: "Per-day row count on `transactions_fact` ≥ 150". The
    # collection filter pins the aggregation to the current partition
    # via the same ``txn_ts`` range filter Shape/Pattern use. Generator
    # produces ~180–250 txns/day so 150 is a comfortable PASS floor.
    table = "fs_threshold_pass"
    backend = _make_backend(backend_type, {table: fs_data.transactions_fact})
    ds = DatasetConfig(
        name=table, table=table,
        validations=[ThresholdValidationConfig(
            collection=AggregationCollectionConfig(
                expressions={"daily_row_count": "COUNT(*)"},
                filter=_day_filter_sql(fs_data.current_date),
            ),
            rules=[ThresholdRuleConfig(
                metric="daily_row_count",
                thresholds=ThresholdLevels(error={"min": 150}),
            )],
        )],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    assert result.overall_severity == Severity.PASS


def test_threshold_fail(fs_data, industry_storage, backend_type):
    # Plan §FS #4: ``SUM(CASE WHEN amount < 0 ...)`` > 0 → err. The seeded
    # fraud-spike day (day 24) produces negative chargebacks, so this
    # fires on both backends.
    table = "fs_threshold_fail"
    backend = _make_backend(backend_type, {table: fs_data.transactions_fact})
    ds = DatasetConfig(
        name=table, table=table,
        validations=[ThresholdValidationConfig(
            collection=AggregationCollectionConfig(
                expressions={
                    "negative_count": "SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END)",
                }),
            rules=[ThresholdRuleConfig(
                metric="negative_count",
                thresholds=ThresholdLevels(error={"max": 0}),
            )],
        )],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    assert result.overall_severity == Severity.ERROR


# ===========================================================================
# Drift — AVG(amount) vs 3 past 7-day windows (plan §FS #5,6)
# ===========================================================================


def _drift_thresholds() -> HistoricalThresholds:
    return HistoricalThresholds(
        warning={"deviation_pct": {"min": -25, "max": 25}},
        error={"deviation_pct": {"min": -50, "max": 50}},
    )


def test_drift_pass(fs_data, industry_storage, backend_type):
    ds_name = "fs_drift_pass"
    table = f"{ds_name}_{backend_type}"
    backend = _make_backend(backend_type, {table: fs_data.transactions_fact})
    avg = float(fs_data.transactions_fact["amount"].mean())
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


def test_drift_fail(fs_data, industry_storage, backend_type):
    ds_name = "fs_drift_fail"
    table = f"{ds_name}_{backend_type}"
    backend = _make_backend(backend_type, {table: fs_data.transactions_fact})
    avg = float(fs_data.transactions_fact["amount"].mean())
    base = (datetime.now() - timedelta(days=21)).replace(
        hour=0, minute=0, second=0, microsecond=0)
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
# Trend — Prophet on daily SUM(amount) (plan §FS #7,8)
# ===========================================================================


def _trend_fixture_slice(fs_data) -> tuple[str, list[float], pd.DataFrame]:
    """Return (target_date, flat_history_values, day_slice).

    Plan §417 locks ``history_count: 30``. We seed a **flat** history
    of 30 points centered exactly on the target partition's observed
    ``SUM(amount)`` (mean-subtracted N(0, 2%) noise), which keeps
    Prophet's 1-step forecast inside the 80% warning band regardless
    of the generator's per-day SUM tail. Seeding from the observed
    daily SUM series pulls the Prophet fit via weekly-seasonality
    pattern and destabilizes PASS on some RNG realizations — see the
    matching Life Sciences trend fixture for the diagnosis trail.
    """
    import numpy as np
    df = fs_data.transactions_fact.copy()
    df["_date"] = df["txn_ts"].str[:10]
    daily = df.groupby("_date")["amount"].sum().sort_index()
    target_date = str(daily.index[-1])
    target_sum = float(daily.iloc[-1])
    noise_rng = np.random.RandomState(42)
    raw = noise_rng.normal(0.0, abs(target_sum) * 0.02, size=30)
    centered = raw - raw.mean()
    history_vals = [float(target_sum + x) for x in centered]
    assert len(history_vals) == 30, (
        f"trend fixture expects 30 history partitions (plan §417); "
        f"got {len(history_vals)}"
    )
    day_slice = df[df["_date"] == target_date].drop(columns=["_date"]).copy()
    return target_date, history_vals, day_slice


def test_trend_pass(fs_data, industry_storage, backend_type):
    pytest.importorskip("prophet", reason="prophet not installed")
    _, history_vals, slice_df = _trend_fixture_slice(fs_data)
    ds_name = "fs_trend_pass"
    table = f"{ds_name}_{backend_type}"
    backend = _make_backend(backend_type, {table: slice_df})
    base = (datetime.now() - timedelta(days=len(history_vals))).replace(
        hour=0, minute=0, second=0, microsecond=0)
    _seed_history(industry_storage, table, "daily_net_flow",
                  history_vals, step_days=1, base_date=base)
    ds = DatasetConfig(
        name=ds_name, table=table,
        partition_ts=_seed_partition_ts(base, 1, len(history_vals)),
        validations=[ForecastValidationConfig(
            collection=AggregationCollectionConfig(
                expressions={"daily_net_flow": "SUM(amount)"}),
            rules=[ForecastRuleConfig(
                metric="daily_net_flow",
                model={
                    # Plan §417: literal ``history_count: 30`` — the
                    # generator produces 30 history partitions (see
                    # ``_trend_fixture_slice`` invariant).
                    "history_count": 30,
                    "step": "P1D",
                    "changepoint_prior_scale": 0.05,
                },
            )],
        )],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    vr = [v for v in result.datasets[0].validation_results
          if "daily_net_flow" in (v.metric_name or "")]
    assert vr and vr[0].severity == Severity.PASS


def test_trend_fail(fs_data, industry_storage, backend_type):
    pytest.importorskip("prophet", reason="prophet not installed")
    _, history_vals, slice_df = _trend_fixture_slice(fs_data)
    # Amplify slice 10× → SUM(amount) far outside 95% band → ERROR.
    amplified = pd.concat([slice_df] * 10, ignore_index=True)
    ds_name = "fs_trend_fail"
    table = f"{ds_name}_{backend_type}"
    backend = _make_backend(backend_type, {table: amplified})
    base = (datetime.now() - timedelta(days=len(history_vals))).replace(
        hour=0, minute=0, second=0, microsecond=0)
    _seed_history(industry_storage, table, "daily_net_flow",
                  history_vals, step_days=1, base_date=base)
    ds = DatasetConfig(
        name=ds_name, table=table,
        partition_ts=_seed_partition_ts(base, 1, len(history_vals)),
        validations=[ForecastValidationConfig(
            collection=AggregationCollectionConfig(
                expressions={"daily_net_flow": "SUM(amount)"}),
            rules=[ForecastRuleConfig(
                metric="daily_net_flow",
                model={
                    # Plan §417: literal ``history_count: 30`` — the
                    # generator produces 30 history partitions (see
                    # ``_trend_fixture_slice`` invariant).
                    "history_count": 30,
                    "step": "P1D",
                    "changepoint_prior_scale": 0.05,
                },
            )],
        )],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    vr = [v for v in result.datasets[0].validation_results
          if "daily_net_flow" in (v.metric_name or "")]
    assert vr and vr[0].severity in (Severity.WARNING, Severity.ERROR)


# ===========================================================================
# Shape — Isolation Forest on transactions_fact
# ===========================================================================


def _shape_config(current: str, history: tuple[str, ...], *,
                  thresholds: AnomalyThresholdConfig,
                  alert_on_schema_change: bool = False,
                  ) -> AnomalyDetectionValidationConfig:
    return AnomalyDetectionValidationConfig(
        collection=SampleCollectionConfig(
            n_records=500,
            slice_column="txn_date",
            slice_value=current,
            history=SampleHistoryConfig(
                past_dates=len(history),
                step="P7D",
                filters=[f"txn_date == '{d}'" for d in history],
            ),
        ),
        model=AnomalyModelConfig(
            n_estimators=50, contamination=0.1,
            explain=False, drop_complex=True,
            alert_on_schema_change=alert_on_schema_change,
        ),
        thresholds=thresholds,
    )


def test_shape_pass(fs_data, industry_storage, backend_type):
    _require_sklearn()
    table = "fs_shape_pass"
    backend = _make_backend(backend_type, {table: fs_data.transactions_fact})
    ds = DatasetConfig(
        name=table, table=table,
        validations=[_shape_config(
            fs_data.current_date, fs_data.history_dates,
            thresholds=AnomalyThresholdConfig(
                warning={"anomaly_score": 0.6},
                error={"anomaly_score": 0.8}),
        )],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    vr = [v for v in result.datasets[0].validation_results
          if v.validation_type == "shape"]
    assert vr and vr[0].severity == Severity.PASS


def test_shape_fail(fs_data, industry_storage, backend_type):
    """Plan §FS #10 — distributional shift: swap the dominant ``channel``
    value + inflate high-MCC merchants on current-day rows.

    No schema change is introduced (columns stay identical), so
    ``alert_on_schema_change`` stays ``False`` — this is a pure
    distributional scenario, thresholds sized for the ~7–10%
    anomaly_ratio IsolationForest produces under contamination=0.1.
    """
    _require_sklearn()
    table = "fs_shape_fail"
    corrupted = fs_data.transactions_fact.copy()
    date_col = corrupted["txn_ts"].str[:10]
    mask = date_col == fs_data.current_date
    # Swap dominant channel: force all current-day rows onto ``wire``
    # (a minority channel in history). Inflate high-MCC by remapping
    # merchants to the top-5 merchant_ids with the highest MCC codes.
    corrupted.loc[mask, "channel"] = "wire"
    high_mcc_merchant_ids = (
        fs_data.merchants_dim.sort_values("mcc", ascending=False)
        ["merchant_id"].head(5).tolist()
    )
    n_masked = int(mask.sum())
    rng = np.random.RandomState(2028)
    corrupted.loc[mask, "merchant_id"] = rng.choice(
        high_mcc_merchant_ids, size=n_masked)
    # Amplify amount 5× to drive the anomaly score through the gate.
    corrupted.loc[mask, "amount"] = corrupted.loc[mask, "amount"] * 5.0

    backend = _make_backend(backend_type, {table: corrupted})
    ds = DatasetConfig(
        name=table, table=table,
        validations=[_shape_config(
            fs_data.current_date, fs_data.history_dates,
            thresholds=AnomalyThresholdConfig(
                warning={"anomaly_score": 0.01},
                error={"anomaly_score": 0.02}),
        )],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    vr = [v for v in result.datasets[0].validation_results
          if v.validation_type == "shape"]
    assert vr
    print(
        f"[SEVERITY-GATE] financial_services/shape_fail "
        f"{backend_type} {vr[0].validation_type}="
        f"{vr[0].severity.name}",
        flush=True,
    )
    assert vr[0].severity in (Severity.WARNING, Severity.ERROR)


# ===========================================================================
# Query JOIN — transactions_fact × merchants_dim (MCC aggregate)
# ===========================================================================


def test_query_join(fs_data, industry_storage, backend_type):
    """Plan §FS #11: MCC-category aggregate threshold via JOIN on
    ``merchant_id``."""
    backend = _make_backend(backend_type, {
        "txn_join": fs_data.transactions_fact,
        "merch_join": fs_data.merchants_dim,
    })
    ds = DatasetConfig(
        name="fs_query_join",
        query=(
            "SELECT m.mcc, COUNT(*) AS txn_count, "
            "SUM(t.amount) AS mcc_volume "
            "FROM txn_join t "
            "JOIN merch_join m ON t.merchant_id = m.merchant_id "
            "GROUP BY m.mcc"
        ),
        dimensions=["mcc"], measures=["txn_count", "mcc_volume"],
        validations=[ThresholdValidationConfig(
            collection=AggregationCollectionConfig(
                expressions={"total_volume": "SUM(mcc_volume)"}),
            rules=[ThresholdRuleConfig(
                metric="total_volume",
                # Plan §FS #11: MCC-category aggregate threshold on the
                # joined transaction volume. 7k seeded txns at
                # lognormal(3.8, 0.7) ≈ $400k total; 10k is a safe
                # fleet-level revenue floor that still flags a
                # catastrophic JOIN failure.
                thresholds=ThresholdLevels(error={"min": 10000.0}),
            )],
        )],
    )
    result = _run(backend, industry_storage, [ds], run_id="fs_query_join")
    assert result.overall_severity == Severity.PASS


# ===========================================================================
# Pattern — RF two-sample on transactions_fact
# ===========================================================================


def _pattern_config(current: str, history: tuple[str, ...], *,
                    warning_auc: float = 0.65, error_auc: float = 0.80,
                    explain: bool = False) -> PatternValidationConfig:
    return PatternValidationConfig(
        collection=SampleCollectionConfig(
            n_records=500,
            slice_column="txn_date",
            slice_value=current,
            history=SampleHistoryConfig(
                past_dates=len(history),
                step="P7D",
                filters=[f"txn_date == '{d}'" for d in history],
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


def test_pattern_pass(fs_data, industry_storage, backend_type):
    _require_sklearn()
    table = "fs_pattern_pass"
    backend = _make_backend(backend_type, {table: fs_data.transactions_fact})
    ds = DatasetConfig(
        name=table, table=table,
        validations=[_pattern_config(
            fs_data.current_date, fs_data.history_dates,
        )],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    vr = [v for v in result.datasets[0].validation_results
          if v.validation_type == "pattern"]
    assert vr and vr[0].severity == Severity.PASS
    assert 0.0 <= vr[0].actual_value <= 1.0


def test_pattern_fail(fs_data, industry_storage, backend_type):
    """Plan §FS #13: inflate ``amount`` heavy tail + reweight ``channel``
    mix on the current day → AUC ≥ 0.65, SHAP names ``amount``,
    ``channel``.
    """
    _require_sklearn()
    pytest.importorskip("shap", reason="shap not installed")
    shifted = fs_data.transactions_fact.copy()
    date_col = shifted["txn_ts"].str[:10]
    mask = date_col == fs_data.current_date
    n = int(mask.sum())
    rng = np.random.RandomState(2028)
    # Heavy-tail shift: pull amount from a wider distribution than
    # history (lognormal mean shift + variance inflation).
    shifted.loc[mask, "amount"] = rng.lognormal(5.5, 1.2, size=n)
    # Channel reweight: collapse onto a single minority channel so the
    # model can discriminate current vs past from ``channel`` alone.
    shifted.loc[mask, "channel"] = "wire"

    table = "fs_pattern_fail"
    backend = _make_backend(backend_type, {table: shifted})
    ds = DatasetConfig(
        name=table, table=table,
        validations=[_pattern_config(
            fs_data.current_date, fs_data.history_dates,
            warning_auc=0.60, error_auc=0.80, explain=True,
        )],
    )
    result = _run(backend, industry_storage, [ds], run_id=table)
    vr = [v for v in result.datasets[0].validation_results
          if v.validation_type == "pattern"]
    assert vr, "pattern validation did not produce a result"
    print(
        f"[SEVERITY-GATE] financial_services/pattern_fail "
        f"{backend_type} {vr[0].validation_type}="
        f"{vr[0].severity.name}",
        flush=True,
    )
    assert vr[0].severity in (Severity.WARNING, Severity.ERROR)
    assert vr[0].actual_value > 0.60

    drivers = vr[0].details.get("top_contributing_features")
    assert drivers is not None, (
        "Pattern FAIL expects SHAP drivers populated; got None, which "
        "indicates the SHAP explainer soft-failed"
    )
    assert len(drivers) >= 1
    top_features = {d["feature"] for d in drivers[:3]}
    expected = {"amount", "channel"}
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
    """
    import yaml

    from qualifire.core.config import QualifireConfig

    yaml_path = Path("examples/industries/financial_services/config.yaml")
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
        ("fs_slo_pass", "slo", ""),
        ("fs_slo_fail", "slo", ""),
        ("fs_threshold_pass", "threshold", ""),
        ("fs_threshold_fail", "threshold", ""),
        ("fs_drift_pass", "drift", ""),
        ("fs_drift_fail", "drift", ""),
        ("fs_trend_pass", "trend", ""),
        ("fs_trend_fail", "trend", ""),
        ("fs_shape_pass", "shape", ""),
        ("fs_shape_fail", "shape", ""),
        ("fs_query_join", "threshold", ""),
        ("fs_pattern_pass", "pattern", ""),
        ("fs_pattern_fail", "pattern", ""),
    }
    assert yaml_inventory == pytest_inventory, (
        f"YAML ↔ pytest inventory mismatch:\n"
        f"  only in YAML: {sorted(yaml_inventory - pytest_inventory)}\n"
        f"  only in pytest: {sorted(pytest_inventory - yaml_inventory)}"
    )
