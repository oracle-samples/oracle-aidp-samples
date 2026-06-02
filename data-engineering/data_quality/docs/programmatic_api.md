# Programmatic API

The `Qualifire` class provides the full programmatic interface with builder methods for each validation type.

## Initialization

There are two construction paths. Pick whichever matches your wiring:

### Constructing from YAML — `Qualifire.from_config`

The recommended path when your owner / bu / system_table /
system_table_backend / jdbc settings live in a YAML config:

```python
from qualifire import Qualifire
from qualifire.backends.spark_backend import SparkBackend

qf = Qualifire.from_config('configs/quality.yaml', backend=SparkBackend(spark))
qf.run_config('configs/quality.yaml', context={'ds': '2026-05-09'})
```

`from_config` lifts `owner`, `bu`, `system_table`,
`system_table_backend`, and `jdbc` from the loaded YAML onto the
instance. Per-config fields (`notifications`, `engine_notify`,
`partition_ts`, `partition_step`, `dataset_parallelism`,
`context`, `datasets`) stay attached to each run's config.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `config_path` | `str` | required | Path to the YAML config file |
| `backend` | `Backend \| None` | required | `SparkBackend(spark)`, `PandasBackend()`, or `None` for the SQLite-only path |
| `notifiers` | `dict[str, Notifier] \| None` | `None` | Runtime notifier instances; win over YAML's `notifications:` block on every call |
| `secret_resolver` | `SecretResolver \| None` | `None` | Pluggable secret resolver; passed through to the instance |
| `warn_on_reinit` | `bool` | `True` | Emit `QualifireReinitWarning` when a later call's YAML disagrees with the construction-time YAML on storage dimensions |
| `**overrides` | — | — | Any of `owner`, `bu`, `system_table`, `system_table_backend`, `jdbc` — wins over the YAML value. Unknown keys raise `TypeError`. |

### Direct construction (no YAML)

For purely programmatic setups where you don't have a YAML config:

```python
from qualifire import Qualifire
from qualifire.backends.spark_backend import SparkBackend

qf = Qualifire(
    backend=SparkBackend(spark),
    system_table="catalog.schema.qf_history",        # optional; required for system-table-backed validators
    system_table_backend="external_catalog",         # external_catalog | delta | sqlite | jdbc
    owner="team-name",
    bu="business-unit",
)
```

### Parameters (direct `Qualifire(...)`)

| Parameter | Type | Default | Description |
|---|---|---|---|
| `backend` | `Backend \| None` | required | `SparkBackend(spark)`, `PandasBackend()`, or `None` for the SQLite-only path |
| `system_table` | `str` | `None` | Fully-qualified system table name |
| `system_table_backend` | `str` | `"external_catalog"` | Storage backend type |
| `jdbc` | `JDBCConfig \| dict \| None` | `None` | JDBC connection settings; required when `system_table_backend="jdbc"` |
| `owner` | `str` | `""` | Owner/team identifier |
| `bu` | `str` | `""` | Business unit |
| `notifiers` | `dict[str, Notifier] \| None` | `None` | Runtime notifier instances; always win over YAML's `notifications:` block |
| `secret_resolver` | `SecretResolver \| None` | `None` | Pluggable secret resolver |
| `warn_on_reinit` | `bool` | `True` | Emit `QualifireReinitWarning` on storage swap during `run_config_parsed` / `backfill` / `deactivate_metric`. Pass `False` to silence when intentional. |

> **Notifier precedence**: programmatic notifiers (whether passed
> via `Qualifire(notifiers={...})`, `Qualifire.from_config(notifiers={...})`,
> or `qf.register_notifier(name, instance)`) ALWAYS win over a
> YAML `notifications:` block of the same name on every engine
> call. There is one consistent contract across all paths.

### JDBC Backend

```python
from qualifire import Qualifire
from qualifire.core.config import JDBCConfig
from qualifire.backends.spark_backend import SparkBackend

qf = Qualifire(
    backend=SparkBackend(spark),
    system_table="qf_history",
    system_table_backend="jdbc",
    jdbc=JDBCConfig(
        url="jdbc:postgresql://db.internal:5432/qualifire",
        user="qf_user",
        password="...",
        properties={"fetchsize": "1000"},
    ),
    owner="team",
    bu="finance",
)
```

Constructing `Qualifire` with `system_table_backend="jdbc"` but no
`jdbc=` kwarg raises `ValueError` with the same migration pointer
that config-load uses. Passing a plain dict (e.g. lifted from a parsed
YAML file) is also accepted — the constructor normalizes it to a
`JDBCConfig`.

## Core Methods

### `qf.validate()`

Run validations on a table or in-memory DataFrame.

```python
result = qf.validate(
    table="catalog.schema.sales",       # registered table/view name
    df=None,                              # optional: in-memory pandas/Spark DataFrame
    validations=[...],                    # list of validation configs
    name="my_check",                      # optional dataset name (defaults to table)
    filter_expr="date = '{{ ds }}'",      # optional WHERE clause
    notify={"warning": ["email"], "error": ["pager"]},  # dataset-level routing
    context={"ds": "2024-06-15"},         # optional extra Jinja variables
    partition_ts="event_dt",              # optional partition_ts expression for the run
)
```

Modes:

| Inputs | Behaviour |
|---|---|
| `table=` only | Validate a registered table / view. |
| `df=` + `name=` | Validate an in-memory DataFrame. `name` is **required** in this mode — it becomes the logical identity for system-table persistence and drift-history keying. |
| `table=` + `df=` | `df` supplies the data; `table` supplies the logical identity. |

`partition_ts=` is a Jinja-rendered SQL expression (literal like
`'2026-04-28'`, column ref like `event_dt`, or function like
`CAST(updated_at AS DATE)`). Drift / forecast / anomaly validators
require it to anchor their history reads — see
[`validators/partition_anchoring.md`](validators/partition_anchoring.md).
The value carries through to df-mode runs (the temp-view swap
preserves it).

Returns `QualifireResult`. Raises `QualifireValidationError` on
ERROR severity — wrap in `try`/`except` and inspect `e.result` if
you want to keep going.

### `qf.validate_query()`

SQL-driven variant of `validate()`. The query supplies the data;
optional `dimensions=` introduces GROUP BY columns automatically.

```python
result = qf.validate_query(
    query="SELECT region, AVG(amount) AS avg_amount FROM raw.sales WHERE ds = '{{ ds }}' GROUP BY region",
    name="sales_by_region",
    dimensions=["region"],
    validations=[...],
    partition_ts="'{{ ds }}'",
    context={"ds": "2024-06-15"},
)
```

Same severity / exception semantics as `validate()`.

### `qf.run_config()`

Run validations from a YAML/JSON config file.

```python
result = qf.run_config(
    config_path="config.yaml",
    context={"ds": "2024-06-15"},  # optional extra variables
)
```

### `qf.write_audit_publish()`

Run the WAP pattern.

```python
# DataFrame-driven
result = qf.write_audit_publish(
    df=my_dataframe,
    target_table="catalog.schema.sales_prod",
    write_options={"mode": "overwrite", "partitionBy": ["date"]},
    validations=[...],
    name="sales_wap",
)

# SQL-driven
result = qf.write_audit_publish(
    target_table="catalog.schema.sales_prod",
    sql="SELECT * FROM raw.sales WHERE date = '{{ ds }}'",
    validations=[...],
    context={"ds": "2024-06-15"},
)
```

## Builder Methods

### `Qualifire.slo_check()`

```python
qf.slo_check(
    column="updated_at",         # column for max_column strategy
    strategy="max_column",       # max_column | delta_log | metadata | custom_sql
    sql=None,                    # SQL for custom_sql strategy
    warning="PT4H",                # warning duration threshold
    error="PT8H",                  # error duration threshold
    notify={"error": ["slack"]}, # optional notification routing
)
```

### `Qualifire.threshold_check()`

```python
qf.threshold_check(
    aggregations={
        "avg_sales": "AVG(sales_amount)",
        "row_count": "COUNT(*)",
    },
    rules=[
        {
            "metric": "avg_sales",
            "thresholds": {
                "warning": {"min": 100, "max": 1000},
                "error": {"min": 50},
            },
        },
    ],
    notify={"error": ["email"]},
)
```

### `Qualifire.drift_check()`

```python
qf.drift_check(
    aggregations={"avg_sales": "AVG(sales_amount)"},
    rules=[{
        "metric": "avg_sales",
        "compare": {
            "past_values": 4,
            "step": "P7D",                      # required — partition cadence
            "missing_strategy": "ignore",       # ignore | substitute | error
            "on_missing_history": "warn",       # ignore | warn | error
        },
        "thresholds": {
            # Bare numbers (`20`) → symmetric absolute bound (back-compat).
            # Dict form `{min, max}` → asymmetric signed bounds.
            "warning": {
                "deviation_pct":      {"min": -25, "max": 50},
                "rate_of_change_pct": {"min": -30, "max": 30},
            },
            "error": {
                "deviation_pct":      {"min": -50, "max": 100},
            },
        },
    }],
)
```

Available signed measures: `deviation_pct`, `deviation_abs`,
`z_score`, `rate_of_change_pct`, `rate_of_change_abs`. Full
contract — including cold-start handling and the partition_ts /
step requirement — in
[`validators/drift.md`](validators/drift.md).

### `Qualifire.trend_check()`

```python
qf.trend_check(
    metric="avg_sales",
    aggregation="AVG(sales_amount)",   # SQL expression
    history_count=90,                   # number of historical points
    step="P1D",                         # required — partition cadence
    # Prophet hyperparameters (optional)
    changepoint_prior_scale=0.05,
    seasonality_prior_scale=10.0,
    seasonality_mode="additive",
    on_missing_history="ignore",        # ignore | warn | error
)
```

Forecast reads history via partition-anchored lookups, the same
contract as drift; see
[`validators/forecast.md`](validators/forecast.md) and
[`validators/partition_anchoring.md`](validators/partition_anchoring.md).

### `Qualifire.shape_check()`

Compares the multi-dimensional shape (distribution) of current data against historical periods. Discovers systemic issues — schema shifts, distribution changes, encoding errors — without predefined rules.

```python
qf.shape_check(
    n_records=10000,
    past_dates=3,
    step="P7D",
    slice_column="date",
    slice_value="{{ ds }}",
    # Model parameters (optional)
    n_estimators=100,
    contamination="auto",
    explain=True,
    drop_complex=False,
    alert_on_schema_change=False,
)
```

### `Qualifire.pattern_check()`

Trains a Random Forest two-sample classifier (past rows = label 0, current rows = label 1) and reports the cross-validated AUC. Complements `shape` by answering a batch-level question: "did this run come from a different distribution than past runs?". See [`validators/pattern.md`](validators/pattern.md) for background.

```python
qf.pattern_check(
    n_records=10000,
    past_dates=3,
    step="P7D",
    slice_column="sale_date",
    slice_value="{{ ds }}",
    # Optional explicit past-slice predicates. If provided, its
    # length defines the number of past slices.
    history_filters=None,
    thresholds={
        "warning": {"auc": 0.65},
        "error":   {"auc": 0.80},
    },
    # Model parameters (optional)
    n_estimators=200,
    max_depth=8,
    cv_folds=5,
    class_weight="balanced",
    explain=True,
    drop_complex=False,
    alert_on_schema_change=False,
    # REQUIRED for any dataset whose current/past slices are defined
    # by a date / partition / ID column — otherwise AUC ~= 1.0 every run.
    exclude_columns=["sale_date", "updated_at", "sale_id"],
    on_missing_history="ignore",
)
```

## Working with Results

```python
from qualifire import QualifireValidationError, Severity

try:
    result = qf.validate(table="...", validations=[...])

    # Inspect results
    print(result.overall_severity)   # Severity.PASS / WARNING / ERROR
    print(result.has_errors)         # bool
    print(result.has_warnings)       # bool
    print(result.run_id)             # UUID string

    for ds in result.datasets:
        print(f"{ds.dataset_name}: {ds.overall_severity.value}")
        for vr in ds.validation_results:
            print(f"  [{vr.severity.value}] {vr.validation_name}: {vr.message}")
            print(f"    actual={vr.actual_value}, expected={vr.expected_value}")
            print(f"    details={vr.details}")

except QualifireValidationError as e:
    # ERROR severity — pipeline should stop
    result = e.result
    for ds in result.datasets:
        for vr in ds.validation_results:
            if vr.severity == Severity.ERROR:
                print(f"FAILED: {vr.message}")
```

## Pandas Backend (Experimental)

> **Experimental.** Only the aggregation / metrics / threshold path is
> exercised end-to-end. The collectors and validators listed below as
> "not supported" call Spark-only APIs and will raise at runtime when
> paired with `PandasBackend`. Use Spark for production workloads.

```python
from qualifire.backends.pandas_backend import PandasBackend

backend = PandasBackend(tables={
    "my_table": my_pandas_df,
})

qf = Qualifire(
    backend=backend,
    system_table=":memory:",
    system_table_backend="sqlite",
    owner="local-dev",
    bu="test",
)
```

### Pandas support matrix

| Feature | Pandas | Notes |
|---|---|---|
| `aggregation` collector | ✅ | Uses `pandasql` under the hood — install `pandasql` first. |
| `metrics` collector | ✅ | Same `pandasql` path as aggregation. |
| `threshold` validator | ✅ | Pure Python — no backend calls. |
| `drift` validator (historical) | ⚠️ | Works only when its collector also works (aggregation / metrics). |
| `trend` validator (forecast) | ⚠️ | Same caveat — plus Prophet must be installed separately. |
| `custom_query` collector | ❌ | Calls `.collect()` on the result, which is Spark-only. |
| `profiling` collector | ❌ | Built on `pyspark.sql.functions`. |
| `recency` / `slo` collectors | ❌ | Use Spark window ops and delta-log metadata. |
| `sample` / `shape` / `pattern` validators | ❌ | Isolation-forest + Random-Forest pipelines rely on the Spark-backed sampler for production-sized workloads. |
| WAP (write-audit-publish) | ❌ | Requires Spark writer + Delta semantics. |

"⚠️" means the validator itself is backend-agnostic but depends on a
collector — use a supported collector or the validator raises when the
collector reaches for a Spark API.

## Dashboards

The full dashboard surface lives under `qualifire.reporting`. All
helpers are importable from the package root.

### Per-run static HTML

`generate_html_report(QualifireResult, output_path=...)` renders a
single run's results into a self-contained HTML page (no JS
dependency).

```python
from qualifire.reporting import generate_html_report

result = qf.validate(...)
generate_html_report(result, output_path="report.html")
```

### Aggregated health report

`HealthReporter(storage).generate(days=N)` aggregates validation rows
over a window into pass/warn/error counts, dataset breakdowns, and
trend buckets. `generate_health_html(report, output_path=...)`
renders the resulting `HealthReport` as static HTML.

```python
from qualifire.reporting import HealthReporter, generate_health_html

report = HealthReporter(storage).generate(days=30)
generate_health_html(report, output_path="health.html")
```

### Interactive dashboard (Plotly drill-down)

Reads the system table directly and emits a self-contained HTML page
with dataset / validation pickers, severity charts, and per-partition
metric history. CSS is scoped via `.qf-dashboard` so it doesn't leak
when displayed in a notebook iframe.

```python
qf.interactive_dashboard(
    output_path="dashboard.html",
    days=30,
    inline_plotly_js=False,    # True embeds plotly.min.js inline (offline-friendly)
)

# Or directly against any SystemTableStorage, without a Qualifire instance:
from qualifire.reporting import generate_interactive_html
generate_interactive_html(storage, output_path="dashboard.html", days=30)
```

### `qf.health_report()`

Same as `HealthReporter(storage).generate(...)` plus an optional
`output_path` for the static HTML.

```python
report = qf.health_report(days=14, output_path="health.html")
```

### Notebook charts (matplotlib + Plotly)

For ad-hoc analysis the package ships per-result charts plus
aggregate dashboard charts that consume a coerced DataFrame from
`load_health_dataframe`. Notebook examples in
[`tests/manual/dashboard_charts.ipynb`](../tests/manual/dashboard_charts.ipynb).

```python
from qualifire.reporting import (
    # Per-result helpers
    plot_metric_history,
    plot_validation_summary,
    plot_anomaly_shap,
    plot_forecast,
    # Aggregate dashboard helpers
    plot_executive_summary,                   # matplotlib KPI panel
    plot_executive_summary_interactive,       # Plotly donut + trend
    plot_dataset_day_heatmap,                 # matplotlib dataset × day
    plot_validation_history_interactive,      # Plotly per-validation history
    plot_severity_hierarchy,                  # Plotly sunburst + treemap
    plot_severity_by_type,                    # matplotlib stacked bar
    plot_metric_distribution_by_severity,     # Plotly violin
    SEVERITY_COLORS,
)
from qualifire.reporting import load_health_dataframe

df = load_health_dataframe(storage, days=30)
plot_executive_summary(df, output_dir="charts/")
```

#### Empty-input contract

Two return-shape conventions, picked so callers can render
unconditionally without an `if df.empty` guard at every site:

| Helper kind | Empty / no-data input | Why |
|---|---|---|
| **matplotlib** (`plot_executive_summary`, `plot_dataset_day_heatmap`, `plot_severity_by_type`) | Returns a `Figure` carrying a centered "no data" message | Notebook authors can `display(fig)` the same way they would on a real chart; PNG export still produces a valid file. |
| **Plotly** (`plot_executive_summary_interactive`, `plot_validation_history_interactive`, `plot_metric_distribution_by_severity`) | Returns `None` | Plotly's "empty-figure-with-text" pattern is awkward; an explicit `None` lets callers do `if fig: fig.show()` cleanly. |
| **`plot_severity_hierarchy`** | Returns `(None, None)` | Mirrors the Plotly pair on empty input. |

Per-result helpers (`plot_metric_history`, `plot_anomaly_shap`,
`plot_forecast`, `plot_validation_summary`) follow the matplotlib
"return a Figure" rule even when the input data is sparse.

### Storage helpers

Two helpers cover the "open a backend handle" / "rebuild a result
from history" patterns the dashboards lean on:

```python
from qualifire.reporting import make_storage, build_result_from_system_table

# Backend-agnostic factory — same options as Qualifire's
# system_table_backend kwarg.
storage = make_storage(
    backend="external_catalog",
    system_table="catalog.qualifire.history",
)

# Reconstruct the latest run as a QualifireResult (or pass run_id=...
# for a specific one).
qr = build_result_from_system_table(storage, days=30)
```

Reporting surface reference: `qualifire/reporting/__init__.py`.
