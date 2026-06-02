# Qualifire

A Python library for automated data quality monitoring with four validation strategies:

1. **SLO Checks** — Freshness / recency monitoring against duration thresholds
2. **Rule-Based Validation** — Static threshold checks on aggregated metrics
3. **Time-Series Forecasting** — Prophet-based dynamic thresholds with prediction bands
4. **Unsupervised Anomaly Detection** — Isolation Forest + SHAP explainability

Designed for **Oracle AIDP** (Spark 3.5.0 + Delta Lake 3.2.0) with support for config-driven (YAML) and programmatic usage.

## Why Qualifire

Modern data platforms ship in hours, not weeks. The failure
mode is silent corruption — a join misfires, an upstream
encoding changes, a holiday breaks an assumption — and the
downstream team hears about it from a customer.

The usual responses (hand-rolled `assert` statements, per-team
DQ conventions, ad-hoc Slack pings) work in isolation but
don't compose. Each pipeline ends up with its own history
store, its own severity ladder, its own alert format. Qualifire
is the in-platform layer that gives every pipeline on AIDP a
shared substrate for **identity, history, severity,
notification, and audit** — so the checks themselves stay
small and pipeline-local, but the surrounding machinery is
one library.

### Impact on users

| Persona | Today | With Qualifire |
|---|---|---|
| **Data engineer** | Hand-rolled asserts, custom retry logic, ad-hoc Slack pings | YAML or `Qualifire(...)` config; one library; history persisted out of the box |
| **Pipeline owner** | "Does the table refresh?" answered by tribal knowledge | SLO + drift on every dataset, persisted to the system table |
| **Operator on-call** | Surprise page at 3 a.m. with no context | WARNING / ERROR severity ladder, per-severity routing, alert dedup, top-feature attribution in the alert body |
| **Compliance / SecOps** | "Where do alerts go and what do they contain?" is unanswerable | Single config surface, redaction policy, system-table audit trail |
| **Platform team** | Each BU rolls its own DQ stack | One backend abstraction, one storage abstraction, one upgrade path |

The benefit isn't more checks — every team writes those
eventually. It's that the checks share identity, history,
severity, notification, and audit with everything else on
the platform.

For a side-by-side against Great Expectations / Deequ / Soda /
dbt tests / Pandera / SaaS DQ vendors, see
[`docs/comparison.md`](docs/comparison.md). For "which validator
do I want?", see [`docs/validators/README.md`](docs/validators/README.md).
For the under-the-hood mental model, see
[`docs/architecture.md`](docs/architecture.md).

## Key Features

- **Warning / Error severity tiers** with per-severity notification routing (including optional success notifications)
- **Write-Audit-Publish (WAP)** pattern as first-class feature (SQL + DataFrame paths)
- **System table** for historical state — enables historical comparison, forecasting, and trend analysis
- **`partition_ts`** as a first-class column: every persisted row carries the partition identity (e.g. `event_dt`, `'{{ ds }}'`, `CURRENT_TIMESTAMP`) so history-backed checks anchor at exactly `partition_ts − k·step` (ISO-8601 durations: `P7D`, `PT1H`, etc.)
- **Signed historical comparisons + rate-of-change** — drift / trend / pattern emit signed `deviation_pct`, `z_score`, and `rate_of_change_pct`; thresholds use explicit `{min, max}` signed bound dicts
- **Dataset / validation descriptions** — optional `description:` on every config; persisted on every system-table row and surfaced in the dashboards
- **Runtime skip-* flags** — `skip_recollection`, `skip_revalidation`, `skip_renotification` (all default-False) short-circuit each pipeline stage when persisted state already covers it. Faster retries; cheaper backfill replays; reproducible CI snapshots without re-paying compute. See `docs/CHANGELOG.md`.
- **Backfill + soft-delete** — `qualifire backfill --partition START..END` re-collects historical metrics; `--parallelism N` fans out across threads; `--soft-delete-prior` tombstones overwritten metrics so history reads stay clean. See `docs/backfill_and_soft_delete.md`.
- **Drift explainer with per-slice breakdown** — Pattern/IsolationForest emit `value_drift_explainer` showing which features shifted; opt-in `drift_breakdown_by_slice` adds per-past-slice deltas so "gradual vs sudden" triage doesn't require diving into raw data.
- **Column-name redaction (HIPAA/GDPR/PCI)** — list sensitive source columns via `Qualifire(redacted_columns=[...], allowlist_columns=[...])` (instance-level) or `DatasetConfig.redacted_columns` (dataset-level); the redacted name never enters `details_json`, so all egress paths inherit the protection.
- **Alert deduplication** and **cross-dataset notification grouping** to prevent alert fatigue
- **Isolation Forest + SHAP** for detecting unknown unknowns with explainability
- **Interactive HTML dashboard** — drill-down by dataset → validation → per-partition history; time-range picker; dark-mode adaptive (`qf.interactive_dashboard(...)`)
- **Parallel execution** — datasets and collect+validate pipelines run concurrently via thread pools
- **Dataset caching** — persist DataFrames with configurable Spark StorageLevel for faster multi-validation reads
- **Single-pass column profiling** — type-aware, composable analyzers with per-column stat control
- **AIDP Jinja parameter integration** — `job.*`, `task.*`, `hub.*`, `workspace.*`
- **Tiered install** — core, forecast, anomaly, or all
- **Architecture overview** — see [`docs/architecture.md`](docs/architecture.md) for end-to-end data flow + WAP lifecycle.

## Installation

Tiered. Pick the install that matches what you'll run:

| Install | Extra | Requirements file | What it adds | Why |
|---|---|---|---|---|
| AIDP Workbench | (none) | `requirements.txt` | jinja2, prophet, shap, plotly, matplotlib | The minimum delta on top of AIDP's pre-provisioned compute. Intentionally tiny so installing does **not** overwrite AIDP-shipped pandas / numpy / pyarrow / pyspark / scikit-learn / pydantic |
| Local development (default) | `[all]` | `requirements-local.txt` | pandas + forecast + anomaly + viz | Every *validator type* + dashboards, but **not** PySpark — full local dev set for non-AIDP environments |
| Core only | (none) | `requirements-core.txt` | pydantic, jinja2, pyyaml, requests | SLO, threshold, historical/drift, custom-query against a Spark backend (Spark provided externally); threshold/historical on `PandasBackend` need `[pandas]` |
| Spark | `[spark]` | `requirements-spark.txt` | + PySpark 3.5+ | Local dev / non-AIDP environments. **Skip on AIDP Workbench** — Spark is pre-provisioned there and pip-installing pyspark overwrites it |
| Pandas SQL | `[pandas]` | (none, use `pip install qualifire[pandas]`) | + pandasql | `PandasBackend` SQL path |
| Forecast | `[forecast]` | `requirements-forecast.txt` | + Prophet | `trend` validator |
| Anomaly | `[anomaly]` | `requirements-anomaly.txt` | + scikit-learn, shap | `shape` (Isolation Forest), `pattern` (Random Forest) — both with SHAP explanations |
| Visualisation | `[viz]` | `requirements-viz.txt` | + plotly, matplotlib | `generate_interactive_html(inline_plotly_js=True)` (the dashboard's inline-bundle path that survives VS Code's notebook sandbox) + `tests/manual/dashboard_charts.ipynb`. Optional — the CDN form of the interactive dashboard works without these |
| Dev | `[dev]` | `requirements-dev.txt` | + pytest, pytest-mock, pytest-cov | Test runner + linting deps |

> **Why `[all]` excludes PySpark:** AIDP Workbench provisions Spark
> out-of-band. Co-installing `pyspark` via `pip install qualifire[all]`
> would overwrite the pre-provisioned runtime. Local devs who want a
> Spark-capable environment combine the two: `pip install
> 'qualifire[all,spark]'`.

```bash
# AIDP Workbench: install only the five packages AIDP doesn't pre-provision
pip install -r requirements.txt
pip install -e .

# Local dev: every validator type + dashboards (no PySpark)
pip install -r requirements-local.txt
pip install -e .
# or
pip install 'qualifire[all]'

# Lean install — SLO + threshold + historical/drift on a Spark backend
# that's already provisioned (Spark provided externally)
pip install -r requirements-core.txt
pip install -e .

# Add PySpark for local dev (skip on AIDP — Spark is pre-provisioned there)
pip install 'qualifire[spark]'

# Pick-and-choose extras
pip install 'qualifire[forecast]'   # Prophet for trend validators
pip install 'qualifire[anomaly]'    # Isolation Forest + SHAP for shape/pattern

# Local dev: all validators + PySpark
pip install 'qualifire[all,spark]'

# Development (test runner + linting deps)
pip install -r requirements-dev.txt
```

### Spark driver-only deployment (zip / wheel on AIDP)

Qualifire is **driver-only** on Spark — there are no Python UDFs,
`mapPartitions`, or RDD-level closures. All Spark interactions are
SQL/Catalyst expressions (`pyspark.sql.functions` builders that
compile to native Catalyst), and validators that need pandas/sklearn
operate on `df.toPandas()` results in driver memory. The wheel or
zip only needs to be installed on the Spark **driver**; executors
do not need the package.

`make install-all` (`Makefile`) runs the full extras + dev install in one
step (without PySpark — see above). `make test` enforces it via a
precheck that errors with a clear message on missing deps.

## Quick Start

### Minimal example (no Spark required)

The shortest path to a green run, using the pandas backend
+ in-memory SQLite system table. Works in any Python REPL
after `pip install -e .`:

```python
import pandas as pd
from qualifire.api import Qualifire
from qualifire.backends.pandas_backend import PandasBackend

backend = PandasBackend()
backend.register_table("orders", pd.DataFrame({
    "order_id": range(150),
    "amount":   [100.0] * 150,
}))

qf = Qualifire(
    backend=backend, system_table=":memory:",
    system_table_backend="sqlite",
    owner="data-eng", bu="finance",
)

result = qf.validate(
    table="orders",
    validations=[
        qf.threshold_check(
            aggregations={"row_count": "COUNT(*)"},
            rules=[{"metric": "row_count",
                    "thresholds": {"error": {"min": 100}}}],
        ),
    ],
)
print("overall_severity:", result.overall_severity.value)
# overall_severity: PASS
```

### Programmatic API

```python
from qualifire import Qualifire
from qualifire.backends.spark_backend import SparkBackend

qf = Qualifire(
    backend=SparkBackend(spark),
    system_table="catalog.schema.qf_history",
    system_table_backend="external_catalog",
    owner="data-engineering",
    bu="finance",
)

# SLO check — data freshness
result = qf.validate(
    table="catalog.schema.sales",
    validations=[
        qf.slo_check(column="updated_at", warning="PT4H", error="PT8H"),
    ],
)
# result.overall_severity => Severity.PASS / WARNING / ERROR

# Threshold check
result = qf.validate(
    table="catalog.schema.sales",
    filter_expr="date = '{{ ds }}'",
    validations=[
        qf.threshold_check(
            aggregations={"avg_sales": "AVG(sales_amount)", "row_count": "COUNT(*)"},
            rules=[
                {"metric": "avg_sales", "thresholds": {"warning": {"min": 100}, "error": {"min": 50}}},
                {"metric": "row_count", "thresholds": {"error": {"min": 1000}}},
            ],
        ),
    ],
    context={"ds": "2024-06-15"},
)

# Historical comparison
# - `partition_ts` makes the history read partition-anchored (anchor − k·step),
#   so the lookback returns exactly the prior 3 weekly partitions.
# - Threshold form: {min: -X, max: Y} signed bound dicts. Useful when a
#   50% drop should fire but a 50% spike is fine, or vice versa.
#   `rate_of_change_pct` compares against the immediate-prior partition.
result = qf.validate(
    table="catalog.schema.sales",
    # Column-ref partition_ts requires dimensions=[col] so the
    # collector's SELECT can GROUP BY the column. Drift then runs
    # one rule per (event_dt, metric) cell.
    partition_ts="event_dt",
    dimensions=["event_dt"],
    validations=[
        qf.drift_check(
            description="Daily revenue drift vs. prior 3 weeks",
            aggregations={"avg_sales": "AVG(sales_amount)"},
            rules=[{
                "metric": "avg_sales",
                "compare": {"past_values": 3, "step": "P7D", "missing_strategy": "ignore"},
                "thresholds": {
                    "warning": {"deviation_pct": 20,
                                "rate_of_change_pct": {"min": -10, "max": 25}},
                    "error":   {"deviation_pct": 50},
                },
            }],
        ),
    ],
)

# Prophet forecast
result = qf.validate(
    table="catalog.schema.sales",
    validations=[
        qf.trend_check(
            metric="avg_sales",
            aggregation="AVG(sales_amount)",
            history_count=90,
            step="P1D",
        ),
    ],
)

# Isolation Forest anomaly detection
result = qf.validate(
    table="catalog.schema.sales",
    validations=[
        qf.shape_check(
            n_records=10000,
            past_dates=3,
            step="P7D",
            slice_column="date",
            slice_value="{{ ds }}",
        ),
    ],
)
```

### Inspecting Validation Results

Every `qf.validate()` call returns a `QualifireResult` (or raises `QualifireValidationError` on ERROR, which carries the result on `.result`).

```python
from qualifire import QualifireValidationError, Severity

try:
    result = qf.validate(
        table="catalog.schema.sales",
        validations=[
            qf.slo_check(column="updated_at", warning="PT4H", error="PT8H"),
            qf.threshold_check(
                aggregations={"row_count": "COUNT(*)", "null_pct": "SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*)"},
                rules=[
                    {"metric": "row_count", "thresholds": {"error": {"min": 100}}},
                    {"metric": "null_pct", "thresholds": {"warning": {"max": 5.0}}},
                ],
            ),
        ],
    )
except QualifireValidationError as e:
    result = e.result  # full result is attached to the exception

# Top-level severity
print(result.overall_severity)     # Severity.PASS / WARNING / ERROR
print(result.has_errors)           # bool
print(result.has_warnings)         # bool
print(result.run_id)               # UUID string

# Per-dataset results
for ds in result.datasets:
    print(f"\n{ds.dataset_name}: {ds.overall_severity.value}")

    for vr in ds.validation_results:
        print(f"  [{vr.severity.value}] {vr.validation_name}")
        print(f"    message: {vr.message}")
        print(f"    actual:  {vr.actual_value}")
        print(f"    expected: {vr.expected_value}")
        print(f"    details: {vr.details}")

# Filter by severity
errors = [vr for ds in result.datasets for vr in ds.validation_results if vr.severity == Severity.ERROR]
warnings = [vr for ds in result.datasets for vr in ds.validation_results if vr.severity == Severity.WARNING]

# Notification outcomes
for nr in result.notifications:
    print(f"  {nr.channel}: {nr.status} ({nr.message})")
```

### Notifications (Programmatic)

Register notification channels on the `Qualifire` instance, then reference them by name in the `notify` param. Notifications sent through the engine are **persisted to the system table** alongside validation results.

```python
from qualifire import Qualifire
from qualifire.backends.spark_backend import SparkBackend
from qualifire.notification.email_notifier import EmailNotifier
from qualifire.notification.slack_notifier import SlackNotifier

qf = Qualifire(
    backend=SparkBackend(spark),
    system_table="catalog.schema.qf_history",
    system_table_backend="external_catalog",
    owner="data-engineering",
    bu="finance",
)

# Step 1: Configure notification channels
qf.register_notifier("email", EmailNotifier(
    smtp_host="smtp.company.com",
    smtp_port=587,
    smtp_user="qualifire@company.com",
    smtp_password="secret",
    recipients=["oncall@company.com", "team-lead@company.com"],
))
qf.register_notifier("slack", SlackNotifier(
    webhook_url="https://hooks.slack.com/services/T.../B.../xxx",
))

# Step 2: Use channel names in the notify param on any validation
result = qf.validate(
    table="catalog.schema.sales",
    validations=[
        qf.threshold_check(
            aggregations={"row_count": "COUNT(*)", "null_pct": "..."},
            rules=[
                {"metric": "row_count", "thresholds": {"warning": {"min": 1000}, "error": {"min": 100}}},
            ],
            notify={"warning": ["email"], "error": ["email", "slack"]},
        ),
    ],
)
# On WARNING: email sent
# On ERROR:   email + slack sent, QualifireValidationError raised
# All notifications persisted to system table
```

Channels can also be passed at init time:

```python
qf = Qualifire(
    backend=SparkBackend(spark),
    owner="data-eng",
    bu="finance",
    notifiers={
        "email": EmailNotifier(smtp_host="smtp.company.com", recipients=["team@co.com"]),
        "slack": SlackNotifier(webhook_url="https://hooks.slack.com/..."),
    },
)
```

When using `run_config()`, channels merge in **two** layers
(later wins):

1. YAML `notifications:` block — declarative defaults.
2. Programmatic notifiers — passed via
   `Qualifire(notifiers={...})`, `Qualifire.from_config(notifiers={...})`,
   or `qf.register_notifier(name, instance)`. Always win over
   a YAML block of the same name.

There is no per-call `run_config(notifiers=...)` kwarg today —
runtime overrides happen at instance construction. See
[`docs/programmatic_api.md`](docs/programmatic_api.md) for the
authoritative contract.

For sending notifications **outside** the engine (e.g., custom logic after inspecting results), call `.send()` directly. These manual sends are **not** persisted to the system table:

```python
from qualifire.core.models import Severity

for ds in result.datasets:
    if ds.overall_severity >= Severity.WARNING:
        slack = SlackNotifier(webhook_url="https://hooks.slack.com/services/...")
        nr = slack.send(ds, ds.overall_severity, "data-eng", "finance")
        print(f"Slack: {nr.status}")

        email = EmailNotifier(
            smtp_host="smtp.company.com",
            smtp_port=587,
            smtp_user="qualifire@company.com",
            smtp_password="secret",
            recipients=["oncall@company.com"],
        )
        nr = email.send(ds, ds.overall_severity, "data-eng", "finance")
        print(f"Email: {nr.status}")
```

### Write-Audit-Publish (WAP) Pattern

```python
# DataFrame-driven WAP
result = qf.write_audit_publish(
    df=my_dataframe,
    target_table="catalog.schema.sales_prod",
    write_options={"mode": "overwrite", "partitionBy": ["date"]},
    validations=[
        qf.threshold_check(
            aggregations={"row_count": "COUNT(*)"},
            rules=[{"metric": "row_count", "thresholds": {"error": {"min": 100}}}],
        ),
    ],
)
# On ERROR: staging table dropped, QualifireValidationError raised
# On PASS/WARNING: data published to target table

# SQL-driven WAP
result = qf.write_audit_publish(
    target_table="catalog.schema.sales_prod",
    write_sql="SELECT * FROM raw.sales WHERE date = '{{ ds }}'",
    validations=[...],
    context={"ds": "2024-06-15"},
)
```

### Config-Driven (YAML)

```yaml
# config.yaml
owner: "data-engineering-team"
bu: "finance"
system_table: "catalog.schema.qualifire_history"
system_table_backend: "external_catalog"
dataset_parallelism: 2                              # run 2 datasets concurrently

notifications:
  email:
    type: email
    smtp_host: "smtp.company.com"
    smtp_port: 587
    recipients: ["team@company.com"]
  slack:
    type: slack
    webhook_url: "https://hooks.slack.com/services/..."

datasets:
  - name: "sales_daily"
    table: "catalog.schema.sales"
    filter: "date = '{{ ds }}'"
    cache: true                             # cache for faster multi-validation reads
    validation_parallelism: 2                 # run SLO + threshold concurrently
    validations:
      - type: "slo"
        recency:
          strategy: "max_column"
          column: "updated_at"
        thresholds:
          warning: "PT4H"
          error: "PT8H"
        notify:
          on_success: ["email"]                   # optional: notify on success
          warning: ["email"]
          error: ["email", "slack"]

      - type: "threshold"
        collection:
          type: "aggregation"
          expressions:
            avg_sales: "AVG(sales_amount)"
            row_count: "COUNT(*)"
        rules:
          - metric: "avg_sales"
            thresholds:
              warning: { min: 100 }
              error: { min: 50 }
        notify:
          error: ["email", "slack"]
```

```python
# Recommended: construct directly from YAML — owner/bu/system_table/jdbc/etc.
# all flow from the config; only the runtime backend stays in Python.
qf = Qualifire.from_config("config.yaml", backend=SparkBackend(spark))
result = qf.run_config("config.yaml", context={"ds": "2024-06-15"})
```

`Qualifire.from_config(path, *, backend, **overrides)` is the
recommended factory for YAML-driven setups. The runtime
`backend` is the only required Python kwarg; everything else
(owner, bu, system_table, system_table_backend, jdbc) flows
from the YAML. Pass any of those names as `**overrides` to win
over the YAML value, or `notifiers={'name': instance}` to
inject a runtime-instance notifier that wins over YAML's
`notifications:` block on every call.

### CLI

```bash
# Validate a config file
qualifire validate-config --config config.yaml

# Run validations
qualifire run --config config.yaml -C ds=2024-06-15

# Set log level
qualifire run --config config.yaml --log-level DEBUG
```

## Architecture

```
qualifire/
├── qualifire/
│   ├── __init__.py              # Public API exports
│   ├── api.py                   # Qualifire class (programmatic entry point)
│   ├── cli.py                   # CLI entry point
│   ├── core/
│   │   ├── config.py            # Pydantic models for YAML/JSON config
│   │   ├── models.py            # Result dataclasses (Severity, ValidationResult, etc.)
│   │   ├── engine.py            # Orchestrator: collect → validate → notify → persist
│   │   ├── context.py           # Jinja2 environment + AIDP parameter bridge
│   │   ├── duration.py          # ISO 8601 duration parsing ("PT4H", "P2D" → timedelta)
│   │   └── exceptions.py        # Exception hierarchy
│   ├── backends/
│   │   ├── base.py              # Backend Protocol
│   │   ├── spark_backend.py     # PySpark implementation
│   │   └── pandas_backend.py    # Pandas implementation (experimental; see docs/programmatic_api.md)
│   ├── collection/
│   │   ├── recency.py           # Freshness: MAX(col), delta_log, metadata, custom SQL
│   │   ├── profiler.py          # Column profiling collector (delegates to profiling engine)
│   │   ├── profiling/           # Single-pass, type-aware profiling engine
│   │   │   ├── analyzers.py     # StatAnalyzer ABC + Numeric/Text/Boolean/Timestamp
│   │   │   └── engine.py        # ProfileEngine: one df.agg() for all stats
│   │   ├── aggregation.py       # User-defined SQL aggregations
│   │   ├── metrics.py           # Named KPI metrics
│   │   ├── sampler.py           # Random N records (for Isolation Forest)
│   │   └── custom_query.py      # Arbitrary SQL
│   ├── validation/
│   │   ├── slo.py               # SLO freshness check
│   │   ├── threshold.py         # Static bounds (min, max, eq, neq, gt, lt, gte, lte)
│   │   ├── historical.py        # Compare against past N values
│   │   ├── forecast.py          # Prophet time-series prediction bands
│   │   └── isolation_forest.py  # Isolation Forest + SHAP
│   ├── notification/
│   │   ├── base.py              # Notifier ABC + plugin registry + deduplication
│   │   ├── email_notifier.py    # SMTP email
│   │   ├── slack_notifier.py    # Slack webhook
│   │   └── webhook_notifier.py  # Generic HTTP webhook
│   ├── wap/
│   │   └── pattern.py           # Write-Audit-Publish orchestration
│   ├── storage/
│   │   ├── base.py              # SystemTableStorage Protocol
│   │   ├── factory.py           # open_storage(...) — single source of truth
│   │   ├── external_catalog.py  # AIDP external catalog (default)
│   │   ├── delta_storage.py     # Delta table (with compact())
│   │   ├── sqlite_storage.py    # SQLite (dev/test)
│   │   └── jdbc_storage.py      # JDBC (Oracle, PostgreSQL, MySQL, ...)
│   └── reporting/
│       ├── html_report.py       # Static + interactive HTML dashboards
│       ├── plots.py             # Matplotlib + Plotly charts (per-result + aggregate)
│       ├── health.py            # HealthReporter / HealthReport
│       ├── system_table.py      # load_health_dataframe, build_result_from_system_table
│       └── storage_factory.py   # make_storage(...) (notebook entrypoint)
├── tests/                       # 850+ pytest tests (real PySpark + mocked Spark)
├── docs/                        # Documentation
├── pyproject.toml
├── Makefile
└── requirements*.txt
```

## Validation Types

| YAML type | Builder | What It Does | When to Use |
|---|---|---|---|
| `slo` | `slo_check()` | Checks data freshness against duration thresholds | Pipeline SLA monitoring |
| `threshold` | `threshold_check()` | Compares metrics against static bounds | Known constraints (min row count, max null rate) |
| `drift` | `drift_check()` | Compares against past N values (deviation %, z-score) | Detecting drift from historical norms |
| `trend` | `trend_check()` | Dynamic thresholds that adapt to trends and seasonality | Seasonal patterns, growth trends |
| `shape` | `shape_check()` | Compares data shape/distribution across periods (per-row Isolation Forest) | Discovering unknown unknowns, systemic data issues |
| `pattern` | `pattern_check()` | Random Forest two-sample classifier over current vs past rows (batch-level AUC) | Detecting multivariate distribution shifts between runs |

## System Table Backends

| Backend | Use Case | Notes |
|---|---|---|
| `external_catalog` | AIDP production (default) | Insert-only, 3-part naming |
| `delta` | Delta Lake environments | Includes `compact()` method |
| `sqlite` | Local development / testing | In-memory or file-based |
| `jdbc` | External databases (Oracle, PostgreSQL, MySQL, ...) | **Requires SparkBackend** (uses Spark JDBC reader/writer) plus a `jdbc:` connection block. Pandas-only deployments cannot use this backend. See [docs/configuration.md](docs/configuration.md#jdbc-system-table). |

## Jinja Context Variables

Built-in variables always available in filter expressions and custom SQL:

```
{{ today }}              # 2024-06-15
{{ yesterday }}          # 2024-06-14
{{ now }}                # 2024-06-15T10:30:00
{{ run_id }}             # UUID for this run
{{ ds }}                 # alias for today
{{ ds_nodash }}          # 20240615
{{ today | date_add(-7) }}          # 7 days ago
{{ today | date_format('%Y%m%d') }} # formatted date
```

AIDP parameters (when running on AIDP):
```
{{ job.id }}, {{ job.name }}, {{ job.run_id }}
{{ task.name }}, {{ task.run_id }}
{{ hub.id }}, {{ hub.region }}
{{ workspace.id }}, {{ workspace.url }}
```

## Error Handling

- **PASS**: All validations within thresholds. Returns `QualifireResult`.
- **WARNING**: Some validations exceed warning thresholds. Returns `QualifireResult`, sends notifications.
- **ERROR**: Validations exceed error thresholds. Raises `QualifireValidationError` (after notifications sent). In WAP, staging table is dropped (rollback).

```python
from qualifire import QualifireValidationError

try:
    result = qf.validate(table="...", validations=[...])
except QualifireValidationError as e:
    print(e.result.overall_severity)
    for ds in e.result.datasets:
        for vr in ds.validation_results:
            print(f"[{vr.severity.value}] {vr.message}")
```

## Testing

```bash
make test          # Run all tests
make test-cov      # Run with coverage report
```

## Best Practices

A handful of patterns surface in the notebooks and customer
deployments. Naming them here so operators can pick them up
without reading 40 notebook cells.

### Use the runtime skip-* flags for retries / replays

The three runtime flags compose freely; default-False means
"behave as today." Opt in for retries / backfill replays / CI
rehearsals where the persisted state is authoritative:

- `--skip-recollection` — skip the collection step when an
  active row exists at `(table, metric, partition_ts, dim)`.
  Filter scope is NOT in the natural key (documented trade-
  off; see `docs/CHANGELOG.md`).
- `--skip-revalidation` — skip the validator when an active
  validation row exists at the partition. Replays the
  persisted verdict (`details.from_cache=True`).
- `--skip-renotification` — skip notification dispatch for
  any key whose prior run already paged at the same severity.

```bash
# 90-day backfill replay without 90 days of pages:
qualifire backfill --config qf.yml \
  --partition 2026-04-01..2026-06-30 \
  --skip-recollection --skip-renotification
```

### Warm up history-backed validators before relying on alerts

Drift / forecast / pattern / anomaly need history to fire
correctly. Before the first real-data run, prime the system
table with N partitions of clean data so the validator's
`past_values=N`, `step="P7D"` lookback resolves. The
[interactive dashboard](docs/programmatic_api.md) helps spot
cold-start signals.

### Keep `partition_ts` consistent across runs

`partition_ts` IS the natural key for history reads. If one
run uses `'{{ ds }}'` (literal) and another uses
`event_dt` (column ref), the system table accumulates rows
under different partition stamps and history reads silently
miss. Pick one shape per dataset and stick with it.

### Configure column redaction at the highest scope you need

For HIPAA / GDPR / PCI compliance, set
`redacted_columns` once at the `Qualifire(...)` instance
level — applies to every dataset; no per-dataset audit. Use
dataset-level `redacted_columns` only when the policy varies
by table.

### Run the dashboard cadence at your data's cadence

`qf.interactive_dashboard(days=30)` re-renders the static HTML
every time. Schedule it after your data refresh — daily data
→ daily dashboard. The renderer reads the system table and
emits HTML; cost scales with the number of rows in the
window, not the dataset size.

### Use WAP for partial-day backfills

When a backfill writes partial data to the published table,
downstream consumers see a flickery state. WAP avoids that:
write to staging, validate, publish atomically. Cf.
[architecture.md WAP lifecycle](docs/architecture.md#2-wap-lifecycle).

## License

Apache License 2.0
