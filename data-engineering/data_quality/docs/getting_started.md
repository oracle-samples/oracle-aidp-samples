# Getting Started

## Prerequisites

- Python 3.11+
- PySpark 3.5.0 (pre-installed on AIDP Workbench)
- Delta Lake 3.2.0 (pre-installed on AIDP Workbench)

## Installation

### On AIDP Workbench

PySpark and Delta Lake are already available. Install Qualifire core
(plus any validator extras you need). **Do not install `[spark]` on
AIDP** — it would co-install pyspark and overwrite the
pre-provisioned runtime; `[all]` intentionally excludes pyspark for
the same reason.

For credentials (SMTP, JDBC, Slack/webhook tokens), pass AIDP's
`aidputils.secrets` resolver into the constructor and reference
secrets in YAML as `secret://name/key`:

```python
import aidputils
from qualifire import Qualifire

qf = Qualifire(
    backend=...,
    secret_resolver=aidputils.secrets,
    system_table="catalog.schema.qf_history",
    owner="data-team", bu="finance",
)
```

`aidputils` is auto-imported into AIDP notebook globals but **cannot**
be imported from custom Python modules — Qualifire is one such
module, so the resolver must be passed in. The resolver Protocol
is duck-typed (`get(name, key=None) -> Any`); any object with a
matching `.get(...)` works for non-AIDP backends. See
[`docs/jinja_rendering.md#secrets`](jinja_rendering.md#secrets)
for the full reference grammar and allowlist.

```bash
# AIDP Workbench — install the minimum delta on top of the
# pre-provisioned compute. Five packages: jinja2, prophet, shap,
# plotly, matplotlib. AIDP already ships pandas / numpy / pyarrow /
# pyspark / pydantic / scikit-learn — `requirements.txt` deliberately
# does NOT list those because re-installing them overwrites the
# platform-tested versions.
pip install -r requirements.txt
pip install -e .

# Local development — full set of optional validator extras.
pip install -r requirements-local.txt
pip install -e .
# Equivalent: pip install 'qualifire[all]'

# Lean install (SLO + threshold + drift on a pre-provisioned Spark)
pip install -r requirements-core.txt
pip install -e .
```

### Local development

Local devs install pyspark separately so the test suite can run:

```bash
pip install -r requirements-local.txt
pip install -r requirements-dev.txt
pip install 'pyspark>=3.5'     # OR `pip install qualifire[spark]`
pip install -e .
```

Or with the Makefile target:

```bash
make install-all
pip install 'pyspark>=3.5'
make test            # `test-precheck` errors out if anything is missing
```

### Composable requirements files

The repo ships several additive requirements files so deployment
targets can pick what they need:

- `requirements.txt` — **AIDP-only minimum**: jinja2, prophet, shap,
  plotly, matplotlib. The five packages AIDP compute does NOT
  pre-provision (or pre-provisions at versions older than what
  qualifire tests against). Intentionally tiny so installing it on
  AIDP does not overwrite pandas / pyspark / scikit-learn /
  pydantic / numpy / pyarrow.
- `requirements-local.txt` — every optional validator extra
  (forecast / anomaly / pandas / viz) but **not** pyspark. The
  default install for non-AIDP environments.
- `requirements-core.txt` — minimum runtime: pydantic, jinja2,
  pyyaml, requests. Reach for this when you only need SLO /
  threshold / drift / custom-query against an externally-provisioned
  Spark backend.
- `requirements-spark.txt` — adds `pyspark>=3.5` plus the matching
  Delta dependency. Use locally for tests, or in container builds
  that need to ship Spark.
- `requirements-viz.txt` — adds matplotlib + plotly + IPython for
  the dashboards under `qualifire.reporting`. Required for the
  notebooks under `tests/manual/` and for
  `generate_interactive_html` / `plot_*` helpers.

Combine them as needed:

```bash
# Full local dev
pip install -r requirements-local.txt -r requirements-spark.txt -r requirements-viz.txt -r requirements-dev.txt
```

### With individual optional extras

```bash
# Prophet for time-series forecasting (trend validator)
pip install 'qualifire[forecast]'

# Isolation Forest + SHAP for anomaly detection (shape/pattern)
pip install 'qualifire[anomaly]'

# Pandas SQL path (threshold/historical on PandasBackend)
pip install 'qualifire[pandas]'

# All optional validators (no pyspark — AIDP-safe)
pip install 'qualifire[all]'

# Local dev convenience: all validators + pyspark
pip install 'qualifire[all,spark]'
```

### From Wheel

The wheel itself only carries the core dependencies. Additional
extras come from `pip install 'qualifire[all]'` after the wheel
install — keeping the wheel install AIDP-safe.

```bash
make wheel
pip install dist/qualifire-0.1.0-py3-none-any.whl
```

#### Spark driver-only deployment

When bundling the wheel or a zip for AIDP, only the Spark **driver**
needs the package. There are no Python UDFs (`udf`, `pandas_udf`)
and no `mapPartitions` / `foreachPartition` / RDD closures in the
codebase — all Spark interaction uses `pyspark.sql.functions`
Catalyst builders that run natively in the JVM. Distribution to
executors via `--py-files` / `sc.addPyFile` is not required.

## Your First Validation

### 1. Programmatic — SLO Check

```python
from pyspark.sql import SparkSession
from qualifire import Qualifire
from qualifire.backends.spark_backend import SparkBackend

spark = SparkSession.builder.getOrCreate()

qf = Qualifire(
    backend=SparkBackend(spark),
    owner="my-team",
    bu="analytics",
)

# Check data freshness
result = qf.validate(
    table="catalog.schema.events",
    validations=[
        qf.slo_check(column="event_timestamp", warning="PT4H", error="PT8H"),
    ],
)

print(result.overall_severity)  # Severity.PASS, WARNING, or ERROR
for ds in result.datasets:
    for vr in ds.validation_results:
        print(f"  [{vr.severity.value}] {vr.message}")
```

### 2. Programmatic — Threshold Check

```python
result = qf.validate(
    table="catalog.schema.orders",
    filter_expr="order_date = '{{ ds }}'",
    validations=[
        qf.threshold_check(
            aggregations={
                "row_count": "COUNT(*)",
                "avg_amount": "AVG(order_amount)",
                "null_rate": "SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)",
            },
            rules=[
                {"metric": "row_count", "thresholds": {"error": {"min": 1000}}},
                {"metric": "avg_amount", "thresholds": {"warning": {"min": 50, "max": 500}, "error": {"min": 10}}},
                {"metric": "null_rate", "thresholds": {"warning": {"max": 5.0}, "error": {"max": 10.0}}},
            ],
        ),
    ],
    context={"ds": "2024-06-15"},
)
```

### 3. Config-Driven (YAML)

Create `my_config.yaml`:

```yaml
owner: "my-team"
bu: "analytics"
system_table: "catalog.schema.qualifire_history"

datasets:
  - name: "orders_check"
    table: "catalog.schema.orders"
    filter: "order_date = '{{ ds }}'"
    validations:
      - type: "slo"
        recency:
          strategy: "max_column"
          column: "updated_at"
        thresholds:
          warning: "PT4H"
          error: "PT8H"

      - type: "threshold"
        collection:
          type: "aggregation"
          expressions:
            row_count: "COUNT(*)"
        rules:
          - metric: "row_count"
            thresholds:
              error: { min: 1000 }
```

Run it:

```python
# Recommended for YAML-driven setups: construct directly from the
# config so owner / bu / system_table / system_table_backend / jdbc
# all flow from the YAML — only the runtime backend stays in Python.
qf = Qualifire.from_config("my_config.yaml", backend=SparkBackend(spark))
result = qf.run_config("my_config.yaml", context={"ds": "2024-06-15"})
```

Or via CLI:

```bash
qualifire run --config my_config.yaml -C ds=2024-06-15
```

### 4. With System Table (Historical State)

Adding a system table enables historical comparison, forecasting, and alert deduplication:

```python
qf = Qualifire(
    backend=SparkBackend(spark),
    system_table="catalog.schema.qualifire_history",
    system_table_backend="external_catalog",  # or "delta", "sqlite"
    owner="my-team",
    bu="analytics",
)
```

Every run is persisted. Future runs can compare against past values.

## Next Steps

- [Configuration Reference](configuration.md) — Full YAML config schema (parallelism, caching, notifications)
- [Column Profiling](profiling.md) — Single-pass profiling engine, stat control, custom analyzers
- [Validators](validators/README.md) — Index of validation types; per-validator pages live in `docs/validators/`
- [Partition anchoring](validators/partition_anchoring.md) — `partition_ts` / `step` contract for drift / forecast
- [WAP Pattern](wap_pattern.md) — Write-Audit-Publish for pipeline safety (with caching)
- [Notifications](notifications.md) — Channels, success notifications, cross-dataset grouping
- [Programmatic API](programmatic_api.md) — Builder methods, dashboards, and advanced usage
