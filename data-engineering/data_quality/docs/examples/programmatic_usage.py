"""Example: Programmatic usage of Qualifire.

Demonstrates all four validation types + WAP pattern using the Python API.
"""

from pyspark.sql import SparkSession

from qualifire import Qualifire, QualifireValidationError, Severity
from qualifire.backends.spark_backend import SparkBackend

# --- Setup ---

spark = SparkSession.builder.getOrCreate()

qf = Qualifire(
    backend=SparkBackend(spark),
    system_table="catalog.schema.qualifire_history",
    system_table_backend="external_catalog",
    owner="data-engineering",
    bu="finance",
)


# --- 1. SLO Check ---

result = qf.validate(
    table="catalog.schema.sales",
    validations=[
        qf.slo_check(column="updated_at", warning="PT4H", error="PT8H"),
    ],
)
print(f"SLO: {result.overall_severity.value}")


# --- 2. Threshold Check ---

result = qf.validate(
    table="catalog.schema.sales",
    filter_expr="sale_date = '{{ ds }}'",
    validations=[
        qf.threshold_check(
            aggregations={
                "row_count": "COUNT(*)",
                "avg_amount": "AVG(sale_amount)",
                "null_pct": "SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)",
            },
            rules=[
                {"metric": "row_count", "thresholds": {"error": {"min": 1000}}},
                {"metric": "avg_amount", "thresholds": {"warning": {"min": 50, "max": 500}}},
                {"metric": "null_pct", "thresholds": {"error": {"max": 5.0}}},
            ],
        ),
    ],
    context={"ds": "2024-06-15"},
)
print(f"Threshold: {result.overall_severity.value}")


# --- 3. Historical Comparison ---

result = qf.validate(
    table="catalog.schema.sales",
    filter_expr="sale_date = '{{ ds }}'",
    validations=[
        qf.drift_check(
            aggregations={"avg_amount": "AVG(sale_amount)"},
            rules=[
                {
                    "metric": "avg_amount",
                    "compare": {"past_values": 4, "step": "P7D"},
                    "thresholds": {
                        "warning": {"deviation_pct": 25},
                        "error": {"deviation_pct": 50},
                    },
                }
            ],
        ),
    ],
    context={"ds": "2024-06-15"},
)
print(f"Historical: {result.overall_severity.value}")


# --- 4. Forecast Check ---

result = qf.validate(
    table="catalog.schema.sales",
    filter_expr="sale_date = '{{ ds }}'",
    validations=[
        qf.trend_check(
            metric="daily_revenue",
            aggregation="SUM(sale_amount)",
            history_count=90,
            step="P1D",
        ),
    ],
    context={"ds": "2024-06-15"},
)
print(f"Forecast: {result.overall_severity.value}")


# --- 5. Anomaly Detection ---

result = qf.validate(
    table="catalog.schema.sales",
    validations=[
        qf.shape_check(
            n_records=10000,
            past_dates=3,
            step="P7D",
            slice_column="sale_date",
            slice_value="{{ ds }}",
        ),
    ],
    context={"ds": "2024-06-15"},
)
print(f"Anomaly: {result.overall_severity.value}")
if result.datasets:
    for vr in result.datasets[0].validation_results:
        if vr.details.get("top_contributing_features"):
            print("Top features:", vr.details["top_contributing_features"])


# --- 6. Write-Audit-Publish ---

try:
    my_df = spark.sql("SELECT * FROM raw.sales WHERE sale_date = '2024-06-15'")

    result = qf.write_audit_publish(
        df=my_df,
        target_table="catalog.schema.sales_prod",
        write_options={"mode": "overwrite", "partitionBy": ["sale_date"]},
        validations=[
            qf.threshold_check(
                aggregations={"row_count": "COUNT(*)"},
                rules=[{"metric": "row_count", "thresholds": {"error": {"min": 100}}}],
            ),
        ],
        name="sales_wap",
    )
    print(f"WAP: Published successfully ({result.overall_severity.value})")

except QualifireValidationError as e:
    print(f"WAP: Publication blocked — {e}")


# --- 7. Combined validations ---

try:
    result = qf.validate(
        table="catalog.schema.sales",
        filter_expr="sale_date = '{{ ds }}'",
        name="sales_comprehensive",
        validations=[
            qf.slo_check(column="updated_at", warning="PT4H", error="PT8H"),
            qf.threshold_check(
                aggregations={"row_count": "COUNT(*)"},
                rules=[{"metric": "row_count", "thresholds": {"error": {"min": 100}}}],
            ),
            qf.drift_check(
                aggregations={"avg_amount": "AVG(sale_amount)"},
                rules=[{
                    "metric": "avg_amount",
                    "compare": {"past_values": 3, "step": "P7D"},
                    "thresholds": {"warning": {"deviation_pct": 20}},
                }],
            ),
        ],
        context={"ds": "2024-06-15"},
    )
    print(f"Combined: {result.overall_severity.value}")
    for ds in result.datasets:
        for vr in ds.validation_results:
            print(f"  [{vr.severity.value}] {vr.validation_name}: {vr.message}")

except QualifireValidationError as e:
    print(f"Validation failed: {e}")
    for ds in e.result.datasets:
        for vr in ds.validation_results:
            if vr.severity == Severity.ERROR:
                print(f"  ERROR: {vr.message}")


# --- 8. Generate HTML report ---

from qualifire.reporting.html_report import generate_html_report

generate_html_report(result, output_path="qualifire_report.html")
print("Report written to qualifire_report.html")
