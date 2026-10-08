"""Generate aws_aidp/fixtures/demo-manifest.json for the POC demo.

Persona: 'Acme Insurance' — typical AWS-stack customer migrating to AIDP.
20 Athena queries are hand-written to exercise the dialect gaps the translator
will demo-fix (date_format, UNNEST, array_agg, ||, APPROX_DISTINCT, ...).
"""
from __future__ import annotations

import json
import time
from pathlib import Path

ACCOUNT = "999900001111"
REGION = "us-east-1"


# --- 20 Athena queries: realistic insurance analytics, each with a dialect gap ---

ATHENA_QUERIES: list[dict] = [
    {
        "name": "customer_revenue_monthly",
        "description": "Monthly revenue per customer for FY-26.",
        "database": "acme_curated",
        "query": (
            "SELECT customer_id,\n"
            "       date_format(order_date, '%Y-%m-%d') AS day,\n"
            "       SUM(premium_amount) AS revenue\n"
            "FROM acme_curated.policy_orders\n"
            "WHERE order_date >= DATE '2026-01-01'\n"
            "GROUP BY 1, 2"
        ),
    },
    {
        "name": "claim_fraud_score",
        "description": "Per-claim fraud signals — unnested fraud_signals array.",
        "database": "acme_curated",
        "query": (
            "SELECT c.claim_id, signal, cardinality(c.fraud_signals) AS signal_count\n"
            "FROM acme_curated.claims c\n"
            "CROSS JOIN UNNEST(c.fraud_signals) AS t(signal)\n"
            "WHERE c.claim_date >= current_date - INTERVAL '30' DAY"
        ),
    },
    {
        "name": "policy_churn_30d",
        "description": "Distinct policies that churned in last 30 days.",
        "database": "acme_curated",
        "query": (
            "SELECT APPROX_DISTINCT(policy_id) AS churned_policies\n"
            "FROM acme_curated.policy_lifecycle\n"
            "WHERE event_type = 'CHURN'\n"
            "  AND event_date >= current_date - INTERVAL '30' DAY"
        ),
    },
    {
        "name": "high_value_customers",
        "description": "Customers with LTV > 50k; uses ||  for human-friendly key.",
        "database": "acme_curated",
        "query": (
            "SELECT customer_id, region || '-' || tier AS segment, ltv\n"
            "FROM acme_curated.customer_value\n"
            "WHERE ltv > 50000\n"
            "ORDER BY ltv DESC"
        ),
    },
    {
        "name": "driver_history_per_policy",
        "description": "Aggregate the driver history per policy into an array.",
        "database": "acme_curated",
        "query": (
            "SELECT policy_id, array_agg(driver_event ORDER BY event_date) AS history\n"
            "FROM acme_curated.driver_events\n"
            "GROUP BY policy_id"
        ),
    },
    {
        "name": "underwriting_risk_buckets",
        "description": "Risk buckets using EXTRACT(YEAR FROM ...) — translates to year().",
        "database": "acme_curated",
        "query": (
            "SELECT customer_id,\n"
            "       CASE WHEN EXTRACT(YEAR FROM birth_date) < 1980 THEN 'senior'\n"
            "            WHEN EXTRACT(YEAR FROM birth_date) < 2000 THEN 'adult'\n"
            "            ELSE 'young' END AS age_bucket\n"
            "FROM acme_curated.customers"
        ),
    },
    {
        "name": "quarterly_premium_collected",
        "description": "Premium collected per quarter, windowed.",
        "database": "acme_curated",
        "query": (
            "SELECT date_trunc('quarter', collected_at) AS qtr,\n"
            "       SUM(amount) AS premium,\n"
            "       SUM(SUM(amount)) OVER (ORDER BY date_trunc('quarter', collected_at)) AS cumulative\n"
            "FROM acme_curated.premium_payments\n"
            "GROUP BY 1"
        ),
    },
    {
        "name": "policy_renewals_pending",
        "description": "Policies due to renew in next 30 days.",
        "database": "acme_curated",
        "query": (
            "SELECT policy_id, end_date\n"
            "FROM acme_curated.policies\n"
            "WHERE end_date BETWEEN current_date AND current_date + INTERVAL '30' DAY"
        ),
    },
    {
        "name": "agent_performance",
        "description": "Agent struct accessor + percentile.",
        "database": "acme_curated",
        "query": (
            "SELECT agent.id AS agent_id, agent.region AS region,\n"
            "       approx_percentile(deal_size, 0.9) AS p90_deal\n"
            "FROM acme_curated.agent_deals\n"
            "GROUP BY agent.id, agent.region"
        ),
    },
    {
        "name": "claim_amount_histogram",
        "description": "Distribution buckets — uses histogram() (UNSUPPORTED in Spark; flag).",
        "database": "acme_curated",
        "query": (
            "SELECT histogram(claim_amount, 10) AS buckets\n"
            "FROM acme_curated.claims\n"
            "WHERE claim_date >= DATE '2026-01-01'"
        ),
    },
    {
        "name": "fraud_signals_recent",
        "description": "Pulls fraud_signals out of a JSON column.",
        "database": "acme_curated",
        "query": (
            "SELECT claim_id,\n"
            "       JSON_EXTRACT(payload, '$.signals[0].type') AS top_signal,\n"
            "       JSON_EXTRACT_SCALAR(payload, '$.score') AS score\n"
            "FROM acme_raw.claim_events\n"
            "WHERE event_date >= current_date - INTERVAL '7' DAY"
        ),
    },
    {
        "name": "customer_lifetime_value",
        "description": "LTV with try_cast on a sometimes-dirty column.",
        "database": "acme_curated",
        "query": (
            "SELECT customer_id, try_cast(ltv_raw AS DOUBLE) AS ltv\n"
            "FROM acme_raw.customer_ltv_feed\n"
            "WHERE try_cast(ltv_raw AS DOUBLE) IS NOT NULL"
        ),
    },
    {
        "name": "policy_count_by_state",
        "description": "element_at on state code array.",
        "database": "acme_curated",
        "query": (
            "SELECT element_at(state_codes, 1) AS primary_state, COUNT(*) AS n\n"
            "FROM acme_curated.policy_coverage\n"
            "GROUP BY 1\n"
            "ORDER BY n DESC"
        ),
    },
    {
        "name": "claim_resolution_time",
        "description": "Resolution time in days using date_diff.",
        "database": "acme_curated",
        "query": (
            "SELECT claim_id,\n"
            "       date_diff('day', filed_at, resolved_at) AS days_to_resolve\n"
            "FROM acme_curated.claims\n"
            "WHERE resolved_at IS NOT NULL"
        ),
    },
    {
        "name": "high_risk_zones",
        "description": "Zip code regex extract.",
        "database": "acme_curated",
        "query": (
            "SELECT customer_id,\n"
            "       regexp_extract(address, '\\\\d{5}', 0) AS zip_code\n"
            "FROM acme_curated.customer_addresses\n"
            "WHERE risk_score > 0.8"
        ),
    },
    {
        "name": "agent_commission_payout",
        "description": "zip two arrays of equal length.",
        "database": "acme_curated",
        "query": (
            "SELECT agent_id, zip(deal_ids, commission_amounts) AS payouts\n"
            "FROM acme_curated.agent_commissions\n"
            "WHERE month = '2026-05'"
        ),
    },
    {
        "name": "customer_age_groups",
        "description": "sequence + bin assignment.",
        "database": "acme_curated",
        "query": (
            "SELECT age_bin, COUNT(customer_id) AS n\n"
            "FROM (\n"
            "  SELECT customer_id,\n"
            "         element_at(sequence(0, 100, 10), CAST(age / 10 AS INTEGER) + 1) AS age_bin\n"
            "  FROM acme_curated.customers\n"
            ") GROUP BY 1"
        ),
    },
    {
        "name": "monthly_payouts",
        "description": "Sum over partition.",
        "database": "acme_curated",
        "query": (
            "SELECT month,\n"
            "       SUM(payout) OVER (PARTITION BY product_line ORDER BY month) AS running_payout\n"
            "FROM acme_curated.payout_summary"
        ),
    },
    {
        "name": "policy_complexity_score",
        "description": "Chained CASE WHEN.",
        "database": "acme_curated",
        "query": (
            "SELECT policy_id,\n"
            "       CASE\n"
            "         WHEN num_riders > 5 AND coverage_amount > 1000000 THEN 'complex'\n"
            "         WHEN num_riders > 2 OR coverage_amount > 500000 THEN 'moderate'\n"
            "         ELSE 'simple'\n"
            "       END AS complexity\n"
            "FROM acme_curated.policies"
        ),
    },
    {
        "name": "claim_audit_trail",
        "description": "Ordinality on UNNEST (UNSUPPORTED in Spark; flag).",
        "database": "acme_curated",
        "query": (
            "SELECT claim_id, step, idx\n"
            "FROM acme_curated.claims c\n"
            "CROSS JOIN UNNEST(c.audit_steps) WITH ORDINALITY AS t(step, idx)"
        ),
    },
]


# --- 8 Glue ETL PySpark scripts: realistic insurance ETL, each exercising a
#     different slice of the Glue→Spark translator (from_catalog, from_options,
#     ApplyMapping, ResolveChoice, Join, Relationalize, getResolvedOptions, ...) ---

_GLUE_HEADER = (
    "import sys\n"
    "from awsglue.transforms import *\n"
    "from awsglue.utils import getResolvedOptions\n"
    "from pyspark.context import SparkContext\n"
    "from awsglue.context import GlueContext\n"
    "from awsglue.job import Job\n"
    "from awsglue.dynamicframe import DynamicFrame\n"
    "\n"
    "args = getResolvedOptions(sys.argv, ['JOB_NAME', 'run_date'])\n"
    "sc = SparkContext()\n"
    "glueContext = GlueContext(sc)\n"
    "spark = glueContext.spark_session\n"
    "job = Job(glueContext)\n"
    "job.init(args['JOB_NAME'], args)\n"
)
_GLUE_FOOTER = "\njob.commit()\n"

GLUE_SCRIPTS: dict[str, str] = {
    # 1. from_catalog read + ApplyMapping + s3 parquet write
    "raw_to_curated_claims": _GLUE_HEADER + (
        '\nclaims = glueContext.create_dynamic_frame.from_catalog(\n'
        '    database="acme_raw", table_name="claim_events", transformation_ctx="claims")\n'
        "mapped = ApplyMapping.apply(frame=claims, mappings=[\n"
        '    ("claim_id", "string", "claim_id", "string"),\n'
        '    ("amount", "string", "claim_amount", "double"),\n'
        '    ("filed_ts", "string", "filed_at", "timestamp")])\n'
        "df = mapped.toDF()\n"
        'df = df.filter(df.claim_amount > 0)\n'
        'curated = DynamicFrame.fromDF(df, glueContext, "curated")\n'
        "glueContext.write_dynamic_frame.from_options(frame=curated,\n"
        '    connection_type="s3",\n'
        '    connection_options={"path": "s3://acme-curated-data/claims/"},\n'
        '    format="parquet")\n'
    ) + _GLUE_FOOTER,

    # 2. from_catalog + ResolveChoice + partitioned write
    "raw_to_curated_policies": _GLUE_HEADER + (
        '\npolicies = glueContext.create_dynamic_frame.from_catalog(\n'
        '    database="acme_raw", table_name="policy_events", transformation_ctx="policies")\n'
        'resolved = ResolveChoice.apply(frame=policies, choice="make_struct")\n'
        "df = resolved.toDF()\n"
        'df.write.mode("overwrite").parquet("s3://acme-curated-data/policies/")\n'
    ) + _GLUE_FOOTER,

    # 3. from_options s3 read (no catalog) + DropNullFields
    "raw_to_curated_customers": _GLUE_HEADER + (
        '\ncust = glueContext.create_dynamic_frame.from_options(\n'
        '    connection_type="s3",\n'
        '    connection_options={"paths": ["s3://acme-raw-data/customer_events/"]},\n'
        '    format="json")\n'
        "clean = DropNullFields.apply(frame=cust)\n"
        "df = clean.toDF()\n"
        "glueContext.write_dynamic_frame.from_options(frame=DynamicFrame.fromDF(df, glueContext, \"c\"),\n"
        '    connection_type="s3",\n'
        '    connection_options={"path": "s3://acme-curated-data/customers/"},\n'
        '    format="parquet")\n'
    ) + _GLUE_FOOTER,

    # 4. Join two catalog frames (flag)
    "telematics_enrichment": _GLUE_HEADER + (
        '\ntel = glueContext.create_dynamic_frame.from_catalog(\n'
        '    database="acme_raw", table_name="telematics_raw", transformation_ctx="tel")\n'
        'veh = glueContext.create_dynamic_frame.from_catalog(\n'
        '    database="acme_curated", table_name="vehicles", transformation_ctx="veh")\n'
        'joined = Join.apply(tel, veh, "vehicle_id", "vehicle_id")\n'
        "df = joined.toDF()\n"
        'df.write.mode("overwrite").parquet("s3://acme-curated-data/telematics_enriched/")\n'
    ) + _GLUE_FOOTER,

    # 5. spark.sql business logic + Filter transform (flag)
    "fraud_signals_build": _GLUE_HEADER + (
        '\nevents = glueContext.create_dynamic_frame.from_catalog(\n'
        '    database="acme_raw", table_name="fraud_signals_raw", transformation_ctx="events")\n'
        "df = events.toDF()\n"
        'df.createOrReplaceTempView("fraud_raw")\n'
        "sig = spark.sql(\"\"\"\n"
        "    SELECT claim_id, collect_list(signal) AS signals, count(*) AS n\n"
        "    FROM fraud_raw WHERE score > 0.5 GROUP BY claim_id\n"
        "\"\"\")\n"
        'sig.write.mode("overwrite").parquet("s3://acme-curated-data/fraud_signals/")\n'
    ) + _GLUE_FOOTER,

    # 6. getResolvedOptions params + Relationalize (flag) + from_catalog
    "ml_features_v1_build": _GLUE_HEADER + (
        '\nfeats = glueContext.create_dynamic_frame.from_catalog(\n'
        '    database="acme_curated", table_name="customers", transformation_ctx="feats")\n'
        'flat = Relationalize.apply(frame=feats, staging_path="s3://acme-ml-artifacts/tmp/")\n'
        "df = flat.select(\"roottable\").toDF()\n"
        'df.write.mode("overwrite").parquet("s3://acme-curated-data/ml_features_v1/")\n'
    ) + _GLUE_FOOTER,

    # 7. SelectFields + RenameField + write_from_options
    "ml_features_v2_build": _GLUE_HEADER + (
        '\nbase = glueContext.create_dynamic_frame.from_catalog(\n'
        '    database="acme_curated", table_name="ml_features_v1", transformation_ctx="base")\n'
        'sel = SelectFields.apply(frame=base, paths=["customer_id", "ltv", "risk_score"])\n'
        'ren = RenameField.apply(frame=sel, old_name="ltv", new_name="lifetime_value")\n'
        "df = ren.toDF()\n"
        "glueContext.write_dynamic_frame.from_options(frame=DynamicFrame.fromDF(df, glueContext, \"v2\"),\n"
        '    connection_type="s3",\n'
        '    connection_options={"path": "s3://acme-curated-data/ml_features_v2/"},\n'
        '    format="parquet")\n'
    ) + _GLUE_FOOTER,

    # 8. plain-ish aggregate job (mostly clean — high PASS rate)
    "daily_aggregates": _GLUE_HEADER + (
        '\npay = glueContext.create_dynamic_frame.from_catalog(\n'
        '    database="acme_curated", table_name="premium_payments", transformation_ctx="pay")\n'
        "df = pay.toDF()\n"
        "agg = (df.groupBy(\"product_line\")\n"
        "         .agg({\"amount\": \"sum\"})\n"
        "         .withColumnRenamed(\"sum(amount)\", \"total_premium\"))\n"
        'agg.write.mode("overwrite").parquet("s3://acme-curated-data/daily_aggregates/")\n'
    ) + _GLUE_FOOTER,
}


def main(out: Path) -> None:
    # ---- S3: 5 buckets, mix of raw/curated/logs ----
    s3_items = [
        {"name": "acme-raw-data",       "region": REGION, "created_at": "2024-03-12 10:14:00+00:00"},
        {"name": "acme-curated-data",   "region": REGION, "created_at": "2024-03-12 10:15:00+00:00"},
        {"name": "acme-athena-results", "region": REGION, "created_at": "2024-04-01 09:00:00+00:00"},
        {"name": "acme-glue-scripts",   "region": REGION, "created_at": "2024-03-15 12:00:00+00:00"},
        {"name": "acme-ml-artifacts",   "region": "us-west-2", "created_at": "2024-06-22 08:00:00+00:00"},
    ]

    # ---- Glue: 1 main db + 1 raw db, 50 tables total, 8 ETL jobs ----
    glue_dbs = [
        {"name": "acme_raw",      "location_uri": "s3://acme-raw-data/"},
        {"name": "acme_curated",  "location_uri": "s3://acme-curated-data/"},
    ]
    table_names = [
        # raw (15)
        "claim_events", "policy_events", "customer_events", "agent_events", "payment_events",
        "address_events", "underwriting_events", "fraud_signals_raw", "telematics_raw",
        "broker_feed", "partner_feed", "external_credit_feed", "weather_feed",
        "vendor_feed", "regulatory_feed",
        # curated (35)
        "policies", "policy_orders", "policy_lifecycle", "policy_coverage", "policy_riders",
        "claims", "claim_decisions", "claim_payouts", "claim_witnesses",
        "customers", "customer_addresses", "customer_value", "customer_ltv_feed",
        "agents", "agent_deals", "agent_commissions",
        "premium_payments", "payout_summary", "renewal_pipeline",
        "drivers", "driver_events", "vehicles", "vehicle_inspections",
        "underwriting_decisions", "underwriting_factors",
        "compliance_events", "fraud_decisions", "fraud_signals",
        "broker_partners", "channel_revenue",
        "weather_zones", "risk_zones", "macro_factors",
        "ml_features_v1", "ml_features_v2",
    ]
    tables = []
    for n in table_names[:15]:
        tables.append({
            "database": "acme_raw", "name": n,
            "location": f"s3://acme-raw-data/{n}/",
            "input_format": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "table_type": "EXTERNAL_TABLE",
            "partition_keys": ["event_date"] if "events" in n or "feed" in n else [],
            "column_count": 12,
        })
    for n in table_names[15:]:
        tables.append({
            "database": "acme_curated", "name": n,
            "location": f"s3://acme-curated-data/{n}/",
            "input_format": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "table_type": "EXTERNAL_TABLE",
            "partition_keys": ["month"] if "monthly" in n or "summary" in n else [],
            "column_count": 18,
        })
    glue_jobs = [
        {"name": f"acme_etl_{i}", "type": "glueetl", "glue_version": "4.0",
         "script_location": f"s3://acme-glue-scripts/etl_{i}.py",
         "python_version": "3", "worker_type": "G.1X", "num_workers": 5,
         "script": GLUE_SCRIPTS[i]}
        for i in ("raw_to_curated_claims", "raw_to_curated_policies", "raw_to_curated_customers",
                  "telematics_enrichment", "fraud_signals_build", "ml_features_v1_build",
                  "ml_features_v2_build", "daily_aggregates")
    ]

    # ---- Athena: 2 workgroups, 20 queries ----
    athena_qs = [
        {**q, "id": f"q-{1000+i:04d}", "workgroup": "analytics" if i % 2 == 0 else "ds_team"}
        for i, q in enumerate(ATHENA_QUERIES)
    ]

    # ---- EMR: 2 clusters ----
    emr_clusters = [
        {"id": "j-ABC123", "name": "acme-spark-prod", "state": "RUNNING",
         "release_label": "emr-6.15.0", "instance_collection_type": "INSTANCE_GROUP",
         "applications": ["Spark", "Hive", "Hadoop", "JupyterHub"],
         "service_role": "EMR_DefaultRole", "auto_terminate": False,
         "log_uri": "s3://acme-emr-logs/"},
        {"id": "j-XYZ789", "name": "acme-ml-train", "state": "WAITING",
         "release_label": "emr-6.15.0", "instance_collection_type": "INSTANCE_FLEET",
         "applications": ["Spark", "TensorFlow", "JupyterEnterpriseGateway"],
         "service_role": "EMR_DefaultRole", "auto_terminate": True,
         "log_uri": "s3://acme-emr-logs/"},
    ]
    emr_notebooks = [
        {"id": "ex-1", "name": "fraud_features_train", "status": "FINISHED",
         "editor_id": "e-EDITOR1", "cluster_id": "j-XYZ789"},
        {"id": "ex-2", "name": "policy_propensity_score", "status": "FINISHED",
         "editor_id": "e-EDITOR1", "cluster_id": "j-XYZ789"},
        {"id": "ex-3", "name": "claim_severity_v3", "status": "RUNNING",
         "editor_id": "e-EDITOR2", "cluster_id": "j-XYZ789"},
        {"id": "ex-4", "name": "ad_hoc_explorations", "status": "FINISHED",
         "editor_id": "e-EDITOR1", "cluster_id": "j-ABC123"},
    ]

    # ---- SageMaker: 3 notebooks, 12 training jobs, 5 models, 2 pipelines ----
    sm_notebooks = [
        {"name": "fraud-model-rd",    "status": "InService",  "instance_type": "ml.t3.medium", "url": "n.sgm/fraud-model-rd"},
        {"name": "claim-severity-rd", "status": "InService",  "instance_type": "ml.t3.large",  "url": "n.sgm/claim-severity-rd"},
        {"name": "churn-prop-rd",     "status": "Stopped",    "instance_type": "ml.t3.medium", "url": "n.sgm/churn-prop-rd"},
    ]
    sm_training = [
        {"name": f"fraud-xgb-{i:02d}",       "status": "Completed", "created_at": f"2026-05-{i+1:02d}T10:00", "ended_at": f"2026-05-{i+1:02d}T11:30"}
        for i in range(6)
    ] + [
        {"name": f"severity-lgbm-{i:02d}",   "status": "Completed", "created_at": f"2026-06-{i+1:02d}T10:00", "ended_at": f"2026-06-{i+1:02d}T11:00"}
        for i in range(4)
    ] + [
        {"name": "churn-xgb-failed-once",    "status": "Failed",    "created_at": "2026-06-10T10:00", "ended_at": "2026-06-10T10:14"},
        {"name": "churn-xgb-retrain-2",      "status": "Completed", "created_at": "2026-06-11T10:00", "ended_at": "2026-06-11T11:00"},
    ]
    sm_models = [
        {"name": "acme-fraud-v3",       "created_at": "2026-05-21T11:30"},
        {"name": "acme-severity-v2",    "created_at": "2026-06-04T11:00"},
        {"name": "acme-churn-v1",       "created_at": "2026-06-11T11:00"},
        {"name": "acme-fraud-v3-shadow","created_at": "2026-06-15T11:00"},
        {"name": "acme-fraud-v2",       "created_at": "2026-03-12T11:00"},
    ]
    sm_pipelines = [
        {"name": "acme-fraud-train-eval",     "display_name": "Fraud train+eval",   "status": "Succeeded",  "created_at": "2026-04-01"},
        {"name": "acme-severity-train-eval",  "display_name": "Severity train+eval","status": "Succeeded",  "created_at": "2026-04-15"},
    ]

    manifest = {
        "account_id": ACCOUNT,
        "region": REGION,
        "scanned_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "sources_scanned": ["s3", "glue", "athena", "emr", "sagemaker"],
        "customer_persona": "Acme Insurance — sample/fixture, no real data",
        "sources": {
            "s3": {
                "summary": {"bucket_count": len(s3_items),
                             "by_region": {"us-east-1": 4, "us-west-2": 1}},
                "items": s3_items,
            },
            "glue": {
                "summary": {"database_count": len(glue_dbs),
                             "table_count": len(tables),
                             "tables_per_db": {"acme_raw": 15, "acme_curated": 35},
                             "job_count": len(glue_jobs)},
                "items": {"databases": glue_dbs, "tables": tables, "jobs": glue_jobs},
            },
            "athena": {
                "summary": {"workgroup_count": 2,
                             "named_query_count": len(athena_qs),
                             "queries_per_workgroup": {"analytics": 10, "ds_team": 10}},
                "items": {"workgroups": ["analytics", "ds_team"],
                          "named_queries": athena_qs},
            },
            "emr": {
                "summary": {"cluster_count": len(emr_clusters),
                             "by_state": {"RUNNING": 1, "WAITING": 1},
                             "notebook_execution_count": len(emr_notebooks)},
                "items": {"clusters": emr_clusters, "notebooks": emr_notebooks},
            },
            "sagemaker": {
                "summary": {"notebook_count": len(sm_notebooks),
                             "training_job_count": len(sm_training),
                             "model_count": len(sm_models),
                             "pipeline_count": len(sm_pipelines)},
                "items": {"notebooks": sm_notebooks, "training_jobs": sm_training,
                          "models": sm_models, "pipelines": sm_pipelines},
            },
        },
    }

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(manifest, indent=2))
    print(f"wrote {out} ({out.stat().st_size:,} bytes)")
    print(f"  s3: {len(s3_items)} buckets")
    print(f"  glue: {len(glue_dbs)} dbs, {len(tables)} tables, {len(glue_jobs)} jobs")
    print(f"  athena: {len(athena_qs)} named queries (across 2 workgroups)")
    print(f"  emr: {len(emr_clusters)} clusters, {len(emr_notebooks)} notebook execs")
    print(f"  sagemaker: {len(sm_notebooks)} nbs, {len(sm_training)} training jobs, "
          f"{len(sm_models)} models, {len(sm_pipelines)} pipelines")


if __name__ == "__main__":
    main(Path(__file__).parent / "demo-manifest.json")
