# Night Shift synthetic failing job (notebook cell). Everything lives in default.nightshift_demo so it
# can be dropped with: DROP SCHEMA default.nightshift_demo CASCADE
#
# Run it once with FAILURE_MODE=none to create default.nightshift_demo.orders_bronze and orders_silver
# (key order_id, ordering column updated_at); the two synthetic entries in jobs/watched_merges.json
# watch those tables. Silver has exactly the bronze columns and no load-time column: a column a statement
# fills from the clock (merged_at = current_timestamp()) differs between a replay and the recompute it is
# compared with, so a watched statement that writes one grades snapshot_wrong and is never drafted for.
# A silver table created by an earlier version of this cell has such a column; drop the schema first.
#
# Parameter FAILURE_MODE in {none, schema_drift, bad_value, upstream_missing, oom_like}. It is read
# from a job parameter placeholder, then the FAILURE_MODE env var, else "none". Each mode fails
# deterministically at the MERGE step so the fingerprinter and the repair harness have a fixture.
import datetime as _dt
import json
import os
import random
import time

FAILURE_MODE = "{{FAILURE_MODE}}"
if FAILURE_MODE.startswith("{{"):
    FAILURE_MODE = os.environ.get("FAILURE_MODE", "none")
FAILURE_MODE = (FAILURE_MODE or "none").strip().lower()
assert FAILURE_MODE in {"none", "schema_drift", "bad_value", "upstream_missing", "oom_like"}, FAILURE_MODE

SCHEMA = "default.nightshift_demo"
BRONZE = f"{SCHEMA}.orders_bronze"
SILVER = f"{SCHEMA}.orders_silver"
SEED = 20261102
N_ROWS = 1000
N_BATCH = 200
REGIONS = ["north", "south", "east", "west", "central"]
STATUSES = ["NEW", "PAID", "SHIPPED", "RETURNED"]

t0 = time.time()
print(json.dumps({"step": "start", "failure_mode": FAILURE_MODE, "schema": SCHEMA}))


def synth_rows(n, rng, start_id=1, day0=_dt.date(2026, 10, 1)):
    rows = []
    for i in range(n):
        oid = start_id + i
        rows.append((
            oid,
            f"C{rng.randint(1, 250):04d}",
            REGIONS[rng.randrange(len(REGIONS))],
            STATUSES[rng.randrange(len(STATUSES))],
            round(rng.uniform(5.0, 950.0), 2),
            rng.randint(1, 12),
            day0 + _dt.timedelta(days=rng.randint(0, 30)),
            _dt.datetime(2026, 11, 1, 23, 0, 0) + _dt.timedelta(seconds=rng.randint(0, 3600)),
        ))
    return rows


COLUMNS = ["order_id", "customer_id", "region", "status", "amount", "quantity", "order_date", "updated_at"]
DDL_TYPES = ("order_id INT, customer_id STRING, region STRING, status STRING, amount DOUBLE, "
             "quantity INT, order_date DATE, updated_at TIMESTAMP")

# ---------------------------------------------------------------- 1. bronze (idempotent, seeded)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
rng = random.Random(SEED)
bronze_df = spark.createDataFrame(synth_rows(N_ROWS, rng), schema=DDL_TYPES)
bronze_df.write.format("delta").mode("overwrite").saveAsTable(BRONZE)
print(json.dumps({"step": "bronze", "rows": spark.table(BRONZE).count()}))

# ---------------------------------------------------------------- 2. silver target (first run seeds it)
spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER} ({DDL_TYPES}) USING DELTA")
if spark.table(SILVER).columns != COLUMNS:
    raise ValueError(f"{SILVER} has columns {spark.table(SILVER).columns}, expected {COLUMNS}: "
                     f"drop the schema (DROP SCHEMA {SCHEMA} CASCADE) and run this cell again")
if spark.table(SILVER).count() == 0:
    spark.sql(f"INSERT INTO {SILVER} SELECT * FROM {BRONZE}")
print(json.dumps({"step": "silver_seed", "rows": spark.table(SILVER).count()}))

# ---------------------------------------------------------------- 3. incoming batch (half updates, half new)
rng_b = random.Random(SEED + 1)
updates = synth_rows(N_BATCH // 2, rng_b, start_id=1)                 # order_id 1..100 -> updates
inserts = synth_rows(N_BATCH // 2, rng_b, start_id=N_ROWS + 1)        # new ids -> inserts
batch_df = spark.createDataFrame(updates + inserts, schema=DDL_TYPES)

# ---------------------------------------------------------------- 4. failure injection (deterministic)
if FAILURE_MODE == "schema_drift":
    # upstream renamed order_date -> orderDate; MERGE fails with "cannot resolve order_date"
    batch_df = batch_df.withColumnRenamed("order_date", "orderDate")
elif FAILURE_MODE == "bad_value":
    # a non-date string in the date column; ANSI mode makes the CAST fail instead of yielding NULL
    from pyspark.sql import functions as F
    spark.conf.set("spark.sql.ansi.enabled", "true")
    batch_df = batch_df.withColumn("order_date", F.col("order_date").cast("string"))
    batch_df = batch_df.withColumn(
        "order_date", F.when(F.col("order_id") == 7, F.lit("2026-13-45 not-a-date")).otherwise(F.col("order_date"))
    ).withColumn("order_date", F.col("order_date").cast("date"))
elif FAILURE_MODE == "upstream_missing":
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE}")
    batch_df = spark.table(BRONZE)  # raises TABLE_OR_VIEW_NOT_FOUND
elif FAILURE_MODE == "oom_like":
    # cartesian explode, guarded to stay under ~5 minutes on a small cluster, then a memory error
    from pyspark.sql import functions as F
    factor = int(os.environ.get("NS_OOM_FACTOR", "2000"))
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.shuffle.partitions", "400")
    big = spark.table(BRONZE).crossJoin(spark.range(factor).withColumnRenamed("id", "k"))
    big = big.withColumn("arr", F.array(*[F.lit(i) for i in range(10)])).withColumn("v", F.explode("arr"))
    deadline = t0 + 270
    agg = big.groupBy("region", "k").agg(F.sum("amount").alias("s"), F.count("*").alias("n"))
    rows_seen = agg.count()
    print(json.dumps({"step": "oom_like_explode", "rows": rows_seen, "elapsed_s": round(time.time() - t0, 1)}))
    if time.time() < deadline:
        sample = agg.limit(200000).toPandas()
        print(json.dumps({"step": "oom_like_collect", "driver_rows": len(sample)}))
    raise MemoryError("oom_like: java.lang.OutOfMemoryError: GC overhead limit exceeded (synthetic)")

# ---------------------------------------------------------------- 5. MERGE into silver
batch_df.createOrReplaceTempView("incoming_batch")
merge_sql = f"""
MERGE INTO {SILVER} t
USING incoming_batch s
ON t.order_id = s.order_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET
  t.customer_id = s.customer_id, t.region = s.region, t.status = s.status, t.amount = s.amount,
  t.quantity = s.quantity, t.order_date = s.order_date, t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (order_id, customer_id, region, status, amount, quantity, order_date, updated_at)
  VALUES (s.order_id, s.customer_id, s.region, s.status, s.amount, s.quantity, s.order_date, s.updated_at)
"""
spark.sql(merge_sql)
silver_count = spark.table(SILVER).count()
print(json.dumps({"step": "merge_done", "silver_rows": silver_count, "failure_mode": FAILURE_MODE,
                  "elapsed_s": round(time.time() - t0, 1)}))
