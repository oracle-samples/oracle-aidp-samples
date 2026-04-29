---
description: Consume an OCI Streaming stream from an AIDP notebook via Spark structured streaming (Kafka-compat). Use when the user mentions OCI Streaming, Kafka on OCI, stream pool, structured streaming, or wants to read Kafka messages into Spark. Auth is SASL/PLAIN with an OCI auth token. Pattern matches the official Oracle AIDP sample.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-streaming-kafka` — OCI Streaming via Spark structured streaming

Mirrors the official Oracle AIDP sample at [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Streaming/StreamingFromOCIStreamingService.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Streaming/StreamingFromOCIStreamingService.ipynb).

## When to use
- User wants to consume an OCI Streaming stream (Kafka-compat) from an AIDP notebook.
- User mentions: "OCI Streaming", "Kafka on OCI", "stream pool", "structured streaming", "Kafka topic".

## When NOT to use
- For batch reads of files in OCI Object Storage → standard `spark.read.format("csv"|"parquet").load("oci://...")` is fine without this skill.
- For other Kafka deployments (Confluent, MSK) — same Spark Kafka API works; just point `bootstrap.servers` at the right broker and skip the OCI-specific username format.

## Prerequisites in the AIDP notebook
1. Spark Kafka connector on the cluster (`spark-sql-kafka-0-10_<scala>:<spark>` — AIDP's `tpcds` cluster has this).
2. Helpers on `sys.path`.
3. OCI Streaming **stream pool OCID** + region.
4. An OCI **auth token** (Profile → Auth tokens → Generate Token in the OCI console). 1-hour TTL — refresh before any job that runs longer than that.
5. A **Volumes-mounted checkpoint location** (`/Volumes/<catalog>/<schema>/<volume>/_checkpoints/...`). **Do NOT use `/Workspace/...` — the streaming engine fails silently.** The helper's `validate_checkpoint_path()` raises a clear `ValueError` if you try.

## Auth: SASL/PLAIN with OCI auth token

```python
import os
from oracle_ai_data_platform_connectors.streaming import (
    bootstrap_for_region, build_kafka_options_sasl_plain,
    validate_checkpoint_path,
)

# Bootstrap. Either generic-regional (default) or cell-prefixed (matches OCI
# Console's "messages-endpoint" shape — pick whichever your stream pool shows):
bootstrap = bootstrap_for_region(os.environ["OCI_REGION"])              # streaming.<region>...:9092
# bootstrap = bootstrap_for_region(os.environ["OCI_REGION"], cell=1)    # cell-1.streaming.<region>...:9092

opts = build_kafka_options_sasl_plain(
    bootstrap_servers=bootstrap,
    tenancy_name=os.environ["OCI_TENANCY_NAME"],     # display name, NOT OCID
    username=os.environ["OCI_USERNAME"],             # OCI user; for IAM-Domains
                                                     # use "oracleidentitycloudservice/<email>"
    stream_pool_ocid=os.environ["OCI_STREAM_POOL_OCID"],
    auth_token=os.environ["OCI_AUTH_TOKEN"],         # 1h TTL — refresh before long jobs
    topic=os.environ["KAFKA_TOPIC"],
    starting_offsets="latest",                       # or "earliest" for backfill
    # Optional tuning (matches the official sample):
    max_partition_fetch_bytes=1024 * 1024,
    max_offsets_per_trigger=5,                       # cap rows per micro-batch (demo-friendly)
)

raw = spark.readStream.format("kafka").options(**opts).load()

# Validate checkpoint path BEFORE starting (saves you from silent FUSE failures)
checkpoint = validate_checkpoint_path(os.environ["KAFKA_CHECKPOINT_VOLUME"])
sink_path  = os.environ["KAFKA_SINK_VOLUME"]   # e.g. /Volumes/default/default/streaming/kafkaStreamingSink

# Match the official sample: write to a Delta sink under /Volumes/.
query = (
    raw.writeStream
       .queryName("OCIStreamingSource")
       .format("delta")
       .option("checkpointLocation", checkpoint)
       .start(sink_path)
)
query.awaitTermination(timeout=120)
print("input rows in last batch:", (query.lastProgress or {}).get("numInputRows"))
```

For an inline test against an existing topic with `print`-style output:

```python
out_df = raw.selectExpr("CAST(key AS STRING) AS k", "CAST(value AS STRING) AS v",
                        "topic", "partition", "offset")
q = (out_df.writeStream.format("memory").queryName("kafka_test")
            .option("checkpointLocation", checkpoint)
            .trigger(processingTime="5 seconds").start())
q.awaitTermination(timeout=60)
spark.sql("SELECT * FROM kafka_test").show()
q.stop()
```

## Username format (the most common gotcha)

OCI Streaming's Kafka SASL username is `<tenancy_name>/<user>/<stream_pool_ocid>`. The middle segment depends on tenancy type:

| Tenancy | `username` argument |
|---|---|
| Legacy IAM | `<email>` |
| IAM Domains (modern) | `oracleidentitycloudservice/<email>` |

If you `oci iam user list` shows the user with `oracleidentitycloudservice/...` prefix, use the prefixed form.

## Gotchas
- **Checkpoint path** — must be `/Volumes/...`. The `validate_checkpoint_path()` helper raises a `ValueError` if you pass `/Workspace/...` or `oci://...`. This is the #1 cause of "stream runs but no data appears" complaints in AIDP.
- **Auth token TTL = 1 hour.** For longer runs, plan to checkpoint, stop the stream, refresh the token, restart from checkpoint. RP-based Kafka SASL (`com.oracle.bmc.auth.sasl.ResourcePrincipalsLoginModule`) is blocked at the AIDP platform level (RP tokens not provided).
- **Username format** — tenancy *name* (display name), NOT tenancy OCID. IAM-Domains users need the `oracleidentitycloudservice/` prefix.
- **Streaming jobs run forever.** The AIDP workflow timeout doesn't apply once a streaming query is started. Set `Max Concurrent Runs = 1` on the wrapping job.
- **Bootstrap host** — the OCI Console's stream-pool detail page shows a "messages-endpoint" like `https://cell-1.streaming.<region>.oci.oraclecloud.com`. Either form (`streaming.<region>...` or `cell-N.streaming.<region>...`) works for the Kafka layer.

## References
- Helpers: [scripts/oracle_ai_data_platform_connectors/streaming/kafka.py](../../scripts/oracle_ai_data_platform_connectors/streaming/kafka.py)
- Official Oracle AIDP sample: [StreamingFromOCIStreamingService.ipynb](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Streaming/StreamingFromOCIStreamingService.ipynb)
- OCI Streaming Kafka compat docs: https://docs.oracle.com/en-us/iaas/Content/Streaming/Tasks/kafkacompatibility_topic-Configuration.htm
