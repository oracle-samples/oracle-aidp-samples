# Publish Spark Structured Streaming metrics to OCI Monitoring

Monitor and alarm on your AIDP streaming jobs: their input and processing rate, micro-batch latency,
watermark lag and state size. The metrics go to **OCI Monitoring** (Metrics Explorer, Alarms,
Dashboards).

The cluster metrics AIDP publishes to your tenancy (namespace `oracle_aidataplatform`) cover tasks,
jobs, shuffle, JVM and host resources. They do **not** include Structured Streaming progress. This
sample adds a small PySpark `StreamingQueryListener` that reads each micro-batch's
`StreamingQueryProgress` and posts the numbers to a **custom OCI Monitoring namespace** that you own.

| File | What it is |
|---|---|
| [oci_streaming_listener.py](./oci_streaming_listener.py) | The listener: extracts the metrics, queues them and posts them in the background |
| [streaming_metrics_to_oci.ipynb](./streaming_metrics_to_oci.ipynb) | Attaches the listener, runs a 4-minute demo stream and reads the metrics back from OCI Monitoring |
| [tests/test_oci_streaming_listener.py](./tests/test_oci_streaming_listener.py) | Offline unit tests (no Spark, no OCI) |

## How it works

```
 Spark driver (AIDP cluster)
 ┌──────────────────────────────────────────────────────────────┐
 │ streaming query ── every micro-batch ──► onQueryProgress()   │
 │                                            │ extract metrics │
 │                                            ▼                 │
 │                                   in-memory queue (bounded)  │
 │                                            │ every 10 s      │
 │                     background thread ◄────┘                 │
 └─────────────────────────────┬────────────────────────────────┘
                               │ PostMetricData (≤ 50 per call), signed with the
                               │ API key read from the AIDP Credential Store
                               ▼
        OCI Monitoring ─ namespace custom_spark_streaming ─► Metrics Explorer / Alarms
```

- Spark calls the listener on the driver for **every streaming query in the session**. Nothing has
  to change in the query code.
- **Posting never blocks a query.** `onQueryProgress` only queues values, and a daemon thread posts
  them every `flush_seconds` (default 10 s) in batches of at most 50 metric objects, the
  `PostMetricData` limit.
- **Failures never break a query.** Post errors and partial failures are counted in
  `listener.stats` (`posted`, `failed`, `dropped`, `last_error`) and never raised. If OCI is
  unreachable for a long time, the bounded queue drops values rather than growing without limit.
- Each value is stamped with the micro-batch's trigger time.

### Metrics

One datapoint per micro-batch for each metric:

| OCI metric | Source in `StreamingQueryProgress` | Equivalent Spark gauge |
|---|---|---|
| `inputRowsPerSecond` | `inputRowsPerSecond` | `inputRate-total` |
| `processedRowsPerSecond` | `processedRowsPerSecond` | `processingRate-total` |
| `triggerExecutionMs` | `durationMs.triggerExecution` | `latency` |
| `batchDurationMs` | `durationMs.addBatch` | – |
| `numInputRows` | `numInputRows` | – |
| `watermarkEpochMs` | `eventTime.watermark` (epoch ms) | `eventTime-watermark` |
| `watermarkLagMs` | trigger time − watermark | – |
| `stateRowsTotal` | Σ `stateOperators[].numRowsTotal` | `states-rowsTotal` |
| `stateMemoryUsedBytes` | Σ `stateOperators[].memoryUsedBytes` | `states-usedBytes` |

- The watermark metrics appear only for queries with a watermark, and the state metrics only for
  stateful queries.
- **Dimensions:** `queryName` (or `unnamed`), `queryId`, `sparkAppId` (set by the notebook), and
  any others you pass. Name your queries with `.queryName(...)` so the metrics are easy to find.

## Setup

### 1. An OCI API key for posting metrics

Resource principal is not available to notebook code on AIDP clusters, so the listener signs its
calls with an **OCI API key**. Use a dedicated IAM user with only the permissions below; don't use
a personal key.

1. In the OCI Console, create a user (or use an existing service user) and add it to a group, for
   example `streaming-metrics-publishers`.
2. Under the user's **API keys**, add an API key and download the private key. Note the
   fingerprint, the user OCID, the tenancy OCID and the region.

### 2. IAM policy

```
Allow group streaming-metrics-publishers to use metrics in compartment <compartment-name>
    where target.metrics.namespace = 'custom_spark_streaming'
Allow group streaming-metrics-publishers to read metrics in compartment <compartment-name>
```

The first statement is enough to publish. The second is used only by the notebook's
"read the metrics back" cell.

### 3. Store the key in the AIDP Credential Store

In AIDP Workbench, create a credential of type **Secret Token**, e.g. named
`streaming_metrics_oci_api_key`, with these five key/value pairs:

| Key | Value |
|---|---|
| `user` | user OCID |
| `tenancy` | tenancy OCID |
| `fingerprint` | API key fingerprint |
| `region` | e.g. `us-ashburn-1` |
| `key_content` | the full PEM text of the private key, including the `-----BEGIN … KEY-----` lines |

The listener reads them with `aidputils.secrets.get(name=..., key=...)` and never prints them. The
private key must not be encrypted with a passphrase.

You can also create the credential through the Credential Store REST API, using the same
`SECRET_TOKEN` type and a `secretTokenPair` list.

### 4. Upload the files

Upload `oci_streaming_listener.py` and `streaming_metrics_to_oci.ipynb` into one workspace folder,
e.g. `/Workspace/streaming-metrics-to-oci-monitoring`. The notebook imports the listener from
`LISTENER_DIR`, so keep that set to the folder you used.

## Run the demo

1. Open `streaming_metrics_to_oci.ipynb` and attach a cluster (Spark 3.4 or later; AIDP clusters run
   Spark 3.5). The OCI Python SDK is already installed on AIDP clusters.
2. In the configuration cell, set `COMPARTMENT_ID`, and `CREDENTIAL_NAME` if you named it
   differently. Keep `RUN_DEMO_MINUTES = 4`, or use `0` to only attach the listener.
3. Run all cells:
   - **Attach** loads the credential, builds the listener and calls `spark.streams.addListener(...)`.
   - **Demo stream:** a `rate` source (200 rows/s) feeds a 10-second windowed count with a 30-second
     watermark into a `noop` sink, so every metric has data. Every 30 s it prints the batch id and
     `listener.stats`.
   - **Final flush** removes the listener and closes it. Expect `failed: 0` and
     `flush_incomplete: False`.
   - **Read back** queries each metric from OCI Monitoring for the demo query.

Example output of the read-back cell (2-minute demo):

```
inputRowsPerSecond       series=1 points=2 min=200 max=200 last=200
processedRowsPerSecond   series=1 points=2 min=2347 max=2597 last=2597
triggerExecutionMs       series=1 points=2 min=469 max=522 last=469
watermarkLagMs           series=1 points=2 ...
stateRowsTotal           series=1 points=2 min=40 max=50 last=50
stateMemoryUsedBytes     series=1 points=2 min=7776 max=15920 last=15920
```

### See the metrics in the OCI Console

**Observability & Management → Monitoring → Metrics Explorer**. Pick your compartment and the
namespace `custom_spark_streaming`. Example queries:

```
inputRowsPerSecond[1m]{queryName = "orders_stream"}.mean()
processedRowsPerSecond[1m]{queryName = "orders_stream"}.mean()
triggerExecutionMs[1m]{queryName = "orders_stream"}.max()
watermarkLagMs[5m].max()
stateMemoryUsedBytes[5m].max()
```

Alarm ideas (**Monitoring → Alarm Definitions**):

| Condition | Query |
|---|---|
| Event time falling behind (> 10 min) | `watermarkLagMs[5m]{queryName = "orders_stream"}.max() > 600000` |
| Micro-batches slower than the trigger interval | `triggerExecutionMs[5m]{queryName = "orders_stream"}.mean() > 30000` |
| Query stopped reporting (stalled or terminated) | `inputRowsPerSecond[5m]{queryName = "orders_stream"}.absent()` |
| State growing without bound | `stateMemoryUsedBytes[1h]{queryName = "orders_stream"}.max() > 2000000000` |

## Use it in your own streaming job

```python
import sys
sys.path.insert(0, "/Workspace/streaming-metrics-to-oci-monitoring")
import oci_streaming_listener as osl

cfg = osl.oci_config_from_secrets(aidputils.secrets, "streaming_metrics_oci_api_key")
listener = osl.OciStreamingMetricsListener.from_config(
    cfg,
    compartment_id="<compartment OCID>",
    namespace="custom_spark_streaming",          # optional, this is the default
    dimensions={"pipeline": "orders"},           # optional: up to 18 extra dimensions
)
spark.streams.addListener(listener)               # before or after starting queries

# ... start / run your streaming queries (name them with .queryName("...")) ...

spark.streams.removeListener(listener)            # when the session is done
print(listener.close())                           # final flush; returns the counters
```

Constructor options: `flush_seconds` (default `10`), `queue_max` (default `10000` values),
`resource_group` (an optional OCI metric resource group), and `log` (default `print`).

## Notes and limits

- **Custom namespace rules.** The name must match `^[A-Za-z][A-Za-z0-9_]*$` and must not start with
  `oci_` or `oracle_`. OCI allows 20 dimensions per metric; the listener uses 2, so you can add up
  to 18. Dimension keys must be printable ASCII without periods or spaces (OCI rejects the whole
  batch otherwise). The constructor rejects values outside these rules.
- **Datapoint timestamps** must be recent (OCI accepts roughly the last 2 hours). Values that wait in
  the queue during a long OCI outage are rejected and counted as `failed`.
- **`close()`** waits up to 90 s for the final flush. One OCI call can take up to the SDK's 60 s read
  timeout. If the flush is still running, the result says `worker_alive: True` /
  `flush_incomplete: True`.
- **Checkpoint location.** On AIDP, use an explicit `file:///…` path (for short demos) or Object
  Storage. A bare `/tmp/...` checkpoint fails with *"Wrong FS: compute:/tmp/…, expected: file:///"*.
- **Endpoint.** `from_config` derives the `telemetry-ingestion` host from the region via
  `oci.regions.endpoint_for`, so government and sovereign realms resolve to their own domain.
- **Small demos and shuffle partitions.** A stateful query keeps one state store per shuffle
  partition. With the default 200 partitions, the tiny demo spent ~8 s per micro-batch; with
  `spark.sql.shuffle.partitions=4` it took ~0.5 s. The listener reports this, it doesn't cause it.
  With 4 partitions, the measured latency matched runs without the listener.
- **Cost.** Each micro-batch produces up to 9 datapoints per query. Custom metrics count toward OCI
  Monitoring ingestion. Use a longer trigger interval, or filter queries in your own subclass, if
  that's too many.

## Tests

```bash
cd data-engineering/streaming-metrics-to-oci-monitoring
python3 -m pytest -q tests
```

The tests cover metric extraction (with and without watermark or state, NaN handling), batching
into requests of 50, dimension and namespace validation, failure and partial-failure accounting,
queue overflow, the background flush, and `close()` with a blocked post.

## Clean up

- Remove the listener: `spark.streams.removeListener(listener); listener.close()`.
- Metrics in a custom namespace expire under OCI Monitoring's retention; there is nothing to delete.
- When you no longer need them, delete the Credential Store entry and deactivate the API key.
