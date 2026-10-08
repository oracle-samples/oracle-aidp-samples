"""Push Spark Structured Streaming progress metrics to OCI Monitoring (custom namespace).

Why this exists: the cluster metrics AIDP publishes to OCI Monitoring (namespace
oracle_aidataplatform) cover tasks, jobs, shuffle, JVM and host resources, but not Structured
Streaming progress - input/processing rate, trigger latency, watermark, state size. This listener
reads those numbers from each StreamingQueryProgress and posts them to a custom OCI Monitoring
namespace you own, where they can be charted and alarmed on.

Copyright (c) 2026, Oracle and/or its affiliates.
Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/

Usage on an AIDP cluster (driver side only; Spark 3.4+ Python listeners):

    import sys; sys.path.insert(0, "/Workspace/<folder with this file>")
    from oci_streaming_listener import OciStreamingMetricsListener, oci_config_from_secrets
    cfg = oci_config_from_secrets(aidputils.secrets, "streaming_metrics_oci_api_key")
    listener = OciStreamingMetricsListener.from_config(
        cfg, compartment_id="<compartment ocid>", namespace="custom_spark_streaming",
        dimensions={"cluster": "my-cluster"})
    spark.streams.addListener(listener)
    ...
    listener.close()            # flushes; also call spark.streams.removeListener(listener)

Metric names (one datapoint per micro-batch, timestamp = the batch's trigger time):
    inputRowsPerSecond      <- Spark gauge inputRate-total
    processedRowsPerSecond  <- processingRate-total
    triggerExecutionMs      <- latency (durationMs.triggerExecution)
    watermarkEpochMs        <- eventTime-watermark (only when the query has a watermark)
    watermarkLagMs          trigger time minus watermark (how far event time trails)
    stateRowsTotal          <- states-rowsTotal (sum over stateful operators)
    stateMemoryUsedBytes    <- states-usedBytes
    numInputRows, batchDurationMs (durationMs.addBatch)
Dimensions: queryName, queryId, plus whatever is passed in `dimensions`.
"""
from __future__ import annotations

import datetime as _dt
import math
import queue
import re
import threading
import time
from typing import Callable, Dict, List, Optional

try:                                    # only needed on the driver, not for the offline tests
    from pyspark.sql.streaming import StreamingQueryListener as _Base
except Exception:                       # pragma: no cover - pyspark absent locally
    _Base = object

CREDENTIAL_KEYS = ("user", "tenancy", "fingerprint", "region", "key_content")
MAX_PER_REQUEST = 50                    # PostMetricData: up to 50 metric objects per call
MAX_DIMENSIONS = 20                     # PostMetricData: up to 20 dimensions per metric
_BUILTIN_DIMS = ("queryName", "queryId")
_NS_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")   # custom namespace: letter, then alphanumerics/_
_NS_RESERVED = ("oci_", "oracle_")
_DIM_KEY_RE = re.compile(r"^[!-~]{1,256}$")   # dimension key: printable ASCII, no spaces


def oci_config_from_secrets(secrets, credential_name: str, keys=CREDENTIAL_KEYS) -> dict:
    """Build an OCI SDK config dict from an AIDP Credential Store 'Secret Token' credential.

    `secrets` is `aidputils.secrets`; the credential holds one secret per key in `keys`
    (key_content = the PEM private key text). Values are never printed."""
    cfg = {}
    for k in keys:
        v = secrets.get(name=credential_name, key=k)
        if not v:
            raise RuntimeError(f"credential '{credential_name}' has no value for key '{k}'")
        cfg[k] = v.strip() if k != "key_content" else v
    return cfg


def _iso_to_ms(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        return int(_dt.datetime.fromisoformat(s).timestamp() * 1000)
    except ValueError:
        return None


def _get(obj, name, default=None):
    """Attribute or key access - works on StreamingQueryProgress and on plain dicts (tests, JSON)."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def progress_metrics(p) -> Dict[str, float]:
    """The metric values of one StreamingQueryProgress (or its dict form). Missing parts are skipped."""
    out: Dict[str, float] = {}
    for src, dst in (("inputRowsPerSecond", "inputRowsPerSecond"),
                     ("processedRowsPerSecond", "processedRowsPerSecond"),
                     ("numInputRows", "numInputRows")):
        v = _get(p, src)
        if v is not None and math.isfinite(float(v)):    # skip None, NaN and +/-inf
            out[dst] = float(v)
    dur = _get(p, "durationMs") or {}
    if _get(dur, "triggerExecution") is not None:
        out["triggerExecutionMs"] = float(_get(dur, "triggerExecution"))
    if _get(dur, "addBatch") is not None:
        out["batchDurationMs"] = float(_get(dur, "addBatch"))
    wm = _iso_to_ms(_get(_get(p, "eventTime") or {}, "watermark"))
    ts = _iso_to_ms(_get(p, "timestamp"))
    if wm is not None and wm > 0:                        # 1970 epoch = no watermark yet
        out["watermarkEpochMs"] = float(wm)
        if ts is not None:
            out["watermarkLagMs"] = float(ts - wm)
    ops = _get(p, "stateOperators") or []
    if ops:
        out["stateRowsTotal"] = float(sum(_get(o, "numRowsTotal", 0) or 0 for o in ops))
        out["stateMemoryUsedBytes"] = float(sum(_get(o, "memoryUsedBytes", 0) or 0 for o in ops))
    return out


def _dims(p, extra: Dict[str, str]) -> Dict[str, str]:
    d = {"queryName": str(_get(p, "name") or "unnamed"), "queryId": str(_get(p, "id") or "")}
    d.update({k: str(v) for k, v in (extra or {}).items() if v is not None})
    return {k: v[:256] for k, v in d.items() if v}       # keep values well inside OCI's length limit


class OciStreamingMetricsListener(_Base):
    """StreamingQueryListener that queues each progress and posts it from a background thread,
    so a slow or failing OCI call never delays the query. `post` is injected for tests."""

    def __init__(self, post: Callable[[List[dict]], int], compartment_id: str,
                 namespace: str = "custom_spark_streaming", resource_group: Optional[str] = None,
                 dimensions: Optional[Dict[str, str]] = None, flush_seconds: float = 10.0,
                 queue_max: int = 10000, log: Callable[[str], None] = print):
        if _Base is not object:
            super().__init__()
        if not _NS_RE.match(namespace) or namespace.startswith(_NS_RESERVED):
            raise ValueError("custom namespace must match ^[A-Za-z][A-Za-z0-9_]*$ and not start with "
                             "oci_ / oracle_")
        reserved = sorted(set(dimensions or {}) & set(_BUILTIN_DIMS))
        if reserved:
            raise ValueError(f"dimension name(s) {reserved} are set by the listener itself")
        extra = {k: v for k, v in (dimensions or {}).items() if v is not None}
        bad = [k for k in extra if not isinstance(k, str) or not _DIM_KEY_RE.match(k) or "." in k]
        if bad:                                 # OCI rejects the whole batch for one bad key
            raise ValueError(f"invalid dimension key(s) {bad}: printable ASCII, 1-256 chars, "
                             "no periods or spaces")
        if len(extra) + len(_BUILTIN_DIMS) > MAX_DIMENSIONS:
            raise ValueError(f"at most {MAX_DIMENSIONS - len(_BUILTIN_DIMS)} extra dimensions "
                             f"(OCI allows {MAX_DIMENSIONS} per metric, 2 are queryName/queryId)")
        self._post, self.compartment_id, self.namespace = post, compartment_id, namespace
        self.resource_group, self.dimensions, self._log = resource_group, extra, log
        self._q: "queue.Queue[dict]" = queue.Queue(maxsize=queue_max)
        self.flush_seconds = float(flush_seconds)
        self._lock = threading.Lock()          # callbacks and the worker run on different threads
        self._stats = {"progress_events": 0, "queued": 0, "posted": 0, "failed": 0, "dropped": 0,
                       "last_error": None}
        self._stop = threading.Event()
        self._worker = threading.Thread(target=self._run, name="oci-streaming-metrics", daemon=True)
        self._worker.start()

    @property
    def stats(self) -> dict:
        """A consistent snapshot of the counters."""
        with self._lock:
            return dict(self._stats)

    def _count(self, **kw) -> None:
        with self._lock:
            for k, v in kw.items():
                if k == "last_error":
                    self._stats[k] = v
                else:
                    self._stats[k] += v

    # ------------------------------------------------------------ construction from an OCI config
    @staticmethod
    def ingestion_endpoint(region: str) -> str:
        """PostMetricData lives on the telemetry-ingestion host, not the default monitoring endpoint.
        The SDK fills in the realm's second-level domain (oraclecloud.com, oraclegovcloud.uk, ...)."""
        import oci
        return oci.regions.endpoint_for(
            "monitoring", region=region,
            service_endpoint_template="https://telemetry-ingestion.{region}.{secondLevelDomain}")

    @classmethod
    def from_config(cls, config: dict, compartment_id: str, **kw) -> "OciStreamingMetricsListener":
        import oci
        oci.config.validate_config(config)
        client = oci.monitoring.MonitoringClient(
            config, service_endpoint=cls.ingestion_endpoint(config["region"]))

        def post(items: List[dict]) -> int:
            details = oci.monitoring.models.PostMetricDataDetails(metric_data=[
                oci.monitoring.models.MetricDataDetails(
                    namespace=i["namespace"], compartment_id=i["compartmentId"], name=i["name"],
                    resource_group=i.get("resourceGroup"), dimensions=i["dimensions"],
                    datapoints=[oci.monitoring.models.Datapoint(
                        timestamp=_dt.datetime.fromtimestamp(i["ts"] / 1000, _dt.timezone.utc),
                        value=i["value"])])
                for i in items])
            r = client.post_metric_data(details)
            return int(r.data.failed_metrics_count or 0)
        return cls(post, compartment_id, **kw)

    # ------------------------------------------------------------ listener callbacks (driver thread)
    def onQueryStarted(self, event):
        self._log(f"[oci-metrics] query started: {getattr(event, 'name', None)} {getattr(event, 'id', '')}")

    def onQueryProgress(self, event):
        try:
            self.enqueue(event.progress)
        except Exception as e:                            # never let metrics break the query
            self._count(last_error=f"enqueue: {e}")

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        self._log(f"[oci-metrics] query terminated: {getattr(event, 'id', '')}")

    # ------------------------------------------------------------ queue and worker
    def enqueue(self, progress) -> int:
        self._count(progress_events=1)
        ts = _iso_to_ms(_get(progress, "timestamp")) or int(time.time() * 1000)
        dims = _dims(progress, self.dimensions)
        n = dropped = 0
        for name, value in progress_metrics(progress).items():
            item = {"namespace": self.namespace, "compartmentId": self.compartment_id, "name": name,
                    "dimensions": dims, "ts": ts, "value": value}
            if self.resource_group:
                item["resourceGroup"] = self.resource_group
            try:
                self._q.put_nowait(item); n += 1
            except queue.Full:
                dropped += 1
        self._count(queued=n, dropped=dropped)
        return n

    def _drain(self) -> List[dict]:
        items = []
        while len(items) < MAX_PER_REQUEST:
            try:
                items.append(self._q.get_nowait())
            except queue.Empty:
                break
        return items

    def flush(self) -> None:
        while True:
            items = self._drain()
            if not items:
                return
            try:
                failed = int(self._post(items) or 0)        # failed items are counted, never re-queued
                self._count(posted=len(items) - failed, failed=failed)
            except Exception as e:
                self._count(failed=len(items), last_error=f"{type(e).__name__}: {str(e)[:300]}")

    def _run(self) -> None:
        while not self._stop.wait(self.flush_seconds):
            self.flush()
        self.flush()

    def close(self, timeout: Optional[float] = 90.0) -> dict:
        """Stop the worker after a final flush and return the stats. The default timeout covers one
        blocked OCI call (the SDK read timeout is 60 s); if the worker is still running when it expires,
        the result says so (worker_alive / flush_incomplete) instead of claiming a clean flush.
        Calling close() again is harmless."""
        self._stop.set()
        self._worker.join(timeout)
        out = self.stats
        out["worker_alive"] = self._worker.is_alive()
        out["flush_incomplete"] = out["worker_alive"] or not self._q.empty()
        return out
