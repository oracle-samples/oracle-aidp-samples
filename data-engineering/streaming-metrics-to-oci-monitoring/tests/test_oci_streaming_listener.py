"""Offline tests for oci_streaming_listener.py - no Spark, no OCI. Run: python3 -m pytest -q tests"""
import pathlib
import sys
import time

import datetime as dt

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import oci_streaming_listener as osl  # noqa: E402

# shape of StreamingQueryProgress.json for a stateful query with a watermark (values from a live
# AIDP run: rate source 200 rows/s, 10 s windows, 30 s watermark)
PROGRESS = {
    "id": "11111111-2222-3333-4444-555555555555", "name": "metrics_probe", "batchId": 12,
    "timestamp": "2026-10-08T01:47:50.000Z", "numInputRows": 1000,
    "inputRowsPerSecond": 200.0, "processedRowsPerSecond": 2304.1,
    "durationMs": {"addBatch": 300, "triggerExecution": 434},
    "eventTime": {"watermark": "2026-10-08T01:47:19.380Z"},
    "stateOperators": [{"numRowsTotal": 30, "memoryUsedBytes": 9000}, {"numRowsTotal": 20, "memoryUsedBytes": 6792}],
}


def ms(iso):
    return int(dt.datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)


TS, WM = ms("2026-10-08T01:47:50.000Z"), ms("2026-10-08T01:47:19.380Z")


def test_progress_metrics_maps_every_streaming_gauge():
    m = osl.progress_metrics(PROGRESS)
    assert m["inputRowsPerSecond"] == 200.0
    assert m["processedRowsPerSecond"] == 2304.1
    assert m["triggerExecutionMs"] == 434.0 and m["batchDurationMs"] == 300.0
    assert m["stateRowsTotal"] == 50.0 and m["stateMemoryUsedBytes"] == 15792.0
    assert m["watermarkEpochMs"] == float(WM)
    assert m["watermarkLagMs"] == TS - WM == 30620


def test_no_watermark_no_state_no_nan():
    p = {"id": "q", "timestamp": "2026-10-08T01:47:50.000Z", "inputRowsPerSecond": float("nan"),
         "processedRowsPerSecond": 0.0, "durationMs": {}, "eventTime": {"watermark": "1970-01-01T00:00:00.000Z"},
         "stateOperators": []}
    m = osl.progress_metrics(p)
    assert m == {"processedRowsPerSecond": 0.0}


def test_attribute_style_progress_objects_work():
    class Obj:
        def __init__(self, **kw): self.__dict__.update(kw)
    p = Obj(**{**PROGRESS, "stateOperators": [Obj(numRowsTotal=5, memoryUsedBytes=7)]})
    m = osl.progress_metrics(p)
    assert m["stateRowsTotal"] == 5.0 and m["stateMemoryUsedBytes"] == 7.0


def make(post, **kw):
    return osl.OciStreamingMetricsListener(post, "ocid1.compartment.oc1..x", flush_seconds=3600, log=lambda s: None, **kw)


def test_enqueue_and_flush_batches_of_50_with_dimensions():
    calls = []
    lst = make(lambda items: calls.append(items) or 0, dimensions={"cluster": "c1"}, resource_group="rg")
    for _ in range(7):
        lst.enqueue(PROGRESS)                    # 9 metrics each -> 63 items -> 2 requests
    lst.flush()
    assert [len(c) for c in calls] == [50, 13]
    i = calls[0][0]
    assert i["namespace"] == "custom_spark_streaming" and i["resourceGroup"] == "rg"
    assert i["dimensions"] == {"queryName": "metrics_probe", "queryId": PROGRESS["id"], "cluster": "c1"}
    assert i["ts"] == TS
    stats = lst.close()
    assert stats["posted"] == 63 and stats["failed"] == 0 and stats["progress_events"] == 7


def test_post_failures_are_counted_never_raised():
    def boom(items):
        raise RuntimeError("401 NotAuthenticated")
    lst = make(boom)
    lst.enqueue(PROGRESS)
    lst.flush()
    s = lst.close()
    assert s["failed"] == 9 and "NotAuthenticated" in s["last_error"]


def test_partial_failures_from_the_api_are_counted():
    lst = make(lambda items: 2)
    lst.enqueue(PROGRESS)
    lst.flush()
    s = lst.close()
    assert s["posted"] == 7 and s["failed"] == 2


def test_queue_overflow_drops_instead_of_blocking():
    lst = make(lambda items: 0, queue_max=5)
    lst.enqueue(PROGRESS)
    s = lst.close()
    assert s["dropped"] == 4 and s["queued"] == 5


def test_on_query_progress_swallows_bad_events():
    lst = make(lambda items: 0)
    lst.onQueryProgress(object())               # no .progress attribute
    assert "enqueue" in lst.stats["last_error"]
    lst.close()


def test_background_worker_flushes_on_its_own():
    got = []
    lst = osl.OciStreamingMetricsListener(lambda items: got.extend(items) or 0, "c", flush_seconds=0.05,
                                          log=lambda s: None)
    lst.enqueue(PROGRESS)
    deadline = time.time() + 3
    while not got and time.time() < deadline:
        time.sleep(0.02)
    lst.close()
    assert len(got) == 9


@pytest.mark.parametrize("ns", ["oci_x", "oracle_x", "1abc", "custom-spark", "a.b", ""])
def test_reserved_or_invalid_namespace_rejected(ns):
    with pytest.raises(ValueError):
        make(lambda items: 0, namespace=ns)


def test_config_from_secrets_reads_each_key_and_rejects_gaps():
    class Secrets:
        def __init__(self, d): self.d = d
        def get(self, name, key): return self.d.get(key)
    full = {"user": " u ", "tenancy": "t", "fingerprint": "f", "region": "us-ashburn-1", "key_content": "PEM\n"}
    cfg = osl.oci_config_from_secrets(Secrets(full), "cred")
    assert cfg["user"] == "u" and cfg["key_content"] == "PEM\n"
    with pytest.raises(RuntimeError, match="fingerprint"):
        osl.oci_config_from_secrets(Secrets({**full, "fingerprint": ""}), "cred")


def test_dimension_limit_is_enforced():
    make(lambda items: 0, dimensions={f"d{i}": "v" for i in range(18)}).close()      # 18 + 2 = 20 ok
    with pytest.raises(ValueError, match="at most 18 extra"):
        make(lambda items: 0, dimensions={f"d{i}": "v" for i in range(19)})


def test_close_reports_a_blocked_worker_and_is_idempotent():
    import threading
    gate = threading.Event()
    lst = osl.OciStreamingMetricsListener(lambda items: gate.wait(5) and 0, "c", flush_seconds=3600,
                                          log=lambda s: None)
    lst.enqueue(PROGRESS)
    s = lst.close(timeout=0.2)                  # final flush is blocked in post()
    assert s["worker_alive"] and s["flush_incomplete"]
    gate.set()
    s2 = lst.close(timeout=5)
    assert not s2["worker_alive"] and not s2["flush_incomplete"] and s2["posted"] == 9


def test_stats_snapshot_is_a_copy():
    lst = make(lambda items: 0)
    snap = lst.stats
    snap["posted"] = 999
    assert lst.stats["posted"] == 0
    lst.close()


def test_builtin_dimension_names_cannot_be_overridden():
    with pytest.raises(ValueError, match="set by the listener"):
        make(lambda items: 0, dimensions={"queryName": "x"})


@pytest.mark.parametrize("key", ["a.b", "a b", "", "café", "x" * 257])
def test_invalid_dimension_keys_rejected(key):
    with pytest.raises(ValueError, match="invalid dimension key"):
        make(lambda items: 0, dimensions={key: "v"})
    make(lambda items: 0, dimensions={"ok_key-1": "a.b c"}).close()   # values may contain anything


def test_ingestion_endpoint_is_realm_aware():
    pytest.importorskip("oci")
    ep = osl.OciStreamingMetricsListener.ingestion_endpoint
    assert ep("us-ashburn-1") == "https://telemetry-ingestion.us-ashburn-1.oraclecloud.com"
    assert ep("uk-gov-london-1") == "https://telemetry-ingestion.uk-gov-london-1.oraclegovcloud.uk"


def test_from_config_uses_the_realm_endpoint(monkeypatch):
    oci = pytest.importorskip("oci")
    seen = {}

    class FakeClient:
        def __init__(self, config, **kw): seen.update(kw)
    monkeypatch.setattr(oci.config, "validate_config", lambda cfg: None)
    monkeypatch.setattr(oci.monitoring, "MonitoringClient", FakeClient)
    cfg = {"user": "u", "tenancy": "t", "fingerprint": "f", "region": "uk-gov-london-1", "key_content": "k"}
    lst = osl.OciStreamingMetricsListener.from_config(cfg, "c", flush_seconds=3600, log=lambda s: None)
    lst.close()
    assert seen["service_endpoint"] == "https://telemetry-ingestion.uk-gov-london-1.oraclegovcloud.uk"
