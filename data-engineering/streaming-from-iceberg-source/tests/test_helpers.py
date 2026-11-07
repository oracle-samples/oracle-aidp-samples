"""Offline tests for adw_sink.py and stream_control.py - no Spark, no database.

Run from the sample folder:  python3 -m pytest -q tests
"""
import pathlib
import sys
import types

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import adw_sink  # noqa: E402
import stream_control  # noqa: E402

# Shape of an ADW wallet's tnsnames.ora (host and service names are made up).
TNSNAMES = """mydb_high = (description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.us-ashburn-1.oraclecloud.com))(connect_data=(service_name=abc_mydb_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))

mydb_low = (description= (retry_count=20)(retry_delay=3)
    (address=(protocol=tcps)(port=1522)(host=adb.us-ashburn-1.oraclecloud.com))
    (connect_data=(service_name=abc_mydb_low.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))
"""


# ---- adw_sink ---------------------------------------------------------------------------------

def test_identifier_accepts_simple_names_and_upper_cases_them():
    assert adw_sink.identifier("orders_stg") == "ORDERS_STG"


@pytest.mark.parametrize("bad", ["", "1orders", "orders; drop table x", "a-b", "x" * 129, "\"quoted\""])
def test_identifier_rejects_anything_else(bad):
    with pytest.raises(ValueError):
        adw_sink.identifier(bad)


def test_tns_descriptor_single_and_multi_line_entries():
    high = adw_sink.tns_descriptor(TNSNAMES, "mydb_high")
    low = adw_sink.tns_descriptor(TNSNAMES, "MYDB_LOW")          # alias lookup ignores case
    assert high.startswith("(description=") and high.endswith("(ssl_server_dn_match=yes)))")
    assert "abc_mydb_low" in low and " " not in low and "\n" not in low
    assert low.count("(") == low.count(")")


def test_tns_descriptor_unknown_alias():
    with pytest.raises(ValueError, match="not found"):
        adw_sink.tns_descriptor(TNSNAMES, "mydb_tp")


def test_hadoop_catalog_coordinates():
    warehouse, path = adw_sink.hadoop_catalog_coordinates(
        "oci://my-bucket@mynamespace/iceberg-streaming/warehouse/iceberg_streaming_demo/order_events/", "us-ashburn-1")
    assert warehouse == ("https://objectstorage.us-ashburn-1.oraclecloud.com/n/mynamespace/b/my-bucket/o/"
                         "iceberg-streaming/warehouse")
    assert path == "iceberg_streaming_demo.order_events"


@pytest.mark.parametrize("bad", ["s3://b/x/y/z", "oci://bucket@ns/db/table", "oci://bucket@ns/w/my.db/t"])
def test_hadoop_catalog_coordinates_rejects(bad):
    with pytest.raises(ValueError):
        adw_sink.hadoop_catalog_coordinates(bad, "us-ashburn-1")


class _FakeSparkContext:
    def __init__(self):
        self.py_files = []

    def addPyFile(self, path):
        self.py_files.append(path)


def _sink():
    spark = types.SimpleNamespace(sparkContext=_FakeSparkContext())
    adw = adw_sink.AdwConnection("U", "P", "mydb_low", "/tmp/wallet", "W")
    return adw_sink.AdwSink(spark, adw, query_name="q", target="orders", staging="orders_stg", batch_log="log")


def test_sink_ships_its_module_to_executors():
    sink = _sink()
    assert len(sink.spark.sparkContext.py_files) == 1
    assert sink.spark.sparkContext.py_files[0].endswith("adw_sink.py")


def test_merge_sql_guards():
    sink = _sink()
    stream, resync = sink.stream_merge_sql, sink.resync_merge_sql
    for sql in (stream, resync):
        assert "MERGE INTO ORDERS t" in sql and "FROM ORDERS_STG s" in sql
        assert "PARTITION BY s.order_id ORDER BY s.event_id DESC" in sql   # one source row per key
        # A delete leaves a tombstone, also for an order ADW has not seen, so an older event
        # can never bring the order back.
        assert "DELETE WHERE" not in sql and "WHERE s.op <> 'D'" not in sql
        assert sql.count("CASE WHEN s.op = 'D' THEN 'Y' ELSE 'N' END") == 2
    # The stream never lets an older event overwrite a newer one ...
    assert "WHERE s.event_id > t.last_event_id" in stream
    # ... while a resync overwrites unless ADW holds an event newer than the order's watermark.
    assert "WHERE t.last_event_id <= s.watermark_id" in resync
    assert sink.insert_sql.startswith("INSERT INTO ORDERS_STG (query_id, batch_id, event_id, order_id")
    for sql in (stream, resync):                      # a query only ever sees its own staged rows
        assert "WHERE s.query_id = :query_id AND s.batch_id = :batch_id" in sql
    assert sink.insert_sql.count(":") == len(adw_sink.STAGING_COLUMNS)
    assert adw_sink.STAGING_COLUMNS[-1] == "watermark_id"


def test_insert_partition_batches_rows_and_commits(monkeypatch, tmp_path):
    calls = {"executemany": [], "commit": 0, "closed": False, "connect": None}

    class Cursor:
        def executemany(self, sql, rows):
            calls["executemany"].append(len(rows))

    class Conn:
        def cursor(self):
            return Cursor()

        def commit(self):
            calls["commit"] += 1

        def close(self):
            calls["closed"] = True

    def connect(**kwargs):
        calls["connect"] = kwargs
        pem = pathlib.Path(kwargs["wallet_location"]) / "ewallet.pem"   # what the driver sees at connect time
        calls["pem"] = (pem.read_bytes(), pem.stat().st_mode & 0o077)
        return Conn()

    monkeypatch.setitem(sys.modules, "oracledb", types.SimpleNamespace(connect=connect))
    monkeypatch.setattr(adw_sink.tempfile, "tempdir", str(tmp_path))
    settings = {"user": "U", "password": "P", "wallet_password": "W", "descriptor": "(description=x)",
                "wallet_pem": "LS0tLS1CRUdJTi0tLS0t"}
    counts = list(adw_sink.insert_partition(iter([(i,) for i in range(5)]), settings, "INSERT ...", array_size=2))
    assert counts == [5]
    assert calls["executemany"] == [2, 2, 1] and calls["commit"] == 1 and calls["closed"]
    assert calls["pem"] == (b"-----BEGIN-----", 0)                  # owner-only while it exists ...
    assert not pathlib.Path(calls["connect"]["wallet_location"]).exists()   # ... and gone after connect
    assert list(tmp_path.iterdir()) == []


def test_checkpoint_query_id(tmp_path):
    (tmp_path / "metadata").write_text('{"id":"f4d24539-8726-4fd8-befb-b7880876a91f"}')
    assert adw_sink.checkpoint_query_id(str(tmp_path)) == "f4d24539-8726-4fd8-befb-b7880876a91f"


def test_upsert_batch_requires_the_checkpoint():
    with pytest.raises(ValueError, match="checkpoint_location"):
        _sink()._stream_query_id()


class _FakeCursor:
    def __init__(self, count):
        self.count, self.executed = count, []

    def execute(self, sql, **binds):
        self.executed.append((sql, binds))

    def fetchone(self):
        return (self.count,)


class _FakeAdw:
    def __init__(self, count):
        self.cursor_ = _FakeCursor(count)

    def connect(self):
        adw = self

        class Conn:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def cursor(self):
                return adw.cursor_
        return Conn()


def _sink_on(tmp_path, other_checkpoint_batches, allow=False):
    (tmp_path / "metadata").write_text('{"id":"11111111-2222-3333-4444-555555555555"}')
    spark = types.SimpleNamespace(sparkContext=_FakeSparkContext())
    return adw_sink.AdwSink(spark, _FakeAdw(other_checkpoint_batches), query_name="q",
                            checkpoint_location=str(tmp_path), allow_new_checkpoint=allow)


def test_new_checkpoint_over_existing_state_is_refused(tmp_path):
    with pytest.raises(RuntimeError, match="another checkpoint"):
        _sink_on(tmp_path, other_checkpoint_batches=5)._stream_query_id()


def test_new_checkpoint_allowed_explicitly_or_on_empty_state(tmp_path):
    assert _sink_on(tmp_path, 5, allow=True)._stream_query_id() == "11111111-2222-3333-4444-555555555555"
    assert _sink_on(tmp_path, 0)._stream_query_id() == "11111111-2222-3333-4444-555555555555"


def test_passwords_are_not_stripped():
    secrets = types.SimpleNamespace(get=lambda name, key: {"user": " U ", "password": " p w ",
                                                           "wallet_password": "w ", "tns": " db_low\n"}[key])
    adw = adw_sink.AdwConnection.from_credential_store(secrets, "c", "/w")
    assert (adw.user, adw.tns_alias, adw.password, adw.wallet_password) == ("U", "db_low", " p w ", "w ")


def test_connection_repr_hides_secrets():
    text = repr(adw_sink.AdwConnection("U", "s3cret", "mydb_low", "/w", "walletpw"))
    assert "s3cret" not in text and "walletpw" not in text and "mydb_low" in text


# ---- stream_control ----------------------------------------------------------------------------

class FakeQuery:
    """Stands in for a StreamingQuery: terminates after `finish_after` polls, or fails."""

    def __init__(self, finish_after=None, error=None, busy_polls=0):
        self.polls, self.finish_after, self.error, self.stopped = 0, finish_after, error, False
        self.busy_polls = busy_polls          # how many status checks report a batch in flight
        self.status_checks = 0
        self.isActive = True
        self.lastProgress = {"batchId": 3, "numInputRows": 10, "durationMs": {"triggerExecution": 5},
                             "timestamp": "2026-11-07T00:00:00Z"}

    def awaitTermination(self, timeout):
        self.polls += 1
        if self.error is not None and self.polls >= 2:
            raise self.error
        return self.finish_after is not None and self.polls >= self.finish_after

    @property
    def status(self):
        self.status_checks += 1
        return {"isTriggerActive": self.status_checks <= self.busy_polls}

    def stop(self):
        self.stopped = True
        self.isActive = False


def test_stop_file_ends_the_run(tmp_path):
    q = FakeQuery()
    original = q.awaitTermination

    def poll(timeout):
        if q.polls == 2:
            stream_control.request_stop(str(tmp_path), "producer")
        return original(timeout)

    q.awaitTermination = poll
    reason = stream_control.run_until_stopped(q, str(tmp_path), "producer", max_minutes=5, poll_seconds=0,
                                              printer=lambda _: None)
    assert reason == "stop file" and q.stopped


def test_an_existing_stop_file_is_honoured(tmp_path):
    # A stop requested between start() and run_until_stopped() must not be lost.
    stream_control.request_stop(str(tmp_path), "consumer")
    q = FakeQuery(finish_after=3)
    reason = stream_control.run_until_stopped(q, str(tmp_path), "consumer", max_minutes=5, poll_seconds=0,
                                              printer=lambda _: None)
    assert reason == "stop file" and q.polls == 1 and q.stopped


def test_clear_stop_before_start(tmp_path):
    stream_control.request_stop(str(tmp_path), "consumer")
    stream_control.clear_stop(str(tmp_path), "consumer")
    q = FakeQuery(finish_after=3)
    reason = stream_control.run_until_stopped(q, str(tmp_path), "consumer", max_minutes=5, poll_seconds=0,
                                              printer=lambda _: None)
    assert reason == "query terminated" and q.polls == 3 and q.stopped


def test_bad_name_still_stops_the_query(tmp_path):
    q = FakeQuery()
    with pytest.raises(ValueError):
        stream_control.run_until_stopped(q, str(tmp_path), "../x", max_minutes=5, poll_seconds=0,
                                         printer=lambda _: None)
    assert q.stopped


def test_time_limit(tmp_path):
    q = FakeQuery()
    reason = stream_control.run_until_stopped(q, str(tmp_path), "p", max_minutes=0, poll_seconds=0,
                                              printer=lambda _: None)
    assert reason == "time limit" and q.stopped


def test_query_error_propagates_after_stop(tmp_path):
    q = FakeQuery(error=RuntimeError("Cannot process overwrite snapshot"))
    with pytest.raises(RuntimeError, match="overwrite"):
        stream_control.run_until_stopped(q, str(tmp_path), "p", max_minutes=5, poll_seconds=0,
                                         printer=lambda _: None)
    assert q.stopped


def test_stop_waits_for_the_running_micro_batch(tmp_path, monkeypatch):
    monkeypatch.setattr(stream_control.time, "sleep", lambda _: None)
    q = FakeQuery(busy_polls=3)
    reason = stream_control.run_until_stopped(q, str(tmp_path), "p", max_minutes=0, poll_seconds=0,
                                              printer=lambda _: None)
    assert reason == "time limit" and q.stopped
    assert q.status_checks == 4                 # three "batch running" answers, then idle


def test_wait_for_idle_times_out(monkeypatch):
    monkeypatch.setattr(stream_control.time, "sleep", lambda _: None)
    q = FakeQuery(busy_polls=10**9)
    assert stream_control.wait_for_idle(q, timeout_seconds=0) is False


def test_stop_file_name_is_validated(tmp_path):
    with pytest.raises(ValueError):
        stream_control.stop_file_path(str(tmp_path), "../evil")


def test_parse_stream_thread_name():
    named = stream_control.parse_stream_thread_name(
        "stream execution thread for iceberg_producer [id = 21f352c6-ebb2-43d1-a23a-c3b02c3210da, "
        "runId = ea4ddb5e-d418-4c3e-b0e7-1a38f8f62e1c]")
    unnamed = stream_control.parse_stream_thread_name(
        "stream execution thread for [id = 7156c947-9da8-461e-8034-36f365264124, "
        "runId = 5e1beb6d-3e5f-4831-bdc4-bb9c1dd67268]")
    assert named == {"name": "iceberg_producer", "id": "21f352c6-ebb2-43d1-a23a-c3b02c3210da",
                     "run_id": "ea4ddb5e-d418-4c3e-b0e7-1a38f8f62e1c"}
    assert unnamed["name"] is None and unnamed["run_id"].startswith("5e1beb6d")
    assert stream_control.parse_stream_thread_name("Executor task launch worker for task 1") is None


def test_committed_source_offset_reads_last_committed_batch(tmp_path):
    (tmp_path / "commits").mkdir()
    (tmp_path / "offsets").mkdir()
    meta = '{"batchWatermarkMs":0,"batchTimestampMs":1794050000000,"conf":{}}'
    for batch, snap in ((0, 111), (1, 222), (2, 333)):
        (tmp_path / "offsets" / str(batch)).write_text(
            f'v1\n{meta}\n{{"version":1,"snapshot_id":{snap},"position":4,"scan_all_files":false}}')
    for batch in (0, 1):                     # batch 2 was planned but has not completed
        (tmp_path / "commits" / str(batch)).write_text('v1\n{"nextBatchWatermarkMs":0}')
    (tmp_path / "commits" / ".1.crc").write_text("")
    offset = stream_control.committed_source_offset(str(tmp_path))
    assert offset == {"version": 1, "snapshot_id": 222, "position": 4, "scan_all_files": False}


def test_committed_source_offset_before_first_batch(tmp_path):
    assert stream_control.committed_source_offset(str(tmp_path)) is None
    (tmp_path / "commits").mkdir()
    assert stream_control.committed_source_offset(str(tmp_path)) is None
