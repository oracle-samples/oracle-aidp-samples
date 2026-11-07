"""Run a Spark Structured Streaming query from an AIDP notebook cell, and stop it cleanly.

Two AIDP behaviours shape this helper (both observed on AIDP Workbench, Spark 3.5.0):

1. While a streaming query started from a notebook is running, the other cells of that same
   notebook cannot run: they wait about 95 seconds and then fail with
   "Command execution failed on compute cluster." Other notebooks cannot see the query either,
   because each notebook has its own Spark session.
2. Closing or deleting the notebook session does not stop the query. It keeps running on the
   cluster until something calls ``query.stop()``.

So each notebook in this sample starts its query in one cell and keeps that cell busy with
``run_until_stopped()``. The cell returns, and the query is always stopped, when one of these
happens:

- another notebook calls ``request_stop()``, which writes a small "stop file" on a Volume;
- the time limit passes;
- the query fails (the error is re-raised so that the cell shows it).

``list_cluster_streams()`` and ``stop_cluster_stream()`` are a last resort for a query whose
notebook is gone. They rely on Spark's internal thread naming, so prefer the stop file.
"""

from __future__ import annotations

import json
import os
import re
import time
from typing import Callable, List, Optional

# Spark names the thread that drives each query:
#   "stream execution thread for <query name> [id = <query id>, runId = <run id>]"
_STREAM_THREAD = re.compile(
    r"^stream execution thread for (?P<name>.*?)\s*\[id = (?P<id>[0-9a-f-]+), runId = (?P<run_id>[0-9a-f-]+)\]$"
)


def stop_file_path(control_dir: str, name: str) -> str:
    """Path of the stop file for the query called ``name``."""
    if not re.fullmatch(r"[A-Za-z0-9_-]+", name):
        raise ValueError(f"name must contain only letters, digits, '_' or '-': {name!r}")
    return os.path.join(control_dir, f"stop_{name}")


def request_stop(control_dir: str, name: str) -> str:
    """Ask the query ``name`` to stop. Call this from any notebook attached to the cluster."""
    path = stop_file_path(control_dir, name)
    os.makedirs(control_dir, exist_ok=True)
    with open(path, "w") as f:
        f.write(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    return path


def clear_stop(control_dir: str, name: str) -> None:
    """Remove a stop file left over from an earlier run."""
    path = stop_file_path(control_dir, name)
    if os.path.exists(path):
        os.remove(path)


def run_until_stopped(
    query,
    control_dir: str,
    name: str,
    max_minutes: float,
    poll_seconds: float = 10,
    report_every_seconds: float = 60,
    printer: Callable[[str], None] = print,
) -> str:
    """Block until a stop file appears, the time limit passes, or the query fails.

    The query is stopped in every case. Returns the reason the cell finished:
    ``"stop file"``, ``"time limit"`` or ``"query terminated"``. If the query failed, its
    ``StreamingQueryException`` propagates after the query has been stopped.

    Call ``clear_stop()`` *before* starting the query: a stop file that exists while this runs
    -- an old one, or a request made since the query started -- stops the query.

    On a stop file or the time limit, it first waits -- up to two minutes -- for the running
    micro-batch to finish. Stopping a query in the middle of a batch cancels that batch's Spark
    job, and AIDP then reports the cell as failed ("Streaming query failed: JobFailed ...
    cancelled job group") although nothing is lost: the batch runs again on restart. A trigger
    that starts between the check and ``stop()`` can still be cancelled; in our runs none was.

    """
    reason = "time limit"
    try:
        marker = stop_file_path(control_dir, name)
        deadline = time.monotonic() + max_minutes * 60
        next_report = time.monotonic() + report_every_seconds
        while True:
            # Returns True once the query has terminated; raises if it terminated with an error.
            if query.awaitTermination(poll_seconds):
                reason = "query terminated"
                break
            if os.path.exists(marker):
                reason = "stop file"
                break
            if time.monotonic() >= deadline:
                break
            if time.monotonic() >= next_report:
                printer(progress_line(query))
                next_report = time.monotonic() + report_every_seconds
        if not wait_for_idle(query):
            printer(f"{name}: a micro-batch is still running after two minutes; stopping anyway "
                    "(it runs again when the query restarts)")
    finally:
        query.stop()
    printer(f"{name}: stopped ({reason}). {progress_line(query)}")
    return reason


def wait_for_idle(query, timeout_seconds: float = 120, poll_seconds: float = 0.5) -> bool:
    """Wait until the query is between micro-batches; True if it is, False on timeout."""
    deadline = time.monotonic() + timeout_seconds
    while query.isActive and query.status.get("isTriggerActive"):
        if time.monotonic() >= deadline:
            return False
        time.sleep(poll_seconds)
    return True


def progress_line(query) -> str:
    """One line summarising the query's last micro-batch."""
    p = query.lastProgress
    if not p:
        return "no micro-batch completed yet"
    return (f"batch {p.get('batchId')}: {p.get('numInputRows')} rows, "
            f"{p.get('durationMs', {}).get('triggerExecution')} ms, at {p.get('timestamp')}")


def committed_source_offset(checkpoint_dir: str, source_index: int = 0) -> Optional[dict]:
    """Source offset of the query's last completed micro-batch, read from its checkpoint.

    Spark writes ``offsets/<batch>`` when it plans a batch and ``commits/<batch>`` when the batch
    completes. A restarted query continues from the offset of the last committed batch. For an
    Iceberg source the offset looks like ``{"version": 1, "snapshot_id": 123, "position": 4,
    "scan_all_files": false}``: the query still needs that snapshot and every later one.
    Returns None if no batch has completed yet.
    """
    commits_dir = os.path.join(checkpoint_dir, "commits")
    if not os.path.isdir(commits_dir):
        return None
    committed = sorted(int(f) for f in os.listdir(commits_dir) if f.isdigit())
    if not committed:
        return None
    with open(os.path.join(checkpoint_dir, "offsets", str(committed[-1]))) as f:
        lines = [line for line in f.read().splitlines() if line.strip()]
    # Line 1 is the log version ("v1"), line 2 the batch metadata, then one line per source.
    sources = lines[2:]
    if source_index >= len(sources):
        raise ValueError(f"the checkpoint has {len(sources)} source(s), no index {source_index}")
    return json.loads(sources[source_index])


def list_cluster_streams(spark) -> List[dict]:
    """Every streaming query running on this cluster, from any notebook. Read only."""
    found = []
    for thread in spark._jvm.java.lang.Thread.getAllStackTraces().keySet().toArray():
        m = _STREAM_THREAD.match(thread.getName())
        if m:
            found.append({"name": m.group("name") or None, "id": m.group("id"), "run_id": m.group("run_id")})
    return found


def stop_cluster_stream(spark, run_id: str) -> bool:
    """Last resort: stop a query whose notebook is gone, by interrupting its driver thread.

    ``query.stop()`` also interrupts this thread, but it marks the query as stopped first. Here
    nothing does, so Spark ends the query with an interruption error. The checkpoint stays
    usable: completed micro-batches stay committed, and a batch that was in flight runs again
    when the query is restarted from the same checkpoint. The cluster is shared, so this can
    stop anyone's query: pass the ``run_id`` of your own query only (see
    ``list_cluster_streams``). Returns True if a matching query was found.
    """
    if not re.fullmatch(r"[0-9a-f-]{36}", run_id):
        raise ValueError(f"not a run id: {run_id!r}")
    for thread in spark._jvm.java.lang.Thread.getAllStackTraces().keySet().toArray():
        m = _STREAM_THREAD.match(thread.getName())
        if m and m.group("run_id") == run_id:
            thread.interrupt()
            return True
    return False


def parse_stream_thread_name(thread_name: str) -> Optional[dict]:
    """Parse a stream execution thread name; None if it is not one. Exposed for tests."""
    m = _STREAM_THREAD.match(thread_name)
    if not m:
        return None
    return {"name": m.group("name") or None, "id": m.group("id"), "run_id": m.group("run_id")}
