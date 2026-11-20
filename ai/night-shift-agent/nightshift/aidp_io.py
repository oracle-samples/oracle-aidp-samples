"""Thin, dry-run-aware wrapper over ``aidp_agent.AIDPClient``.

Everything that mutates the platform honours ``self.dry_run`` (default True): the intended call is
logged and a ``{"dry_run": True, ...}`` dict is returned without touching the control plane.

``aidp_agent`` is imported lazily so unit tests can inject a fake client and never need the package.

Expected client: a class importable as ``aidp_agent.AIDPClient``, constructed with
``region, data_lake_id, workspace_id, auth_type, config_profile`` and exposing these methods
(the laptop scripts and this module use nothing else):
  list_jobs(**params) / get_job(key) / update_job(key, **body)          -> workspaces/{ws}/jobs
  list_job_runs(**params)                                               -> workspaces/{ws}/jobRuns
  list_recent_job_runs(**params)                                        -> workspaces/{ws}/recentJobRuns (fallback)
  list_clusters() / get_cluster(key) / update_cluster(key, **body)      -> workspaces/{ws}/clusters
  start_cluster(key) / stop_cluster(key)                                -> .../clusters/{key}/actions/{start|stop}
  summarize_metrics(key, **body)                                        -> .../clusters/{key}/actions/summarizeMetricsData
  nb_create_session(path, name, cluster_id) -> {"id", "kernel": {"id"}}
  nb_execute_code(kernel_id, session_id, code, timeout=) -> {"status", "outputs": [...], "error"?}
  nb_delete_session(session_id)
  nb_save_file(path, text) / nb_get_contents(path) -> {"content": str}   (workspace files; scripts/deploy_workspace.py)
  nb_save_notebook(path, nb_dict)                                        (scripts/stage_approvals_notebook.py)
  client._base._ws_path(workspace_id)                                   -> "workspaces/{ws}" path prefix
  client._base._request_checked(method, path, body=, params=)           -> generic signed request
"""
from __future__ import annotations

import json
import logging
from typing import Any

from .config import METRIC_AGGREGATIONS, METRIC_INTERVALS, METRIC_NAMES, Env

logger = logging.getLogger(__name__)

_FAILED_STATES = {"FAILED", "ERROR", "TIMED_OUT", "TIMEOUT", "CANCELED", "CANCELLED", "INTERNAL_ERROR"}
# A run is finished only in a state known to be terminal; everything else counts as active.
DONE_STATES = _FAILED_STATES | {"SUCCEEDED", "SUCCESS", "SUCCESSFUL", "COMPLETED", "DONE", "FINISHED", "SKIPPED", "TERMINATED",
                                "UPSTREAM_FAILED", "UPSTREAM_CANCELED", "KILLED", "ABORTED", "STOPPED"}
# The active values the job-runs list accepts as its ``status`` filter, and the largest page it returns.
ACTIVE_RUN_STATUSES = ("RUNNING", "PENDING", "QUEUED", "BLOCKED")
RUN_PAGE = 100
_SQL_MARK = "__NS_ROWS__"


class AidpIO:
    """Jobs, clusters, metrics and notebook-kernel SQL for the Night Shift actuator."""

    def __init__(self, env: Env, dry_run: bool = True, client: Any | None = None) -> None:
        self.env = env
        self.dry_run = dry_run
        self._client = client
        self._session_id: str | None = None
        self._kernel_id: str | None = None

    # ------------------------------------------------------------------ client
    @property
    def client(self) -> Any:
        if self._client is None:
            from aidp_agent import AIDPClient  # lazy: only the live path needs it

            self.env.require_live()
            self._client = AIDPClient(
                region=self.env.region,
                data_lake_id=self.env.data_lake_id,
                workspace_id=self.env.workspace_id,
                auth_type="api_key",
                config_profile=self.env.config_profile,
            )
        return self._client

    def request(self, method: str, path: str, body: dict | None = None, params: dict | None = None) -> Any:
        """Generic signed request for paths the client lacks. ``path`` is relative to the workspace
        (e.g. ``clusters/{key}/actions/summarizeMetricsData``) unless it starts with ``/``."""
        base = self.client._base
        full = path if path.startswith("/") else f"{base._ws_path(self.env.workspace_id)}/{path}"
        if method.upper() not in ("GET", "HEAD") and self.dry_run:
            return self._dry("request", method=method, path=full, body=body)
        resp = base._request_checked(method, full, body=body, params=params)
        return resp.json() if resp.text else {}

    def _dry(self, op: str, **kw: Any) -> dict:
        logger.info("[dry-run] %s %s", op, json.dumps(kw, default=str))
        return {"dry_run": True, "op": op, **kw}

    @staticmethod
    def _items(payload: Any) -> list:
        if isinstance(payload, dict):
            return list(payload.get("items") or payload.get("data") or [])
        return list(payload or [])

    # -------------------------------------------------------------------- jobs
    def list_jobs(self) -> list[dict]:
        return self._items(self.client.list_jobs())

    def get_job(self, key: str) -> dict:
        return self.client.get_job(key)

    def update_job(self, key: str, body: dict) -> dict:
        """PUT the full job body (the API replaces, so callers re-send name/path/tasks/jobClusters)."""
        if self.dry_run:
            return self._dry("update_job", key=key, body=body)
        return self.client.update_job(key, **body)

    @staticmethod
    def _run_items(payload: Any) -> list:
        """The run rows of a job-runs response, or an error. Unlike ``_items`` this never turns a payload it does not
        recognise (an error body, ``None``, a list under an unknown key) into "no runs": the callers decide from this
        whether a cluster is busy."""
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for key in ("items", "data"):
                if isinstance(payload.get(key), list):
                    return payload[key]
        shape = sorted(payload) if isinstance(payload, dict) else type(payload).__name__
        raise RuntimeError(f"job runs response not recognised ({shape}); runs unknown")

    def list_recent_runs(self, hours: int = 24) -> list[dict]:
        """Job runs from the newest page of the run history (``RUN_PAGE`` rows), filtered to the window; a run that
        is not in a finished state is kept whatever its start time, so a long run never ages out of the list.

        ``list_job_runs`` is the primary source; ``list_recent_job_runs`` is only tried as a fallback. Run rows carry
        ``startTime`` as epoch milliseconds and ``state.status`` / ``state.stateMessage``. One page does not reach back
        a fixed time on a busy workspace; ``list_active_runs`` is what answers "is anything running"."""
        from datetime import datetime, timedelta, timezone

        try:
            runs = self._run_items(self.client.list_job_runs(limit=RUN_PAGE))
        except Exception:  # noqa: BLE001
            runs = self._run_items(self.client.list_recent_job_runs())
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        kept: list[dict] = []
        for r in runs:
            ts = _parse_ts(r.get("startTime") or r.get("timeStarted") or r.get("timeCreated") or r.get("endTime"))
            if ts is None or ts >= cutoff or self.run_status(r) not in DONE_STATES:
                kept.append(r)
        return kept

    def list_active_runs(self) -> list[dict]:
        """Every job run in an active state, asked for by status so the answer does not depend on how far back one
        page of the run history reaches. Raises when a query fails, when a response is not a run list, or when a status
        returns a full page (there may be more than the page holds, and the ones cut off could be the runs on the
        cluster in question); the tick reads any of these as unknown, never as idle."""
        out: list[dict] = []
        for status in ACTIVE_RUN_STATUSES:
            page = self._run_items(self.client.list_job_runs(status=status, limit=RUN_PAGE))
            if len(page) >= RUN_PAGE:
                raise RuntimeError(f"{len(page)} job runs in state {status} fill a whole page; the list may be cut off, active runs unknown")
            out.extend(page)
        return out

    @staticmethod
    def run_status(r: dict) -> str:
        st = r.get("state")
        if isinstance(st, dict):
            return str(st.get("status") or st.get("lifecycleState") or "").upper()
        return str(st or r.get("lifecycleState") or r.get("status") or "").upper()

    @staticmethod
    def run_message(r: dict) -> str:
        st = r.get("state")
        if isinstance(st, dict):
            return str(st.get("stateMessage") or st.get("message") or "")
        return str(r.get("stateMessage") or r.get("message") or "")

    def recent_failures(self, hours: int = 24) -> list[dict]:
        return [r for r in self.list_recent_runs(hours) if self.run_status(r) in _FAILED_STATES]

    # ---------------------------------------------------------------- clusters
    def list_clusters(self) -> list[dict]:
        return self._items(self.client.list_clusters())

    def get_cluster(self, key: str) -> dict:
        return self.client.get_cluster(key)

    def update_cluster_workers(self, key: str, min_w: int, max_w: int, auto_terminate_min: int | None = None) -> dict:
        """Update worker bounds using the cluster body field names of the Workbench REST API."""
        # Body shape: workerConfig.minWorkerCount/maxWorkerCount, autoTerminationMinutes.
        # Whether a PUT applies in place on a running cluster is untested; dry_run is the default.
        body: dict[str, Any] = {"workerConfig": {"minWorkerCount": int(min_w), "maxWorkerCount": int(max_w)}}
        if auto_terminate_min is not None:
            body["autoTerminationMinutes"] = int(auto_terminate_min)
        if self.dry_run:
            return self._dry("update_cluster", key=key, body=body)
        return self.client.update_cluster(key, **body)

    def stop_cluster(self, key: str) -> dict:
        if self.dry_run:
            return self._dry("stop_cluster", key=key)
        return self.client.stop_cluster(key)

    def start_cluster(self, key: str) -> dict:
        if self.dry_run:
            return self._dry("start_cluster", key=key)
        return self.client.start_cluster(key)

    # ----------------------------------------------------------------- metrics
    def summarize_metrics(
        self,
        cluster_key: str,
        metric: str,
        begin_iso: str,
        end_iso: str,
        aggregation: str = "MEAN",
        interval: str = "5m",
    ) -> list[dict] | None:
        """POST summarizeMetricsData. Returns the ``results`` list, or None when it is empty
        (unknown metric / no data). Never returns zero for an unknown metric."""
        if aggregation not in METRIC_AGGREGATIONS:
            raise ValueError(f"aggregation must be one of {METRIC_AGGREGATIONS}, got {aggregation!r}")
        if interval not in METRIC_INTERVALS:
            raise ValueError(f"interval must be one of {METRIC_INTERVALS}, got {interval!r}")
        if metric not in METRIC_NAMES:
            raise ValueError(f"metric must be one of {METRIC_NAMES}, got {metric!r}")
        body = {
            "metricName": metric,
            "timeBegin": begin_iso,
            "timeEnd": end_iso,
            "aggregationType": aggregation,
            "interval": interval,
        }
        payload = self.client.summarize_metrics(cluster_key, **body)  # read-only: allowed in dry-run
        results = payload.get("results") if isinstance(payload, dict) else payload
        if not results:
            return None
        return list(results)

    @staticmethod
    def metric_mean(results: list[dict] | None) -> float | None:
        """Mean of all data points across nodes; None when results is None (unknown)."""
        if not results:
            return None
        vals = [
            float(p.get("value"))
            for r in results
            for p in (r.get("aggregatedDataPoints") or [])
            if p.get("value") is not None
        ]
        return sum(vals) / len(vals) if vals else None

    # ----------------------------------------------------------- notebook SQL
    def _ensure_session(self) -> tuple[str, str]:
        if self._session_id is None:
            nb_path = self.env.probe_notebook
            nb_name = nb_path.rsplit("/", 1)[-1]
            sess = self.client.nb_create_session(nb_path, nb_name, self.env.cluster_id)
            self._session_id = sess["id"]
            self._kernel_id = sess["kernel"]["id"]
            logger.info("kernel session %s opened on cluster %s", self._session_id, self.env.cluster_id)
        return self._kernel_id, self._session_id  # type: ignore[return-value]

    def run_code(self, code: str, timeout: int = 300) -> dict:
        """Execute Python on the lazily-created kernel; returns the raw client result dict."""
        kernel_id, session_id = self._ensure_session()
        return self.client.nb_execute_code(kernel_id, session_id, code, timeout=timeout)

    def run_sql(self, sql: str, timeout: int = 300) -> list[tuple]:
        """Run ``spark.sql(sql)`` in the kernel and return rows as tuples (JSON round-trip).

        A per-call nonce is appended to the output marker so cumulative or repeated stdout chunks from
        the kernel can never be mistaken for this call's rows."""
        import uuid

        mark = _SQL_MARK + uuid.uuid4().hex[:8]
        code = (
            "import json as _j\n"
            "def _c(v):\n"
            "    return v.isoformat() if hasattr(v, 'isoformat') else v\n"
            f"_df = spark.sql({sql!r})\n"
            "_rows = [[_c(v) for v in r] for r in _df.collect()]\n"
            f"print({mark!r} + _j.dumps(_rows, default=str) + {mark!r})\n"
        )
        result = self.run_code(code, timeout=timeout)
        return parse_sql_result(result, mark)

    def close(self) -> None:
        if self._session_id is not None:
            if getattr(self, "keep_session", False):
                # a long kernel run outlived the client deadline; deleting the session would kill it mid-flight
                logger.warning("kernel session %s left open on purpose (a heartbeat run may still be in flight)", self._session_id)
                self._session_id = None
                self._kernel_id = None
                return
            try:
                self.client.nb_delete_session(self._session_id)
            finally:
                self._session_id = None
                self._kernel_id = None


def parse_sql_result(result: dict, mark: str = _SQL_MARK) -> list[tuple]:
    """Extract the marked JSON rows from an ``nb_execute_code`` result; raise on kernel error.

    The kernel streams stdout in chunks that may repeat earlier text, so every MARK...MARK pair is
    inspected and the last one that parses wins. The cell prints the marker for every statement, a
    write included (an empty JSON list), so a result without the marker means the statement did not
    run to its end: that is an error whatever the reported status, as is an empty or unparsable
    payload (the same no-silent-zero rule as the budget read). The gate's decision row is written
    through this path, and an unconfirmed write must never pass for a recorded decision."""
    if result.get("error"):
        err = result["error"]
        detail = f"{err.get('ename', '')}: {err.get('evalue', '')}" if isinstance(err, dict) else str(err)[:300]
        raise RuntimeError(f"kernel error: {detail}")
    text = "".join(
        str(o.get("text", "")) for o in result.get("outputs", []) if o.get("output_type") == "stream"
    )
    if mark not in text:
        raise RuntimeError(f"kernel status {result.get('status', 'unknown')}; no rows marker in output, "
                           f"the statement is not confirmed to have completed; raw tail: {text[-200:]!r}")
    parts = text.split(mark)
    last_err: Exception | None = None
    for payload in reversed(parts[1:-1] if len(parts) > 2 else parts[1:]):
        payload = payload.strip()
        if not payload:
            continue
        try:
            return [tuple(r) for r in json.loads(payload)]
        except Exception as e:  # noqa: BLE001
            last_err = e
    raise RuntimeError(f"no parsable rows payload in kernel output ({last_err}); raw tail: {text[-400:]!r}")



def _parse_ts(value: Any):
    from datetime import datetime, timezone

    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / (1000 if value > 1e11 else 1), tz=timezone.utc)
    try:
        ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    except ValueError:
        return None
