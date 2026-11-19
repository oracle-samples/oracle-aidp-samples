"""AidpRestClient — typed primitives for the AIDP control-plane REST surface.

Canonical implementation used by the plugin and the ``aidp-rest`` skill.
``skills/aidp-rest/client.py`` is a path-resolving re-export shim that imports
from here.
"""
from __future__ import annotations

import json
import re
import time
import urllib.parse
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import oci
import requests

API_VERSION = "20260430"
DEFAULT_TIMEOUT_S = 60


# ---------------------------------------------------------------------------
# Resource summaries
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WorkspaceSummary:
    key: str
    display_name: str | None


@dataclass(frozen=True)
class ClusterSummary:
    key: str
    display_name: str | None
    state: str  # ACTIVE | STOPPED | FAILED | CREATING


@dataclass(frozen=True)
class RunResult:
    status: str  # PENDING | RUNNING | SUCCESS | FAILED | CANCELED | TIMED_OUT
    raw: dict[str, Any]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class AidpRestError(RuntimeError):
    """REST call returned a non-success status or unexpected shape."""


# ---------------------------------------------------------------------------
# Signer factory
# ---------------------------------------------------------------------------


def _build_signer(cfg: dict) -> Any:
    """Construct the right OCI signer for the profile shape.

    - **Session-token profile**: ``security_token_file`` is set in the loaded
      OCI config → load the token file + the corresponding ``key_file``
      private key, return a :class:`oci.auth.signers.SecurityTokenSigner`.
      This is the signer for ``oci session authenticate`` profiles (the
      laptop-CLI default — e.g. ``AIDP_SESSION``).
    - **API-key profile**: ``security_token_file`` absent → construct the
      classic :class:`oci.signer.Signer` from
      ``tenancy`` + ``user`` + ``fingerprint`` + ``key_file``.

    Raises ``AidpRestError`` with a clear remediation hint when the
    session-token file is missing or empty (expired sessions manifest
    this way on some OCI CLI versions).
    """
    token_file = cfg.get("security_token_file")
    if token_file:
        token_path = Path(token_file).expanduser()
        try:
            token = token_path.read_text().strip()
        except FileNotFoundError as e:
            raise AidpRestError(
                f"OCI session token file not found at {token_path}; "
                "run `oci session refresh` (or `oci session authenticate`) "
                "to (re)create it."
            ) from e
        if not token:
            raise AidpRestError(
                f"OCI session token file at {token_path} is empty; "
                "run `oci session refresh` to refresh the session."
            )
        private_key_path = Path(cfg["key_file"]).expanduser()
        private_key = oci.signer.load_private_key_from_file(str(private_key_path))
        return oci.auth.signers.SecurityTokenSigner(token, private_key)
    # API-key profile.
    return oci.signer.Signer(
        tenancy=cfg["tenancy"],
        user=cfg["user"],
        fingerprint=cfg["fingerprint"],
        private_key_file_location=cfg["key_file"],
    )


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class AidpRestClient:
    """One client per (region, aidp_id, workspace_key) tuple.

    Authentication uses the local OCI config (``~/.oci/config``). The default
    profile is used unless ``oci_profile`` is given. The signer factory
    (:func:`_build_signer`) handles both API-key and session-token profiles
    transparently.

    All methods raise :class:`AidpRestError` on non-2xx responses or when the
    response shape diverges from the empirically-verified contract.
    """

    def __init__(
        self,
        *,
        region: str,
        aidp_id: str,
        workspace_key: str,
        oci_profile: str = "DEFAULT",
        request_timeout_s: int = DEFAULT_TIMEOUT_S,
        log: Callable | None = None,
    ) -> None:
        self.region = region
        self.aidp_id = aidp_id
        self.workspace_key = workspace_key
        self.request_timeout_s = request_timeout_s
        self._log = log or (lambda stage, **kw: None)
        cfg = oci.config.from_file(profile_name=oci_profile)
        self._signer = _build_signer(cfg)

    # ---- URL building -----------------------------------------------------

    @property
    def host(self) -> str:
        return f"https://datalake.{self.region}.oci.oraclecloud.com"

    @property
    def base(self) -> str:
        return (
            f"{self.host}/{API_VERSION}/aiDataPlatforms/{self.aidp_id}"
            f"/workspaces/{self.workspace_key}"
        )

    @property
    def datalake_base(self) -> str:
        return f"{self.host}/{API_VERSION}/aiDataPlatforms/{self.aidp_id}"

    # ---- Low-level HTTP ---------------------------------------------------

    def _request(
        self,
        method: str,
        url: str,
        *,
        json_body: dict | None = None,
        timeout: int | None = None,
    ) -> requests.Response:
        return requests.request(
            method,
            url,
            auth=self._signer,
            json=json_body,
            timeout=timeout or self.request_timeout_s,
        )

    def _ok(
        self,
        r: requests.Response,
        *,
        expected: tuple[int, ...] = (200,),
        context: str = "",
    ) -> dict[str, Any]:
        if r.status_code not in expected:
            raise AidpRestError(
                f"{context or r.url}: HTTP {r.status_code} (expected one of "
                f"{expected}) body={r.text[:500]}"
            )
        try:
            return r.json()
        except ValueError as e:
            raise AidpRestError(
                f"{context}: non-JSON response: {r.text[:200]}"
            ) from e

    # ---- Workspace + cluster discovery -----------------------------------

    def list_workspaces(self) -> list[WorkspaceSummary]:
        r = self._request("GET", f"{self.datalake_base}/workspaces")
        d = self._ok(r, context="list_workspaces")
        items = d.get("items", d) if isinstance(d, dict) else d
        out: list[WorkspaceSummary] = []
        for it in items:
            out.append(
                WorkspaceSummary(key=it["key"], display_name=it.get("displayName"))
            )
        return out

    def find_workspace_by_name(self, display_name: str) -> WorkspaceSummary:
        for ws in self.list_workspaces():
            if ws.display_name == display_name:
                return ws
            r = self._request("GET", f"{self.datalake_base}/workspaces/{ws.key}")
            d = self._ok(r, context=f"get_workspace({ws.key})")
            if d.get("displayName") == display_name:
                return WorkspaceSummary(
                    key=d["key"], display_name=d.get("displayName")
                )
        raise AidpRestError(f"no workspace with displayName={display_name!r}")

    # ---- Credential store -----------------------------------------------

    def check_credential_exists(
        self,
        display_name: str,
        *,
        timeout: int | None = None,
    ) -> bool:
        """Return True iff a credential with the given ``display_name``
        exists in the AIDP credential store for the current data-lake.

        **Per-AIDP scope, NOT per-workspace**. All workspaces under the
        same ``aiDataPlatformId`` share one credential store.

        Implementation note: the per-resource ``GET /credentials/<key>``
        endpoint expects a UUID, not a display name (it 400s with
        ``Invalid credentialV2Key`` on names). So this primitive LISTs
        and walks ``items[]``. Today's tenant has ~2 entries — LIST cost
        is dominated by network RTT, not body size.

        Future pagination: if AIDP downstream adds a ``nextPage`` token,
        follow it transparently. No paginated response observed today.

        Raises :class:`AidpRestError` on any non-2xx (transport / IAM /
        endpoint-shape regression). Distinct from "credential not found"
        which returns ``False`` cleanly.
        """
        r = self._request(
            "GET", f"{self.datalake_base}/credentials", timeout=timeout
        )
        body = self._ok(r, context="list_credentials")
        items = body.get("items", []) if isinstance(body, dict) else []
        return any(it.get("displayName") == display_name for it in items)

    def list_clusters(self) -> list[ClusterSummary]:
        r = self._request("GET", f"{self.base}/clusters")
        d = self._ok(r, context="list_clusters")
        items = d.get("items", d) if isinstance(d, dict) else d
        out: list[ClusterSummary] = []
        for it in items:
            out.append(
                ClusterSummary(
                    key=it["key"],
                    display_name=it.get("displayName"),
                    state=it.get("state", "UNKNOWN"),
                )
            )
        return out

    def find_cluster_by_name(self, display_name: str) -> ClusterSummary:
        for c in self.list_clusters():
            if c.display_name == display_name:
                return c
        raise AidpRestError(f"no cluster with displayName={display_name!r}")

    def get_cluster(self, cluster_key: str) -> dict[str, Any]:
        r = self._request("GET", f"{self.base}/clusters/{cluster_key}")
        return self._ok(r, context=f"get_cluster({cluster_key})")

    def verify_cluster_active(self, cluster_key: str) -> None:
        state = self.get_cluster(cluster_key)["state"]
        self._log("cluster_state", state=state)
        if state != "ACTIVE":
            raise AidpRestError(
                f"cluster {cluster_key} state={state}, expected ACTIVE"
            )

    def start_cluster(self, cluster_key: str) -> dict[str, Any]:
        r = self._request(
            "POST",
            f"{self.base}/clusters/{cluster_key}/actions/start",
            json_body={},  # empty body REQUIRED — null body returns 400
        )
        return self._ok(
            r, expected=(200, 202), context=f"start_cluster({cluster_key})"
        )

    def wait_cluster_active(
        self,
        cluster_key: str,
        *,
        timeout_s: int = 600,
        poll_s: int = 10,
    ) -> None:
        deadline = time.time() + timeout_s
        last = None
        while time.time() < deadline:
            state = self.get_cluster(cluster_key)["state"]
            if state != last:
                self._log("cluster_wait", state=state)
                last = state
            if state == "ACTIVE":
                return
            if state in ("FAILED",):
                raise AidpRestError(
                    f"cluster transitioned to {state} while waiting"
                )
            time.sleep(poll_s)
        raise AidpRestError(
            f"cluster {cluster_key} did not reach ACTIVE within {timeout_s}s"
        )

    # ---- Notebook contents API -------------------------------------------

    def upload_notebook(self, workspace_path: str, ipynb: dict) -> str:
        enc = urllib.parse.quote(workspace_path, safe="")
        body = {
            "name": Path(workspace_path).name,
            "path": workspace_path,
            "type": "notebook",
            "format": "json",
            "content": ipynb,
        }
        r = self._request(
            "PUT",
            f"{self.base}/notebook/api/contents/{enc}",
            json_body=body,
            timeout=120,
        )
        d = self._ok(
            r,
            expected=(200, 201),
            context=f"upload_notebook({workspace_path})",
        )
        path = d.get("path", workspace_path)
        self._log("notebook_uploaded", path=path)
        return path

    def get_notebook(self, workspace_path: str) -> dict[str, Any]:
        enc = urllib.parse.quote(workspace_path, safe="")
        r = self._request(
            "GET",
            f"{self.base}/notebook/api/contents/{enc}?type=notebook&content=1",
        )
        return self._ok(r, context=f"get_notebook({workspace_path})")

    # ---- Jobs + JobRuns + TaskRuns ---------------------------------------

    def create_notebook_job(
        self,
        *,
        name: str,
        description: str,
        notebook_path: str,
        cluster_key: str,
        cluster_name: str,
        task_key: str,
    ) -> str:
        """Create a single-task NOTEBOOK_TASK job. Returns jobKey.

        Bakes in the empirically-required quirks: ``path='jobs'``,
        ``maxConcurrentRuns=1``, redundant cluster ref in both
        ``jobClusters[]`` and ``tasks[].cluster``.
        """
        cluster_ref = {
            "clusterKey": cluster_key,
            "clusterName": cluster_name,
            "newCluster": None,
        }
        body = {
            "name": name,
            "description": description,
            "path": "jobs",  # required client-side; undocumented
            "maxConcurrentRuns": 1,  # required client-side; undocumented
            "jobClusters": [cluster_ref],
            "parameters": [],
            "tasks": [
                {
                    "type": "NOTEBOOK_TASK",
                    "taskKey": task_key,
                    "runIf": "ALL_SUCCESS",
                    "notebookPath": notebook_path,
                    "cluster": cluster_ref,
                    "parameters": [],
                    "dependsOn": [],
                    "maxRetries": 0,
                }
            ],
        }
        r = self._request("POST", f"{self.base}/jobs", json_body=body)
        d = self._ok(r, expected=(201,), context="create_notebook_job")
        job_key = d["key"]
        self._log("job_created", jobKey=job_key)
        return job_key

    def submit_run(
        self, job_key: str, *, parameters: list[dict] | None = None
    ) -> str:
        body = {
            "jobKey": job_key,
            "parameters": parameters or [],
            "queue": {"isEnabled": False},
        }
        r = self._request("POST", f"{self.base}/jobRuns", json_body=body)
        d = self._ok(r, expected=(201,), context=f"submit_run({job_key})")
        run_key = d["key"]
        self._log("jobRun_submitted", jobRunKey=run_key)
        return run_key

    def get_run(
        self, run_key: str, *, timeout: int | None = None
    ) -> dict[str, Any]:
        """Return a job-run payload.

        ``timeout`` is a per-call HTTP timeout override; default ``None``
        falls back to ``self.request_timeout_s``. ``dispatch_via_rest`` uses
        it for bounded post-timeout diagnostics.
        """
        r = self._request(
            "GET", f"{self.base}/jobRuns/{run_key}", timeout=timeout
        )
        return self._ok(r, context=f"get_run({run_key})")

    TERMINAL_STATUSES: frozenset[str] = frozenset(
        {"SUCCESS", "FAILED", "CANCELED", "TIMED_OUT"}
    )

    def poll_run(
        self,
        run_key: str,
        *,
        timeout_s: int = 1800,
        interval_s: int = 20,
        on_status_change: Callable | None = None,
    ) -> RunResult:
        """Poll until terminal state. Tolerates transient
        ``ReadTimeout`` / ``ConnectionError``."""
        deadline = time.time() + timeout_s
        last_status: str | None = None
        run: dict[str, Any] = {}
        while time.time() < deadline:
            try:
                run = self.get_run(run_key)
            except (
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
            ) as e:
                self._log("poll_warn", err=type(e).__name__)
                time.sleep(interval_s)
                continue
            status = run["state"]["status"]
            if status != last_status:
                self._log("poll", status=status)
                if on_status_change is not None:
                    on_status_change(status)
                last_status = status
            if status in self.TERMINAL_STATUSES:
                return RunResult(status=status, raw=run)
            time.sleep(interval_s)
        raise AidpRestError(
            f"poll_run({run_key}): deadline exceeded after {timeout_s}s"
        )

    @staticmethod
    def resolve_task_run_key(run: dict[str, Any], task_key: str) -> str:
        trk = (run.get("taskToTaskRunMap") or {}).get(task_key)
        if trk:
            return trk
        summary_map = run.get("taskRunSummaryMap") or {}
        if len(summary_map) == 1:
            return next(iter(summary_map))
        raise AidpRestError(
            f"no taskRunKey for task_key={task_key!r}; "
            f"taskToTaskRunMap keys="
            f"{list((run.get('taskToTaskRunMap') or {}).keys())}, "
            f"taskRunSummaryMap keys={list(summary_map.keys())}"
        )

    def fetch_output(
        self,
        task_run_key: str,
        *,
        output_key: str = "",
        timeout: int | None = None,
    ) -> str:
        """Fetch the executed-notebook JSON string for a task run.

        ``output_key`` must be ``""`` (empty string) for notebook tasks —
        ``"main"`` returns a misleading 404. The notebook JSON lands at
        ``data[0].value`` (NOT ``data[0].content``).

        ``timeout`` is a per-call HTTP timeout override; default ``None``
        falls back to ``self.request_timeout_s``. ``dispatch_via_rest`` uses
        it for bounded post-timeout diagnostics.
        """
        r = self._request(
            "POST",
            f"{self.base}/taskRuns/{task_run_key}/actions/fetchOutput",
            json_body={"outputKey": output_key},
            timeout=timeout,
        )
        if r.status_code != 200:
            raise AidpRestError(
                f"fetch_output({task_run_key}, outputKey={output_key!r}): "
                f"HTTP {r.status_code} body={r.text[:300]}"
            )
        out = r.json()
        data_arr = out.get("data") or []
        if not data_arr:
            return ""
        return data_arr[0].get("value", "") or ""

    # ---- Output-marker parsing -------------------------------------------

    @staticmethod
    def parse_marker(
        executed_notebook: dict[str, Any],
        *,
        begin: str,
        end: str,
        decode_base64: bool = False,
    ) -> dict[str, Any] | None:
        """Walk ``cells[*].outputs[*]`` of an executed notebook for a stdout
        marker block. Returns the parsed JSON or None if absent.

        ``decode_base64=True`` means the payload between ``begin`` and ``end``
        is base64-encoded JSON; decode it before ``json.loads``. Required for
        cluster-side markers because
        AIDP's Jupyter wraps stdout as ``display_data text/plain`` and
        strips JSON-escape backslashes, corrupting embedded quotes
        inside failed-step messages.

        Hardening: on ``json.JSONDecodeError`` against the ``BEGIN..END``
        body, attempt a regex extraction of ``run_id`` and return a sentinel
        payload
        ``{"run_id": "<id>", "_marker_parse_failed": True, "_raw_marker": ...}``
        so the dispatcher can surface a typed ``DispatchMarkerDegradedError``
        carrying the resume handle. If the regex also can't find a
        ``run_id``, the original ``json.JSONDecodeError`` propagates.
        """
        import base64

        for cell in executed_notebook.get("cells", []):
            for output in cell.get("outputs", []):
                for src in ("text", "data"):
                    value = output.get(src)
                    if value is None:
                        continue
                    if src == "data":
                        value = value.get("text/plain", "")
                    if isinstance(value, list):
                        value = "".join(value)
                    if begin in value:
                        b = value.index(begin) + len(begin)
                        e = value.index(end, b)
                        payload = value[b:e].strip()
                        if decode_base64:
                            # The run/drift producer base64-wraps the marker so
                            # it survives AIDP's display_data text/plain capture
                            # (which strips JSON-escape backslashes and corrupts
                            # any payload carrying quotes/reprs — e.g. a failed
                            # step's error_message). Tolerate a raw-JSON payload
                            # too (pre-fix notebooks / test fixtures):
                            # validate=True makes b64decode RAISE on raw JSON's
                            # non-alphabet chars ({ " : ...) rather than silently
                            # stripping them, so the fallback is unambiguous.
                            try:
                                payload = base64.b64decode(
                                    payload, validate=True
                                ).decode("utf-8")
                            except ValueError:
                                pass  # not base64 — treat as raw JSON
                        try:
                            return json.loads(payload)
                        except json.JSONDecodeError:
                            # Plain-decode markers can arrive with JSON
                            # escapes stripped. Recover the run_id via regex
                            # so the operator gets a resume handle; if even
                            # that fails, re-raise the original decode error.
                            m = re.search(
                                r'"run_id"\s*:\s*"([^"]+)"', payload,
                            )
                            if m is None:
                                raise
                            return {
                                "run_id": m.group(1),
                                "_marker_parse_failed": True,
                                "_raw_marker": payload[:2000],
                            }
        return None

    # Pattern for "ExceptionClass: message" lines in a Python traceback
    # emitted by AIDP's notebook runtime as an stderr stream. Some AIDP
    # outputs do not use output_type=error for cell exceptions; they emit the
    # full traceback into output_type=stream, name=stderr instead.
    # Single-line ($ end-of-line, no dotall) so chained exceptions produce
    # separate matches; the LAST match is the outermost exception that
    # propagated. Anchored on the start of a non-indented line so we don't
    # false-match "Exception:" inside a stack frame body. Ename must be a
    # Python identifier; dots are allowed for module-path enames.
    _STDERR_TRACEBACK_PATTERN = re.compile(
        r"(?m)^([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*"
        r"(?:Error|Exception|Warning|Interrupt|Exit|Timeout|Failure)):"
        r"[ \t]*(.*)$"
    )

    @staticmethod
    def extract_cell_errors(
        executed_notebook: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Walk executed-notebook cells for exception-shaped outputs.

        Recognizes two shapes:

        1. **Canonical Jupyter error output** (``output_type="error"``) —
           the documented shape for cells that raised. Produced by
           ``nbconvert`` and most kernels.

        2. **AIDP stderr-stream tracebacks** — some AIDP notebook runtime
           outputs capture cell exceptions as
           ``output_type="stream", name="stderr"`` with the Python
           traceback in ``text``. The final ``ExceptionClass: message``
           line is regex-matched and surfaced as
           ``{"ename": ..., "evalue": ...}``. This is what makes
           dispatch_via_rest's cell-error enrichment fire on real cluster
           output.
        """
        errors: list[dict[str, Any]] = []
        for i, cell in enumerate(executed_notebook.get("cells", [])):
            for output in cell.get("outputs", []):
                ot = output.get("output_type")
                if ot == "error":
                    errors.append(
                        {
                            "cell_index": i,
                            "ename": output.get("ename"),
                            "evalue": output.get("evalue"),
                            "traceback": output.get("traceback", []),
                        }
                    )
                elif ot == "stream" and output.get("name") == "stderr":
                    text = output.get("text", "")
                    if isinstance(text, list):
                        text = "".join(text)
                    # Search across the whole stream — multiple
                    # tracebacks in one stderr output is possible
                    # (chained exception). The LAST match is the
                    # outermost exception that propagated.
                    matches = list(
                        AidpRestClient._STDERR_TRACEBACK_PATTERN.finditer(text)
                    )
                    if matches:
                        m = matches[-1]
                        ename = m.group(1)
                        evalue = m.group(2).strip()
                        errors.append(
                            {
                                "cell_index": i,
                                "ename": ename,
                                "evalue": evalue,
                                "traceback": text.splitlines(),
                            }
                        )
        return errors


__all__ = [
    "AidpRestClient",
    "AidpRestError",
    "WorkspaceSummary",
    "ClusterSummary",
    "RunResult",
    "_build_signer",
]
