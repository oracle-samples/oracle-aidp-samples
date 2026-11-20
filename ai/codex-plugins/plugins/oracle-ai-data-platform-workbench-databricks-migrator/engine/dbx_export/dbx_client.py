"""Databricks REST client — GET-only.

By design, only fetch endpoints are wired in. This client cannot trigger a
run, modify a notebook, change a job, or delete anything on the Databricks
side. The exporter reads from DBX and writes to AIDP — never the
reverse.

Endpoints used:
  GET /api/2.2/jobs/get?job_id=...        — job definition
  GET /api/2.0/workspace/export           — notebook export (JUPYTER format)
  GET /api/2.0/workspace-files/<path>     — large workspace file streaming
  GET /api/2.0/workspace/list             — directory listing (rarely)
  GET /api/2.0/dbfs/read                  — chunked file read
  GET /api/2.0/dbfs/get-status            — DBFS file metadata (size)
  GET /api/2.1/clusters/get?cluster_id=…  — existing cluster definition
  GET /api/2.0/libraries/cluster-status   — libraries installed on cluster
"""

from __future__ import annotations

import base64
import json
import os
import posixpath
import re
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.auth import HTTPBasicAuth


DBX_EXPORT_FORMAT = "JUPYTER"
DBFS_CHUNK = 1024 * 1024  # 1 MB — DBFS read API hard limit per call
MAX_EXPORT_RETRIES = 8
EXPORT_BACKOFF_SEC = 6
# Default attempt count for GitHub-via-proxy reads on a transient 429/5xx,
# used when the customer YAML omits `dbx_source.proxy_max_retries`. The proxy
# is customer-controlled and rate-limits (observed: nginx/1.25.5 HTTP 429 on
# list_github_dir_entries during the examplecorp auto_ft export, 2026-09-20) once many
# developers export concurrently, so this is higher than MAX_EXPORT_RETRIES (8,
# which governs the workspace/DBFS paths we do control). Default 10 rides out a
# transient burst; the schema caps it at 20 so a mistyped value can't turn a
# sustained outage into an hours-long grind (see _GITHUB_RETRY_MAX_DELAY_SEC).
# See DbxConfig.proxy_max_retries and DbxClient._github_get_with_retry.
DEFAULT_PROXY_MAX_RETRIES = 10
# Per-attempt sleep ceiling for the GitHub retry loop. Caps BOTH the linear
# exporter back-off AND a server `Retry-After` (nginx omits it → linear back-off;
# a hostile/misconfigured header like `Retry-After: 3600` would otherwise sleep
# an hour per read). Bounds one exhausted read to ~attempts x this, so a
# sustained outage fails in bounded time instead of wedging the export.
_GITHUB_RETRY_MAX_DELAY_SEC = 90
# HTTP statuses treated as transient (retry, and never cache as a result): a
# rate limit or gateway blip, not a permanent answer. A 404 is deliberately NOT
# here — an absent path is a real, cacheable "empty".
_TRANSIENT_HTTP_STATUSES = (429, 500, 502, 503, 504)
# Hard ceiling for a proxy-fetched external library body. The largest real
# fat-jars seen in migrations are well under this; anything bigger is far
# more likely a runaway/looping response than a legitimate artifact.
MAX_EXTERNAL_FETCH_BYTES = 256 * 1024 * 1024

_OVERSIZED_NOTEBOOK_ERROR_CODES = frozenset({
    "MAX_NOTEBOOK_SIZE_EXCEEDED",
    "PAYLOAD_TOO_LARGE",
    "MAX_PAYLOAD_SIZE_EXCEEDED",
})
_OVERSIZED_NOTEBOOK_MESSAGE_MARKERS = (
    "notebook size exceeded the byte limit",
    "notebook has exceeded the memory limit",
    "has exceeded the memory limit. try clearing cell outputs",
    "maximum notebook size exceeded",
)


def _export_api_path(p: str) -> str:
    """Convert a DBX path to the form workspace/export expects.

    Modern DBX (>=13.x) returns notebook paths from /api/2.x/jobs/get like:
        /Workspace/Users/foo@bar/notebook.ipynb

    But /api/2.0/workspace/export rejects that form with 404. It requires the
    canonical REST form:
        /Users/foo@bar/notebook

    This translator is applied ONLY at the export-API boundary. Everywhere
    else in the package, paths are kept verbatim as DBX returned them.
    Idempotent on already-canonical paths.
    """
    if p.startswith("/Workspace/"):
        p = p[len("/Workspace"):]   # keep leading '/'
    elif p.startswith("Workspace/"):
        p = "/" + p[len("Workspace/"):]
    if p.endswith(".ipynb"):
        p = p[: -len(".ipynb")]
    return p


def _response_error(response) -> Tuple[str, str]:
    """Return structured Databricks error code/message when available."""
    try:
        payload = response.json()
    except (ValueError, TypeError):
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    code = str(payload.get("error_code") or payload.get("code") or "").strip()
    message = str(payload.get("message") or payload.get("error") or "").strip()
    if not message:
        message = str(getattr(response, "text", "") or "").strip()
    return code, message


def _is_oversized_notebook_error(response) -> bool:
    """Classify only explicit notebook/payload size errors.

    Do not search the full raw response for generic words such as ``PAYLOAD``:
    a valid path can contain that word (for example an MLflow experiment named
    ``*_PAYLOAD_RUN_JOBS``) without being an oversized notebook.
    """
    code, message = _response_error(response)
    if code.upper() in _OVERSIZED_NOTEBOOK_ERROR_CODES:
        return True
    normalized = " ".join(message.lower().split())
    return any(marker in normalized for marker in _OVERSIZED_NOTEBOOK_MESSAGE_MARKERS)


_PROXY_USERINFO_RE = re.compile(r"://[^/\s@]+:[^/\s@]*@")


def _scrub_proxy_creds(text: Any) -> str:
    """Strip any ``scheme://user:pass@`` userinfo from a string before it can
    reach a log line, exception message, or DbxExportError.detail.

    requests/urllib3 can embed the full proxy URL (including credentials) in
    a ProxyError/ConnectionError's str() when the tunnel setup fails, and a
    destination URL can carry its own userinfo too. Every place that turns
    such text into a message MUST go through this first — never print or
    raise raw exception text or URLs for proxied/external requests.

    Relies on forward_proxy_url() percent-encoding credentials before they
    ever reach a URL: an encoded credential cannot contain a raw ``/``,
    space, or ``@``, so the regex's exclusion of those characters can't be
    defeated by a credential value that happens to contain them.
    """
    return _PROXY_USERINFO_RE.sub("://***:***@", str(text))


class DbxExportError(RuntimeError):
    """Raised when a notebook/file cannot be fetched from DBX. Carries reason."""

    def __init__(self, reason: str, detail: str = "", status: int = 0):
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail
        self.status = status

    @property
    def is_job_not_found(self) -> bool:
        """Only definitive job absence permits a candidate skip.

        Permission, transport and service failures never establish absence,
        even if their diagnostic text mentions a missing job.
        """
        return self.status == 404 or (
            self.reason == "job_not_found" and self.status in (0, 400)
        )


@dataclass
class ExportedFile:
    """Result of exporting a notebook-or-utility from DBX or GitHub.

    is_notebook=True  → content is .ipynb bytes (extension=".ipynb"), should be
                        uploaded via Jupyter contents API as a notebook.
    is_notebook=False → content is the original utility file bytes (.py, .scala),
                        should be uploaded as a plain file preserving extension.
    """
    content: bytes
    is_notebook: bool
    extension: str  # ".ipynb" | ".py" | ".scala" | ...
    export_method: Optional[str] = None
    # Actual repository filename used by a successful Git fetch, not a discovery alias.
    resolved_git_path: Optional[str] = None


@dataclass
class DbxConfig:
    """Where + how to talk to Databricks.

    Either (host + token) OR (proxy + basic_auth) must be set. The proxy form
    matches the reference notebook's Databricks proxy VNIC used from the
    AIDP cluster; the host+token form is for direct local access.
    """

    host: Optional[str] = None  # e.g. https://dbc-12345.cloud.databricks.com
    token: Optional[str] = None
    proxy: Optional[str] = None  # e.g. host:port
    # No hardcoded default credential — this repo is headed for public
    # release. Proxy basic auth is OPTIONAL: the pilot forward proxy needs no
    # HTTP basic auth, so `--dbx-proxy <host:port>` alone must work. If a proxy
    # DOES require basic auth, supply both (CLI --proxy-basic-user/
    # --proxy-basic-pass, or env DBX_PROXY_BASIC_USER/DBX_PROXY_BASIC_PASS);
    # http_kwargs()/forward_proxy_url() apply them only when both are present,
    # and never fall back to a hardcoded credential.
    proxy_basic_user: Optional[str] = None
    proxy_basic_pass: Optional[str] = None
    # When True, s3://.../http(s)://... library sources (JARs/wheels recorded
    # on a cluster's library status, not stored in DBX workspace/DBFS) are
    # fetched by tunnelling through the same proxy as an HTTP forward proxy,
    # rather than being skipped as "external, unfetchable".
    fetch_external_via_proxy: bool = False
    # Max attempts for a GitHub-via-proxy read (dir listing / notebook / resource)
    # on a transient 429/5xx before giving up. Customer-tunable via
    # `dbx_source.proxy_max_retries` because the proxy is customer-controlled and
    # its rate limit varies with how many developers export at once; defaults to
    # DEFAULT_PROXY_MAX_RETRIES (10) when the YAML omits it. Only the GitHub proxy
    # reads honour this (see _github_get_with_retry); workspace/DBFS paths keep
    # MAX_EXPORT_RETRIES, since those endpoints are not the customer bottleneck.
    proxy_max_retries: int = DEFAULT_PROXY_MAX_RETRIES

    def base_url(self, api_version: str = "2.1") -> str:
        if self.host:
            return f"{self.host.rstrip('/')}/api/{api_version}"
        if self.proxy:
            return f"http://{self.proxy}/databricks/api/{api_version}"
        raise ValueError("DbxConfig requires either host+token or proxy")

    def github_proxy_base(self) -> Optional[str]:
        """Base URL for the GitHub proxy (only available in proxy mode).

        Used to fetch git-backed Databricks job notebooks via pilot-job
        migration proxy's GitHub passthrough."""
        if self.proxy:
            return f"http://{self.proxy}/github"
        return None

    def http_kwargs(self) -> Dict[str, Any]:
        if self.host:
            return {"headers": {"Authorization": f"Bearer {self.token}"}}
        # Proxy mode: basic auth is OPTIONAL. The pilot forward proxy needs no
        # HTTP basic auth, so `--dbx-proxy` alone works (no creds required).
        # Attach HTTPBasicAuth only when the operator supplied both — for a
        # proxy that does require it — never a hardcoded default.
        if self.proxy_basic_user and self.proxy_basic_pass:
            return {"auth": HTTPBasicAuth(self.proxy_basic_user, self.proxy_basic_pass)}
        return {}

    def forward_proxy_url(self) -> Optional[str]:
        """Forward-proxy URL for tunnelling requests to *external* hosts
        (s3.amazonaws.com, arbitrary http(s) URLs) through the same box that
        serves the DBX/GitHub passthrough routes.

        Credentials go in the URL userinfo, percent-encoded, so `requests`
        attaches them as Proxy-Authorization on the CONNECT tunnel. They must
        NOT be passed via `auth=` on the outer request — that would attach
        Authorization to the DESTINATION request instead, which the
        destination doesn't expect and the proxy doesn't need. Encoding is
        mandatory here (not optional cleanup): an un-encoded credential
        containing `/`, `@`, or whitespace would both break the URL's syntax
        and defeat _scrub_proxy_creds's leak-prevention regex.
        """
        if not self.proxy:
            return None
        # Basic auth optional (see http_kwargs). No creds → plain proxy URL;
        # the pilot proxy needs no auth. Encode only when both are supplied.
        if self.proxy_basic_user and self.proxy_basic_pass:
            user = urllib.parse.quote(self.proxy_basic_user, safe="")
            password = urllib.parse.quote(self.proxy_basic_pass, safe="")
            return f"http://{user}:{password}@{self.proxy}"
        return f"http://{self.proxy}"


class DbxClient:
    """Minimal Databricks REST client (GET only)."""

    def __init__(self, config: DbxConfig):
        self.config = config

    @staticmethod
    def _transient_retry_delay(response: requests.Response, attempt: int) -> float:
        """Return the server-requested delay, or the exporter backoff."""
        raw = response.headers.get("Retry-After") if response.headers else None
        if raw is not None:
            try:
                return max(0.0, float(raw))
            except (TypeError, ValueError):
                pass
        return float(EXPORT_BACKOFF_SEC * attempt)

    def _get_workspace_file_with_retry(
        self,
        url: str,
        *,
        retry_transient: bool,
        operation: str = "workspace file",
        **kwargs: Any,
    ) -> requests.Response:
        """GET a non-JAR workspace file with bounded 429/5xx retry.

        JAR exports deliberately pass ``retry_transient=False`` so unavailable
        binary dependencies fail quickly. Plain Python/config resources and
        init scripts retry because strict dependency mode cannot safely
        continue without them.
        """
        attempts = MAX_EXPORT_RETRIES if retry_transient else 1
        transient_statuses = (429, 500, 502, 503, 504)
        response = None
        for attempt in range(1, attempts + 1):
            response = requests.get(url, **kwargs)
            if response.status_code not in transient_statuses or attempt == attempts:
                return response
            delay = self._transient_retry_delay(response, attempt)
            print(
                f"[dbx_export] {operation} HTTP {response.status_code}; "
                f"retry {attempt}/{attempts - 1} in {delay:g}s"
            )
            close = getattr(response, "close", None)
            if callable(close):
                close()
            time.sleep(delay)
        assert response is not None
        return response

    def _github_get_with_retry(
        self,
        url: str,
        *,
        accept: str,
        operation: str,
        timeout: int = 120,
    ) -> requests.Response:
        """GET a GitHub-via-proxy URL, retrying transient 429/5xx.

        The customer proxy fronting GitHub rate-limits (nginx 429) once many
        developers export concurrently — observed as HTTP 429 on
        list_github_dir_entries during the examplecorp auto_ft export (2026-09-20),
        which was previously swallowed into an empty directory listing and
        surfaced downstream as a misleading `git_path_unresolved`. Every
        GitHub read (dir listing, notebook, resource) routes through here so a
        transient burst is ridden out rather than aborting the export.

        Attempt count comes from the customer-tunable
        `dbx_source.proxy_max_retries` (default 10), NOT MAX_EXPORT_RETRIES,
        because only this customer-controlled hop is the bottleneck. Back-off
        reuses `_transient_retry_delay`, so the server's `Retry-After` is
        honoured when present (the correct, well-behaved response to a 429),
        else the linear exporter back-off. On exhaustion the final response is
        returned unchanged so each caller keeps its own non-200 handling.

        NOTE (deliberate, not an oversight): this deliberately mirrors
        `_get_workspace_file_with_retry` rather than being folded into it — the
        two differ in attempt-count source (this: the customer knob; that:
        MAX_EXPORT_RETRIES), the per-attempt delay clamp (GitHub-only), and the
        Accept-header handling. Parameterising the workspace helper to serve both
        would touch the validated workspace/DBFS path, which this PR intentionally
        leaves alone. De-duplicating the loop (and sweeping the inline
        `_TRANSIENT_HTTP_STATUSES` literal at the other sites) is a tracked
        cleanliness follow-up, out of scope of this retry change.
        """
        kwargs = dict(self.config.http_kwargs())
        headers = {**kwargs.pop("headers", {}), "Accept": accept}
        attempts = max(1, int(self.config.proxy_max_retries))
        transient_statuses = _TRANSIENT_HTTP_STATUSES
        response = None
        for attempt in range(1, attempts + 1):
            response = requests.get(url, headers=headers, timeout=timeout, **kwargs)
            if response.status_code not in transient_statuses or attempt == attempts:
                return response
            # Clamp per-attempt sleep so neither the linear back-off nor a large
            # (or hostile) server `Retry-After` can wedge a single read for
            # minutes/hours — bounds one exhausted read to ~attempts x the cap.
            delay = min(_GITHUB_RETRY_MAX_DELAY_SEC,
                        self._transient_retry_delay(response, attempt))
            print(
                f"[dbx_export] {operation} HTTP {response.status_code}; "
                f"retry {attempt}/{attempts - 1} in {delay:g}s"
            )
            close = getattr(response, "close", None)
            if callable(close):
                close()
            time.sleep(delay)
        assert response is not None
        return response

    # ─── Probe ────────────────────────────────────────────────────────

    def probe(self) -> Tuple[bool, int, str]:
        """Read-only connectivity check.

        Hits /api/2.2/jobs/list?limit=1 — a small, side-effect-free request
        that exercises the full auth path. Returns (ok, http_status, detail).
        """
        url = f"{self.config.base_url('2.2')}/jobs/list"
        try:
            r = requests.get(
                url, params={"limit": 1}, timeout=15, **self.config.http_kwargs()
            )
        except requests.exceptions.RequestException as e:
            return False, 0, f"network error: {e.__class__.__name__}: {e}"
        if r.status_code == 200:
            try:
                count = len(r.json().get("jobs", []))
                return True, 200, f"OK — jobs/list returned {count} item(s)"
            except ValueError:
                return True, 200, "OK — got 200 but non-JSON body (unexpected; still passes)"
        return False, r.status_code, (r.text or "")[:400]

    # ─── Jobs ─────────────────────────────────────────────────────────

    def get_job(self, job_id: str) -> Dict[str, Any]:
        url = f"{self.config.base_url('2.2')}/jobs/get"
        try:
            r = requests.get(url, params={"job_id": job_id}, **self.config.http_kwargs())
        except requests.RequestException as exc:
            # Keep transport failures in the ordinary child-failure/report path.
            # Requests diagnostics can contain proxy credentials; use type only.
            raise DbxExportError("get_job_failed", type(exc).__name__) from None
        if r.status_code != 200:
            reason = "get_job_failed"
            if r.status_code == 404:
                reason = "job_not_found"
            elif r.status_code == 400:
                try:
                    payload = r.json()
                except ValueError:
                    payload = None
                if isinstance(payload, dict):
                    code = payload.get("error_code")
                    # Live jobs/get 2.0 returns INVALID_PARAMETER_VALUE for
                    # absence as well as malformed requests (PR #229 R3).
                    # Require the complete observed message for THIS request;
                    # a prefix, another id, or the code alone proves nothing.
                    if code == "RESOURCE_DOES_NOT_EXIST" or (
                        code == "INVALID_PARAMETER_VALUE"
                        and str(job_id).isascii() and str(job_id).isdigit()
                        and payload.get("message") == f"Job {job_id} does not exist."
                    ):
                        reason = "job_not_found"
            raise DbxExportError(reason, f"HTTP {r.status_code}: {r.text[:300]}", r.status_code)
        return r.json()

    def list_job_runs(self, job_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """List recent runs for a job (caller sorts by start_time if a specific
        ordering is required). Used to find which live/ephemeral cluster
        instance a job's run actually used, so library sources can be read
        from the cluster that really ran it rather than guessed from the job
        definition."""
        url = f"{self.config.base_url('2.1')}/jobs/runs/list"
        r = requests.get(
            url, params={"job_id": job_id, "limit": limit, "expand_tasks": "false"},
            **self.config.http_kwargs(),
        )
        if r.status_code != 200:
            raise DbxExportError("list_job_runs_failed", f"HTTP {r.status_code}: {r.text[:300]}", r.status_code)
        return r.json().get("runs", [])

    def get_run(self, run_id: int) -> Dict[str, Any]:
        """Fetch full run detail (task list + cluster_instance per task)."""
        url = f"{self.config.base_url('2.1')}/jobs/runs/get"
        r = requests.get(url, params={"run_id": run_id}, **self.config.http_kwargs())
        if r.status_code != 200:
            raise DbxExportError("get_run_failed", f"HTTP {r.status_code}: {r.text[:300]}", r.status_code)
        return r.json()

    # ─── Clusters ─────────────────────────────────────────────────────

    def get_cluster(self, cluster_id: str) -> Optional[Dict[str, Any]]:
        url = f"{self.config.base_url('2.1')}/clusters/get"
        r = requests.get(url, params={"cluster_id": cluster_id}, **self.config.http_kwargs())
        if r.status_code == 404:
            return None
        if r.status_code != 200:
            raise DbxExportError("get_cluster_failed", f"HTTP {r.status_code}: {r.text[:300]}", r.status_code)
        return r.json()

    def get_cluster_libraries(self, cluster_id: str) -> Optional[Dict[str, Any]]:
        url = f"{self.config.base_url('2.0')}/libraries/cluster-status"
        r = requests.get(url, params={"cluster_id": cluster_id}, **self.config.http_kwargs())
        if r.status_code == 404:
            return None
        if r.status_code != 200:
            # Non-fatal — libraries are an enrichment — but don't go silent:
            # a 401/403/429/500 here makes "libraries omitted" a debugging mystery.
            print(f"[dbx_export] WARN: get_cluster_libraries({cluster_id}) "
                  f"HTTP {r.status_code}: {r.text[:200]} — libraries will be omitted")
            return None
        return r.json()

    # ─── Workspace notebook export ────────────────────────────────────

    def _workspace_export_raw(
        self,
        api_path: str,
        fmt: str,
        *,
        allow_empty: bool = False,
        direct_download: bool = False,
    ) -> bytes:
        """Single workspace/export round-trip. Returns raw bytes (after base64
        decode). Raises DbxExportError on non-2xx (same retry/back-off rules
        as export_notebook)."""
        url = f"{self.config.base_url('2.0')}/workspace/export"
        params = {"path": api_path, "format": fmt}
        if direct_download:
            params["direct_download"] = True
        last_status, last_text = 0, ""
        for attempt in range(1, MAX_EXPORT_RETRIES + 1):
            r = requests.get(url, params=params, **self.config.http_kwargs())
            last_status, last_text = r.status_code, (r.text[:300] if r.text else "")
            if r.status_code == 200:
                if direct_download:
                    content = getattr(r, "content", b"")
                    # Some enterprise proxies ignore direct-download response
                    # streaming and preserve the normal base64 JSON envelope.
                    # Accept that shape without confusing it with raw IPYNB
                    # JSON (which has ``cells`` rather than ``content``).
                    try:
                        direct_payload = r.json()
                    except (ValueError, TypeError):
                        direct_payload = None
                    if isinstance(direct_payload, dict) and isinstance(
                        direct_payload.get("content"), str
                    ):
                        encoded_content = direct_payload["content"]
                        if encoded_content == "" and allow_empty:
                            return b""
                        if encoded_content:
                            return base64.b64decode(encoded_content)
                    if content == b"" and allow_empty:
                        return b""
                    if not content:
                        raise DbxExportError(
                            "empty_export", f"empty direct-download for {api_path}", 200
                        )
                    return content
                payload = r.json()
                if "content" not in payload:
                    raise DbxExportError("empty_export", f"missing content for {api_path}", 200)
                content = payload.get("content")
                if content == "" and allow_empty:
                    return b""
                if not content:
                    raise DbxExportError("empty_export", f"empty content for {api_path}", 200)
                return base64.b64decode(content)
            if r.status_code == 400:
                if _is_oversized_notebook_error(r):
                    raise DbxExportError("payload_too_large", last_text, 400)
                if "UNSUPPORTED" in last_text.upper() or "FORMAT" in last_text.upper():
                    raise DbxExportError("unsupported_export_format", last_text, 400)
                raise DbxExportError("bad_request", last_text, 400)
            if r.status_code == 404:
                raise DbxExportError("not_found", last_text, 404)
            if r.status_code in (429, 500, 502, 503, 504):
                if attempt < MAX_EXPORT_RETRIES:
                    time.sleep(EXPORT_BACKOFF_SEC * attempt)
                continue
            raise DbxExportError("export_failed", f"HTTP {r.status_code}: {last_text}", r.status_code)
        raise DbxExportError("max_retries_exceeded",
                             f"workspace/export retried {MAX_EXPORT_RETRIES} times, "
                             f"last HTTP {last_status}: {last_text}", last_status)

    def _workspace_files_raw(self, api_path: str) -> bytes:
        """Try raw workspace-files routes for an oversized notebook.

        Databricks workspace metadata identifies notebooks without a filename
        extension, while the workspace-files download route can require an
        explicit representation extension.  In particular, a notebook at
        ``/Users/u/model`` may be downloadable only as
        ``/workspace-files/Users/u/model.ipynb``.  Try the metadata path first
        (it is valid for ordinary workspace files), then the lossless IPYNB
        representation.  Both operations are read-only.
        """
        candidates = [api_path]
        if not api_path.lower().endswith(".ipynb"):
            candidates.append(api_path + ".ipynb")

        failures: List[str] = []
        last_status = 0
        for candidate in candidates:
            encoded_path = urllib.parse.quote(candidate, safe="/")
            url = f"{self.config.base_url('2.0')}/workspace-files{encoded_path}"
            try:
                response = requests.get(
                    url, stream=True, **self.config.http_kwargs(), timeout=120,
                )
            except requests.exceptions.RequestException as exc:
                failures.append(f"{candidate}: network error: {exc}")
                continue
            last_status = response.status_code
            if response.status_code != 200:
                detail = response.text[:500] if getattr(response, "text", "") else ""
                failures.append(f"{candidate}: HTTP {response.status_code}: {detail}")
                continue

            chunks = []
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if chunk:
                    chunks.append(chunk)
            content = b"".join(chunks)
            if content:
                return content
            failures.append(f"{candidate}: empty HTTP 200 response")
            last_status = 200

        raise DbxExportError(
            "workspace_files_failed",
            " | ".join(failures),
            last_status,
        )

    @staticmethod
    def _is_ipynb_document(content: bytes) -> bool:
        try:
            notebook = json.loads(content.decode("utf-8"))
        except (UnicodeDecodeError, ValueError, TypeError):
            return False
        return (
            isinstance(notebook, dict)
            and isinstance(notebook.get("cells"), list)
            and isinstance(notebook.get("nbformat"), int)
        )

    def export_notebook(self, dbx_path: str) -> ExportedFile:
        """Export a workspace path. Returns ExportedFile.

        Detection rules:
          1. Try `format=JUPYTER` — DBX server-side converts SOURCE-stored
             notebooks for us and gives us the .ipynb directly. We trust
             that output (we're an exporter, not an executor; DBX owns its
             own SOURCE→JUPYTER conversion). If JUPYTER fails with bad-
             format/unsupported, the target isn't a notebook → step 2.
          2. Try `format=SOURCE`. If the bytes carry a Databricks notebook
             header, it's a notebook stored as .py (convert ourselves —
             validation still applies HERE because we did the conversion).
             Otherwise it's a plain utility — return the bytes as-is,
             preserving the .py / .scala extension.
          3. For an explicit notebook-size error, retry JUPYTER and SOURCE
             with ``direct_download=true``, then try the raw workspace-files
             route. Accept fallback bytes only after IPYNB structure or
             SOURCE-to-IPYNB cell-fidelity validation succeeds.

        Post-upload fingerprint validation in Phase 2 still catches upload
        corruption regardless of source.
        """
        api_path = _export_api_path(dbx_path)
        source_ext = os.path.splitext(api_path)[1].lower()
        empty_source_exts = {".py", ".scala", ".r", ".sql"}
        allow_empty_source = source_ext in empty_source_exts

        # Step 1: try JUPYTER. On an explicit size-limit response, retry with
        # direct_download=true so the bytes do not have to fit in the base64
        # JSON response envelope.
        oversized_errors: List[str] = []
        oversized_recovery = False
        source_bytes: Optional[bytes] = None
        source_method = "workspace_source"
        try:
            ipynb_bytes = self._workspace_export_raw(
                api_path, DBX_EXPORT_FORMAT, allow_empty=allow_empty_source,
            )
            if not ipynb_bytes and allow_empty_source:
                return ExportedFile(
                    content=b"", is_notebook=False, extension=source_ext,
                    export_method="workspace_jupyter",
                )
            if self._is_unsplit_notebook(ipynb_bytes):
                raise DbxExportError(
                    "unsplit_source_export",
                    f"{api_path}: exported notebook is a single cell still containing "
                    f"'COMMAND ----------' separators — SOURCE was not split into cells",
                    0,
                )
            return ExportedFile(
                content=ipynb_bytes,
                is_notebook=True,
                extension=".ipynb",
                export_method="workspace_jupyter",
            )
        except DbxExportError as e:
            if e.reason == "payload_too_large":
                oversized_recovery = True
                oversized_errors.append(f"JUPYTER encoded: {e.detail}")
                try:
                    direct_ipynb = self._workspace_export_raw(
                        api_path,
                        DBX_EXPORT_FORMAT,
                        allow_empty=allow_empty_source,
                        direct_download=True,
                    )
                    if not direct_ipynb and allow_empty_source:
                        return ExportedFile(
                            content=b"", is_notebook=False, extension=source_ext,
                            export_method="workspace_jupyter_direct",
                        )
                    if self._is_ipynb_document(direct_ipynb):
                        if self._is_unsplit_notebook(direct_ipynb):
                            raise DbxExportError(
                                "unsplit_source_export",
                                f"{api_path}: direct JUPYTER export remained unsplit",
                                0,
                            )
                        return ExportedFile(
                            content=direct_ipynb,
                            is_notebook=True,
                            extension=".ipynb",
                            export_method="workspace_jupyter_direct",
                        )
                    if self._is_dbx_notebook_source(direct_ipynb):
                        source_bytes = direct_ipynb
                        source_method = "workspace_jupyter_direct_source"
                    else:
                        oversized_errors.append(
                            "JUPYTER direct: response was neither IPYNB nor "
                            "Databricks SOURCE"
                        )
                except DbxExportError as direct_error:
                    oversized_errors.append(
                        f"JUPYTER direct: {direct_error.reason}: {direct_error.detail}"
                    )
            # Bad format / unsupported → not a notebook, fall through to SOURCE.
            # Also fall through on an unsplit JUPYTER result: the SOURCE path
            # below (with content-based language detection) can split it
            # correctly; if it still comes out unsplit there, that raise
            # propagates as a real failure.
            if e.reason != "payload_too_large" and e.reason not in (
                "bad_request", "unsupported_export_format", "unsplit_source_export"
            ):
                raise

        # Step 2: try SOURCE; treat as utility unless Databricks header is present.
        if source_bytes is None:
            try:
                source_bytes = self._workspace_export_raw(
                    api_path,
                    "SOURCE",
                    allow_empty=allow_empty_source,
                    direct_download=oversized_recovery,
                )
                if oversized_recovery:
                    source_method = "workspace_source_direct"
            except DbxExportError as source_error:
                if source_error.reason == "payload_too_large" and not oversized_recovery:
                    oversized_recovery = True
                    oversized_errors.append(f"SOURCE encoded: {source_error.detail}")
                    try:
                        source_bytes = self._workspace_export_raw(
                            api_path,
                            "SOURCE",
                            allow_empty=allow_empty_source,
                            direct_download=True,
                        )
                        source_method = "workspace_source_direct"
                    except DbxExportError as direct_source_error:
                        oversized_errors.append(
                            "SOURCE direct: "
                            f"{direct_source_error.reason}: {direct_source_error.detail}"
                        )
                elif oversized_recovery:
                    oversized_errors.append(
                        f"SOURCE direct: {source_error.reason}: {source_error.detail}"
                    )
                else:
                    raise

        # Final read-only fallback through the raw workspace-files route. This
        # never clears outputs or modifies the source workspace.
        if source_bytes is None and oversized_recovery:
            try:
                source_bytes = self._workspace_files_raw(api_path)
                source_method = "workspace_files_raw"
            except DbxExportError as raw_error:
                oversized_errors.append(
                    f"workspace-files raw: {raw_error.reason}: {raw_error.detail}"
                )
                raise DbxExportError(
                    "source_platform_size_limit",
                    f"{api_path}: all read-only oversized-notebook fallbacks failed; "
                    + " | ".join(oversized_errors),
                    raw_error.status or 400,
                ) from raw_error

        if source_bytes is None:
            raise DbxExportError(
                "empty_export", f"no source bytes returned for {api_path}", 200
            )

        # The raw route can return an IPYNB object directly.
        if self._is_ipynb_document(source_bytes):
            if self._is_unsplit_notebook(source_bytes):
                raise DbxExportError(
                    "unsplit_source_export",
                    f"{api_path}: raw oversized fallback remained unsplit",
                    0,
                )
            return ExportedFile(
                content=source_bytes,
                is_notebook=True,
                extension=".ipynb",
                export_method=source_method,
            )
        if self._is_dbx_notebook_source(source_bytes):
            # Notebook stored as .py/.scala — we do the conversion, so validate it.
            # Determine language from the SOURCE header, NOT the path: a workspace
            # notebook path has no extension, so api_path.endswith(".scala") was
            # always False and Scala notebooks were parsed with the Python cell
            # separator ('# COMMAND' vs the actual '// COMMAND') → everything
            # collapsed into ONE cell. The header tells us the language:
            #   "// Databricks notebook source"  -> scala (// COMMAND / // MAGIC)
            #   "-- Databricks notebook source"  -> sql
            #   "#  Databricks notebook source"  -> python / R
            _head = source_bytes[:256].decode("utf-8", errors="replace").lstrip()
            if _head.startswith("//"):
                lang = "scala"
            elif _head.startswith("--"):
                lang = "sql"
            elif api_path.lower().endswith(".scala"):
                lang = "scala"
            else:
                lang = "python"
            converted = self._dbx_source_to_ipynb(source_bytes, language=lang)
            ok, msg = self._validate_conversion(source_bytes, converted, language=lang)
            if not ok:
                raise DbxExportError("conversion_validation_failed", msg, 0)
            if self._is_unsplit_notebook(converted):
                raise DbxExportError(
                    "unsplit_source_export",
                    f"{api_path}: SOURCE→ipynb conversion (lang={lang}) produced a single "
                    f"cell still containing 'COMMAND ----------' separators",
                    0,
                )
            return ExportedFile(
                content=converted,
                is_notebook=True,
                extension=".ipynb",
                export_method=source_method,
            )
        if oversized_recovery:
            raise DbxExportError(
                "oversized_fallback_invalid",
                f"{api_path}: fallback bytes were neither a valid IPYNB document nor "
                "Databricks SOURCE; refusing an unverifiable migration",
                0,
            )
        # Plain utility — preserve as-is.
        ext = source_ext if source_ext in empty_source_exts else ".py"
        return ExportedFile(
            content=source_bytes,
            is_notebook=False,
            extension=ext,
            export_method=source_method,
        )

    def get_workspace_status(self, dbx_path: str) -> Optional[Dict[str, Any]]:
        """Return workspace object metadata, retrying transient failures.

        ``None`` means the metadata could not be established; callers retain
        their existing export behaviour rather than guessing the object type.
        """
        api_path = _export_api_path(dbx_path)
        url = f"{self.config.base_url('2.0')}/workspace/get-status"
        for attempt in range(1, MAX_EXPORT_RETRIES + 1):
            try:
                response = requests.get(
                    url, params={"path": api_path}, **self.config.http_kwargs(),
                    timeout=60,
                )
            except requests.exceptions.RequestException as exc:
                if attempt == MAX_EXPORT_RETRIES:
                    print(
                        f"[dbx_export] WARN: get_workspace_status({dbx_path}) "
                        f"network error: {exc}"
                    )
                    return None
                time.sleep(EXPORT_BACKOFF_SEC * attempt)
                continue
            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError:
                    return None
            if response.status_code == 404:
                return None
            if response.status_code in (429, 500, 502, 503, 504):
                if attempt < MAX_EXPORT_RETRIES:
                    time.sleep(EXPORT_BACKOFF_SEC * attempt)
                continue
            print(
                f"[dbx_export] WARN: get_workspace_status({dbx_path}) HTTP "
                f"{response.status_code}: {response.text[:200]}"
            )
            return None
        return None

    # ─── Git-backed notebooks (via GitHub proxy) ──────────────────────
    #
    # Databricks notebooks stored in GitHub live as `.py` files in the
    # Databricks SOURCE format (cells separated by `# COMMAND ----------`,
    # markdown cells start with `# MAGIC %md`). AIDP only supports `.ipynb`,
    # so we convert SOURCE -> Jupyter after fetching.

    @staticmethod
    def _is_dbx_notebook_source(content: bytes) -> bool:
        """Detect whether `.py` / `.scala` bytes are a Databricks notebook
        SOURCE (cells separated by `# COMMAND ----------`, MAGIC lines) vs a
        plain utility module.

        Databricks always prepends one of these as the very first non-blank
        line of a notebook stored in SOURCE format:
            # Databricks notebook source   (python / R)
            // Databricks notebook source  (scala)
            -- Databricks notebook source  (sql)

        Plain Python / Scala / SQL utility files never have that header.
        """
        head = content[:1024].decode("utf-8", errors="replace")
        for line in head.split("\n"):
            if line.strip():
                s = line.strip()
                return s.startswith("# Databricks notebook source") or \
                       s.startswith("// Databricks notebook source") or \
                       s.startswith("-- Databricks notebook source")
        return False

    @staticmethod
    def _is_unsplit_notebook(ipynb_bytes: bytes) -> bool:
        """Fidelity safety net: True if the .ipynb is a SINGLE code cell that
        still contains Databricks `COMMAND ----------` cell separators as text.

        A correctly-formed notebook never carries those markers inside a cell —
        they only survive when a SOURCE-format export was wrapped whole into one
        cell instead of being split (e.g. when the cell language was misdetected
        so the wrong separator was used). Language-independent; guards both the
        JUPYTER and SOURCE export paths.
        """
        import json as _json
        try:
            nb = _json.loads(ipynb_bytes.decode("utf-8", errors="replace"))
        except Exception:
            return False
        cells = nb.get("cells", [])
        if len(cells) != 1 or cells[0].get("cell_type") != "code":
            return False
        s = cells[0].get("source", "")
        src = "".join(s) if isinstance(s, list) else (s or "")
        return "COMMAND ----------" in src

    @staticmethod
    def _parse_dbx_source_cells(content: bytes, language: str = "python") -> List[Tuple[str, str]]:
        """Parse Databricks SOURCE format into (cell_type, normalized_text) tuples.

        Cell type rule: a cell is MARKDOWN only if it's a `%md` magic — i.e.
        every non-blank line starts with the magic prefix AND the first such
        line begins with `%md`. Other `# MAGIC` magics (%run, %sh, %fs, etc.)
        produce CODE cells with the magic preserved in the source (matching
        what DBX's JUPYTER server-side export does).
        """
        text = content.decode("utf-8", errors="replace")
        text = text.replace("# Databricks notebook source", "")
        text = text.replace("// Databricks notebook source", "")
        text = text.replace("-- Databricks notebook source", "")
        text = text.strip()
        if language == "scala":
            sep, magic_prefix = "// COMMAND ----------", "// MAGIC"
        elif language == "sql":
            sep, magic_prefix = "-- COMMAND ----------", "-- MAGIC"
        else:
            sep, magic_prefix = "# COMMAND ----------", "# MAGIC"
        # Databricks UI cell-title marker. The SOURCE format includes it as
        # the first line of a cell (e.g. `# DBTITLE 1,My Title`). The JUPYTER
        # server-side export strips it (the title lives in cell metadata, not
        # source). For cross-validation we must do the same so the two sides
        # agree. Pattern: "# DBTITLE <number>,<title-text>" — works for both
        # python (# DBTITLE) and scala (// DBTITLE).
        dbtitle_re = re.compile(
            r"^(?:#|//)\s*DBTITLE\s+\d+\s*,.*$"
        )
        cells: List[Tuple[str, str]] = []
        for block in (b.strip() for b in text.split(sep)):
            if not block:
                continue
            lines = block.splitlines()
            # Strip any leading DBTITLE marker line(s).
            while lines and dbtitle_re.match(lines[0]):
                lines = lines[1:]
            # The block may have been ONLY a DBTITLE — skip it.
            if not lines:
                continue
            all_magic_or_blank = all(l.startswith(magic_prefix) or not l.strip() for l in lines)
            is_md = False
            if all_magic_or_blank:
                # Look at the first MAGIC line to decide: %md → markdown, else → code.
                for l in lines:
                    if l.startswith(magic_prefix):
                        after = l[len(magic_prefix):].lstrip()
                        is_md = after.startswith("%md")
                        break
            if is_md:
                md_lines = []
                for line in lines:
                    if line.startswith(f"{magic_prefix} %md"):
                        rest = line[len(f"{magic_prefix} %md"):].strip()
                        if rest:
                            md_lines.append(rest)
                    elif line.startswith(magic_prefix):
                        md_lines.append(line[len(magic_prefix):].lstrip())
                    else:
                        md_lines.append(line)
                cells.append(("markdown", "\n".join(md_lines).rstrip()))
            else:
                cleaned = []
                for line in lines:
                    if line.startswith(magic_prefix):
                        # SOURCE adds one separator space after MAGIC. All
                        # remaining whitespace belongs to the customer's code
                        # (REF-114: lstrip flattened a conditional Python exit).
                        cleaned.append(line[len(magic_prefix):].removeprefix(" "))
                    else:
                        cleaned.append(line)
                cells.append(("code", "\n".join(cleaned).rstrip()))
        return cells

    @staticmethod
    def _count_source_cells(content: bytes, language: str = "python") -> int:
        return len(DbxClient._parse_dbx_source_cells(content, language))

    @staticmethod
    def _validate_conversion(
        source_bytes: bytes,
        ipynb_bytes: bytes,
        language: str = "python",
    ) -> Tuple[bool, str]:
        """Compare parsed source cells against converted .ipynb cells.

        Checks (in order):
          1. Same cell count (after trimming trailing empty cells, which DBX's
             JUPYTER export sometimes adds as a cosmetic artifact but SOURCE
             doesn't — same notebook content either way).
          2. Same cell_type at each index (code vs markdown)
          3. Same normalized cell text at each index

        Returns (ok, detail). detail is "OK" on success, or a one-line summary
        of the first mismatch on failure.
        """
        import json as _json
        src_cells = DbxClient._parse_dbx_source_cells(source_bytes, language)
        try:
            nb = _json.loads(ipynb_bytes.decode("utf-8"))
        except Exception as e:
            return False, f"converted .ipynb is not valid JSON: {e}"
        ipynb_cells = nb.get("cells", [])

        # Drop trailing empty cells from both sides — purely cosmetic.
        def _ipynb_text(c):
            s = c.get("source", "")
            return ("".join(s) if isinstance(s, list) else (s or "")).strip()
        while src_cells and not src_cells[-1][1].strip():
            src_cells = src_cells[:-1]
        while ipynb_cells and not _ipynb_text(ipynb_cells[-1]):
            ipynb_cells = ipynb_cells[:-1]

        # Normalize whitespace for content comparison:
        #   - per-line trailing whitespace stripped (DBX's JUPYTER vs SOURCE
        #     exports sometimes disagree by a single trailing space — cosmetic)
        #   - leading/trailing blank lines removed
        #   - internal indentation preserved (semantically significant)
        def _norm_blank_lines(s: str) -> str:
            lines = [l.rstrip() for l in s.split("\n")]
            i, j = 0, len(lines)
            while i < j and not lines[i]:
                i += 1
            while j > i and not lines[j - 1]:
                j -= 1
            return "\n".join(lines[i:j])

        if len(src_cells) != len(ipynb_cells):
            return False, (
                f"cell count differs: source={len(src_cells)}, ipynb={len(ipynb_cells)}"
            )
        for i, ((src_type, src_text), cell) in enumerate(zip(src_cells, ipynb_cells)):
            ipynb_type = cell.get("cell_type")
            ipynb_text = "".join(cell.get("source", [])) if isinstance(cell.get("source"), list) else (cell.get("source") or "")
            if src_type != ipynb_type:
                return False, (
                    f"cell {i}: type mismatch source={src_type} vs ipynb={ipynb_type} "
                    f"(src preview: {src_text[:80]!r})"
                )
            src_n   = _norm_blank_lines(src_text)
            ipynb_n = _norm_blank_lines(ipynb_text)
            if src_n != ipynb_n:
                return False, (
                    f"cell {i}: source text mismatch — "
                    f"src({len(src_n)}b)={src_n[:80]!r} vs "
                    f"ipynb({len(ipynb_n)}b)={ipynb_n[:80]!r}"
                )
        return True, "OK"

    @staticmethod
    def _dbx_source_to_ipynb(content: bytes, language: str = "python") -> bytes:
        """Convert Databricks SOURCE-format .py/.scala bytes into .ipynb bytes.

        Delegates parsing to `_parse_dbx_source_cells` so the converter and
        the cross-validator share the same classification logic — fixing a
        parser bug in one place automatically fixes it in both.
        """
        import json as _json
        parsed = DbxClient._parse_dbx_source_cells(content, language=language)
        cells = []
        for i, (cell_type, src) in enumerate(parsed):
            cell = {
                "id": f"cell-{i}",
                "metadata": {},
                "source": list((src + "\n").splitlines(keepends=True)),
            }
            if cell_type == "markdown":
                cell["cell_type"] = "markdown"
            else:
                cell["cell_type"] = "code"
                cell["execution_count"] = None
                cell["outputs"] = []
            cells.append(cell)
        nb = {
            "cells": cells,
            "metadata": {
                "kernelspec": {"name": "python3", "display_name": "Python 3"},
                "language_info": {"name": language},
            },
            "nbformat": 4,
            "nbformat_minor": 5,
        }
        return _json.dumps(nb).encode("utf-8")

    @staticmethod
    def parse_github_owner_repo(git_url: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract (owner, repo) from a GitHub clone URL like
        https://github.com/owner/repo.git . Returns (None, None) on no match."""
        import re
        u = (git_url or "").strip().rstrip("/")
        if u.endswith(".git"):
            u = u[:-4]
        m = re.match(r"https?://github\.com/([^/]+)/([^/]+)", u)
        if m:
            return m.group(1), m.group(2)
        return None, None

    def _github_dir_cache(self):
        # Per-client cache so repeated lookups don't re-hit the proxy.
        if not hasattr(self, "_gh_dir_cache"):
            self._gh_dir_cache = {}
        return self._gh_dir_cache

    def _github_dir_entries_cache(self):
        if not hasattr(self, "_gh_dir_entries_cache"):
            self._gh_dir_entries_cache = {}
        return self._gh_dir_entries_cache

    def list_github_dir_entries(
        self, owner: str, repo: str, dir_path: str, branch: str
    ) -> List[Dict[str, Any]]:
        """Return raw directory entries without discarding names or types."""
        base = self.config.github_proxy_base()
        if not base:
            raise DbxExportError(
                "no_github_proxy", "github proxy required for git-source notebooks", 0
            )
        cache = self._github_dir_entries_cache()
        key = f"{owner}/{repo}@{branch}:{dir_path}"
        if key in cache:
            return cache[key]
        encoded = urllib.parse.quote(dir_path, safe="/")
        url = f"{base}/repos/{owner}/{repo}/contents/{encoded}?ref={urllib.parse.quote(branch)}"
        try:
            r = self._github_get_with_retry(
                url,
                accept="application/vnd.github.v3+json",
                operation=f"list_github_dir_entries({key})",
                timeout=60,
            )
        except requests.exceptions.RequestException as e:
            raise DbxExportError("github_list_failed", f"{e}", 0)
        if r.status_code == 404:
            # A genuine 404 is an absent/empty dir — safe to report empty and
            # cache so we don't re-request it.
            print(
                f"[dbx_export] WARN: list_github_dir_entries({key}) HTTP 404: "
                f"{r.text[:200]} — treating dir as empty (path lookups may miss)"
            )
            cache[key] = []
            return []
        if r.status_code != 200:
            # Anything not 200/404 is NOT an absent dir: either a transient
            # status that survived _github_get_with_retry's attempts (proxy/GitHub
            # outage) or a consistent failure like 401/403 (auth). Raise loudly
            # and do NOT cache — caching would make it look like an empty
            # directory and poison every later lookup of the same key (cache hit
            # above), silently dropping notebooks; and we don't retry a
            # consistently-failing status.
            raise DbxExportError(
                "github_list_failed",
                f"HTTP {r.status_code} listing {key} (transient statuses tried up to "
                f"{max(1, int(self.config.proxy_max_retries))}x): {r.text[:200]}",
                r.status_code,
            )
        entries = r.json()
        if not isinstance(entries, list):
            print(
                f"[dbx_export] WARN: list_github_dir_entries({key}) returned "
                "non-list body — treating dir as empty"
            )
            cache[key] = []
            return []
        cache[key] = entries
        return entries

    def list_github_dir(self, owner: str, repo: str, dir_path: str, branch: str) -> Dict[str, str]:
        """List a GitHub directory via the proxy. Returns {basename-no-ext: actual-filename}.

        Used to resolve notebook paths that don't carry a file extension —
        the DBX job spec stores them that way for git-backed tasks, but
        github needs the full filename to fetch."""
        cache = self._github_dir_cache()
        key = f"{owner}/{repo}@{branch}:{dir_path}"
        if key in cache:
            return cache[key]
        entries = self.list_github_dir_entries(owner, repo, dir_path, branch)
        lookup: Dict[str, str] = {}
        stem_priority: Dict[str, int] = {}
        preferred = {".ipynb": 0, ".py": 1, ".scala": 2, ".sql": 3, ".r": 4}
        for e in entries:
            name = e.get("name", "")
            if not name:
                continue
            # Preserve every exact filename so same-stem siblings survive.
            lookup[name] = name
            stem = name.rsplit(".", 1)[0] if "." in name else name
            extension = posixpath.splitext(name)[1].lower()
            priority = preferred.get(extension, 100)
            if stem not in lookup or priority < stem_priority.get(stem, 101):
                lookup[stem] = name
                stem_priority[stem] = priority
        cache[key] = lookup
        return lookup

    def export_notebook_from_git(self, repo_path: str, owner: str, repo: str, branch: str) -> bytes:
        """Fetch a notebook's raw bytes from a GitHub repo via the proxy.

        repo_path is a path relative to the repo root, with or without a file
        extension. If the extension is missing, we list the parent directory
        on GitHub and pick the file whose stem matches.
        """
        base = self.config.github_proxy_base()
        if not base:
            raise DbxExportError("no_github_proxy", "github proxy required for git-source notebooks", 0)

        path = repo_path.lstrip("/")
        if "/" in path:
            parent, base_name = path.rsplit("/", 1)
        else:
            parent, base_name = "", path
        trimmed_parent = parent.strip()
        # Preserve existing Git names. A different parent is attempted only
        # after the exact directory/filename resolution or raw fetch is 404.
        parents = (parent,) if parent == trimmed_parent else (parent, trimmed_parent)
        for current_parent in parents:
            path = f"{current_parent}/{base_name}" if current_parent else base_name
            try:
                # Resolve the extension in this candidate's directory.
                has_ext = path.lower().endswith((".ipynb", ".py", ".scala", ".sql", ".r"))
                if not has_ext:
                    lookup = self.list_github_dir(owner, repo, current_parent, branch)
                    actual = lookup.get(base_name)
                    if not actual:
                        # Try common fallback: .ipynb then .py.
                        for ext in (".ipynb", ".py"):
                            if base_name + ext in lookup.values():
                                actual = base_name + ext
                                break
                    if not actual:
                        raise DbxExportError(
                            "git_path_unresolved",
                            f"could not resolve {repo_path!r} in {owner}/{repo}@{branch} "
                            f"(parent dir contained: {sorted(lookup.values())[:10]})",
                            404,
                        )
                    path = f"{current_parent}/{actual}" if current_parent else actual

                encoded = urllib.parse.quote(path, safe="/")
                url = f"{base}/repos/{owner}/{repo}/contents/{encoded}?ref={urllib.parse.quote(branch)}"
                try:
                    r = self._github_get_with_retry(
                        url,
                        accept="application/vnd.github.v3.raw",
                        operation=f"export_notebook_from_git({owner}/{repo}:{path})",
                        timeout=120,
                    )
                except requests.exceptions.RequestException as e:
                    raise DbxExportError("github_fetch_failed", f"{e}", 0)
                if r.status_code == 404:
                    raise DbxExportError("git_not_found", f"{path} not in {owner}/{repo}@{branch}", 404)
                if r.status_code != 200:
                    raise DbxExportError("github_fetch_failed",
                                         f"HTTP {r.status_code}: {r.text[:300]}", r.status_code)
            except DbxExportError as error:
                if error.status != 404 or current_parent == trimmed_parent:
                    raise
                print(
                    f"[dbx_export][git-parent-trim] {owner}/{repo}@{branch}: "
                    f"exact path {repo_path!r} returned 404 ({error.reason}); "
                    f"retrying parent {parent!r} -> {trimmed_parent!r}"
                )
                continue
            break

        # Decide notebook vs utility based on the actual file content:
        #   - .ipynb: always a notebook (JSON)
        #   - .py / .scala with "# Databricks notebook source" header: notebook
        #     stored as source → convert + validate → upload as .ipynb
        #   - .py / .scala without that header: plain utility module → upload
        #     as-is preserving the extension
        low = path.lower()
        if low.endswith(".ipynb"):
            return ExportedFile(content=r.content, is_notebook=True, extension=".ipynb", resolved_git_path=path)
        if low.endswith((".py", ".scala")):
            lang = "scala" if low.endswith(".scala") else "python"
            ext = ".scala" if low.endswith(".scala") else ".py"
            if not self._is_dbx_notebook_source(r.content):
                return ExportedFile(content=r.content, is_notebook=False, extension=ext, resolved_git_path=path)
            converted = self._dbx_source_to_ipynb(r.content, language=lang)
            ok, msg = self._validate_conversion(r.content, converted, language=lang)
            if not ok:
                raise DbxExportError("conversion_validation_failed", msg, 0)
            return ExportedFile(content=converted, is_notebook=True, extension=".ipynb", resolved_git_path=path)
        # .sql / .r / other extensions: upload as-is preserving extension.
        # Caller's fingerprint check is skipped for non-notebooks.
        last_ext = "." + path.rsplit(".", 1)[-1] if "." in path.rsplit("/", 1)[-1] else ""
        return ExportedFile(content=r.content, is_notebook=False, extension=last_ext, resolved_git_path=path)

    def export_resource_from_git(
        self, repo_path: str, owner: str, repo: str, branch: str
    ) -> ExportedFile:
        """Fetch a repository resource by its exact path.

        Resource discovery already obtains the real filename from the GitHub
        directory listing.  Do not run that filename through the notebook
        extension resolver: its lookup is keyed by filename stem, so a path
        such as ``configs/default.yaml`` would be looked up as the literal
        stem ``default.yaml`` and incorrectly fail with
        ``git_path_unresolved``.
        """
        base = self.config.github_proxy_base()
        if not base:
            raise DbxExportError(
                "no_github_proxy", "github proxy required for git resources", 0
            )

        path = repo_path.lstrip("/")
        extension = posixpath.splitext(posixpath.basename(path))[1]
        if not extension:
            raise DbxExportError(
                "git_resource_extension_missing",
                f"resource path has no extension: {repo_path!r}",
                0,
            )

        encoded = urllib.parse.quote(path, safe="/")
        url = (
            f"{base}/repos/{owner}/{repo}/contents/{encoded}"
            f"?ref={urllib.parse.quote(branch)}"
        )
        try:
            r = self._github_get_with_retry(
                url,
                accept="application/vnd.github.v3.raw",
                operation=f"export_resource_from_git({owner}/{repo}:{path})",
                timeout=120,
            )
        except requests.exceptions.RequestException as e:
            raise DbxExportError("github_fetch_failed", f"{e}", 0)
        if r.status_code == 404:
            raise DbxExportError(
                "git_not_found", f"{path} not in {owner}/{repo}@{branch}", 404
            )
        if r.status_code != 200:
            raise DbxExportError(
                "github_fetch_failed",
                f"HTTP {r.status_code}: {r.text[:300]}",
                r.status_code,
            )
        return ExportedFile(
            content=r.content,
            is_notebook=False,
            extension=extension,
            resolved_git_path=path,
        )

    # ─── Workspace file (binary) — for JARs in /Workspace/ ────────────

    # ─── Fix 7i (2026-07-25): workspace folder listing ────────────────
    def list_workspace_folder(self, dbx_path: str) -> List[Dict[str, Any]]:
        """List items in a Databricks workspace folder.

        Returns a list of dicts with `path` and `object_type` (NOTEBOOK, FILE,
        DIRECTORY, REPO, LIBRARY, ...). Empty list if the folder doesn't exist
        or is inaccessible — the caller can no-op cleanly.

        Used by the dependency resolver to discover SIBLING .py module files
        that live next to a notebook (e.g. HelperFunctions/constants.py which
        HelperFunctions.ipynb star-imports). Those files aren't reachable via
        %run scanning, so we list the notebook's parent folder and export any
        .py we find. Cheap: one HTTP call per unique folder.
        """
        api_path = _export_api_path(dbx_path)
        url = f"{self.config.base_url('2.0')}/workspace/list"
        try:
            r = self._get_workspace_file_with_retry(
                url,
                retry_transient=True,
                operation=f"workspace folder listing {dbx_path}",
                params={"path": api_path},
                **self.config.http_kwargs(),
                timeout=60,
            )
        except requests.exceptions.RequestException as e:
            # A network hiccup here shouldn't abort the whole export — the
            # notebook itself already exported successfully. Log and return
            # empty so processing continues.
            print(f"[dbx_export] WARN: list_workspace_folder({dbx_path}) network "
                  f"error: {e} — treating folder as empty")
            return []
        if r.status_code == 404:
            # Folder doesn't exist (perhaps the notebook's parent path doesn't
            # exist as a listable folder — happens for /Repos entries under
            # certain workspace layouts). Not an error for our purposes.
            return []
        if r.status_code in (429, 500, 502, 503, 504):
            raise DbxExportError(
                "workspace_list_retries_exhausted",
                f"list_workspace_folder({dbx_path}) exhausted retries; "
                f"HTTP {r.status_code}: {r.text[:300]}",
                r.status_code,
            )
        if r.status_code != 200:
            print(f"[dbx_export] WARN: list_workspace_folder({dbx_path}) HTTP "
                  f"{r.status_code}: {r.text[:200]} — treating as empty")
            return []
        try:
            body = r.json()
        except ValueError:
            print(f"[dbx_export] WARN: list_workspace_folder({dbx_path}) non-JSON "
                  f"body — treating as empty")
            return []
        objects = body.get("objects", []) or []
        # Normalise: each entry MUST have `path` and `object_type`; drop others
        # so callers can't accidentally read stale fields.
        return [
            {"path": o.get("path", ""), "object_type": o.get("object_type", "")}
            for o in objects
            if o.get("path")
        ]


    def export_workspace_file(self, dbx_path: str) -> bytes:
        """Export a non-notebook file from the DBX workspace (jar, sh, etc.).

        Same path-normalization as export_notebook. Tries workspace/export
        ?format=SOURCE first (base64 in JSON body, capped at ~10 MB). On 400
        PAYLOAD_TOO_LARGE, falls back to the newer workspace-files API which
        streams raw bytes.
        """
        api_path = _export_api_path(dbx_path)
        retry_transient = not dbx_path.lower().endswith(".jar")
        # Path 1: workspace/export (works for small files)
        url1 = f"{self.config.base_url('2.0')}/workspace/export"
        r = self._get_workspace_file_with_retry(
            url1,
            retry_transient=retry_transient,
            params={"path": api_path, "format": "SOURCE"},
            **self.config.http_kwargs(),
        )
        if r.status_code == 200:
            content = r.json().get("content", "")
            return base64.b64decode(content)
        if r.status_code == 400 and ("TOO_LARGE" in r.text.upper() or "PAYLOAD" in r.text.upper()):
            # Fall through to streaming endpoint.
            pass
        elif r.status_code == 404:
            raise DbxExportError("not_found", r.text[:300], 404)
        elif r.status_code != 400:
            raise DbxExportError(
                "workspace_export_failed", f"HTTP {r.status_code}: {r.text[:300]}", r.status_code
            )

        # Path 2: workspace-files streaming (newer API)
        url2 = f"{self.config.base_url('2.0')}/workspace-files{api_path}"
        r2 = self._get_workspace_file_with_retry(
            url2,
            retry_transient=retry_transient,
            stream=True,
            **self.config.http_kwargs(),
        )
        if r2.status_code != 200:
            raise DbxExportError(
                "workspace_files_failed",
                f"HTTP {r2.status_code}: {r2.text[:300] if r2.content else ''}",
                r2.status_code,
            )
        chunks = []
        for chunk in r2.iter_content(chunk_size=64 * 1024):
            if chunk:
                chunks.append(chunk)
        return b"".join(chunks)

    def export_workspace_resource(self, dbx_path: str) -> ExportedFile:
        """Export a plain workspace resource while preserving its extension.

        ``export_notebook`` intentionally treats extension-less workspace
        objects as notebooks and historically defaulted every non-Scala
        SOURCE object to ``.py``.  Repository resources discovered from local
        imports (YAML, JSON, TOML, package ``.py`` files, etc.) must instead be
        fetched as raw files and retain their real filename.
        """
        content = self.export_workspace_file(dbx_path)
        basename = posixpath.basename(dbx_path)
        extension = posixpath.splitext(basename)[1]
        return ExportedFile(
            content=content,
            is_notebook=False,
            extension=extension,
        )

    # ─── DBFS read (chunked) ──────────────────────────────────────────

    def get_dbfs_status(self, dbfs_path: str) -> Optional[Dict[str, Any]]:
        url = f"{self.config.base_url('2.0')}/dbfs/get-status"
        r = requests.get(url, params={"path": dbfs_path}, **self.config.http_kwargs())
        if r.status_code != 200:
            return None
        return r.json()

    def read_dbfs_file(self, dbfs_path: str) -> bytes:
        """Read a file from DBFS, chunking at 1 MB (the API hard limit)."""
        status = self.get_dbfs_status(dbfs_path)
        if status is None:
            raise DbxExportError("not_found", f"DBFS not found: {dbfs_path}", 404)
        total = status.get("file_size", 0)
        url = f"{self.config.base_url('2.0')}/dbfs/read"
        out = bytearray()
        offset = 0
        while offset < total:
            length = min(DBFS_CHUNK, total - offset)
            r = requests.get(
                url,
                params={"path": dbfs_path, "offset": offset, "length": length},
                **self.config.http_kwargs(),
            )
            if r.status_code != 200:
                raise DbxExportError(
                    "dbfs_read_failed",
                    f"HTTP {r.status_code} at offset {offset}: {r.text[:200]}",
                    r.status_code,
                )
            content = r.json().get("data", "")
            chunk_bytes = base64.b64decode(content) if content else b""
            if not chunk_bytes:
                break
            out.extend(chunk_bytes)
            offset += len(chunk_bytes)
            if len(chunk_bytes) < length:
                break
        return bytes(out)

    # ─── External (s3/http) library sources via forward proxy ─────────

    def fetch_external_via_proxy(self, uri: str) -> bytes:
        """Fetch an s3:// or http(s):// library source by tunnelling through
        the proxy as a forward HTTP proxy.

        Only meaningful when config.proxy is set — there is no equivalent for
        host+token mode (a direct DBX host has no forward-proxy route to the
        public internet; direct-download callers must reach s3/http
        themselves). Raises DbxExportError if config.fetch_external_via_proxy
        is False, so callers can't silently start making outbound calls
        that weren't opted into.
        """
        if not self.config.fetch_external_via_proxy:
            raise DbxExportError(
                "external_fetch_disabled",
                f"DbxConfig.fetch_external_via_proxy is False — refusing to fetch "
                f"external URI {_scrub_proxy_creds(uri)!r}",
                0,
            )
        proxy_url = self.config.forward_proxy_url()
        if not proxy_url:
            raise DbxExportError(
                "no_forward_proxy", "external fetch requires DbxConfig.proxy to be set", 0
            )

        if uri.startswith("s3://"):
            rest = uri[len("s3://"):]
            bucket, _, key = rest.partition("/")
            if not bucket or not key:
                # Scrub the URI: a malformed source can still carry userinfo.
                raise DbxExportError(
                    "malformed_uri", f"malformed s3 uri {_scrub_proxy_creds(uri)!r}", 0
                )
            url = f"https://{bucket}.s3.amazonaws.com/{key}"
        elif uri.startswith(("http://", "https://")):
            url = uri
        else:
            raise DbxExportError(
                "unsupported_scheme",
                f"cannot proxy-fetch {_scrub_proxy_creds(uri)!r} (not s3/http)",
                0,
            )

        proxies = {"http": proxy_url, "https": proxy_url}
        last_status, last_text = 0, ""
        for attempt in range(1, MAX_EXPORT_RETRIES + 1):
            try:
                r = requests.get(url, proxies=proxies, timeout=600)
            except requests.exceptions.RequestException as e:
                if attempt == MAX_EXPORT_RETRIES:
                    raise DbxExportError(
                        "external_fetch_failed", f"network error: {_scrub_proxy_creds(e)}", 0
                    )
                time.sleep(EXPORT_BACKOFF_SEC * attempt)
                continue
            last_status = r.status_code
            headers = getattr(r, "headers", None) or {}
            content_type = (
                (headers.get("Content-Type") or "").split(";")[0].strip().lower()
            )
            if r.status_code == 200:
                # Size cap — check the declared Content-Length first (when
                # present), then the actual body: a proxy can omit or lie
                # about the header.
                declared_len = None
                raw_len = headers.get("Content-Length")
                if raw_len is not None:
                    try:
                        declared_len = int(raw_len)
                    except (TypeError, ValueError):
                        declared_len = None
                if declared_len is not None and declared_len > MAX_EXTERNAL_FETCH_BYTES:
                    raise DbxExportError(
                        "external_fetch_too_large",
                        f"Content-Length {declared_len} exceeds "
                        f"{MAX_EXTERNAL_FETCH_BYTES}-byte cap for "
                        f"{_scrub_proxy_creds(uri)!r}",
                        200,
                    )
                body = r.content
                if len(body) > MAX_EXTERNAL_FETCH_BYTES:
                    raise DbxExportError(
                        "external_fetch_too_large",
                        f"body of {len(body)} bytes exceeds "
                        f"{MAX_EXTERNAL_FETCH_BYTES}-byte cap for "
                        f"{_scrub_proxy_creds(uri)!r}",
                        200,
                    )
                # A 200 body must actually BE a zip archive: a proxy/edge
                # error page served as text/plain (or with no Content-Type)
                # would otherwise be stored and uploaded as jar bytes and
                # reported VALIDATED — silent artifact corruption. The
                # PK\x03\x04 local-file-header magic covers .jar/.whl/.zip.
                # size >= 100 stays as a cheap floor against a truncated or
                # empty-but-200 response. NOTE: r.text (a full-body decode)
                # is only touched on the rejection path below, never on the
                # success path — multi-hundred-MB jars must not pay a
                # throwaway text decode.
                if (
                    len(body) >= 100
                    and content_type != "text/html"
                    and body.startswith(b"PK\x03\x04")
                ):
                    return body
                last_text = _scrub_proxy_creds(r.text[:300] if r.text else "")
                raise DbxExportError(
                    "not_a_jar",
                    f"HTTP 200 body is not a jar/zip archive "
                    f"(content-type={content_type!r}, {len(body)} bytes, "
                    f"missing PK magic) for {_scrub_proxy_creds(uri)!r}: {last_text}",
                    200,
                )
            last_text = _scrub_proxy_creds(r.text[:300] if r.text else "")
            if r.status_code in (429, 500, 502, 503, 504):
                if attempt < MAX_EXPORT_RETRIES:
                    time.sleep(EXPORT_BACKOFF_SEC * attempt)
                continue
            raise DbxExportError(
                "external_fetch_failed",
                f"HTTP {last_status}, content-type={content_type!r}, "
                f"{len(r.content)} bytes: {last_text}",
                last_status,
            )
        raise DbxExportError(
            "max_retries_exceeded",
            f"external fetch of {_scrub_proxy_creds(uri)!r} retried {MAX_EXPORT_RETRIES} times, "
            f"last HTTP {last_status}: {last_text}",
            last_status,
        )

    # ─── Unified file fetch by URL scheme ─────────────────────────────

    def fetch_file_uri(self, uri: str) -> Tuple[bytes, str]:
        """Fetch a library/init-script file by its DBX URI.

        Recognized schemes:
          dbfs:/...        → DBFS chunked read
          /Workspace/...   → workspace/export or workspace-files
          file:/...        → cluster-local file, NOT fetchable remotely
          s3://...         → S3 — fetched via forward proxy if
                              config.fetch_external_via_proxy is set, else
                              returned as a record-only marker
          http(s)://...    → same as s3 above
        Returns (content_bytes, source_type) where source_type ∈
          {"dbfs", "workspace", "external"}. For "external" without proxy
          fetch enabled, returns (b"", "external").
        """
        if uri.startswith("dbfs:"):
            path = uri[len("dbfs:"):]
            return self.read_dbfs_file(path), "dbfs"
        # Workspace URIs may arrive with or without the /Workspace/ prefix
        # (DBX UI-style vs REST-style). Normalize to canonical REST form
        # before handing to export_workspace_file.
        if uri.startswith("/Workspace/"):
            return self.export_workspace_file(uri[len("/Workspace"):]), "workspace"
        if uri.startswith("Workspace/"):
            return self.export_workspace_file("/" + uri[len("Workspace/"):]), "workspace"
        if uri.startswith(("s3://", "http://", "https://")):
            if self.config.fetch_external_via_proxy:
                return self.fetch_external_via_proxy(uri), "external"
            return b"", "external"
        if uri.startswith("file:"):
            return b"", "external"
        if uri.startswith("/"):
            return self.export_workspace_file(uri), "workspace"
        raise DbxExportError("unknown_scheme", f"Cannot fetch {uri!r}", 0)
