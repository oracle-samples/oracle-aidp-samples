"""AIDP REST API client using OCI SDK authentication.

Uses the same authentication and upload pattern as:
- the AIDP MCP server (make_signer with auto-detection)
- the Rust reference implementation (uploadFileMeta → PAR URL → PUT)

Authentication order (auto-detected):
  1. Resource Principal (OCI_RESOURCE_PRINCIPAL_VERSION env var)
  2. Instance Principal (OCI_INSTANCE_PRINCIPAL env var)
  3. Session Token (security_token_file in ~/.oci/config)
  4. API Key (fallback from ~/.oci/config)

Config from .env:
  AIDP_REGION, AIDP_INSTANCE_ID, AIDP_WORKSPACE_KEY, OCI_PROFILE
"""

import json
import logging
import os
import time
from hashlib import sha256 as _sha256
from pathlib import Path
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

API_VERSION = "20240831"


# ── OCI Authentication ────────────────────────────────────────────────────────


def make_signer(profile: str = "DEFAULT", config_file: str = "~/.oci/config"):
    """Auto-detect best OCI auth: Resource Principal → Instance Principal → Session Token → API Key.

    Same pattern as the AIDP MCP server.
    """
    try:
        import oci
    except ImportError:
        raise ImportError("oci package required for AIDP deploy. Install: pip install oci")

    # 1. Resource Principal (OCI Functions, Kubernetes)
    if os.environ.get("OCI_RESOURCE_PRINCIPAL_VERSION"):
        logger.info("Using Resource Principal authentication")
        return oci.auth.signers.get_resource_principals_signer()

    # 2. Instance Principal (OCI Compute VMs)
    if os.environ.get("OCI_INSTANCE_PRINCIPAL"):
        logger.info("Using Instance Principal authentication")
        return oci.auth.signers.InstancePrincipalsSecurityTokenSigner()

    # 3/4. Config file (Session Token or API Key)
    config = oci.config.from_file(os.path.expanduser(config_file), profile)

    if "security_token_file" in config:
        # Session Token auth
        token_path = os.path.expanduser(config["security_token_file"])
        with open(token_path, encoding="utf-8") as f:
            token = f.read().strip()
        private_key = oci.signer.load_private_key_from_file(config["key_file"])
        logger.info("Using Session Token authentication (profile: %s)", profile)
        return oci.auth.signers.SecurityTokenSigner(token, private_key)
    else:
        # API Key auth
        logger.info("Using API Key authentication (profile: %s)", profile)
        return oci.Signer(
            tenancy=config["tenancy"],
            user=config["user"],
            fingerprint=config["fingerprint"],
            private_key_file_location=config["key_file"],
            pass_phrase=config.get("pass_phrase"),
        )


class _OCIRequestAuth:
    """Requests auth adapter that signs with OCI signer."""

    def __init__(self, signer):
        self._signer = signer

    def __call__(self, prepared_request):
        parsed = urlparse(prepared_request.url)
        method = prepared_request.method.lower()

        now = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
        prepared_request.headers["date"] = now
        prepared_request.headers["host"] = parsed.hostname

        body = prepared_request.body
        if body:
            body_bytes = body if isinstance(body, bytes) else body.encode("utf-8")
            digest = _sha256(body_bytes).digest()
            import base64
            sha256_b64 = base64.b64encode(digest).decode()
            prepared_request.headers["x-content-sha256"] = sha256_b64
            prepared_request.headers["content-length"] = str(len(body_bytes))
            if "content-type" not in prepared_request.headers:
                prepared_request.headers["content-type"] = "application/json"

        self._signer.do_request_sign(prepared_request)
        return prepared_request


# ── AIDP Client ───────────────────────────────────────────────────────────────


class AIDPClient:
    """REST client for Oracle AI Data Platform.

    Uses OCI SDK authentication (same as the AIDP MCP server and the Rust reference implementation).
    """

    def __init__(self, region: str, aidp_instance_id: str, signer,
                 workspace_key: str = ""):
        self.region = region
        self.aidp_instance_id = aidp_instance_id
        self.workspace_key = workspace_key
        self.base_url = (
            f"https://aidp.{region}.oci.oraclecloud.com"
            f"/{API_VERSION}/dataLakes/{aidp_instance_id}"
        )
        self._session = requests.Session()
        self._session.auth = _OCIRequestAuth(signer)

    @classmethod
    def from_config(
        cls,
        profile: str = None,
        config_file: str = "~/.oci/config",
        region: str = "",
        instance_id: str = "",
        workspace_key: str = "",
    ):
        """Create client from explicit values, falling back to .env config /
        environment (AIDP_REGION, AIDP_INSTANCE_ID, AIDP_WORKSPACE_KEY,
        OCI_PROFILE)."""
        from .. import config as cfg

        region = region or cfg.AIDP_REGION or os.environ.get("AIDP_REGION", "")
        instance_id = instance_id or cfg.AIDP_INSTANCE_ID or os.environ.get("AIDP_INSTANCE_ID", "")
        workspace_key = (
            workspace_key or cfg.AIDP_WORKSPACE_KEY or os.environ.get("AIDP_WORKSPACE_KEY", "")
        )
        oci_profile = profile or cfg.OCI_PROFILE or os.environ.get("OCI_PROFILE", "DEFAULT")

        if not region or not instance_id:
            raise RuntimeError(
                "AIDP_REGION and AIDP_INSTANCE_ID required for deploy. "
                "Set in ~/.infa2aidp/.env or environment."
            )

        signer = make_signer(profile=oci_profile, config_file=config_file)
        return cls(region, instance_id, signer, workspace_key)

    # Transient statuses worth one more try: throttling and the 5xx a busy
    # control plane returns (a 503 "Service Unavailable" interrupted a live
    # upload on 2026-09-25 and failed the deployment half way). A POST is
    # retried only on 429/503, where the server did not act: after a
    # 500/502/504 a POST /jobs may already have created the job, and a replay
    # would fail on "already exists" or start a second job run.
    _RETRY_STATUSES = (429, 500, 502, 503, 504)
    _RETRY_STATUSES_POST = (429, 503)

    @staticmethod
    def _retry_delay(resp, attempt: int) -> float:
        """Seconds to wait: Retry-After as seconds or as an HTTP-date (both
        are valid per RFC 9110), else exponential backoff."""
        raw = (resp.headers.get("retry-after") or "").strip()
        if raw:
            try:
                return max(0.0, float(raw))
            except ValueError:
                from email.utils import parsedate_to_datetime
                try:
                    from datetime import datetime, timezone
                    when = parsedate_to_datetime(raw)
                    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())
                except (TypeError, ValueError):
                    pass
        return float(2 ** attempt)

    def _request(self, method: str, path: str, expected: tuple = (), **kwargs) -> requests.Response:
        """Make an authenticated AIDP API request.

        ``expected`` lists non-2xx statuses the caller handles itself (e.g.
        409 from mkdir on an existing folder); they are not logged as errors.
        Throttling and transient 5xx responses are retried with backoff.
        """
        url = f"{self.base_url}{path}"
        retryable = self._RETRY_STATUSES_POST if method.upper() == "POST" else self._RETRY_STATUSES
        for attempt in range(4):
            resp = self._session.request(method, url, **kwargs)
            if resp.status_code not in retryable or attempt == 3:
                break
            delay = self._retry_delay(resp, attempt)
            logger.warning("AIDP API %s %s -> %d, retrying in %.0fs", method, path, resp.status_code, delay)
            time.sleep(min(delay, 30))
        if not resp.ok and resp.status_code not in expected:
            logger.error("AIDP API %s %s → %d: %s", method, path, resp.status_code, resp.text[:200])
        return resp

    # ── Workspace Operations ──────────────────────────────────────

    def list_workspaces(self) -> list:
        resp = self._request("GET", "/workspaces")
        resp.raise_for_status()
        return resp.json().get("items", [])

    def list_notebooks(self, workspace_key: str = None, path: str = "Workspace") -> list:
        ws = workspace_key or self.workspace_key
        import urllib.parse
        encoded = urllib.parse.quote(path, safe="")
        resp = self._request(
            "GET",
            f"/workspaces/{ws}/notebook/api/contents/{encoded}?type=directory&content=1"
        )
        if resp.ok:
            return resp.json().get("content", [])
        return []

    # ── File Upload (uploadFileMeta → PAR URL → PUT) ─────────────

    def upload_file(
        self,
        workspace_key: str,
        local_path: str,
        remote_path: str,
        overwrite: bool = True,
    ) -> None:
        """Upload a file to AIDP workspace.

        Two-step upload pattern (same as the Rust reference implementation):
        1. POST uploadFileMeta → get PAR URL
        2. PUT file content to PAR URL
        3. POST uploadFileMeta with UPDATE action to confirm

        Args:
            workspace_key: AIDP workspace key
            local_path: Local file path
            remote_path: Remote path in workspace (e.g., /Workspace/Migrated/nb_claims.ipynb)
            overwrite: Overwrite if exists
        """
        header_path = remote_path
        if header_path.startswith("/Workspace/"):
            header_path = header_path[len("/Workspace/"):]

        upload_headers = {
            "Content-Type": "application/json",
            "Path": header_path,
            "Type": "FILE",
        }
        object_description = os.path.basename(local_path)

        # Step 1: Get PAR URL
        resp = self._request(
            "POST",
            f"/workspaces/{workspace_key}/actions/uploadFileMeta",
            params={"isOverwrite": str(overwrite).lower(), "objectDescription": object_description},
            json={"action": "CREATE"},
            headers=upload_headers,
        )
        resp.raise_for_status()
        par_url = resp.json().get("parUrl")
        if not par_url:
            raise ValueError(f"No PAR URL returned for upload: {resp.json()}")

        # Step 2: PUT file to PAR URL
        file_size = os.path.getsize(local_path)
        with open(local_path, "rb") as f:
            upload_resp = requests.put(par_url, data=f, headers={"Content-Length": str(file_size)})
            upload_resp.raise_for_status()
            etag = upload_resp.headers.get("etag")

        # Step 3: Confirm upload
        resp = self._request(
            "POST",
            f"/workspaces/{workspace_key}/actions/uploadFileMeta",
            params={"isOverwrite": str(overwrite).lower(), "objectDescription": object_description},
            json={"action": "UPDATE", "eTag": etag, "size": file_size},
            headers=upload_headers,
        )
        resp.raise_for_status()
        logger.info("Uploaded %s → %s", os.path.basename(local_path), remote_path)

    def upload_notebook(self, local_path: str, remote_path: str,
                        workspace_key: str = None) -> None:
        """Upload a notebook to AIDP workspace."""
        ws = workspace_key or self.workspace_key
        if not ws:
            raise ValueError("workspace_key required. Set AIDP_WORKSPACE_KEY in .env.")
        self.upload_file(ws, local_path, remote_path)

    # ── Workspace objects (folders / existence) ───────────────────

    @staticmethod
    def workspace_relative(path: str) -> str:
        """The ``Path`` header / ``path`` body value the workspace object
        API expects: relative to the workspace root, no leading slash, no
        ``/Workspace/`` prefix (``/Workspace/Migrated/x.ipynb`` ->
        ``Migrated/x.ipynb``)."""
        p = path.strip()
        if p.startswith("/Workspace/"):
            p = p[len("/Workspace/"):]
        elif p == "/Workspace":
            p = ""
        return p.lstrip("/")

    def mkdir(self, path: str, workspace_key: str = None) -> None:
        """Create a workspace folder (``POST .../actions/mkdir``). An
        already-existing folder is not an error."""
        ws = workspace_key or self.workspace_key
        rel = self.workspace_relative(path)
        if not rel:
            return
        resp = self._request(
            "POST", f"/workspaces/{ws}/actions/mkdir", expected=(409,),
            json={"path": rel, "description": None},
        )
        if resp.status_code == 409 or (
            not resp.ok and "exist" in resp.text.lower()
        ):
            return
        resp.raise_for_status()

    def list_objects(self, path: str = "", workspace_key: str = None) -> list:
        """Files, folders and notebooks directly under ``path``
        (``GET .../objects?path=``)."""
        ws = workspace_key or self.workspace_key
        rel = self.workspace_relative(path)
        resp = self._request("GET", f"/workspaces/{ws}/objects", params={"path": rel or "/"})
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):   # a bare list is accepted, as intended
            return data
        return data.get("items", [])

    def object_exists(self, path: str, workspace_key: str = None) -> bool:
        rel = self.workspace_relative(path)
        parent, _, name = rel.rpartition("/")
        for item in self.list_objects(parent, workspace_key):
            item_name = item.get("name") or item.get("displayName") or ""
            item_path = self.workspace_relative(str(item.get("path", "")))
            if item_name == name or item_path == rel:
                return True
        return False

    # ── Cluster Operations ────────────────────────────────────────

    def list_clusters(self, workspace_key: str = None) -> list:
        ws = workspace_key or self.workspace_key
        resp = self._request("GET", f"/workspaces/{ws}/clusters")
        resp.raise_for_status()
        return resp.json().get("items", [])

    # ── Job Operations ────────────────────────────────────────────

    def list_jobs(self, workspace_key: str = None) -> list:
        """Every job in the workspace. The list API pages (25 per call by
        default, ``limit`` capped at 100, continuation in the
        ``opc-next-page`` header -- measured live 2026-09-24), so a single
        GET on a workspace with more than a page of jobs would miss the
        job a re-deploy is looking for."""
        ws = workspace_key or self.workspace_key
        items, page = [], None
        while True:
            params = {"limit": 100}
            if page:
                params["page"] = page
            resp = self._request("GET", f"/workspaces/{ws}/jobs", params=params)
            resp.raise_for_status()
            items.extend(resp.json().get("items", []))
            page = resp.headers.get("opc-next-page")
            if not page:
                return items

    def create_job(self, workspace_key: str, job_config: dict) -> dict:
        """``POST /workspaces/{ws}/jobs`` with an AIDP job body: ``name``,
        ``path`` ("jobs"), ``tasks`` (``taskKey``/``type``/``notebookPath``/
        ``dependsOn``/``runIf``/``cluster``), ``jobClusters``, optional
        ``schedule`` {``quartzCronExpression``, ``timezoneId``, ``pauseStatus``},
        ``maxConcurrentRuns``.
        Returns the created job (its ``key`` is the job id)."""
        ws = workspace_key or self.workspace_key
        resp = self._request("POST", f"/workspaces/{ws}/jobs", json=job_config)
        resp.raise_for_status()
        return resp.json()

    def update_job(self, workspace_key: str, job_key: str, job_config: dict) -> dict:
        """Replace an existing job definition.

        ``PUT`` is the only verb the service accepts here: ``PATCH`` returns
        404, so the whole definition must be sent, not a delta. AIDP rejects
        a create whose name already exists (``JOB_VALIDATE_0031``), which
        made every re-deploy fail until this existed.
        """
        ws = workspace_key or self.workspace_key
        resp = self._request("PUT", f"/workspaces/{ws}/jobs/{job_key}", json=job_config)
        resp.raise_for_status()
        return resp.json()

    def find_job_by_name(self, name: str, workspace_key: str = None) -> dict | None:
        """The job whose ``name`` matches exactly, or None.

        AIDP reports the clash as ``<name>.job`` but stores and lists the
        name without that suffix, so compare against the bare name.
        """
        for job in self.list_jobs(workspace_key):
            if job.get("name") == name:
                return job
        return None

    @staticmethod
    def cluster_ref(cluster_key: str) -> dict:
        """The ``cluster`` reference a job task carries. Only ``clusterKey``:
        the workflow engine treats ``clusterName`` as a lookup-by-name and
        fails with "Cluster not found" if it is not the real display name,
        so it is omitted and the server resolves the key."""
        return {"clusterKey": cluster_key}

    # ── Connection Test ───────────────────────────────────────────

    def test_connection(self) -> bool:
        """Test if the AIDP connection works."""
        try:
            self.list_workspaces()
            return True
        except Exception as exc:
            logger.error("AIDP connection test failed: %s", exc)
            return False
