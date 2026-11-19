"""AIDP REST client with hard isolation guardrails.

Every write method asserts the target is inside <export_output> (notebooks/files)
or carries the <prefix>_ namespace (workflows/clusters). Bypassing the
guardrails requires editing this file — they are not flags.

Endpoints used:
  Notebook CRUD:
    GET    /workspaces/{ws}/notebook/api/contents/{path}?type=notebook&content=1
    PUT    /workspaces/{ws}/notebook/api/contents/{path}
  Files (3-step upload via PAR URL):
    POST   /workspaces/{ws}/actions/uploadFileMeta
    PUT    {parUrl}             (external object store, not signed)
    POST   /workspaces/{ws}/actions/uploadFileMeta   (finalize)
  Files (download via PAR URL):
    POST   /workspaces/{ws}/actions/downloadFileMeta
    GET    {parUrl}
  Workflows:
    GET    /workspaces/{ws}/jobs
    POST   /workspaces/{ws}/jobs
    PUT    /workspaces/{ws}/jobs/{key}
  Clusters:
    GET    /workspaces/{ws}/clusters
    POST   /workspaces/{ws}/clusters
    PATCH  /workspaces/{ws}/clusters/{key}/libraries
  Objects listing (used only to probe existence; never to modify):
    GET    /workspaces/{ws}/objects?path=...
"""

from __future__ import annotations

import json
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

from . import sanitizer
from .auth import _get_aidputils_from_kernel, aidp_base_url, aidp_workspace_url


# Same-basename artifact aliasing is licensed ONLY by the verified relocation
# incident (the tool-owned dependency repository moving …/jars → …/libs);
# filename equality does not establish content identity for arbitrary workload
# libraries (PR #88 review P1). Trust is EXACT directories, never a parent
# prefix (round 4: a shallow configured dir must not trust the workspace),
# and it follows the CONFIGURED repository dir so both consumers agree —
# keep in sync with scripts/cluster_lifecycle._artifact_alias_dirs().
_DEFAULT_LIB_REPOSITORY_DIR = "/Workspace/migration-dependencies/libs"
_artifact_alias_dirs_configured: Optional[set] = None


def set_artifact_alias_repository(lib_repository_dir: str) -> None:
    """Called by __main__ once the customer's lib_repository_dir is resolved."""
    global _artifact_alias_dirs_configured
    _artifact_alias_dirs_configured = _derive_alias_dirs(lib_repository_dir)


def _derive_alias_dirs(lib_dir: str) -> set:
    lib_dir = (lib_dir or _DEFAULT_LIB_REPOSITORY_DIR).rstrip("/")
    dirs = {lib_dir}
    parent, _, base = lib_dir.rpartition("/")
    if base == "libs" and parent:
        dirs.add(parent + "/jars")
    return dirs


def artifact_alias_dirs() -> set:
    if _artifact_alias_dirs_configured is not None:
        return _artifact_alias_dirs_configured
    return _derive_alias_dirs(_DEFAULT_LIB_REPOSITORY_DIR)


class AidpApiError(RuntimeError):
    def __init__(self, op: str, status: int, body: str):
        super().__init__(f"{op}: HTTP {status} — {body[:300]}")
        self.op = op
        self.status = status
        self.body = body


@dataclass
class AidpConfig:
    lake_ocid: str
    workspace_id: str
    export_output: str       # required — used for write guardrails
    prefix: str = "ai_tool"  # required — used for workflow/cluster name guardrails

    @property
    def base(self) -> str:
        return aidp_base_url(self.lake_ocid)

    @property
    def ws(self) -> str:
        return aidp_workspace_url(self.lake_ocid, self.workspace_id)


def _strip_workspace(p: str) -> str:
    """For notebook CRUD endpoint: the {path} segment is workspace-relative."""
    if p.startswith("/Workspace/"):
        return p[len("/Workspace/"):]
    if p.startswith("Workspace/"):
        return p[len("Workspace/"):]
    return p.lstrip("/")


class AidpClient:
    """REST client with built-in guardrails. Use it for ALL AIDP writes."""

    def __init__(self, signer, config: AidpConfig):
        self.signer = signer
        self.config = config

    # ─── Notebooks ────────────────────────────────────────────────────

    def get_notebook(self, aidp_path: str) -> Optional[Dict[str, Any]]:
        """Return the notebook dict if it exists, else None.

        Read-only — does not enforce target-base (we sometimes probe outside
        only as a no-op safety check, never to modify).
        """
        rel = _strip_workspace(aidp_path)
        encoded = urllib.parse.quote(rel, safe="")
        url = (
            f"{self.config.ws}/notebook/api/contents/{encoded}"
            f"?type=notebook&content=1"
        )
        r = requests.get(url, auth=self.signer)
        if r.status_code == 404:
            return None
        if r.status_code != 200:
            raise AidpApiError("get_notebook", r.status_code, r.text)
        return r.json().get("content")

    def put_notebook(self, aidp_path: str, notebook_content: Dict[str, Any]) -> None:
        """Create or update a notebook. ASSERTS target-base before sending."""
        sanitizer.assert_under_export_output(
            aidp_path, self.config.export_output, label="notebook_path"
        )
        rel = _strip_workspace(aidp_path)
        encoded = urllib.parse.quote(rel, safe="")
        url = f"{self.config.ws}/notebook/api/contents/{encoded}"
        body = {"type": "notebook", "content": notebook_content}
        r = requests.put(url, json=body, auth=self.signer)
        if r.status_code not in (200, 201):
            raise AidpApiError("put_notebook", r.status_code, r.text)

    # ─── Files (3-step upload) ────────────────────────────────────────

    def create_directory(self, aidp_path: str) -> None:
        """Ensure a workspace folder exists. Idempotent — 200 if exists or created.

        Uses POST /objects with `path` + `type=FOLDER` headers. Asserts
        target-base before sending. Required before upload_file() into a
        previously non-existent folder; the Jupyter contents API can't
        create directories on AIDP (returns 500 _save_directory missing).
        """
        sanitizer.assert_under_export_output(
            aidp_path, self.config.export_output, label="folder_path"
        )
        url = f"{self.config.ws}/objects"
        headers = {"Content-Type": "application/json", "path": aidp_path, "type": "FOLDER"}
        r = requests.post(url, headers=headers, auth=self.signer, data="")
        if r.status_code in (200, 201, 409):
            return
        raise AidpApiError("create_directory", r.status_code, r.text)

    def upload_file(self, aidp_path: str, content: bytes, overwrite: bool = False) -> None:
        """Upload a file (text source like .py / .sh / .yaml or binary like .jar / .whl).

        Uses the Jupyter contents API: PUT /notebook/api/contents/{path} with
        body {"type":"file","format":"text|base64","content":...}. Auto-detects
        text vs binary by attempting UTF-8 decode. Auto-creates the parent
        folder via POST /objects with type=FOLDER. ASSERTS target-base before
        any HTTP call.

        `overwrite` is informational — contents PUT overwrites by default.
        """
        sanitizer.assert_under_export_output(
            aidp_path, self.config.export_output, label="file_path"
        )
        # Fast path — direct FUSE write when running ON the AIDP cluster.
        # /Workspace there is a local FUSE mount, and large binary uploads
        # (jars/whl) over the HTTPS contents API to the aidp.<region> endpoint
        # frequently die with SSLError ("Max retries exceeded") on big bodies.
        # Writing the bytes straight to the mounted path is reliable and avoids
        # the control-plane endpoint entirely. Gated on the AIDP kernel's
        # `aidputils` actually being present (_get_aidputils_from_kernel), not
        # just os.path.isdir("/Workspace") — a laptop that happens to have a
        # same-named directory must never take this path. Set
        # AIDP_NO_FUSE_UPLOAD=1 to force the REST path even on-cluster.
        import os as _os
        if (aidp_path.startswith("/Workspace/")
                and _get_aidputils_from_kernel() is not None
                and _os.path.isdir("/Workspace")
                and _os.environ.get("AIDP_NO_FUSE_UPLOAD") != "1"):
            try:
                _os.makedirs(_os.path.dirname(aidp_path), exist_ok=True)
                with open(aidp_path, "wb") as _fh:
                    _fh.write(content)
                if (_os.path.exists(aidp_path)
                        and _os.path.getsize(aidp_path) == len(content)):
                    return
            except OSError:
                pass  # fall through to the REST contents API below
        # Ensure parent folder exists. Walk up creating any missing ancestors
        # within export_output. Best-effort; uploads surface the real error.
        import posixpath as _pp
        parent = _pp.dirname(aidp_path)
        norm_base = self.config.export_output.rstrip("/")
        ancestors = []
        cur = parent
        while cur and cur != norm_base and cur.startswith(norm_base):
            ancestors.append(cur)
            cur = _pp.dirname(cur)
        for folder in reversed(ancestors):
            try:
                self.create_directory(folder)
            except AidpApiError:
                pass

        rel = _strip_workspace(aidp_path)
        encoded = urllib.parse.quote(rel, safe="")
        url = f"{self.config.ws}/notebook/api/contents/{encoded}"

        try:
            text = content.decode("utf-8")
            body = {"type": "file", "format": "text", "content": text}
        except UnicodeDecodeError:
            import base64 as _b64
            body = {"type": "file", "format": "base64",
                    "content": _b64.b64encode(content).decode("ascii")}

        last_exc = None
        for attempt in range(10):  # Increased from 3 to 10 retries for large JARs
            try:
                r = requests.put(url, json=body, auth=self.signer, timeout=1800)  # 30 min timeout for large JARs
                break
            except requests.exceptions.RequestException as e:
                last_exc = e
                if attempt < 9:  # Updated from 2 to 9
                    wait_time = min(2 ** attempt, 60)  # Cap wait at 60 seconds
                    print(f"Upload attempt {attempt + 1} failed, retrying in {wait_time}s...")
                    time.sleep(wait_time)
        else:
            raise AidpApiError(
                "upload_file", 0,
                f"network error after 10 attempts: {type(last_exc).__name__}: {last_exc}")
        if r.status_code not in (200, 201):
            raise AidpApiError("upload_file", r.status_code, r.text)

    def download_file(
        self,
        aidp_path: str,
        file_type: str = "FILE",
        max_bytes: Optional[int] = None,
    ) -> bytes:
        """Read a file from AIDP via downloadFileMeta → PAR URL.

        Read-only — no guardrail enforcement (we only ever download what we
        just uploaded, to validate round-trip integrity).
        """
        url = f"{self.config.ws}/actions/downloadFileMeta"
        headers = {"Content-Type": "application/json", "path": aidp_path, "type": file_type}
        try:
            r = requests.post(url, headers=headers, auth=self.signer, data="", timeout=600)
        except requests.exceptions.RequestException as e:
            raise AidpApiError("downloadFileMeta", 0, f"network error: {type(e).__name__}: {e}")
        if r.status_code != 200:
            raise AidpApiError("downloadFileMeta", r.status_code, r.text)
        par = r.json().get("parUrl")
        if not par:
            raise AidpApiError("downloadFileMeta", 200, "no parUrl in response")
        try:
            r2 = requests.get(par, timeout=600, stream=True)
        except requests.exceptions.RequestException as e:
            raise AidpApiError("par_get", 0, f"network error: {type(e).__name__}: {e}")
        if r2.status_code != 200:
            raise AidpApiError("par_get", r2.status_code, r2.text[:300])
        content_length = r2.headers.get("Content-Length")
        if max_bytes is not None and content_length:
            try:
                if int(content_length) > max_bytes:
                    raise AidpApiError(
                        "par_get", 413,
                        f"workspace file exceeds {max_bytes} byte limit: {aidp_path}",
                    )
            except ValueError:
                pass
        content = bytearray()
        for chunk in r2.iter_content(chunk_size=1024 * 1024):
            if not chunk:
                continue
            content.extend(chunk)
            if max_bytes is not None and len(content) > max_bytes:
                raise AidpApiError(
                    "par_get", 413,
                    f"workspace file exceeds {max_bytes} byte limit: {aidp_path}",
                )
        return bytes(content)

    # ─── Objects (existence probe) ────────────────────────────────────

    def object_exists(self, aidp_path: str) -> bool:
        """Read-only probe — used for skip-if-exists logic for jars/init scripts."""
        url = f"{self.config.ws}/objects?path={urllib.parse.quote(aidp_path, safe='')}"
        r = requests.get(url, auth=self.signer)
        if r.status_code == 404:
            return False
        if r.status_code != 200:
            return False
        # API returns the parent listing; check that our path appears.
        items = r.json().get("items", [])
        for it in items:
            if it.get("path") == aidp_path:
                return True
        # Some implementations return just the object itself with no listing.
        return False

    # ─── Workflows ────────────────────────────────────────────────────

    def list_workspace_objects(self, aidp_path: str) -> List[Dict[str, Any]]:
        """List every object directly under an AIDP workspace directory.

        This read-only helper is used to index the configured fallback JAR
        directory before cluster creation. Pagination is required because a
        shared dependency directory can contain more than one API page.
        """
        url_base = (
            f"{self.config.ws}/objects?path="
            f"{urllib.parse.quote(aidp_path, safe='')}"
        )
        items: List[Dict[str, Any]] = []
        next_page: Optional[str] = None
        while True:
            url = url_base
            if next_page:
                url = (
                    f"{url_base}&page="
                    f"{urllib.parse.quote(next_page, safe='')}"
                )
            r = requests.get(url, auth=self.signer)
            if r.status_code != 200:
                raise AidpApiError("list_workspace_objects", r.status_code, r.text)
            items.extend(r.json().get("items", []) or [])
            next_page = r.headers.get("opc-next-page")
            if not next_page:
                return items

    def _paginated_get(self, url_base: str) -> List[Dict[str, Any]]:
        """GET all pages of a list endpoint. AIDP returns 100 per page and
        passes the next-page token via the `opc-next-page` response header."""
        items: List[Dict[str, Any]] = []
        next_page: Optional[str] = None
        while True:
            url = url_base
            if next_page:
                sep = "&" if "?" in url_base else "?"
                url = f"{url_base}{sep}page={urllib.parse.quote(next_page)}"
            r = requests.get(url, auth=self.signer)
            if r.status_code != 200:
                raise AidpApiError("list", r.status_code, r.text)
            items.extend(r.json().get("items", []) or [])
            next_page = r.headers.get("opc-next-page")
            if not next_page:
                break
        return items

    def list_workflows(self) -> List[Dict[str, Any]]:
        return self._paginated_get(f"{self.config.ws}/jobs")

    def find_workflow_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        # Stream pages and early-return on match instead of loading every page.
        url_base = f"{self.config.ws}/jobs"
        next_page: Optional[str] = None
        while True:
            url = url_base
            if next_page:
                url = f"{url_base}?page={urllib.parse.quote(next_page)}"
            r = requests.get(url, auth=self.signer)
            if r.status_code != 200:
                raise AidpApiError("find_workflow", r.status_code, r.text)
            for j in r.json().get("items", []) or []:
                if j.get("name") == name:
                    return j
            next_page = r.headers.get("opc-next-page")
            if not next_page:
                return None

    def create_workflow_shell(self, name: str, description: str = "") -> Dict[str, Any]:
        sanitizer.assert_has_prefix(name, self.config.prefix, label="workflow_name")
        url = f"{self.config.ws}/jobs"
        body = {
            "name": name,
            "path": "jobs",
            "description": description,
            "maxConcurrentRuns": 1,
        }
        r = requests.post(url, json=body, auth=self.signer)
        if r.status_code not in (200, 201, 202):
            raise AidpApiError("create_workflow", r.status_code, r.text)
        return r.json()

    def update_workflow(self, job_key: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        # Re-assert prefix on the payload's name (paranoia: a bug could rewrite name).
        name = payload.get("name", "")
        if name:
            sanitizer.assert_has_prefix(name, self.config.prefix, label="workflow_name")
        url = f"{self.config.ws}/jobs/{job_key}"
        r = requests.put(url, json=payload, auth=self.signer)
        if r.status_code not in (200, 201, 202):
            raise AidpApiError("update_workflow", r.status_code, r.text)
        return r.json()

    # ─── Clusters ─────────────────────────────────────────────────────

    def list_clusters(self) -> List[Dict[str, Any]]:
        return self._paginated_get(f"{self.config.ws}/clusters")

    def get_cluster(self, cluster_key: str) -> Dict[str, Any]:
        """Read the full cluster definition, including attached init scripts."""
        r = requests.get(f"{self.config.ws}/clusters/{cluster_key}", auth=self.signer)
        if r.status_code != 200:
            raise AidpApiError("get_cluster", r.status_code, r.text)
        return r.json()

    def find_cluster_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        # Stream + early-return; clusters list can be paginated too.
        url_base = f"{self.config.ws}/clusters"
        next_page: Optional[str] = None
        while True:
            url = url_base
            if next_page:
                url = f"{url_base}?page={urllib.parse.quote(next_page)}"
            r = requests.get(url, auth=self.signer)
            if r.status_code != 200:
                raise AidpApiError("find_cluster", r.status_code, r.text)
            for c in r.json().get("items", []) or []:
                if c.get("displayName") == name:
                    return c
            next_page = r.headers.get("opc-next-page")
            if not next_page:
                return None
        return None

    def create_cluster(self, cluster_payload: Dict[str, Any]) -> Dict[str, Any]:
        name = cluster_payload.get("displayName", "")
        sanitizer.assert_has_prefix(name, self.config.prefix, label="cluster_displayName")
        url = f"{self.config.ws}/clusters"
        r = requests.post(url, json=cluster_payload, auth=self.signer)
        if r.status_code not in (200, 201, 202):
            raise AidpApiError("create_cluster", r.status_code, r.text)
        return r.json()

    def list_cluster_libraries(self, cluster_key: str) -> List[Dict[str, Any]]:
        """Return the libraries currently attached to a cluster."""
        url = f"{self.config.ws}/clusters/{cluster_key}/libraries"
        r = requests.get(url, auth=self.signer)
        if r.status_code != 200:
            raise AidpApiError("list_cluster_libraries", r.status_code, r.text)
        return r.json().get("items", []) or []

    @staticmethod
    def _library_identity(item: Dict[str, Any]) -> Optional[Tuple[str, str]]:
        """Return the stable AIDP identity of a library request/state item."""
        for field in ("path", "package", "coordinates"):
            value = item.get(field)
            if value:
                # GET responses do not consistently echo the request's `type`,
                # but do preserve the identifying field itself.
                return field, str(value)
        return None

    def _current_library_state(
        self, cluster_key: str,
    ) -> Tuple[Dict[Tuple[str, str], List[str]], Dict[str, List[Tuple[str, str]]]]:
        """One GET → (identity→statuses, basename→[(path, status)]).

        Versioned binary artifacts (.jar/.whl) are additionally indexed by
        basename: the dependency repository moved …/jars → …/libs (PR #77
        rename), and a cluster provisioned before the rename carries the SAME
        jar under the OLD path (incident: cluster c6364486, 2026-09-09).
        Jar/wheel filenames carry version(+hash) identity, so basename
        equality is content-safe for these types ONLY — requirements/text
        paths legitimately share basenames and stay full-path. Keep in sync
        with scripts/cluster_lifecycle.py::_best_artifact_registration.
        """
        current: Dict[Tuple[str, str], List[str]] = {}
        by_basename: Dict[str, List[Tuple[str, str]]] = {}
        for library in self.list_cluster_libraries(cluster_key):
            identity = self._library_identity(library)
            if identity is not None:
                status = str(library.get("status") or "").upper()
                current.setdefault(identity, []).append(status)
                if identity[0] == "path" and identity[1].endswith((".jar", ".whl")):
                    by_basename.setdefault(
                        identity[1].rsplit("/", 1)[-1], []).append((identity[1], status))
        return current, by_basename

    def _library_satisfied_by(
        self, cluster_key: str, item: Dict[str, Any],
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """How an install request is already satisfied on the cluster.

        Returns (kind, wait_item):
          ("exact", item)              — this identity is attached/pending;
                                         wait on the item itself.
          ("basename", {"path": p})    — a same-basename registration under a
                                         DIFFERENT repository path satisfies
                                         it; wait on THAT registration, never
                                         on the skipped item's own identity
                                         (which will never appear — waiting on
                                         it cost a 600s stall + false 408 per
                                         call on every pre-rename cluster,
                                         PR #88 review major 2).
          (None, None)                 — installable; PATCH it.
        """
        reinstallable = {"FAILED", "UNINSTALL_ON_RESTART", "DELETED"}
        current, by_basename = self._current_library_state(cluster_key)
        identity = self._library_identity(item)
        if identity is None:
            return None, None
        status = self._effective_library_status(current.get(identity, []))
        if status is not None and status not in reinstallable:
            return "exact", item
        alias_dirs = artifact_alias_dirs()
        if (identity[0] == "path" and identity[1].endswith((".jar", ".whl"))
                and identity[1].rsplit("/", 1)[0] in alias_dirs):
            base = identity[1].rsplit("/", 1)[-1]
            rows = [(p, s) for p, s in by_basename.get(base, [])
                    if p != identity[1] and p.rsplit("/", 1)[0] in alias_dirs]
            # BEST registration across all candidate paths (loaded > staged >
            # transient), never first-in-API-order — a FAILED row beside an
            # INSTALLED one must resolve to the INSTALLED one deterministically.
            rank = {"INSTALLED": 0, "RESOLVED": 0, "INSTALL_ON_RESTART": 1,
                    "PENDING": 2, "RESOLVING": 2, "INSTALLING": 2}
            candidates = []
            for path in {p for p, _s in rows}:
                eff = self._effective_library_status(
                    [s for p, s in rows if p == path])
                if eff is not None and eff not in reinstallable:
                    candidates.append((rank.get(eff, 3), path))
            if candidates:
                return "basename", {"path": min(candidates)[1]}
        return None, None

    @staticmethod
    def _effective_library_status(statuses: List[str]) -> Optional[str]:
        """Choose the live operation state when AIDP retains stale rows.

        AIDP can return an old FAILED/DELETED row beside the row created by a
        newer install for the same path.  An active row must therefore win
        over stale reinstallable terminals, and a healthy terminal wins once
        the active row has settled.
        """
        normalized = [str(status or "UNKNOWN").upper() for status in statuses]
        transient = ("PENDING", "RESOLVING", "INSTALLING")
        successful = ("INSTALLED", "RESOLVED", "INSTALL_ON_RESTART")
        for group in (transient, successful, ("FAILED",),
                      ("UNINSTALL_ON_RESTART", "DELETED")):
            for status in group:
                if status in normalized:
                    return status
        return normalized[-1] if normalized else None

    def install_cluster_libraries(self, cluster_key: str, items: List[Dict[str, Any]]) -> None:
        """PATCH /clusters/{key}/libraries with INSTALL items.

        Caller is responsible for ensuring the cluster's displayName matches
        the prefix (we verify by looking it up).
        """
        # Look up the cluster to confirm its name matches the prefix — never
        # modify a cluster we did not create.
        for c in self.list_clusters():
            if c.get("key") == cluster_key:
                sanitizer.assert_has_prefix(
                    c.get("displayName", ""), self.config.prefix, label="cluster_displayName"
                )
                break
        else:
            raise AidpApiError("install_cluster_libraries", 404, f"cluster {cluster_key} not found")
        url = f"{self.config.ws}/clusters/{cluster_key}/libraries"
        requested_items = []
        seen_identities = set()
        for item in items:
            identity = self._library_identity(item)
            marker = identity or ("payload", json.dumps(item, sort_keys=True))
            if marker in seen_identities:
                continue
            seen_identities.add(marker)
            requested_items.append(item)
        for item in requested_items:
            # AIDP serializes library operations per cluster.  Submit and
            # settle one item at a time so a large batch cannot collide with
            # itself in the asynchronous operations queue.
            kind, wait_item = self._library_satisfied_by(cluster_key, item)
            if kind is not None:
                if kind == "basename":
                    print(
                        "[AIDP] library already attached under another "
                        f"repository path; skipping INSTALL and settling on "
                        f"{wait_item['path']}",
                        flush=True,
                    )
                self._wait_for_library_terminal_state(cluster_key, wait_item)
                continue
            retry_waits = (0, 2, 4, 8, 16)
            for attempt, wait_seconds in enumerate(retry_waits):
                if wait_seconds:
                    time.sleep(wait_seconds)
                r = requests.patch(
                    url,
                    json={"items": [item]},
                    auth=self.signer,
                )
                if r.status_code != 409:
                    break
                kind, wait_item = self._library_satisfied_by(cluster_key, item)
                if kind is not None:
                    self._wait_for_library_terminal_state(cluster_key, wait_item)
                    r = None
                    break
                if attempt == len(retry_waits) - 1:
                    break
            if r is None:
                continue
            if r.status_code not in (200, 201, 202):
                raise AidpApiError(
                    "install_cluster_libraries", r.status_code, r.text
                )
            self._wait_for_library_terminal_state(cluster_key, item)

    def _wait_for_library_terminal_state(
        self,
        cluster_key: str,
        item: Dict[str, Any],
        timeout_seconds: int = 600,
        poll_seconds: int = 2,
        stale_terminal_grace_polls: int = 2,
    ) -> str:
        """Wait until one accepted library operation leaves its active state.

        AIDP may briefly return only an older terminal row immediately after a
        new install request is accepted. Give that stale snapshot a small,
        bounded visibility grace before treating it as the new operation's
        result.
        """
        identity = self._library_identity(item)
        if identity is None:
            return "UNKNOWN"
        transient = {"PENDING", "RESOLVING", "INSTALLING"}
        successful = {"INSTALLED", "RESOLVED", "INSTALL_ON_RESTART"}
        deadline = time.monotonic() + timeout_seconds
        last_status = "NOT_VISIBLE"
        stale_terminal_polls = 0
        while time.monotonic() < deadline:
            matches = [
                library for library in self.list_cluster_libraries(cluster_key)
                if self._library_identity(library) == identity
            ]
            statuses = [
                str(library.get("status") or "UNKNOWN").upper()
                for library in matches
            ]
            status = self._effective_library_status(statuses)
            if status in successful:
                return status
            if status in {"FAILED", "UNINSTALL_ON_RESTART", "DELETED"}:
                stale_terminal_polls += 1
                if stale_terminal_polls <= stale_terminal_grace_polls:
                    last_status = status
                    time.sleep(poll_seconds)
                    continue
                if status == "FAILED":
                    raise AidpApiError(
                        "install_cluster_libraries", 409,
                        f"library {identity[1]} entered FAILED state",
                    )
                return status
            stale_terminal_polls = 0
            if status not in transient and status not in {None, "", "UNKNOWN"}:
                return status
            if status:
                last_status = status
            time.sleep(poll_seconds)
        raise AidpApiError(
            "install_cluster_libraries", 408,
            f"timed out waiting for {identity[1]} (last status={last_status})",
        )
