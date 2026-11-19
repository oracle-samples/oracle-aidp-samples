"""Talk to AIDP through the `aidp` CLI.

The same bargain as the Node M parser: an optional external tool, discovered
at runtime, never a Python dependency. `aidp-cli` already solves OCI request
signing, endpoint resolution and profile handling, and re-implementing that
here would mean taking on the `oci` SDK and breaking `dependencies = []`.

Every call here was exercised against a live AIDP workspace before it was
written; the payload shapes are observed, not read off a doc page.
"""
from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path

TIMEOUT_SECONDS = 180
# list-jobs returns 25 per page unless asked; a workspace holding more than one
# page made a page-1-only read report a job as absent when it was on page 2.
PAGE_SIZE = 100
MAX_PAGES = 200


def _literal(value: str) -> str:
    """aidp-cli runs every option value through json.loads: `--path Infinity`
    becomes the float inf and lists folder "inf". A JSON string stays a string."""
    return json.dumps(value)


class AidpUnavailable(Exception):
    """The `aidp` CLI is not installed."""


class AidpError(Exception):
    """The CLI ran and the API refused."""

    def __init__(self, message, *, status=None):
        super().__init__(message)
        self.status = status


class AidpClient:
    def __init__(self, *, workspace_key, instance_id=None, profile=None,
                 auth=None, executable="aidp", timeout=TIMEOUT_SECONDS):
        self.workspace_key = workspace_key
        self.instance_id = instance_id
        self.profile = profile
        self.auth = auth
        self.executable = executable
        self.timeout = timeout

    @staticmethod
    def available(executable="aidp") -> bool:
        return shutil.which(executable) is not None

    def _run(self, args, body=None):
        found = shutil.which(self.executable)
        if found is None:
            raise AidpUnavailable(
                f"{self.executable!r} not found on PATH; "
                f"install it with `pip install aidp-cli` to publish")
        command = [found] + list(args)
        if self.instance_id:
            command += ["--instance-id", self.instance_id]
        if self.profile:
            command += ["--profile", self.profile]
        if self.auth:
            command += ["--auth", self.auth]
        command += ["--timeout", str(self.timeout)]

        scratch = None
        try:
            if body is not None:
                scratch = tempfile.NamedTemporaryFile(
                    "w", suffix=".json", delete=False, encoding="utf-8")
                json.dump(body, scratch)
                scratch.close()
                command += ["--body", f"@{scratch.name}"]
            done = subprocess.run(command, capture_output=True, text=True,
                                  timeout=self.timeout + 30)
        except subprocess.TimeoutExpired as exc:
            raise AidpError(f"aidp timed out after {self.timeout}s") from exc
        finally:
            if scratch is not None:
                Path(scratch.name).unlink(missing_ok=True)

        # An API refusal can arrive on stderr with a non-zero exit; read
        # whichever stream carries the JSON so its code and message survive.
        text = done.stdout or ""
        if "{" not in text and "{" in (done.stderr or ""):
            text = done.stderr
        start = text.find("{")
        if start < 0:
            raise AidpError(
                f"aidp returned no JSON (exit {done.returncode}): "
                f"{(done.stderr or text).strip()[:300]}")
        try:
            payload = json.loads(text[start:text.rindex("}") + 1])
        except (ValueError, json.JSONDecodeError) as exc:
            raise AidpError(f"aidp returned unparseable JSON: {text[:200]!r}") from exc
        status = payload.get("status")
        if isinstance(status, int) and status >= 400:
            raise AidpError(
                f"AIDP {status} {payload.get('code', '')}: "
                f"{payload.get('message', '')}".strip(), status=status)
        return payload

    def _pages(self, args) -> list:
        """Every item of a list call, following `opc-next-page`."""
        found, page = [], None
        for _ in range(MAX_PAGES):
            payload = self._run(list(args) + ["--limit", str(PAGE_SIZE)]
                                + (["--page", _literal(page)] if page else []))
            data = payload.get("data") or {}
            items = data.get("items") if isinstance(data, dict) else None
            found.extend(items if isinstance(items, list) else [])
            headers = payload.get("headers")
            headers = headers if isinstance(headers, dict) else {}
            page = next((v for k, v in headers.items()
                         if str(k).lower() == "opc-next-page" and v), None)
            if not page:
                return found
        raise AidpError(f"{' '.join(args[:2])} still paging after {MAX_PAGES} pages")

    # --- notebooks --------------------------------------------------------
    @staticmethod
    def object_path(path: str) -> str:
        """`/Workspace/a/b.ipynb` -> `a/b.ipynb`.

        The notebook API maps `/Workspace` onto the workspace root; the object
        API wants that same location relative, with no leading slash.
        """
        relative = path.strip("/")
        if relative == "Workspace" or relative.startswith("Workspace/"):
            relative = relative[len("Workspace"):].lstrip("/")
        return relative

    def folder_names(self, folder: str) -> set:
        """Names directly inside a workspace folder; empty if it does not exist.

        `notebook get-content` is no use for this: on a live workspace it
        answered 500 for notebooks and folders alike, after minutes.
        """
        try:
            items = self._pages(["workspace-object", "list", self.workspace_key,
                                 "--path", _literal(self.object_path(folder))])
        except AidpError as exc:
            if exc.status == 404:
                return set()
            raise
        return {str(i.get("displayName") or str(i.get("path", "")).rsplit("/", 1)[-1])
                for i in items if isinstance(i, dict)}

    def put_notebook(self, path: str, ipynb_text: str) -> dict:
        return self._run(["notebook", "update-content", self.workspace_key, path],
                         body={"type": "notebook", "format": "json",
                               "content": json.loads(ipynb_text)})

    # --- jobs -------------------------------------------------------------
    def list_jobs(self) -> list:
        """Every job in the workspace, not just the first page."""
        return self._pages(["workflow", "list-jobs", self.workspace_key])

    def create_job(self, definition: dict) -> str:
        payload = self._run(["workflow", "create-job", self.workspace_key],
                            body=definition)
        key = (payload.get("data") or {}).get("key")
        if not key:
            raise AidpError(f"create-job returned no key: {str(payload)[:200]}")
        return key

    def cluster_problem(self, cluster_key: str) -> str:
        """"" when `cluster_key` can run a notebook job here; otherwise why not.

        Reported in review: the workspace's "Default Master Catalog Compute"
        key is accepted when a job is created and the job fails only at run
        time. MEASURED on AIDP, 2026-10-01, a seed job on that key:
        "WORKFLOW_EXECUTION_0055 - Cluster ... is not in Active state in
        workspace ..." (the reviewer saw WORKFLOW_EXECUTION_0071 for the same
        key). It is not in `cluster list`, which lists the USER clusters --
        per the CLI's own help, the only type a workspace notebook can attach
        to -- and `cluster get-default` is what returns it. So the rule is:
        the key must be one `cluster list` returns.
        """
        users = {c.get("key"): c.get("displayName") or "?"
                 for c in self._pages(["cluster", "list", self.workspace_key])}
        if cluster_key in users:
            return ""
        listed = ", ".join(f"{name} ({key})" for key, name in users.items()) or "none"
        try:
            default = (self._run(["cluster", "get-default"]).get("data") or {}).get("key")
        except AidpError:
            default = None
        if cluster_key == default:
            return (f"cluster {cluster_key} is this workspace's Default Master "
                    f"Catalog Compute, which cannot run notebook jobs: they are "
                    f"accepted and then fail at run time. Pass one of its "
                    f"clusters instead: {listed}")
        return (f"cluster {cluster_key} is not one of this workspace's clusters; "
                f"its clusters are: {listed}")

    def get_job(self, job_key: str) -> dict:
        """A job's current definition (`data` of get-job)."""
        return self._run(["workflow", "get-job", self.workspace_key, job_key]).get("data") or {}

    def update_job(self, job_key: str, definition: dict) -> None:
        """Replace a job's definition; same body shape as create-job."""
        self._run(["workflow", "update-job", self.workspace_key, job_key],
                  body=definition)

    def run_job(self, job_key: str) -> str:
        """Start one run of a job; the run's key.

        Used by `scripts/seed_demo_data.py`. `publish` itself never runs
        anything -- running is a separate, deliberate step (RUNBOOK step 7).
        """
        payload = self._run(["workflow", "create-job-run", self.workspace_key],
                            body={"jobKey": job_key})
        key = (payload.get("data") or {}).get("key")
        if not key:
            raise AidpError(f"create-job-run returned no key: {str(payload)[:200]}")
        return key

    def task_runs(self, run_key: str) -> list:
        """[(task key, status, message, task run key)] for one job run.

        The status sits at `state.status`, not at the top of each item --
        a poller that read `status` saw None for every task and stopped.
        `--sort-by` is required by the API; without it the call is refused.
        """
        items = self._pages(["workflow", "list-task-runs", self.workspace_key,
                             "--job-run-key", run_key,
                             "--sort-by", "timeCreated", "--sort-order", "ASC"])
        out = []
        for item in items:
            state = item.get("state") if isinstance(item.get("state"), dict) else {}
            out.append((item.get("taskKey"), state.get("status"),
                        state.get("stateMessage") or "", item.get("key")))
        return out

    def task_output(self, task_run_key: str) -> str:
        """What a task printed, including a failed one's trace, as text.

        `fetch-output` requires a body, and an empty one is what it takes.
        """
        payload = self._run(["workflow", "fetch-output", self.workspace_key,
                             task_run_key], body={})
        return json.dumps(payload.get("data", payload))
