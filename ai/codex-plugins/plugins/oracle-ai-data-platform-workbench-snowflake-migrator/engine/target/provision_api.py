"""Command/body builders for the DOCUMENTED AIDP data-plane API. Pure.

The structure-clone transport (`executor.py`) speaks the legacy path family
(`/20240831/dataLakes/...`) that one live migration verified, so it is kept
as-is until a live run proves the new one. Everything NEW — workspaces,
clusters, libraries, jobs, async operations, testConnection, workspace
contents — is built HERE against the contract Oracle currently documents:

    https://aidp.{region}.oci.oraclecloud.com/20260430/aiDataPlatforms/{id}/...

What this module encodes, and how each part stands after the 2026-09-16 live
validation campaign:

  * **LIVE-VERIFIED** — async creates answer 202/201 with an
    `aidp-async-operation-key` header, and `GET /asyncOperations/{key}`
    reports SUCCEEDED/FAILED with error fields. `catalogType` is
    `EXTERNAL | INTERNAL`. Jobs, job runs, task runs and `fetchOutput` all
    work, in the shapes below. `POST /actions/testConnection` works but
    needs an EXISTING catalog key (it resolves RBAC `DESCCATALOG` on it).
  * **LIVE-VERIFIED, THE HARD WAY** — workspace FILES do not go through the
    Jupyter contents API on the validated build (200 on PUT, then 404/500 on
    read-back, and it cannot create directories). The working surface is
    `workspace-object` through the `aidp` CLI, with RELATIVE paths.
    DELETING one is the exception: `aidp workspace-object delete` drops the
    object path into the URI unencoded, so its slashes become path segments
    and the call comes back `NotAuthorizedOrNotFound`. The working route is
    `oci raw-request --http-method DELETE` against
    `.../workspaces/{ws}/objects/{path}` with the path **percent-encoded as a
    single segment** (`a%2Fb%2Fc.json`), which returns `204`. Live-verified
    2026-09-19.
  * **STILL INFERRED** — the per-type cluster-library item payload.
    Documented envelope, undocumented artifact field.

Every command is printed before it runs.

Two backends, by necessity rather than preference: the REST calls go through
`oci raw-request`, and the workspace-file calls go through the `aidp` CLI,
whose flags were established against the installed 4.2.1 client.
"""
from __future__ import annotations

import json
from urllib.parse import quote

from .coords import region_from_ocid
from .executor import paged_uri

__all__ = ["PROVISION_API_VERSION", "ProvisionBackendUnsupported",
           "build_provision_command",
           "build_workspace_body", "build_cluster_body",
           "build_library_items", "build_job_body", "build_job_run_body",
           "build_driver_notebook",
           "build_test_connection_body", "content_path"]

PROVISION_API_VERSION = "20260430"


class ProvisionBackendUnsupported(RuntimeError):
    """Only oci_raw is wired for provisioning; the aidp CLI flags are TBD."""


def _base(platform_ocid: str) -> str:
    region = region_from_ocid(platform_ocid)
    return (f"https://aidp.{region}.oci.oraclecloud.com/"
            f"{PROVISION_API_VERSION}/aiDataPlatforms/{platform_ocid}")


def content_path(*segments: str) -> str:
    """A workspace contents path: absolute, single slashes, no blanks."""
    cleaned = [s.strip("/") for s in segments if s and s.strip("/")]
    return "/" + "/".join(cleaned)


def build_workspace_body(display_name: str, *, description: str = "",
                         default_catalog_key: str | None = None,
                         subnet_id: str | None = None) -> dict:
    body: dict = {"displayName": display_name}
    if description:
        body["description"] = description
    if default_catalog_key:
        body["defaultCatalogKey"] = default_catalog_key
    if subnet_id:
        body["networkConfigurationDetails"] = {"subnetId": subnet_id}
    return body


# The AIDP compute shape. NOT an OCI VM shape: `VM.Standard.E4.Flex` is
# rejected ("shape is not supported for driver shape for Create compute
# cluster"), and every cluster on the validated deployment -- including the
# platform's own default one -- runs `amd.generic`. Live-established.
DEFAULT_SHAPE = "amd.generic"

# `clusterRuntimeConfig.sparkVersion` is required ("Missing sparkVersion") and
# is the RUNTIME version, not the API version. 3.5.0 is what the validated
# deployment's own clusters run, and it matches the documented Spark 3.5 /
# Delta 3.2 runtime.
DEFAULT_SPARK_VERSION = "3.5.0"


def build_cluster_body(display_name: str, *,
                       cluster_type: str = "USER",
                       shape: str = DEFAULT_SHAPE,
                       ocpus: int = 2, memory_gbs: int = 32,
                       min_workers: int = 1, max_workers: int = 2,
                       spark_version: str = DEFAULT_SPARK_VERSION) -> dict:
    """CreateClusterDetails, in the shape the API enumerated for us.

    LIVE-VERIFIED, one 400 at a time: both `driverConfig` AND `workerConfig`
    are required ("Worker config with worker shape, minWorkerCount, and
    MaxWorkerCount must be provided. Driver config with driver shape also
    must be provided"); the shape is an AIDP compute family, not an OCI VM
    shape; and `clusterRuntimeConfig.sparkVersion` is required too
    ("Missing sparkVersion").

    The defaults are small and uniform on purpose: the requirement was
    same-name clusters on the AIDP default config, with sizing left as a
    separate decision (`COMPUTE_PROPOSAL.md`). Nothing here reads a Snowflake
    warehouse size.
    """
    shape_config = {"ocpus": ocpus, "memoryInGBs": memory_gbs, "gpus": 0}
    return {"displayName": display_name,
            "type": cluster_type,
            "driverConfig": {"driverShape": shape,
                             "driverShapeConfig": dict(shape_config)},
            "workerConfig": {"workerShape": shape,
                             "workerShapeConfig": dict(shape_config),
                             "minWorkerCount": min_workers,
                             "maxWorkerCount": max_workers},
            "clusterRuntimeConfig": {"sparkVersion": spark_version}}


def build_library_items(*, pypi: list[str] = (),
                        workspace_files: list[str] = (),
                        maven: list[str] = ()) -> dict:
    """The PATCH /libraries body.

    `operation` and `type` are documented; the FIELD CARRYING THE ARTIFACT
    NAME is not (inferred: `package` for PYPI, `path` for WORKSPACE_FILE,
    `coordinates` for MAVEN). Confirm on the first live run.
    """
    items: list[dict] = []
    items += [{"operation": "INSTALL", "type": "PYPI", "package": p}
              for p in pypi]
    items += [{"operation": "INSTALL", "type": "WORKSPACE_FILE", "path": p}
              for p in workspace_files]
    items += [{"operation": "INSTALL", "type": "MAVEN", "coordinates": m}
              for m in maven]
    if not items:
        raise ValueError("no library items; refusing to PATCH an empty change")
    return {"items": items}


def build_job_body(name: str, *, notebook_path: str, cluster_key: str,
                   max_concurrent_runs: int = 1,
                   task_parameters: dict[str, str] | None = None) -> dict:
    """One job, one NOTEBOOK task, MANUAL (no schedule): migrations are
    driven runs, not crons.

    LIVE-VERIFIED SHAPE (2026-09-16, one 400 at a time against a real
    deployment, then a SUCCESS run): a task carries `runIf` (required) and
    its own `cluster: {clusterKey}`; a NOTEBOOK_TASK carries `notebookPath` +
    `source: WORKSPACE`. PYTHON_TASK (`filePath`) was accepted at creation
    and then failed every run with "Unexpected error ... using file" -- .py
    resolution from workspace objects does not work on the validated build,
    which is why jobs here are DRIVER NOTEBOOKS generated by provisioning.
    Top-level job `parameters` were probed on a live run and reach the
    notebook neither as argv nor as environment, so the driver notebook
    carries its arguments INLINE, regenerated when they change -- auditable
    in the console, editable by hand.

    That probe looked in argv and the environment only. The platform's own
    route is `oidlUtils.parameters.getParameter(name, default)` inside the
    notebook (the getting-started samples use it), and a NOTEBOOK_TASK
    takes `parameters: [{name, value}]` (the aidp CLI's create-job schema).
    `task_parameters` are sent that way, and every generated stage notebook
    reads them over its PARAMS literals -- which is what lets ONE notebook
    back one workflow per schema. LIVE-VERIFIED 2026-09-29: a task passing
    `schema=PROBE_OK` was read back by getParameter in the notebook within
    20 s; the same names were NOT present in the environment."""
    task: dict = {"type": "NOTEBOOK_TASK", "taskKey": name,
                  "notebookPath": notebook_path,
                  "source": "WORKSPACE",
                  "runIf": "ALL_SUCCESS",
                  "cluster": {"clusterKey": cluster_key}}
    if task_parameters:
        task["parameters"] = [{"name": k, "value": str(v)}
                              for k, v in task_parameters.items()]
    return {"name": name,
            "maxConcurrentRuns": max_concurrent_runs,
            "tasks": [task]}


def build_driver_notebook(script_workspace_path: str,
                          arguments: dict[str, str]) -> dict:
    """A one-cell driver notebook: set argv, runpy the script off /Workspace.

    /Workspace is the live-verified mount of the workspace tree on cluster
    filesystems. The arguments are BAKED IN because job parameters do not
    reach the notebook (probed live); the trade-off is deliberate — the exact
    invocation is visible and editable in the console, and provisioning
    regenerates the driver when defaults change.
    """
    argv = []
    for key, value in sorted(arguments.items()):
        argv += [f"--{key}", str(value)]
    src = (
        "# Generated by the snowflake-migrator provisioner. Edit the ARGS\n"
        "# below to change this job's run; the script itself is at SCRIPT.\n"
        f"SCRIPT = {'/Workspace/' + script_workspace_path.strip('/')!r}\n"
        f"ARGS = {argv!r}\n"
        "import os, runpy, sys\n"
        "# The scripts import a shared module (snowmig_source) that sits\n"
        "# beside them on the /Workspace mount.\n"
        "sys.path.insert(0, os.path.dirname(SCRIPT))\n"
        "sys.argv = [SCRIPT] + ARGS\n"
        "print('running', SCRIPT, ARGS, flush=True)\n"
        "# A script's sys.exit(0) raises SystemExit, which a notebook cell\n"
        "# reports as an error. So the exit CODE decides, and a non-zero\n"
        "# one is re-raised so the job still fails when the work did.\n"
        "try:\n"
        "    runpy.run_path(SCRIPT, run_name='__main__')\n"
        "    code = 0\n"
        "except SystemExit as exit_request:\n"
        "    code = int(exit_request.code or 0)\n"
        "print('exit code:', code, flush=True)\n"
        "if code:\n"
        "    raise RuntimeError(f'{SCRIPT} exited {code}')\n")
    return {"cells": [{"cell_type": "code", "execution_count": None,
                       "metadata": {}, "outputs": [], "source": [src]}],
            "metadata": {"snowmig": {"generated": True,
                                     "script": script_workspace_path,
                                     "arguments": dict(sorted(arguments.items()))}},
            "nbformat": 4, "nbformat_minor": 5}


def build_job_run_body(job_key: str,
                       parameters: dict[str, str] | None = None) -> dict:
    body: dict = {"jobKey": job_key}
    if parameters:
        body["parameters"] = [{"name": k, "value": str(v)}
                              for k, v in sorted(parameters.items())]
    return body


def build_test_connection_body(key: str, *, source_type: str = "SNOWFLAKE",
                               connection_properties: dict | None = None,
                               display_name: str | None = None) -> dict:
    body: dict = {"key": key, "sourceType": source_type}
    if connection_properties is not None:
        body["connectionDetails"] = {
            "connectionProperties": dict(connection_properties)}
        if display_name:
            body["connectionDetails"]["displayName"] = display_name
    return body


def build_provision_command(backend: str, operation: str, platform_ocid: str,
                            **kwargs) -> list[str]:
    """The argv for one provisioning operation. Pure — runs nothing."""
    if backend != "oci_raw":
        raise ProvisionBackendUnsupported(
            f"backend {backend!r}: provisioning runs on the oci_raw backend "
            f"(`oci raw-request`) only.")
    base = _base(platform_ocid)

    def raw(method: str, uri: str, body: dict | None = None,
            body_file: str | None = None,
            page: str | None = None) -> list[str]:
        # `page` is the `opc-next-page` token of the previous list response;
        # None leaves the URI exactly as it was.
        cmd = ["oci", "raw-request", "--http-method", method,
               "--target-uri", paged_uri(uri, page)]
        if body_file:
            cmd += ["--request-body", f"file://{body_file}"]
        elif body is not None:
            cmd += ["--request-body", json.dumps(body)]
        return cmd

    ws = kwargs.get("workspace")
    page = kwargs.get("page")

    if operation == "list_workspaces":
        return raw("GET", f"{base}/workspaces", page=page)
    if operation == "create_workspace":
        return raw("POST", f"{base}/workspaces", kwargs["body"])
    if operation == "get_async_operation":
        return raw("GET", f'{base}/asyncOperations/{kwargs["key"]}')

    if operation == "list_clusters":
        return raw("GET", f"{base}/workspaces/{ws}/clusters", page=page)
    if operation == "create_cluster":
        return raw("POST", f"{base}/workspaces/{ws}/clusters", kwargs["body"])
    if operation == "delete_cluster":
        return raw("DELETE", f"{base}/workspaces/{ws}/clusters/"
                             f'{kwargs["cluster"]}')
    if operation in ("start_cluster", "stop_cluster", "restart_cluster"):
        action = operation.split("_", 1)[0]
        return raw("POST", f"{base}/workspaces/{ws}/clusters/"
                           f'{kwargs["cluster"]}/actions/{action}', {})
    if operation == "list_libraries":
        return raw("GET", f"{base}/workspaces/{ws}/clusters/"
                          f'{kwargs["cluster"]}/libraries', page=page)
    if operation == "install_libraries":
        return raw("PATCH", f"{base}/workspaces/{ws}/clusters/"
                            f'{kwargs["cluster"]}/libraries', kwargs["body"])

    # --- workspace files: the LIVE-VERIFIED surface is `workspace-object`,
    # driven through the aidp CLI whose flags were validated 2026-09-16.
    # (The Jupyter contents API on a live build returned 200 on PUT and then
    # 500/404 on read-back -- a 2xx that is not the claim, again -- and its
    # VolumeContentsManager cannot create directories at all.)
    # Paths are workspace-RELATIVE: a leading slash is a live 400.
    region = region_from_ocid(platform_ocid)

    def aidp_cli(*args: str) -> list[str]:
        return ["aidp", *args, "--instance-id", platform_ocid,
                "--auth", str(kwargs.get("cli_auth") or "api_key"),
                "--region", region]

    if operation == "create_ws_folder":
        return aidp_cli("workspace-object", "create", ws,
                        "--type", "FOLDER", "--path", kwargs["path"],
                        "--body", "{}")
    if operation == "upload_ws_file":
        return aidp_cli("workspace-object", "create", ws,
                        "--type", str(kwargs.get("object_type") or "FILE"),
                        "--path", kwargs["path"],
                        "--is-overwrite",
                        "--body", f'@{kwargs["local_path"]}')
    if operation == "list_ws_objects":
        return aidp_cli("workspace-object", "list", ws,
                        "--path", kwargs["path"])
    if operation == "download_ws_file":
        # What the console itself does (HAR, 2026-09-29): POST
        # .../workspaces/{ws}/actions/downloadFileMeta with the path in a
        # header, answered with a short-lived pre-authenticated `parUrl`
        # that a plain GET then reads. `workspace-object get` answers 404
        # NotAuthorizedOrNotFound for the same file, so it is not the route.
        return aidp_cli("workspace-object", "download-with-par", ws,
                        "--path", kwargs["path"], "--type", "FILE",
                        "--should-generate-new-par")

    if operation == "list_jobs":
        return raw("GET", f"{base}/workspaces/{ws}/jobs", page=page)
    if operation == "create_job":
        return raw("POST", f"{base}/workspaces/{ws}/jobs", kwargs["body"])
    if operation == "run_job":
        return raw("POST", f"{base}/workspaces/{ws}/jobRuns", kwargs["body"])
    if operation == "get_job_run":
        return raw("GET", f'{base}/workspaces/{ws}/jobRuns/{kwargs["key"]}')
    if operation == "list_job_runs":
        # `sortBy` is REQUIRED: omitting it is a live
        # `400 WORKFLOW_0007 ... Possible cause: Invalid SortBy: null`, and
        # `startTime` is rejected as a sort key even though it is a field on
        # every run. `timeCreated` works.
        return raw("GET", f'{base}/workspaces/{ws}/jobRuns'
                          f'?jobKey={kwargs["job_key"]}&sortBy=timeCreated',
                   page=page)
    if operation == "cancel_job_run":
        # Live-verified (2026-09-19): answers 202, and the run reads CANCELED
        # on the next poll. Used by the cold-start watchdog in jobs.py to let
        # go of a run the cluster never picked up.
        return aidp_cli("workflow", "cancel-job-run", ws, kwargs["run_key"])
    if operation == "delete_job":
        # Live-verified (2026-09-29): answers 204 and the job leaves the
        # listing. Used only for copy jobs the approved plan no longer names,
        # and only when the operator asked (provision
        # --delete-stale-copy-jobs).
        return aidp_cli("workflow", "delete-job", ws, kwargs["job_key"])
    if operation == "get_job":
        # The job definition, tasks and their `parameters` included (the
        # aidp CLI's `workflow get-job`). Read by `run` to check task
        # parameter names before a run is paid for.
        return raw("GET", f'{base}/workspaces/{ws}/jobs/{kwargs["job_key"]}')
    if operation == "list_task_runs":
        # `sortBy` is REQUIRED: without it the call fails "Invalid SortBy:
        # null" (live).
        return raw("GET", f"{base}/workspaces/{ws}/taskRuns"
                          f'?jobRunKey={kwargs["run_key"]}'
                          f"&sortBy=timeCreated", page=page)
    if operation == "get_task_run":
        # The one read that carries a failed task's `errorTrace`: the job
        # run's envelope says only "Exception during execution of notebook",
        # and the task-run list items omit it (live 2026-09-29).
        return raw("GET", f"{base}/workspaces/{ws}/taskRuns/"
                          f'{kwargs["task_run_key"]}')
    if operation == "fetch_task_output":
        return raw("POST", f"{base}/workspaces/{ws}/taskRuns/"
                           f'{kwargs["task_run_key"]}/actions/fetchOutput',
                   {})

    if operation == "test_connection":
        # The body carries the Snowflake credential; the transport spools it
        # and passes `body_file`, which `raw()` prefers over the inline body.
        return raw("POST", f"{base}/actions/testConnection",
                   kwargs.get("body"), body_file=kwargs.get("body_file"))

    # --- teardown --scope credential|all. Each shape below ran live on the
    # validated DataLake on 2026-09-29 before it was wired here.
    if operation == "delete_ws_object":
        # `aidp workspace-object delete` drops the path into the URI
        # unencoded and answers 404. The path percent-encoded as ONE segment
        # (`a%2Fb%2Fc.json`) answers 204, and the object leaves the listing.
        return raw("DELETE", f"{base}/workspaces/{ws}/objects/"
                             f'{quote(kwargs["path"].lstrip("/"), safe="")}')
    if operation == "delete_workspace":
        # 204 with an async operation key in a header; the operation reads
        # SUCCEEDED about a minute later, and the workspace takes its files
        # with it.
        return raw("DELETE", f"{base}/workspaces/{ws}")
    if operation == "delete_catalog":
        # 202 with an async operation key. `is-forced` is a request HEADER
        # (the aidp client sends it as one): as a query parameter
        # (`?force=true`, `?isCascade=true`) it is ignored, and an INTERNAL
        # catalog holding schemas stayed ACTIVE. `--is-forced` deleted one
        # with its tables (SUCCEEDED in ~130 s); an EXTERNAL catalog needs
        # no force.
        return aidp_cli("catalog", "delete", kwargs["catalog"],
                        *(["--is-forced"] if kwargs.get("forced") else []))
    if operation == "list_catalogs":
        # The listing `snowmig catalogs` reads (the dataLakes family).
        return raw("GET", f"https://aidp.{region}.oci.oraclecloud.com/"
                          f"20240831/dataLakes/{platform_ocid}/catalogs",
                   page=page)

    raise ValueError(f"unknown provisioning operation {operation!r}")
