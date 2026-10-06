"""Provision the migration environment inside AIDP. I/O injected as `call`.

The one-time setup the prod flow needs, in dependency order:

  1. the WORKSPACE (name translated by `naming.translate_name`, so a name the
     API might reject never reaches it);
  2. the migration CLUSTER (default `migration-assets`, small default config —
     sizing is a later, explicit decision);
  3. cluster LIBRARIES, when the fallback paths need any (PyPI/Maven; a
     restart follows, as the doc requires);
  4. the workspace folder `backup-snowflake-migration/` with the
     data-migration SCRIPTS and the migration PLAN artifacts;
  5. four parametrised JOBS (discover / structure / copy / reconcile) wired
     to those scripts — created paused-by-absence-of-schedule: running one is
     always a human's call.

The EXTERNAL catalog itself is NOT registered here — that is the existing
`snowmig.py catalog` stage, one writer per concern.

Discipline is the house discipline: look first, create only if absent, poll
the read-back with a bounded backoff, and record per step
`{step, action, verified, detail}`. Dry run by default; nothing reaches AIDP
without `execute=True`. Every underlying REST shape is the DOCUMENTED
20260430 contract (see provision_api.py) and remains ⚠️ unverified live.
"""
from __future__ import annotations

import datetime
import gzip
import hashlib
import json
import os
import pathlib
import re
import shutil
import tempfile
import time
from typing import Callable

from retry import is_retryable, retry_call
from migration_config import ConfigError, load_config, snowflake_block
from plan.preflight import SECRET_PATH_FIELDS

from .naming import translate_name
from .runner import is_active, is_conflict
from .stage_notebooks import (
    DIAGNOSE_NOTEBOOK_NAME, STAGES, build_diagnose_notebook,
    build_stage_notebook, check_stage_params)
from .provision_api import (
    build_cluster_body, build_job_body,
    build_library_items, build_provision_command, build_workspace_body,
)

__all__ = ["JOB_SPECS", "SCRIPTS_FOLDER", "PLAN_FOLDER", "REPORTS_FOLDER",
           "BACKUP_FOLDER", "PLAN_BACKUP_FILES", "plan_backup_names",
           "COPY_JOB_PREFIX", "plan_copy_schemas", "copy_job_specs",
           "DOWNLOAD_TIMEOUT", "PLAN_PUSH_FILES", "plan_push_inputs",
           "download_ws_file", "carry_forward",
           "ProvisionTransportError",
           "make_provision_call", "provision", "render_provision",
           "source_config_payload",
           "async_operation_key", "connection_test_outcome"]


class ProvisionTransportError(RuntimeError):
    """A provisioning call failed. Named so the CLI can report it as a
    message with the partial result intact, rather than a traceback that
    loses the record of what was already created."""

# Workspace-object paths are RELATIVE (a leading slash is a live 400).
_ROOT = "backup-snowflake-migration"
SCRIPTS_FOLDER = f"{_ROOT}/scripts"
PLAN_FOLDER = f"{_ROOT}/plan"
# What the SCRIPTS receive as --reports-dir: /Workspace is the live-verified
# mount of the workspace tree on cluster filesystems (probed on a real run).
REPORTS_FOLDER = f"/Workspace/{_ROOT}/reports"
# Runbook S6 backs the discovery manifest up here, dated, BEFORE any later
# stage reads it, and S9 backs the full plan up here before scope is reduced.
# Provisioning used to create scripts/, plan/ and reports/ only, so the first
# thing that tried to write a backup found no folder to write it into.
BACKUP_FOLDER = f"{_ROOT}/backup"

# One job per NOTEBOOK. The names mirror the stage flags (sans `--`) and are
# written into the notebook's own PARAMS cell as defaults; a job TASK's
# `parameters` override them at run time through
# oidlUtils.parameters.getParameter (live-verified; see stage_notebooks).
#
# There is no longer a driver wrapper. Each stage is a single self-contained
# `.ipynb` -- parameters, helpers and logic in one object -- so the code a
# user opens in the console is exactly the code the job runs. AIDP types a
# workspace object by extension (`.py` -> FILE, `.ipynb` -> NOTEBOOK) and a
# job task needs a NOTEBOOK, so `.ipynb` is not a preference here.
#
# `source-mode` defaults to `connector`: reading Snowflake directly from the
# cluster is the path proven end to end (a table read and a pushdown), and it
# needs no successful catalog crawl. Pass `--source-mode external-catalog`
# plus `--external-catalog` when the crawl works.
JOB_SPECS: tuple[dict, ...] = (
    {"name": "snowmig_00_discover", "notebook": "00_discover_snowflake.ipynb",
     "parameters": ("source-mode", "source-config", "source-catalog",
                    "reports-dir", "backup-dir")},
    {"name": "snowmig_01_structure", "notebook": "01_create_structure.ipynb",
     "parameters": ("source-mode", "source-config", "source-catalog",
                    "target-catalog", "reports-dir")},
    {"name": "snowmig_02_copy_schema", "notebook": "02_copy_schema.ipynb",
     "parameters": ("source-mode", "source-config", "source-catalog",
                    "target-catalog", "schema", "reports-dir")},
    {"name": "snowmig_03_reconcile", "notebook": "03_reconcile.ipynb",
     "parameters": ("target-catalog", "reports-dir")},
)

# The shared helpers are INLINED into each generated notebook (see
# target/stage_notebooks.py), so nothing is uploaded beside them and no
# notebook depends on a module sitting on the /Workspace mount.


# The per-schema copy workflows (runbook S11): ONE job per source schema,
# each with ONE task running THE SAME 02_copy_schema notebook and passing
# its schema as a task parameter. The notebook reads it at run time with
# oidlUtils.parameters.getParameter (see the PARAMS cell the notebooks are
# generated with), so there is one script, and each schema still gets its
# own job, run history and evidence in the console.
COPY_STAGE_NOTEBOOK = "02_copy_schema.ipynb"
COPY_JOB_PREFIX = "snowmig_02_copy_"


def plan_copy_schemas(ddl_plan: dict) -> list[str]:
    """The source schemas an approved ddl_plan moves tables for, sorted.

    The same reading 01_create_structure and 02_copy_schema apply: a TABLE
    statement with a three-part `source_identifier`; its middle part is the
    value `--schema` takes. Views are not copied, so they add no job.
    """
    out = set()
    for stmt in ddl_plan.get("statements") or []:
        source = str(stmt.get("source_identifier") or "").split(".")
        if len(source) != 3:
            continue
        if str(stmt.get("object_type") or "TABLE").upper() == "VIEW":
            continue
        out.add(source[1])
    return sorted(out)


# The plan artifacts a push carries into plan/, whichever of them exist.
PLAN_PUSH_FILES = ("inventory.json", "plan.json", "ddl_plan.json",
                   "PLANNED_OBJECTS.md", "DDL_PLAN.md", "SUMMARY.md")


def plan_push_inputs(out_dir) -> tuple[list[pathlib.Path], list[str]]:
    """(plan files, copy schemas) that a provision push of `out_dir` takes.

    Whatever plan artifacts exist travel with the scripts, and the copy
    schemas are read from the APPROVED ddl_plan.json among them -- no plan
    yet, no copy jobs. One reading for `snowmig provision` and the demo, so
    the demo cannot drift back to the pre-plan shape it once showed.
    """
    out = pathlib.Path(out_dir)
    files = [out / n for n in PLAN_PUSH_FILES if (out / n).is_file()]
    ddl = out / "ddl_plan.json"
    if not ddl.is_file():
        return files, []
    try:
        plan = json.loads(ddl.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        # Name the file and the remedy, as snowmig._read does: the decoder's
        # own message says where in the text, not which artifact.
        raise ValueError(
            f"{ddl} is not valid JSON ({exc}); delete it and re-run the "
            f"stage that produces it (`snowmig ddl`)") from exc
    return files, plan_copy_schemas(plan)


def copy_job_specs(schemas) -> list[dict]:
    """One job spec per schema: its job name, the shared notebook, and the
    task parameter that scopes it to the schema.

    Names go through the same safe-charset translation as every other AIDP
    name. Two schemas that translate to the same name (`A-B` and `A_B`)
    would share a job and a notebook, so that is refused rather than
    resolved by guessing which one wins.

    A name equal to a stage job's -- a schema named SCHEMA would get
    `snowmig_02_copy_schema`, the generic parameterless copy job -- is
    disambiguated to `snowmig_02_copy_schema_<slug>`, so the plan's job is
    never skipped as "the generic one" nor adopted from it.
    """
    stage_jobs = {spec["name"] for spec in JOB_SPECS}
    specs, seen = [], {}
    for schema in schemas:
        slug = translate_name(schema, kind="schema").name
        name = f"{COPY_JOB_PREFIX}{slug}"
        if name in stage_jobs:
            name = f"{COPY_JOB_PREFIX}schema_{slug}"
        if name in seen:
            raise ValueError(
                f"schemas {seen[name]!r} and {schema!r} both give the copy "
                f"job name {name!r}, so their copy jobs would collide. Scope "
                f"one of them out with --restrictions and run it as its own "
                f"wave.")
        seen[name] = schema
        specs.append({"name": name,
                      "notebook": COPY_STAGE_NOTEBOOK,
                      "task_parameters": {"schema": schema}})
    return specs


def _listed_task_parameters(job: dict) -> dict | None:
    """The task parameters a job listing carries, when it carries them
    (None when it does not: the listing may be a summary)."""
    tasks = job.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        return None
    params = (tasks[0] or {}).get("parameters")
    if not isinstance(params, list):
        return None
    return {str(p.get("name")): str(p.get("value")) for p in params
            if isinstance(p, dict)}


def make_provision_call(platform_ocid: str, *, backend: str = "oci_raw",
                        run_process=None) -> Callable[..., dict]:
    """A `call(operation, **kwargs) -> dict` over the documented API."""
    import subprocess

    from .coords import region_from_ocid
    from .runner import (DEFAULT_CLI_TIMEOUT, _printable, _session_expired,
                         expired_session_message, spool_body)
    from .executor import collect_pages, parse_cli_envelope

    def _run(cmd):
        # A spawned CLI must not inherit variables that repoint its own
        # interpreter; see cli_environment in snowmig.py.
        env = dict(os.environ)
        for name in ("PYTHONHOME", "PYTHONPATH", "PYTHONUSERBASE",
                     "PYTHONNOUSERSITE", "PYTHONSTARTUP",
                     "PYTHONEXECUTABLE", "PYTHONSAFEPATH"):
            env.pop(name, None)
        # Bounded: a child that never returns used to hold this stage
        # open indefinitely while a cluster billed (live 2026-09-24).
        return subprocess.run(cmd, capture_output=True, text=True,
                              check=False, encoding="utf-8",
                              errors="replace", env=env,
                              timeout=DEFAULT_CLI_TIMEOUT)

    runner = run_process or _run

    def _once(operation: str, kwargs: dict) -> tuple[list[dict], dict]:
        """One request: its rows and its response headers (lower-cased)."""
        spooled = None
        # File CONTENT never goes through this transport at all: uploads use
        # the `workspace-object` surface, which takes a local path. A body
        # carrying `content` would mean someone reintroduced the Jupyter
        # contents path, so refuse rather than spool it into argv.
        body = kwargs.get("body")
        if isinstance(body, dict) and "content" in body:
            raise ValueError(
                "this transport does not carry file content; upload through "
                "the workspace-object operations (upload_ws_file), which take "
                "a local path")
        # `connectionDetails` is the Snowflake credential -- the testConnection
        # body carries the same password or PEM the catalog registration did.
        # It travels by file, exactly like create_catalog's body, so it is
        # never an argv element for `ps` or process auditing to record.
        if isinstance(body, dict) and "connectionDetails" in body:
            spooled = spool_body(body, prefix="snowmig_testconn_")
            kwargs = {**kwargs, "body_file": spooled}
        try:
            cmd = build_provision_command(backend, operation, platform_ocid,
                                          **kwargs)

            def attempt():
                print("  $ " + " ".join(_printable(c) for c in cmd))
                proc = runner(cmd)
                if proc.returncode != 0:
                    if _session_expired(proc.stdout, proc.stderr):
                        raise ProvisionTransportError(
                            f"{operation}: "
                            + expired_session_message(
                                None, region_from_ocid(platform_ocid)))
                    raise ProvisionTransportError(
                        f"{operation} failed (exit {proc.returncode}): "
                        f"{(proc.stderr or proc.stdout or '')[:300]}")
                # Parsed INSIDE the attempt: `oci raw-request` exits 0 on an
                # HTTP error and carries it in the body, so a 429/503 is only
                # an exception once the envelope is read -- parsed after
                # retry_call it was never retried. The aidp CLI's literal
                # "Response:" prefix is stripped by the parser; the headers
                # come back with the rows because a list endpoint names its
                # next page in one of them.
                return parse_cli_envelope(proc.stdout or "")
            # Includes the workspace->cluster race: a create answered 409
            # "not in an active state" was not applied, so it is repeated
            # with backoff instead of leaving the run partial.
            rows, headers = retry_call(
                attempt, label=operation,
                retryable=is_retryable(
                    read=operation.startswith(("get_", "list_"))))
        finally:
            if spooled:
                try:
                    os.unlink(spooled)
                except OSError:
                    pass
        if headers.get("opc-next-page") and cmd[0] == "aidp":
            # The workspace-object listing rides the aidp CLI, whose paging
            # flags are undocumented. Page one handed back as the whole would
            # read every object past it as absent; refuse and say why.
            raise ProvisionTransportError(
                f"{operation}: the aidp CLI answered with a next-page token, "
                f"so this listing is only its first page and the rest cannot "
                f"be requested through that CLI; the listing is incomplete "
                f"and was not used.")
        return rows, headers

    def call(operation: str, **kwargs) -> dict:
        if operation.startswith("list_"):
            # A collection may span pages: `opc-next-page` is followed until
            # the server stops sending one, so jobs.in_flight_runs and every
            # look-first check see the whole collection.
            def fetch(page):
                rows, headers = _once(operation, {**kwargs, "page": page}
                                      if page else kwargs)
                return rows, headers.get("opc-next-page")

            try:
                items = collect_pages(fetch, operation)
            except ProvisionTransportError:
                raise
            except RuntimeError as exc:
                raise ProvisionTransportError(str(exc)) from exc
            return {"items": items}
        rows, headers = _once(operation, kwargs)
        row = rows[0] if rows else {}
        # An async action answers 202 with an EMPTY body and its operation
        # key in a response header. The parser cannot put a header into a
        # row, so the transport keeps them beside it, under `_headers`, for
        # async_operation_key to read.
        if headers and isinstance(row, dict):
            row = dict(row, _headers=headers)
        return row

    return call


# Where the async operation key of a 202 may ride. `aidp-async-operation-key`
# is the header live-verified on the validated deployment; the documented
# testConnection contract names `oidl-async-operation-key` and
# `datalake-async-operation-key`; `opc-work-request-id` is the OCI-wide
# convention. All four are read, headers first, case-insensitively.
_ASYNC_KEY_HEADERS = ("aidp-async-operation-key",
                      "datalake-async-operation-key",
                      "oidl-async-operation-key", "opc-work-request-id")
_ASYNC_TERMINAL = ("SUCCEEDED", "SUCCESS", "FAILED", "CANCELED", "CANCELLED")


def async_operation_key(payload: dict) -> str | None:
    """The async operation key a 202 carried, wherever the envelope put it.

    `oci raw-request` prints `{"data": <body>, "headers": {...}, "status"}`.
    For an empty body the key rides in a HEADER, which make_provision_call
    keeps under `_headers`; when the parser returned the whole envelope (a
    null body) the headers sit under `headers`; a body may also carry it as
    `key`, at the top level or under `data`. Reading it at the top level of
    the parsed row only -- as the catalog stage once did -- found nothing in
    any of these shapes, so the poll never ran and every test reported
    PENDING.
    """
    if not isinstance(payload, dict):
        return None
    for headers in (payload.get("_headers"), payload.get("headers")):
        if isinstance(headers, dict):
            lowered = {str(k).lower(): v for k, v in headers.items()}
            for name in _ASYNC_KEY_HEADERS:
                if lowered.get(name):
                    return str(lowered[name])
    for name in _ASYNC_KEY_HEADERS:
        if payload.get(name):
            return str(payload[name])
    data = payload.get("data")
    if isinstance(data, dict) and data.get("key"):
        return str(data["key"])
    if payload.get("key"):
        return str(payload["key"])
    return None


def connection_test_outcome(call: Callable[..., dict], probe: dict, *,
                            delays: tuple[float, ...] = (5.0, 10.0, 15.0,
                                                         20.0, 30.0),
                            sleep: Callable[[float], None] | None = None
                            ) -> dict:
    """The verdict of a testConnection POST, read back through
    `GET /asyncOperations/{key}` with a bounded backoff.

    {requested, status, operation_key, error?, note?}. PENDING means one
    thing: the key was found and the operation had not ended when the poll
    budget ran out. A 202 whose envelope carries no key is reported as
    exactly that -- the verdict cannot be read -- and a poll that fails is
    UNREADABLE with its error. None of these is a pass.
    """
    sleep = sleep or time.sleep
    key = async_operation_key(probe)
    outcome: dict = {"requested": True, "status": "PENDING",
                     "operation_key": key}
    if not key:
        outcome["note"] = (
            "the API accepted the test request but its envelope carried no "
            "async operation key (in a header or the body), so the verdict "
            "cannot be read; PENDING is not a pass")
        return outcome
    last = "PENDING"
    for delay in delays:
        sleep(delay)
        try:
            op = call("get_async_operation", key=key)
        except Exception as exc:
            outcome["status"] = "UNREADABLE"
            outcome["error"] = (f"GET asyncOperations/{key}: "
                                f"{str(exc)[:200]}")
            return outcome
        last = str(op.get("status") or op.get("lifecycleState")
                   or "PENDING").upper()
        if last in _ASYNC_TERMINAL:
            outcome["status"] = last
            if op.get("errorCode") or op.get("errorMessage"):
                outcome["error"] = (f'{op.get("errorCode")}: '
                                    f'{op.get("errorMessage")}')
            return outcome
    outcome["note"] = (
        f"the operation still reported {last} after {len(delays)} polls "
        f"over {sum(delays):g}s, so the verdict was not readable within "
        f"the budget; PENDING is not a pass -- re-check operation {key}")
    return outcome


def _match(items: list[dict], display_name: str) -> dict | None:
    wanted = display_name.strip().lower()
    for item in items or []:
        name = str(item.get("displayName") or item.get("name")
                   or item.get("key") or "").lower()
        if name == wanted:
            return item
    return None


def _prior_created_jobs(prior: dict | None, ws_key: str | None,
                        datalake_ocid: str | None) -> list[dict]:
    """The jobs an earlier EXECUTED record of this out dir proves this
    migration created on workspace `ws_key`: its `created_jobs`, plus -- for
    a record that predates that field -- every job step or copy job it
    recorded as `created`. A record of another workspace key, or of another
    aiDataPlatform, proves nothing about this one."""
    if not prior or prior.get("dry_run") is not False or not ws_key:
        return []
    if (prior.get("workspace") or {}).get("key") != ws_key:
        return []
    if (datalake_ocid and prior.get("datalake_ocid")
            and prior["datalake_ocid"] != datalake_ocid):
        return []
    deleted = {str(n).lower() for n in prior.get("deleted_copy_jobs") or []}
    jobs, seen = [], set()

    def add(entry: dict) -> None:
        name = str(entry.get("name") or "")
        if name and name.lower() not in seen and name.lower() not in deleted:
            seen.add(name.lower())
            jobs.append(entry)
    for entry in prior.get("created_jobs") or []:
        add(dict(entry))
    for st in prior.get("steps") or []:
        if st.get("step") == "job" and st.get("action") == "created":
            add({"name": str(st.get("detail") or "").split(" ", 1)[0]
                 .rstrip(":"), "created_run": prior.get("run")})
    for job in prior.get("copy_jobs") or []:
        if job.get("status") == "created":
            add({"name": job.get("job"), "created_run": prior.get("run")})
    return jobs


def _poll(list_fn, display_name: str, delays: tuple[float, ...], *,
          require_active: bool = False) -> tuple[dict | None, str | None]:
    """`(item, listing_error)`: the item once it is visible (and ACTIVE, when
    asked for), else None. With `require_active`, an item that appeared but
    was still settling when the budget ran out is returned as last seen, so
    the caller can tell "never visible" from "visible, not yet ACTIVE".

    `listing_error` is the last error when EVERY listing raised. That is
    "could not look", not "absent": it used to be swallowed, so a 401 or 503
    right after an accepted create was reported as "never became visible"
    with the error recorded nowhere. One good listing is enough to make a
    miss a real miss, so the error is then None."""
    last, error, listed_once = None, None, False
    for attempt in range(len(delays) + 1):
        try:
            found = _match(list_fn().get("items") or [], display_name)
            listed_once = True
        except Exception as exc:
            found, error = None, str(exc)[:200]
        if found is not None:
            last = found
            if not require_active or is_active(found):
                return found, None
        if attempt < len(delays):
            time.sleep(delays[attempt])
    return last, (None if listed_once else error)


def _cluster_state(item: dict | None) -> str | None:
    """A cluster's state, upper-cased, or None when it carries none.

    Clusters report it as `state` (live: CREATING, then ACTIVE), not the
    `lifecycleState` a workspace carries -- which `is_active` reads, and
    which reads as ACTIVE when absent, so a CREATING cluster passed it."""
    value = (item or {}).get("state") or (item or {}).get("lifecycleState")
    return str(value).upper() if value else None


def _key(item: dict, fallback: str) -> str:
    return str(item.get("key") or item.get("id") or fallback)


def source_config_payload(path: pathlib.Path) -> dict:
    """What `--source-config` places on the workspace: the `snowflake:` block
    of the operator's migration config, and nothing else.

    The in-AIDP scripts read only that block (the data-plane loader unwraps
    it), so the `aidp:` half -- the DataLake OCID and the target
    coordinates -- has no business on the mount and is not copied. The
    block carries the credential, which is why the upload is opt-in; a
    copy that carries MORE than the scripts read is exposure for nothing.
    It is written as JSON, which the loader reads without PyYAML.

    A `*_path` secret is refused here, before anything is uploaded. The
    path names a file on THIS machine; the copy is read on the cluster from
    /Workspace/..., where that path does not exist. That failure used to
    surface five minutes later, as a raw FileNotFoundError in the job log,
    after `preflight` had called the path "readable" -- on the laptop.
    """
    block = snowflake_block(load_config(path))
    laptop_only = [f for f in SECRET_PATH_FIELDS if block.get(f)]
    if laptop_only:
        raise ConfigError(
            f"--source-config {path} carries {', '.join(laptop_only)}: a "
            f"path to a file on this machine. The copy placed on the "
            f"workspace is read on the cluster from /Workspace/{PLAN_FOLDER}/, "
            f"where that path does not exist, so connector mode cannot use "
            f"it. Inline the secret under `snowflake:` instead (private_key: "
            f"| for a PEM, password: for a password, token: for a PAT), "
            f"then re-run.")
    return {"snowflake": dict(block)}


def _credential_line(source_name: str, remote: str) -> str:
    return (f"{source_name} -> {remote} — CARRIES THE SNOWFLAKE CREDENTIAL "
            f"(the snowflake: block only; aidp: is not copied). Readable by "
            f"every member of the workspace and by every cluster in it via "
            f"/Workspace; remove it when the migration is done")


# The plan artifacts that are backed up, dated, on every push: the plan
# itself and the DDL plan S10 executes. The rest of plan/ is derived from
# these and is not worth a copy per push.
PLAN_BACKUP_FILES = ("plan.json", "ddl_plan.json")

# A plan file larger than this goes up gzipped as <name>.gz, with a pointer
# under the plain name that the stages follow (snowmig_source.read_plan_json).
# Live 2026-09-29, the 50k-table plan: the workspace upload route took 612 s
# for 64 MB, failed 502 Bad Gateway on ddl_plan.json (274 MB) and
# inventory.json (293 MB), and the failed overwrite left plan/ddl_plan.json
# empty. JSON plans compress 49-75x.
COMPRESS_OVER_BYTES = 16 * 2 ** 20
PLAN_POINTER_KEY = "snowmig_compressed_to"


def _stage_large_uploads(files, staged_dir, *, pointer: bool) -> list[tuple]:
    """(local path, remote name, detail note, needs) per upload, in order.

    A file over COMPRESS_OVER_BYTES becomes its gzip copy `<name>.gz` and --
    when `pointer`, for the plan/ folder the stages read -- a pointer under
    the plain name, listed after the copy and `needs`-ing it, so the plain
    name never points at something that did not land. A plain copy an
    earlier, smaller push left there is replaced by the pointer rather than
    being read as this plan.
    """
    out = []
    for path, name in files:
        path = pathlib.Path(path)
        try:
            size = path.stat().st_size
        except OSError:
            size = 0
        if size <= COMPRESS_OVER_BYTES:
            out.append((path, name, "", None))
            continue
        data = path.read_bytes()
        gz_name = f"{name}.gz"
        gz = pathlib.Path(staged_dir) / gz_name
        gz.write_bytes(gzip.compress(data, compresslevel=6))
        out.append((gz, gz_name,
                    f" ({size / 2 ** 20:.0f} MB, uploaded gzipped: "
                    f"{gz.stat().st_size / 2 ** 20:.1f} MB; the workspace "
                    f"upload route fails on large files)", None))
        if pointer:
            ptr = pathlib.Path(staged_dir) / f"{name}.pointer"
            ptr.write_text(json.dumps({
                PLAN_POINTER_KEY: gz_name, "format": "gzip", "bytes": size,
                "sha256": hashlib.sha256(data).hexdigest(),
                "reason": "too large to upload as is; the stages read the "
                          "plan through this pointer"}, indent=2),
                encoding="utf-8")
            out.append((ptr, name, f" (pointer to {gz_name})", gz_name))
    return out


def plan_backup_names(plan_files, *, stamp: str,
                      label: str | None = None) -> list[tuple]:
    """(local path, backup name) for each plan artifact that is backed up.

    `ddl_plan.json` pushed at 2026-09-29T03:10:00Z with label FULL becomes
    `ddl_plan_20260929T031000Z_FULL.json`. The label is the operator's word
    for which plan this is (FULL before a scope reduction, REDUCED after);
    it is folded to the safe charset, never trusted as a path.
    """
    suffix = ""
    if label:
        safe = re.sub(r"[^A-Za-z0-9_-]+", "_", label).strip("_")
        suffix = f"_{safe}" if safe else ""
    return [(p, f"{pathlib.Path(p).stem}_{stamp}{suffix}.json")
            for p in plan_files if pathlib.Path(p).name in PLAN_BACKUP_FILES]


def _push_plan_folders(call, ws_key: str, step, staged: str,
                       folders: list[tuple[str, list]]) -> None:
    """Upload each folder's plan files, then read the folder back once.

    The read-back is the claim -- a 2xx never was one -- gathered once per
    folder rather than per file (each listing is its own CLI process; the
    live run spent seven). A file too large to upload goes up gzipped
    behind a pointer (_stage_large_uploads); a pointer whose copy did not
    land is not uploaded, and is reported with why.
    """
    for folder, files in folders:
        kind = "backup" if folder == BACKUP_FOLDER else "upload"
        uploads = _stage_large_uploads(files, staged,
                                       pointer=folder == PLAN_FOLDER)
        upload_errors: dict[str, str] = {}
        for path, name, _note, needs in uploads:
            if needs and needs in upload_errors:
                upload_errors[name] = (f"not uploaded: the gzip copy it "
                                       f"points at, {needs}, did not land")
                continue
            try:
                call("upload_ws_file", workspace=ws_key,
                     path=f"{folder}/{name}", local_path=str(path))
            except Exception as exc:
                upload_errors[name] = str(exc)[:200]
        if not uploads:
            continue
        try:
            items = call("list_ws_objects", workspace=ws_key,
                         path=folder).get("items") or []
            listing_failure = None
        except Exception as exc:
            items, listing_failure = [], str(exc)[:200]
        for _path, name, note, _needs in uploads:
            remote = f"{folder}/{name}"
            if name in upload_errors:
                # The upload itself raised: that is what to report, not the
                # absence it necessarily causes in the listing.
                step(kind, "failed", False,
                     f"{remote}{note}: {upload_errors[name]}")
                continue
            if listing_failure is not None:
                step(kind, "upload_requested", None,
                     f"{remote}{note}: uploaded, but the folder could not "
                     f"be listed to confirm it ({listing_failure})")
                continue
            found = any(
                str(i.get("path") or "").endswith("/" + name)
                or i.get("displayName") == name for i in items)
            step(kind, "uploaded" if found else "upload_requested", found,
                 f"{remote}{note}" if found
                 else f"{remote}{note}: not visible in listing")


def _pypi_from_requirements(path: pathlib.Path | None) -> list[str]:
    if path is None or not path.is_file():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            out.append(line)
    return out


def _current_credential(prior: dict) -> str | None:
    """The credential path the earlier record's notebooks were pointed at:
    its own request, else the newest object it tracks that no later
    --source-config superseded. Still to be LOOKED FOR before use."""
    superseded = set(prior.get("credential_superseded") or [])
    for obj in (prior.get("credential_requested"),
                *(prior.get("credential_objects") or []),
                *(prior.get("credential_unconfirmed") or [])):
        if obj and obj not in superseded:
            return obj
    return None


def _inherited(prior: dict, *, external_catalog, target_catalog,
               source_mode, credential_given: bool):
    """(external_catalog, target_catalog, source_mode, credential path)
    after inheriting from `prior` wherever no argument gave a value. The
    source mode travels with the catalogs: external-catalog notebooks
    regenerated as connector, with no source-config, fail every stage."""
    return (external_catalog or prior.get("external_catalog"),
            target_catalog or prior.get("target_catalog"),
            source_mode or prior.get("source_mode"),
            None if credential_given else _current_credential(prior))


def provision(*, call: Callable[..., dict] | None, workspace_name: str,
              cluster_name: str = "migration-assets",
              scripts: list[pathlib.Path],
              plan_files: list[pathlib.Path] = (),
              stage_params: dict | None = None,
              requirements: pathlib.Path | None = None,
              maven: list[str] = (),
              external_catalog: str | None = None,
              target_catalog: str | None = None,
              source_mode: str | None = "connector",
              source_config: pathlib.Path | None = None,
              warehouse_clusters: list[dict] = (),
              execute: bool = False,
              delays: tuple[float, ...] = (3.0, 5.0, 10.0, 15.0),
              subnet_id: str | None = None,
              reuse_existing: bool = False,
              warehouse_cluster_mode: str = "new",
              existing_cluster_id: str | None = None,
              output_dir: str = "report/output",
              refresh_notebooks: bool = False,
              plan_label: str | None = None,
              copy_schemas=(),
              delete_stale_copy_jobs: bool = False,
              prior: dict | None = None,
              datalake_ocid: str | None = None,
              now: datetime.datetime | None = None) -> dict:
    """Provision this migration's own environment inside AIDP.

    `reuse_existing=False` is the default and the rule: a migration creates
    its own workspace, cluster and jobs so that everything it touches can be
    identified, audited and torn down as a unit. A name already in use is a
    COLLISION -- reported, and the run stops so the user can choose another
    name. Adopting a stranger's workspace silently makes the blast radius of
    the migration unknowable.

    With `reuse_existing`, a stage notebook already on the workspace is KEPT
    as it is unless `refresh_notebooks` is set: operators set schema, mode
    and verify by editing its PARAMS cell in the console, so regenerating it
    would discard that work without saying so. Kept notebooks are listed in
    the result.

    `stage_params` are written into the PARAMS cell of every stage that
    declares the name, or, as `<stage key>.<name>`, of that stage only.
    They are checked BEFORE anything is called: a name no stage declares, a
    switch given something other than true/false, a value outside a flag's
    choices in any stage it reaches (`mode=overwrite` reaches 01 too, which
    rejects it), or values that would land only on a notebook this run
    keeps are refused with a ValueError, never dropped -- a scope flag that
    silently does nothing reads as applied.

    `prior` is the earlier EXECUTED record of this out dir. A
    `reuse_existing` re-push into the workspace it records inherits the
    catalogs, the source mode (`source_mode=None`) and the credential path
    it baked in, where no argument gives them -- only when it names the same aiDataPlatform (`datalake_ocid`)
    and, once listed, the same workspace KEY; a name alone is not the same
    workspace. An inherited credential path is listed on the workspace
    before it is baked into a notebook or announced. A credential object is
    recorded in `credential_objects` only once its upload is read back;
    one that may or may not have landed is `credential_unconfirmed`.
    """
    stamp = (now or datetime.datetime.now(datetime.timezone.utc)
             ).strftime("%Y%m%dT%H%M%SZ")
    # The four stage jobs, then one copy job per schema of the approved plan.
    # With per-schema jobs the generic copy JOB is not created: it has no
    # schema anywhere (the notebook requires one), so running it could only
    # fail. Its NOTEBOOK is still uploaded -- every per-schema job runs it.
    job_specs = list(JOB_SPECS) + copy_job_specs(copy_schemas)
    no_job = ({spec["name"] for spec in JOB_SPECS
               if spec["notebook"] == COPY_STAGE_NOTEBOOK}
              if copy_schemas else set())
    stage_params = dict(stage_params or {})
    if stage_params:
        check_stage_params(stage_params, copy_schemas=copy_schemas)
        if reuse_existing and not refresh_notebooks:
            raise ValueError(
                "--stage-param " + ", ".join(sorted(stage_params))
                + " with --reuse-existing needs --refresh-notebooks: a stage "
                "notebook already on the workspace is kept as it is, so the "
                "value would never reach its PARAMS cell. Add "
                "--refresh-notebooks (it regenerates every stage notebook "
                "from this run's flags, discarding console edits to PARAMS), "
                "or edit the PARAMS cell in the console instead.")
    ws_name = translate_name(workspace_name, kind="workspace")
    cl_name = translate_name(cluster_name, kind="cluster")
    pypi = _pypi_from_requirements(requirements)
    # The source config, when given, is the ONE credential-bearing object
    # this stage places on the workspace. Only its `snowflake:` block goes,
    # as JSON under the same stem; the operator's file itself never travels,
    # whoever put it in plan_files. A laptop-only `*_path` secret is refused
    # here, before anything -- dry run or not -- is written.
    source_payload = None
    credential_object = None
    if source_config is not None:
        source_payload = source_config_payload(source_config)
        credential_object = f"{PLAN_FOLDER}/{source_config.stem}.json"
        plan_files = [p for p in plan_files
                      if pathlib.Path(p).resolve() != source_config.resolve()]
    # What a re-push may inherit, and from which record. In a dry run the
    # workspace key is unknown, so the preview inherits provisionally; an
    # executed run confirms the key before using any of it.
    inherit_from = None
    if (reuse_existing and prior and prior.get("dry_run") is False
            and (prior.get("workspace") or {}).get("requested")
            == workspace_name
            and not (datalake_ocid and prior.get("datalake_ocid")
                     and prior["datalake_ocid"] != datalake_ocid)):
        inherit_from = prior
    inherited_credential = None
    if inherit_from is not None and not execute:
        (external_catalog, target_catalog, source_mode,
         inherited_credential) = _inherited(
            inherit_from, external_catalog=external_catalog,
            target_catalog=target_catalog, source_mode=source_mode,
            credential_given=credential_object is not None)
    # `connector` only when nothing -- flag or inheritance -- gave a mode.
    requested_mode, source_mode = source_mode, source_mode or "connector"
    # One cluster per Snowflake warehouse, named after it. Sizing is NOT
    # carried over: the user asked for same-name clusters on the AIDP default
    # config, and the `compute` stage's proposal stays a proposal until
    # somebody decides on it.
    #
    # Provenance is recorded POSITIVELY: `created` is True only on the create
    # path. teardown and the billing report act on nothing else, so a
    # cluster adopted here, or the existing one a warehouse maps to, is
    # never stopped or deleted as if it were the migration's.
    existing_mode = warehouse_cluster_mode == "existing"
    warehouse_targets = []
    # One DISTINCT name per warehouse, the migration cluster's reserved: two
    # names that fold alike (COMPUTE_WH, COMPUTE) would share one cluster.
    from sizing.warehouse_map import cluster_base_name, cluster_names
    distinct = cluster_names(
        [str(wh.get("name") or wh.get("warehouse") or "").strip()
         for wh in warehouse_clusters or ()], reserved={cl_name.name})
    for wh in warehouse_clusters or ():
        source_name = str(wh.get("name") or wh.get("warehouse") or "").strip()
        if not source_name:
            continue
        if existing_mode:
            # `compute.warehouse_clusters: existing` -- every warehouse maps
            # to one cluster that is already there. Its display name is not
            # known here, and the warehouse's base name is not it.
            warehouse_targets.append(
                {"warehouse": source_name, "name": None,
                 "existing_cluster": existing_cluster_id,
                 "key": existing_cluster_id, "uses_existing": True,
                 "created": False, "renamed": False,
                 "notes": [f"mapped to the existing cluster "
                           f"{existing_cluster_id}; not created, not "
                           f"resized"],
                 "source_size": wh.get("size")})
            continue
        # Named from the warehouse's BASE name (COMPUTE_WH -> compute),
        # unless that collides with another warehouse's or the migration
        # cluster's.
        name = distinct[source_name]
        base = cluster_base_name(source_name)
        note = (f"named from the base name of {source_name}"
                if name == base else
                f"named from {source_name} in full: its base name `{base}` "
                f"would collide with another warehouse's or the migration "
                f"cluster's")
        warehouse_targets.append(
            {"warehouse": source_name, "name": name,
             "renamed": name != source_name, "created": False,
             "notes": [note], "source_size": wh.get("size")})

    out: dict = {
        "dry_run": not execute,
        "workspace": {"requested": workspace_name, "name": ws_name.name,
                      "renamed": ws_name.changed, "notes": ws_name.notes,
                      "created": False},
        "cluster": {"requested": cluster_name, "name": cl_name.name,
                    "renamed": cl_name.changed, "notes": cl_name.notes,
                    "created": False},
        # The push these records were written by; a `created` flag carries
        # the run that set it (`created_run`).
        "run": stamp,
        "warehouse_clusters": warehouse_targets,
        "scripts_folder": SCRIPTS_FOLDER, "plan_folder": PLAN_FOLDER,
        "reports_folder": REPORTS_FOLDER,
        "libraries": {"pypi": pypi, "maven": list(maven)},
        "external_catalog": external_catalog,
        "target_catalog": target_catalog,
        "source_mode": source_mode,
        # Workspace objects that hold a credential, so the report can say so
        # in one place and the operator knows what to remove afterwards.
        # A re-push that inherited the path records it too: the object is
        # still on the workspace, and the next push inherits from here.
        # In a dry run, what would hold it; executed, only what was read
        # back on the workspace (an upload that raised or was not seen is
        # `credential_unconfirmed`: it may have landed).
        "credential_objects": ([] if execute else
                               [credential_object] if credential_object
                               else [inherited_credential]
                               if inherited_credential else []),
        "credential_requested": credential_object,
        "credential_unconfirmed": [],
        "datalake_ocid": datalake_ocid,
        # Stage notebooks left as found on the workspace (reuse_existing
        # without refresh_notebooks), so PROVISION.md can list them.
        "notebooks_kept": [],
        # The explicit --stage-param values, as given, so the record says
        # what this run wrote into PARAMS beyond the derived coordinates.
        "stage_params": dict(stage_params),
        # One copy job per schema of the approved plan (runbook S11), as
        # {schema, job, notebook, status}; registered here, never run by
        # provision. `status` is this run's OUTCOME for the job -- "would
        # register" in a dry run, "not registered" until a create or reuse
        # says otherwise -- so a halt is never reported as a registration.
        "copy_jobs": [{"schema": sp["task_parameters"]["schema"],
                       "job": sp["name"],
                       "notebook": f'{SCRIPTS_FOLDER}/{sp["notebook"]}',
                       "status": ("not registered" if execute
                                  else "would register")}
                      for sp in job_specs if sp.get("task_parameters")],
        # Copy jobs on the workspace that the present plan does not name
        # (a schema reduced out of it): still runnable, so reported.
        "stale_copy_jobs": [],
        # Stale copy jobs this push deleted, on --delete-stale-copy-jobs.
        "deleted_copy_jobs": [],
        # Every job on this workspace that a push of THIS migration created,
        # as {name, key, created_run}, carried from the earlier record of
        # the same workspace. A re-push records the same job as `reused`,
        # so this list, not the steps, is the proof of ownership that
        # --delete-stale-copy-jobs acts on.
        "created_jobs": [],
        "steps": [],
    }

    if inherit_from is not None and not execute:
        out["inherited_from"] = {"run": inherit_from.get("run"),
                                 "provisional": True}

    def step(name: str, action: str, verified: bool | None,
             detail: str = "") -> None:
        out["steps"].append({"step": name, "action": action,
                             "verified": verified, "detail": detail})

    if not execute:
        # "create", not "ensure": a taken name halts the --execute run (it
        # is a collision to resolve) unless --reuse-existing adopts it.
        step("workspace", "would create" if not reuse_existing
             else "would create or reuse", None, ws_name.name)
        step("cluster", "would create" if not reuse_existing
             else "would create or reuse", None, cl_name.name)
        for target in warehouse_targets:
            if target.get("uses_existing"):
                step("warehouse-cluster", "would use existing", None,
                     f'{target["warehouse"]} -> existing cluster '
                     f'{existing_cluster_id} (not created, not resized)')
                continue
            step("warehouse-cluster", "would ensure", None,
                 f'{target["warehouse"]} -> {target["name"]} '
                 f'(AIDP default config; source size '
                 f'{target["source_size"] or "unknown"} NOT carried over)')
        if pypi or maven:
            step("libraries", "would install + restart", None,
                 ", ".join(pypi + list(maven)))
        # PREVIEW WHAT EXECUTE ACTUALLY UPLOADS: one generated `.ipynb` per
        # stage, never the `.py` under engine/dataplane/. Those are the
        # canonical SOURCES and they stay on the operator's machine -- AIDP
        # types a workspace object by extension, so a `.py` would land as a
        # FILE and no job could run it. Previewing the source names advertised
        # an upload that never happens, which is the one thing a dry run may
        # not do.
        for spec in JOB_SPECS:
            step("upload", "would upload", None,
                 f'{spec["notebook"]} (generated) -> '
                 f'{SCRIPTS_FOLDER}/{spec["notebook"]}')
        step("upload", "would upload", None,
             f"{DIAGNOSE_NOTEBOOK_NAME} (generated, no job) -> "
             f"{SCRIPTS_FOLDER}/{DIAGNOSE_NOTEBOOK_NAME}")
        for path in plan_files:
            big = (pathlib.Path(path).is_file()
                   and pathlib.Path(path).stat().st_size > COMPRESS_OVER_BYTES)
            step("upload", "would upload", None,
                 f"{path.name} -> {PLAN_FOLDER}/{path.name}"
                 + (".gz (gzipped, over the upload size) + a pointer under "
                    "the plain name" if big else ""))
        for path, name in plan_backup_names(plan_files, stamp=stamp,
                                            label=plan_label):
            step("backup", "would upload", None,
                 f"{path.name} -> {BACKUP_FOLDER}/{name}")
        if credential_object:
            step("upload", "would upload", None,
                 _credential_line(source_config.name, credential_object))
        for spec in job_specs:
            if spec["name"] in no_job:
                continue
            step("job", "would create", None, spec["name"])
        return out

    if call is None:
        raise ValueError("execute=True requires a transport callable")
    credential_ready = False

    # 1 · workspace: look, create if absent, poll until visible AND ACTIVE --
    found = _match(call("list_workspaces").get("items") or [], ws_name.name)
    ws_created, ws_list_error = False, None
    if found is None:
        call("create_workspace",
             body=build_workspace_body(ws_name.name,
                                       description="snowflake-migrator "
                                                   "migration workspace",
                                       subnet_id=subnet_id))
        ws_created = True
        # A workspace reports ACTIVE seconds after its POST returns, and a
        # cluster created inside that window is a 409. So wait for ACTIVE,
        # not just for the name to appear; a slow ACTIVE is recorded, not a
        # stop, because the cluster POST below retries on the 409 anyway.
        found, ws_list_error = _poll(lambda: call("list_workspaces"),
                                     ws_name.name, delays,
                                     require_active=True)
        settling = found is not None and not is_active(found)
        if ws_list_error is not None:
            detail = f"{ws_name.name}: read_back_failed: {ws_list_error}"
        elif settling:
            detail = (f'{ws_name.name}: visible, but lifecycleState='
                      f'{found.get("lifecycleState")} after the poll budget; '
                      f'the cluster POST is retried on 409 while it settles')
        else:
            detail = ws_name.name
        step("workspace", "create_requested" if found is None else "created",
             found is not None, detail)
    elif reuse_existing:
        step("workspace", "reused", True, _key(found, ws_name.name))
    else:
        step("workspace", "name_taken", False, ws_name.name)
        step("halt", "stopped", False,
             f"a workspace named {ws_name.name!r} already exists and this "
             f"migration does not reuse what it did not create. Choose "
             f"another --workspace-name, or pass --reuse-existing if you "
             f"really mean to migrate into someone else's workspace. If a "
             f"previous run of THIS migration created it (its PROVISION.md "
             f"lists the workspace step), --reuse-existing is the intended "
             f"resume, not a rule violation.")
        return out
    ws_key = _key(found or {}, ws_name.name)
    out["workspace"]["key"] = ws_key
    out["created_jobs"] = _prior_created_jobs(prior, ws_key, datalake_ocid)
    if ws_created:
        out["workspace"].update(created=True, created_run=stamp)
    if inherit_from is not None and found is not None:
        was = (inherit_from.get("workspace") or {}).get("key")
        if was == ws_key:
            (external_catalog, target_catalog, source_mode,
             inherited_credential) = _inherited(
                inherit_from, external_catalog=external_catalog,
                target_catalog=target_catalog, source_mode=requested_mode,
                credential_given=credential_object is not None)
            source_mode = source_mode or "connector"
            out.update(external_catalog=external_catalog,
                       target_catalog=target_catalog,
                       source_mode=source_mode,
                       inherited_from={"run": inherit_from.get("run"),
                                       "workspace": ws_key})
        else:
            step("inherit", "skipped", None,
                 f"the earlier record names workspace key {was}, this "
                 f"workspace is {ws_key}: a different workspace under the "
                 f"same name, so no catalog or credential path was "
                 f"inherited")
    if found is None:
        if ws_created and ws_list_error is not None:
            step("halt", "stopped", False,
                 f"the workspace {ws_name.name!r} was created (POST "
                 f"accepted) but could not be listed to confirm it "
                 f"({ws_list_error}); nothing else was attempted. Once "
                 f"listing works, re-run the same command with "
                 f"--reuse-existing to continue into it -- a plain re-run "
                 f"would halt on name_taken")
        else:
            step("halt", "stopped", False,
                 "the workspace never became visible; nothing else was "
                 "attempted")
        return out

    # 2 · cluster ------------------------------------------------------------
    # From here on the workspace EXISTS, so nothing below may raise out of
    # this function: an exception would lose the record of it, and the next
    # run would halt on name_taken and call the operator's own workspace
    # "someone else's". A failure is a recorded step plus a halt that says
    # how to resume.
    resume = (
        f"workspace {ws_name.name!r} (key {ws_key}) WAS created by this run "
        f"and is recorded above. Re-run the same command with "
        f"--reuse-existing to continue into it; do not pick a new "
        f"--workspace-name, or this one is orphaned."
        if ws_created else
        f"workspace {ws_name.name!r} (key {ws_key}) is the one being reused; "
        f"fix the cause above and re-run the same command.")
    cl_list_error = None
    try:
        found = _match(
            call("list_clusters", workspace=ws_key).get("items") or [],
            cl_name.name)
    except Exception as exc:
        step("cluster", "failed", False, f"list_clusters: {str(exc)[:200]}")
        step("halt", "stopped", False, resume)
        return out
    if found is None:
        # A cluster POSTed before the workspace reports ACTIVE is a 409
        # "ongoing operation". Retried with the bounded backoff, each retry
        # on the record; anything else fails the step and halts with the
        # record intact.
        for attempt in range(len(delays) + 1):
            try:
                call("create_cluster", workspace=ws_key,
                     body=build_cluster_body(cl_name.name))
                break
            except Exception as exc:
                if is_conflict(exc) and attempt < len(delays):
                    step("cluster", "retried", None,
                         f"attempt {attempt + 1}: 409/ongoing operation on "
                         f"workspace {ws_key}; waiting {delays[attempt]:g}s")
                    time.sleep(delays[attempt])
                    continue
                step("cluster", "failed", False,
                     f"create_cluster: {str(exc)[:200]}")
                step("halt", "stopped", False, resume)
                return out
        found, cl_list_error = _poll(
            lambda: call("list_clusters", workspace=ws_key), cl_name.name,
            delays)
        # Visible is not ready: live (2026-09-29) the cluster read CREATING
        # a minute after this step said "created, verified". The state is
        # recorded and said, so nothing downstream reads visible as ACTIVE.
        state = _cluster_state(found)
        detail = (f"{cl_name.name}: read_back_failed: {cl_list_error}"
                  if cl_list_error is not None else cl_name.name)
        if state and state != "ACTIVE":
            detail += (f" (state {state} -- not ACTIVE yet; a job submitted "
                       f"now waits for it)")
        step("cluster", "create_requested" if found is None else "created",
             found is not None, detail)
        if found is not None:
            # When the compute clock started, for the billing report.
            out["cluster"].update(created=True, created_run=stamp)
            if state:
                out["cluster"]["state_at_create"] = state
            out["cluster"]["created_at"] = datetime.datetime.now(
                datetime.timezone.utc).isoformat()
        else:
            # Accepted, key never seen: teardown must say so, not skip it.
            out["cluster"]["create_requested"] = True
    elif reuse_existing:
        step("cluster", "reused", True, _key(found, cl_name.name))
    else:
        step("cluster", "name_taken", False, cl_name.name)
        step("halt", "stopped", False,
             f"a cluster named {cl_name.name!r} already exists in this "
             f"workspace and this migration does not reuse what it did not "
             f"create. Choose another --cluster-name, or pass "
             f"--reuse-existing.")
        return out
    if found is None:
        # Falling through would bake the DISPLAY NAME into four job bodies as
        # a clusterKey, and they would be "created" and unrunnable.
        seen = ("was created (POST accepted) but could not be listed to "
                f"confirm it ({cl_list_error})" if cl_list_error is not None
                else "never became visible")
        step("halt", "stopped", False,
             f"the cluster {seen}, so its key is unknown; jobs would be "
             f"created bound to an invalid cluster. Nothing else was "
             f"attempted. " + resume)
        return out
    cluster_key = _key(found, cl_name.name)
    out["cluster"]["key"] = cluster_key

    # 2b · one cluster per Snowflake warehouse, same name, default config ----
    # These are the customer's own compute, not the migration's: a failure on
    # one is recorded and the rest continue, and NOTHING here changes the
    # migration cluster the jobs are bound to.
    if existing_mode:
        # `compute.warehouse_clusters: existing` -- every warehouse maps to
        # one cluster that is already there. Nothing is created or resized,
        # and the record says it was not created here (uses_existing,
        # created: false), so teardown leaves it alone.
        for target in warehouse_targets:
            step("warehouse-cluster", "uses_existing", True,
                 f'{target["warehouse"]} -> existing cluster '
                 f'{existing_cluster_id} (not created, not resized)')
        warehouse_targets = []
    for target in warehouse_targets:
        try:
            existing = _match(
                call("list_clusters", workspace=ws_key).get("items") or [],
                target["name"])
        except Exception as exc:
            # Could not look is not absent: creating on a failed read is how
            # a duplicate gets made. Recorded, and the next warehouse tried.
            step("warehouse-cluster", "failed", False,
                 f'{target["warehouse"]} -> {target["name"]}: '
                 f'list_clusters: {str(exc)[:200]}')
            continue
        if existing is not None:
            target["key"] = _key(existing, target["name"])
            step("warehouse-cluster",
                 "reused" if reuse_existing else "name_taken",
                 bool(reuse_existing),
                 f'{target["warehouse"]} -> {target["name"]}'
                 + ("" if reuse_existing else
                    ": a cluster of that name already exists and was left "
                    "untouched; it is not this migration's"))
            continue
        try:
            call("create_cluster", workspace=ws_key,
                 body=build_cluster_body(target["name"]))
        except Exception as exc:
            step("warehouse-cluster", "failed", False,
                 f'{target["warehouse"]} -> {target["name"]}: '
                 f'{str(exc)[:200]}')
            continue
        seen, seen_error = _poll(
            lambda: call("list_clusters", workspace=ws_key), target["name"],
            delays)
        target["key"] = _key(seen or {}, target["name"]) if seen else None
        if seen:
            target.update(created=True, created_run=stamp,
                          created_at=datetime.datetime.now(
                              datetime.timezone.utc).isoformat())
            tail = ""
        else:
            # Accepted, key never seen: teardown must say so, not skip it.
            target["create_requested"] = True
            tail = (" — accepted, but it could not be listed to confirm it; "
                    f"read_back_failed: {seen_error}"
                    if seen_error is not None else
                    " — accepted, but it never became visible")
        step("warehouse-cluster",
             "created" if seen else "create_requested", seen is not None,
             f'{target["warehouse"]} -> {target["name"]}' + tail)

    # 3 · libraries (only when a fallback needs them) ------------------------
    if pypi or maven:
        # The library item shape is the one field family still inferred, so
        # a failure here is expected-possible and must
        # not cost the record of the workspace and cluster above.
        try:
            call("install_libraries", workspace=ws_key, cluster=cluster_key,
                 body=build_library_items(pypi=pypi, maven=list(maven)))
            call("restart_cluster", workspace=ws_key, cluster=cluster_key)
        except Exception as exc:
            step("libraries", "failed", False,
                 f"{len(pypi) + len(maven)} requested; the install or restart "
                 f"was rejected ({str(exc)[:160]}). The cluster is unchanged "
                 f"as far as this run knows")
        else:
            try:
                listed = call("list_libraries", workspace=ws_key,
                              cluster=cluster_key).get("items") or []
                step("libraries", "install_requested + restart", None,
                     f"{len(pypi) + len(maven)} requested; the server lists "
                     f"{len(listed)} — confirm after the restart settles")
            except Exception as exc:
                step("libraries", "install_requested + restart", None,
                     f"library list unreadable ({str(exc)[:120]}); "
                     f"NOT confirmed, not assumed")

    # 4 · the folder tree, then scripts and plan artifacts -------------------
    # Created through the workspace-object surface (live-verified); the
    # Jupyter contents API returned 200-then-unreadable on a real build.
    for folder in (_ROOT, SCRIPTS_FOLDER, PLAN_FOLDER, f"{_ROOT}/reports",
                   BACKUP_FOLDER):
        try:
            call("create_ws_folder", workspace=ws_key, path=folder)
        except Exception as exc:
            text = str(exc)
            if "409" in text and "already exist" in text.lower():
                # Every re-push meets its own folders: 409 "Directory already
                # exists" is the folder being there, not a failure. It was
                # recorded unconfirmed, with the raw multi-line response in
                # the table, and the board then flagged a clean push as
                # "5 not confirmed".
                step("folder", "exists", True, folder)
                continue
            # Any other refusal: existence is decided by the per-file listing
            # below, so record and continue.
            step("folder", "create_failed_or_exists", None,
                 f"{folder}: {' '.join(text.split())[:160]}")

    # The plan goes to plan/ (what S10 reads) AND, dated, to backup/ (runbook
    # S9: the full plan is backed up before scope is reduced, and every plan
    # that drives a run stays recoverable). The dated names never collide,
    # so a reduced plan pushed later cannot overwrite the full one's copy.
    backups = [(path, name) for path, name in
               plan_backup_names(plan_files, stamp=stamp, label=plan_label)]
    staged = tempfile.mkdtemp(prefix="snowmig_plan_push_")
    try:
        _push_plan_folders(call, ws_key, step, staged, [
            (PLAN_FOLDER, [(p, p.name) for p in plan_files]),
            (BACKUP_FOLDER, backups)])
    finally:
        shutil.rmtree(staged, ignore_errors=True)

    if credential_object:
        # The derived `snowflake:` block, written to a temp file for the
        # CLI to read and removed right after -- the copy on the mount is
        # the only one meant to outlive this call.
        name = credential_object.rsplit("/", 1)[-1]
        fd, local = tempfile.mkstemp(prefix="snowmig_source_", suffix=".json")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(source_payload, fh)
            try:
                call("upload_ws_file", workspace=ws_key,
                     path=credential_object, local_path=local)
                items = call("list_ws_objects", workspace=ws_key,
                             path=PLAN_FOLDER).get("items") or []
                found = any(
                    str(i.get("path") or "").endswith("/" + name)
                    or i.get("displayName") == name for i in items)
                step("upload", "uploaded" if found else "upload_requested",
                     found,
                     _credential_line(source_config.name, credential_object)
                     + ("" if found else "; not visible in listing"))
            except Exception as exc:
                found = False
                step("upload", "failed", False,
                     f"{credential_object}: {str(exc)[:200]} (it carries "
                     f"the credential; check whether it landed)")
            credential_ready = found
            out["credential_objects" if found
                else "credential_unconfirmed"].append(credential_object)
        finally:
            try:
                os.unlink(local)
            except OSError:
                pass
    elif inherited_credential:
        # Named by the earlier record is not "on the workspace": its upload
        # may have failed, or the object been removed since. Looked for
        # before it is baked into a notebook or announced as holding it.
        name = inherited_credential.rsplit("/", 1)[-1]
        try:
            items = call("list_ws_objects", workspace=ws_key,
                         path=PLAN_FOLDER).get("items") or []
            present = any(str(i.get("path") or "").endswith("/" + name)
                          or i.get("displayName") == name for i in items)
            why = f"is not in the listing of {PLAN_FOLDER}"
        except Exception as exc:
            present = False
            why = f"could not be looked for ({str(exc)[:160]})"
        if present:
            out["credential_objects"].append(inherited_credential)
            step("credential", "inherited", True,
                 f"{inherited_credential}: placed by an earlier push of this "
                 f"migration and still on the workspace; not re-uploaded. It "
                 f"CARRIES THE SNOWFLAKE CREDENTIAL; remove it when the "
                 f"migration is done")
        else:
            out.setdefault("credential_missing", []).append(
                inherited_credential)
            step("credential", "missing", False,
                 f"{inherited_credential}: named by the earlier record, but "
                 f"it {why}, so no notebook was pointed at it. Re-run with "
                 f"--source-config <the migration config> to place it")
            inherited_credential = None

    # 5 · stage notebooks + jobs ---------------------------------------------
    # This run's coordinates are written into each stage notebook's own
    # PARAMS cell as defaults -- visible and editable in the console,
    # regenerated here when they change. A job task's `parameters` override
    # them at run time (oidlUtils.parameters.getParameter, live-verified);
    # the per-schema copy jobs pass `schema` that way.
    defaults = {"reports-dir": REPORTS_FOLDER, "source-mode": source_mode,
                "backup-dir": f"/Workspace/{BACKUP_FOLDER}",
                "output-dir": f"/Workspace/{output_dir}" if output_dir else ""}
    # Written last so an explicit --stage-param wins over a derived
    # coordinate: the operator naming a value outranks this function
    # guessing one. Every explicit name is declared by at least one stage
    # (checked on entry); a stage that does not declare it skips it
    # (build_stage_notebook filters per stage).
    explicit = dict(stage_params)
    if external_catalog:
        defaults["source-catalog"] = external_catalog
    if target_catalog:
        defaults["target-catalog"] = target_catalog
    if credential_object and credential_ready:
        # The scripts read the credential from the derived copy ON THE MOUNT,
        # so the path they receive is the /Workspace one, not the local one.
        # Only once it was read back there: a path to nothing is not baked.
        defaults["source-config"] = f"/Workspace/{credential_object}"
    elif inherited_credential:
        # A re-push that did not re-upload it: the copy an earlier push of
        # this same migration placed is still there, and still the path.
        defaults["source-config"] = f"/Workspace/{inherited_credential}"
    defaults.update(explicit)
    try:
        existing = call("list_jobs", workspace=ws_key).get("items") or []
    except Exception as exc:
        # The last bare call past the cluster. An expired session token here
        # escaped provision() and lost the record of the workspace and
        # cluster created seconds earlier.
        step("job", "failed", False, f"list_jobs: {str(exc)[:200]}")
        step("halt", "stopped", False, resume)
        return out
    stages_by_notebook = {st.notebook_name: st for st in STAGES}

    # Which stage notebooks are already on the workspace. Looked up once,
    # and only when they are to be kept: a fresh run has nothing to keep,
    # and --refresh-notebooks asks for the overwrite.
    keep_existing = reuse_existing and not refresh_notebooks
    present: set[str] = set()
    listing_error: Exception | None = None
    if keep_existing:
        try:
            listed = call("list_ws_objects", workspace=ws_key,
                          path=SCRIPTS_FOLDER).get("items") or []
            for item in listed:
                present.add(str(item.get("path") or "").rsplit("/", 1)[-1])
                present.add(str(item.get("displayName") or ""))
        except Exception as exc:
            listing_error = exc

    copy_status = {j["job"]: j for j in out["copy_jobs"]}

    def _outcome(spec: dict, status: str) -> None:
        if spec["name"] in copy_status:
            copy_status[spec["name"]]["status"] = status

    def _create_job(spec: dict, *, kept: bool = False) -> None:
        notebook_path = f'{SCRIPTS_FOLDER}/{spec["notebook"]}'
        found_job = _match(existing, spec["name"])
        if found_job is not None:
            overwritten = (
                "OVERWRITTEN from this run's flags; console edits to its "
                "PARAMS cell are gone")
            wanted = spec.get("task_parameters")
            listed = _listed_task_parameters(found_job) if wanted else None
            if wanted and listed is not None and any(
                    listed.get(k) != str(v) for k, v in wanted.items()):
                # A job of this name that runs something else is not this
                # plan's workflow, whatever its name says.
                step("job", "name_taken", False,
                     f'{spec["name"]} already exists with task parameters '
                     f'{listed}, not {wanted}; it was NOT adopted. Delete or '
                     f'rename it in the console, then re-push.')
                _outcome(spec, "name taken, not registered")
                return
            if reuse_existing:
                step("job", "reused", True,
                     f'{spec["name"]} (stage notebook '
                     f'{"kept" if kept else overwritten})')
                _outcome(spec, "reused")
            else:
                step("job", "name_taken", False,
                     f'{spec["name"]} already exists and was NOT adopted; '
                     f'its stage notebook was {overwritten}, but the job '
                     f'itself is not this migration\'s. Rename or '
                     f'--reuse-existing.')
                _outcome(spec, "name taken, not registered")
            return
        body = build_job_body(spec["name"], notebook_path=notebook_path,
                              cluster_key=cluster_key,
                              task_parameters=spec.get("task_parameters"))
        try:
            call("create_job", workspace=ws_key, body=body)
            found, job_list_error = _poll(
                lambda: call("list_jobs", workspace=ws_key), spec["name"],
                delays)
            step("job", "created" if found else "create_requested",
                 found is not None,
                 f'{spec["name"]}: read_back_failed: {job_list_error}'
                 if job_list_error is not None else spec["name"])
            if found is not None:
                _own(spec["name"], found)
            _outcome(spec, "created" if found else
                     "create requested, not confirmed")
        except Exception as exc:
            step("job", "failed", False, f'{spec["name"]}: {str(exc)[:200]}')
            _outcome(spec, "failed, not registered")

    def _own(name: str, job: dict) -> None:
        key = job.get("key") or job.get("id")
        out["created_jobs"] = [e for e in out["created_jobs"]
                               if str(e.get("name")).lower() != name.lower()]
        out["created_jobs"].append({"name": name,
                                    "key": str(key) if key else None,
                                    "created_run": stamp})

    def _created_here(job: dict, name: str) -> bool:
        """This migration's records show it created this job. A recorded
        key must match the listed one: a job of the same name made later by
        someone else is not ours."""
        entry = next((e for e in out["created_jobs"]
                      if str(e.get("name")).lower() == name.lower()), None)
        if entry is None:
            return False
        listed = job.get("key") or job.get("id")
        return not (entry.get("key") and listed
                    and str(listed) != str(entry["key"]))

    def _delete_stale(job: dict, name: str, why: str) -> None:
        """Delete one copy job the plan no longer names, and READ IT BACK:
        only a job gone from the listing is recorded deleted. Asked for
        explicitly (--delete-stale-copy-jobs); never done by default, and
        only for a job this migration's records show it created -- on a
        reused workspace a job of that name may be someone else's."""
        if not _created_here(job, name):
            step("job", "stale", False,
                 f"{name}: {why}; NOT deleted -- no record of this "
                 f"migration shows it created this job, so it may belong "
                 f"to someone else on this workspace. Delete it in the "
                 f"console if it is this migration's")
            return
        key = job.get("key") or job.get("id")
        if not key:
            step("job", "stale", False,
                 f"{name}: {why}; the listing carries no job key, so it was "
                 f"NOT deleted -- delete it in the console")
            return
        try:
            call("delete_job", workspace=ws_key, job_key=str(key))
        except Exception as exc:
            step("job", "stale", False,
                 f"{name}: {why}; the delete was refused "
                 f"({str(exc)[:160]}) -- it is still runnable")
            return
        try:
            listed = call("list_jobs", workspace=ws_key).get("items") or []
        except Exception as exc:
            step("job", "stale_delete_requested", None,
                 f"{name}: {why}; delete sent and accepted, but the listing "
                 f"could not be read back ({str(exc)[:120]}) -- check the "
                 f"console")
            return
        if _match(listed, name) is None:
            out["deleted_copy_jobs"].append(name)
            out["created_jobs"] = [
                e for e in out["created_jobs"]
                if str(e.get("name")).lower() != name.lower()]
            step("job", "stale_deleted", True,
                 f"{name}: {why}; deleted, and gone from the listing")
        else:
            step("job", "stale_delete_requested", None,
                 f"{name}: {why}; delete sent, but it is still listed -- "
                 f"check the console")

    # Notebooks uploaded (or kept) and read back by this run. A per-schema
    # copy job shares the 02 notebook, so it only needs that one to be here.
    notebooks_ready: set[str] = set()
    kept_by_notebook: dict[str, bool] = {}
    for spec in job_specs:
        if spec.get("task_parameters"):
            if spec["notebook"] not in notebooks_ready:
                step("job", "failed", False,
                     f'{spec["name"]}: its notebook {spec["notebook"]} is not '
                     f'on the workspace (see its step above), so the job was '
                     f'NOT created rather than pointed at nothing')
                continue
            _create_job(spec, kept=kept_by_notebook.get(spec["notebook"],
                                                        False))
            continue
        stage = stages_by_notebook.get(spec["notebook"])
        if stage is None:
            step("notebook", "failed", False,
                 f'{spec["notebook"]}: no stage definition. The job was NOT '
                 f'created rather than pointed at a notebook that does not '
                 f'exist.')
            continue
        notebook_path = f'{SCRIPTS_FOLDER}/{spec["notebook"]}'
        if keep_existing and listing_error is not None:
            # Could not look. Overwriting on that would be the guess this
            # flag exists to prevent; creating a job for a notebook that may
            # not exist would be the other one.
            step("notebook", "failed", False,
                 f"{notebook_path}: could not list {SCRIPTS_FOLDER} to tell "
                 f"whether it already exists ({str(listing_error)[:160]}); "
                 f"neither overwritten nor created. Re-run, or pass "
                 f"--refresh-notebooks to regenerate it regardless")
            continue
        kept = keep_existing and spec["notebook"] in present
        if kept:
            step("notebook", "kept", True,
                 f"{notebook_path}: already on the workspace and left as it "
                 f"is -- its PARAMS cell (schema, mode, verify) keeps whatever "
                 f"was set in the console. Pass --refresh-notebooks to "
                 f"regenerate it from this run's flags")
            out["notebooks_kept"].append(spec["notebook"])
        else:
            try:
                # Built here, with this run's coordinates already in PARAMS,
                # so the notebook on the workspace is ready to run unedited.
                nb = build_stage_notebook(stage, overrides=defaults)
                fd, local = tempfile.mkstemp(prefix="snowmig_stage_",
                                             suffix=".ipynb")
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    json.dump(nb, fh, indent=1)
                try:
                    call("upload_ws_file", workspace=ws_key,
                         path=notebook_path, local_path=local,
                         object_type="NOTEBOOK")
                finally:
                    os.unlink(local)
                # Read it back, like every other upload: a 2xx is not the
                # claim.
                listed = call("list_ws_objects", workspace=ws_key,
                              path=SCRIPTS_FOLDER).get("items") or []
                name = notebook_path.rsplit("/", 1)[-1]
                seen = any(str(i.get("path") or "").endswith("/" + name)
                           or i.get("displayName") == name for i in listed)
                step("notebook", "uploaded" if seen else "upload_requested",
                     seen, notebook_path if seen
                     else f"{notebook_path}: not visible in the listing")
                if not seen:
                    continue
            except Exception as exc:
                step("notebook", "failed", False,
                     f"{notebook_path}: {str(exc)[:200]}")
                continue

        notebooks_ready.add(spec["notebook"])
        kept_by_notebook[spec["notebook"]] = kept
        if spec["name"] in no_job:
            superseded = _match(existing, spec["name"])
            if superseded is not None and delete_stale_copy_jobs:
                _delete_stale(superseded, spec["name"],
                              "superseded by the per-schema copy jobs; it "
                              "has no schema, so running it could only fail")
            elif superseded is not None:
                # There, from an earlier push without a plan: it has no
                # schema, so running it can only fail. Said, not hidden.
                step("job", "exists_superseded", None,
                     f'{spec["name"]}: on the workspace from an earlier push, '
                     f'superseded by the per-schema copy jobs below; it has '
                     f'no schema, so running it can only fail -- delete it '
                     f'in the console')
            else:
                step("job", "not_created", None,
                     f'{spec["name"]}: superseded by the per-schema copy '
                     f'jobs below, which run this same notebook with a '
                     f'schema')
            continue
        _create_job(spec, kept=kept)

    # A copy job an earlier plan registered, for a schema this plan no
    # longer names, is still on the workspace and still runnable: it would
    # copy data the approved plan excludes, or nothing at all. Nothing is
    # deleted behind the operator's back; it is a failed step until it is
    # removed in the console.
    # Compared case-insensitively, like _match: a listing that spells a
    # planned job in another case is still that job.
    planned = {spec["name"].lower() for spec in job_specs}
    generic = {spec["name"].lower() for spec in JOB_SPECS}
    for job in existing:
        name = str(job.get("displayName") or job.get("name") or "")
        if (name.lower().startswith(COPY_JOB_PREFIX)
                and name.lower() not in planned
                and name.lower() not in generic):
            out["stale_copy_jobs"].append(name)
            if delete_stale_copy_jobs:
                _delete_stale(job, name, "its schema is not in this plan")
                continue
            step("job", "stale", False,
                 f"{name} is on the workspace but its schema is not in this "
                 f"plan (reduced out, or renamed); it is still runnable. "
                 f"Delete it in the console, "
                 + ("re-run provision with --delete-stale-copy-jobs, "
                    if _created_here(job, name) else
                    "(no record shows this migration created it, so "
                    "--delete-stale-copy-jobs will not), ")
                 + "or re-plan to include the schema")

    # 6 · the environment diagnosis, beside the stages, with NO job --------
    # README step 8 has the operator open it from scripts/ before the jobs;
    # only the four job notebooks were uploaded, so it was never there. Built
    # with this run's config path so it runs unedited, and recorded under its
    # own step name: it is not one of the job notebooks.
    diagnose_path = f"{SCRIPTS_FOLDER}/{DIAGNOSE_NOTEBOOK_NAME}"
    if keep_existing and listing_error is not None:
        # Same rule as the stage notebooks: could not look is not absent.
        step("diagnose", "failed", False,
             f"{diagnose_path}: could not list {SCRIPTS_FOLDER} to tell "
             f"whether it already exists ({str(listing_error)[:160]}); "
             f"neither overwritten nor created. Re-run, or pass "
             f"--refresh-notebooks to regenerate it regardless")
        return out
    if keep_existing and DIAGNOSE_NOTEBOOK_NAME in present:
        step("diagnose", "kept", True,
             f"{diagnose_path}: already on the workspace and left as it is. "
             f"Pass --refresh-notebooks to regenerate it from this run's "
             f"config path")
        out["notebooks_kept"].append(DIAGNOSE_NOTEBOOK_NAME)
        return out
    try:
        nb = build_diagnose_notebook(overrides=defaults)
        fd, local = tempfile.mkstemp(prefix="snowmig_diagnose_",
                                     suffix=".ipynb")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(nb, fh, indent=1)
        try:
            call("upload_ws_file", workspace=ws_key, path=diagnose_path,
                 local_path=local, object_type="NOTEBOOK")
        finally:
            os.unlink(local)
        listed = call("list_ws_objects", workspace=ws_key,
                      path=SCRIPTS_FOLDER).get("items") or []
        seen = any(str(i.get("path") or "").endswith("/" + DIAGNOSE_NOTEBOOK_NAME)
                   or i.get("displayName") == DIAGNOSE_NOTEBOOK_NAME
                   for i in listed)
        step("diagnose", "uploaded" if seen else "upload_requested", seen,
             diagnose_path if seen
             else f"{diagnose_path}: not visible in the listing")
    except Exception as exc:
        step("diagnose", "failed", False,
             f"{diagnose_path}: {str(exc)[:200]}")

    return out


def carry_forward(res: dict, prior: dict | None) -> dict:
    """Carry what an earlier EXECUTED push recorded into this one, so no
    executed push ever drops an allocation from the record teardown reads.

    Provenance: a re-push into this migration's own workspace (the
    documented plan push is `--reuse-existing`) finds the workspace and
    clusters the first push created and records them as `reused`. The
    earlier record is the proof they were created here, so its `created`
    flag (and when) is carried onto the same keys -- only from a record of
    the same workspace key; a record of another workspace proves nothing
    about this one. A cluster whose provenance the earlier record cannot
    tell (it predates provenance) stays unknown here
    (`provenance_unknown: true`): this push finding it proves no more.

    Allocations: every cluster the earlier record proves this migration
    created, and that this push does not record itself --
    the plan push carries no --warehouse-clusters, a push may go to
    another workspace -- is kept under `earlier_allocations`, each with its
    own workspace key. Credential objects are kept the same way (see
    _carry_credentials). A record of another aiDataPlatform is never "the
    same workspace", whatever its key. Returns `res`, updated in place.
    """
    from .provenance import CREATED, REQUESTED, UNKNOWN, cluster_records
    if (not prior or prior.get("dry_run") is not False
            or res.get("dry_run") is not False):
        return res
    ws = (res.get("workspace") or {}).get("key")
    prior_ws = prior.get("workspace") or {}
    same_ws = bool(ws) and prior_ws.get("key") == ws
    if (same_ws and res.get("datalake_ocid") and prior.get("datalake_ocid")
            and res["datalake_ocid"] != prior["datalake_ocid"]):
        same_ws = False          # a key is only unique within one platform
    if same_ws and prior_ws.get("created") and not res["workspace"].get(
            "created"):
        res["workspace"].update(
            created=True, created_run=prior_ws.get("created_run")
            or prior.get("run"), created_by_earlier_push=True)
    _carry_credentials(res, prior, same_ws)
    records = cluster_records(prior)
    owned = {r["cluster"]: r for r in records if r["provenance"] == CREATED}
    unknown = {r["cluster"] for r in records if r["provenance"] == UNKNOWN}
    here = [res.get("cluster") or {}, *(res.get("warehouse_clusters") or [])]
    for rec in here:
        earlier = owned.get(rec.get("key")) if same_ws else None
        if rec.get("created") or rec.get("uses_existing"):
            continue
        if earlier is None:
            if same_ws and rec.get("key") in unknown:
                rec["provenance_unknown"] = True
            continue
        was = earlier["record"]
        rec.update(created=True,
                   created_run=was.get("created_run") or prior.get("run"),
                   created_by_earlier_push=True)
        if was.get("created_at") and not rec.get("created_at"):
            rec["created_at"] = was["created_at"]
    recorded = {(ws, rec.get("key")) for rec in here
                if same_ws and rec.get("key")
                and (rec.get("created") or rec.get("provenance_unknown"))}
    kept, seen = [], set()
    for r in records:
        # A create that was accepted and never listed is carried too, by
        # name: teardown keeps saying it cannot identify it until someone
        # does, rather than a later push forgetting it was ever asked for.
        at = (r["workspace"], r["cluster"] or f'name:{r["name"]}')
        if (r["provenance"] not in (CREATED, REQUESTED, UNKNOWN)
                or at in recorded or at in seen):
            continue
        seen.add(at)
        was = r["record"]
        entry = {"kind": "cluster", "key": r["cluster"], "name": r["name"],
                 "role": r["role"], "workspace": r["workspace"],
                 "created": r["provenance"] == CREATED,
                 "created_run": was.get("created_run") or prior.get("run")}
        if r["provenance"] == REQUESTED:
            entry["create_requested"] = True
        if r["provenance"] == UNKNOWN:
            entry["provenance_unknown"] = True
        if was.get("created_at"):
            entry["created_at"] = was["created_at"]
        if prior.get("datalake_ocid"):
            entry["datalake_ocid"] = prior["datalake_ocid"]
        kept.append(entry)
    if kept:
        res["earlier_allocations"] = kept
    return res


def _carry_credentials(res: dict, prior: dict, same_ws: bool) -> None:
    """Every credential placement stays tracked. Objects the earlier record
    tracked on the same workspace are kept in `credential_objects` (one
    this push's --source-config replaces is flagged in
    `credential_superseded`: it still holds the previous credential);
    objects on another workspace go to `earlier_credential_objects`."""
    missing = set(res.get("credential_missing") or [])
    earlier = [dict(e) for e in prior.get("earlier_credential_objects") or []]
    if not same_ws:
        where = (prior.get("workspace") or {}).get("key")
        for obj in [*(prior.get("credential_objects") or []),
                    *(prior.get("credential_unconfirmed") or [])]:
            earlier.append({"workspace": where, "path": obj})
    else:
        mine = res.setdefault("credential_objects", [])
        unsure = res.setdefault("credential_unconfirmed", [])
        superseded = [o for o in prior.get("credential_superseded") or []]
        requested = res.get("credential_requested")
        for obj in prior.get("credential_objects") or []:
            if obj in mine or obj in missing or obj in unsure:
                continue
            mine.append(obj)
            if requested and obj != requested and obj not in superseded:
                superseded.append(obj)
        for obj in prior.get("credential_unconfirmed") or []:
            if obj not in mine and obj not in missing and obj not in unsure:
                unsure.append(obj)
        superseded = [o for o in superseded if o in mine or o in unsure]
        if superseded:
            res["credential_superseded"] = superseded
    if earlier:
        res["earlier_credential_objects"] = earlier


# Seconds one GET of a pre-authenticated download URL may take. The process
# default socket timeout is None, so without it a stalled object-storage GET
# held `fetch` -- and `run` after a successful discovery -- open for ever.
DOWNLOAD_TIMEOUT = 120


def download_ws_file(call: Callable[..., dict], *, workspace: str,
                     path: str, dest: pathlib.Path,
                     opener: Callable | None = None,
                     timeout: float = DOWNLOAD_TIMEOUT,
                     sleep: Callable[[float], None] | None = None) -> dict:
    """Bring one workspace file down to `dest`; returns {path, dest, size}.

    Two steps, the console's own: ask for a pre-authenticated URL, then GET
    it. The URL grants read access to the object for as long as it lives,
    so it is never printed, logged or returned -- nor carried by an error or
    a retry line. The GET is bounded by `timeout` and retried by the read
    rule (a transient error or a timeout). The byte count is checked
    against the size the server reported: a short read is an error, not a
    smaller file.
    """
    import urllib.request

    meta = call("download_ws_file", workspace=workspace, path=path)
    url = meta.get("parUrl")
    if not url:
        raise ProvisionTransportError(
            f"download {path}: the server returned no download URL "
            f"(fields: {sorted(k for k in meta if not k.startswith('_'))})")

    def get() -> bytes:
        try:
            with (opener or urllib.request.urlopen)(url,
                                                     timeout=timeout) as resp:
                return resp.read()
        except Exception as exc:
            text = str(exc).replace(url, "<pre-authenticated URL>")
            # `from None`: the original exception may carry the URL.
            raise ProvisionTransportError(
                f"download {path}: GET failed: {text[:200]}") from None

    data = retry_call(get, label=f"download {path}",
                      retryable=is_retryable(read=True), sleep=sleep)
    expected = meta.get("size")
    if expected is not None and int(expected) != len(data):
        raise ProvisionTransportError(
            f"download {path}: read {len(data)} byte(s), the server reported "
            f"{expected}; nothing was written")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    return {"path": path, "dest": str(dest), "size": len(data)}


def _provision_next(res: dict) -> str:
    """The runbook step that follows this push. It used to say "run the
    discover job (or the script by hand)" after every push: that skipped the
    catalogs (S3/S4), offered a hand run the runbook forbids, and was still
    printed after the plan push, when S10 is next."""
    tail = " Every run is a human's call; no schedule was created."
    if res["dry_run"]:
        return "Next: re-run with `--execute` after reviewing the plan above."
    if res.get("copy_jobs"):
        return ("Next: run `snowmig_01_structure` (runbook S10) with "
                "`snowmig run`. The per-schema copy jobs are registered and "
                "never run by the migrator: copying rows is the customer's "
                "decision." + tail)
    return ("Next: register the catalogs with `snowmig catalog` -- the "
            "EXTERNAL source (runbook S3), then the INTERNAL target (S4) -- "
            "and then run the `snowmig_00_discover` job (S6) with "
            "`snowmig run`." + tail)


def render_provision(res: dict) -> str:
    lines = ["# Provisioning — the migration environment inside AIDP", ""]
    if res["dry_run"]:
        lines += ["**DRY RUN — nothing was created.** Re-run with `--execute` "
                  "after reviewing the plan below.", ""]
    lines += [
        f'Workspace: `{res["workspace"]["name"]}`'
        + (f' (translated from `{res["workspace"]["requested"]}` — '
           + "; ".join(res["workspace"]["notes"]) + ")"
           if res["workspace"]["renamed"] else ""),
        f'Cluster: `{res["cluster"]["name"]}`',
        f'Scripts: `{res["scripts_folder"]}` · Plan: `{res["plan_folder"]}` · '
        f'Reports: `{res["reports_folder"]}`',
        ""]
    ws_key = (res.get("workspace") or {}).get("key")
    cl_key = (res.get("cluster") or {}).get("key")
    if not res["dry_run"] and ws_key and cl_key:
        # The keys are what every later command needs, and they are not the
        # display names above. They used to be only in provision_result.json
        # (the steps table showed them only with --reuse-existing).
        lines += [
            "## Hand-off — the keys every later command needs", "",
            "| | Key |", "|---|---|",
            f"| workspace | `{ws_key}` |", f"| cluster | `{cl_key}` |", "",
            f"Pass `--workspace {ws_key} --cluster-id {cl_key}` on each later "
            "command, or put them under `aidp.workspace` / `aidp.cluster_id` "
            "in the config -- one or the other. They are never read from "
            "this record implicitly.", ""]

    if res.get("warehouse_clusters"):
        lines += ["## Snowflake warehouses → AIDP compute clusters", "",
                  "Same name, **AIDP default config**. The source size is "
                  "reported and deliberately NOT carried over: a Snowflake "
                  "warehouse size is not a Spark shape, and "
                  "`COMPUTE_PROPOSAL.md` keeps that a decision rather than a "
                  "default.", "",
                  "| Warehouse | Source size | AIDP cluster | Renamed |",
                  "|---|---|---|---|"]
        for target in res["warehouse_clusters"]:
            note = ("; ".join(target.get("notes") or [])
                    if target.get("renamed") else "no")
            cluster = (f'existing cluster `{target.get("existing_cluster")}` '
                       f'(not created by this migration)'
                       if target.get("uses_existing")
                       else f'`{target["name"]}`')
            lines.append(f'| `{target["warehouse"]}` | '
                         f'{target.get("source_size") or "unknown"} | '
                         f'{cluster} | {note} |')
        lines.append("")

    if res.get("earlier_allocations"):
        lines += [
            "## Allocated by an earlier push (still this migration's)", "",
            "Created by an earlier executed push of this migration and not "
            "recorded again by this one, so they are carried here: "
            "`teardown` still reaches them.", "",
            "| Cluster | Key | Role | Workspace | Created by push |",
            "|---|---|---|---|---|"]
        lines += [f'| `{a.get("name")}` | `{a.get("key")}` | {a.get("role")} '
                  f'| `{a.get("workspace")}` | {a.get("created_run") or "?"} |'
                  for a in res["earlier_allocations"]]
        lines.append("")

    if res.get("credential_objects"):
        lines += [
            "## Credential placed on the workspace", "",
            "`--source-config` puts the Snowflake connection -- the "
            "`snowflake:` block of the migration config, **credential "
            "included**; the `aidp:` block is not copied -- on the workspace "
            "mount so the in-AIDP scripts can reach Snowflake themselves. It "
            "is readable by **every member of this workspace and every "
            "cluster in it** via `/Workspace`, for as long as it stays "
            "there:", ""]
        superseded = set(res.get("credential_superseded") or [])
        lines += [f"- `{obj}`" + (" -- superseded by this push's "
                                  "`--source-config`; it still holds the "
                                  "previous credential; remove it"
                                  if obj in superseded else "")
                  for obj in res["credential_objects"]]
        lines += ["",
                  "Remove it from the workspace once the migration is done, "
                  "and rotate the Snowflake credential if anyone who must "
                  "not hold it can read this workspace.", ""]
    if res.get("credential_unconfirmed"):
        lines += ["## Credential placement NOT confirmed", "",
                  "An upload of the Snowflake credential was attempted and "
                  "could not be read back. It may or may not be on the "
                  "workspace; check, and remove it if it is:", ""]
        lines += [f"- `{obj}`" for obj in res["credential_unconfirmed"]]
        lines.append("")
    if res.get("earlier_credential_objects"):
        lines += ["## Credential placed by an earlier push on another "
                  "workspace", "",
                  "Still tracked here so it is not forgotten; remove it "
                  "there once the migration is done:", ""]
        lines += [f'- `{e.get("path")}` in workspace `{e.get("workspace")}`'
                  for e in res["earlier_credential_objects"]]
        lines.append("")

    if res.get("copy_jobs"):
        statuses = {j.get("status") for j in res["copy_jobs"]}
        if res["dry_run"]:
            verdict = ("**Would be registered** by `--execute`, and never "
                       "run by it")
        elif statuses <= {"created", "reused"}:
            verdict = "**Registered, never run**"
        else:
            verdict = ("**NOT all registered** -- see the Status column; "
                       "none is ever run by provision")
        lines += [
            "## Per-schema copy workflows (runbook S11)", "",
            f'{len(res["copy_jobs"])} job(s), one per schema of the approved '
            "`ddl_plan.json`, each with ONE task running the SAME "
            "`02_copy_schema` notebook and passing `schema` as a task "
            "parameter, which the notebook reads at run time "
            f"(`oidlUtils.parameters.getParameter`). {verdict}: moving rows "
            "is the customer's decision.", "",
            "| Schema (task parameter) | Job | Notebook | Status |",
            "|---|---|---|---|"]
        lines += [f'| `{j["schema"]}` | `{j["job"]}` | `{j["notebook"]}` | '
                  f'{j.get("status") or "—"} |'
                  for j in res["copy_jobs"]]
        lines.append("")

    deleted = set(res.get("deleted_copy_jobs") or [])
    still = [n for n in res.get("stale_copy_jobs") or [] if n not in deleted]
    if deleted:
        lines += [
            "## Copy jobs NOT in this plan — deleted (--delete-stale-copy-jobs)",
            "", "Registered by an earlier push for a schema the present plan "
            "does not name; deleted by this push and read back gone:", ""]
        lines += [f"- `{name}`" for name in sorted(deleted)]
        lines.append("")
    if still:
        lines += [
            "## Copy jobs NOT in this plan (still on the workspace)", "",
            "Registered by an earlier push for a schema the present plan "
            "does not name. They are still runnable; delete them in the "
            "console, re-run provision with --delete-stale-copy-jobs, or "
            "re-plan to include the schema:", ""]
        lines += [f"- `{name}`" for name in still]
        lines.append("")

    if res.get("notebooks_kept"):
        lines += [
            "## Stage notebooks kept as found", "",
            "`--reuse-existing` left these notebooks as they are on the "
            "workspace, so whatever their PARAMS cells hold (schema, mode, "
            "verify, counts) still holds. Pass `--refresh-notebooks` to "
            "regenerate them from this run's flags -- that discards console "
            "edits:", ""]
        lines += [f'- `{res["scripts_folder"]}/{n}`'
                  for n in res["notebooks_kept"]]
        lines.append("")

    if res.get("stage_params"):
        lines += [
            "## Stage parameters (`--stage-param`)", "",
            "Written into the PARAMS cell of every stage notebook that "
            "declares the name; a `<stage>.<name>` only into that "
            "stage's:", ""]
        lines += [f"- `{k}` = `{v}`" for k, v in res["stage_params"].items()]
        lines.append("")

    lines += [
        "| Step | Action | Verified | Detail |", "|---|---|---|---|"]
    for s in res["steps"]:
        verified = {True: "yes", False: "**no**", None: "—"}[s["verified"]]
        # One line per cell: a raw CLI error carries newlines and pipes, and
        # either one breaks the table it lands in.
        detail = " ".join(str(s["detail"] or "").split()).replace("|", "\\|")
        lines.append(f'| {s["step"]} | {s["action"]} | {verified} | '
                     f'{detail} |')
    lines += [
        "",
        "Pending is pending: `create_requested` means the API accepted the "
        "request and the object never became visible within the poll budget "
        "— check the console before proceeding. `read_back_failed` in its "
        "detail means the listing itself errored: the object may well exist, "
        "it just could not be looked at.",
        "",
        _provision_next(res),
        ""]
    return "\n".join(lines)
