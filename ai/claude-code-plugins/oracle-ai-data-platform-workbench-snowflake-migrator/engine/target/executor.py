"""AIDP execution backends. Command construction is pure and testable.

The ladder, in order of preference:

  1. `oci raw-request` -- the documented REST API, used whenever `oci` is
     installed (`detect_backend`). `oci ai-data-platform` covers only the
     control plane (instance lifecycle, work requests), NOT catalogs,
     schemas, tables or clusters, so those go through raw-request.
  2. `aidp` CLI -- the fallback when `oci` is absent. Workspace files, job
     cancel/delete and catalog delete always use it (`provision_api.py`).

If neither is present that is a loud failure, not a silent no-op.

⚠️ The exact `aidp` CLI flags and the REST paths below are UNVERIFIED against a
live deployment -- the CLI is not installed here and no AIDP environment was
available. That is why `build_command` is pure and every command is printed
before it runs: a human can check the command against their deployment before
anything executes. Getting a flag wrong should produce an obvious CLI usage
error, not a silent partial migration.

⚠️ PATH-FAMILY NOTE (2026-09-16): Oracle's current REST reference documents
`/20260430/aiDataPlatforms/{id}/...` -- not this module's
`/20240831/dataLakes/{id}/...` -- and adds an `aidp-async-operation-key`
waiter and jobs/clusters/workspaces surfaces. The legacy family here is what
one live migration verified, so it stays until a live run proves the new
one. Everything NEW is built on the documented contract in
`provision_api.py`. Two corrections the doc already settles: there is NO SQL
endpoint (the 404 is real, permanently), and there is NO `notebookRuns`
endpoint -- programmatic notebook execution is a Job with a NOTEBOOK_TASK, so
`run_notebook`/`run_status` below are legacy guesses kept only until the
job-based path replaces them.
"""
from __future__ import annotations

import json
import re
import shutil
import urllib.parse
from typing import Callable

from .coords import region_from_ocid

__all__ = ["BackendError", "NoBackendAvailable", "StatementTooLarge",
           "BACKENDS",
           "build_command", "detect_backend", "parse_cli_json",
           "parse_cli_envelope", "collect_pages", "paged_uri",
           "MAX_ARGV_STATEMENT", "MAX_LIST_PAGES"]

# SQL is passed as one argv element. A single argument is capped well below
# ARG_MAX (128KB on Linux, 256KB on macOS), and exceeding it produces a bare
# E2BIG from the kernel with no hint about what to do. Refuse earlier, and say
# which knob fixes it.
MAX_ARGV_STATEMENT = 100_000

BACKENDS = ("aidp_cli", "oci_raw")

_API_VERSION = "20240831"

# A list endpoint answers one PAGE and names the next in the `opc-next-page`
# response header, which the next GET sends back as `page=`. Following it
# is what makes a listing the whole collection; the cap is there so a server
# that keeps handing out tokens cannot hold a deploy forever.
MAX_LIST_PAGES = 200


class BackendError(RuntimeError):
    """The backend returned an error. Raised, never returned as data.

    `oci raw-request` exits 0 on an HTTP error and puts the error in the
    response BODY, so exit status proves nothing. Treating that body as data
    made a 404 look like one row of results, and the smoke test reported PASS
    against an endpoint that does not exist.
    """


class NoBackendAvailable(RuntimeError):
    """Neither the aidp CLI nor the oci CLI is installed."""


class StatementTooLarge(ValueError):
    """The SQL will not fit in a single command-line argument."""


def detect_backend(*, which: Callable[[str], str | None] = shutil.which) -> str:
    """Pick a transport, preferring the one whose contract was verified.

    `oci_raw` (the documented REST surface driven through `oci raw-request`)
    is what the whole control plane was live-verified against. The `aidp`
    CLI's control-plane subcommands were NOT: this module had guessed
    `--datalake-id` and an `--output json` flag, neither of which exists in
    CLI 4.2.1, so preferring it meant every AIDP-side check on a machine with
    `aidp` installed took a path that could not work. The CLI IS verified for
    workspace files, which `provision_api.py` drives with its own builder.
    """
    if which("oci"):
        return "oci_raw"
    if which("aidp"):
        return "aidp_cli"
    raise NoBackendAvailable(
        "no AIDP execution backend found: install the `oci` CLI (preferred) "
        "or the `aidp` CLI. The migrator will not guess at a transport.")


def _endpoint(target) -> str:
    region = region_from_ocid(target.datalake_ocid)
    return f"https://aidp.{region}.oci.oraclecloud.com/{_API_VERSION}"


def paged_uri(uri: str, page: str | None) -> str:
    """`uri` asking for `page`, the token a previous response named in its
    `opc-next-page` header. Byte-identical to `uri` when there is none, so
    a first request looks exactly as it did before pagination existed."""
    if not page:
        return uri
    sep = "&" if "?" in uri else "?"
    return f"{uri}{sep}page={urllib.parse.quote(str(page), safe='')}"


def collect_pages(fetch: Callable[[str | None], tuple[list[dict], str | None]],
                  operation: str, *,
                  max_pages: int = MAX_LIST_PAGES) -> list[dict]:
    """Every item of a paged listing. `fetch(page_token)` performs ONE request
    (None for the first page) and returns (rows, next_token); the loop ends
    when the server sends no token. A token seen twice, or more than
    `max_pages` pages, raises rather than spinning: a listing that cannot
    finish is an error, not a shorter list."""
    items: list[dict] = []
    token: str | None = None
    seen: set[str] = set()
    for _ in range(max_pages):
        rows, nxt = fetch(token)
        items.extend(rows)
        if not nxt:
            return items
        if nxt in seen:
            raise RuntimeError(
                f"{operation}: the server returned page token {nxt!r} twice; "
                f"pagination did not terminate, so the listing is unusable")
        seen.add(nxt)
        token = nxt
    raise RuntimeError(
        f"{operation}: more than {max_pages} pages; pagination did not "
        f"terminate, so the listing is unusable")


def build_command(backend: str, operation: str, target, **kwargs) -> list[str]:
    """Build the argv for one operation. Pure -- runs nothing.

    An `aidp` invocation gets the global flags appended here rather than at
    each of the dozen call sites: the CLI's default auth is `security_token`,
    and it needs the region explicitly. Omitting them is an auth failure that
    reads like a permissions problem.
    """
    argv = _build_command(backend, operation, target, **kwargs)
    if argv and argv[0] == "aidp":
        if "--auth" not in argv:
            argv += ["--auth", str(kwargs.get("cli_auth") or "api_key")]
        if "--region" not in argv:
            argv += ["--region", region_from_ocid(target.datalake_ocid)]
    return argv


def _build_command(backend: str, operation: str, target, **kwargs) -> list[str]:
    if backend not in BACKENDS:
        raise ValueError(f"unknown backend {backend!r}; expected one of {BACKENDS}")

    if operation == "sql":
        sql = kwargs["sql"]
        if len(sql.encode()) > MAX_ARGV_STATEMENT:
            raise StatementTooLarge(
                f"the batch is {len(sql.encode()):,} bytes, over the "
                f"{MAX_ARGV_STATEMENT:,}-byte limit for one command-line "
                f"argument. Lower --chunk-size and re-run; the deployment is "
                f"chunked precisely so this is adjustable.")
        if backend == "aidp_cli":
            return ["aidp", "sql", "execute",
                    "--instance-id", target.datalake_ocid,
                    "--workspace-id", target.workspace,
                    "--cluster-id", target.cluster_id,
                    "--catalog", target.catalog,
                    "--statement", sql]
        return ["oci", "raw-request", "--http-method", "POST",
                "--target-uri",
                f"{_endpoint(target)}/dataLakes/{target.datalake_ocid}"
                f"/workspaces/{target.workspace}/sql/execute",
                "--request-body",
                json.dumps({"clusterId": target.cluster_id,
                            "catalog": target.catalog,
                            "statement": sql})]

    # --- catalog CRUD: the working transport for a structure-only clone ---
    if operation in ("create_schema", "create_table", "create_view"):
        relation = {"create_schema": "schemas", "create_table": "tables",
                    "create_view": "views"}[operation]
        body = json.dumps(kwargs["body"])
        if backend == "aidp_cli":
            return ["aidp", "schema",
                    {"create_schema": "create", "create_table": "create-table",
                     "create_view": "create-view"}[operation],
                    "--instance-id", target.datalake_ocid,
                    "--from-json", body]
        return ["oci", "raw-request", "--http-method", "POST",
                "--target-uri", f"{_endpoint(target)}/dataLakes/"
                                f"{target.datalake_ocid}/{relation}",
                "--request-body", body]

    if operation == "create_catalog":
        # The body carries the credential; given a spooled file it travels as
        # file:// rather than argv, where `ps` shows it to every user.
        body = (f'file://{kwargs["body_file"]}' if kwargs.get("body_file")
                else json.dumps(kwargs["body"]))
        if backend == "aidp_cli":
            return ["aidp", "catalog", "create",
                    "--instance-id", target.datalake_ocid,
                    "--from-json", body]
        return ["oci", "raw-request", "--http-method", "POST",
                "--target-uri", f"{_endpoint(target)}/dataLakes/"
                                f"{target.datalake_ocid}/catalogs",
                "--request-body", body]

    # The aidp CLI branches below take no `page`: its paging flags are
    # undocumented, and a guessed one is a usage error on every second page.
    # The oci_raw branches send the token back as `page=`.
    page = kwargs.get("page")

    if operation == "list_catalogs":
        if backend == "aidp_cli":
            return ["aidp", "catalog", "list",
                    "--instance-id", target.datalake_ocid]
        return ["oci", "raw-request", "--http-method", "GET",
                "--target-uri", paged_uri(
                    f"{_endpoint(target)}/dataLakes/"
                    f"{target.datalake_ocid}/catalogs", page)]

    if operation == "delete_table":
        key = f'{kwargs["catalog"]}.{kwargs["schema"]}.{kwargs["table"]}'
        if backend == "aidp_cli":
            return ["aidp", "schema", "delete-table", "--instance-id",
                    target.datalake_ocid, "--key", key]
        return ["oci", "raw-request", "--http-method", "DELETE",
                "--target-uri", f"{_endpoint(target)}/dataLakes/"
                                f"{target.datalake_ocid}/tables/{key}"]

    if operation == "delete_view":
        key = f'{kwargs["catalog"]}.{kwargs["schema"]}.{kwargs["view"]}'
        if backend == "aidp_cli":
            return ["aidp", "schema", "delete-view", "--instance-id",
                    target.datalake_ocid, "--key", key]
        return ["oci", "raw-request", "--http-method", "DELETE",
                "--target-uri", f"{_endpoint(target)}/dataLakes/"
                                f"{target.datalake_ocid}/views/{key}"]

    if operation == "delete_schema":
        key = f'{kwargs["catalog"]}.{kwargs["schema"]}'
        if backend == "aidp_cli":
            return ["aidp", "schema", "delete", "--instance-id",
                    target.datalake_ocid, "--key", key]
        return ["oci", "raw-request", "--http-method", "DELETE",
                "--target-uri", f"{_endpoint(target)}/dataLakes/"
                                f"{target.datalake_ocid}/schemas/{key}"]

    if operation == "list_schemas":
        if backend == "aidp_cli":
            return ["aidp", "schema", "list", "--instance-id",
                    target.datalake_ocid, "--catalog-key", target.catalog]
        return ["oci", "raw-request", "--http-method", "GET",
                "--target-uri", paged_uri(
                    f"{_endpoint(target)}/dataLakes/"
                    f"{target.datalake_ocid}/schemas"
                    f"?catalogKey={target.catalog}", page)]

    if operation in ("list_tables_in", "list_views_in"):
        relation = "tables" if operation == "list_tables_in" else "views"
        # Fully qualified: a bare schemaKey returns 400 InvalidParameter.
        schema = kwargs["schema"]
        qualified = (schema if schema.startswith(f'{kwargs["catalog"]}.')
                     else f'{kwargs["catalog"]}.{schema}')
        if backend == "aidp_cli":
            return ["aidp", "schema",
                    "list-tables" if relation == "tables" else "list-views",
                    "--instance-id", target.datalake_ocid,
                    "--catalog-key", kwargs["catalog"],
                    "--schema-key", qualified]
        return ["oci", "raw-request", "--http-method", "GET",
                "--target-uri", paged_uri(
                    f"{_endpoint(target)}/dataLakes/"
                    f"{target.datalake_ocid}/{relation}"
                    f'?catalogKey={kwargs["catalog"]}'
                    f"&schemaKey={qualified}", page)]

    if operation in ("get_table", "get_view"):
        relation = "tables" if operation == "get_table" else "views"
        name = kwargs.get("table") or kwargs.get("view")
        # Objects are addressed by their fully-qualified KEY.
        key = f'{kwargs["catalog"]}.{kwargs["schema"]}.{name}'
        if backend == "aidp_cli":
            return ["aidp", "schema",
                    "get-table" if operation == "get_table" else "get-view",
                    "--instance-id", target.datalake_ocid,
                    "--key", key]
        return ["oci", "raw-request", "--http-method", "GET",
                "--target-uri", f"{_endpoint(target)}/dataLakes/"
                                f"{target.datalake_ocid}/{relation}/{key}"]

    if operation == "list_tables":
        schema = kwargs["schema"]
        if backend == "aidp_cli":
            return ["aidp", "catalog", "list-tables",
                    "--instance-id", target.datalake_ocid,
                    "--catalog", target.catalog,
                    "--schema", schema]
        # schemaKey must be FULLY QUALIFIED. A bare schema returns 400
        # InvalidParameter -- verified live.
        qualified = (schema if schema.startswith(f"{target.catalog}.")
                     else f"{target.catalog}.{schema}")
        return ["oci", "raw-request", "--http-method", "GET",
                "--target-uri", paged_uri(
                    f"{_endpoint(target)}/dataLakes/{target.datalake_ocid}"
                    f"/tables?catalogKey={target.catalog}"
                    f"&schemaKey={qualified}", page)]

    if operation == "upload_notebook":
        path, local = kwargs["workspace_path"], kwargs["local_path"]
        if backend == "aidp_cli":
            return ["aidp", "workspace", "upload",
                    "--instance-id", target.datalake_ocid,
                    "--workspace-id", target.workspace,
                    "--path", path, "--file", local]
        return ["oci", "raw-request", "--http-method", "PUT",
                "--target-uri",
                f"{_endpoint(target)}/dataLakes/{target.datalake_ocid}"
                f"/notebook/workspaces/{target.workspace}/api/contents{path}",
                "--request-body", f"file://{local}"]

    if operation == "run_notebook":
        path = kwargs["workspace_path"]
        if backend == "aidp_cli":
            return ["aidp", "notebook", "run",
                    "--instance-id", target.datalake_ocid,
                    "--workspace-id", target.workspace,
                    "--cluster-id", target.cluster_id,
                    "--path", path]
        return ["oci", "raw-request", "--http-method", "POST",
                "--target-uri",
                f"{_endpoint(target)}/dataLakes/{target.datalake_ocid}"
                f"/workspaces/{target.workspace}/notebookRuns",
                "--request-body",
                json.dumps({"clusterId": target.cluster_id, "notebookPath": path})]

    if operation == "run_status":
        run_id = kwargs["run_id"]
        if backend == "aidp_cli":
            return ["aidp", "notebook", "run-status",
                    "--instance-id", target.datalake_ocid,
                    "--run-id", run_id]
        return ["oci", "raw-request", "--http-method", "GET",
                "--target-uri",
                f"{_endpoint(target)}/dataLakes/{target.datalake_ocid}"
                f"/workspaces/{target.workspace}/notebookRuns/{run_id}"]

    raise ValueError(f"unknown operation {operation!r}")


def parse_cli_json(stdout: str) -> list[dict]:
    """Parse CLI/REST JSON into a row list.

    Non-JSON output raises. A silent empty list here would make a failed create
    indistinguishable from a success that returned no rows.

    An HTTP error carried in the BODY also raises. `oci raw-request` exits 0 on
    a 404 and returns `{"data": {"code": ...}, "status": "404 Not Found"}`, so
    the exit code proves nothing and the old `return [payload]` turned that
    error object into one row of "results".
    """
    return parse_cli_envelope(stdout)[0]


def parse_cli_envelope(stdout: str) -> tuple[list[dict], dict[str, str]]:
    """The rows AND the response headers of one CLI/REST envelope, header
    names lower-cased; `{}` when the output carries none.

    `oci raw-request` prints `{"data": ..., "headers": {...}, "status": ...}`
    and a list endpoint names its next page in the `opc-next-page` header,
    so the headers are part of the answer: dropping them read every listing
    as its first page. Everything `parse_cli_json` says about non-JSON output
    and errors in the body holds here too.
    """
    text = (stdout or "").strip()
    # The aidp CLI prefixes its JSON with a literal `Response:` line. Without
    # this, every successful aidp call read as "backend output is not JSON".
    if text.startswith("Response:"):
        text = text[len("Response:"):].strip()
    if not text:
        return [], {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"backend output is not JSON ({exc.msg}): {text[:300]}") from exc

    if isinstance(payload, list):
        return payload, {}
    if not isinstance(payload, dict):
        raise RuntimeError(
            f"unexpected backend payload type: {type(payload).__name__}")

    _raise_if_error(payload)

    raw_headers = payload.get("headers")
    headers = ({str(k).lower(): v for k, v in raw_headers.items()}
               if isinstance(raw_headers, dict) else {})
    return _rows(payload), headers


def _rows(payload: dict) -> list[dict]:
    data = payload.get("data")
    # Collections arrive as {"data": {"items": [...]}} -- unwrap, or three
    # schemas get reported as one.
    if isinstance(data, dict) and isinstance(data.get("items"), list):
        return data["items"]
    for key in ("data", "items", "rows", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
    if isinstance(data, dict):
        return [data]
    return [payload]


def _raise_if_error(payload: dict) -> None:
    """Raise BackendError if this envelope reports an HTTP or service error."""
    status = str(payload.get("status") or "")
    code_match = re.match(r"\s*(\d{3})", status)
    if code_match and not 200 <= int(code_match.group(1)) < 300:
        detail = payload.get("data")
        raise BackendError(
            f"backend returned {status.strip()}: "
            f"{json.dumps(detail)[:300] if detail is not None else '(no body)'}")

    # Defence in depth: an OCI error object is recognisable without a status.
    data = payload.get("data")
    if isinstance(data, dict) and "code" in data and "message" in data \
            and "items" not in data:
        raise BackendError(
            f'backend returned an error: {data["code"]} — {data["message"]}')
