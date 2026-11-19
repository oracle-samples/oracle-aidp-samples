"""Bind a resolved target to a `run_sql(sql, params=None) -> list[dict]` callable
backed by the `aidp` CLI or `oci raw-request`.

The subprocess call is injected so this module is unit-testable without either
CLI installed. A non-zero exit or non-JSON output raises: a silent empty result
would make a failed CREATE indistinguishable from a success returning no rows.

`dry_run=True` returns (run_sql, planned_commands) and invokes nothing, so the
exact command can be shown to a human before anything executes.
"""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
from typing import Callable

from retry import is_retryable, retry_call
from .coords import region_from_ocid
from .executor import build_command, parse_cli_json

__all__ = ["BackendError", "CatalogTransportError", "make_run_sql",
           "spool_body"]

_MAX_STDERR = 500


# The `oci` CLI answers an expired session profile by PROMPTING on stdout
# ("Do you want to re-authenticate your CLI session profile? [Y/n]:") and,
# with no tty, exiting 1 with "Abort:" on stderr. Truncated into a transport
# error that reads "failed (exit 1): Abort:", which says nothing. The state
# is ordinary and the remedy is one command, so it is named.
_EXPIRED_SESSION = "this cli session has expired"


def _session_expired(*streams: str | None) -> bool:
    return any(_EXPIRED_SESSION in (s or "").lower() for s in streams)


def expired_session_message(profile: str | None, region: str | None) -> str:
    who = f" --profile {profile}" if profile else ""
    where = f" --region {region}" if region else ""
    # "This call", not "nothing": the message is raised by ONE call, and a
    # run submitted by an earlier call in the same stage was sent. Saying
    # nothing was sent after a job run was accepted invited a second run.
    return ("the OCI CLI session profile has expired, so this call was not "
            f"sent. Refresh it with `oci session authenticate{who}{where}` "
            "and re-run.")


def spool_body(body: dict, *, prefix: str) -> str:
    """Write a credential-bearing request body to a private temp file.

    In argv a body is visible to every user on the host via `ps`, and
    process-creation auditing records it permanently. So a body that carries
    `connectionDetails` travels by file -- created 0600 where the OS has mode
    bits -- and the command references the path. The caller unlinks it after
    the call, success or failure. One helper for every transport, so the
    `create_catalog` and `testConnection` paths cannot drift apart again.
    """
    fd, path = tempfile.mkstemp(prefix=prefix, suffix=".json")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(body, fh)
    return path


def is_conflict(exc: Exception) -> bool:
    """A 409 "ongoing operation": the resource is still settling after its
    own POST returned. Worth retrying with a bounded backoff.

    Shared because it is a fact about the platform, not about a caller: the
    catalog deploy and the provisioner both meet it, and two copies of a
    rule about someone else's API is two things to update when that API
    grows another way of saying the same thing.
    """
    text = str(exc)
    return "409" in text or "ongoing" in text.lower()


def is_active(item: dict) -> bool:
    """ACTIVE -- or carrying no lifecycleState at all, since an absent field
    is not evidence of settling."""
    return str((item or {}).get("lifecycleState") or "ACTIVE").upper() == "ACTIVE"


class BackendError(RuntimeError):
    """The aidp/oci CLI exited non-zero."""


class CliTimeout(RuntimeError):
    """A CLI child did not return inside its budget."""


class CatalogTransportError(RuntimeError):
    """A catalog CRUD call (make_call) failed: a non-zero CLI exit, an
    expired session, a timeout, or a listing the CLI cannot page.

    Named so the CLI reports it as its one-line `error:` with the remedy,
    like ProvisionTransportError and BackendError. It was a bare
    RuntimeError (and a timeout escaped as subprocess.TimeoutExpired), which
    main() does not catch, so `catalogs` on an expired session printed a
    traceback. Still a RuntimeError, so every caller that catches one --
    the deploy's retry and read-back loops -- is unchanged."""


# Generous, because a cluster create legitimately takes minutes -- but
# finite, because the alternative is what happened live: a single child
# that never returned held the stage for 107 minutes while a cluster
# billed, with nothing on the console to say so.
DEFAULT_CLI_TIMEOUT = 900


def run_cli(cmd: list[str], *, run_process=None,
            timeout: int = DEFAULT_CLI_TIMEOUT):
    """Run a CLI command, bounded. Raises CliTimeout rather than hanging."""
    proc = run_process or _default_run_process
    try:
        return proc(cmd, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise CliTimeout(
            f"`{' '.join(cmd[:3])}` timed out after {timeout}s and was "
            f"killed. Nothing here can tell a slow call from a stuck one, "
            f"so the wait is bounded: re-run, and if it recurs check the "
            f"network path to the endpoint before raising the budget.")


def _default_run_process(cmd: list[str], timeout: int | None = None):
    return subprocess.run(cmd, capture_output=True, text=True, check=False,
                          encoding="utf-8", errors="replace",
                          timeout=timeout)


def make_run_sql(target, *, backend: str,
                 run_process: Callable[..., object] | None = None,
                 dry_run: bool = False):
    proc = run_process or _default_run_process
    planned: list[str] = []

    def run_sql(sql: str, params: dict | None = None) -> list[dict]:
        if params:
            raise ValueError(
                "bound parameters are not supported by the AIDP backends; "
                "interpolate before calling")
        cmd = build_command(backend, "sql", target, sql=sql)
        if dry_run:
            planned.append(" ".join(cmd))
            return []
        def attempt():
            result = proc(cmd)
            if getattr(result, "returncode", 0) != 0:
                raise BackendError(
                    f"{backend} exited {result.returncode}: "
                    f"{(result.stderr or '')[:_MAX_STDERR]}")
            # Inside the attempt: `oci raw-request` exits 0 and puts an HTTP
            # error in the body, so a throttled statement is only visible
            # here, where the retry rule can still see it.
            return parse_cli_json(getattr(result, "stdout", ""))
        # A statement may be DDL: repeated only on a refusal that proves it
        # did not run (429), never on a 5xx that may have applied it.
        return retry_call(attempt, label="sql",
                          retryable=is_retryable(read=False))

    return (run_sql, planned) if dry_run else run_sql


def _printable(arg: str) -> str:
    """One argv element, safe to print.

    `connectionDetails` carries the Snowflake credential. The old flat
    truncation only hid it by luck -- any reordering of the body and it
    printed -- so redaction is explicit and keyed on content: the field NAMES
    are shown, the values never are.
    """
    if "connectionDetails" in arg:
        try:
            payload = json.loads(arg)
        except ValueError:
            payload = None
        if isinstance(payload, dict) \
                and isinstance(payload.get("connectionDetails"), dict):
            payload = {**payload, "connectionDetails": {
                k: "<redacted>" for k in payload["connectionDetails"]}}
            arg = json.dumps(payload)
        else:
            return "<redacted: carries connectionDetails>"
    return arg if len(arg) < 200 else arg[:200] + "…<truncated>"


def _is_read(operation: str) -> bool:
    return operation.startswith(("get_", "list_"))


def make_call(target, *, backend: str, run_process=None):
    """A `call(operation, **kwargs) -> dict` for the catalog CRUD transport.

    Separate from `make_run_sql` because the catalog API is not SQL: it takes
    an operation plus a JSON body and returns one object, not rows.
    """
    from .executor import build_command, collect_pages, parse_cli_envelope

    runner = run_process or _default_run_process

    def _once(operation: str, kwargs: dict) -> tuple[list[dict], str | None]:
        """One request: its rows, and the next-page token if the server sent
        one in `opc-next-page`."""
        body = kwargs.get("body")
        spooled = None
        # A create_catalog body carries the credential. In argv it is visible
        # to every user on the host via `ps`, so it travels by file (0600,
        # removed after the call) and the command references the path.
        if operation == "create_catalog" and isinstance(body, dict) \
                and "connectionDetails" in body:
            spooled = spool_body(body, prefix="snowmig_catalog_")
            kwargs = {**kwargs, "body_file": spooled}
        try:
            cmd = build_command(backend, operation, target, **kwargs)

            def attempt():
                print("  $ " + " ".join(_printable(c) for c in cmd))
                try:
                    proc = runner(cmd)
                except subprocess.TimeoutExpired as exc:
                    raise CatalogTransportError(
                        f"{operation}: `{' '.join(cmd[:3])}` timed out after "
                        f"{exc.timeout:g}s and was killed. A slow call and a "
                        f"stuck one look the same from here: re-run, and if it "
                        f"recurs check the network path to the endpoint."
                    ) from exc
                if proc.returncode != 0:
                    if _session_expired(proc.stdout, proc.stderr):
                        raise CatalogTransportError(
                            f"{operation}: "
                            + expired_session_message(
                                getattr(target, "oci_profile", None),
                                region_from_ocid(target.datalake_ocid)))
                    raise CatalogTransportError(
                        f"{operation} failed (exit {proc.returncode}): "
                        f"{(proc.stderr or proc.stdout or '')[:300]}")
                # Inside the attempt, so an HTTP error `oci raw-request`
                # carried in a 0-exit body meets the retry rule too.
                return parse_cli_envelope(proc.stdout)
            # A read is retried on any transient failure; a write only on a
            # response that proves it was not applied (see retry.py). A
            # timed-out write is NOT retried: _NOT_APPLIED does not match it.
            rows, headers = retry_call(
                attempt, label=operation,
                retryable=is_retryable(read=_is_read(operation)))
        finally:
            if spooled:
                try:
                    os.unlink(spooled)
                except OSError:
                    pass
        next_page = headers.get("opc-next-page")
        if next_page and cmd[0] == "aidp":
            # The CLI's paging flags are undocumented, so the rest cannot be
            # asked for. Page one handed back as the whole collection would
            # make every object past it "absent"; say so instead.
            raise CatalogTransportError(
                f"{operation}: the aidp CLI answered with a next-page token "
                f"({str(next_page)[:40]!r}), so this listing holds only its "
                f"first page, and this plugin requests further pages only "
                f"through the oci CLI. Use the oci CLI (backend oci_raw), "
                f"which follows opc-next-page.")
        return rows, next_page

    def call(operation: str, **kwargs) -> dict:
        # A list operation returns a COLLECTION; a create/get returns ONE
        # object. Collapsing both to rows[0] made key resolution see a single
        # schema instead of the list, so every read-back failed while the
        # objects had in fact been created. A collection may also span
        # PAGES: `opc-next-page` is followed until the server stops sending
        # one, so an object past page one is not read as absent either.
        if operation.startswith("list_"):
            items = collect_pages(
                lambda page: _once(operation, {**kwargs, "page": page}
                                   if page else kwargs),
                operation)
            return {"items": items}
        rows, _ = _once(operation, kwargs)
        return rows[0] if rows else {}

    return call
