"""Extract catalog metadata from a Databricks workspace via the UC REST API.

Runs on the migrator host. Needs only DATABRICKS_HOST + DATABRICKS_TOKEN —
no SQL warehouse, no cluster, no notebook job.

Output: a catalog_pack.json with the per-table descriptors needed by
migrate_catalog.py to reconstruct AIDP DDL.

Usage:
    python scripts/extract_catalog_databricks.py \
        --host https://workspace.cloud.databricks.com \
        --token-file ~/.databricks/token \
        --catalogs samples,main \
        --schemas-only samples:tpch,samples:nyctaxi \
        --out reports/catalog_pack_$(date +%Y%m%d).json

The PAT is read from DATABRICKS_TOKEN in the env or from --token-file (a
file only its owner can read, chmod 600). --token <value> is refused with
exit code 2: a token on the command line is visible in `ps`, shell history
and CI transcripts (SEC-NEW-DATABRICKS-03).
If --host is omitted, looks up DATABRICKS_HOST.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import requests


# ---------- credential ingress (SEC-NEW-DATABRICKS-03) ----------
#
# The PAT used to be an ordinary ``--token <value>`` flag. argv is the one
# place a secret must never travel: it is readable by every user on the host
# in ``ps``, it lands in shell history, CI transcripts, terminal recordings
# and the support requests a failing command gets pasted into. The flag is
# kept only so an old command fails loudly (exit 2, remediation text, value
# never stored); the token now comes from DATABRICKS_TOKEN or from a file
# that only its owner can read.

TOKEN_ARGV_REFUSAL = "Do not pass tokens in argv. Use DATABRICKS_TOKEN or --token-file."

#: What a redacted secret is replaced with in anything this script prints or stores.
REDACTED = "***"

#: Permission bits a token file must NOT have on POSIX: anything that lets
#: the group or the world read, write or execute it.
_GROUP_OR_WORLD = 0o077

#: Secrets to scrub from error text (filled by main once the token is known).
_REDACT_SECRETS: list = []


class RejectTokenArgv(argparse.Action):
    """``--token`` is declared only so that it can be refused.

    Declared with ``nargs="?"`` so ``--token dapi...``, ``--token=dapi...``
    and a bare ``--token`` are all consumed by this action rather than
    falling through as an unknown argument. The action never stores the
    value: it calls ``parser.error``, which prints the usage line plus the
    remediation text to stderr and exits 2 -- the value itself is not echoed.
    """

    def __init__(self, option_strings, dest, **kwargs):
        kwargs["nargs"] = "?"
        kwargs.setdefault("help", argparse.SUPPRESS)
        super().__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        parser.error(TOKEN_ARGV_REFUSAL)


class ArgvSafeParser(argparse.ArgumentParser):
    """An ``ArgumentParser`` whose error messages never repeat an argv value.

    argparse echoes the offending text in its own diagnostics -- "ambiguous
    option: --tok=dapi... could match --token, --token-file", "unrecognized
    arguments: -t dapi..." -- so a mistyped ``--token`` would put the PAT on
    stderr and into the CI log after all. Here abbreviated long options are
    off (no "ambiguous option" path), unknown arguments are reported by count
    with the remediation text, and as defence in depth every error message
    is scrubbed of the argv values themselves before it is printed.
    """

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("allow_abbrev", False)
        super().__init__(*args, **kwargs)
        self._argv_values: list = []

    def parse_args(self, args=None, namespace=None):
        self._argv_values = list(sys.argv[1:] if args is None else args)
        namespace, extras = self.parse_known_args(args, namespace)
        if extras:
            self.error("%d unrecognized argument%s (not shown). %s"
                       % (len(extras), "" if len(extras) == 1 else "s", TOKEN_ARGV_REFUSAL))
        return namespace

    def error(self, message):
        super().error(self.redact_argv(message))

    def redact_argv(self, message: str) -> str:
        """*message* with every argv value (and the value half of any
        ``--opt=value``) replaced by REDACTED, longest first. This parser's
        own option strings and values shorter than 8 characters are left
        alone: they are not token-shaped, and replacing them would garble
        ordinary words of the message."""
        values = set()
        for item in self._argv_values:
            values.add(item)
            if "=" in item:
                values.add(item.split("=", 1)[1])
        for value in sorted(values, key=len, reverse=True):
            if len(value) >= 8 and value not in self._option_string_actions:
                message = message.replace(value, REDACTED)
        return message


def _file_mode(path: Path) -> int:
    """Permission bits of *path*; split out so tests can pin the POSIX rule
    on a platform whose filesystem cannot express it."""
    return path.stat().st_mode & 0o777


def read_token_file(path: str | None, *, enforce_mode: bool | None = None) -> str:
    """Return the token held in *path* (first line, stripped), or ``""`` when
    no path was given.

    Refuses a file that is readable by anyone but its owner (``st_mode &
    0o077`` must be 0) -- a 0644 token file is the shell-history problem in a
    different place. *enforce_mode* defaults to "on POSIX only": Windows
    reports 0666 for every ordinary file regardless of its ACL, so the check
    would refuse every file there and is skipped with a note instead. A
    missing or unreadable file raises ``ValueError`` naming the path, never
    the content.
    """
    if not path:
        return ""
    p = Path(path).expanduser()
    if enforce_mode is None:
        enforce_mode = os.name != "nt"
    try:
        if enforce_mode:
            mode = _file_mode(p)
            if mode & _GROUP_OR_WORLD:
                raise ValueError(
                    f"token file {p} is mode {mode:04o}; it must be readable "
                    f"by its owner only (chmod 600 {p})"
                )
        else:
            print(f"[extract] token file {p}: permission check skipped on this platform",
                  file=sys.stderr)
        with open(p, encoding="utf-8") as fh:
            first_line = fh.readline()
    except FileNotFoundError:
        raise ValueError(f"token file not found: {p}") from None
    except (IsADirectoryError, PermissionError) as exc:
        raise ValueError(f"token file {p} could not be read: {type(exc).__name__}") from None
    token = first_line.strip()
    if not token:
        raise ValueError(f"token file {p} is empty")
    return token


def resolve_token(token_file: str | None) -> tuple[str, str]:
    """The PAT and where it came from.

    Returns ``(token, source)`` where *source* is ``"--token-file"``,
    ``"DATABRICKS_TOKEN"`` or ``"none"`` -- the source is what diagnostics
    may print; the token is not.
    """
    token = read_token_file(token_file)
    if token:
        return token, "--token-file"
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if token:
        return token, "DATABRICKS_TOKEN"
    return "", "none"


def reject_url_credentials(host: str | None) -> None:
    """Refuse a ``--host`` / ``DATABRICKS_HOST`` of the form
    ``https://user:token@workspace``: it would put the credential in every
    request URL, every ``requests`` exception message and every proxy log.
    The message deliberately does not echo the offending value."""
    netloc = (host or "").split("://", 1)[-1].split("/", 1)[0]
    if "@" in netloc:
        raise ValueError(
            "credentials in --host / DATABRICKS_HOST are not accepted. " + TOKEN_ARGV_REFUSAL
        )


def _redact(text: str) -> str:
    """*text* with every known secret replaced by :data:`REDACTED`
    (longest first, so a secret containing another is redacted whole)."""
    for secret in sorted({s for s in _REDACT_SECRETS if s}, key=len, reverse=True):
        text = text.replace(secret, REDACTED)
    return text


def _get(url: str, token: str, *, max_attempts: int = 6, **kwargs) -> dict:
    """REST GET with bearer auth + exponential backoff honoring Retry-After.

    For large-scale catalog (100k+ tables): the original
    single-retry policy exhausts immediately under sustained 429s. We now do
    up to 6 attempts with backoff = min(60s, 2 ** (attempt-1)) seconds,
    honoring `Retry-After` when the server returns it.

    Raises requests.HTTPError on final non-retryable failure.
    """
    headers = {"Authorization": f"Bearer {token}"}
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, headers=headers, timeout=60, **kwargs)
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            r = None
        if r is not None and r.status_code < 400:
            return r.json()
        # Retry on 429 + 5xx + transient connection errors
        retryable = r is None or r.status_code == 429 or 500 <= r.status_code < 600
        if not retryable or attempt == max_attempts:
            if r is not None:
                r.raise_for_status()
            assert last_exc is not None
            raise last_exc
        # Backoff: prefer server's Retry-After if present, else exponential
        wait = 2 ** (attempt - 1)
        if r is not None and r.headers.get("Retry-After"):
            try:
                wait = max(wait, int(r.headers["Retry-After"]))
            except ValueError:
                pass
        wait = min(wait, 60)
        time.sleep(wait)
    return {}  # unreachable, satisfies type


def list_catalogs(host: str, token: str) -> list[dict]:
    return _get(f"{host}/api/2.1/unity-catalog/catalogs", token).get("catalogs", [])


def list_schemas(host: str, token: str, catalog_name: str) -> list[dict]:
    url = f"{host}/api/2.1/unity-catalog/schemas?catalog_name={catalog_name}"
    return _get(url, token).get("schemas", [])


def list_tables_in_schema(host: str, token: str, catalog: str, schema: str,
                          max_per_page: int = 50) -> list[dict]:
    """Paginated table listing."""
    out: list[dict] = []
    page_token: str = ""
    while True:
        url = (f"{host}/api/2.1/unity-catalog/tables"
               f"?catalog_name={catalog}&schema_name={schema}"
               f"&max_results={max_per_page}")
        if page_token:
            url += f"&page_token={page_token}"
        resp = _get(url, token)
        out.extend(resp.get("tables", []))
        page_token = resp.get("next_page_token", "")
        if not page_token:
            break
    return out


def get_table_detail(host: str, token: str, full_name: str) -> dict:
    """Get full table descriptor including columns and properties."""
    url = (f"{host}/api/2.1/unity-catalog/tables/{full_name}"
           f"?include_delta_metadata=true&include_browse=true")
    return _get(url, token)


def list_volumes_in_schema(host: str, token: str, catalog: str, schema: str) -> list[dict]:
    """Best-effort volume listing — endpoint may not be available on all workspaces."""
    try:
        url = f"{host}/api/2.1/unity-catalog/volumes?catalog_name={catalog}&schema_name={schema}"
        return _get(url, token).get("volumes", [])
    except requests.HTTPError as e:
        if e.response.status_code in (403, 404):
            return []
        raise


# ---------- main extract ----------

def extract(
    host: str,
    token: str,
    catalog_filter: list[str] | None,
    schema_filter: dict[str, list[str]] | None,
    skip_systems: bool = True,
) -> dict:
    """Walk catalogs -> schemas -> tables and produce a catalog pack.

    catalog_filter: if set, only these catalog names are walked.
    schema_filter: {catalog_name: [schema_names...]} — if a catalog is in here,
        only the listed schemas are walked. Catalogs not in this dict get full sweep.
    skip_systems: drop 'system' and 'information_schema' (they're auto-generated).
    """
    started_at = datetime.utcnow().isoformat() + "Z"
    pack: dict = {
        "format_version": 1,
        "extracted_at": started_at,
        "source_workspace": host,
        "catalogs": [],
        "schemas": [],
        "tables": [],
        "volumes": [],
        "errors": [],
        "stats": {"catalogs": 0, "schemas": 0, "tables_listed": 0,
                  "tables_detailed": 0, "tables_failed": 0, "volumes": 0},
    }

    print(f"[extract] listing catalogs in {host}", flush=True)
    catalogs = list_catalogs(host, token)
    if catalog_filter:
        catalogs = [c for c in catalogs if c.get("name") in catalog_filter]
    if skip_systems:
        # Drop only the literal `system` catalog (UC metadata). `samples` is also
        # tagged SYSTEM_CATALOG but it's user-facing demo data we want to include.
        catalogs = [c for c in catalogs if c.get("name") != "system"]
    print(f"[extract] {len(catalogs)} catalogs to walk: {[c['name'] for c in catalogs]}", flush=True)

    for cat in catalogs:
        cat_name = cat["name"]
        pack["catalogs"].append(cat)
        pack["stats"]["catalogs"] += 1

        try:
            schemas = list_schemas(host, token, cat_name)
        except Exception as e:
            pack["errors"].append({"stage": "list_schemas", "catalog": cat_name, "error": _redact(str(e))})
            continue

        if schema_filter and cat_name in schema_filter:
            wanted = set(schema_filter[cat_name])
            schemas = [s for s in schemas if s.get("name") in wanted]
        if skip_systems:
            schemas = [s for s in schemas if s.get("name") != "information_schema"]

        print(f"[extract] catalog {cat_name}: {len(schemas)} schemas", flush=True)

        for sch in schemas:
            sch_name = sch["name"]
            pack["schemas"].append(sch)
            pack["stats"]["schemas"] += 1

            try:
                tables = list_tables_in_schema(host, token, cat_name, sch_name)
            except Exception as e:
                pack["errors"].append({"stage": "list_tables", "schema": f"{cat_name}.{sch_name}",
                                       "error": _redact(str(e))})
                continue
            pack["stats"]["tables_listed"] += len(tables)
            print(f"[extract]   {cat_name}.{sch_name}: {len(tables)} tables", flush=True)

            # The list-tables response already includes full columns + properties
            # for most tables — only Delta-shared tables come back with empty
            # `columns`. Per internal code review: skip the per-table detail
            # fetch when the list result is already complete, falling back to
            # get_table_detail() only when columns are missing. At large-scale catalogs
            # this collapses ~N sequential GETs into ~N/page list calls.
            for t in tables:
                full_name = t.get("full_name") or f"{cat_name}.{sch_name}.{t['name']}"
                has_columns = bool(t.get("columns"))
                try:
                    if has_columns:
                        # List response is already sufficient for the rewriter
                        pack["tables"].append(t)
                    else:
                        # Fall back to per-table fetch — may still return empty
                        # columns for share-based tables, which the rewriter
                        # handles via NO_COLUMNS_VISIBLE skip.
                        detail = get_table_detail(host, token, full_name)
                        pack["tables"].append(detail)
                    pack["stats"]["tables_detailed"] += 1
                except Exception as e:
                    pack["errors"].append({"stage": "get_table", "table": full_name, "error": _redact(str(e))})
                    pack["stats"]["tables_failed"] += 1

            # Volumes (UC only)
            try:
                vols = list_volumes_in_schema(host, token, cat_name, sch_name)
                pack["volumes"].extend(vols)
                pack["stats"]["volumes"] += len(vols)
            except Exception as e:
                pack["errors"].append({"stage": "list_volumes", "schema": f"{cat_name}.{sch_name}",
                                       "error": _redact(str(e))})

    pack["finished_at"] = datetime.utcnow().isoformat() + "Z"
    return pack


def _parse_schema_filter(s: str | None) -> dict[str, list[str]]:
    """Parse 'cat1:sch1,cat1:sch2,cat2:sch3' -> {cat1:[sch1,sch2], cat2:[sch3]}."""
    if not s:
        return {}
    out: dict[str, list[str]] = {}
    for item in s.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" not in item:
            raise ValueError(f"--schemas-only entries must be 'catalog:schema', got: {item!r}")
        c, sch = item.split(":", 1)
        out.setdefault(c, []).append(sch)
    return out


def main():
    # SEC-NEW-DATABRICKS-03: --token <value> is refused (exit 2) before the
    # value is stored anywhere, mistyped forms (--tok=..., -t ..., a bare
    # value) are refused without being echoed, and the PAT comes from
    # DATABRICKS_TOKEN or an owner-only --token-file.
    ap = ArgvSafeParser()
    ap.add_argument("--host", default=os.environ.get("DATABRICKS_HOST"))
    ap.add_argument("--token", action=RejectTokenArgv)
    ap.add_argument("--token-file", default=None, metavar="PATH",
                    help="File holding the Databricks PAT (owner-only, chmod 600); "
                         "else DATABRICKS_TOKEN in the env")
    ap.add_argument("--catalogs", default="", help="Comma-separated catalog names; default = all non-system")
    ap.add_argument("--schemas-only", default="",
                    help="Comma-separated catalog:schema filter (e.g., 'samples:tpch,samples:nyctaxi')")
    ap.add_argument("--skip-systems", action="store_true", default=True,
                    help="Skip 'system' catalog and 'information_schema' (default: on)")
    ap.add_argument("--out", required=True, help="Path to write catalog pack JSON")
    args = ap.parse_args()

    try:
        reject_url_credentials(args.host)
        token, token_source = resolve_token(args.token_file)
    except ValueError as exc:
        sys.exit(f"ERROR: {exc}")
    if not args.host or not token:
        sys.exit("ERROR: --host (or DATABRICKS_HOST) and a token are required. "
                 + TOKEN_ARGV_REFUSAL)
    _REDACT_SECRETS.append(token)
    print(f"[extract] token source: {token_source}", flush=True)

    cat_filter = [c.strip() for c in args.catalogs.split(",") if c.strip()] or None
    sch_filter = _parse_schema_filter(args.schemas_only)

    try:
        pack = extract(args.host, token, cat_filter, sch_filter, args.skip_systems)
    except Exception as exc:  # a library error must not echo the token
        sys.exit(f"ERROR: extract failed: {type(exc).__name__}: {_redact(str(exc))}")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(pack, indent=2), encoding="utf-8")

    s = pack["stats"]
    print(f"\n[extract] PACK WRITTEN: {args.out}")
    print(f"[extract] stats: {s['catalogs']} catalogs, {s['schemas']} schemas, "
          f"{s['tables_detailed']}/{s['tables_listed']} tables detailed, "
          f"{s['volumes']} volumes, {s['tables_failed']} failed")
    if pack["errors"]:
        print(f"[extract] {len(pack['errors'])} errors (first 3):")
        for e in pack["errors"][:3]:
            print(f"  {e}")


if __name__ == "__main__":
    main()
