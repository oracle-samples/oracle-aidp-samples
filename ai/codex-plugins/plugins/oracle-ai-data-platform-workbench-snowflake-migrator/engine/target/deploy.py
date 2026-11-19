"""Deploy generated DDL to AIDP. Dry-run unless explicitly told otherwise.

Two AIDP behaviours drive the shape of this module:
  * Per-statement DDL is silently discarded when the session closes, so DDL is
    batched into one execution per chunk.
  * A chunk can report success while individual statements inside it failed, so
    every statement's object is probed individually afterwards. The chunk's own
    return value is not trusted.

VERIFIED MEANS "THE RIGHT STRUCTURE IS THERE", not "a name matched". The DDL is
CREATE ... IF NOT EXISTS, so an object that already exists with different
columns is left untouched -- reporting that as a clone would be a false report
about someone else's table. So the probe checks three things in order:
existence by EXACT name, then the column list by DESCRIBE, and it reports
"structure unverified" rather than "verified" when it cannot compare.

A chunk error does not abort the run: the remaining chunks are attempted and the
error is recorded, because a partial deployment that is accurately reported is
more useful than an aborted one that is not.

SCOPED TO ONE CATALOG PER RUN. Bronze mirrors the source, so a multi-database
estate produces one AIDP Standard Catalog per Snowflake database. Rather than
fan out across all of them from a single confirmation, a run deploys only the
statements belonging to `target.catalog` and reports the rest as out of scope.
Deploying a second catalog is a second explicit invocation.
"""
from __future__ import annotations

import datetime
from typing import Callable

from .ddl import build_create_schema, quote_backtick

__all__ = ["deploy", "RefusedToExecute", "VERIFY_OUTCOMES"]

VERIFY_OUTCOMES = ("verified", "mismatch", "absent", "unverified_structure",
                   "probe_failed")

# SHOW output column that carries the object name. AIDP's exact shape is
# unverified, so several spellings are accepted -- and if none is present the
# probe says so instead of picking a value and hoping.
_NAME_KEYS = ("tableName", "viewName", "name", "table_name", "view_name",
              "TABLE_NAME", "VIEW_NAME", "NAME")

# DESCRIBE emits trailing metadata sections after the columns. Everything from
# the first blank or `#`-prefixed row onwards is not a column.
_DESCRIBE_NAME_KEYS = ("col_name", "COL_NAME", "name", "column_name")
_DESCRIBE_TYPE_KEYS = ("data_type", "DATA_TYPE", "type", "dataType")


def _first(row: dict, keys) -> str | None:
    for k in keys:
        if k in row and row[k] is not None:
            return str(row[k])
    return None


def _norm_type(value: str) -> str:
    """Compare types ignoring case and internal spacing only."""
    return "".join(str(value).split()).upper()


def _describe_columns(rows: list[dict]) -> list[tuple[str, str]] | None:
    """(name, type) pairs from DESCRIBE, or None if the shape is unrecognised."""
    cols: list[tuple[str, str]] = []
    for row in rows:
        name = _first(row, _DESCRIBE_NAME_KEYS)
        if name is None:
            return None
        name = name.strip()
        if not name or name.startswith("#"):
            break               # metadata section -- columns are done
        cols.append((name, _first(row, _DESCRIBE_TYPE_KEYS) or ""))
    return cols


def _compare_columns(expected: list[dict],
                     actual: list[tuple[str, str]]) -> str | None:
    """None if the structures match, else a one-line description of the diff."""
    want = [(str(c.get("name", "")).upper(), _norm_type(c.get("type", "")))
            for c in expected]
    got = [(n.upper(), _norm_type(ty)) for n, ty in actual]
    if want == got:
        return None
    if len(want) != len(got):
        return (f"column count differs: planned {len(want)}, found {len(got)} "
                f"(planned {[n for n, _ in want]}, found {[n for n, _ in got]})")
    diffs = [f"position {i + 1}: planned {w[0]} {w[1]}, found {g[0]} {g[1]}"
             for i, (w, g) in enumerate(zip(want, got)) if w != g]
    return "; ".join(diffs)


def _verify_object(run_sql, stmt: dict) -> tuple[str, str]:
    """(outcome, detail) for one deployed object. Outcome is in VERIFY_OUTCOMES."""
    catalog, schema, name = _split_fqn(stmt["target_fqn"])
    kind = (stmt.get("object_type") or "TABLE").upper()
    show = "SHOW VIEWS" if kind == "VIEW" else "SHOW TABLES"
    qualified = f"{quote_backtick(catalog)}.{quote_backtick(schema)}"

    try:
        rows = run_sql(f"{show} IN {qualified} "
                       f"LIKE '{_like_literal(name)}'")
    except Exception as exc:
        return "probe_failed", f"existence probe failed: {str(exc)[:200]}"

    names = [_first(r, _NAME_KEYS) for r in rows]
    if rows and all(n is None for n in names):
        return "probe_failed", (
            f"{show} returned rows with no recognisable name column "
            f"(keys: {sorted(rows[0])}); refusing to assume which value is the "
            f"object name")
    if not any(n is not None and n.upper() == name.upper() for n in names):
        return "absent", (
            "not present after its chunk reported completion"
            if not rows else
            f"no object named {name} present; {show} returned {names}")

    expected = stmt.get("expected_columns")
    if not expected:
        return "unverified_structure", (
            "exists, but the plan carried no column list to compare it against")

    fqn = ".".join(quote_backtick(p) for p in (catalog, schema, name))
    try:
        described = run_sql(f"DESCRIBE {'VIEW' if kind == 'VIEW' else 'TABLE'} {fqn}")
    except Exception as exc:
        return "unverified_structure", (
            f"exists, but its structure could not be read: {str(exc)[:200]}")

    actual = _describe_columns(described)
    if actual is None:
        return "unverified_structure", (
            "exists, but DESCRIBE output had no recognisable column-name field")

    diff = _compare_columns(expected, actual)
    if diff is None:
        return "verified", "exists with the planned columns"
    return "mismatch", (
        f"exists but its structure differs from the plan -- {diff}. The DDL is "
        f"CREATE IF NOT EXISTS, so this object was left as it was found and has "
        f"NOT been cloned.")


def _like_literal(name: str) -> str:
    """Escape LIKE wildcards for a Spark LIKE pattern body.

    `_` matches any single character and is in most real table names, so an
    unescaped probe matches names other than the one asked for. The quote is
    backslash-escaped, not doubled: doubling is two literals in Spark.
    """
    return (name.replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_")
                .replace("'", "\\'"))


class RefusedToExecute(RuntimeError):
    """Execution was requested without the arguments that make it safe."""


def _split_fqn(target_fqn: str) -> tuple[str, str, str]:
    catalog, schema, table = target_fqn.split(".", 2)
    return catalog, schema, table


def deploy(ddl_plan: dict, *, target=None, execute: bool = False,
           run_sql: Callable[..., list[dict]] | None = None,
           chunk_size: int = 25) -> dict:
    all_statements = [s for s in ddl_plan.get("statements", []) if s.get("sql")]
    blocked_count = len(ddl_plan.get("blocked", []))

    # Scope to the confirmed catalog. Out-of-scope objects are reported, not run.
    if target is not None:
        scope = target.catalog.upper()
        in_scope_at = {i for i, s in enumerate(all_statements)
                       if _split_fqn(s["target_fqn"])[0].upper() == scope}
        statements = [s for i, s in enumerate(all_statements) if i in in_scope_at]
        # By index: `s not in statements` compared dicts pairwise, which is
        # O(n^2) and gets slow exactly when an estate is large.
        out_of_scope = [s for i, s in enumerate(all_statements)
                        if i not in in_scope_at]
    else:
        statements, out_of_scope = all_statements, []

    out = {
        "ran_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "dry_run": not execute,
        "statements": statements,
        "statement_count": len(statements),
        "blocked_count": blocked_count,
        "catalog_in_scope": target.catalog if target is not None else None,
        "out_of_scope_count": len(out_of_scope),
        "out_of_scope_catalogs": sorted({
            _split_fqn(s["target_fqn"])[0] for s in out_of_scope}),
        "executed": 0, "verified": 0, "failed": [], "chunk_errors": [],
        # Per-source-identifier lists so plan/status.py can report a status per
        # object rather than only an aggregate count.
        "attempted_targets": [], "verified_targets": [], "failed_targets": [],
        # Exists but does not match the plan. Kept apart from both verified and
        # failed: the statement ran, and the object in AIDP is someone else's.
        "mismatched_targets": [], "mismatches": [],
        # Exists, but we could not compare its structure. Not a clone claim.
        "unverified_structure_targets": [], "unverified_structure": [],
    }
    if not execute:
        return out

    if target is None:
        raise RefusedToExecute(
            "execute=True requires a resolved target; ask the user for the AIDP "
            "datalake OCID, workspace, cluster and catalog and pass them explicitly")
    if run_sql is None:
        raise RefusedToExecute("execute=True requires a run_sql callable")

    schemas = {_split_fqn(s["target_fqn"])[:2] for s in statements}
    for catalog, schema in sorted(schemas):
        try:
            run_sql(build_create_schema(catalog, schema))
        except Exception as exc:
            out["chunk_errors"].append(f"CREATE SCHEMA {catalog}.{schema}: {exc}")

    for start in range(0, len(statements), chunk_size):
        chunk = statements[start:start + chunk_size]
        batch = ";\n".join(s["sql"] for s in chunk)
        out["attempted_targets"] += [s.get("source_identifier") for s in chunk]
        try:
            run_sql(batch)
            out["executed"] += len(chunk)
        except Exception as exc:
            out["chunk_errors"].append(
                f"chunk {start // chunk_size}: {str(exc)[:300]}")

        # Never trust the chunk's own result. Probe each object.
        for stmt in chunk:
            ident = stmt.get("source_identifier")
            outcome, detail = _verify_object(run_sql, stmt)
            if outcome == "verified":
                out["verified"] += 1
                out["verified_targets"].append(ident)
            elif outcome == "mismatch":
                out["mismatched_targets"].append(ident)
                out["mismatches"].append({
                    "source_identifier": ident,
                    "target_fqn": stmt["target_fqn"], "reason": detail})
            elif outcome == "unverified_structure":
                out["unverified_structure_targets"].append(ident)
                out["unverified_structure"].append({
                    "source_identifier": ident,
                    "target_fqn": stmt["target_fqn"], "reason": detail})
            else:
                out["failed_targets"].append(ident)
                out["failed"].append({
                    "source_identifier": ident,
                    "target_fqn": stmt["target_fqn"], "reason": detail})
    return out
