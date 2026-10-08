"""Generate the shallow-clone notebook. Pure: builds ipynb JSON, runs nothing.

The notebook is the deliverable the user executes, so it is written to be read
before it is run:

  * the first cell states plainly that it creates empty structure and moves no
    data, and names the source and destination;
  * schemas are created before objects, and views after the tables they read;
  * every statement is embedded as data and executed in a loop that prints
    `[i/N] name ... ok (0.4s)`, so a long run is monitorable from the output
    rather than silent;
  * a verification cell probes each object individually afterwards, because a
    batch can report success while statements inside it failed;
  * blocked objects appear in markdown only -- they are listed so the gap is
    visible, and never in code, so the notebook cannot attempt them.

It contains no INSERT, COPY INTO, MERGE, UPDATE, DELETE, TRUNCATE or DROP. A
test asserts that.
"""
from __future__ import annotations

import datetime

__all__ = ["NOTEBOOK_NAME", "build_notebook", "notebook_workspace_path"]

NOTEBOOK_NAME = "snowmig_shallow_clone"


def notebook_workspace_path(catalog: str) -> str:
    """Where the notebook lands in the AIDP workspace filesystem.

    Note: notebooks live in the WORKSPACE, not in a data catalog -- catalogs hold
    tables and views. The catalog name is in the filename so one notebook per
    catalog is unambiguous.
    """
    return f"/Workspace/Shared/{NOTEBOOK_NAME}_{catalog}.ipynb"


def _md(*lines: str) -> dict:
    return {"cell_type": "markdown", "metadata": {},
            "source": [l + "\n" for l in lines]}


def _code(*lines: str) -> dict:
    return {"cell_type": "code", "metadata": {}, "execution_count": None,
            "outputs": [], "source": [l + "\n" for l in lines]}


# Verify-cell body, kept as source rather than an escaped list of string
# literals so that what runs on the cluster is readable here.
#
# Existence is NOT the claim. The DDL is CREATE ... IF NOT EXISTS, so an
# object that already existed with different columns was left exactly as it
# was found -- calling that a clone would be a false report about someone
# else's table. So the cell matches the name EXACTLY, then compares the
# column list, and reports "structure differs" and "unverified" as
# outcomes distinct from "verified".
_VERIFY_BODY = r"""
# `_` and `%` are LIKE wildcards, and `_` is in most real table names, so an
# unescaped probe matches names other than the one asked for.
def _like(name):
    out = name.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    # Backslash, not doubling: '' is two adjacent literals in Spark.
    return out.replace("'", "\\'")


def _name_of(row):
    d = row.asDict()
    for key in ("tableName", "viewName", "name", "table_name"):
        if d.get(key) is not None:
            return str(d[key]).upper()
    return None


def _columns_of(fqn, kind):
    what = "VIEW" if kind == "VIEW" else "TABLE"
    cols = []
    for r in spark.sql(f"DESCRIBE {what} {fqn}").collect():
        name = (r["col_name"] or "").strip()
        if not name or name.startswith("#"):
            break            # metadata section -- the columns are done
        cols.append((name.upper(), "".join(str(r["data_type"]).split()).upper()))
    return cols


_verified, _missing, _mismatched, _unchecked = [], [], [], []

for _kind, _cat, _sch, _name, _cols in _expected:
    _label = f"{_cat}.{_sch}.{_name}"
    _fqn = f"`{_cat}`.`{_sch}`.`{_name}`"
    _show = "SHOW VIEWS" if _kind == "VIEW" else "SHOW TABLES"
    try:
        _rows = spark.sql(
            f"{_show} IN `{_cat}`.`{_sch}` LIKE '{_like(_name)}'").collect()
    except Exception as _exc:
        _missing.append(f"{_label} (probe failed: {_exc})")
        continue

    # An EXACT name match, not merely a non-empty result.
    if _name.upper() not in [_name_of(_r) for _r in _rows]:
        _missing.append(_label)
        continue

    if not _cols:
        _unchecked.append(f"{_label} (no planned column list to compare)")
        continue
    try:
        _actual = _columns_of(_fqn, _kind)
    except Exception as _exc:
        _unchecked.append(f"{_label} (structure unreadable: {_exc})")
        continue

    _want = [(str(_n).upper(), "".join(str(_t).split()).upper())
             for _n, _t in _cols]
    if _want == _actual:
        _verified.append(_label)
    else:
        _mismatched.append(f"{_label}: planned {_want}, found {_actual}")

print(f"verified {len(_verified)}/{len(_expected)} "
      f"(planned structure, not just a name that exists)")
for _m in _missing:
    print(f"  MISSING: {_m}")
for _m in _mismatched:
    print(f"  STRUCTURE DIFFERS -- left as found, NOT cloned: {_m}")
for _m in _unchecked:
    print(f"  UNVERIFIED STRUCTURE: {_m}")
print()
print("Verified objects are EMPTY. No data was copied.")
"""


def build_notebook(ddl_plan: dict, plan: dict, *, catalog: str,
                   source: dict) -> dict:
    statements = [s for s in ddl_plan.get("statements", [])
                  if s.get("sql")
                  and s["target_fqn"].split(".", 1)[0].upper() == catalog.upper()]
    if not statements:
        raise ValueError(
            f"no statements target catalog {catalog!r}; refusing to write a "
            "notebook that would do nothing")

    schemas = sorted({tuple(s["target_fqn"].split(".")[:2]) for s in statements})
    tables = [s for s in statements if s.get("object_type") != "VIEW"]
    views = [s for s in statements if s.get("object_type") == "VIEW"]
    blocked = ddl_plan.get("blocked") or []
    generated = datetime.datetime.now(datetime.timezone.utc).isoformat()

    cells = [_md(
        f"# Shallow clone → `{catalog}`",
        "",
        "**This notebook creates empty structure. It moves no data.** Every table "
        "it creates has its columns and **zero rows**; copying rows is a separate, "
        "later phase.",
        "",
        f"- Source: Snowflake account `{source.get('account', 'n/a')}` "
        f"(region `{source.get('region', 'n/a')}`)",
        f"- Destination: AIDP catalog `{catalog}`",
        f"- Mapping: {plan.get('bronze_mapping', 'database → Standard Catalog')}",
        f"- Generated: `{generated}` by the snowflake-migrator plugin",
        "",
        f"Creates {len(schemas)} schema(s), {len(tables)} table(s) and "
        f"{len(views)} view(s). Contains no `INSERT`, `COPY INTO`, `MERGE`, "
        "`UPDATE`, `DELETE`, `TRUNCATE` or `DROP`.",
        "",
        "Run the cells in order. Progress prints per object, so a long run stays "
        "visible.")]

    cells.append(_code(
        "# Progress helper. Each statement is timed and reported individually so a",
        "# long run is monitorable from the output instead of appearing to hang.",
        "import time",
        "",
        "_results = []",
        "",
        "",
        "def _apply(label, sql, index, total):",
        "    started = time.time()",
        "    try:",
        "        spark.sql(sql)",
        "        elapsed = time.time() - started",
        "        print(f'[{index}/{total}] {label} ... ok ({elapsed:.1f}s)')",
        "        _results.append((label, 'ok', elapsed, ''))",
        "    except Exception as exc:",
        "        elapsed = time.time() - started",
        "        print(f'[{index}/{total}] {label} ... FAILED ({elapsed:.1f}s): {exc}')",
        "        _results.append((label, 'failed', elapsed, str(exc)[:300]))",
        "",
        "",
        "print('helper ready')"))

    cells.append(_md("## 1. Schemas",
                     "",
                     "Created before any object. `IF NOT EXISTS`, so re-running is "
                     "safe. No `COMMENT` is emitted on a schema create; table and "
                     "column comments are carried in the statements below."))
    schema_lines = ["_schema_sql = ["]
    schema_lines += [f"    ('{c}.{sc}', 'CREATE SCHEMA IF NOT EXISTS `{c}`.`{sc}`'),"
                     for c, sc in schemas]
    schema_lines += [
        "]",
        "",
        "for _i, (_label, _sql) in enumerate(_schema_sql, 1):",
        "    _apply(_label, _sql, _i, len(_schema_sql))"]
    cells.append(_code(*schema_lines))

    if tables:
        cells.append(_md("## 2. Tables",
                         "",
                         "Managed Delta, explicit `USING DELTA`, `IF NOT EXISTS`. "
                         "**Empty — no rows are written.**"))
        lines = ["_table_sql = ["]
        for s in tables:
            lines.append(f"    ({s['target_fqn']!r}, {s['sql']!r}),")
        lines += ["]", "",
                  "for _i, (_label, _sql) in enumerate(_table_sql, 1):",
                  "    _apply(_label, _sql, _i, len(_table_sql))"]
        cells.append(_code(*lines))

    if views:
        cells.append(_md("## 3. Views",
                         "",
                         "Created after the tables they read. The SQL is carried "
                         "over from Snowflake **without dialect translation** — "
                         "verify each result against the source before relying on "
                         "it."))
        lines = ["_view_sql = ["]
        for s in views:
            lines.append(f"    ({s['target_fqn']!r}, {s['sql']!r}),")
        lines += ["]", "",
                  "for _i, (_label, _sql) in enumerate(_view_sql, 1):",
                  "    _apply(_label, _sql, _i, len(_view_sql))"]
        cells.append(_code(*lines))

    cells.append(_md("## 4. Verify",
                     "",
                     "Each object is probed individually and its columns are "
                     "compared with the plan, so `verified` counts only objects "
                     "read back with the planned structure."))
    verify = ["_expected = ["]
    for s in statements:
        kind = "VIEW" if s.get("object_type") == "VIEW" else "TABLE"
        cat, sch, name = s["target_fqn"].split(".", 2)
        cols = [(c.get("name"), c.get("type"))
                for c in (s.get("expected_columns") or [])]
        verify.append(f"    ({kind!r}, {cat!r}, {sch!r}, {name!r}, {cols!r}),")
    verify.append("]")
    verify += _VERIFY_BODY.strip("\n").split("\n")
    cells.append(_code(*verify))

    if blocked:
        cells.append(_md(
            "## Not attempted",
            "",
            "These objects could not be migrated. They are listed so the gap is "
            "visible, and deliberately do **not** appear in any code cell above.",
            "",
            *[f"- `{b['source_identifier']}` ({b.get('object_type', '?')}) — "
              f"{b.get('reason', 'no reason recorded')}" for b in blocked]))

    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python",
                           "name": "python3"},
            "language_info": {"name": "python"},
            "snowmig": {"generated_at": generated, "catalog": catalog,
                        "moves_data": False,
                        "statement_count": len(statements),
                        "blocked_count": len(blocked)},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
