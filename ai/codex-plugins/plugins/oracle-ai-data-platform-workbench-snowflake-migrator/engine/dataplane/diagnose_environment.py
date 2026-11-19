"""# Snowflake → AIDP migration: environment diagnosis

Run this INSIDE AIDP (any cluster) before the migration jobs. It answers
four questions, in order, and each check prints a verdict rather than a
traceback:

1. **Where do workspace files land on this cluster?** (The migration jobs'
   default paths are under `/Workspace`.)
2. **Can this cluster reach Snowflake at all?** (TCP 443 to the account host.)
3. **Do the credentials work through the AIDP Snowflake connector?**
   (`current_user()` via pushdown — the path the migration scripts use.)
4. **Does the registered EXTERNAL catalog expose any schemas?**
   (The catalog is filled by its crawler, which is checked separately from
   the connector.)

`CONFIG_PATH` points at the config's `snowflake:` block, which
`provision --source-config` places on the workspace mount as JSON
(`plan/<stem>.json`). `provision` fills the path in when it uploads this
notebook. The config is echoed as its SHAPE only: key names with
`<set>` / `<unset>`, never a value — the file holds the credential, and cell
output is persisted with the workspace object.

Nothing here writes anything anywhere.
"""
import json
import os
import pathlib
import socket
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from snowmig_source import (  # noqa: E402
    SnowflakeSource, SourceConfigError, load_source_config)

# %% parameters
# ── PARAMETERS ─────────────────────────────────────────────────
# Edit these, then run the notebook top to bottom. `provision` writes this
# run's values in when it uploads the notebook.
CONFIG_PATH = "/Workspace/backup-snowflake-migration/plan/snowmig-config.json"
EXTERNAL_CATALOG = ""   # optional: the registered EXTERNAL catalog's name
SESSION_SCHEMA = ""     # a REAL schema for the pushdown check; blank = the config's `schema:`

# %% 1 · the workspace mount


def verdict(name, ok, detail=""):
    print(f"[{'PASS' if ok else 'FAIL'}] {name}"
          + (f" — {detail}" if detail else ""), flush=True)
    return ok


found = [p for p in ("/Workspace", "/workspace", "/mnt/workspace")
         if os.path.isdir(p)]
verdict("workspace mount", bool(found),
        f"visible at {found}" if found else
        "no workspace tree on this filesystem — a job cannot read uploaded "
        "files")
plan_dir = os.path.dirname(CONFIG_PATH)
if os.path.isdir(plan_dir):
    verdict("plan folder present", True,
            f"{plan_dir}: {sorted(os.listdir(plan_dir))[:8]}")
else:
    verdict("plan folder present", False,
            f"{plan_dir} not found — was `provision --source-config` run?")

# %% 2 · the config, read back as its SHAPE only (never a value)


def _shape(value):
    """Key names with `<set>`/`<unset>`; never a value, at any depth.

    An allow-list of fields to show would still print the ones it allows,
    and a deny-list of secret-looking names is what leaked a nested block
    whole. So nothing is shown but the shape.
    """
    if isinstance(value, dict):
        return {str(k): _shape(v) for k, v in value.items()}
    return "<set>" if value not in (None, "") else "<unset>"


config = {}
host = ""
try:
    # YAML or JSON, and it unwraps `snowflake:` -- the shape that reaches
    # the mount. Reading the top level for `account` found only the envelope.
    config = load_source_config(CONFIG_PATH)
    verdict("source config readable", True, json.dumps(_shape(config)))
except SourceConfigError as exc:
    verdict("source config readable", False, str(exc))
except Exception as exc:
    # A parser's own message quotes the offending line, and on this file
    # the line a stray `{` or `: ` breaks is the password line. Position
    # only.
    mark = getattr(exc, "problem_mark", None)
    line = (mark.line + 1) if mark is not None else getattr(exc, "lineno", None)
    verdict("source config readable", False,
            f"{type(exc).__name__}" + (f" near line {line}" if line else "")
            + "; the parser's message is withheld because it quotes the file")
if config:
    account = str(config.get("account") or "").strip()
    host = str(config.get("host")
               or (f"{account}.snowflakecomputing.com" if account else "")
               ).strip()
print("   host to be used:", host or "(unknown)")
print("   CONFIRM WITH THE USER: that host, and the user, role, warehouse and "
      "database in PREFLIGHT_CONFIG.md on the operator's machine — values "
      "are not echoed here.")

# %% 3 · network egress from THIS cluster
if host:
    try:
        socket.create_connection((host, 443), timeout=10).close()
        verdict("cluster → Snowflake :443", True, host)
    except Exception as exc:
        verdict("cluster → Snowflake :443", False,
                f"{type(exc).__name__}: {exc} — the cluster has no route; "
                f"the connector path cannot work either")
else:
    # A missing verdict reads as "not checked" at best and "fine" at worst.
    verdict("cluster → Snowflake :443", False,
            "host unknown: the config was not readable, see above")

# %% 4 · the AIDP Snowflake connector, with these credentials


def _spark():
    """The notebook's own session: getOrCreate returns the active one."""
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()


try:
    source = SnowflakeSource(_spark(), mode="connector", config=config,
                             session_schema=SESSION_SCHEMA or None)
    rows = source.pushdown(
        "select current_user() U, current_role() R, current_warehouse() W, "
        "count(*) N from INFORMATION_SCHEMA.TABLES").collect()
    verdict("connector pushdown", True, str([r.asDict() for r in rows]))
except Exception as exc:
    verdict("connector pushdown", False,
            f"{type(exc).__name__}: {str(exc)[:400]}")
    print("   DATA_ACCESS_LAYER_0031 here means SESSION_SCHEMA is not a real "
          "schema; the connector refuses INFORMATION_SCHEMA in that option.")

# %% 5 · the EXTERNAL catalog: has its crawler actually populated it?
if EXTERNAL_CATALOG:
    try:
        schemas = _spark().sql(f"SHOW SCHEMAS IN `{EXTERNAL_CATALOG}`").collect()
        verdict("external catalog populated", bool(schemas),
                f"{len(schemas)} schema(s)" if schemas else
                "registered but EMPTY — no crawl has populated it yet. Check "
                "its refresh status; the crawler reaches Snowflake outside "
                "the cluster's network path, so a crawl error (for example "
                "CONNECTOR_0067, 'Login has timed out') can occur while the "
                "connector check above passes")
    except Exception as exc:
        verdict("external catalog populated", False,
                f"{type(exc).__name__}: {str(exc)[:300]}")
else:
    print("[SKIP] external catalog — EXTERNAL_CATALOG not set. A skip is not "
          "a pass.")

# %% [markdown] Reading the result
# | Pattern | What it means | What to do |
# |---|---|---|
# | connector PASS, catalog EMPTY | the credentials work; no crawl has populated the catalog yet | run the migration with `--source-mode connector` (the default) |
# | egress FAIL | the cluster has no route to Snowflake | fix the cluster's network/NAT before anything else |
# | connector FAIL, egress PASS | credentials, role, warehouse or key | re-confirm the config fields with the user, then retry |
# | config FAIL | the file is not on the mount, or not readable | re-run `provision --source-config`; a YAML value with `:`, `{`, `[`, `*` or a leading quote must be single-quoted |
# | mount FAIL | jobs cannot read uploaded files | do not run the jobs; re-check the workspace upload |
