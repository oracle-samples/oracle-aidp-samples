"""Print the nightshift_ops ledger DDL; with --apply, run it through AidpIO.run_sql (one kernel session).

  python scripts/deploy_ops_schema.py                 # print only
  python scripts/deploy_ops_schema.py --apply         # create schema + tables on the configured cluster
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
import json


# --- env plumbing (shared shape across scripts; no secrets, auth via the OCI profile) ---------------
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from nightshift.config import Env  # noqa: E402

_ENV_FLAGS = {
    "region": "NS_REGION", "data_lake_id": "NS_DATA_LAKE_ID", "workspace_id": "NS_WORKSPACE_ID",
    "cluster_id": "NS_CLUSTER_ID", "config_profile": "NS_OCI_PROFILE", "ops": "NS_OPS",
    "probe_notebook": "NS_NOTEBOOK",
}


def add_env_args(parser: argparse.ArgumentParser) -> None:
    g = parser.add_argument_group("environment overrides (default: NS_* env vars, then config.py)")
    for field, var in _ENV_FLAGS.items():
        g.add_argument(f"--{field.replace('_', '-')}", dest=f"env_{field}", default=None, help=f"overrides {var}")
    parser.add_argument("--env", action="append", default=[], metavar="KEY=VALUE",
                        help="generic override, e.g. --env cluster_id=abc (repeatable)")
    parser.add_argument("-v", "--verbose", action="store_true")


def env_from_args(args: argparse.Namespace) -> Env:
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    values = {f: os.environ.get(var, getattr(Env(), f)) for f, var in _ENV_FLAGS.items()}
    for field in _ENV_FLAGS:
        if getattr(args, f"env_{field}", None):
            values[field] = getattr(args, f"env_{field}")
    for kv in args.env:
        k, _, v = kv.partition("=")
        if k not in _ENV_FLAGS:
            raise SystemExit(f"unknown --env key {k!r}; valid: {', '.join(_ENV_FLAGS)}")
        values[k] = v
    return Env(**values)
# ---------------------------------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="execute the DDL (default: print only)")
    add_env_args(p)
    args = p.parse_args(argv)
    env = env_from_args(args)

    try:
        from nightshift.ledger import ddl_statements
    except ImportError:
        from nightshift.actuator import _FallbackLedger
        print("# nightshift.ledger not present; using fallback (schema only)", file=sys.stderr)
        ddl_statements = _FallbackLedger().ddl_statements
    statements = ddl_statements(env.ops)
    for s in statements:
        print(s.rstrip().rstrip(";") + ";\n")
    if not args.apply:
        print(f"# {len(statements)} statements for {env.ops} (dry; pass --apply to run)", file=sys.stderr)
        return 0

    from nightshift.aidp_io import AidpIO
    io = AidpIO(env, dry_run=False)
    results = []
    try:
        for s in statements:
            rows = io.run_sql(s, timeout=300)
            results.append({"sql": s.split("\n", 1)[0][:80], "rows": len(rows)})
            print(json.dumps(results[-1]), file=sys.stderr)
    finally:
        io.close()
    print(json.dumps({"applied": len(results), "ops": env.ops}, indent=2), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
