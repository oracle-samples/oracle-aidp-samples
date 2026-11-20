"""Run the flight-recorder cell on the configured cluster via a kernel session.

  python scripts/run_recorder.py --job-name orders_silver --run-id manual-001
  python scripts/run_recorder.py --job-name x --run-id y --print-only      # show the cell, no live call
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
    p.add_argument("--job-name", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--max-stages", type=int, default=200)
    p.add_argument("--timeout", type=int, default=300)
    p.add_argument("--print-only", action="store_true", help="print the generated cell and exit")
    add_env_args(p)
    args = p.parse_args(argv)
    env = env_from_args(args)

    from nightshift.recorder import recorder_cell, summary_from_output
    code = recorder_cell(env.ops, args.job_name, args.run_id, max_stages=args.max_stages)
    if args.print_only:
        print(code)
        return 0

    from nightshift.aidp_io import AidpIO
    io = AidpIO(env, dry_run=False)
    try:
        result = io.run_code(code, timeout=args.timeout)
    finally:
        io.close()
    summary = summary_from_output(result)
    if result.get("error"):
        print(json.dumps(result["error"], indent=2), file=sys.stderr)
    print(json.dumps(summary or {"ok": False, "raw_status": result.get("status")}, indent=2))
    return 0 if summary and summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
