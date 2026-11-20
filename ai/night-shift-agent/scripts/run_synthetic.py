"""Run notebooks/synthetic_failing_job.py on the configured cluster with a failure mode.

Usage: python scripts/run_synthetic.py --mode none|schema_drift|bad_value|upstream_missing|oom_like [--timeout 600]
Prints the cell stdout and any error, as JSON. Never embeds secrets.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from nightshift.aidp_io import AidpIO  # noqa: E402
from nightshift.config import Env  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="none")
    ap.add_argument("--timeout", type=int, default=600)
    args = ap.parse_args()
    src = (ROOT / "notebooks" / "synthetic_failing_job.py").read_text(encoding="utf-8")
    code = src.replace("{{FAILURE_MODE}}", args.mode)
    io = AidpIO(Env(), dry_run=True)
    try:
        res = io.run_code(code, timeout=args.timeout)
        out = []
        err = None
        for o in res.get("outputs", []):
            if o.get("output_type") == "stream":
                out.append(o.get("text", ""))
            elif o.get("output_type") == "error":
                err = {"ename": o.get("ename"), "evalue": str(o.get("evalue"))[:1500]}
        print(json.dumps({"mode": args.mode, "status": res.get("status"), "stdout": "".join(out)[-4000:], "error": err or res.get("error")}, indent=1))
    finally:
        io.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
