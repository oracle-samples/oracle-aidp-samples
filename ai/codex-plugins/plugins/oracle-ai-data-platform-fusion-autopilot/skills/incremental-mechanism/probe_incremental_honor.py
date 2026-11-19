"""incremental-mechanism helper — does BICC honor the lineage delta for a PVO?

The empirical step the heuristic classification skips. Given a source PVO, this
loads it three ways through the live BICC connector and compares row counts to
decide whether `fusion.initial.extract-date` is pushed down server-side — i.e.
whether the node can use rung-1 native BICC incremental (`incrementalCapable: true`).

`fusion.initial.extract-date` is applied by BICC against the PVO's OWN lineage
attribute (no column is named in the option), so this works for any PVO without
knowing its LUD column up front; the column is auto-detected only for min/max
context. Metadata + COUNT only — it never writes to the lakehouse.

Method (ONE full extract, so it's affordable even on the ~10M-row balance cube):
  A. no watermark   -> full baseline N
  B. recent wm      -> ~0 rows IF honored, == N if ignored
  C. mid wm         -> 0 < m < N IF honored (discriminates by date), == N if ignored

Verdict:
  * B ~ N                 -> IGNORED  (native delta unavailable; consider rung 2/3/4)
  * B ~ 0 and C < N       -> HONORED  (rung 1 available; gate snapshot cubes on the
                                       correctness caveat in SKILL.md)
  * else                  -> AMBIGUOUS (inspect counts + LUD span; widen + re-run)

HONORED proves BICC *honors* the cursor; it does NOT prove the cursor *never
silently misses* a retroactive change. For master-data/transaction entities that
is sufficient; for snapshot/aggregate cubes it is necessary-but-not-sufficient.

Usage (identifiers from `dev/.envrc` or env; Fusion params from the bundle):
    python3 skills/incremental-mechanism/probe_incremental_honor.py \
        --datastore FscmTopModelAM.FinExtractAM.GlBiccExtractAM.CodeCombinationExtractPVO \
        --label gl_coa \
        --service-url "$AIDP_FUSION_SERVICE_URL" \
        --username "$FUSION_BICC_USER" \
        --external-storage "$AIDP_FUSION_EXTERNAL_STORAGE"
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# skills/incremental-mechanism/<this> -> parents[2] is the checkout root.
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "skills/aidp-rest"))

try:
    from client import AidpRestClient  # type: ignore[import-not-found]
except ImportError as exc:  # pragma: no cover — operator environment only
    raise SystemExit(
        f"probe requires the aidp-rest skill on sys.path ({ROOT}/skills/aidp-rest); {exc}"
    )

def _utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _default_recent_wm() -> str:
    """A cursor just beyond now should return zero rows when BICC honors it."""
    return _utc_iso(datetime.now(timezone.utc) + timedelta(days=1))


def _env_or(arg: str | None, env_key: str, *, required: bool = True) -> str:
    if arg is not None:
        return arg
    val = os.environ.get(env_key)
    if val:
        return val
    if not required:
        return ""
    raise SystemExit(f"missing required arg or env var {env_key!r}")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--datastore", required=True, help="Full AM-hierarchy PVO path.")
    p.add_argument("--label", required=True, help="Short node id for output filenames.")
    p.add_argument("--region", default=None)
    p.add_argument("--aidp-id", dest="aidp_id", default=None)
    p.add_argument("--workspace-key", dest="workspace_key", default=None)
    p.add_argument("--cluster-key", dest="cluster_key", default=None)
    p.add_argument("--cluster-name", dest="cluster_name", default=None)
    p.add_argument(
        "--workspace-dir", dest="workspace_dir",
        default="/Workspace/Shared/fusion-autopilot-incremental-probe",
    )
    p.add_argument("--secret-name", dest="secret_name", default="fusion_bicc_password")
    p.add_argument("--secret-key", dest="secret_key", default="password")
    p.add_argument("--service-url", dest="service_url", default=None)
    p.add_argument("--username", default=None)
    p.add_argument("--external-storage", dest="external_storage", default=None)
    p.add_argument("--schema", default="Financial")
    p.add_argument(
        "--recent-wm", dest="recent_wm", default=None,
        help="Recent watermark for the zero-row probe. Default: current UTC time + 1 day.",
    )
    p.add_argument(
        "--mid-wm", dest="mid_wm", default=None,
        help="Midpoint watermark for the partial-row probe. Default: midpoint of observed LUD min/max when available.",
    )
    p.add_argument("--timeout", type=int, default=3600)
    p.add_argument(
        "--out-dir", dest="out_dir", default=None,
        help="Where to write the marker JSON (default: cwd).",
    )
    return p.parse_args()


def build_notebook(
    *, datastore: str, secret_name: str, secret_key: str, service_url: str,
    username: str, external_storage: str, schema: str, recent_wm: str, mid_wm: str | None,
) -> dict:
    cell = (
        'import json, base64 as _b64, traceback\n'
        'from datetime import timezone\n'
        'from pyspark.sql import functions as F\n'
        'def _fmt_ts(v):\n'
        '    if v is None:\n'
        '        return None\n'
        '    if hasattr(v, "tzinfo"):\n'
        '        if v.tzinfo is None:\n'
        '            return v.isoformat() + "Z"\n'
        '        return v.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")\n'
        '    return str(v)\n'
        f'pw = aidputils.secrets.get(name={secret_name!r}, key={secret_key!r})\n'
        'def _reader(watermark=None):\n'
        '    r = (spark.read.format("aidataplatform")\n'
        '         .option("type", "FUSION_BICC")\n'
        f'         .option("fusion.service.url", {service_url!r})\n'
        f'         .option("user.name", {username!r})\n'
        '         .option("password", pw)\n'
        f'         .option("schema", {schema!r})\n'
        f'         .option("fusion.external.storage", {external_storage!r})\n'
        f'         .option("datastore", {datastore!r}))\n'
        '    if watermark:\n'
        '        r = r.option("fusion.initial.extract-date", watermark)\n'
        '    return r.load()\n'
        f'_recent_wm = {recent_wm!r}\n'
        f'_mid_wm = {mid_wm!r}\n'
        f'payload = {{"datastore": {datastore!r}, "recent_wm": _recent_wm, "mid_wm": _mid_wm}}\n'
        'try:\n'
        '    df_full = _reader(None)\n'
        '    fields = [f.name for f in df_full.schema.fields]\n'
        '    lud = next((c for c in fields if c.lower().endswith("lastupdatedate")), None)\n'
        '    payload["lud_col"] = lud\n'
        '    full_count = df_full.count()\n'
        '    payload["full_count"] = full_count\n'
        '    if lud:\n'
        '        mm = df_full.agg(F.min(lud).alias("mn"), F.max(lud).alias("mx")).collect()[0]\n'
        '        payload["min_lud"] = _fmt_ts(mm["mn"]); payload["max_lud"] = _fmt_ts(mm["mx"])\n'
        '        if _mid_wm is None and mm["mn"] and mm["mx"]:\n'
        '            _mid_wm = _fmt_ts(mm["mn"] + (mm["mx"] - mm["mn"]) / 2)\n'
        '            payload["mid_wm"] = _mid_wm\n'
        '    payload["recent_count"] = _reader(_recent_wm).count()\n'
        '    payload["mid_count"] = _reader(_mid_wm).count() if _mid_wm else None\n'
        '    rc, mc = payload["recent_count"], payload["mid_count"]\n'
        '    if full_count > 0 and rc >= 0.99 * full_count:\n'
        '        payload["verdict"] = "IGNORED"\n'
        '    elif full_count > 0 and mc is not None and rc <= max(1, 0.01 * full_count) and mc < 0.99 * full_count:\n'
        '        payload["verdict"] = "HONORED"\n'
        '    else:\n'
        '        payload["verdict"] = "AMBIGUOUS"\n'
        '    payload["status"] = "ok"\n'
        'except Exception as e:\n'
        '    payload["status"] = "error"\n'
        '    payload["error_type"] = type(e).__name__\n'
        '    payload["error_message"] = str(e)[:1500]\n'
        '    payload["traceback_tail"] = traceback.format_exc()[-1500:]\n'
        '_b = _b64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")\n'
        'print("AIDP_INCR_HONOR_BEGIN", _b, "AIDP_INCR_HONOR_END")\n'
    )
    return {
        "cells": [
            {"cell_type": "code", "execution_count": None, "metadata": {},
             "outputs": [], "source": cell.splitlines(keepends=True)},
        ],
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.10"},
        },
        "nbformat": 4, "nbformat_minor": 5,
    }


def _parse_marker(executed: dict) -> dict:
    for cell in executed.get("cells", []):
        for output in cell.get("outputs", []):
            for src in ("text", "data"):
                v = output.get(src)
                if v is None:
                    continue
                if src == "data":
                    v = v.get("text/plain", "")
                if isinstance(v, list):
                    v = "".join(v)
                if "AIDP_INCR_HONOR_BEGIN" in v:
                    a = v.index("AIDP_INCR_HONOR_BEGIN") + len("AIDP_INCR_HONOR_BEGIN")
                    b = v.index("AIDP_INCR_HONOR_END", a)
                    return json.loads(base64.b64decode(v[a:b].strip()).decode("utf-8"))
    return {}


def main() -> int:
    args = _parse_args()
    region = _env_or(args.region, "AIDP_REGION")
    aidp_id = _env_or(args.aidp_id, "AIDP_ID")
    workspace_key = _env_or(args.workspace_key, "AIDP_WORKSPACE_KEY")
    cluster_key = _env_or(args.cluster_key, "AIDP_CLUSTER_KEY")
    cluster_name = _env_or(args.cluster_name, "AIDP_CLUSTER_NAME")
    service_url = _env_or(args.service_url, "AIDP_FUSION_SERVICE_URL")
    username = _env_or(args.username, "FUSION_BICC_USER")
    external_storage = _env_or(args.external_storage, "AIDP_FUSION_EXTERNAL_STORAGE")
    recent_wm = args.recent_wm or _default_recent_wm()

    client = AidpRestClient(region=region, aidp_id=aidp_id, workspace_key=workspace_key)
    client.verify_cluster_active(cluster_key)
    nb = build_notebook(
        datastore=args.datastore, secret_name=args.secret_name,
        secret_key=args.secret_key, service_url=service_url, username=username,
        external_storage=external_storage, schema=args.schema,
        recent_wm=recent_wm, mid_wm=args.mid_wm,
    )
    nb_name = f"incr_honor_{args.label}_{int(time.time())}.ipynb"
    nb_path = f"{args.workspace_dir}/{nb_name}"
    print(f"==> [{args.label}] uploading {nb_name}")
    client.upload_notebook(nb_path, nb)
    job_key = client.create_notebook_job(
        name=nb_name.removesuffix(".ipynb").replace(".", "_"),
        description=f"incremental-mechanism BICC lineage-delta honor probe ({args.label})",
        notebook_path=nb_path, cluster_key=cluster_key,
        cluster_name=cluster_name, task_key="incr_honor",
    )
    run_key = client.submit_run(job_key)
    print(f"==> [{args.label}] job={job_key} run={run_key} — polling")
    result = client.poll_run(run_key, timeout_s=args.timeout, interval_s=15)
    print(f"==> [{args.label}] terminal status: {result.status}")
    task_run_key = client.resolve_task_run_key(result.raw, "incr_honor")
    nb_str = client.fetch_output(task_run_key)
    executed = json.loads(nb_str) if nb_str else {}
    marker = _parse_marker(executed)
    marker.setdefault("_terminal_status", result.status)
    marker.setdefault("_label", args.label)
    if not marker.get("status"):
        marker["_cell_errors"] = client.extract_cell_errors(executed)[:3]

    out_dir = Path(args.out_dir) if args.out_dir else Path.cwd()
    out_path = out_dir / f"incr_honor_{args.label}_{int(time.time())}.json"
    out_path.write_text(json.dumps(marker, indent=2))
    print(f"==> [{args.label}] marker written to {out_path}")
    print(json.dumps(marker, indent=2)[:2000])
    return 0 if marker.get("status") == "ok" else 1


if __name__ == "__main__":  # pragma: no cover — operator entry point
    raise SystemExit(main())
