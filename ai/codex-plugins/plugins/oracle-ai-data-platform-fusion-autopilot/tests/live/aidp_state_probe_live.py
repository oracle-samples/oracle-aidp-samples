"""Operator-runnable: acquire the two signals aidp-fusion-status reconciles.

Dispatches one notebook on an ACTIVE cluster that returns, as a marker JSON:
  * ``live``  — {table -> {exists, row_count}} for the gold schema (SHOW TABLES
    + COUNT(*));
  * ``state`` — latest-per-dataset rows from
    ``<catalog>.<bronzeSchema>.fusion_autopilot_state``.

Feed the result to ``skills/aidp-fusion-status/status_report.py``. Reuses the
bundle's own AidpRestClient (no wheel). NOT collected by CI (not test_*.py);
operator supplies coords via flags or AIDP_* env. The laptop
``aidp-fusion-autopilot status`` is local-Spark-only / no-JSON, so this cluster
probe is the truthful path until ``status --json`` ships.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "skills" / "aidp-rest"))
from client import AidpRestClient  # noqa: E402

_MARKER = "AIDP_STATUS_RESULT:"


def _notebook(catalog: str, schemas: list[str], state_table: str) -> dict:
    code = f'''
import json
cat, schemas, state_t = {catalog!r}, {schemas!r}, {state_table!r}
out = {{"live": {{}}, "state": []}}
for sch in schemas:
    try:
        for r in spark.sql(f"SHOW TABLES IN {{cat}}.{{sch}}").collect():
            t = r["tableName"]
            key = f"{{sch}}.{{t}}"
            try:
                n = spark.sql(f"SELECT COUNT(*) c FROM {{cat}}.{{sch}}.`{{t}}`").collect()[0]["c"]
                out["live"][key] = {{"exists": True, "row_count": int(n)}}
            except Exception as e:
                out["live"][key] = {{"exists": True, "row_count": None, "error": str(e)[:120]}}
    except Exception as e:
        out.setdefault("live_errors", {{}})[sch] = f"{{type(e).__name__}}: {{e}}"
try:
    q = f"""
      WITH ranked AS (
        SELECT dataset_id, layer, mode, last_run_at, status, row_count, skip_reason,
               ROW_NUMBER() OVER (PARTITION BY dataset_id ORDER BY last_run_at DESC) rn
        FROM {{cat}}.{{state_t}}
      )
      SELECT dataset_id, layer, mode, CAST(last_run_at AS STRING) last_run_at,
             status, row_count, skip_reason
      FROM ranked WHERE rn = 1 ORDER BY layer, dataset_id
    """
    for r in spark.sql(q).collect():
        out["state"].append({{k: r[k] for k in
            ["dataset_id","layer","mode","last_run_at","status","row_count","skip_reason"]}})
except Exception as e:
    out["state_error"] = f"{{type(e).__name__}}: {{e}}"
print("{_MARKER} " + json.dumps(out))
'''
    return {
        "nbformat": 4, "nbformat_minor": 5,
        "metadata": {"kernelspec": {"name": "python3", "language": "python", "display_name": "Python 3"}},
        "cells": [{"cell_type": "code", "metadata": {}, "execution_count": None,
                   "outputs": [], "source": code.strip().splitlines(keepends=True)}],
    }


def _marker(nb_json: str) -> dict:
    nb = json.loads(nb_json)
    for cell in nb.get("cells", []):
        for o in cell.get("outputs", []):
            text = o.get("text") or (o.get("data", {}) or {}).get("text/plain") or ""
            if isinstance(text, list):
                text = "".join(text)
            for line in text.splitlines():
                if _MARKER in line:
                    return json.loads(line.split(_MARKER, 1)[1].strip())
    raise SystemExit("marker not found in executed notebook")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--aidp-id", default=os.environ.get("AIDP_DATALAKE_OCID"))
    ap.add_argument("--workspace-key", default=os.environ.get("AIDP_WORKSPACE_KEY"))
    ap.add_argument("--cluster-key", default=os.environ.get("AIDP_CLUSTER_ID"))
    ap.add_argument("--cluster-name", default=os.environ.get("AIDP_CLUSTER", "fusion_autopilot_dev"))
    ap.add_argument("--region", default=os.environ.get("AIDP_REGION", "us-ashburn-1"))
    ap.add_argument("--oci-profile", default=os.environ.get("AIDP_OCI_PROFILE", "DEFAULT"))
    ap.add_argument("--catalog", default=os.environ.get("AIDP_CATALOG", "fusion_catalog"))
    ap.add_argument("--schemas", default="bronze,silver,gold",
                    help="Comma-separated schemas to probe for live tables (all layers state references).")
    ap.add_argument("--state-table", default="bronze.fusion_autopilot_state")
    ap.add_argument("--workspace-root", default="Shared")
    ap.add_argument("--poll-timeout", type=int, default=900)
    ap.add_argument("--out", default=None)
    ns = ap.parse_args(argv)
    for req in ("aidp_id", "workspace_key", "cluster_key"):
        if not getattr(ns, req):
            raise SystemExit(f"missing --{req.replace('_','-')} (or its AIDP_* env var)")

    client = AidpRestClient(region=ns.region, aidp_id=ns.aidp_id, workspace_key=ns.workspace_key,
                            oci_profile=ns.oci_profile,
                            log=lambda stage, **kw: print(f"[{stage}] {kw}", file=sys.stderr))
    client.verify_cluster_active(ns.cluster_key)
    uid = str(os.getpid())
    schemas = [s.strip() for s in ns.schemas.split(",") if s.strip()]
    path = f"/Workspace/{ns.workspace_root}/aidp_fusion_status_probe_{uid}.ipynb"
    client.upload_notebook(path, _notebook(ns.catalog, schemas, ns.state_table))
    job = client.create_notebook_job(
        name=f"aidp_fusion_status_probe_{uid}", description="aidp-fusion-status evidence",
        notebook_path=path, cluster_key=ns.cluster_key, cluster_name=ns.cluster_name, task_key="probe",
    )
    result = client.poll_run(client.submit_run(job), timeout_s=ns.poll_timeout)
    if result.status != "SUCCESS":
        raise SystemExit(f"status probe ended {result.status}")
    out = _marker(client.fetch_output(client.resolve_task_run_key(result.raw, "probe")))
    text = json.dumps(out, indent=2)
    if ns.out:
        Path(ns.out).write_text(text, encoding="utf-8")
        print(f"wrote {ns.out} (live={len(out.get('live',{}))} tables, "
              f"state={len(out.get('state',[]))} rows)", file=sys.stderr)
    else:
        print(text)
    return 0


if __name__ == "__main__":  # pragma: no cover — operator entry point
    sys.exit(main())
