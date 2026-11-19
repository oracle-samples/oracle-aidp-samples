"""Operator-runnable: list a LIVE AIDP catalog schema's tables + columns.

Runs a tiny ``SHOW TABLES`` / ``DESCRIBE`` notebook on an ACTIVE cluster via
the bundle's own ``AidpRestClient`` (no wheel, no orchestrator) and prints the
live-catalog JSON that ``oac-dataset-advisor/catalog_inventory.py`` consumes.

This is the EVIDENCE step for oac-dataset-advisor — the live AIDP gold layer,
not pack YAMLs. NOT collected by CI (filename isn't ``test_*.py``); operator
supplies real coords via flags or env (AIDP_DATALAKE_OCID, AIDP_WORKSPACE_KEY,
AIDP_CLUSTER_ID, AIDP_CLUSTER, AIDP_REGION, AIDP_OCI_PROFILE, AIDP_CATALOG).

Usage:
  .venv/bin/python tests/live/aidp_catalog_probe_live.py --schema gold
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

_MARKER = "AIDP_CATALOG_RESULT:"


def _notebook(catalog: str, schema: str) -> dict:
    code = f'''
import json
cat, sch = {catalog!r}, {schema!r}
out = {{"catalog": cat, "schema": sch, "tables": {{}}}}
try:
    rows = spark.sql(f"SHOW TABLES IN {{cat}}.{{sch}}").collect()
    names = [r["tableName"] for r in rows]
    for t in names:
        cols = spark.sql(f"DESCRIBE TABLE {{cat}}.{{sch}}.`{{t}}`").collect()
        cl = []
        for r in cols:
            cn = (r["col_name"] or "").strip()
            if not cn or cn.startswith("#"):
                break
            cl.append({{"name": cn, "type": r["data_type"]}})
        out["tables"][t] = cl
except Exception as e:
    out["error"] = f"{{type(e).__name__}}: {{e}}"
print("{_MARKER} " + json.dumps(out))
'''
    return {
        "nbformat": 4, "nbformat_minor": 5,
        "metadata": {"kernelspec": {"name": "python3", "language": "python", "display_name": "Python 3"}},
        "cells": [{"cell_type": "code", "metadata": {}, "execution_count": None,
                   "outputs": [], "source": code.strip().splitlines(keepends=True)}],
    }


def _extract_marker(nb_json: str) -> dict:
    nb = json.loads(nb_json)
    for cell in nb.get("cells", []):
        for o in cell.get("outputs", []):
            text = o.get("text") or (o.get("data", {}) or {}).get("text/plain") or ""
            if isinstance(text, list):
                text = "".join(text)
            for line in text.splitlines():
                if _MARKER in line:
                    return json.loads(line.split(_MARKER, 1)[1].strip())
    raise SystemExit("marker not found in executed notebook output")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--aidp-id", default=os.environ.get("AIDP_DATALAKE_OCID"))
    ap.add_argument("--workspace-key", default=os.environ.get("AIDP_WORKSPACE_KEY"))
    ap.add_argument("--cluster-key", default=os.environ.get("AIDP_CLUSTER_ID"))
    ap.add_argument("--cluster-name", default=os.environ.get("AIDP_CLUSTER", "fusion_autopilot_dev"))
    ap.add_argument("--region", default=os.environ.get("AIDP_REGION", "us-ashburn-1"))
    ap.add_argument("--oci-profile", default=os.environ.get("AIDP_OCI_PROFILE", "DEFAULT"))
    ap.add_argument("--catalog", default=os.environ.get("AIDP_CATALOG", "fusion_catalog"))
    ap.add_argument("--schema", default="gold")
    ap.add_argument("--workspace-root", default="Shared")
    ap.add_argument("--poll-timeout", type=int, default=900)
    ap.add_argument("--out", default=None, help="Write live-catalog JSON here (else stdout).")
    ns = ap.parse_args(argv)
    for req in ("aidp_id", "workspace_key", "cluster_key"):
        if not getattr(ns, req):
            raise SystemExit(f"missing --{req.replace('_','-')} (or its AIDP_* env var)")

    client = AidpRestClient(region=ns.region, aidp_id=ns.aidp_id,
                            workspace_key=ns.workspace_key, oci_profile=ns.oci_profile,
                            log=lambda stage, **kw: print(f"[{stage}] {kw}", file=sys.stderr))
    client.verify_cluster_active(ns.cluster_key)

    # Unique suffix (pid) so repeat probes don't 409 on a pre-existing job/notebook.
    uid = f"{os.getpid()}"
    path = f"/Workspace/{ns.workspace_root}/aidp_fusion_catalog_probe_{ns.schema}_{uid}.ipynb"
    client.upload_notebook(path, _notebook(ns.catalog, ns.schema))
    job = client.create_notebook_job(
        name=f"aidp_fusion_catalog_probe_{uid}", description="oac-dataset-advisor live evidence",
        notebook_path=path, cluster_key=ns.cluster_key, cluster_name=ns.cluster_name,
        task_key="probe",
    )
    run_key = client.submit_run(job)
    result = client.poll_run(run_key, timeout_s=ns.poll_timeout)
    if result.status != "SUCCESS":
        raise SystemExit(f"probe run ended {result.status}")
    trk = client.resolve_task_run_key(result.raw, "probe")
    listing = _extract_marker(client.fetch_output(trk))

    text = json.dumps(listing, indent=2)
    if ns.out:
        Path(ns.out).write_text(text, encoding="utf-8")
        print(f"wrote {ns.out} ({listing.get('catalog')}.{listing.get('schema')}: "
              f"{len(listing.get('tables', {}))} tables)", file=sys.stderr)
    else:
        print(text)
    return 0


if __name__ == "__main__":  # pragma: no cover — operator entry point
    sys.exit(main())
