"""Step-1 spike (feature bicc-period-window-extract) — does BICC expose a
datastore WHERE-filter via /biacm/rest, and does it honor it on extract?

Runs CLUSTER-SIDE (the authoritative Fusion BICC credential lives in the AIDP
secret store, not dev/.envrc — the laptop copy 401s against the pod). Staged:

  phase=get      READ-ONLY. Probe /biacm/rest endpoints with the secret-store
                 credential; confirm auth works; locate BalanceExtractPVO's
                 datastore-metadata + current filter. Mutates nothing.
  phase=put      GUARDED. GET+save original filter, PUT a period predicate,
                 re-GET to verify, then RESTORE the original in a finally.
  (extract verification — whether a set filter actually drops the row count —
   is a follow-up once phase=get reveals the real API shape.)

Operator-only, no asserts/fixtures. Mirrors dispatch_bicc_smoke.py plumbing.
"""
from __future__ import annotations
import argparse, base64, json, os, sys, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts")); sys.path.insert(0, str(ROOT / "skills/aidp-rest"))
from client import AidpRestClient  # noqa: E402


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--phase", choices=("get", "put"), default="get")
    p.add_argument("--service-url", required=True)
    p.add_argument("--username", required=True)
    p.add_argument("--secret-name", default="fusion_bicc_password")
    p.add_argument("--secret-key", default="password")
    p.add_argument("--datastore", default="BalanceExtractPVO",
                   help="datastore short name to locate in the metadata listing.")
    p.add_argument("--workspace-dir", default="/Workspace/Shared/fusion-autopilot-bicc-spike")
    return p.parse_args()


def build_cell(*, phase: str, service_url: str, username: str,
               secret_name: str, secret_key: str, datastore: str) -> str:
    # Read-only GET probe across candidate BICC REST shapes. The real API
    # version/path is unknown on this pod — discover it empirically.
    return (
        'import json, base64 as _b64, traceback, urllib.request, urllib.error\n'
        f'pw = aidputils.secrets.get(name={secret_name!r}, key={secret_key!r})\n'
        f'USER = {username!r}\n'
        f'POD = {service_url!r}\n'
        f'DS = {datastore!r}\n'
        'def _req(method, path, body=None):\n'
        '    url = POD + path\n'
        '    data = json.dumps(body).encode() if body is not None else None\n'
        '    req = urllib.request.Request(url, data=data, method=method)\n'
        '    tok = _b64.b64encode((USER + ":" + pw).encode()).decode()\n'
        '    req.add_header("Authorization", "Basic " + tok)\n'
        '    req.add_header("Accept", "application/json")\n'
        '    if body is not None: req.add_header("Content-Type", "application/json")\n'
        '    try:\n'
        '        r = urllib.request.urlopen(req, timeout=45)\n'
        '        return r.status, r.read().decode("utf-8", "replace")\n'
        '    except urllib.error.HTTPError as e:\n'
        '        return e.code, e.read().decode("utf-8", "replace")[:2000]\n'
        '    except Exception as e:\n'
        '        return -1, repr(e)[:500]\n'
        'payload = {"phase": ' + repr(phase) + ', "probe": {}}\n'
        'CANDIDATES = [\n'
        '    "/biacm/rest",\n'
        '    "/biacm/rest/",\n'
        '    "/biacm/rest/v1",\n'
        '    "/biacm/rest/latest",\n'
        '    "/biacm/rest/v1/dataStoreSets",\n'
        '    "/biacm/rest/v1/datastores",\n'
        '    "/biacm/rest/v1/extracts",\n'
        '    "/biacm/rest/v1/cloudExtractConfigurations",\n'
        '    "/biacm/rest/v1/offering",\n'
        '    "/biacm/rest/v1/jobs",\n'
        '    "/biacm/rest/v1/extractRunHistory",\n'
        '    "/biacm",\n'
        ']\n'
        'try:\n'
        '    for ep in CANDIDATES:\n'
        '        sc, body = _req("GET", ep)\n'
        '        payload["probe"][ep] = {"status": sc, "head": body[:1200]}\n'
        '    # note any endpoint that resolved (non-404) or mentions the target DS\n'
        '    payload["resolved"] = {ep: pr.get("status") for ep, pr in payload["probe"].items()\n'
        '                           if pr.get("status") not in (404, -1)}\n'
        '    payload["mentions_target"] = [ep for ep, pr in payload["probe"].items()\n'
        '                                  if DS in (pr.get("head") or "")]\n'
        'except Exception as e:\n'
        '    payload["error"] = traceback.format_exc()[-1500:]\n'
        '_b = _b64.b64encode(json.dumps(payload).encode()).decode()\n'
        'print("SPIKE_BEGIN", _b, "SPIKE_END")\n'
    )


def main() -> int:
    a = _args()
    c = AidpRestClient(region=os.environ["AIDP_REGION"], aidp_id=os.environ["AIDP_ID"],
                       workspace_key=os.environ["AIDP_WORKSPACE_KEY"])
    c.verify_cluster_active(os.environ["AIDP_CLUSTER_KEY"])
    cell = build_cell(phase=a.phase, service_url=a.service_url, username=a.username,
                      secret_name=a.secret_name, secret_key=a.secret_key, datastore=a.datastore)
    nb = {"cells": [{"cell_type": "code", "execution_count": None, "metadata": {},
                     "outputs": [], "source": cell.splitlines(keepends=True)}],
          "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                       "language_info": {"name": "python", "version": "3.10"}},
          "nbformat": 4, "nbformat_minor": 5}
    name = f"bicc_spike_{a.phase}_{int(time.time())}.ipynb"
    path = f"{a.workspace_dir}/{name}"
    c.upload_notebook(path, nb)
    jk = c.create_notebook_job(name=name.removesuffix(".ipynb").replace(".", "_"),
                               description=f"BICC filter spike ({a.phase})", notebook_path=path,
                               cluster_key=os.environ["AIDP_CLUSTER_KEY"],
                               cluster_name=os.environ["AIDP_CLUSTER_NAME"], task_key="bicc_spike")
    rk = c.submit_run(jk); print(f"==> run={rk} polling")
    r = c.poll_run(rk, timeout_s=600, interval_s=15); print("status:", r.status)
    executed = json.loads(c.fetch_output(c.resolve_task_run_key(r.raw, "bicc_spike")))
    full = ""
    for cell_ in executed.get("cells", []):
        for o in cell_.get("outputs", []):
            t = o.get("text") or (o.get("data") or {}).get("text/plain") or ""
            full += "".join(t) if isinstance(t, list) else t
    import re
    m = re.search(r"SPIKE_BEGIN\s+(\S+)\s+SPIKE_END", full)
    if m:
        print(json.dumps(json.loads(base64.b64decode(m.group(1))), indent=2)[:4000])
    else:
        print("NO MARKER; tail:", full[-1500:])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
