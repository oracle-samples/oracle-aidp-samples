"""Diagnostic for dbx_export's jar-fetch paths (s3/http proxy-fetch AND DBFS
chunked read) — built on the REAL DbxClient, not a reimplementation, so a
result here reflects exactly what dbx_export would do.

Two modes:

1) Direct URI (you already know the exact source):
    python3 scripts/test_proxy_jar_fetch.py \
        --dbx-proxy <host:port> \
        --uri "s3://<bucket>/<key>/name.jar"

2) Find-by-job (no URI needed — looks up the real recorded source by
   scanning the live/ephemeral cluster instance(s) used by the job's latest
   successful run, via DbxClient.list_job_runs/get_run/get_cluster_libraries):
    python3 scripts/test_proxy_jar_fetch.py \
        --dbx-proxy <host:port> \
        --job-id <dbx-job-id> --jar-name-match <substring-of-jar-name>

In find-by-job mode, a dbfs: source checks the local AIDP Unity-Catalog
Volumes mirror (/Volumes/default/default/dbfs/...) first when run from an
AIDP notebook/cluster — avoids the proxy round-trip entirely when the file
is already mirrored. --uri mode always goes through the s3/http proxy path
(DbxClient.fetch_external_via_proxy) since it's meant to test that path
specifically.

Add --export-output to also upload the validated jar into the AIDP workspace,
using the SAME path-building (sanitizer.dbx_to_aidp_path, KIND_JAR ->
<export-output>/jars/<basename>) and guardrail (assert_under_export_output)
that dbx_export's own phase2_upload_files uses for every jar it fetches — so
this proves the fetch-then-place-under-EXPORT_OUTPUT flow end to end, not
just that the bytes are downloadable. Without --export-output, the script
stays fully read-only (no AIDP writes).
"""
import argparse
import io
import os
import sys
import time
import zipfile

# Standalone bootstrap: make both the repo root (for dbx_export/) and
# scripts/ importable no matter what cwd this diagnostic is launched from.
# Computed from __file__ with os.path — never by string-matching '/scripts/',
# which breaks on Windows separators or if the file is ever relocated.
_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPTS_DIR)
for _p in (_SCRIPTS_DIR, _REPO_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from dbx_export.auth import DEFAULT_LAKE_OCID, DEFAULT_WORKSPACE_ID, DEFAULT_OCI_PROFILE, get_oci_signer
from dbx_export.aidp_client import AidpClient, AidpConfig, AidpApiError
from dbx_export.dbx_client import DbxClient, DbxConfig, DbxExportError
from dbx_export.sanitizer import dbx_to_aidp_path, KIND_JAR


def _lib_repr(lib: dict) -> str:
    for kind in ("jar", "whl", "egg"):
        if kind in lib:
            return f"{kind}:{lib[kind]}"
    if "maven" in lib:
        return f"maven:{lib['maven'].get('coordinates', '')}"
    if "pypi" in lib:
        return f"pypi:{lib['pypi'].get('package', '')}"
    return str(lib)


def find_library_entry(dbx: DbxClient, job_id: str, name_match: str) -> dict:
    """Scan the live cluster instance(s) used by the job's latest successful
    run for a library entry whose repr contains name_match. Never guesses a
    URI — returns the exact entry Databricks itself recorded."""
    runs = dbx.list_job_runs(job_id)
    if not runs:
        raise RuntimeError(f"no runs found for job {job_id}")
    runs.sort(key=lambda r: r.get("start_time", 0), reverse=True)
    run = next((r for r in runs if (r.get("state") or {}).get("result_state") == "SUCCESS"), runs[0])
    detail = dbx.get_run(run["run_id"])

    seen_clusters = {}
    for t in detail.get("tasks", []):
        cid = (t.get("cluster_instance") or {}).get("cluster_id")
        if cid and cid not in seen_clusters:
            seen_clusters[cid] = t.get("task_key", "")

    for cid, tk in seen_clusters.items():
        data = dbx.get_cluster_libraries(cid)
        if data is None:
            print(f"  [skip cluster {cid}] no library status available")
            continue
        statuses = data.get("library_statuses", [])
        print(f"  cluster {cid} (task {tk}): {len(statuses)} library entries")
        for st in statuses:
            lib = st.get("library", {})
            rep = _lib_repr(lib)
            if name_match in rep:
                print(f"FOUND on cluster {cid} (task {tk}): status={st.get('status')} {rep}")
                return lib
    raise RuntimeError(f"{name_match!r} not found on any cluster instance for job {job_id}'s latest run")


def fetch_dbfs_via_local_mirror_or_client(dbx: DbxClient, dbfs_uri: str) -> bytes:
    """Check the AIDP Volumes mirror first (fast, no network); fall back to
    DbxClient.read_dbfs_file (proxy chunked read) if not present."""
    dbfs_rel = dbfs_uri[len("dbfs:"):]
    local_mirror_path = f"/Volumes/default/default/dbfs{dbfs_rel}"

    for attempt in range(2):
        if os.path.exists(local_mirror_path):
            with open(local_mirror_path, "rb") as f:
                data = f.read()
            print(f"FOUND locally at {local_mirror_path}: {len(data)} bytes (no proxy call needed)")
            return data
        if attempt == 0:
            print(f"Not present at {local_mirror_path} yet — retrying after 5s (FUSE consistency delay)")
            time.sleep(5)

    print(f"Not present at local mirror — falling back to DbxClient.read_dbfs_file: {dbfs_rel}")
    return dbx.read_dbfs_file(dbfs_rel)


def validate_jar(content: bytes) -> bool:
    try:
        zf = zipfile.ZipFile(io.BytesIO(content))
        names = zf.namelist()
        print(f"VALID jar/zip archive — {len(names)} entries. Sample: {names[:5]}")
        return True
    except zipfile.BadZipFile:
        print("NOT a valid zip/jar — likely an error page or truncated body:")
        print(content[:300])
        return False


def upload_to_export_output(args, source_uri: str, content: bytes) -> bool:
    """Place the validated jar under <export-output>/jars/<basename>, using the
    SAME sanitizer.dbx_to_aidp_path(KIND_JAR) + AidpClient.upload_file that
    dbx_export's real phase2_upload_files uses — proves the fetch actually
    lands where the production pipeline would put it, guardrails included
    (upload_file asserts the path stays under export_output)."""
    aidp_path = dbx_to_aidp_path(source_uri, args.export_output, KIND_JAR)
    print(f"Uploading to EXPORT_OUTPUT path: {aidp_path}")
    _, signer = get_oci_signer(profile=args.oci_profile, config_path=args.oci_config_path)
    aidp_cfg = AidpConfig(
        lake_ocid=args.lake_ocid, workspace_id=args.workspace_id,
        export_output=args.export_output, prefix=args.prefix,
    )
    aidp = AidpClient(signer=signer, config=aidp_cfg)
    try:
        aidp.upload_file(aidp_path, content, overwrite=args.overwrite)
    except AidpApiError as e:
        print(f"UPLOAD FAILED: {e}")
        return False

    fetched = aidp.download_file(aidp_path)
    ok = fetched == content
    print(f"Read-back verified: {ok} ({len(fetched)} bytes at {aidp_path})")
    return ok


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dbx-proxy", required=True, help="host:port of the Databricks proxy VNIC")
    p.add_argument("--uri", default=None, help="s3:// or http(s):// source URI (direct mode)")
    p.add_argument("--job-id", default=None, help="DBX job_id to search (find-by-job mode)")
    p.add_argument("--jar-name-match", default=None, help="substring to find the right library entry")
    p.add_argument("--proxy-user", default=os.environ.get("DBX_PROXY_BASIC_USER"),
                    help="HTTP Basic Auth username for --dbx-proxy (env: DBX_PROXY_BASIC_USER)")
    p.add_argument("--proxy-pass", default=os.environ.get("DBX_PROXY_BASIC_PASS"),
                    help="HTTP Basic Auth password for --dbx-proxy (env: DBX_PROXY_BASIC_PASS)")
    p.add_argument("--export-output", default=None,
                    help="If set, upload the validated jar to <export-output>/jars/<basename> "
                         "via AidpClient (same path dbx_export itself would use). "
                         "Omit to stay fully read-only.")
    p.add_argument("--prefix", default="ai_tool")
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--oci-profile", default=DEFAULT_OCI_PROFILE)
    p.add_argument("--oci-config-path", default=None)
    p.add_argument("--lake-ocid", default=DEFAULT_LAKE_OCID)
    p.add_argument("--workspace-id", default=DEFAULT_WORKSPACE_ID)
    args = p.parse_args()

    if not args.uri and not (args.job_id and args.jar_name_match):
        print("ERROR: pass either --uri, or both --job-id and --jar-name-match", file=sys.stderr)
        return 2
    # Proxy basic auth is OPTIONAL — mirror dbx_export: authenticate only when
    # BOTH creds are supplied; a partial pair is a config mistake (warn, then
    # proceed WITHOUT auth); neither means the proxy needs no basic auth.
    if bool(args.proxy_user) != bool(args.proxy_pass):
        print("WARNING: only one of --proxy-user/--proxy-pass (or env "
              "DBX_PROXY_BASIC_USER/DBX_PROXY_BASIC_PASS) supplied; proxy basic "
              "auth needs both — proceeding WITHOUT proxy basic auth", file=sys.stderr)
        args.proxy_user = None
        args.proxy_pass = None

    dbx = DbxClient(DbxConfig(
        proxy=args.dbx_proxy,
        proxy_basic_user=args.proxy_user,
        proxy_basic_pass=args.proxy_pass,
        fetch_external_via_proxy=True,
    ))

    if args.uri:
        source_uri = args.uri
        print(f"Fetching {source_uri!r} through proxy {args.dbx_proxy!r} ...")
        try:
            content = dbx.fetch_external_via_proxy(source_uri)
        except DbxExportError as e:
            print(f"FAILED: reason={e.reason} status={e.status} detail={e.detail}")
            return 1
        print(f"OK: {len(content)} bytes fetched")
    else:
        lib = find_library_entry(dbx, args.job_id, args.jar_name_match)
        source_uri = next((lib[k] for k in ("jar", "whl", "egg") if k in lib), None)
        if source_uri is None:
            if "maven" in lib:
                print(f"'{lib['maven'].get('coordinates')}' is a Maven coordinate, not a downloadable "
                      f"file — AIDP resolves Maven libraries itself, nothing to fetch")
                return 1
            print(f"FAILED: unrecognized library entry shape: {lib}")
            return 1

        print(f"\nAttempting download of: {source_uri}")
        try:
            if source_uri.startswith("dbfs:"):
                content = fetch_dbfs_via_local_mirror_or_client(dbx, source_uri)
            else:
                content = dbx.fetch_external_via_proxy(source_uri)
        except DbxExportError as e:
            print(f"FAILED: reason={e.reason} status={e.status} detail={e.detail}")
            return 1
        print(f"Total: {len(content)} bytes")

    if not validate_jar(content):
        return 1

    if args.export_output:
        return 0 if upload_to_export_output(args, source_uri, content) else 1
    print("(no --export-output given — staying read-only, nothing uploaded)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
