#!/usr/bin/env python3
"""
Import (export) Databricks notebook(s) by path — INCLUDING notebook.run(<variable>)
children that dbx_export's static resolver skips (it only follows quoted-literal
paths) — and optionally upload them into AIDP under --export-output.

Reuses dbx_export's own tested components: DbxClient (proxy/host export incl.
GitHub proxy for GIT-source tasks), AidpClient (target-base upload with
guardrails), dbx_to_aidp_path (path mapping), and extract_run_targets
(%run + literal notebook.run).

Read-only on Databricks.

━━━ Workspace notebooks (no GIT involvement) ━━━
  %run $CODE_BASE/scripts/import_dbx_notebook.py \
    --dbx-proxy $DBX_PROXY --export-output $EXPORT_OUTPUT --recursive \
    /Users/foo@bar.com/some_notebook

━━━ GIT-source notebooks (job has git_source populated) ━━━
Preferred: point at the DBX job — owner/repo/branch auto-derived from job spec,
and bare-name positional args resolve as siblings of the job's first GIT task's
directory in the repo (covers dynamic %run/notebook.run children the static
extractor can't reach):
  %run $CODE_BASE/scripts/import_dbx_notebook.py \
    --dbx-proxy $DBX_PROXY --export-output $EXPORT_OUTPUT \
    --dbx-job-id 1000000000000014 \
    Txn_Budget_Monitoring_examplevendor \
    Txn_Budget_Monitoring_example_M14

Fallback (no job to point at): supply owner/repo/branch + full repo-relative paths:
  %run $CODE_BASE/scripts/import_dbx_notebook.py \
    --dbx-proxy $DBX_PROXY --export-output $EXPORT_OUTPUT \
    --git-owner example-data-science --git-repo my-repo --git-branch main \
    /GIT/Hourly_Monitoring/master

━━━ Local (direct host+token) saves .ipynb under --out ━━━
  export DATABRICKS_HOST=... DATABRICKS_TOKEN=...
  python3 scripts/import_dbx_notebook.py <path> --recursive --out ./dbx_missing
"""
from __future__ import annotations
import argparse, json, os, posixpath, re, sys

# AIDP defaults sourced from aidp_migrator.yaml (via aidp_config) with env-var fallback.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _aidp_env_defaults import load_aidp_defaults
_defs = load_aidp_defaults()
DEFAULT_LAKE_OCID    = _defs["lake_ocid"]
DEFAULT_WORKSPACE_ID = _defs["workspace_id"]
DEFAULT_OCI_PROFILE  = _defs["oci_profile"]


def _load_dbx_export(code_base):
    """Put CODE_BASE on sys.path and import dbx_export's reusable pieces."""
    cb = code_base or os.environ.get("CODE_BASE") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if cb not in sys.path:
        sys.path.insert(0, cb)
    from dbx_export.dbx_client import DbxClient, DbxConfig, DbxExportError
    from dbx_export.aidp_client import AidpClient, AidpConfig
    from dbx_export.sanitizer import dbx_to_aidp_path, KIND_NOTEBOOK
    from dbx_export.dependency_resolver import extract_run_targets, _resolve_run_target
    from dbx_export.auth import get_oci_signer
    return dict(DbxClient=DbxClient, DbxConfig=DbxConfig, DbxExportError=DbxExportError,
                AidpClient=AidpClient, AidpConfig=AidpConfig, dbx_to_aidp_path=dbx_to_aidp_path,
                KIND_NOTEBOOK=KIND_NOTEBOOK, extract_run_targets=extract_run_targets,
                _resolve_run_target=_resolve_run_target, get_oci_signer=get_oci_signer)


def resolve_variable_runs(nb_dict, canonical_path, resolve_target):
    """Resolve notebook.run(<variable>, ...) targets dbx_export can't.
    Builds var->literal-path map from `<var> = "....ipynb"` assignments across the
    whole notebook (covers if/else branches), then maps bare-identifier
    notebook.run(<var>) calls to those literals. Returns (canonical_paths, warnings)."""
    parent_dir = posixpath.dirname(canonical_path)
    full = "\n".join(
        ("".join(c.get("source", [])) if isinstance(c.get("source"), list) else (c.get("source") or ""))
        for c in nb_dict.get("cells", []) if c.get("cell_type") == "code"
    )
    varmap: dict[str, set] = {}
    for m in re.finditer(r'(\w+)\s*=\s*[\'"]([^\'"]+)[\'"]', full):
        v = m.group(2)
        if v.endswith(".ipynb") or v.startswith("/Workspace") or v.startswith("/Users") or v.startswith("/Repos"):
            varmap.setdefault(m.group(1), set()).add(v)
    deps, warn = set(), []
    for m in re.finditer(r'notebook\.run\s*\(\s*([A-Za-z_]\w*)\s*[,\)]', full):
        var = m.group(1)
        if var in varmap:
            for lit in varmap[var]:
                t = resolve_target(lit, parent_dir)
                if t:
                    deps.add(t)
        else:
            warn.append(f"{canonical_path}: notebook.run({var}) — no literal assignment found; unresolved")
    return deps, warn


def _aidp_target_exists(aidp, dest, is_notebook):
    """True if a notebook/file already exists at dest. On ANY uncertainty (probe
    error) returns True — fail-safe: we never overwrite when we can't confirm absence."""
    import urllib.parse
    import requests as _rq
    try:
        if is_notebook:
            return aidp.get_notebook(dest) is not None
        r = _rq.get(f"{aidp.config.ws}/objects?path={urllib.parse.quote(dest, safe='')}",
                    auth=aidp.signer, timeout=60)
        return r.status_code != 404          # 200/other -> treat as exists (safe)
    except Exception:
        return True                          # can't verify -> assume exists -> do NOT write


def main():
    ap = argparse.ArgumentParser(description="Export DBX notebook(s) + %%run/notebook.run deps; upload to AIDP target-base or save local.")
    ap.add_argument("paths", nargs="+", help="notebook path(s) (AIDP /Workspace/... or DBX /Users/...)")
    ap.add_argument("--dbx-proxy", default=os.environ.get("DBX_PROXY"), help="DBX proxy host:port (env DBX_PROXY) - from inside AIDP")
    ap.add_argument("--dbx-host", default=os.environ.get("DATABRICKS_HOST"), help="DBX host (env DATABRICKS_HOST) - direct")
    ap.add_argument("--dbx-token", default=os.environ.get("DATABRICKS_TOKEN"), help="DBX PAT (env DATABRICKS_TOKEN)")
    ap.add_argument("--export-output", default=os.environ.get("EXPORT_OUTPUT"), help="AIDP dest base; if set, uploads into AIDP")
    ap.add_argument("--prefix", default="ai_tool")
    ap.add_argument("--lake-ocid", default=DEFAULT_LAKE_OCID)
    ap.add_argument("--workspace-id", default=DEFAULT_WORKSPACE_ID)
    ap.add_argument("--oci-profile", default=DEFAULT_OCI_PROFILE)
    ap.add_argument("--oci-config-path", default=os.environ.get("OCI_CONFIG"), help="Laptop-side OCI config path (defaults to $OCI_CONFIG env var or standard OCI SDK resolution). Not consumed on cluster — cluster fetches from AIDP Credential Store via aidputils.secrets.")
    ap.add_argument("--code-base", default=os.environ.get("CODE_BASE"), help="repo root containing dbx_export/ (default: parent of this script)")
    ap.add_argument("--out", default=None, help="local dir if NOT uploading to --export-output (default ./dbx_missing)")
    ap.add_argument("--recursive", action="store_true", help="also export %%run / notebook.run deps (incl. variable paths)")
    ap.add_argument("--overwrite", action="store_true", help="overwrite existing target notebooks/files (DEFAULT: skip existing, never overwrite)")
    # ─── BEGIN: GIT-source support (added 2026-07-20 for the pilot deployment GIT-backed jobs) ─
    # When paths live on GitHub instead of DBX workspace (job source == "GIT"),
    # workspace/export returns 404. Route to DbxClient.export_notebook_from_git
    # via pilot-jobs GitHub proxy (http://<proxy>/github, SAME pilotjobs auth).
    #
    # Two ways to opt in:
    #   (preferred) --dbx-job-id <ID>
    #       Fetch the job spec via the DBX proxy, read its git_source
    #       block to derive owner/repo/branch automatically, and enumerate
    #       every GIT-source task's repo-relative notebook_path. Any bare-name
    #       positional args are resolved as SIBLINGS of the job's first GIT
    #       task's directory in the repo (this covers dynamic %run/notebook.run
    #       children the static extractor can't reach).
    #   (fallback) --git-owner + --git-repo + --git-branch
    #       Manual override for expert use / jobs whose git_source isn't
    #       populated. Positional args are treated as repo-relative paths
    #       (leading "/", leading "GIT/" segment, and .ipynb extension all
    #       stripped).
    ap.add_argument("--dbx-job-id", default=os.environ.get("DBX_JOB_ID"),
                    help="DBX job ID (env DBX_JOB_ID). When set, fetches the job "
                         "spec to auto-derive git_source (owner/repo/branch) and "
                         "the tasks list. Positional args become sibling names of "
                         "the first GIT task's directory in the repo.")
    ap.add_argument("--git-owner", default=os.environ.get("DBX_GIT_OWNER"),
                    help="GitHub owner (env DBX_GIT_OWNER) - manual override for "
                         "--dbx-job-id auto-discovery.")
    ap.add_argument("--git-repo", default=os.environ.get("DBX_GIT_REPO"),
                    help="GitHub repo name (env DBX_GIT_REPO) - manual override.")
    ap.add_argument("--git-branch", default=os.environ.get("DBX_GIT_BRANCH"),
                    help="GitHub branch (env DBX_GIT_BRANCH), e.g. main / master - manual override.")
    ap.add_argument("--sibling-dir", default=None,
                    help="Repo directory used as base when resolving bare-name "
                         "positional args as siblings. Defaults to the first GIT "
                         "task's directory when --dbx-job-id is set.")
    # ─── END: GIT-source support ────────────────────────────────────────────
    a = ap.parse_args()

    if not (a.dbx_proxy or (a.dbx_host and a.dbx_token)):
        print("ERROR: need --dbx-proxy (env DBX_PROXY)  OR  --dbx-host + --dbx-token")
        return 1
    if not a.export_output and not a.out:
        a.out = "./dbx_missing"

    M = _load_dbx_export(a.code_base)
    # NOTE: this legacy CLI reads args only (no customer YAML), so it uses the
    # DbxConfig default for `proxy_max_retries` (the GitHub-via-proxy retry knob).
    # Intentional — not wiring a CLI flag here to avoid arg sprawl; add one only
    # if this path becomes a primary entry point. (Known follow-up; see PR #134.)
    dbx = M["DbxClient"](M["DbxConfig"](host=a.dbx_host, token=a.dbx_token, proxy=a.dbx_proxy))

    # ─── BEGIN: --dbx-job-id auto-discovery of git_source (added 2026-07-20) ─
    # Extract owner/repo/branch + task paths from the DBX job spec so the user
    # doesn't need to know any of this — they just pass --dbx-job-id + the
    # sibling notebook names they want.
    _git_task_dirs = []   # ordered list of repo-relative dirs (from GIT tasks)
    if a.dbx_job_id:
        if not a.dbx_proxy:
            print("ERROR: --dbx-job-id requires --dbx-proxy (host mode fetches the "
                  "job spec but the GitHub proxy is proxy-only)")
            return 1
        try:
            _job = dbx.get_job(a.dbx_job_id)
        except Exception as _e:
            print(f"ERROR: failed to fetch job spec for {a.dbx_job_id}: {_e}")
            return 1
        _settings = _job.get("settings") or _job   # jobs/get shape may vary
        _git_source = _settings.get("git_source") or {}
        _git_url = _git_source.get("git_url") or _git_source.get("gitUrl") or ""
        _git_branch = (_git_source.get("git_branch") or _git_source.get("gitBranch")
                       or _git_source.get("git_tag") or _git_source.get("git_commit") or "")
        # Parse owner/repo from https://github.com/OWNER/REPO(.git)
        _m = re.match(r"https?://github\.com/([^/]+)/([^/.]+)(?:\.git)?", _git_url.rstrip("/"))
        if not _m:
            print(f"ERROR: job {a.dbx_job_id} has no parseable git_source.git_url "
                  f"(got: {_git_url!r})")
            return 1
        _owner, _repo = _m.group(1), _m.group(2)
        # Only override manual flags when the user didn't set them explicitly.
        if not a.git_owner: a.git_owner = _owner
        if not a.git_repo:  a.git_repo  = _repo
        if not a.git_branch: a.git_branch = _git_branch
        # Enumerate GIT tasks + build sibling-dir default from the first one.
        for _t in _settings.get("tasks") or []:
            _nb = _t.get("notebook_task") or {}
            if _nb.get("source") != "GIT":
                continue
            _repo_path = (_nb.get("notebook_path") or "").lstrip("/")
            if not _repo_path:
                continue
            _dir = posixpath.dirname(_repo_path)
            if _dir and _dir not in _git_task_dirs:
                _git_task_dirs.append(_dir)
        if _git_task_dirs and not a.sibling_dir:
            a.sibling_dir = _git_task_dirs[0]
        print(f"[dbx-job-id] {a.dbx_job_id}: owner={a.git_owner} repo={a.git_repo} "
              f"branch={a.git_branch}  git_task_dirs={_git_task_dirs}  "
              f"sibling_dir={a.sibling_dir!r}")
    # ─── END: --dbx-job-id auto-discovery ───────────────────────────────────

    # Validate GIT-source flags: all-or-nothing (after auto-discovery filled them in).
    _git_all = all([a.git_owner, a.git_repo, a.git_branch])
    _git_any = any([a.git_owner, a.git_repo, a.git_branch])
    if _git_any and not _git_all:
        print("ERROR: --git-owner / --git-repo / --git-branch must be provided together "
              "(or use --dbx-job-id to auto-derive from the job spec)")
        return 1
    if _git_all and not a.dbx_proxy:
        # GitHub proxy is only available in proxy mode (http://<proxy>/github).
        print("ERROR: git flags require --dbx-proxy (host mode has no github proxy)")
        return 1
    aidp = None
    if a.export_output:
        _oci_cfg, signer = M["get_oci_signer"](a.oci_profile, a.oci_config_path)   # returns (config, signer)
        aidp = M["AidpClient"](signer=signer, config=M["AidpConfig"](
            lake_ocid=a.lake_ocid, workspace_id=a.workspace_id, export_output=a.export_output, prefix=a.prefix))
    canon = M["_resolve_run_target"]

    # ─── BEGIN: GIT path auto-detection (added 2026-07-20) ──────────────────
    # A DBX canonical path starting with `/GIT/` (or the /Workspace/GIT/ form
    # before canonicalization) means the notebook lives on GitHub, not in the
    # DBX workspace. We fetch via export_notebook_from_git and keep the AIDP
    # dest under `notebooks/GIT/…` via dbx_to_aidp_path(is_git=True).
    #
    # The `GIT/` segment itself is our AIDP-side convention prefix, NOT part
    # of the repo-relative path. Strip it before calling the git proxy so the
    # request reaches the right file on GitHub.
    def _is_git_path(cp: str) -> bool:
        return cp.startswith("/GIT/") or cp == "/GIT"

    def _repo_relative(cp: str) -> str:
        # /GIT/Hourly_Monitoring/master  →  Hourly_Monitoring/master
        return cp[len("/GIT/"):] if cp.startswith("/GIT/") else cp.lstrip("/")
    # ─── END: GIT path auto-detection ───────────────────────────────────────

    # ─── BEGIN: build initial queue (added 2026-07-20 --dbx-job-id support) ──
    # Two flavors of items land in the queue:
    #   1) GIT task paths auto-discovered from --dbx-job-id (each task's
    #      notebook_task.notebook_path — already repo-relative).
    #   2) Positional args from the CLI:
    #      - Absolute workspace paths (/Users/, /Repos/, /Shared/, or
    #        /Workspace/... other than /GIT/) → workspace/export.
    #      - /GIT/... or /Workspace/GIT/... → repo-relative under the sibling_dir
    #        or the repo root.
    #      - Bare names (no slash, no leading /) → resolved as siblings of
    #        --sibling-dir (auto-set to the job's first GIT task's dir).
    #
    # Canonicalize by prepending /GIT/<sibling_dir>/ to bare names so the
    # existing _is_git_path/_repo_relative code path handles them uniformly.
    _initial = []
    # Job-derived GIT tasks first.
    if _git_task_dirs is not None:
        # Add every GIT task path (already repo-relative) as /GIT/<path>.
        for _t in (_job.get("settings") or _job).get("tasks", []) if a.dbx_job_id else []:
            _nb = _t.get("notebook_task") or {}
            if _nb.get("source") != "GIT":
                continue
            _rp = (_nb.get("notebook_path") or "").lstrip("/")
            if _rp:
                _initial.append("/GIT/" + _rp)
    # Positional args.
    for _p in a.paths:
        _p = _p.strip()
        if not _p:
            continue
        if "/" not in _p and not _p.startswith("/"):
            # Bare notebook name → sibling of sibling_dir
            if not a.sibling_dir:
                print(f"WARN: bare name {_p!r} passed but no --sibling-dir/--dbx-job-id "
                      f"context; skipping (specify a full path or --sibling-dir)")
                continue
            _initial.append("/GIT/" + a.sibling_dir.strip("/") + "/" + _p)
        else:
            _initial.append(_p)
    # Canonicalize via _resolve_run_target (strips /Workspace/, .ipynb, normalizes ../).
    queue = [canon(p, "/") for p in _initial]
    # ─── END: build initial queue ───────────────────────────────────────────
    seen, saved, skipped, missing, warns = set(), [], [], [], []
    while queue:
        cp = queue.pop(0)
        if not cp or cp in seen:
            continue
        seen.add(cp)
        is_git = _is_git_path(cp)
        if is_git and not _git_all:
            missing.append(f"{cp}  (GIT path but --git-owner/--git-repo/--git-branch not set)")
            print(f"  [MISSING] {cp}  (need --git-owner/--git-repo/--git-branch)")
            continue
        try:
            if is_git:
                ef = dbx.export_notebook_from_git(
                    _repo_relative(cp), a.git_owner, a.git_repo, a.git_branch)
            else:
                ef = dbx.export_notebook(cp)
        except M["DbxExportError"] as e:
            missing.append(f"{cp}  ({e})"); print(f"  [MISSING] {cp}"); continue
        except Exception as e:
            missing.append(f"{cp}  ({type(e).__name__}: {e})"); print(f"  [ERR] {cp}: {repr(e)[:100]}"); continue
        content = ef.content
        # Recurse FIRST (uses exported content) so deps are discovered even when the
        # notebook itself is skipped-because-exists below.
        if a.recursive and ef.is_notebook:
            try:
                nb = json.loads(content)
            except Exception:
                nb = {}
            for t in M["extract_run_targets"](nb, cp):     # %run + literal notebook.run (dbx_export)
                queue.append(t)
            dv, wv = resolve_variable_runs(nb, cp, canon)   # notebook.run(<variable>) - our addition
            queue.extend(dv); warns.extend(wv)
        # Write — but NEVER overwrite an existing target unless --overwrite.
        try:
            if aidp:
                # is_git=True keeps AIDP dest under notebooks/GIT/... (matches
                # what full dbx_export produces for GIT-source jobs, so the DAG
                # lookup + %run rewrites downstream find the file).
                # Strip the /GIT/ marker from cp before passing — dbx_to_aidp_path
                # will re-add its own "GIT/" segment when is_git=True. Without
                # this strip we'd double the prefix → notebooks/GIT/GIT/... .
                _cp_for_dest = cp[len("/GIT/"):] if is_git else cp
                dest = M["dbx_to_aidp_path"](_cp_for_dest, a.export_output, M["KIND_NOTEBOOK"], is_git=is_git)
                if not a.overwrite and _aidp_target_exists(aidp, dest, ef.is_notebook):
                    skipped.append(dest); print(f"  [SKIP exists] {dest}"); continue
                if ef.is_notebook:
                    aidp.put_notebook(dest, json.loads(content))
                else:
                    aidp.upload_file(dest, content)
                saved.append(dest); print(f"  [AIDP] {dest}")
            else:
                local = os.path.join(a.out, cp.lstrip("/") + (".ipynb" if ef.is_notebook else ef.extension))
                if not a.overwrite and os.path.exists(local):
                    skipped.append(local); print(f"  [SKIP exists] {local}"); continue
                os.makedirs(os.path.dirname(local) or ".", exist_ok=True)
                with open(local, "wb") as f:
                    f.write(content)
                saved.append(local); print(f"  [local] {local}")
        except Exception as e:
            missing.append(f"{cp}  (write failed: {type(e).__name__}: {e})"); print(f"  [WRITE-ERR] {cp}: {repr(e)[:120]}")

    mode = "OVERWRITE" if a.overwrite else "no-overwrite (existing targets skipped)"
    print(f"\n=== done [{mode}]: {len(saved)} written, {len(skipped)} skipped(exists), {len(missing)} missing, {len(warns)} unresolved ===")
    for s in skipped:
        print("  SKIPPED (already exists, NOT overwritten):", s)
    for w in warns:
        print("  UNRESOLVED:", w)
    for m in missing:
        print("  MISSING:", m)
    return 0 if not missing else 1


# %run-friendly (matches dbx_export/run.py): run at module level when this file is
# the invoked entrypoint — via `python3 script.py ...` OR AIDP `%run script.py ...`
# (where __name__ is not "__main__") — but NOT on a bare `import` (sys.argv[0] then
# won't be this script). Never sys.exit: print the code so the AIDP kernel survives.
_ENTRY = bool(sys.argv) and os.path.basename(sys.argv[0]).startswith("import_dbx_notebook")
if __name__ == "__main__" or _ENTRY:
    try:
        _rc = main()
    except SystemExit as _e:      # argparse --help / bad-arg exits
        _rc = _e.code
    print(f"\n[import_dbx_notebook] exit code: {_rc}")
