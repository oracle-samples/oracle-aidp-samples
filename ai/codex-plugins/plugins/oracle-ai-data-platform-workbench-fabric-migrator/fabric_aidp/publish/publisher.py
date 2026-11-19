"""Push a completed migration into an AIDP workspace.

The one part of this tool that writes anywhere. Its rules exist because the
sibling projects hit each of these:

  * **Dry run by default.** `publish` prints what it would do and changes
    nothing. `--apply` is the only thing that makes it act.
  * **Never overwrite.** A notebook path or job name that already exists is
    skipped, not replaced, and a job whose notebook was skipped is refused
    (exit 1) -- it would otherwise run someone else's notebook. The Databricks
    validator creates a *clone*; it never edits the workflow it read.
    (`notebook update-content` itself overwrites silently, so the check has
    to happen here.)
  * **A prefix per person.** Two people publishing the same demo into one
    workspace collided, so every path and job name carries one. `--apply`
    refuses to run without it; a dry run warns.
  * **Notebooks first, jobs second.** A job task pointing at a notebook that
    is not there yet fails at run time, far from the cause.
  * **Refuse a job whose notebooks did not all upload.** Same reason.
  * **Convert every notebook before sending any.** A notebook that will not
    become an .ipynb is refused in the plan, with its jobs, so it cannot
    stop a run half-way through the uploads.
"""
from __future__ import annotations

import json
from pathlib import Path

from fabric_aidp.migrate.runner import IN_PROGRESS_MARKER, WORKSPACE_PREFIX
from fabric_aidp.publish.aidp_client import AidpClient, AidpError, AidpUnavailable
from fabric_aidp.publish.to_ipynb import task_parameters_read, to_ipynb
from fabric_aidp.translate.pipeline_to_aidp_job import JOB_NAME

DEFAULT_WORKSPACE_ROOT = "/Workspace"
# Where a migrated `.job.json` says its notebooks are: `migrate` writes every
# task's notebookPath as "<WORKSPACE_PREFIX>/<name>.py", knowing nothing of
# where publish will put them. So it is the key job tasks are looked up by,
# and `workspace_root` -- where the notebooks actually land -- is not. Tried
# keyed on that: every job under a non-default root pointed at "notebooks this
# migration did not produce". The two constants are both "/Workspace" today,
# which is why keying on DEFAULT_WORKSPACE_ROOT happened to work.
MIGRATED_ROOT = WORKSPACE_PREFIX


class PublishError(Exception):
    """The migration cannot be published as it stands."""


_PUBLISHABLE = {"ok", "needs_manual_review"}


def _reported(out_dir: Path, report: dict):
    """(notebooks, jobs) the report says this migration produced.

    Not a glob: migrate writes into an existing directory and never deletes,
    so a --filter re-run, an interrupted run, or a pipeline that has since
    become blocked leaves files behind that the report no longer vouches for
    -- including the job file of a pipeline now refused whole.
    """
    rows = report.get("results") if isinstance(report, dict) else None
    if not isinstance(rows, list):
        raise PublishError(f"{out_dir / 'report.json'} has no results list")
    root = out_dir.resolve()
    found = {"notebook": [], "pipeline_job": []}
    for row in rows:
        if not isinstance(row, dict) or row.get("status") not in _PUBLISHABLE:
            continue
        if row.get("kind") not in found or not isinstance(row.get("output_path"), str):
            continue
        path = (out_dir / row["output_path"]).resolve()
        if root not in path.parents or not path.is_file():
            raise PublishError(
                f"report.json lists {row['output_path']!r}, which is not a file "
                f"inside {out_dir}; run `verify` on this migration")
        found[row["kind"]].append(path)
    return sorted(found["notebook"]), sorted(found["pipeline_job"])


def _setup_notebook(out_dir: Path, report: dict):
    """The setup notebook `migrate` wrote, if the report vouches for one.

    Read from `report["setup"]`, like everything else publish sends, and not
    globbed for. It is uploaded before every other notebook because every
    migrated job runs it as its first task (see migrate/setup.py): without it
    each job is refused here, correctly, as pointing at a notebook this
    migration did not produce.
    """
    setup = report.get("setup") if isinstance(report, dict) else None
    relative = setup.get("notebook") if isinstance(setup, dict) else None
    if not relative:
        return None
    path = (out_dir / relative).resolve()
    if out_dir.resolve() not in path.parents or not path.is_file():
        raise PublishError(
            f"report.json lists setup notebook {relative!r}, which is not a "
            f"file inside {out_dir}; re-run `migrate`")
    return path


def _remote_path(root: str, prefix: str, name: str) -> str:
    parts = [root.rstrip("/")] + ([prefix.strip("/")] if prefix else []) + [name]
    return "/".join(parts)


def plan_publish(out_dir, *, prefix="", workspace_root=DEFAULT_WORKSPACE_ROOT,
                 cluster_key=None) -> dict:
    """What publishing would do. Reads the migration only; contacts nothing."""
    out_dir = Path(out_dir)
    if not out_dir.is_dir():
        raise PublishError(f"no such migration directory: {out_dir}")
    if prefix and not JOB_NAME.fullmatch(prefix):
        # It becomes the front of every job name, and AIDP rejects the job
        # only after every notebook has been uploaded.
        raise PublishError(
            f"--prefix {prefix!r} must be a letter followed by letters, digits "
            f"or underscores: AIDP rejects any other job name")
    report = out_dir / "report.json"
    if not report.is_file():
        raise PublishError(
            f"{out_dir} has no report.json; publish reads a completed migration, "
            f"so run `migrate` first")
    if (out_dir / IN_PROGRESS_MARKER).exists():
        raise PublishError(
            f"{out_dir} holds an interrupted migration ({IN_PROGRESS_MARKER} is "
            f"present), so its report and artifacts disagree; re-run `migrate`")
    try:
        loaded = json.loads(report.read_text(encoding="utf-8"))
        notebooks, job_files = _reported(out_dir, loaded)
    except (OSError, ValueError) as exc:
        raise PublishError(f"cannot read {report}: {exc}") from exc
    setup = _setup_notebook(out_dir, loaded)
    if setup is not None:
        clash = [p.name for p in notebooks if p.stem == setup.stem]
        if clash:
            # Two notebooks would land on one remote path, and every job's
            # first task would run whichever was uploaded last.
            raise PublishError(
                f"a migrated notebook is named {clash[0]!r}, the same as the "
                f"setup notebook every job runs first; rename the Fabric "
                f"notebook and migrate again")
        notebooks = [setup] + notebooks

    uploads, uploaded_names = [], {}
    refused_notebooks, unconvertible = [], {}
    # What the conversion noticed and could not fix, and what each notebook
    # ends up reading back from its task. `to_ipynb` used to decide both in
    # silence: it is a translator that runs after `verify`, so nothing it
    # found reached a reader at all.
    findings, parameters_read = [], {}
    for path in notebooks:
        remote = _remote_path(workspace_root, prefix, path.stem + ".ipynb")
        # Converted here, before anything is sent, because the conversion is
        # what can fail. It used to happen inside the upload loop, guarded
        # only against AIDP errors: one migrated notebook with no
        # `# Fabric notebook source` header (graded REVIEW, NB06) raised
        # IpynbParseError out of `publish --apply` after ~30 notebooks had
        # uploaded and before any job was created -- a traceback, and a
        # workspace left half-published.
        try:
            text = path.read_text(encoding="utf-8")
            converted = []
            to_ipynb(text, findings=converted)
            parameters_read[f"{MIGRATED_ROOT}/{path.name}"] = (
                task_parameters_read(text))
        except (OSError, ValueError) as exc:
            reason = (f"cannot be converted to .ipynb, so AIDP could not run "
                      f"it as a task: {exc}")
            refused_notebooks.append({"notebook": path.name, "local": str(path),
                                      "remote": remote, "reason": reason})
            unconvertible[f"{MIGRATED_ROOT}/{path.name}"] = path.name
            continue
        findings.extend({"notebook": path.name, "rule": f.rule,
                         "detail": f.detail, "severity": f.severity}
                        for f in converted)
        uploads.append({"local": str(path), "remote": remote})
        uploaded_names[f"{MIGRATED_ROOT}/{path.name}"] = remote

    jobs, blocked = [], []
    for path in job_files:
        try:
            definition = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            blocked.append({"job": path.name, "reason": f"unreadable: {exc}"})
            continue
        refused = sorted({unconvertible[t.get("notebookPath")]
                          for t in definition.get("tasks", [])
                          if t.get("notebookPath") in unconvertible})
        if refused:
            blocked.append({
                "job": definition.get("name", path.stem),
                "reason": f"tasks point at notebooks publish refused because "
                          f"they cannot be converted to .ipynb: {refused}"})
            continue
        missing = [t.get("notebookPath") for t in definition.get("tasks", [])
                   if t.get("notebookPath") not in uploaded_names]
        if missing:
            blocked.append({
                "job": definition.get("name", path.stem),
                "reason": f"tasks point at notebooks this migration did not "
                          f"produce: {sorted(set(missing))}"})
            continue
        # The two halves of this feature, checked against each other. The
        # translator puts a task parameter in the job knowing nothing about
        # the notebook; the notebook re-reads what its parameters cell
        # declares knowing nothing about the job. Where they disagree, AIDP
        # supplies the value and the notebook ignores it -- silently, and
        # at PASS, which is the defect the parameter work exists to close.
        for task in definition.get("tasks", []):
            read = parameters_read.get(task.get("notebookPath"))
            if read is None:
                continue
            for parameter in task.get("parameters") or []:
                name = parameter.get("name")
                if name in read:
                    continue
                findings.append({
                    "job": definition.get("name", path.stem),
                    "rule": "NB37_TASK_PARAMETER_IGNORED",
                    "detail": f"task {task.get('taskKey')!r} passes {name!r} to "
                              f"{task.get('notebookPath')}, and that notebook's "
                              f"parameters cell does not read it back, so the "
                              f"notebook runs on its own default and the value "
                              f"in the job has no effect",
                    "severity": "flag"})
        definition = json.loads(json.dumps(definition))
        for task in definition.get("tasks", []):
            task["notebookPath"] = uploaded_names[task["notebookPath"]]
            if cluster_key:
                task["cluster"] = {"clusterKey": cluster_key}
        if prefix:
            definition["name"] = f"{prefix}_{definition.get('name', path.stem)}"
        if not JOB_NAME.fullmatch(str(definition.get("name") or "")):
            blocked.append({"job": definition.get("name", path.stem),
                            "reason": "not a name AIDP accepts (a letter, then letters, "
                                      "digits or underscores); re-run `migrate`"})
            continue
        if not cluster_key and any("cluster" not in t for t in definition.get("tasks", [])):
            blocked.append({
                "job": definition.get("name", path.stem),
                "reason": "no cluster key; an AIDP task cannot run without one "
                          "-- pass --cluster-key"})
            continue
        jobs.append({"name": definition["name"], "definition": definition,
                     "source": str(path)})

    # Two pipelines landing on one job name: refuse all of them, whichever
    # sorts first -- publishing one would be an arbitrary choice.
    clashes = {}
    for job in jobs:
        clashes.setdefault(job["name"], []).append(Path(job["source"]).name)
    for name, sources in clashes.items():
        if len(sources) > 1:
            blocked.append({"job": name, "reason": f"{len(sources)} pipelines map to this "
                            f"job name: {', '.join(sorted(sources))}; rename one in Fabric"})
    jobs = [j for j in jobs if len(clashes[j["name"]]) == 1]

    return {"out_dir": str(out_dir), "prefix": prefix,
            "notebooks": uploads, "jobs": jobs, "blocked": blocked,
            "refused_notebooks": refused_notebooks, "findings": findings}


def publish(out_dir, *, workspace_key, cluster_key=None, prefix="",
            workspace_root=DEFAULT_WORKSPACE_ROOT, instance_id=None,
            profile=None, auth=None, apply=False, reuse_existing=False,
            log=None) -> dict:
    """Dry run unless `apply` is true.

    `reuse_existing` is the way out of a run whose notebooks uploaded but whose
    job did not: the notebooks are then at this prefix's paths and skipped, and
    without it their job would be refused forever. It trusts that what is at
    those paths is this migration's -- say so explicitly, on purpose.
    """
    def say(message):
        if log:
            log(message)

    if apply and not prefix:
        # The prefix is what keeps two people's publishes apart, and an empty
        # one used to pass validation (`if prefix and ...`): notebooks landed
        # straight in the workspace root and jobs took the bare pipeline name,
        # the collision the prefix exists to prevent. Refused before anything
        # is read, so nothing can have been sent.
        raise PublishError(
            "--prefix is required with --apply: without it notebooks land in "
            f"{workspace_root.rstrip('/') or '/'} itself and jobs get "
            "un-namespaced names that collide with anyone else publishing "
            "there; pass --prefix <yourname>")
    planned = plan_publish(out_dir, prefix=prefix, workspace_root=workspace_root,
                           cluster_key=cluster_key)
    planned["applied"] = bool(apply)

    # Said on both paths, before anything is sent. These are the conversion's
    # own findings, and they used to have nowhere to go: `to_ipynb` runs
    # after `verify`, so none of this is in report.json and a reader who
    # read the artifact never saw it.
    for finding in planned["findings"]:
        where = finding.get("notebook") or finding.get("job") or ""
        say(f"  {finding['severity'].upper():8s} {finding['rule']} {where}: "
            f"{finding['detail']}")

    if not apply:
        if not prefix:
            say(f"warning: no --prefix, so this plan puts notebooks in "
                f"{workspace_root.rstrip('/') or '/'} itself and jobs under their "
                f"bare pipeline names; --apply will refuse to run without one.")
        say(f"dry run -- nothing was sent. "
            f"{len(planned['notebooks'])} notebook(s), {len(planned['jobs'])} job(s) "
            f"would be created, "
            f"{len(planned['blocked']) + len(planned['refused_notebooks'])} refused, "
            f"{len(planned['findings'])} finding(s) from the .ipynb conversion.")
        say("re-run with --apply to publish.")
        return planned

    if not workspace_key:
        raise PublishError("--workspace-key is required to publish")
    client = AidpClient(workspace_key=workspace_key, instance_id=instance_id,
                        profile=profile, auth=auth)
    if not client.available():
        raise PublishError(
            "the `aidp` CLI is not installed; `pip install aidp-cli` to publish")

    results = {"notebooks": [], "jobs": [], "blocked": planned["blocked"],
               "findings": planned["findings"]}
    # Read what is already there before writing anything: if either read
    # fails, nothing has been sent yet.
    try:
        present = {folder: client.folder_names(folder) for folder in
                   sorted({u["remote"].rsplit("/", 1)[0] for u in planned["notebooks"]})}
        existing = {j.get("name") for j in client.list_jobs()} if planned["jobs"] else set()
    except (AidpError, AidpUnavailable) as exc:
        raise PublishError(
            f"cannot read the workspace, so cannot prove publish would not "
            f"overwrite anything; nothing was sent: {exc}") from exc
    # A job on a key that cannot run jobs is accepted by create-job and fails
    # only at run time, after every notebook is uploaded. Said here instead,
    # before anything is sent. See AidpClient.cluster_problem.
    if planned["jobs"] and cluster_key:
        try:
            problem = client.cluster_problem(cluster_key)
        except (AidpError, AidpUnavailable) as exc:
            raise PublishError(f"cannot check --cluster-key; nothing was sent: {exc}") from exc
        if problem:
            raise PublishError(f"{problem}; nothing was sent")
    for upload in planned["notebooks"]:
        folder, name = upload["remote"].rsplit("/", 1)
        if name in present[folder]:
            results["notebooks"].append(dict(
                upload, status="skipped",
                reason="a notebook already exists at this path"))
            say(f"  skipped  {upload['remote']}: already exists (publish never overwrites)")
            continue
        try:
            # Planning already converted it; this guards only against the
            # file changing since, which must still not end the run.
            payload = to_ipynb(Path(upload["local"]).read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            results["notebooks"].append(dict(upload, status="error", error=str(exc)))
            say(f"  FAILED   {upload['remote']}: cannot convert to .ipynb: {exc}")
            continue
        try:
            client.put_notebook(upload["remote"], payload)
            results["notebooks"].append(dict(upload, status="uploaded"))
            say(f"  uploaded {upload['remote']}")
        except (AidpError, AidpUnavailable) as exc:
            results["notebooks"].append(dict(upload, status="error", error=str(exc)))
            say(f"  FAILED   {upload['remote']}: {exc}")

    usable = {"uploaded", "skipped"} if reuse_existing else {"uploaded"}
    failed = {u["remote"] for u in results["notebooks"] if u["status"] not in usable}
    for job in planned["jobs"]:
        if job["name"] in existing:
            results["jobs"].append(dict(job, status="skipped",
                                        reason="a job of this name already exists"))
            say(f"  skipped  {job['name']}: already exists (publish never overwrites)")
            continue
        blocked_by = [t["notebookPath"] for t in job["definition"]["tasks"]
                      if t["notebookPath"] in failed]
        if blocked_by:
            results["jobs"].append(dict(
                job, status="refused",
                reason=f"notebook not uploaded by this run: {blocked_by}"))
            say(f"  REFUSED  {job['name']}: its notebooks were not uploaded by this run "
                f"(if they are this migration's, re-run with --reuse-existing-notebooks)")
            continue
        try:
            key = client.create_job(job["definition"])
            results["jobs"].append(dict(job, status="created", key=key))
            say(f"  created  job {job['name']} ({key})")
        except (AidpError, AidpUnavailable) as exc:
            results["jobs"].append(dict(job, status="error", error=str(exc)))
            say(f"  FAILED   job {job['name']}: {exc}")

    planned.update(results)
    return planned
