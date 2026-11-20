"""Export destination planning and exact, verified legacy lookup; no writes.

Invalid identifiers and duplicate targets fail closed before uploads, regardless
of strict/overwrite flags (REF-244 review and issue #249).
"""
from dataclasses import dataclass, field
from typing import Dict, Tuple
from requests.exceptions import RequestException

from . import sanitizer
from .aidp_client import AidpApiError
from .dbx_client import _export_api_path
from .report import FileResult, JobResult, NotebookResult
from .validator import cell_fingerprint, file_sha256


def file_target_path(source, kind, root, *, legacy=False):
    kind = kind if kind in {"jar", "whl", "init_script", "requirements"} else "config"
    return sanitizer.dbx_to_aidp_path(source, root, kind, legacy=legacy)


def file_upload_candidate(node):
    """Match phase 2's fetched-file set; an empty byte payload is valid."""
    return node.aidp_repo_path is None and node.fetch_error is None and node.content is not None


def _notebook_source_identity(source, node):
    # Workspace export strips ONLY the documented API aliases. Git paths
    # name repository files, where extensions are significant source identity.
    if node.git_owner and node.git_repo:
        origin = ("git", node.git_owner, node.git_repo, node.git_branch or "main", source.lstrip("/"))
    else:
        origin = ("workspace", _export_api_path(source))
    return origin, node.is_notebook, node.final_extension


def _trimmed_parent(path):
    """Return path as the Git fetcher's 404-only parent-whitespace fallback retries it."""
    parent, _, name = path.rpartition("/")
    return f"{parent.strip()}/{name}" if parent.strip() else name


def _git_source_path_aliases(keys, entries):
    """REF-294: accept only fetched stem/.py or stem/.ipynb references to one Git file.

    REF-278: a reference the 404-only parent-whitespace fallback recovered
    ("Folder /name", fetched as "Folder/name.py") names that file as well.
    Category 2 (artifact identity): discovery keys and notebook promotion stay
    unchanged. Filename resemblance or equal bytes alone never prove identity.
    """
    if not all(key[0] == "notebook" for key in keys):
        return False
    nodes = [entries[key] for key in keys]
    if any(not (node.git_owner and node.git_repo and node.resolved_git_path)
           or node.export_error is not None or node.content is None for node in nodes):
        return False
    identities = {(node.git_owner, node.git_repo, node.git_branch or "main",
                   node.resolved_git_path, node.is_notebook, node.final_extension)
                  for node in nodes}
    if len(identities) != 1:
        return False
    resolved = nodes[0].resolved_git_path
    suffix = next((suffix for suffix in (".py", ".ipynb") if resolved.endswith(suffix)), None)
    if suffix is None:
        return False
    names = {resolved, resolved[:-len(suffix)]}
    references = {key[1].lstrip("/") for key in keys}
    recovered = {_trimmed_parent(reference) for reference in references - names}
    return recovered <= names if recovered else references == names


def _notebook_fingerprint(node):
    return cell_fingerprint(node.nb_dict["cells"]) if node.is_notebook else file_sha256(node.content)


@dataclass
class ExportPathPlan:
    notebooks: Dict[str, str] = field(default_factory=dict)
    files: Dict[str, str] = field(default_factory=dict)
    generated: Dict[str, str] = field(default_factory=dict)
    verified: Dict[Tuple[str, str], str] = field(default_factory=dict)
    verified_sizes: Dict[Tuple[str, str], int] = field(default_factory=dict)
    errors: Dict[Tuple[str, str], str] = field(default_factory=dict)
    # Discovery alias -> the node whose upload/verification outcome it shares.
    notebook_aliases: Dict[str, str] = field(default_factory=dict)


def _existing_file(aidp, path):
    # object_exists() historically treats service/permission errors as absence.
    # Use the authoritative download metadata/read path; a failed PAR GET is
    # not proof that the workspace object is absent.
    try:
        return aidp.download_file(path)
    except AidpApiError as exc:
        if exc.op == "downloadFileMeta" and exc.status == 404:
            return None
        raise


def prepare_export_paths(graph, root, report, aidp=None):
    """Validate all graph destinations before dry-run returns or uploads start.

    Shared-repository references are not new export destinations. Exact lookup
    errors never mean absence; remapped existing objects require equal content.
    """
    plan, legacy, entries = ExportPathPlan(), {}, {}
    for source, node in graph.notebooks.items():
        key = ("notebook", source)
        entries[key] = node
        try:
            kwargs = dict(is_git=bool(node.git_owner and node.git_repo), extension=node.final_extension)
            plan.notebooks[source] = sanitizer.dbx_to_aidp_path(source, root, "notebook", **kwargs)
            legacy[key] = sanitizer.dbx_to_aidp_path(source, root, "notebook", legacy=True, **kwargs)
        except ValueError as exc:
            plan.errors[key] = str(exc)
    for source, node in graph.files.items():
        key = ("file", source)
        if node.aidp_repo_path is not None:
            plan.files[source] = node.aidp_repo_path
            continue
        if not file_upload_candidate(node):
            continue  # phase 2 retains its record-only/failure/fallback report
        entries[key] = node
        try:
            plan.files[source] = file_target_path(source, node.kind, root)
            legacy[key] = file_target_path(source, node.kind, root, legacy=True)
        except ValueError as exc:
            plan.errors[key] = str(exc)

    def target(key):
        if key[0] == "generated":
            return plan.generated.get(key[1], "UNRESOLVED")
        return (plan.notebooks if key[0] == "notebook" else plan.files).get(key[1], "UNRESOLVED")

    def register_generated():
        from .foreach_bridge import foreach_unsupported_reason
        from .job_converter import convert_dbx_task
        for job_id, job in graph.jobs.items():
            if not job.job_def:
                continue
            settings = job.job_def.get("settings") or job.job_def
            for task in settings.get("tasks", []) or []:
                if not task.get("for_each_task") or foreach_unsupported_reason(task) is not None:
                    continue
                key = ("generated", f"{job_id}/{task.get('task_key', '')}")
                entries[key] = job
                try:
                    converted = convert_dbx_task(task, root, {}, dbx_job_id=str(job_id),
                        notebook_path_map=plan.notebooks, file_path_map=plan.files)
                    plan.generated[key[1]] = converted["notebookPath"]
                except ValueError as exc:
                    plan.errors[key] = str(exc)

    def reject_collisions():
        plan.notebook_aliases.clear()
        reverse = {}
        for key in entries:
            if key not in plan.errors:
                reverse.setdefault(target(key), []).append(key)
        for destination, keys in reverse.items():
            if len(keys) > 1:
                reason = f"duplicate export destination {destination!r}: {keys!r}"
                source_aliases = _git_source_path_aliases(keys, entries)
                if source_aliases or (all(key[0] == "notebook" for key in keys) and len({
                        _notebook_source_identity(key[1], entries[key]) for key in keys}) == 1):
                    try:
                        fingerprints = {_notebook_fingerprint(entries[key]) for key in keys}
                        if source_aliases:
                            # Also detect a branch moving between the two reads.
                            fingerprints = {(_notebook_fingerprint(entries[key]), file_sha256(entries[key].content))
                                            for key in keys}
                    except (KeyError, TypeError, ValueError) as exc:
                        reason = f"cannot verify notebook source aliases at {destination!r}: {type(exc).__name__}"
                    else:
                        if len(fingerprints) == 1:
                            message = f"INFO notebook-source-aliases: {keys!r} identify one source at {destination}"
                            if message not in report.errors:
                                report.errors.append(message)
                            if source_aliases:
                                canonical = keys[0][1]
                                plan.notebook_aliases.update({key[1]: canonical for key in keys[1:]})
                            continue
                        reason = f"conflicting content for notebook source aliases at {destination!r}: {keys!r}"
                for key in keys:
                    plan.errors[key] = reason

    register_generated()
    reject_collisions()
    existing = {}
    if not plan.errors and aidp is not None:
        for key, old in legacy.items():
            current, node = target(key), entries[key]
            if old == current or node.content is None:
                continue
            if (key[0] == "notebook" and node.export_error is not None) or (
                    key[0] == "file" and node.fetch_error is not None):
                continue
            try:
                if key[0] == "notebook" and node.is_notebook:
                    new_content, old_content = aidp.get_notebook(current), aidp.get_notebook(old)
                else:
                    new_content, old_content = _existing_file(aidp, current), _existing_file(aidp, old)
            except AidpApiError as exc:
                # download_file wraps transport errors; its body may contain
                # the temporary PAR URL. Report only the operation/status.
                plan.errors[key] = f"destination lookup failed (not absence): {exc.op}: HTTP {exc.status}"
                continue
            except ValueError as exc:
                plan.errors[key] = f"destination lookup response invalid (not absence): {type(exc).__name__}"
                continue
            except RequestException as exc:
                # A failed PAR download can include its secret URL in str(exc).
                plan.errors[key] = f"destination lookup transport failure ({type(exc).__name__}); no absence inferred"
                continue
            if new_content is not None and old_content is not None:
                plan.errors[key] = f"both current and legacy targets exist: {current!r}, {old!r}; manual reconciliation required"
                continue
            content = new_content if new_content is not None else old_content
            if content is None:
                report.errors.append(f"INFO path-resolution: {key!r}: current/legacy absent; create {current}")
                continue
            chosen = current if new_content is not None else old
            (plan.notebooks if key[0] == "notebook" else plan.files)[key[1]] = chosen
            existing[key] = content
            report.errors.append(f"INFO path-resolution: {key!r}: selected {chosen}; canonical={current}; legacy={old}")

        # Selected legacy targets must not alias another source's current path.
        register_generated()
        reject_collisions()
        for key, content in existing.items():
            if key in plan.errors:
                continue
            node = entries[key]
            try:
                if key[0] == "notebook" and node.is_notebook:
                    expected, actual = _notebook_fingerprint(node), cell_fingerprint(content["cells"])
                else:
                    payload = node.content
                    if key[0] == "file" and node.kind == "init_script":
                        from .cluster_converter import rewrite_init_script
                        text, _, redundant, _ = rewrite_init_script(payload.decode("utf-8"), plan.files)
                        payload = text.encode("utf-8")
                        if redundant:
                            node.skip_attach = True
                    expected, actual = file_sha256(payload), file_sha256(content)
                if expected != actual:
                    plan.errors[key] = f"remapped existing target differs from source: {target(key)!r}; manual reconciliation required (overwrite cannot establish source identity)"
                else:
                    plan.verified[key] = expected
                    if isinstance(content, bytes):
                        plan.verified_sizes[key] = len(content)
            except (KeyError, TypeError, ValueError) as exc:
                plan.errors[key] = f"cannot verify remapped existing target {target(key)!r}: {exc}"

    for key, reason in plan.errors.items():
        node = entries[key]
        detail = dict(artifact_type=key[0], source=key[1], target=target(key), reason=reason)
        if detail in report.path_plan_errors:
            continue
        report.path_plan_errors.append(detail)
        report.errors.append(f"export-path-invalid: {key!r}: {reason}")
        if key[0] == "notebook":
            report.notebooks.append(NotebookResult(dbx_path=key[1], aidp_path=target(key), role=node.role,
                status="EXPORT_FAILED", placeholder_reason="invalid_export_path", placeholder_detail=reason))
        elif key[0] == "file":
            report.files.append(FileResult(dbx_source=key[1], aidp_path=target(key), kind=node.kind,
                status="FAILED", failure_reason=reason))
        else:
            settings = node.job_def.get("settings") or node.job_def
            if not any(row.dbx_job_id == node.dbx_job_id and row.status == "FAILED" for row in report.jobs):
                report.jobs.append(JobResult(dbx_job_id=node.dbx_job_id,
                    dbx_job_name=settings.get("name", "UNRESOLVED"), aidp_workflow_name="UNRESOLVED",
                    status="FAILED", failure_reason=reason))
    return plan
