"""Path / name sanitization + target-base guardrails.

Single source of truth for two concerns:
  (a) turning DBX names / paths into AIDP-safe names (spaces -> underscore, etc.)
  (b) computing the final AIDP target path under <target-base> and asserting
      that every write the rest of the codebase performs stays inside the
      export namespace.

If sanitizer.assert_under_export_output() raises, the export aborts before
any AIDP API call goes out — this is the foundation of the "no impact on
existing notebooks/workflows/clusters" guarantee.
"""

from __future__ import annotations

import hashlib
import posixpath
import re
from typing import Literal


def rewrite_embedded_workspace_ref(target: str) -> str:
    """Normalize a %run target that embeds an absolute workspace path.

    A relative %run in a git-backed notebook (e.g. ``Users/x@y.com/nb``) gets
    joined onto the parent's repo dir, yielding an EMBEDDED workspace path like
    ``/Workspace/<repo>/HelperFunctions/Users/x@y.com/nb`` that doesn't exist in
    the repo. If the target contains a ``/Users//Repos//Shared/`` segment past
    position 0, rewrite it to that absolute workspace path. Repeat until the
    path is stable: dependency scans can prepend both the export root and the
    parent notebook owner, producing more than one embedded workspace root.
    The deepest root is the actual absolute reference. Otherwise return the
    target unchanged.

    Single source of truth — used by dependency_resolver (dbx_export) and
    build_dag_from_workflow so all %run resolvers agree.

    LIMITATION: matching is purely lexical on the ``/Users/``, ``/Repos/`` and
    ``/Shared/`` segments. A repo that contains a folder literally named
    ``Users``/``Repos``/``Shared`` past position 0 would have its %run target
    incorrectly truncated at that segment. Low likelihood (these are reserved
    workspace roots), but callers should be aware.
    """
    current = target
    while True:
        best = -1
        for root in ("/Users/", "/Repos/", "/Shared/"):
            # Start at position 1 so an already-rooted path such as
            # ``/Users/owner/Users/actual/notebook`` can still detect the
            # second, embedded workspace root.
            idx = current.find(root, 1)
            if idx > 0 and (best == -1 or idx < best):
                best = idx
        if best <= 0:
            return current
        current = current[best:]

# Buckets under <target-base>/  — every artifact lives in exactly one.
KIND_NOTEBOOK = "notebook"
KIND_JAR = "jar"
KIND_WHL = "whl"
KIND_INIT_SCRIPT = "init_script"
KIND_REQUIREMENTS = "requirements"
KIND_CONFIG = "config"

Kind = Literal["notebook", "jar", "whl", "init_script", "requirements", "config"]

_BUCKET_FOR_KIND = {
    KIND_NOTEBOOK: "notebooks",
    KIND_JAR: "jars",
    KIND_WHL: "python_dependencies",
    KIND_INIT_SCRIPT: "init_scripts",
    KIND_REQUIREMENTS: "python_dependencies",
    KIND_CONFIG: "configs",
}


def sanitize_segment(s: str) -> str:
    """Sanitize a single path segment or display name.

    AIDP's objects and notebook-save APIs allow ASCII letters, digits,
    underscores, dots, plus signs, hyphens, at-signs and spaces. Preserve the
    established space/hyphen/plus-to-underscore mapping for export identity,
    then replace every unsupported character. Collapse and trim underscores.
    Dots and at-signs survive; filenames need not start with a letter.
    """
    if not s:
        return s
    out = s.translate(str.maketrans({
        " ": "_",
        "-": "_",
        "(": "_",
        ")": "_",
        ":": "_",
        "+": "_",
        "&": "_",
    }))
    # [verified: 2026-10] REF-244: an apostrophe in a parent folder makes notebook PUT fail with
    # HTTP 500. Use the API charset for every segment, including filenames;
    # notebook PUT does not consistently validate auto-created parent folders.
    out = re.sub(r"[^A-Za-z0-9_.+@ -]", "_", out)
    out = re.sub(r"_+", "_", out)
    out = out.strip("_")
    # AIDP reserves the workspace-root name: a MID-PATH folder segment named
    # exactly "Workspace" (capital W) cannot hold sub-folders — notebook saves
    # under it fail with a misleading HTTP 409 "Directory already exists".
    # Deps mirror the original /Workspace/... source path, producing
    # .../deps/Workspace/Users/... which trips this. Rename the reserved
    # segment so it no longer collides. Applied uniformly via dbx_to_aidp_path,
    # so upload targets and rewritten %run/notebook.run references stay in sync.
    if out == "Workspace":
        out = "Workspace_"
    return out


def migration_job_output_path(output_base: str, job_name: str) -> str:
    """Shared job folder for notebooks, registry, runtime files and clone callers.

    Oracle tool modification: quoted %run takes the IPython-only path on AIDP
    (mixed-language reproduction, 2026-10-08). Sanitize whitespace-bearing job
    folders, not the workflow's display name; preserve existing space-free job
    folders for resume compatibility. Job folders retain the historical mapping
    even when the file/notebook API charset changes; this function never moves
    an existing artifact tree. Slash/backslash-bearing display names previously
    failed before writes; encode those alone with the workspace underscore
    mapping (ADR 0004). Do not activate this branch for other punctuation.
    """
    base = output_base.rstrip("/")
    if not base or re.search(r"\s", base):
        raise ValueError("Migration output base must be nonempty and whitespace-free")
    if not job_name or any(part.strip() in {".", ".."}
                           for part in re.split(r"[/\\]", job_name)):
        raise ValueError("Migration job name must be nonempty without traversal segments")
    has_separator = any(c in job_name for c in "/\\")
    folder = sanitize_segment(re.sub(r"\s", "_", job_name)) if has_separator else (
        _legacy_sanitize_segment(re.sub(r"\s", "_", job_name))
        if re.search(r"\s", job_name) else job_name
    )
    if not folder:
        raise ValueError("Migration job name sanitizes to an empty folder")
    # Whitespace replacement and underscore stripping can reconstruct traversal
    # segments even when the raw name was neither '.' nor '..' (PR #226 F1).
    if folder in {".", ".."}:
        raise ValueError("Migration job name sanitizes to a traversal segment")
    return f"{base}/{folder}"


def sanitize_path(p: str, *, legacy: bool = False) -> str:
    """Sanitize each segment of a slash-separated path, preserving structure.

    Collapses '//', resolves '..' and '.' via posixpath.normpath. Does not
    add or strip a leading '/'.
    """
    if not p:
        return p
    leading_slash = p.startswith("/")
    originals = [seg for seg in p.split("/") if seg]
    segments = [(_legacy_sanitize_segment(seg) if legacy else sanitize_segment(seg)) for seg in originals]
    if any(not segment for segment in segments):
        raise ValueError(f"Path contains a segment that sanitizes to an empty name: {p!r}")
    if any(segment in {".", ".."} and original != segment
           for original, segment in zip(originals, segments)):
        raise ValueError(f"Path sanitization manufactures a traversal segment: {p!r}")
    joined = "/".join(segments)
    joined = posixpath.normpath(joined) if joined else ""
    return ("/" + joined) if leading_slash else joined


def _legacy_sanitize_segment(s: str) -> str:
    """Frozen historical resource mapping; never apply workspace API rules.

    Existing workflow/cluster lookup names must remain byte-for-byte stable.
    Keep sanitize_segment self-contained because DAG scans copy its source
    into cluster code without importing this module.
    """
    if not s:
        return s
    out = s.translate(str.maketrans({
        " ": "_", "-": "_", "(": "_", ")": "_",
        ":": "_", "+": "_", "&": "_",
    }))
    out = re.sub(r"_+", "_", out).strip("_")
    return "Workspace_" if out == "Workspace" else out


def _sanitize_aidp_resource_name(
    name: str, fallback: str, rejected_chars: str = "[]'",
) -> str:
    """Replace AIDP-rejected characters in a workflow or cluster name.

    Apostrophes and brackets are replaced with underscores before
    historical normalization so its reserved
    ``Workspace_`` sentinel is not stripped afterward.  A stable digest is
    added only for names bearing the supplied rejected characters: replacing the
    delimiter would otherwise make distinct resources such as ``[prod] sync``
    and ``prod sync`` resolve to the same AIDP name.
    """
    has_rejected_chars = any(char in name for char in rejected_chars)
    resource_safe = name.translate(str.maketrans({c: "_" for c in rejected_chars}))
    resource_safe = re.sub(r"_+", "_", resource_safe).strip("_")
    base = _legacy_sanitize_segment(resource_safe)
    digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:8]
    if base and has_rejected_chars:
        return f"{base.rstrip('_')}_{digest}"
    if base:
        return base
    return f"{fallback}_{digest}"


def validate_resource_prefix(prefix: str) -> None:
    """Reject invalid operator namespaces before discovery or API calls."""
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_/]*", prefix):
        raise ValueError(
            "Resource prefix must start with an ASCII letter and contain only "
            "ASCII letters, digits, underscores or forward slashes"
        )


def _legacy_resource_name(name: str, prefix: str, fallback: str) -> str:
    base = _sanitize_aidp_resource_name(name, fallback)
    return f"{prefix}_{base}" if not base.startswith(f"{prefix}_") else base


def legacy_workflow_name(name: str, prefix: str) -> str:
    """Pre-#268 workflow identifier, for exact existing-resource lookup only.

    Never use this name for new resources: the API now rejects characters
    (including dots) that earlier exports retained. Keep the original helper's
    mappings and bracket/apostrophe hashes so re-export finds those resources.
    """
    return _legacy_resource_name(name, prefix, "workflow")


def legacy_cluster_name(name: str, prefix: str) -> str:
    """Pre-#268 cluster identifier, for exact existing-resource lookup only."""
    return _legacy_resource_name(name, prefix, "cluster")


def sanitize_workflow_name(name: str, prefix: str) -> str:
    """Return a letter-led workflow name containing only alphanumerics, _ and /."""
    return _sanitize_strict_resource_name(name, prefix, "workflow")


def _sanitize_strict_resource_name(name: str, prefix: str, fallback: str) -> str:
    # Workflow create returned HTTP 400 for '*' in run ca92d3c4 (2026-10-16):
    # names must start with a letter; only underscore/slash punctuation is allowed.
    # Cluster POST independently returned the same 400 for '*' on 2026-10-19.
    # Validate the operator's prefix rather than silently changing its namespace.
    validate_resource_prefix(prefix)
    # Keep established space/dash/parens/colon/plus/ampersand mappings and names stable.
    # Newly replaced characters join the existing collision-disambiguation path.
    rejected_chars = "".join(
        c for c in name if c not in " -():+&" and not re.fullmatch(r"[A-Za-z0-9_/]", c)
    )
    base = _sanitize_aidp_resource_name(name, fallback, rejected_chars)
    return f"{prefix}_{base}" if not base.startswith(f"{prefix}_") else base


def sanitize_cluster_name(name: str, prefix: str) -> str:
    """Use the same live-verified resource-name contract as workflows."""
    return _sanitize_strict_resource_name(name, prefix, "cluster")


def sanitize_task_key(key: str) -> str:
    """Return an AIDP-safe task key, including rejected ``:``/``+`` chars."""
    return key.translate(str.maketrans({
        "-": "_",
        "(": "_",
        ")": "_",
        " ": "_",
        ":": "_",
        "+": "_",
    }))


def normalize_workspace_path(p: str) -> str:
    """Ensure /Workspace/ prefix; normalize separators."""
    if not p:
        return p
    if not p.startswith("/Workspace/"):
        if p.startswith("Workspace/"):
            p = "/" + p
        elif p.startswith("/"):
            p = "/Workspace" + p
        else:
            p = "/Workspace/" + p
    # Collapse '//' and resolve '.' / '..' via posixpath.
    return posixpath.normpath(p)


def dbx_to_aidp_path(
    dbx_path: str,
    export_output: str,
    kind: Kind,
    is_git: bool = False,
    extension: str = ".ipynb",
    *,
    legacy: bool = False,
) -> str:
    """Map a DBX path to its AIDP destination under <target-base>.

    DBX path is the canonical DBX REST form (no /Workspace/ prefix, no .ipynb
    suffix). Examples:
        /Users/foo@bar/MyNotebook
        /Optimiser_Utils/Randomizer trigger
        /Repos/foo@bar/my_repo/notebook

    For notebooks, the result mirrors the full DBX path under
    <target-base>/notebooks/ AND appends .ipynb (AIDP requires the extension).
    Git-backed notebooks (`is_git=True`) get an extra `GIT/` segment in their
    AIDP path so they're easy to identify (matches the reference notebook's
    fix_aidp_workspace_path pattern).

    ``legacy=True`` computes the exact pre-REF-244 candidate for read-only
    existence/content verification; it must not create a new copied artifact.

    For files (jar/whl/init/requirements/config), the result is flat under the
    appropriate bucket, keyed by sanitized basename.
    """
    export_output = normalize_workspace_path(export_output).rstrip("/")
    bucket = _BUCKET_FOR_KIND[kind]

    if kind == KIND_NOTEBOOK:
        # Strip /Workspace/ if caller passed it, and strip any existing file
        # extension (.ipynb/.py/.scala) before appending the desired one.
        rel = dbx_path
        if rel.startswith("/Workspace/"):
            rel = rel[len("/Workspace/"):]
        # Resource files use the notebook bucket so their repository-relative
        # layout is preserved. Strip the caller-supplied extension first to
        # avoid results such as ``default.yaml.yaml``.
        if extension and rel.lower().endswith(extension.lower()):
            rel = rel[: -len(extension)]
        for known_ext in (".ipynb", ".py", ".scala", ".sql", ".r"):
            if rel.lower().endswith(known_ext):
                rel = rel[: -len(known_ext)]
                break
        rel = rel.lstrip("/")
        rel = sanitize_path(rel, legacy=legacy)
        if is_git:
            return f"{export_output}/{bucket}/GIT/{rel}{extension}"
        return f"{export_output}/{bucket}/{rel}{extension}"

    # File-kinds: flatten to basename.
    base = posixpath.basename(dbx_path) or "unnamed"
    base = _legacy_sanitize_segment(base) if legacy else sanitize_segment(base)
    if not base or base in {".", ".."}:
        raise ValueError(f"File name sanitizes to an empty name: {dbx_path!r}")
    return f"{export_output}/{bucket}/{base}"


# ─── Guardrails ──────────────────────────────────────────────────────


class TargetBaseViolation(RuntimeError):
    """Raised when a write would land outside <target-base>. Hard abort."""


def validate_export_output(export_output: str) -> str:
    """Reject target-base values that risk colliding with real workspaces.

    Strict on input format — does NOT silently wrap arbitrary paths. The CLI
    user must pass a `/Workspace/...` path explicitly.

    Must be at least 1 segment under /Workspace/, not in the reserved set:
        /Workspace/ai_dbx_exported            ✓
        /Workspace/team/ai_dbx_exported        ✓
        /Workspace/Users (or /Workspace/Users/*) ✗  (user-home root)
        /Workspace/Shared/*                    ✗
        /Workspace/Repos/*                     ✗
        /Workspace, /Workspace/                ✗
        /, /etc/passwd, etc.                   ✗
    Returns the normalized form.
    """
    if not isinstance(export_output, str) or not export_output.startswith("/Workspace/"):
        raise TargetBaseViolation(
            f"--export-output must start literally with /Workspace/: got {export_output!r}"
        )
    p = posixpath.normpath(export_output.rstrip("/"))
    if p == "/Workspace" or not p.startswith("/Workspace/"):
        raise TargetBaseViolation(
            f"--export-output must be a subdirectory of /Workspace/: got {export_output!r}"
        )
    after = p[len("/Workspace/"):]
    first = after.split("/", 1)[0]
    if first in {"Users", "Shared", "Repos"}:
        raise TargetBaseViolation(
            f"--export-output {p!r} starts with reserved root {first!r}; "
            f"pick something like /Workspace/ai_dbx_exported to keep "
            f"exported artifacts isolated"
        )
    return p


def assert_under_export_output(path: str, export_output: str, label: str = "path") -> None:
    """Raise TargetBaseViolation if path is not inside export_output.

    Called by aidp_client before every write. This is the load-bearing
    guardrail for isolation — bypass it and bugs can hit production paths.
    """
    norm_path = normalize_workspace_path(path).rstrip("/")
    norm_base = normalize_workspace_path(export_output).rstrip("/")
    if norm_path != norm_base and not norm_path.startswith(norm_base + "/"):
        raise TargetBaseViolation(
            f"Refusing write: {label}={norm_path!r} is not under "
            f"export_output={norm_base!r}"
        )


def assert_has_prefix(name: str, prefix: str, label: str = "name") -> None:
    """Raise if name does not start with `<prefix>_`. For workflows/clusters."""
    expected = f"{prefix}_"
    if not name.startswith(expected):
        raise TargetBaseViolation(
            f"Refusing create: {label}={name!r} does not start with "
            f"required prefix {expected!r}"
        )
