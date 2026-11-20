"""dbx_export CLI — Databricks Job → AIDP Workflow EXPORTER.

Copies DBX artifacts to AIDP for later migration. Does not rewrite code.

Run with:
    python3 -m dbx_export --job-id <dbx_job_id> [options]

Three-phase flow:
  Phase 1 — Discover + download from DBX + validate locally (no AIDP writes)
  PRE-FLIGHT — print plan; failed support-files become warnings, not aborts
  Phase 2 — Upload notebooks + files to AIDP with read-back validation
  Phase 3 — Create AIDP cluster + workflow definitions
"""

from __future__ import annotations

import argparse
import ast
import copy
import os
import re
import sys
import time
import warnings
from dataclasses import replace
from datetime import datetime
from enum import IntEnum
from types import CodeType
from typing import Any, Dict, List, Optional, Set, Tuple

from packaging import version as pkg_version
from scripts.pip_commands import parse_pip_arguments, parse_pip_tokens
from packaging.utils import (
    InvalidWheelFilename,
    canonicalize_name,
    parse_wheel_filename,
)

from . import placeholder as ph_mod
from . import sanitizer
from .aidp_client import AidpClient, AidpConfig, AidpApiError
from .auth import (
    DEFAULT_LAKE_OCID,
    DEFAULT_OCI_PROFILE,
    DEFAULT_WORKSPACE_ID,
    get_oci_signer,
)
from .cluster_converter import (
    ShapeCapacityError,
    build_library_install_items,
    convert_dbx_cluster,
    learn_limits_and_renormalize,
)
from .dbx_client import DbxClient, DbxConfig, DbxExportError
from .dependency_resolver import DependencyResolver, Graph, can_skip_absent_soft_match
from .job_converter import (
    convert_dbx_job,
    task_conversion_refusal,
    validate_workflow_parameter_overrides,
)
from .jar_discovery import discover_notebook_jars, reconcile_failed_declared_jars
from .import_audit import ImportAuditResult, audit_notebook_imports
from .customer_cluster_init_scripts import (
    attach_cluster_init_script,
    prepare_cluster_init_script_rules,
    verify_existing_cluster_init_script,
)
from .ray_cluster_init_script import (
    attach_ray_init_script,
    prepare_ray_cluster_init_script_rules,
    verify_existing_ray_init_script,
)
from .customer_cluster_libraries import (
    check_cluster_library_conflicts,
    filter_cluster_library_refs,
    prepare_cluster_library_rules,
    verify_cluster_library_contract,
)
from .param_analysis import analyze_task_params
from .report import (
    FileResult,
    JobResult,
    NotebookResult,
    Report,
    render_pre_flight,
)
from .requirements import (
    CANONICAL_REQUIREMENTS_PREFIX,
    RequirementConsolidationError,
    RequirementSource,
    consolidate_requirement_sources,
    load_compatibility_policy,
    target_runtime_requirements_source,
)
from .sanitizer import (
    KIND_NOTEBOOK,
    KIND_INIT_SCRIPT,
    KIND_JAR,
    KIND_WHL,
    KIND_REQUIREMENTS,
    KIND_CONFIG,
    dbx_to_aidp_path,
    sanitize_cluster_name,
    sanitize_workflow_name,
    validate_export_output,
)
from .validator import (
    cell_diff,
    cell_fingerprint,
    file_sha256,
    parse_notebook_bytes,
)


from .path_plan import file_target_path, file_upload_candidate, prepare_export_paths

# Shared AIDP dependency repository — the single location every migration
# artifact (JARs AND wheels, incl. the aidp_compat wheel) is staged into and
# resolved from. Checked FIRST, before any Databricks/proxy fetch. The old
# ``.../jars`` sub-directory was consolidated into ``.../libs``.
DEFAULT_AIDP_LIB_DIR = "/Workspace/migration-dependencies/libs"
# Back-compat alias: older code/tests referenced DEFAULT_AIDP_JAR_DIR.
DEFAULT_AIDP_JAR_DIR = DEFAULT_AIDP_LIB_DIR


# ─── Exit codes (the process's machine-readable API) ────────────────


class ExitCode(IntEnum):
    """Named, collision-free exit codes so CI/cron can react without parsing
    logs. Keep values stable — they are a public contract."""
    OK = 0
    BAD_ARGS = 2
    ROOT_JOB_FETCH_FAILED = 3
    STRICT_TASK_FAILED = 5
    STRICT_DEPS_FAILED = 6
    STRICT_TASK_UPLOAD_FAILED = 8
    MANDATORY_FILE_UPLOAD_FAILED = 9
    JOB_CREATE_FAILED = 10
    PROBE_FAILED = 11
    OPTIONAL_FILE_UPLOAD_FAILED = 12   # JAR soft-fail
    SPARK_TUNING_DROPPED = 14
    REQUIREMENTS_CONFLICT = 15
    JAR_DISCOVERY_FAILED = 16
    PATH_PLAN_FAILED = 17


# File kinds whose upload failure is RECOVERABLE (operator can attach later).
# Everything else is a mandatory dependency and must fail loud. Single source
# of truth so the upload + integrity paths can never disagree.
RECOVERABLE_FILE_KINDS = frozenset({"jar"})


# ─── Logging ────────────────────────────────────────────────────────


def tprint(*args, **kwargs):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}]", *args, **kwargs)


# ─── Phase 1 — Discover + download + validate ───────────────────────


def phase1_discover(dbx: DbxClient, root_job_id: str, max_depth: int,
                    parameter_bindings: Optional[Dict[str, str]] = None) -> Graph:
    tprint(f"Phase 1: discovering job {root_job_id} (max depth {max_depth})…")
    resolver = DependencyResolver(
        dbx, max_depth=max_depth, log=tprint,
        parameter_bindings=parameter_bindings,
    )
    graph = resolver.resolve(root_job_id)
    tprint(
        f"Phase 1: discovered "
        f"{len(graph.jobs)} jobs, "
        f"{len(graph.notebooks)} notebooks, "
        f"{len(graph.files)} files, "
        f"{len(graph.external_refs)} external refs"
    )
    return graph


def phase1_fetch_files(
    dbx: DbxClient,
    graph: Graph,
    lib_repo: Optional["_LibRepoIndex"] = None,
) -> None:
    """Download every non-external file referenced in graph (jars / init / whl).

    AIDP-first: when ``lib_repo`` is supplied, each jar/wheel is resolved against
    the shared AIDP dependency repository BEFORE any Databricks/proxy fetch. On a
    name match (exact version preferred, else highest), the DBX fetch is skipped
    and Phase 2 attaches the already-staged object.
    """
    for uri, node in graph.files.items():
        if node.content is not None or node.fetch_error is not None:
            continue
        if node.aidp_repo_path is not None:
            continue
        if lib_repo is not None and node.kind in ("jar", "whl"):
            match = lib_repo.resolve(uri, node.kind)
            if match:
                node.aidp_repo_path, node.aidp_repo_provenance = match
                tprint(
                    f"  [aidp-first] {node.kind} already staged; skipping DBX "
                    f"fetch: {uri} -> {node.aidp_repo_path} ({node.aidp_repo_provenance})"
                )
                continue
        try:
            content, src_type = dbx.fetch_file_uri(uri)
            if src_type == "external" and not content:
                # Proxy fetch disabled (or file:/ scheme) — record-only marker.
                node.fetch_error = "external_uri_skipped"
                node.error_detail = f"Cannot fetch {uri} (s3/http); record-only"
            else:
                # Includes proxy-fetched external jars: store the bytes so the
                # normal phase-2 upload path ships them to AIDP.
                node.content = content
                if src_type == "external":
                    tprint(f"  [file-fetch] external via proxy: {uri} ({len(content)} bytes)")
        except DbxExportError as e:
            node.fetch_error = e.reason
            node.error_detail = e.detail
            tprint(f"  [file-fetch-fail] {uri}: {e.reason}")


def phase1_discover_existing_cluster_libraries(dbx: DbxClient, graph: Graph) -> None:
    """Add file-backed libraries attached to referenced all-purpose clusters."""
    from dbx_export.dependency_resolver import FileNode, _classify_library_entry

    for job_id, job_node in graph.jobs.items():
        if not job_node.job_def:
            continue
        settings = job_node.job_def.get("settings") or job_node.job_def
        existing_ids = {
            task.get("existing_cluster_id")
            for task in settings.get("tasks", []) or []
            if task.get("existing_cluster_id")
        }
        for cluster_id in sorted(existing_ids):
            status = dbx.get_cluster_libraries(cluster_id) or {}
            context = f"existing_cluster:{cluster_id}"
            for item in status.get("library_statuses", []) or []:
                library = item.get("library") or {}
                pypi_package = (library.get("pypi") or {}).get("package")
                if pypi_package:
                    custom_repo = (library.get("pypi") or {}).get("repo")
                    if custom_repo:
                        tprint(
                            f"[existing-cluster][WARN] PyPI library {pypi_package!r} "
                            f"declares custom repo {custom_repo!r}; AIDP resolves "
                            "from its configured mirror only; availability must "
                            "be verified"
                        )
                    library_status = str(item.get("status") or "").upper()
                    if library_status != "INSTALLED":
                        tprint(
                            f"[existing-cluster][WARN] PyPI library {pypi_package!r} "
                            f"on {cluster_id} is {library_status or 'UNREPORTED'}; "
                            "not carried into target requirements or used for suppression"
                        )
                        continue
                    try:
                        from packaging.requirements import Requirement
                        from packaging.requirements import InvalidRequirement
                        Requirement(str(pypi_package))
                        graph.cluster_pypi_requirements.setdefault(context, set()).add(
                            str(pypi_package).strip()
                        )
                    except InvalidRequirement:
                        tprint(
                            f"[existing-cluster][WARN] unparseable PyPI library "
                            f"{pypi_package!r} on {cluster_id}; inference not suppressed"
                        )
                    continue
                kind, uri = _classify_library_entry(library)
                if not kind or not uri:
                    continue
                if uri in graph.files:
                    graph.files[uri].referenced_by.append(context)
                else:
                    graph.files[uri] = FileNode(
                        dbx_source=uri,
                        kind=kind,
                        referenced_by=[context],
                    )


# ─── Fix 7c (2026-07-25): extract pip deps from notebook %pip cells ──
#
# Rationale — a pilot-customer AutoRefit 2026-07-25 continuation:
#   DBX cluster spec has hand-curated requirements files (e.g. the ref cluster
#   c3aaf2d7 has /Workspace/dbc/initScript/AutoRefit_Feature_Generation_UNIT_
#   requirement.txt listing brotli, matplotlib, tqdm, slack_sdk, etc.). Our
#   dbx_export ALREADY captures cluster library.requirements entries (see
#   _classify_library_entry in dependency_resolver.py). What it MISSED was
#   notebooks that self-install via `%pip install X` inside a cell — those
#   packages aren't in the cluster spec but ARE needed at runtime.
#
#   Without this fix, `%pip install` inside a cell fires DURING the
#   migration run, which:
#     (a) Installs mid-run into /aidp/libraries, shadowing runtime defaults
#         and racing the consolidated-requirements install
#     (b) OR gets intercepted by our Fix 7f auto-install-on-ModuleNotFoundError
#         handler that installs via cluster-libraries API AFTER the cell
#         has already failed — expensive round trips and race conditions
#         with the running kernel (brotli C-extension case).
#
# What this does:
#   1. Scan every exported notebook for `%pip install X ...` and
#      `!pip install X ...` lines (skips magic-only lines like %run).
#   2. Aggregate unique package specs (handles version pins, extras, alt
#      syntax; skips flags like `-r` / `--upgrade`).
#   3. Synthesise a FileNode with kind="requirements" containing the
#      aggregated requirements.txt bytes. Registers under a synthetic
#      URI so phase2_upload_files uploads it and phase3 attaches it as a
#      cluster library (WORKSPACE_FILE requirements install).
#
# Effect on AutoRefit: matplotlib / brotli / tqdm / slack_sdk get bundled
# into the workflow's synthetic requirements file at export time, uploaded
# to AIDP, and attached as a WORKSPACE_FILE library on the fresh cluster.
# When the notebook's %pip cell fires at runtime, the packages are already
# installed → no-op → no failed install / no fix-loop churn.

# Also matches `!pip3` and `!python -m pip` / `!python3 -m pip`: all spell the
# same request, and a spelling missed here is a package the cluster never gets.
# Keep in sync with scripts/job_migrate.py::_PIP_INSTALL_LINE_RE.
_PIP_MAGIC_RE = re.compile(
    r'''^\s*(?:%pip|!pip3?|!python3?\s+-m\s+pip)\s+install\s+(.+?)$''',
    re.MULTILINE,
)

# ─── Fix 7c-ext (2026-07-28): subprocess / os.system pip install detection ─
#
# Rationale — Example_Monitoring_Generic_Job 2026-07-27 hang:
#   Customer notebook `001_library_importing.ipynb` had:
#       def import_libraries():
#         try:
#           from slack_sdk.webhook import WebhookClient
#           ...
#         except:
#           for lib in ["slack_sdk", "knockknock", "openpyxl"]:
#             try:
#               subprocess.check_call([sys.executable, '-m', 'pip', 'install', lib])
#             except: ...
#           import_libraries()   # RECURSION on failure
#
#   The `_PIP_MAGIC_RE` above only catches `%pip` / `!pip` — NOT subprocess-based
#   pip invocation. So slack_sdk/knockknock/openpyxl never made it into
#   pip_deps.txt, the cluster libs didn't have them, and the notebook's
#   runtime subprocess.check_call fired against AIDP (no outbound). Result:
#   17-minute cell hang, until manual cluster-library patch broke the loop.
#
#   These regexes catch the additional pip-install patterns so their package
#   specs are aggregated into the same pip_deps.txt as %pip installs, then
#   installed on the cluster at export time.

# subprocess.<method>([..., 'pip', 'install', <arg1>, <arg2>, ...])
# Matches: check_call, check_output, run, call, Popen
# Captures the FULL arg list content (between the outer brackets) so a
# downstream parser can extract literal string args after 'install'.
_SUBPROCESS_PIP_RE = re.compile(
    r"""subprocess\.(?:check_call|check_output|run|call|Popen)\s*\(\s*
        \[                                  # start of arg list
        ([^\[\]]*?                          # args (non-nested; nested lists rare)
        ['"]pip['"]\s*,\s*['"]install['"]   # anchor tokens
        [^\[\]]*?)                          # remaining args
        \]""",
    re.VERBOSE | re.DOTALL,
)

# os.system("pip install X Y")   or   os.system(f"pip install {var}")
# Captures the string content — parser then finds 'pip install' inside.
_OS_SYSTEM_PIP_RE = re.compile(
    r"""os\.system\s*\(\s*[fFrRbB]{0,2}['"]([^'"]*?pip\s+install\s+[^'"]+)['"]\s*\)""",
)

# pip.main(['install', ...]) — rare but seen in older notebooks. Two forms:
#   pip.main(['install', 'X'])                     — positional
#   pip.main(args=['install', 'X'])                — kwarg (P2-4 fix)
# The optional `args=` prefix accommodates both.
_PIP_MAIN_RE = re.compile(
    r"""pip\.main\s*\(\s*(?:args\s*=\s*)?\[\s*['"]install['"]\s*,\s*([^\[\]]*?)\]""",
    re.DOTALL,
)


def _normalize_shell_quoted_pip_spec(spec: str) -> str:
    """Remove one balanced shell-quote pair from a pip package spec.

    Quotes are valid command-shell grouping syntax in notebook commands such
    as ``%pip install "pydantic<2"``, but they are not valid when copied
    verbatim into a requirements file.  Preserve the package/version text and
    leave unmatched or mixed quotes untouched so malformed input is not
    silently rewritten into a different requirement.
    """
    normalized = (spec or "").strip()
    if (
        len(normalized) >= 2
        and normalized[0] == normalized[-1]
        and normalized[0] in {'"', "'"}
    ):
        return normalized[1:-1]
    return normalized


def _is_static_pkg_spec(spec: str) -> bool:
    """Reject specs that contain f-string interpolation placeholders — they
    aren't valid pip identifiers and would fail at cluster install time.
    Also reject empty, whitespace-only, and specs that look like URIs
    (http://, git+, etc. are legitimate pip specs but not our target here —
    keep this filter narrow to interpolations only)."""
    if not spec or not spec.strip():
        return False
    # `{name}` / `{name!r}` / nested braces → f-string interpolation
    if "{" in spec or "}" in spec:
        return False
    return True


def _parse_subprocess_pip_args(
    arg_list_text: str, *, findings=None, managed_names=None, barrier_state=None,
) -> List[str]:
    """Parse literal argv without dropping variables or re-splitting strings.

    Also used when a malformed cell prevents structural loop analysis. An
    incomplete argv is unresolved, never a collection of quoted fragments.
    """
    try:
        elements = ast.parse("[" + arg_list_text + "]", mode="eval").body.elts
        tail = _subprocess_pip_tail(elements)
        if tail is None:
            return []
        args = _pip_literal_strings(tail)
        if args is None:
            raise ValueError("nonliteral arguments")
    except (SyntaxError, ValueError):
        _pip_subprocess_unresolved(
            "cannot safely resolve subprocess pip argv", findings, barrier_state
        )
        return []
    return _requirements_from_pip_parse(
        parse_pip_tokens(args), findings=findings, managed_names=managed_names,
        barrier_state=barrier_state,
    )


def _pip_literal_strings(elements):
    if all(isinstance(e, ast.Constant) and isinstance(e.value, str) for e in elements):
        return [e.value for e in elements]
    return None


def _subprocess_pip_tail(elements):
    for i in range(len(elements) - 1):
        if _pip_literal_strings(elements[i:i + 2]) == ["pip", "install"]:
            return elements[i + 2:]
    return None


def _pip_subprocess_unresolved(reason, findings, barrier_state=None):
    import warnings

    if findings is not None:
        findings.append(("UNRESOLVED", reason))
    # Barrier state is structured source metadata, not a public finding.  The
    # findings list is consumed by the long-standing report contract.
    if barrier_state is not None:
        barrier_state["inference_barrier"] = True
    warnings.warn(f"pip UNRESOLVED: {reason}", UserWarning, stacklevel=3)


def _pip_binds_name(node, name):
    # Several Python bindings are stored as strings, not Name(Store) nodes.
    return ((isinstance(node, ast.Name) and node.id == name
             and isinstance(node.ctx, (ast.Store, ast.Del)))
            or (isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef,
                                  ast.ClassDef, ast.ExceptHandler,
                                  ast.MatchAs, ast.MatchStar)) and node.name == name)
            or (isinstance(node, ast.MatchMapping) and node.rest == name)
            or (isinstance(node, ast.alias) and (node.asname or node.name.split('.')[0]) == name)
            or (isinstance(node, (ast.Global, ast.Nonlocal)) and name in node.names))


def _pip_list_may_change(node, name, parents):
    """Permit only the witnessed standalone print of the known literal list.

    A string can become executable code: exec(f"items = {items} + ...")
    invalidated PR #273's blanket formatted-read exemption. Saved strings,
    arbitrary sinks, extra expressions and custom output streams therefore
    stay unresolved; this is not a general data-flow or side-effect analyzer.
    Only fresh function-local lists qualify: notebook/class/global/nonlocal
    bindings and lists captured by nested scopes remain unresolved. Python's
    compiler supplies the local/cell classification without executing source.
    The existing resolver still rejects intervening aliases, escapes/mutations.
    """
    formatted_reads = set()
    for statement in ast.walk(node):
        if not isinstance(statement, ast.Expr):
            continue
        call = statement.value
        if not (isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
                and call.func.id == "print" and len(call.args) == 1
                and not call.keywords and isinstance(call.args[0], ast.JoinedStr)):
            continue
        parts = call.args[0].values
        if not all(
            (isinstance(part, ast.Constant) and isinstance(part.value, str))
            or (isinstance(part, ast.FormattedValue)
                and isinstance(part.value, ast.Name) and part.value.id == name
                and isinstance(part.value.ctx, ast.Load)
                and (part.format_spec is None or (
                    isinstance(part.format_spec, ast.JoinedStr)
                    and all(isinstance(value, ast.Constant) and isinstance(value.value, str)
                            for value in part.format_spec.values))))
            for part in parts
        ):
            continue
        scope = statement
        while scope in parents:
            scope = parents[scope]
            if isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef,
                                  ast.ClassDef, ast.Lambda)):
                break
        if not isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if any(_pip_binds_name(part, "print")
               or (isinstance(part, ast.arg) and part.arg == "print")
               for part in ast.walk(scope)):
            continue
        root = scope
        while root in parents:
            root = parents[root]
        try:
            codes = [compile(root, "<pip diagnostic scope>", "exec",
                             flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT, dont_inherit=True)]
        except SyntaxError:
            # Invalid surrounding Python cannot establish an isolated local.
            continue
        first_line = min([scope.lineno] + [d.lineno for d in scope.decorator_list])
        isolated = False
        while codes:
            code = codes.pop()
            if code.co_name == scope.name and code.co_firstlineno == first_line:
                isolated = name in code.co_varnames and name not in code.co_cellvars
                break
            codes.extend(c for c in code.co_consts if isinstance(c, CodeType))
        if not isolated:
            continue
        formatted_reads.update(part.value for part in parts
                               if isinstance(part, ast.FormattedValue))
    return any(
        _pip_binds_name(part, name)
        or (isinstance(part, ast.Name) and part.id == name
            and part not in formatted_reads)
        for part in ast.walk(node)
    )


def _pip_loop_items(loop, parents):
    """Resolve only a literal iterable or an unambiguous preceding assignment.

    Never evaluate expressions, follow aliases, cross function/class scopes,
    or infer a binding through a statement that may mutate/alias the iterable.
    """
    value = loop.iter
    if isinstance(value, ast.Name):
        name, child = value.id, loop
        value = None
        while child in parents:
            parent = parents[child]
            for _, field in ast.iter_fields(parent):
                if not isinstance(field, list) or child not in field:
                    continue
                for previous in reversed(field[:field.index(child)]):
                    if (isinstance(previous, ast.Assign) and len(previous.targets) == 1
                            and isinstance(previous.targets[0], ast.Name)
                            and previous.targets[0].id == name):
                        value = previous.value
                        break
                    if _pip_list_may_change(previous, name, parents):
                        return None
            if value is not None or isinstance(
                    parent, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
                break
            # A compound statement's header or another suite may change the
            # binding (walrus in an if, assignment before an except, etc.).
            # Do not infer a value across that control-flow boundary.
            for _, field in ast.iter_fields(parent):
                nodes = field if isinstance(field, list) else [field]
                if child in nodes:
                    continue
                if any(isinstance(part, ast.AST) and _pip_list_may_change(part, name, parents)
                       for part in nodes):
                    return None
            if isinstance(parent, ast.ExceptHandler) and parent.name == name:
                return None
            child = parent
    if isinstance(value, (ast.List, ast.Tuple)):
        return _pip_literal_strings(value.elts)
    return None


def _pip_match_is_active(match, active_starts, *, offset=0) -> bool:
    """Check the command/name token, not its quoted arguments or leading space.

    None preserves the existing raw-text extraction for shell init scripts;
    notebook consumers always supply Python lexical context.
    """
    start = match.start() + len(match[0]) - len(match[0].lstrip())
    return active_starts is None or offset + start in active_starts


def _python_pip_active_starts(
    source: str, *, context: str, findings=None, barrier_state=None
) -> Set[int]:
    """Locate executable tokens without compiling notebook magic syntax.

    Tokenization recognizes comments, escaped quotes and multiline strings
    while allowing the %/! tokens used by notebook commands. Keep original
    offsets: regexes still read unmodified arguments from the original source.
    """
    import io
    import tokenize
    import warnings

    # Match readline/tokenize's physical lines, not splitlines()' additional
    # Unicode/form-feed boundaries inside strings.
    line_offsets = [0] + [match.end() for match in re.finditer("\n", source)]
    active = set()
    try:
        for token in tokenize.generate_tokens(io.StringIO(source).readline):
            if token.type == tokenize.ERRORTOKEN and token.string in ("'", '"'):
                raise tokenize.TokenError("unterminated string", token.start)
            if token.type in (tokenize.NAME, tokenize.OP, tokenize.ERRORTOKEN):
                row, column = token.start
                active.add(line_offsets[row - 1] + column)
    except (tokenize.TokenError, SyntaxError) as exc:
        # Never fall back to scanning raw text: that reclassifies documentation
        # as dependencies. Retain only positions already classified and expose
        # the incomplete scan, rather than silently losing unknown commands.
        row = exc.lineno if isinstance(exc, SyntaxError) else exc.args[1][0]
        reason = (f"source scan incomplete at line {row} ({type(exc).__name__}); "
                  "unclassified install commands require manual review")
        if findings is not None:
            findings.append(("UNRESOLVED", reason))
        if barrier_state is not None:
            barrier_state["inference_barrier"] = True
        warnings.warn(f"pip UNRESOLVED: {context}: {reason}", UserWarning, stacklevel=3)
    return active


def _extract_subprocess_pip_specs(
    src: str, *, active_starts=None, findings=None, managed_names=None,
    barrier_state=None,
) -> List[str]:
    """Reconstruct each supported subprocess argv before shared pip parsing.

    Loop iterations are separate commands, NOT adjacent pip arguments. Keep
    option values in position and analyze each call once (no literal/loop
    double scan). Unknown bindings are reported rather than guessed.
    """
    specs: List[str] = []
    matches = [m for m in _SUBPROCESS_PIP_RE.finditer(src)
               if _pip_match_is_active(m, active_starts)]
    if not matches:
        return specs

    # Python ASTs do not accept notebook magics. Mask only lexically active
    # magic lines in this analysis copy; never alter notebook source or strings.
    active = active_starts
    if active is None:
        active = _python_pip_active_starts(
            src, context="subprocess scan", findings=findings,
            barrier_state=barrier_state,
        )
    lines, offset = [], 0
    for line in src.split("\n"):
        stripped = line.lstrip(" \t")
        indent = len(line) - len(stripped)
        if stripped.startswith(("%", "!")) and offset + indent in active:
            lines.append(line[:indent] + "pass")
        else:
            lines.append(line)
        offset += len(line) + 1
    try:
        tree = ast.parse("\n".join(lines))
    except SyntaxError:
        # Init scripts can embed Python in a literal (quoted-delimiter)
        # heredoc. Parse each such body in its own scope, not the surrounding
        # shell or an adjacent heredoc. Unquoted delimiters allow shell
        # expansion and are deliberately not treated as literal Python.
        heredocs = []
        if active_starts is None:
            heredocs = list(re.finditer(
                r"""(?m)^[ \t]*python(?:3(?:\.\d+)?)?[ \t]+-[ \t]+<<[ \t]*"""
                r"""(?P<quote>['"])(?P<end>[A-Za-z_]\w*)(?P=quote)[ \t]*\n"""
                r"""(?P<body>[\s\S]*?)^(?P=end)$""", src))
            for heredoc in heredocs:
                specs.extend(_extract_subprocess_pip_specs(
                    heredoc.group("body"), findings=findings,
                    managed_names=managed_names, barrier_state=barrier_state))
        # Preserve standalone literal commands even in an otherwise invalid
        # cell, but never reconstruct a loop from partial syntax.
        for match in matches:
            if any(h.start("body") <= match.start() < h.end("body") for h in heredocs):
                continue
            if _pip_match_is_active(match, active):
                specs.extend(_parse_subprocess_pip_args(
                    match.group(1), findings=findings,
                    managed_names=managed_names, barrier_state=barrier_state))
        return specs

    parents = {child: parent for parent in ast.walk(tree)
               for child in ast.iter_child_nodes(parent)}
    for call in sorted((n for n in ast.walk(tree) if isinstance(n, ast.Call)),
                       key=lambda n: (n.lineno, n.col_offset)):
        func = call.func
        if not (isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name)
                and func.value.id == "subprocess"
                and func.attr in {"check_call", "check_output", "run", "call", "Popen"}
                and call.args and isinstance(call.args[0], ast.List)):
            continue
        tail = _subprocess_pip_tail(call.args[0].elts)
        if tail is None:
            continue
        # Scope and real AST containment replace the old 500-character window.
        loops, node = [], call
        while node in parents:
            parent = parents[node]
            if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef,
                                   ast.ClassDef, ast.Lambda)):
                break
            if isinstance(parent, (ast.For, ast.AsyncFor)):
                # A for-else suite is not an iteration of the loop.
                if node not in parent.body:
                    break
                loops.append(parent)
            node = parent
        if any(_pip_loop_items(loop, parents) == [] for loop in loops):
            continue
        literal = _pip_literal_strings(tail)
        if literal is not None:
            specs.extend(_requirements_from_pip_parse(
                parse_pip_tokens(literal), findings=findings,
                managed_names=managed_names, barrier_state=barrier_state))
            continue

        loop = loops[0] if len(loops) == 1 and isinstance(loops[0], ast.For) else None
        items = _pip_loop_items(loop, parents) if loop is not None else None
        variable = loop.target.id if loop is not None and isinstance(loop.target, ast.Name) else None
        # A body can reassign the loop target or mutate/alias the iterable.
        # Those shapes need data-flow analysis, not guessed package values.
        if loop is not None and variable is not None:
            for statement in loop.body:
                if (any(_pip_binds_name(n, variable) for n in ast.walk(statement))
                        or (isinstance(loop.iter, ast.Name)
                            and _pip_list_may_change(statement, loop.iter.id, parents))):
                    items = None
                    break
        supported = items is not None and variable is not None and all(
            (isinstance(arg, ast.Constant) and isinstance(arg.value, str))
            or (isinstance(arg, ast.Name) and arg.id == variable) for arg in tail)
        if not supported:
            _pip_subprocess_unresolved(
                f"cannot safely resolve subprocess pip argv at line {call.lineno}; "
                "requires literal arguments and an unmodified literal-list loop",
                findings, barrier_state)
            continue
        parsed = [parse_pip_tokens([
            arg.value if isinstance(arg, ast.Constant) else item for arg in tail
        ]) for item in items]
        # Do not salvage guessed later dependencies from a malformed loop
        # (e.g. ['-r', 'requirements.txt']). Keep all its findings visible.
        extracted = []
        for result in parsed:
            extracted.extend(_requirements_from_pip_parse(
                result, findings=findings, managed_names=managed_names,
                barrier_state=barrier_state))
        if not any(result.unresolved for result in parsed):
            specs.extend(extracted)
    return specs


def _extract_os_system_pip_specs(
    src: str, *, active_starts=None, findings=None, managed_names=None,
    barrier_state=None,
) -> List[str]:
    """os.system("pip install X Y") → ['X', 'Y']."""
    specs: List[str] = []
    for m in _OS_SYSTEM_PIP_RE.finditer(src):
        if not _pip_match_is_active(m, active_starts):
            continue
        # Reuse the shared pip-args parser via a leading 'pip install' token
        # stripped off; only pass the packages.
        text = m.group(1)
        # Take everything after 'pip install' (which the regex already matched).
        pip_idx = text.lower().find("pip install")
        if pip_idx < 0:
            continue
        args_text = text[pip_idx + len("pip install"):].strip()
        specs.extend(_parse_pip_install_line(
            args_text, findings=findings, managed_names=managed_names,
            barrier_state=barrier_state))
    return specs


def _extract_pip_main_specs(
    src: str, *, active_starts=None, findings=None, managed_names=None,
    barrier_state=None,
) -> List[str]:
    """pip.main(['install', 'X', ...]) → ['X', ...]."""
    specs: List[str] = []
    for m in _PIP_MAIN_RE.finditer(src):
        if not _pip_match_is_active(m, active_starts):
            continue
        # Content is the args AFTER 'install',' — parse quoted literals.
        args = re.findall(r"""['"]([^'"]*)['"]""", m.group(1))
        specs.extend(_requirements_from_pip_parse(
            parse_pip_tokens(args), findings=findings,
            managed_names=managed_names, barrier_state=barrier_state))
    return specs


def _requirements_from_pip_parse(
    parsed, *, findings=None, managed_names=None, barrier_state=None,
) -> List[str]:
    """Exporter projection: retain complete specs and report skipped forms."""
    import warnings

    for finding in parsed.unresolved:
        if findings is not None:
            findings.append(("UNRESOLVED", finding))
        warnings.warn(f"pip UNRESOLVED: {finding}", UserWarning, stacklevel=3)
    if getattr(parsed, "inference_barrier", False):
        barrier = (
            "pip provider form is unresolved; inferred imports suppressed for "
            "this source"
        )
        if barrier_state is not None:
            barrier_state["inference_barrier"] = True
        warnings.warn(f"pip BARRIER: {barrier}", UserWarning, stacklevel=3)
    for notice in parsed.notices:
        if findings is not None:
            findings.append(("NOTE", notice))
        warnings.warn(f"pip NOTE: {notice}", UserWarning, stacklevel=3)
    if managed_names is not None:
        managed_names.update(parsed.managed_names)
    return parsed.requirements


def _parse_pip_install_line(
    args_text: str, *, findings=None, managed_names=None, barrier_state=None,
) -> List[str]:
    """Shared argument parser; preserve pins/extras/markers for requirements.

    Requirements files can also retain complete direct references and wheel
    paths. Never split those references into a bare name and URL.
    """
    return _requirements_from_pip_parse(
        parse_pip_arguments(args_text), findings=findings,
        managed_names=managed_names, barrier_state=barrier_state,
    )


def _notebook_cell_pip_specs(
    cell: dict, *, context="notebook cell", findings=None, managed_names=None,
    barrier_state=None,
):
    """Yield (requirement, non_magic) from executable notebook source only.

    Both initial discovery and cluster consolidation must use this boundary.
    Filtering only the first pass still installs examples via the second one.
    This is read-only: never mask or rewrite the customer's notebook contents.
    """
    if cell.get("cell_type") != "code":
        return
    source = cell.get("source", "")
    if isinstance(source, list):
        source = "".join(source)
    # Databricks documentation magics may still be stored as code cells.
    # Ignore only leading blank/comment annotations, never real code or strings.
    first = next((line.strip() for line in source.splitlines()
                  if line.strip() and not line.lstrip().startswith("#")), "")
    if first.split(maxsplit=1)[:1] in (["%md"], ["%md-sandbox"]):
        return
    if not any(pattern.search(source) for pattern in (
            _PIP_MAGIC_RE, _SUBPROCESS_PIP_RE, _OS_SYSTEM_PIP_RE, _PIP_MAIN_RE)):
        return
    active_starts = _python_pip_active_starts(
        source, context=context, findings=findings, barrier_state=barrier_state
    )
    for match in _PIP_MAGIC_RE.finditer(source):
        if _pip_match_is_active(match, active_starts):
            for spec in _parse_pip_install_line(
                match.group(1), findings=findings,
                managed_names=managed_names, barrier_state=barrier_state,
            ):
                yield spec, False
    for extractor in (_extract_subprocess_pip_specs,
                      _extract_os_system_pip_specs, _extract_pip_main_specs):
        for spec in extractor(
            source, active_starts=active_starts, findings=findings,
            managed_names=managed_names, barrier_state=barrier_state,
        ):
            yield spec, True


# ── pip spec conflict resolution ──────────────────────────────────────────
# Databricks %pip is notebook-scoped, so two notebooks can pin different
# versions of the same package. Aggregated into ONE AIDP cluster-library
# requirements file they must be mutually consistent — two hard pins of the same
# package (xgboost==1.7.6 + xgboost==2.0.3) make pip's resolver fail
# ("ResolutionImpossible"). Policy (confirmed with operator, 2026-08-05): keep
# the HIGHEST pinned version, drop the rest, and report every collapse so the
# choice is never silent. Edit the source notebooks if a lower version is truly
# required.
_PIP_NAME_RE = re.compile(r'^\s*([A-Za-z0-9][A-Za-z0-9._-]*)\s*(?:\[[^\]]*\])?\s*(.*)$')
_PIP_EXACT_RE = re.compile(r'^==\s*([A-Za-z0-9][A-Za-z0-9._!+-]*)')


def _pip_spec_name_version(spec: str):
    """Return (canonical_name, exact_version) for a pip spec.

    canonical_name is PEP 503-normalized (lowercase, runs of ._- -> -) for
    GROUPING only — the original spec text is preserved in output. exact_version
    is set only for '==' pins. Returns (None, None) for specs we won't touch
    (flags, URLs, git+, 'pkg @ url', editable installs) so they pass through
    verbatim."""
    s = (spec or "").strip()
    if (not s or s.startswith("-") or "://" in s
            or s.startswith("git+") or " @ " in s):
        return None, None
    m = _PIP_NAME_RE.match(s)
    if not m:
        return None, None
    name = re.sub(r"[._-]+", "-", m.group(1).lower())
    em = _PIP_EXACT_RE.match(m.group(2).strip())
    return name, (em.group(1) if em else None)


def _pip_version_key(v: str):
    """Sortable version key that never raises when comparing mixed inputs.

    A rank prefix keeps parseable and unparseable keys from ever being compared
    directly: ``max()`` over a group that mixes a real version and an
    unparseable one (e.g. ``1.3.0`` vs the wildcard-derived ``1.2.``) used to hit
    ``TypeError: '>' not supported between 'Version' and 'tuple'``. With the
    rank, a parseable version (rank 1) always outranks an unparseable one
    (rank 0), and only same-rank keys — Version vs Version, or tuple vs tuple —
    are ever compared element-wise.
    """
    try:
        from packaging.version import Version
        return (1, Version(v))
    except Exception:
        return (0, tuple((int(p), "") if p.isdigit() else (0, p)
                         for p in re.split(r"[._-]+", v)))


def _resolve_pip_spec_conflicts(specs):
    """Collapse the spec set to one entry per package so a single cluster-level
    requirements file installs cleanly.

    Returns (resolved_sorted, conflicts, dedups):
      resolved_sorted : list[str]  — one spec per package + untouched passthrough
                                     (sorted → byte-identical across re-runs)
      conflicts       : list[dict] — {name, versions:[...], chosen} for >=2 distinct == pins
      dedups          : list[dict] — {name, specs:[...], chosen} for bare+pin / duplicate collapses
    """
    from collections import defaultdict
    grouped = defaultdict(list)      # canonical name -> [original specs]
    passthrough = []                 # specs with no parseable name (urls, flags…)
    for spec in specs:
        name, _ = _pip_spec_name_version(spec)
        if name is None:
            passthrough.append(spec)
        else:
            grouped[name].append(spec)

    resolved, conflicts, dedups = [], [], []
    for name, group in grouped.items():
        uniq = sorted(set(group))
        if len(uniq) == 1:
            resolved.append(uniq[0])
            continue
        pinned = [(s, _pip_spec_name_version(s)[1]) for s in uniq]
        pinned = [(s, v) for s, v in pinned if v]
        chosen = (max(pinned, key=lambda sv: _pip_version_key(sv[1]))[0]
                  if pinned else uniq[-1])
        resolved.append(chosen)
        distinct = sorted({v for _, v in pinned})
        if len(distinct) >= 2:
            # Show the ORIGINAL version text in the audit — the parsed key
            # truncates a wildcard (`1.2.*` -> `1.2.`), which misleads the
            # operator reading the warning/header.
            def _ver_disp(s):
                m = re.search(r'(?:===|==)\s*(.+)$', s)
                return m.group(1).strip() if m else s
            versions = sorted({_ver_disp(s) for s, _ in pinned})
            conflicts.append({"name": name, "versions": versions, "chosen": chosen})
        else:
            dedups.append({"name": name, "specs": uniq, "chosen": chosen})
    conflicts.sort(key=lambda c: c["name"])
    dedups.sort(key=lambda d: d["name"])
    return sorted(resolved + passthrough), conflicts, dedups


def discover_notebook_pip_deps(graph: Graph, export_output: str, root_job_id: str,
                               report=None,
                               compatibility_policy_file: Optional[str] = None,
                               ) -> Optional[str]:
    """Scan notebooks for `%pip install` / `!pip install` and emit a
    synthetic requirements FileNode. Returns the synthetic URI if created,
    None if no pip installs were found.

    The URI is a fixed identifier — later phases key on it to distinguish
    the generated file from workspace-referenced ones.
    """
    from dbx_export.dependency_resolver import FileNode

    seen_specs: Set[str] = set()
    referenced_by: List[str] = []

    for dbx_path, nb_node in graph.notebooks.items():
        if not nb_node.is_notebook or nb_node.nb_dict is None:
            continue
        for i, cell in enumerate(nb_node.nb_dict.get("cells", [])):
            findings = []
            for spec, non_magic in _notebook_cell_pip_specs(
                    cell, context=f"{dbx_path}:cell{i}", findings=findings):
                if spec and spec not in seen_specs:
                    seen_specs.add(spec)
                    suffix = "[non-magic]" if non_magic else ""
                    referenced_by.append(f"{dbx_path}:cell{i}{suffix}")
            # Record even when this cell yielded no requirements. Python's
            # warning filters must not determine what survives in the report.
            if report is not None:
                report.add_pip_findings(dbx_path, i, findings)

    if not seen_specs:
        return None

    # Synthetic URI — unique per root job. Uses a path form so
    # `posixpath.basename` produces a clean filename (colons in the URI
    # would land inside the destination filename on AIDP → API quirks).
    # The `/dbx_export_generated/` prefix disambiguates from workspace refs.
    _safe_job = re.sub(r'[^A-Za-z0-9_.-]', '_', str(root_job_id))
    synthetic_uri = f"/dbx_export_generated/pip_deps_{_safe_job}.txt"
    # Collapse to one spec per package so the aggregated cluster-library
    # requirements file installs cleanly (see _resolve_pip_spec_conflicts).
    resolved_specs, conflicts, dedups = _resolve_pip_spec_conflicts(seen_specs)

    # Runtime-provided packages (compatibility policy) are unpinned at the
    # LATER consolidation stage regardless of what wins here — reporting
    # "kept highest (mlflow==2.21.0)" for a pin that is then dropped misled
    # the operator (live export, 2026-09-10). Collapse them to the bare name
    # already at this stage so the log and the pip_deps file tell one story.
    try:
        from dbx_export.requirements import load_compatibility_policy
        # Same policy SELECTION as the consolidation stage: explicit YAML
        # (paths.compatibility_policy_file) > env > repo default. Loading the
        # no-arg default here while stage 2 used the customer's file meant a
        # YAML-only policy reached one stage but not the other — the historic
        # config-routing class (PR #88 review, P2, 2026-09-10).
        _policy = load_compatibility_policy(compatibility_policy_file)
        _runtime_provided = _policy.runtime_provided
        _protected = dict(_policy.protected_packages)
    except Exception as _pe:
        tprint(f"  [Fix 7c][WARN] compatibility policy unavailable ({str(_pe)[:80]}) "
               "— runtime-provided/protected handling deferred to consolidation")
        _runtime_provided = frozenset()
        _protected = {}

    def _rebuild_policy_spec(spec: str, pin: str = "") -> Optional[str]:
        """Rebuild a pip spec with the policy version, preserving the FULL
        PEP 508 grammar — name, extras, AND environment markers. Incremental
        regex surgery kept dropping one field per review round (extras in
        round 3, markers in round 4 — `pkg==X; python_version < "3.10"` must
        stay conditional); the real parser handles the whole grammar at once.
        Returns None when the spec isn't a plain requirement (URLs/flags) —
        caller leaves those untouched for consolidation to police."""
        try:
            from packaging.requirements import Requirement
            _req = Requirement(spec)
        except Exception:
            return None
        if _req.url:
            return None
        _extras = "[" + ",".join(sorted(_req.extras)) + "]" if _req.extras else ""
        _marker = f'; {_req.marker}' if _req.marker else ""
        return f"{_req.name}{_extras}{pin}{_marker}"

    if _runtime_provided:
        _rp_conflicts = [c for c in conflicts if c["name"] in _runtime_provided]
        conflicts = [c for c in conflicts if c["name"] not in _runtime_provided]
        _seen_rp = set()
        for _i, _spec in enumerate(resolved_specs):
            _name, _ = _pip_spec_name_version(_spec)
            if _name in _runtime_provided:
                # Unpin the VERSION only — extras AND markers survive
                # (mlflow[databricks]==2.14.2; python_version < "3.10" →
                # mlflow[databricks]; python_version < "3.10").
                _bare = _rebuild_policy_spec(_spec)
                if _bare is not None and _spec != _bare:
                    resolved_specs[_i] = _bare
                    _seen_rp.add((_name, _spec))
        for _name, _spec in sorted(_seen_rp):
            _versions = next((", ".join(c["versions"]) for c in _rp_conflicts
                              if c["name"] == _name), _spec)
            tprint(f"  [Fix 7c][runtime-provided] {_name}: dropped pin(s) "
                   f"({_versions}) — AIDP runtime ships this package; the "
                   "runtime version wins (config/aidp_compatibility_policy.yaml)")
        resolved_specs = sorted(set(resolved_specs))
    if _protected:
        # Mirror the consolidation stage for protected packages too, so the
        # pip_deps file and the final consolidated file never disagree
        # (PR #88 review, major 3 — the mlflow one-story rule applies here
        # equally). The replacement is reported per package, never silent.
        _prot_conflicts = [c for c in conflicts if c["name"] in _protected]
        conflicts = [c for c in conflicts if c["name"] not in _protected]
        _replaced = []
        for _i, _spec in enumerate(resolved_specs):
            _name, _ = _pip_spec_name_version(_spec)
            if _name in _protected:
                # Replace the VERSION with the protected pin — extras AND
                # markers survive (a conditional requirement stays
                # conditional; PR #88 review round 4).
                _pin = str(_protected[_name].specifier)
                _target = _rebuild_policy_spec(_spec, pin=_pin)
                if _target is not None and _spec != _target:
                    resolved_specs[_i] = _target
                    _replaced.append((_name, _spec, _target))
        for _name, _old, _new in sorted(_replaced):
            # Name EVERY dropped pin, incl. the conflict losers highest-wins
            # already removed upstream — the stage-1 log must never lose a pin
            # that the consolidation header names (adversarial-verify finding).
            _all_dropped = sorted({_old} | {
                f"{_name}=={v}" for c in _prot_conflicts
                if c["name"] == _name for v in c["versions"]})
            tprint(f"  [Fix 7c][protected] {_name}: workload pin(s) "
                   f"{', '.join(_all_dropped)} replaced by {_new!r} — AIDP "
                   "protected runtime package (config/aidp_compatibility_policy.yaml)")
        resolved_specs = sorted(set(resolved_specs))

    # Sorted so re-runs produce byte-identical content (deterministic upload).
    header = [
        "# Auto-generated by dbx_export Fix 7c (2026-07-25).",
        f"# Aggregated %pip install specs from workflow {root_job_id}.",
        "# Regenerated on every export — edit the source notebooks, not this file.",
    ]
    for c in conflicts:
        header.append(
            f"# CONFLICT: {c['name']} pinned to {', '.join(c['versions'])} across "
            f"notebooks — kept highest ({c['chosen']}).")
    lines = header + [""] + resolved_specs
    content = ("\n".join(lines) + "\n").encode("utf-8")

    graph.files[synthetic_uri] = FileNode(
        dbx_source=synthetic_uri,
        kind="requirements",
        referenced_by=referenced_by[:20],  # cap to keep report readable
        content=content,       # pre-populated → phase1_fetch_files skips it
        fetch_error=None,
    )
    tprint(f"[Fix 7c] extracted {len(seen_specs)} unique %pip install spec(s) "
           f"across {len(referenced_by)} notebook cell reference(s); resolved to "
           f"{len(resolved_specs)} package line(s)")
    for c in conflicts:
        # This is the WORKFLOW-LEVEL %pip aggregate (the pip_deps file). The
        # authoritative per-cluster winner can differ (highest of the notebooks
        # on each cluster) and is reported separately as
        # `requirements-conflict: <cluster>: …` by consolidate_cluster_requirements.
        # Labelled "aggregate" so this line is not read as the delivered value.
        msg = (f"workflow %pip aggregate: {c['name']} pinned to "
               f"{', '.join(c['versions'])} — kept highest ({c['chosen']}); "
               f"per-cluster requirements may differ (see requirements-conflict "
               f"lines). Edit source notebooks if a different version is required.")
        tprint(f"  [Fix 7c][conflict][WARN] {msg}")
        if report is not None:
            report.errors.append(f"WARN pip-conflict (aggregate): {msg}")
    for d in dedups:
        tprint(f"  [Fix 7c][dedup] {d['name']}: {', '.join(d['specs'])} -> {d['chosen']}")
    for _s in resolved_specs[:10]:
        tprint(f"           - {_s}")
    if len(resolved_specs) > 10:
        tprint(f"           ... and {len(resolved_specs) - 10} more")
    return synthetic_uri


def _task_cluster_context(job_id: str, task: dict) -> Optional[str]:
    """Return the stable install boundary used by a Databricks task."""
    if task.get("job_cluster_key"):
        return f"job:{job_id}/job_cluster:{task['job_cluster_key']}"
    if task.get("existing_cluster_id"):
        return f"existing_cluster:{task['existing_cluster_id']}"
    if task.get("new_cluster"):
        return f"job:{job_id}/task_cluster:{task.get('task_key', '?')}"
    return None


def _notebooks_reachable_from(graph: Graph, roots: Set[str]) -> Set[str]:
    """Return task notebooks plus their transitive notebook dependencies."""
    reachable = set(roots)
    changed = True
    while changed:
        changed = False
        for path, node in graph.notebooks.items():
            if path in reachable:
                continue
            parents = {
                reference.split(":", 1)[1]
                if reference.startswith(("resource-of:", "sibling-of:"))
                else reference
                for reference in node.referenced_by
            }
            if parents.intersection(reachable):
                reachable.add(path)
                changed = True
    return reachable


def _pip_sources_for_notebooks(graph: Graph, notebook_paths: Set[str], *, report=None) -> "list[RequirementSource]":
    """One RequirementSource per notebook (named `notebook %pip: <path>`).

    Emitting one source per notebook — rather than merging every notebook into a
    single "notebook pip commands" source — is what lets a version-conflict WARN
    name the specific notebook that pinned the losing version, so "Edit source if
    a different version is required" is actionable. Raw specs only; version /
    extras / marker resolution is centralized in consolidate_requirement_sources.
    """
    out: "list[RequirementSource]" = []
    for path in sorted(notebook_paths):
        node = graph.notebooks.get(path)
        if not node or not node.is_notebook or node.nb_dict is None:
            continue
        specs: Set[str] = set()
        managed_names: Set[str] = set()
        unresolved_passthrough = False
        inference_barrier = False
        barrier_state = {}
        for i, cell in enumerate(node.nb_dict.get("cells", [])):
            findings = []
            specs.update(spec for spec, _ in _notebook_cell_pip_specs(
                cell, context=f"{path}:cell{i}", findings=findings,
                managed_names=managed_names, barrier_state=barrier_state))
            if report is not None:
                report.add_pip_findings(path, i, findings)
            inference_barrier = inference_barrier or barrier_state.get(
                "inference_barrier", False
            )
            unresolved_passthrough = unresolved_passthrough or inference_barrier
        if specs or managed_names or unresolved_passthrough or inference_barrier:
            out.append(RequirementSource(
                f"notebook %pip: {path}",
                "\n".join(sorted(specs)),
                managed_names=frozenset(managed_names),
                unresolved_passthrough=unresolved_passthrough,
                inference_barrier=inference_barrier,
            ))
    return out


def _pypi_specs(libraries: list) -> Set[str]:
    """Extract Databricks ``libraries[].pypi.package`` requirements."""
    specs: Set[str] = set()
    for library in libraries or []:
        package = (library.get("pypi") or {}).get("package")
        if package and str(package).strip():
            specs.add(str(package).strip())
    return specs


def _artifact_distribution_name(value: Any) -> Optional[str]:
    """Return the declared distribution supplied by one WHL/EGG artifact."""
    raw = str(value or "").strip()
    if not raw:
        return None
    basename = raw.split("?", 1)[0].split("#", 1)[0].replace("\\", "/").rsplit("/", 1)[-1]
    if basename.lower().endswith(".whl"):
        try:
            name, _, _, _ = parse_wheel_filename(basename)
        except InvalidWheelFilename:
            return None
        return canonicalize_name(name)
    if basename.lower().endswith(".egg"):
        stem = basename[:-4]
        match = re.match(r"^(.+?)-(?=\d)", stem)
        if match:
            return canonicalize_name(match.group(1))
    return None


def _artifact_distribution_names(libraries: list) -> Set[str]:
    names: Set[str] = set()
    for library in libraries or []:
        for key in ("whl", "egg"):
            name = _artifact_distribution_name(library.get(key))
            if name:
                names.add(name)
    return names


def _pip_source_for_init_scripts(
    graph: Graph, accepted_refs: Set[str]
) -> RequirementSource:
    """Extract pip commands embedded in init scripts for one cluster context."""
    specs: Set[str] = set()
    managed_names: Set[str] = set()
    unresolved_passthrough = False
    inference_barrier = False
    barrier_state = {}
    accepted_init_uris = {
        uri for uri, node in graph.files.items()
        if node.kind == "init_script" and accepted_refs.intersection(node.referenced_by)
    }
    for uri, node in graph.files.items():
        if node.kind != "init_script" or node.content is None:
            continue
        if not accepted_refs.intersection(node.referenced_by):
            continue
        source = node.content.decode("utf-8", errors="replace")
        findings = []
        for match in _PIP_MAGIC_RE.finditer(source):
            specs.update(_parse_pip_install_line(
                match.group(1), findings=findings, managed_names=managed_names,
                barrier_state=barrier_state))
        for extractor in (
            _extract_subprocess_pip_specs,
            _extract_os_system_pip_specs,
            _extract_pip_main_specs,
        ):
            specs.update(extractor(
                source, findings=findings, managed_names=managed_names,
                barrier_state=barrier_state,
            ))
        # Shell init scripts normally use plain ``pip install`` rather than a
        # notebook magic. Keep this deliberately line-scoped.
        for match in re.finditer(
            r"(?m)^\s*(?:python\d*\s+-m\s+)?"
            r"(?:/databricks/python/bin/)?pip3?\s+install\s+(.+)$",
            source,
        ):
            specs.update(_parse_pip_install_line(
                match.group(1), findings=findings, managed_names=managed_names,
                barrier_state=barrier_state))
        inference_barrier = inference_barrier or barrier_state.get(
            "inference_barrier", False
        )
        unresolved_passthrough = unresolved_passthrough or inference_barrier
    # Init-script wheel/egg providers are managed even when no pip command is
    # present in the script. Feed them to the audit before import inference so
    # an inferred package cannot duplicate a declared provider.
    for uri, node in graph.files.items():
        if node.kind not in ("whl", "egg"):
            continue
        if not any(
            ref.startswith("init_script:") and ref.split(":", 1)[1] in accepted_init_uris
            for ref in node.referenced_by
        ):
            continue
        name = _artifact_distribution_name(node.aidp_repo_path or uri)
        if name:
            managed_names.add(name)
    return RequirementSource(
        "init-script pip commands",
        "\n".join(sorted(specs)),
        managed_names=frozenset(managed_names),
        unresolved_passthrough=unresolved_passthrough,
        inference_barrier=inference_barrier,
    )


def consolidate_cluster_requirements(
    graph: Graph,
    root_job_id: str,
    target_runtime_requirements_file: Optional[str] = None,
    compatibility_policy_file: Optional[str] = None,
    report: Optional[Report] = None,
) -> Dict[str, str]:
    """Create one installable requirements artifact per workflow cluster.

    ``report`` (optional): when supplied, every per-cluster version collapse is
    recorded as ``WARN requirements-conflict: <context>: …`` so the export
    report reflects what each cluster's requirements file actually pins (the
    per-cluster winner can differ between clusters — highest of the notebooks
    that run on each). Without this the collapse is only printed to stdout and
    the report is silent about a real version override.
    """
    from dbx_export.dependency_resolver import FileNode

    explicit_policy = bool(
        compatibility_policy_file
        or os.environ.get("AIDP_COMPATIBILITY_POLICY_FILE")
    )
    try:
        compatibility_policy = load_compatibility_policy(
            compatibility_policy_file
        )
    except RequirementConsolidationError as exc:
        # The repository default is optional so a reduced source distribution
        # can still export. An operator-provided path is a guardrail choice:
        # typo/corruption must fail loudly rather than silently disabling
        # protected-package enforcement.
        if explicit_policy:
            raise
        compatibility_policy = None
        tprint(
            "[requirements][WARNING] compatibility policy unavailable; "
            f"continuing without compatibility rewrites: {exc}"
        )
    contexts: Dict[str, dict] = {}
    for job_id, job_node in graph.jobs.items():
        if not job_node.job_def:
            continue
        settings = job_node.job_def.get("settings") or job_node.job_def
        for cluster in settings.get("job_clusters", []) or []:
            key = cluster.get("job_cluster_key")
            if key:
                context = f"job:{job_id}/job_cluster:{key}"
                entry = contexts.setdefault(
                    context, {
                        "task_refs": set(), "roots": set(), "pypi": set(),
                        "managed_artifacts": set(),
                    }
                )
                libraries = (cluster.get("new_cluster") or {}).get("libraries", [])
                entry["pypi"].update(_pypi_specs(libraries))
                entry["managed_artifacts"].update(
                    _artifact_distribution_names(libraries)
                )
        for task in settings.get("tasks", []) or []:
            context = _task_cluster_context(job_id, task)
            if not context:
                continue
            entry = contexts.setdefault(
                context, {
                    "task_refs": set(), "roots": set(), "pypi": set(),
                    "managed_artifacts": set(),
                }
            )
            entry["task_refs"].add(f"job:{job_id}/task:{task.get('task_key', '?')}")
            notebook = (task.get("notebook_task") or {}).get("notebook_path")
            if notebook:
                entry["roots"].add(notebook.strip())
            entry["pypi"].update(_pypi_specs(task.get("libraries", [])))
            entry["managed_artifacts"].update(
                _artifact_distribution_names(task.get("libraries", []))
            )
            entry["pypi"].update(_pypi_specs(
                (task.get("new_cluster") or {}).get("libraries", [])
            ))
            entry["managed_artifacts"].update(_artifact_distribution_names(
                (task.get("new_cluster") or {}).get("libraries", [])
            ))

    result: Dict[str, str] = {}
    reported_import_messages: Set[str] = set()
    safe_job = re.sub(r"[^A-Za-z0-9_.-]", "_", str(root_job_id))
    for context, entry in sorted(contexts.items()):
        sources = [target_runtime_requirements_source(target_runtime_requirements_file)]
        accepted_refs = set(entry["task_refs"]) | {context}
        for uri, node in sorted(graph.files.items()):
            if node.kind != "requirements" or uri.startswith(CANONICAL_REQUIREMENTS_PREFIX):
                continue
            if node.content is None or not accepted_refs.intersection(node.referenced_by):
                continue
            sources.append(RequirementSource(
                name=uri,
                content=node.content.decode("utf-8", errors="strict"),
            ))
        for uri, node in sorted(graph.files.items()):
            if node.kind != "whl" or not accepted_refs.intersection(node.referenced_by):
                continue
            artifact_name = _artifact_distribution_name(
                node.aidp_repo_path or uri
            )
            if artifact_name:
                entry["managed_artifacts"].add(artifact_name)
        reachable = _notebooks_reachable_from(graph, entry["roots"])
        sources.extend(_pip_sources_for_notebooks(graph, reachable, report=report))
        if entry["pypi"]:
            sources.append(RequirementSource(
                "Databricks PyPI libraries", "\n".join(sorted(entry["pypi"]))
            ))
        if entry["managed_artifacts"]:
            sources.append(RequirementSource(
                "declared WHL/EGG libraries",
                "",
                managed_names=frozenset(entry["managed_artifacts"]),
            ))
        init_source = _pip_source_for_init_scripts(graph, accepted_refs)
        entry["managed_artifacts"].update(init_source.managed_names)
        if (
            init_source.content
            or init_source.managed_names
            or init_source.inference_barrier
        ):
            sources.append(init_source)
        if context in graph.cluster_pypi_requirements:
            sources.append(RequirementSource(
                f"existing cluster libraries: {context}",
                "\n".join(sorted(graph.cluster_pypi_requirements[context])),
            ))

        for source in sources:
            if not source.inference_barrier:
                continue
            message = (
                f"WARN pip-inference-barrier: {context}: {source.name}; "
                "unresolved provider forms suppressed notebook import inference"
            )
            tprint(f"[requirements] {message}")

        try:
            import_audit = audit_notebook_imports(
                graph,
                reachable,
                sources,
                compatibility_policy=compatibility_policy,
            )
        except Exception as exc:
            # Import inference is an additive optimization.  A malformed or
            # unexpectedly shaped source must not abort an otherwise valid
            # export; preserve the explicit requirements and make the skipped
            # inference operator-visible.
            message = (
                f"WARN import-audit-unavailable: {context}: {type(exc).__name__}: {exc}; "
                "continuing without inferred notebook requirements"
            )
            warnings.warn(message, UserWarning, stacklevel=2)
            tprint(f"[requirements] {message}")
            if report is not None:
                report.errors.append(message)
            import_audit = None
        if import_audit is None:
            import_audit = ImportAuditResult(
                requirement_source=RequirementSource(
                    "approved inferred Python imports", ""
                ),
            )
        if import_audit.requirement_source.content:
            sources.append(import_audit.requirement_source)
        for spec, occurrence in import_audit.inferred:
            message = (
                f"INFO inferred-python-dependency: {spec} | "
                f"source={occurrence.source_path} | "
                f"dependency_chain={' -> '.join(occurrence.dependency_chain)} | "
                f"reason=inferred from `{occurrence.statement}`"
            )
            if message not in reported_import_messages:
                tprint(f"[requirements] {message}")
                if report is not None:
                    report.errors.append(message)
                reported_import_messages.add(message)
        for occurrence in import_audit.unknown:
            message = (
                f"WARN unknown-python-import: {occurrence.module} | "
                f"source={occurrence.source_path} | "
                f"dependency_chain={' -> '.join(occurrence.dependency_chain)} | "
                f"reason=no approved import-to-pip mapping for "
                f"`{occurrence.statement}`"
            )
            if message not in reported_import_messages:
                tprint(f"[requirements] {message}")
                if report is not None:
                    report.errors.append(message)
                reported_import_messages.add(message)
        for occurrence in import_audit.managed_by_passthrough:
            message = (
                f"INFO import-managed-by-passthrough: {occurrence.module} | "
                f"source={occurrence.source_path} | "
                f"dependency_chain={' -> '.join(occurrence.dependency_chain)} | "
                "reason=explicit wheel/VCS/editable or unclassified pip "
                "source preserved; no inferred duplicate added"
            )
            if message not in reported_import_messages:
                tprint(f"[requirements] {message}")
                if report is not None:
                    report.errors.append(message)
                reported_import_messages.add(message)
        for occurrence in import_audit.alternate_provider:
            message = (
                f"WARN import-managed-by-alternate-provider: {occurrence.module} | "
                f"source={occurrence.source_path} | "
                f"dependency_chain={' -> '.join(occurrence.dependency_chain)} | "
                "reason=explicit provider family does not match the approved "
                "distribution aliases; inferred duplicate suppressed"
            )
            if message not in reported_import_messages:
                tprint(f"[requirements] {message}")
                if report is not None:
                    report.errors.append(message)
                reported_import_messages.add(message)
        for unparsed in import_audit.unparsed:
            message = f"WARN import-audit-unparsed: {unparsed}"
            if message not in reported_import_messages:
                tprint(f"[requirements] {message}")
                if report is not None:
                    report.errors.append(message)
                reported_import_messages.add(message)

        safe_context = re.sub(r"[^A-Za-z0-9_.-]", "_", context)
        synthetic_uri = (
            f"{CANONICAL_REQUIREMENTS_PREFIX}{safe_job}_{safe_context}.txt"
        )
        def _on_conflict(c, _ctx=context):
            # Name the cluster context and the dropped constraint, and record it
            # in the export report (WARN prefix is counted by Report.summary()),
            # so an operator triaging a run-time import failure sees which
            # cluster pinned which version and what it overrode.
            from dbx_export.requirements import _conflict_summary
            summary = _conflict_summary(c)
            tprint(f"  [requirements][conflict][WARN] {_ctx}: {summary}. "
                   "Edit source if a different version is required.")
            if report is not None:
                report.errors.append(
                    f"WARN requirements-conflict: {_ctx}: {summary}")

        graph.files[synthetic_uri] = FileNode(
            dbx_source=synthetic_uri,
            kind="requirements",
            referenced_by=[context, "migration-baseline"],
            content=consolidate_requirement_sources(
                sources,
                compatibility_policy=compatibility_policy,
                on_conflict=_on_conflict,
            ).encode("utf-8"),
            fetch_error=None,
        )
        result[context] = synthetic_uri
        tprint(
            f"[requirements] cluster {context}: consolidated {len(sources)} "
            f"source(s) into {synthetic_uri}"
        )
    return result


def _attach_only_consolidated_requirements(conv, graph: Graph, context: str) -> str:
    """Attach only the canonical requirements file for this cluster."""
    canonical = [
        uri for uri, node in graph.files.items()
        if node.kind == "requirements"
        and uri.startswith(CANONICAL_REQUIREMENTS_PREFIX)
        and context in node.referenced_by
    ]
    if len(canonical) != 1:
        raise RequirementConsolidationError(
            f"expected one consolidated requirements artifact for {context}, "
            f"found {canonical}"
        )
    # PyPI entries were folded into the canonical file by
    # consolidate_cluster_requirements(). Keeping them here would install the
    # same package twice in independent resolver transactions.
    conv.library_refs = [
        lib for lib in conv.library_refs
        if not lib.get("requirements") and not lib.get("pypi")
    ]
    conv.library_refs.append({"requirements": canonical[0]})
    return canonical[0]


def _attach_context_jars(conv, graph: Graph, context: str) -> List[str]:
    """Attach notebook-discovered JARs for one cluster install boundary."""
    conv.library_refs = filter_cluster_library_refs(conv.library_refs, graph, context)
    existing = {
        lib.get("jar") for lib in conv.library_refs
        if isinstance(lib, dict) and lib.get("jar")
    }
    attached: List[str] = []
    for uri, node in graph.files.items():
        if node.kind != "jar" or context not in node.referenced_by:
            continue
        if uri not in existing:
            conv.library_refs.append({"jar": uri})
            existing.add(uri)
        attached.append(uri)
    return sorted(set(attached))


def _context_jar_install_items(
    graph: Graph,
    context: str,
    uri_to_aidp: Dict[str, str],
) -> List[Dict[str, Any]]:
    """Build install operations for resolved JARs on a reused cluster."""
    refs = [
        {"jar": uri} for uri, node in graph.files.items()
        if node.kind == "jar" and context in node.referenced_by
    ]
    return build_library_install_items(
        filter_cluster_library_refs(refs, graph, context, uri_to_aidp), uri_to_aidp,
    )


def _install_context_jars_on_cluster(
    aidp: AidpClient,
    cluster_key: str,
    graph: Graph,
    context: str,
    uri_to_aidp: Dict[str, str],
    tprint=None,
    report: Optional[Report] = None,
) -> int:
    """Best-effort install of discovered JARs on a reused/override cluster."""
    items = _context_jar_install_items(graph, context, uri_to_aidp)
    if not items:
        return 0
    try:
        aidp.install_cluster_libraries(cluster_key, items)
    except AidpApiError as exc:
        message = (
            f"notebook-resolved JAR attach failed for {context} on "
            f"{cluster_key}: {exc}"
        )
        if tprint is not None:
            tprint(f"    [jar-discovery][WARN] {message}")
        if report is not None:
            report.errors.append(f"WARN jar-library-install: {message}")
        return 0
    return len(items)


def phase1_discover_init_script_artifacts(
    dbx: DbxClient, graph: Graph, lib_repo: Optional["_LibRepoIndex"] = None
) -> None:
    """Scan downloaded init scripts for /Workspace/... JAR/wheel references
    and enqueue them as additional files to fetch. This catches dependencies
    that init scripts copy at runtime but aren't listed in libraries[]."""
    from dbx_export.cluster_converter import find_artifact_refs_in_script
    from dbx_export.dependency_resolver import FileNode
    newly_enqueued: List[str] = []
    for uri, node in list(graph.files.items()):
        if node.kind != "init_script" or not node.content:
            continue
        try:
            text = node.content.decode("utf-8", errors="replace")
        except Exception:
            continue
        for ref in find_artifact_refs_in_script(text):
            if ref in graph.files:
                graph.files[ref].referenced_by.append(f"init_script:{uri}")
                continue
            # Infer kind from extension.
            low = ref.lower()
            kind = "jar" if low.endswith(".jar") else (
                "whl" if low.endswith((".whl", ".egg")) else "config"
            )
            graph.files[ref] = FileNode(
                dbx_source=ref, kind=kind,
                referenced_by=[f"init_script:{uri}"],
            )
            newly_enqueued.append(ref)
    if newly_enqueued:
        tprint(f"  [init-scripts] discovered {len(newly_enqueued)} artifact ref(s) inside init scripts:")
        for r in newly_enqueued:
            tprint(f"      - {r}")
        # Fetch the newly-enqueued ones (AIDP-first when the repo is indexed).
        phase1_fetch_files(dbx, graph, lib_repo)


# ─── Phase 2 — Upload ──────────────────────────────────────────────


def _path_for_notebook(
    dbx_path: str,
    export_output: str,
    is_git: bool = False,
    extension: str = ".ipynb",
) -> str:
    return dbx_to_aidp_path(dbx_path, export_output, KIND_NOTEBOOK,
                            is_git=is_git, extension=extension)


def _path_for_file(uri: str, kind: str, export_output: str) -> str:
    # Any kind not explicitly bucketed (e.g. .sh / .zip / .tar.gz / .so files
    # referenced by init scripts) falls back to KIND_CONFIG → configs/ bucket.
    return file_target_path(uri, kind, export_output)


def configured_lib_repository_dir(config_path: Optional[str] = None) -> str:
    """Load ``paths.lib_repository_dir`` from YAML when configuration is present.

    The directory is the PRIMARY shared dependency repository (checked before
    any Databricks fetch), not a fallback — hence the name. Deprecated key
    aliases are still read for older configs, in order: ``lib_fallback_dir``
    (post-consolidation interim name) then ``jar_fallback_dir`` (pre-
    consolidation). Legacy exporter calls that do not provide YAML retain the
    historical shared dependency directory. If a YAML path is explicitly
    supplied, configuration errors are surfaced instead of silently falling back.
    """
    from aidp_config import ConfigError, load_config

    try:
        cfg = load_config(cli_config_path=config_path)
    except ConfigError:
        if config_path or os.environ.get("AIDP_CONFIG_PATH"):
            raise
        return DEFAULT_AIDP_LIB_DIR
    configured = getattr(cfg.paths, "lib_repository_dir", None)
    if configured is None:
        # Deprecated aliases — configs predating the rename/consolidation.
        configured = getattr(cfg.paths, "lib_fallback_dir", None)
    if configured is None:
        configured = getattr(cfg.paths, "jar_fallback_dir", DEFAULT_AIDP_LIB_DIR)
    return str(configured).rstrip("/")


# Back-compat aliases for external callers/tests that imported the old names.
configured_lib_fallback_dir = configured_lib_repository_dir
configured_jar_fallback_dir = configured_lib_repository_dir


def _resolve_proxy_secret(ref, field_name: str) -> Optional[str]:
    """Materialise a YAML `secrets.dbx_proxy_basic_*` entry into a string.

    Shapes (mirrors the schema's `secret` definition):
    - None: key absent — proxy needs no auth for this deployment.
    - str: plaintext value (interim form) — used as-is.
    - credential-store ref ({from, name, key} / SimpleNamespace): resolved via
      aidputils, which only exists inside an AIDP cluster kernel. Laptop-side
      the ref cannot be fetched — return None so the caller proceeds without
      auth (CLI/env remain the laptop-side way to supply creds); the loud
      401 from an auth-requiring proxy makes the gap obvious.
    """
    if ref is None:
        return None
    if isinstance(ref, str):
        return ref.strip() or None
    name = getattr(ref, "name", None) or (ref.get("name") if isinstance(ref, dict) else None)
    key = getattr(ref, "key", None) or (ref.get("key") if isinstance(ref, dict) else None)
    if not name:
        return None
    from .auth import _get_aidputils_from_kernel

    aidputils = _get_aidputils_from_kernel()
    if aidputils is None:
        tprint(
            f"[auth][WARN] secrets.{field_name} is a credential-store ref but "
            "aidputils is unavailable (not running on an AIDP cluster kernel) — "
            "proceeding without it; pass --proxy-basic-user/--proxy-basic-pass "
            "or env DBX_PROXY_BASIC_USER/DBX_PROXY_BASIC_PASS instead."
        )
        return None
    try:
        value = aidputils.secrets.get(name, key=key) if key else aidputils.secrets.get(name)
    except Exception as exc:
        raise RuntimeError(
            f"secrets.{field_name}: credential-store fetch failed for "
            f"credential {name!r} key {key!r}: {exc}"
        ) from exc
    value = str(value).strip()
    if not value:
        raise RuntimeError(
            f"secrets.{field_name}: credential {name!r} key {key!r} resolved "
            "to an empty value — fix the credential in the AIDP store."
        )
    return value


# ─── AIDP-first dependency resolution ────────────────────────────────────────
# Before fetching a jar/wheel from Databricks, prefer an artifact already staged
# in the shared AIDP dependency repository (lib_repository_dir). Matching is by
# artifact NAME (version-agnostic): an exact version match wins; otherwise the
# highest available version of the same name is used. This avoids re-shipping
# bytes AIDP already has and lets admins pin approved builds.
#
# Wheels follow PEP 427 and parse reliably. JAR filenames have NO version
# standard — when a jar name cannot be confidently parsed into (name, version),
# ONLY an exact-basename match is honored (never a fuzzy highest-version guess
# that could attach the wrong jar).
#
# Wheel TAG safety: a wheel's python/abi/platform tags are part of its identity.
# True target compatibility cannot be decided at export time (it depends on the
# AIDP cluster the wheel eventually installs on), so only the provably-safe
# subset is substituted automatically: identical tag sets, or a staged
# generic-major pure-Python wheel (py3-none-any for a Python-3 request —
# platform-independent AND generic across Python-3 minors). Any other tag
# mismatch (arch, ABI, python version) is NOT substituted — a loud MANUAL
# ACTION REQUIRED log is emitted and the normal Databricks fetch proceeds.

def _parse_wheel(
    basename: str,
) -> Tuple[Optional[str], Optional[pkg_version.Version], Optional[frozenset]]:
    """(canonical distribution, Version, tag frozenset) for a PEP-427 wheel,
    else (None, None, None)."""
    if not basename.endswith(".whl"):
        return None, None, None
    try:
        name, ver, _build, tags = parse_wheel_filename(basename)
    except (InvalidWheelFilename, pkg_version.InvalidVersion):
        return None, None, None
    return canonicalize_name(name), ver, tags


def _parse_wheel_name_version(
    basename: str,
) -> Tuple[Optional[str], Optional[pkg_version.Version]]:
    """(canonical distribution, Version) for a PEP-427 wheel, else (None, None)."""
    name, ver, _tags = _parse_wheel(basename)
    return name, ver


# Interpreter tag: '<impl><major><minor?>', e.g. cp311 / py3 / py36 / pp310.
_INTERP_TAG_RE = re.compile(r"^([a-z]+)(\d)(\d*)$")

# Documented target-runtime contract: AIDP clusters run CPython 3 (Spark 3.5 /
# Python 3.11). Substitution decisions are anchored to this target major —
# what matters is whether the artifact installs on AIDP, not which extra
# interpreters the Databricks-side wheel happened to support. (A py2.py3
# request must never be narrowed to a py2-only staged wheel: the request runs
# on Python 3, the replacement would not.)
_TARGET_PYTHON_MAJOR = 3


def _generic_pure_majors(tags: frozenset) -> Set[int]:
    """Python majors covered by a wheel's GENERIC pure tags (py3-none-any →
    {3}; py2.py3-none-any → {2, 3}). Version-specific tags (py36) and
    non-pure tags contribute nothing."""
    majors: Set[int] = set()
    for t in tags:
        if t.abi != "none" or t.platform != "any":
            continue
        m = _INTERP_TAG_RE.match(t.interpreter)
        if m and m.group(1) == "py" and m.group(3) == "":
            majors.add(int(m.group(2)))
    return majors


def _wheel_tags_compatible(
    requested: Optional[frozenset], staged: Optional[frozenset]
) -> bool:
    """Safe-substitution rule for wheels.

    - Identical tag sets: same build target, safe.
    - Staged pure wheel (none-any): platform-independent but NOT necessarily
      Python-version-independent — reusable only when it covers the AIDP
      target major (``py3``; ``py2.py3`` qualifies via its py3 tag) AND the
      requested wheel targets Python 3 too. A Python-2-only staged wheel is
      never substituted, even for a ``py2.py3`` request. Version-specific
      pure tags (``py36``) are NOT substituted.
    Anything else needs a human decision.
    """
    if staged is None or requested is None:
        return False
    if staged == requested:
        return True
    req_targets_py3 = any(
        m and int(m.group(2)) == _TARGET_PYTHON_MAJOR
        for m in (_INTERP_TAG_RE.match(t.interpreter) for t in requested)
    )
    return req_targets_py3 and _TARGET_PYTHON_MAJOR in _generic_pure_majors(staged)


def _parse_jar_name_version(
    basename: str,
) -> Tuple[Optional[str], Optional[pkg_version.Version]]:
    """(name, Version) for a ``<name>-<version>.jar``, else (None, None).

    The version begins at the FIRST ``-`` token that starts with a digit AND
    makes the remaining tail parse as a PEP-440 version. Names carrying git
    hashes / non-standard tails (e.g. ``hudi-...-0.15.0-3975ca4.jar``) do not
    parse and return (None, None) so the caller matches by exact basename only.
    """
    if not basename.endswith(".jar"):
        return None, None
    parts = basename[: -len(".jar")].split("-")
    for i in range(1, len(parts)):
        if not parts[i][:1].isdigit():
            continue
        tail = "-".join(parts[i:])
        try:
            ver = pkg_version.parse(tail)
        except pkg_version.InvalidVersion:
            continue
        return "-".join(parts[:i]), ver
    return None, None


class _LibRepoIndex:
    """One paginated listing of lib_repository_dir, indexed for name resolution."""

    def __init__(self, aidp: AidpClient, lib_dir: str):
        self.lib_dir = lib_dir.rstrip("/")
        self.basenames: Set[str] = set()
        # (kind, canonical_name) -> list[(Version, basename, tags-or-None)]
        self._by_name: Dict[
            Tuple[str, str],
            List[Tuple[pkg_version.Version, str, Optional[frozenset]]],
        ] = {}
        self._loaded = False
        self._aidp = aidp

    def _list_dir(self, path: str, quiet_missing: bool = False) -> List[str]:
        """Basenames under one workspace dir; [] when it is absent/unlistable.

        ``quiet_missing`` suppresses the warning for a 404 only — used by the
        OPTIONAL legacy /jars probe, where absence is the expected state on a
        clean installation. Permission/server failures still warn.
        """
        try:
            items = self._aidp.list_workspace_objects(path)
        except AidpApiError as exc:
            if not (quiet_missing and exc.status == 404):
                tprint(f"  [aidp-first][WARN] cannot list {path}: {exc}")
            return []
        return [
            (it.get("path") or it.get("name") or "").rsplit("/", 1)[-1]
            for it in items
        ]

    def _load(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        basenames = self._list_dir(self.lib_dir)
        # Transition story: installations upgraded from the pre-consolidation
        # layout may still hold artifacts under the legacy sibling `jars/`
        # directory. Index it too (at lower precedence — same-name entries in
        # the primary dir win exact-basename matching first) and nudge loudly.
        if self.lib_dir.rsplit("/", 1)[-1] == "libs":
            legacy_dir = f"{self.lib_dir.rsplit('/', 1)[0]}/jars"
            legacy = [b for b in self._list_dir(legacy_dir, quiet_missing=True)
                      if b.endswith((".jar", ".whl")) and b not in set(basenames)]
            if legacy:
                tprint(
                    f"  [aidp-first][WARN][DEPRECATED LAYOUT] {len(legacy)} "
                    f"artifact(s) still under legacy {legacy_dir}; also indexing "
                    f"them, but move everything to {self.lib_dir}"
                )
                self._legacy_dir = legacy_dir
                self._legacy_basenames = set(legacy)
        for base in basenames + list(getattr(self, "_legacy_basenames", ())):
            if base.endswith(".jar"):
                self.basenames.add(base)
                name, ver = _parse_jar_name_version(base)
                if name is not None:
                    self._by_name.setdefault(("jar", name), []).append((ver, base, None))
            elif base.endswith(".whl"):
                self.basenames.add(base)
                name, ver, tags = _parse_wheel(base)
                if name is not None:
                    self._by_name.setdefault(("whl", name), []).append((ver, base, tags))

    def _dir_for(self, base: str) -> str:
        """The directory a basename was indexed from (primary or legacy)."""
        if base in getattr(self, "_legacy_basenames", ()):
            return self._legacy_dir
        return self.lib_dir

    def resolve(self, uri: str, kind: str) -> Optional[Tuple[str, str]]:
        """Return (aidp_path, provenance) for an AIDP-first match, else None."""
        self._load()
        basename = uri.rstrip("/").rsplit("/", 1)[-1]
        # 1) Exact-basename match (also the ONLY path for unparseable jars).
        candidates = [basename]
        sanitized = sanitizer.sanitize_segment(basename)
        if sanitized != basename:
            candidates.append(sanitized)
        for cand in candidates:
            if cand in self.basenames:
                d = self._dir_for(cand)
                return (
                    f"{d}/{cand}",
                    f"AIDP-first: exact basename {cand} present in {d}",
                )
        # 2) Name match with version preference (exact requested → else highest).
        want_tags: Optional[frozenset] = None
        if kind == "whl":
            name, want, want_tags = _parse_wheel(basename)
        elif kind == "jar":
            name, want = _parse_jar_name_version(basename)
        else:
            name, want = None, None
        if name is None:
            return None
        entries = self._by_name.get((kind, name))
        if not entries:
            return None
        # Wheel tag safety gate: substitution is only safe for identical tag
        # sets or a pure-Python staged wheel. Incompatible-tag candidates are
        # excluded; if that excludes everything, demand a human decision.
        if kind == "whl":
            usable = [e for e in entries if _wheel_tags_compatible(want_tags, e[2])]
            if not usable:
                rejected = ", ".join(b for _v, b, _t in sorted(entries, key=lambda e: e[0]))
                tprint(
                    f"  [aidp-first][ERROR][MANUAL ACTION REQUIRED] wheel tag "
                    f"mismatch for {basename}: staged candidate(s) [{rejected}] in "
                    f"{self.lib_dir} target a different python/abi/platform — NOT "
                    f"substituting; falling back to the Databricks fetch. Verify "
                    f"platform compatibility and stage a matching wheel manually."
                )
                return None
            entries = usable
        if want is not None:
            for ver, base, _tags in entries:
                if ver == want:
                    d = self._dir_for(base)
                    return (
                        f"{d}/{base}",
                        f"AIDP-first: exact version {name}=={ver} in {d}",
                    )
        best_ver, best_base, _best_tags = max(entries, key=lambda e: e[0])
        d = self._dir_for(best_base)
        tprint(
            f"  [aidp-first][WARN][VERSION SUBSTITUTION] {basename}: requested "
            f"{name}{('==' + str(want)) if want is not None else ''} is not "
            f"staged; reusing highest available {best_ver} ({best_base}) from "
            f"{d}. Review that this version change is acceptable."
        )
        return (
            f"{d}/{best_base}",
            f"AIDP-first: {name} requested"
            f"{('==' + str(want)) if want is not None else ''} not staged; "
            f"using highest available {best_ver} ({best_base}) in {d}",
        )


def _find_existing_jar_fallback(
    aidp: AidpClient,
    uri: str,
    lib_repository_dir: str,
    expected_content: Optional[bytes] = None,
) -> Optional[Tuple[str, str]]:
    """Return a checksum-verified shared JAR and its SHA-256 digest.

    When source bytes are available (the upload-error path), basename equality
    is never sufficient: the fallback must have identical content. When DBX
    fetch itself failed, record the shared object's digest and provenance so
    the otherwise-unverifiable reuse remains explicit in the export report.
    """
    basename = uri.rstrip("/").rsplit("/", 1)[-1]
    fallback_dir = lib_repository_dir.rstrip("/")
    candidates = [f"{fallback_dir}/{basename}"]
    sanitized = sanitizer.sanitize_segment(basename)
    sanitized_path = f"{fallback_dir}/{sanitized}"
    if sanitized_path not in candidates:
        candidates.append(sanitized_path)
    expected_sha = file_sha256(expected_content) if expected_content is not None else None
    for path in candidates:
        if not aidp.object_exists(path):
            continue
        try:
            fallback_content = aidp.download_file(path)
        except AidpApiError as exc:
            tprint(f"  [jar-fallback][WARN] cannot verify {path}: {exc}")
            continue
        fallback_sha = file_sha256(fallback_content)
        if expected_sha is not None and fallback_sha != expected_sha:
            tprint(
                f"  [jar-fallback][REJECT] checksum mismatch for {path}: "
                f"source={expected_sha}, fallback={fallback_sha}"
            )
            continue
        return path, fallback_sha
    return None


def resolve_failed_jar_fallbacks(
    aidp: AidpClient,
    graph: Graph,
    lib_repository_dir: str,
) -> None:
    """Resolve failed DBX JAR fetches from the shared AIDP JAR repository.

    Probe the original basename first because shared/admin-managed JARs retain
    their uploaded filename. Probe the sanitizer-normalized basename second so
    files staged by the exporter are also reusable. This is read-only; cluster
    attachment still happens later through ``uri_to_aidp``.
    """
    for uri, node in graph.files.items():
        if node.kind != "jar" or node.fetch_error is None:
            continue
        match = _find_existing_jar_fallback(aidp, uri, lib_repository_dir)
        if match:
            candidate, digest = match
            node.aidp_fallback_sha256 = digest
            node.aidp_fallback_provenance = (
                f"rejected shared candidate {candidate}; DBX source bytes "
                "unavailable for checksum comparison"
            )
            tprint(
                f"  [jar-fallback][REJECT] DBX export failed for {uri}; "
                f"candidate {candidate} has sha256={digest}, but source identity "
                "cannot be established without DBX bytes"
            )


def phase2_upload_notebooks(
    aidp: AidpClient,
    graph: Graph,
    export_output: str,
    overwrite: bool,
    run_id: str,
    root_job_id: str,
    report: Report,
    path_plan=None,
) -> Tuple[Dict[str, str], List[str]]:
    """Upload every successfully-exported notebook.

    Failed exports get NO upload — the workflow task that references the
    canonical AIDP path is still created (AIDP allows a workflow with a
    dangling notebookPath; the task fails at runtime if anyone tries to
    run it). This avoids cluttering AIDP with placeholder notebooks.

    Returns:
        nb_target_paths: {dbx_path → aidp_path} (always populated for all
                         notebooks, even failed exports, so the workflow
                         payload uses the canonical path)
        failed_task_notebook_paths: list of dbx_path where role=task AND
                         the export OR upload failed (consumed by --strict)
    """
    path_plan = path_plan if path_plan is not None else prepare_export_paths(graph, export_output, report, aidp)
    nb_target_paths = path_plan.notebooks
    if path_plan.errors:
        return nb_target_paths, [source for source, node in graph.notebooks.items() if node.role == "task"]
    failed_task = []
    first_result = len(report.notebooks)
    for dbx_path, node in graph.notebooks.items():
        if dbx_path in path_plan.notebook_aliases:
            continue  # the canonical node's outcome is copied below, including failures
        aidp_path = nb_target_paths[dbx_path]
        fingerprint = path_plan.verified.get(("notebook", dbx_path))
        if fingerprint is not None:
            tprint(f"  [OK] verified existing path (no rewrite): {aidp_path}")
            report.notebooks.append(NotebookResult(
                dbx_path=dbx_path, aidp_path=aidp_path, role=node.role, status="VALIDATED",
                dbx_fingerprint=fingerprint, aidp_fingerprint=fingerprint,
                cells=len(node.nb_dict.get("cells", [])) if node.is_notebook else None,
                size_bytes=len(node.content) if node.content is not None else None,
                export_method=node.export_method))
            continue

        # Determine action: upload real, upload placeholder, or skip.
        existing = None
        existing_is_placeholder = False
        if not node.is_notebook:
            # Local Python/config resources are workspace FILE objects. The
            # notebook contents endpoint can reject YAML/JSON/plain .py and
            # must not be used as their existence probe.
            existing = aidp.object_exists(aidp_path)
        else:
            try:
                existing = aidp.get_notebook(aidp_path)
            except AidpApiError as e:
                report.errors.append(f"probe-failed: {aidp_path}: {e}")
            existing_is_placeholder = ph_mod.is_placeholder(existing)

        # Defensive: if the BFS somehow skipped fetching this notebook (e.g.
        # discovered late and not re-queued), treat it as an export failure
        # so the rest of the pipeline doesn't crash on nb_dict=None.
        if node.export_error is None and node.content is None:
            node.export_error = "not_fetched"
            node.error_detail = "Notebook was discovered but never fetched from DBX (BFS gap)"
            tprint(f"  [WARN] {dbx_path}: discovered but never fetched")

        # Case A: notebook export failed → skip upload. The workflow task
        # still references the canonical AIDP path (computed above); AIDP
        # accepts dangling notebookPath values, the task just fails at
        # runtime if anyone runs it. No placeholder is created.
        if node.export_error is not None:
            tprint(f"  [SKIP] export failed, no upload: {dbx_path}  ({node.export_error})")
            if node.role == "task":
                failed_task.append(dbx_path)
            report.notebooks.append(NotebookResult(
                dbx_path=dbx_path, aidp_path=aidp_path, role=node.role,
                status="EXPORT_FAILED",
                placeholder_reason=node.export_error,   # reused field for the reason
                placeholder_detail=node.error_detail,
            ))
            continue

        # Case B: real export OK.
        # Skip if real exists and not overwriting (but DO overwrite placeholders
        # left by older runs — that's the migration path).
        if existing and not existing_is_placeholder and not overwrite:
            label = "notebook" if node.is_notebook else "resource"
            tprint(f"  SKIP {label} (exists): {aidp_path}")
            report.notebooks.append(NotebookResult(
                dbx_path=dbx_path, aidp_path=aidp_path, role=node.role,
                status="SKIPPED_EXISTS",
            ))
            continue

        # Utility (plain .py / .scala) — upload as a file with byte-level SHA
        # round-trip, not a notebook. No cell fingerprint applies.
        if not node.is_notebook:
            try:
                aidp.upload_file(aidp_path, node.content, overwrite=overwrite)
            except AidpApiError as e:
                tprint(f"  [FAIL] utility upload: {aidp_path}: {e}")
                if node.role == "task":
                    failed_task.append(dbx_path)
                report.errors.append(f"utility-upload-failed: {aidp_path}: {e}")
                continue
            local_sha = file_sha256(node.content)
            try:
                fetched = aidp.download_file(aidp_path)
                remote_sha = file_sha256(fetched)
            except AidpApiError as e:
                # Upload returned 200; the read-back GET failed → unverified,
                # not proven-broken. Record at the boundary (report + task
                # failure) instead of silently accepting, but don't hard-abort a
                # 200-confirmed upload over a verification blip.
                tprint(f"  [WARN] read-back of {aidp_path} failed ({e}); "
                       f"upload returned 200 but could not be verified")
                if node.role == "task":
                    failed_task.append(dbx_path)
                report.errors.append(f"utility-readback-unverified: {aidp_path}: {e}")
                continue
            if local_sha != remote_sha:
                tprint(f"  [FAIL] sha mismatch on utility {aidp_path}")
                if node.role == "task":
                    failed_task.append(dbx_path)
                report.errors.append(f"utility-sha-mismatch: {aidp_path}")
                continue
            tprint(f"  [OK] utility ({node.final_extension}): {aidp_path}  ({len(node.content)} bytes)")
            report.notebooks.append(NotebookResult(
                dbx_path=dbx_path, aidp_path=aidp_path, role=node.role,
                status="VALIDATED",
                dbx_fingerprint=local_sha, aidp_fingerprint=remote_sha,
                size_bytes=len(node.content),
                export_method=node.export_method,
            ))
            continue

        # Compute fingerprint before upload.
        dbx_fp = cell_fingerprint(node.nb_dict["cells"])
        try:
            aidp.put_notebook(aidp_path, node.nb_dict)
        except AidpApiError as e:
            tprint(f"  [FAIL] notebook upload: {aidp_path}: {e}")
            if node.role == "task":
                failed_task.append(dbx_path)
            report.errors.append(f"notebook-upload-failed: {aidp_path}: {e}")
            continue

        # Read-back validation.
        try:
            read_back = aidp.get_notebook(aidp_path)
            if read_back is None:
                raise AidpApiError("read_back", 0, "GET returned None right after PUT")
            aidp_fp = cell_fingerprint(read_back.get("cells", []))
        except AidpApiError as e:
            tprint(f"  [FAIL] read-back: {aidp_path}: {e}")
            if node.role == "task":
                failed_task.append(dbx_path)
            report.errors.append(f"notebook-readback-failed: {aidp_path}: {e}")
            continue

        if dbx_fp != aidp_fp:
            mismatches = cell_diff(node.nb_dict["cells"], read_back.get("cells", []))
            tprint(f"  [FAIL] fingerprint mismatch: {aidp_path}  ({len(mismatches)} mismatched cells)")
            report.errors.append(
                f"fingerprint-mismatch: {aidp_path}: {mismatches[:2]}"
            )
            # The notebook uploaded but didn't round-trip cleanly. Leave the
            # (possibly-corrupted) upload on AIDP and record the failure — no
            # placeholder. Workflow task still references the path.
            # NOTE (intentional): a task fingerprint-mismatch ALWAYS counts as a
            # failed task — deliberately NOT gated on --no-placeholder, so a
            # silently-corrupted task notebook surfaces regardless of that flag.
            if node.role == "task":
                failed_task.append(dbx_path)
            report.notebooks.append(NotebookResult(
                dbx_path=dbx_path, aidp_path=aidp_path, role=node.role,
                status="FINGERPRINT_MISMATCH",
                placeholder_reason="fingerprint_mismatch",
                placeholder_detail=str(mismatches[:2])[:500],
            ))
            continue

        tprint(f"  [OK] notebook: {aidp_path}  ({len(node.nb_dict.get('cells', []))} cells)")
        report.notebooks.append(NotebookResult(
            dbx_path=dbx_path, aidp_path=aidp_path, role=node.role,
            status="VALIDATED",
            dbx_fingerprint=dbx_fp, aidp_fingerprint=aidp_fp,
            cells=len(node.nb_dict.get("cells", [])),
            size_bytes=len(node.content) if node.content else None,
            export_method=node.export_method,
        ))

    # Preserve every discovery record while performing one physical upload.
    # Some existing upload failure branches report only an error; materialize
    # that failure for the shared artifact so no alias can look successful.
    outcomes = {row.dbx_path: row for row in report.notebooks[first_result:]}
    for alias, canonical in path_plan.notebook_aliases.items():
        outcome = outcomes.get(canonical)
        if outcome is None:
            outcome = NotebookResult(
                dbx_path=canonical, aidp_path=nb_target_paths[canonical],
                role=graph.notebooks[canonical].role, status="FAILED",
                placeholder_reason="shared_artifact_upload_failed",
                placeholder_detail="Upload or read-back failed; see report errors.")
            outcomes[canonical] = outcome
            report.notebooks.append(outcome)
        outcome.referenced_by = list(graph.notebooks[canonical].referenced_by)
        report.notebooks.append(replace(
            outcome, dbx_path=alias, role=graph.notebooks[alias].role,
            alias_of=canonical, referenced_by=list(graph.notebooks[alias].referenced_by)))
        if outcome.status not in {"VALIDATED", "SKIPPED_EXISTS"}:
            for source in (canonical, alias):
                if graph.notebooks[source].role == "task" and source not in failed_task:
                    failed_task.append(source)
        tprint(f"  [alias] {alias} shares {canonical}: {outcome.status} ({outcome.aidp_path})")

    return nb_target_paths, failed_task


def phase2_upload_files(
    aidp: AidpClient,
    graph: Graph,
    export_output: str,
    lib_repository_dir: str,
    overwrite: bool,
    report: Report,
    strict_jars: bool = False,
    path_plan=None,
) -> Tuple[Dict[str, str], bool]:
    """Upload jars / init scripts / wheels. Returns (uri→aidp_path, any_cluster_file_failed).

    Init scripts get rewritten before upload: any /Workspace/...artifact path
    they reference is swapped for the AIDP destination where we uploaded that
    artifact, and DBX runtime paths (e.g. /databricks/jars/) are mapped to
    their AIDP equivalents.
    """
    from dbx_export.cluster_converter import rewrite_init_script

    path_plan = path_plan if path_plan is not None else prepare_export_paths(graph, export_output, report, aidp)
    if path_plan.errors:
        return {}, True

    uri_to_aidp: Dict[str, str] = {}
    cluster_file_failed = False

    # Pre-compute URI → AIDP destination for every file. We need this map
    # BEFORE uploading init scripts so we can rewrite their content.
    planned: Dict[str, str] = {}
    for uri, node in graph.files.items():
        if node.aidp_repo_path is not None:
            # AIDP-first hit: the artifact already lives in the shared repo, so
            # any init-script reference to it should rewrite to that path.
            planned[uri] = node.aidp_repo_path
        elif file_upload_candidate(node):
            planned[uri] = path_plan.files[uri]

    for uri, node in graph.files.items():
        # AIDP-first hit (Phase 1): the shared repository already holds a
        # matching jar/wheel. Attach it as-is — no DBX bytes were fetched and
        # nothing is uploaded.
        if node.aidp_repo_path is not None:
            uri_to_aidp[uri] = node.aidp_repo_path
            tprint(f"  [OK] AIDP-first: {uri} -> {node.aidp_repo_path}")
            report.files.append(FileResult(
                dbx_source=uri, aidp_path=node.aidp_repo_path, kind=node.kind,
                status="SKIPPED_EXISTS",
                failure_reason=node.aidp_repo_provenance,
            ))
            continue
        if node.fetch_error == "external_uri_skipped":
            report.files.append(FileResult(
                dbx_source=uri, aidp_path="", kind=node.kind,
                status="EXTERNAL_RECORD_ONLY",
                failure_reason=node.error_detail or "external",
            ))
            continue
        if node.fetch_error is None and node.content is None:
            node.fetch_error = "not_fetched"
            node.error_detail = "File was discovered but no downloaded content is available"
            tprint(f"  [WARN] {uri}: discovered but never fetched")
        if node.fetch_error is not None:
            if node.kind == "jar" and node.aidp_replacement_paths:
                replacements = ", ".join(node.aidp_replacement_paths)
                message = (
                    f"DBX JAR unavailable ({node.fetch_error}); verified JVM "
                    f"references are covered by replacement JAR(s): {replacements}"
                )
                tprint(f"  [WARN] JAR source missing; replacement used: {message}")
                report.errors.append(f"WARN jar-replacement-used: {message}")
                report.files.append(FileResult(
                    dbx_source=uri,
                    aidp_path=replacements,
                    kind=node.kind,
                    status="SKIPPED_REPLACED",
                    failure_reason=message,
                ))
                continue
            if node.kind == "jar" and node.aidp_fallback_path:
                aidp_path = node.aidp_fallback_path
                uri_to_aidp[uri] = aidp_path
                tprint(f"  [OK] JAR fallback: {uri} -> {aidp_path}")
                report.files.append(FileResult(
                    dbx_source=uri,
                    aidp_path=aidp_path,
                    kind=node.kind,
                    status="SKIPPED_EXISTS",
                    failure_reason=(
                        f"DBX fetch failed ({node.fetch_error}); reused shared AIDP JAR; "
                        f"sha256={node.aidp_fallback_sha256}; "
                        f"provenance={node.aidp_fallback_provenance}"
                    ),
                ))
                continue
            if node.kind == "jar":
                basename = uri.rstrip("/").rsplit("/", 1)[-1]
                sanitized = sanitizer.sanitize_segment(basename)
                checked = [f"{lib_repository_dir.rstrip('/')}/{basename}"]
                sanitized_path = f"{lib_repository_dir.rstrip('/')}/{sanitized}"
                if sanitized_path not in checked:
                    checked.append(sanitized_path)
                message = (
                    f"JAR export failed and no verified shared AIDP JAR was found. "
                    f"DBX source={uri}; checked={checked}; "
                    f"candidate_provenance={node.aidp_fallback_provenance or 'none'}; "
                    f"candidate_sha256={node.aidp_fallback_sha256 or 'none'}; "
                    f"error={node.fetch_error}: {node.error_detail or ''}"
                )
                if strict_jars:
                    raise RuntimeError("Cannot proceed: " + message)
                cluster_file_failed = True
                tprint(f"  [WARN] {message}")
                report.files.append(FileResult(
                    dbx_source=uri, aidp_path="", kind=node.kind,
                    status="FAILED", failure_reason=message,
                ))
                continue
            cluster_file_failed = True
            tprint(f"  [FAIL] file fetch: {uri}: {node.fetch_error}")
            report.files.append(FileResult(
                dbx_source=uri, aidp_path="", kind=node.kind,
                status="FAILED", failure_reason=node.fetch_error,
            ))
            continue

        aidp_path = planned.get(uri) or path_plan.files[uri]
        fingerprint = path_plan.verified.get(("file", uri))
        if fingerprint is not None:
            if node.skip_attach:
                report.files.append(FileResult(dbx_source=uri, aidp_path=aidp_path, kind=node.kind,
                    status="SKIPPED_REDUNDANT", failure_reason="all-DBX-runtime — covered by AIDP libraries API"))
                continue
            uri_to_aidp[uri] = aidp_path
            tprint(f"  [OK] verified existing path (no rewrite): {aidp_path}")
            report.files.append(FileResult(dbx_source=uri, aidp_path=aidp_path, kind=node.kind,
                status="VALIDATED", sha256=fingerprint,
                size_bytes=path_plan.verified_sizes[("file", uri)]))
            continue
        # Auto-generated files (synthetic pip_deps, URI prefix /dbx_export_generated/)
        # are regenerated from the current source on every run — their content changes
        # (e.g. the pip conflict resolver dropping a pin). They must ALWAYS overwrite,
        # never be shadowed by a stale copy from a prior run, independent of the
        # --overwrite flag (which governs user-authored notebooks/jars we won't clobber).
        # Without this, a broken earlier run's pip_deps kept its conflicting pins and
        # got installed despite the resolver fixing them.
        is_generated = uri.startswith("/dbx_export_generated/")
        _exists = aidp.object_exists(aidp_path)
        if _exists and not overwrite and not is_generated:
            tprint(f"  SKIP file (exists): {aidp_path}")
            uri_to_aidp[uri] = aidp_path
            report.files.append(FileResult(
                dbx_source=uri, aidp_path=aidp_path, kind=node.kind,
                status="SKIPPED_EXISTS",
                size_bytes=len(node.content) if node.content else 0,
            ))
            continue
        if _exists and is_generated:
            tprint(f"  [regenerated] overwriting stale generated file: {aidp_path}")

        # For init scripts: rewrite content using the planned URI→AIDP map.
        # The rewriter comments out lines that reference DBX runtime paths
        # (e.g. /databricks/, /dbfs/) because those don't exist on AIDP; the
        # JARs are installed via the libraries API instead.
        upload_content = node.content
        if node.kind == "init_script":
            try:
                text = node.content.decode("utf-8", errors="strict")
                new_text, replacements, became_no_op, partial = rewrite_init_script(text, planned)
                if became_no_op:
                    tprint(
                        f"  [SKIP init_script] {uri}: all logical lines were DBX-only; "
                        f"JARs installed via libraries API — not uploading or attaching"
                    )
                    node.skip_attach = True
                    report.files.append(FileResult(
                        dbx_source=uri, aidp_path=aidp_path, kind=node.kind,
                        status="SKIPPED_REDUNDANT",
                        failure_reason="all-DBX-runtime — covered by AIDP libraries API",
                    ))
                    continue
                if replacements:
                    tprint(f"  [rewrite] {aidp_path}: {len(replacements)} substitution(s)")
                    for old, new in replacements:
                        tprint(f"      {old}  ->  {new}")
                    upload_content = new_text.encode("utf-8")
                if partial:
                    msg = (
                        f"init script {uri} partially exported — some DBX-specific "
                        f"lines were commented out but other commands survived. "
                        f"REVIEW {aidp_path} manually before relying on it."
                    )
                    tprint(f"  [WARN] {msg}")
                    report.errors.append(f"WARN partial-init-script: {msg}")
            except UnicodeDecodeError:
                tprint(f"  [rewrite] {aidp_path}: skipping (non-text init script)")

        try:
            aidp.upload_file(aidp_path, upload_content, overwrite=overwrite)
        except AidpApiError as e:
            if node.kind == "jar":
                fallback_match = _find_existing_jar_fallback(
                    aidp, uri, lib_repository_dir, expected_content=upload_content
                )
                if fallback_match:
                    fallback_path, fallback_digest = fallback_match
                    message = (
                        f"JAR upload errored; fallback used: {fallback_path} "
                        f"(sha256={fallback_digest}; failed target: {aidp_path}; error: {e})"
                    )
                    tprint(f"  [WARN] {message}")
                    node.aidp_fallback_path = fallback_path
                    node.aidp_fallback_sha256 = fallback_digest
                    node.aidp_fallback_provenance = "checksum matched DBX source bytes"
                    planned[uri] = fallback_path
                    uri_to_aidp[uri] = fallback_path
                    report.errors.append(f"WARN {message}")
                    report.files.append(FileResult(
                        dbx_source=uri,
                        aidp_path=fallback_path,
                        kind=node.kind,
                        status="SKIPPED_EXISTS",
                        size_bytes=len(upload_content),
                        failure_reason=message,
                    ))
                    continue
            # Only JARs are optional (operator can attach one manually later).
            # Wheels, init scripts and requirements.txt are mandatory deps:
            # a missing one silently produces ModuleNotFoundError / broken
            # cluster provisioning at runtime, so fail loud instead.
            if node.kind not in RECOVERABLE_FILE_KINDS or strict_jars:
                raise RuntimeError(
                    f"Cannot proceed: required {node.kind} upload failed: {aidp_path}: {e}")
            tprint(f"  [FAIL] upload: {aidp_path}: {e}")
            cluster_file_failed = True
            report.files.append(FileResult(
                dbx_source=uri, aidp_path=aidp_path, kind=node.kind,
                status="FAILED", failure_reason=str(e),
            ))
            continue

        # Read-back SHA validation against the bytes we actually uploaded
        # (which differs from node.content for rewritten init scripts).
        sha_local = file_sha256(upload_content)
        try:
            fetched = aidp.download_file(aidp_path)
            sha_remote = file_sha256(fetched)
        except AidpApiError as e:
            # The upload PUT already returned 200 — the read-back is an
            # independent verification GET that can fail transiently even when
            # the object is fine. So this is "unverified", NOT "broken": record
            # it at the boundary (report + exit code) but do NOT abort a
            # 200-confirmed upload over a verification blip. (A provable sha
            # mismatch below is different — that raises for mandatory kinds.)
            tprint(f"  [WARN] read-back of {aidp_path} failed ({e}); "
                   f"upload returned 200 but could not be verified")
            cluster_file_failed = True
            report.errors.append(f"file-readback-unverified: {aidp_path} ({node.kind}): {e}")
            report.files.append(FileResult(
                dbx_source=uri, aidp_path=aidp_path, kind=node.kind,
                status="UNVERIFIED", failure_reason=f"readback_unverified: {e}",
            ))
            continue
        if sha_remote != sha_local:
            if node.kind not in RECOVERABLE_FILE_KINDS:
                raise RuntimeError(
                    f"Cannot proceed: required {node.kind} upload corrupted "
                    f"(sha256 mismatch): {aidp_path}")
            tprint(f"  [FAIL] sha mismatch on {aidp_path}: {sha_local} != {sha_remote}")
            cluster_file_failed = True
            report.files.append(FileResult(
                dbx_source=uri, aidp_path=aidp_path, kind=node.kind,
                status="FAILED", failure_reason="sha256_mismatch",
            ))
            continue

        uri_to_aidp[uri] = aidp_path
        tprint(f"  [OK] file: {aidp_path}  ({len(upload_content)} bytes)")
        report.files.append(FileResult(
            dbx_source=uri, aidp_path=aidp_path, kind=node.kind,
            status="VALIDATED", sha256=sha_local, size_bytes=len(upload_content),
        ))

    return uri_to_aidp, cluster_file_failed


# ─── Phase 3 helpers ───────────────────────────────────────────────


# Cap on shape-limits heal cycles per cluster create. AIDP validates OCPUs
# before memory, so a shape with no limits table yet (e.g. gpu.a10) NEEDS two
# cycles: cycle 1 learns the valid OCPU list, cycle 2 learns the memory list
# for the corrected OCPU count; 3 leaves one spare. Termination is additionally
# guaranteed by learn_limits_and_renormalize()'s contract — it signals a retry
# only when the payload actually mutated, so identical retries can't loop.
MAX_LIMITS_HEAL_CYCLES = 3


def _is_limits_validation_error(error_text: str) -> bool:
    """True for AIDP create errors that state shape limits we can learn from.
    Delegates to cluster_shape_mapping.is_limits_validation_error — defined
    via the SAME regexes as the parser, so the detector and the learner can
    never drift apart when AIDP rewords its messages."""
    from .cluster_shape_mapping import is_limits_validation_error
    return is_limits_validation_error(error_text)


def _limits_startup_gate(args) -> "Optional[ExitCode]":
    """Shape-limits table, validated BEFORE any conversion.

    One global table serves every region — live-probed identical across
    us-ashburn-1 / us-phoenix-1 / ap-mumbai-1 / ap-tokyo-1 on 2026-07-30
    (precedence: --shape-limits-path > $AIDP_SHAPE_LIMITS_PATH > the
    committed config/aidp_shape_limits.json). Discovery is NEVER run
    automatically — it sends create-API probes and may create/delete
    clusters, so it requires explicit operator intent.

    The gate applies ONLY to runs that can create clusters: --notebook-only
    exits before Phase 3, and --cluster-key reuses an existing cluster for
    every task — neither consumes shape limits, so a broken table must not
    block them.

    Returns an ExitCode to fail the run, or None to continue.
    """
    can_create_clusters = not args.notebook_only and not args.cluster_key
    if not can_create_clusters:
        return None
    from . import cluster_shape_mapping as _csm
    if args.shape_limits_path:
        # One authoritative override channel: the flag feeds the same env
        # var the module already resolves — no module-global state needed.
        os.environ["AIDP_SHAPE_LIMITS_PATH"] = str(args.shape_limits_path)
    check = _csm.check_limits_table()
    if check["status"] == "broken":
        print(f"ERROR: shape-limits table {check['path']} is corrupt — "
              f"refusing to run (and it will never be overwritten). "
              f"Fix or delete it, or restore the committed "
              f"config/aidp_shape_limits.json.", file=sys.stderr)
        return ExitCode.BAD_ARGS
    if check["status"] == "missing":
        print(f"WARNING: no shape-limits table exists ({check['path']}). "
              f"Continuing with lazy API-error learning — cluster specs "
              f"pass through unvalidated and learned limits build the "
              f"table incrementally (marked partial). The committed "
              f"config/aidp_shape_limits.json covers the standard shapes.")
    return None


def _create_with_limits_heal(aidp, payload, tprint_fn, report):
    """aidp.create_cluster with bounded shape-limits self-healing.

    OCPU/memory are pre-snapped onto the limits table
    (config/aidp_shape_limits.json) by the converter, but that table can
    lag reality (AIDP platform update, or a shape with no probe data yet
    like gpu.a10). Each time the create
    400s with a shape-validation message ("Valid values are: [...]"), learn
    the real limits from the error text via
    cluster_converter.learn_limits_and_renormalize() — which persists them to
    the limits file and re-snaps the payload — and try again, up to
    MAX_LIMITS_HEAL_CYCLES times. Convergence typically takes one cycle
    (stale table) or two (unknown shape: OCPU list, then memory list).

    Raises the underlying AidpApiError unchanged when the error isn't a
    limits message, nothing new could be learned, or the cycle cap is hit.
    """
    for cycle in range(MAX_LIMITS_HEAL_CYCLES + 1):
        try:
            return aidp.create_cluster(payload)
        except AidpApiError as e:
            if (not _is_limits_validation_error(str(e))
                    or cycle >= MAX_LIMITS_HEAL_CYCLES):
                raise
            learned_lines = learn_limits_and_renormalize(payload, str(e))
            if not learned_lines:
                raise  # nothing new / no mutation — a retry would be identical
            for line in learned_lines:
                tprint_fn(f"    [shape][WARN]{line}")
            # "WARN " prefix: Report.summary() counts bare entries as errors;
            # a SUCCESSFUL recovery is a warning, not an error.
            report.errors.append(
                f"WARN shape-limits-learned: create 400 on cluster "
                f"displayName={payload.get('displayName')!r}; limits file "
                f"updated from the API error and payload re-normalized "
                f"(heal cycle {cycle + 1})"
            )
    raise RuntimeError("unreachable: heal loop must return or raise")


def _create_cluster_with_intel_fallback(aidp, payload, tprint_fn, report):
    """Create a cluster with both recovery mechanisms layered.

    Shape-limits self-healing is handled by _create_with_limits_heal (bounded
    multi-cycle — see there). This wrapper adds the intel.generic →
    amd.generic shape-NAME fallback on top:

    `intel.generic` is unverified against AIDP at code-write time (no Intel
    clusters exist in the workspace we probed to verify shape names; docs
    mention Intel as a supported family but don't spell the shape string).
    If AIDP rejects the create with "shape is not supported" AND the payload
    used intel.generic, retry with amd.generic per the "when unsure, assume
    AMD" fallback rule — and that retry is itself heal-capable, so a limits
    400 on the amd-substituted payload still self-heals. WARN loudly so
    operators know the fallback fired.

    Any other create failure propagates unchanged (fail-loud).
    """
    try:
        return _create_with_limits_heal(aidp, payload, tprint_fn, report)
    except AidpApiError as e:
        msg = str(e).lower()
        used_intel = (
            payload.get("driverConfig", {}).get("driverShape") == "intel.generic"
            or payload.get("workerConfig", {}).get("workerShape") == "intel.generic"
        )
        if used_intel and "shape is not supported" in msg:
            # Fallback to amd.generic. Every shape selector must be replaced:
            # driverConfig carries BOTH driverShape and driverNodeType (the
            # cluster-create schema encodes the driver shape in driverNodeType
            # too) — leaving either behind sends an internally inconsistent
            # payload with AMD shape fields alongside an Intel selector.
            replaced = []
            for cfg_key, shape_key in (("driverConfig", "driverShape"),
                                       ("driverConfig", "driverNodeType"),
                                       ("workerConfig", "workerShape")):
                if payload.get(cfg_key, {}).get(shape_key) == "intel.generic":
                    payload[cfg_key][shape_key] = "amd.generic"
                    replaced.append(f"{cfg_key}.{shape_key}")
            if "nodeType" in payload and payload["nodeType"] == "intel.generic":
                payload["nodeType"] = "amd.generic"
                replaced.append("nodeType")
            tprint_fn(f"    [shape][WARN] AIDP rejected intel.generic — retrying "
                      f"with amd.generic (replaced: {', '.join(replaced)}). "
                      f"Compute stays x86-class but architecture family "
                      f"differs — review if Intel-specific perf mattered.")
            # "WARN " prefix: a successful fallback must not inflate the
            # report's error count (Report.summary() severity convention).
            report.errors.append(
                f"WARN shape-fallback: intel.generic → amd.generic ({', '.join(replaced)}) "
                f"on cluster displayName={payload.get('displayName')!r}"
            )
            return _create_with_limits_heal(aidp, payload, tprint_fn, report)
        raise


# ─── Phase 3 — Cluster + Workflow ──────────────────────────────────

# Marker prefix used on report.errors entries for Spark tuning that could not
# be translated to an AIDP equivalent and had to be dropped. main() scans for
# it to return a non-zero exit code so the operator acknowledges the change.
SPARK_TUNING_DROP_MARKER = "spark-tuning-dropped"


def _report_dropped_conf(conv, workflow_name: str, report: Report) -> None:
    """Log a cluster conversion's dropped/translated Spark settings.

    - translated: intent was preserved via an AIDP-native equivalent → INFO.
    - tuning drops with no AIDP equivalent: escalate to WARN + report.errors
      (dropping loses adaptive-partitioning intent → possible regression/OOM).
    - everything else: low-signal [drop].
    """
    for k, v, note in conv.translated_conf:
        tprint(f"    [translate] {k}={v}: {note}")
    for k, v, why in conv.dropped_env + conv.dropped_conf:
        if "no AIDP equivalent" in why:
            tprint(f"    [WARN] {k}={v} dropped — AIDP will use its default; "
                   f"verify workload performance (tuning intent lost)")
            report.errors.append(
                f"{SPARK_TUNING_DROP_MARKER}: {workflow_name}: {k}={v} ({why})")
        else:
            tprint(f"    [drop] {k}={v}  ({why})")


def _record_workflow_conversion_decisions(
    workflow_name: str,
    decisions: List[str],
    report: Report,
) -> None:
    """Surface each explicit workflow conversion in logs and the run report."""
    for decision in decisions:
        if decision.startswith("WARN "):
            rendered = decision[len("WARN "):]
            tprint(f"  [workflow-parameter][WARN] {rendered}")
            report.errors.append(
                f"WARN workflow-parameter: {workflow_name}: {rendered}"
            )
        else:
            tprint(f"  [workflow-parameter] {decision}")
            report.errors.append(
                f"INFO workflow-parameter: {workflow_name}: {decision}"
            )


def _analyze_job_parameter_plan(
    graph: Graph,
    job_node,
    log=tprint,
) -> Any:
    """Build the static parameter plan used by preflight and Phase 3."""
    settings = job_node.job_def.get("settings") or job_node.job_def
    graph_notebooks = getattr(graph, "notebooks", {}) or {}
    notebooks_by_task: Dict[str, Any] = {}
    context_dependencies: Dict[str, List[Dict[str, Any]]] = {}
    children: Dict[str, List[str]] = {}
    for child_path, child in graph_notebooks.items():
        if child.role in {"run_dep", "task"}:
            for parent_path in child.referenced_by:
                children.setdefault(parent_path, []).append(child_path)
    for task in settings.get("tasks", []) or []:
        notebook = task.get("notebook_task") or {}
        path = (notebook.get("notebook_path") or "").strip()
        node = graph_notebooks.get(path)
        if node is not None and node.nb_dict is not None:
            notebooks_by_task[task.get("task_key", "")] = node.nb_dict
        seen, pending = {path}, list(children.get(path, []))
        while pending:
            child_path = pending.pop()
            if child_path in seen:
                continue
            seen.add(child_path)
            child = graph_notebooks[child_path]
            if child.nb_dict is not None:
                context_dependencies.setdefault(task.get("task_key", ""), []).append(child.nb_dict)
            pending.extend(children.get(child_path, []))
    return analyze_task_params(
        settings, notebooks_by_task, log=log, context_dependencies=context_dependencies,
    )


def _task_support_preflight_checks(graph: Graph) -> List[Dict[str, str]]:
    """Predict Phase-3 task-conversion refusals for every fetched job.

    Shares task_conversion_refusal with the converter so preflight and Phase 3
    can never disagree. Shaped like the override preflight checks (job_id/
    name/status/detail) so the existing refused_checks reporting, the dry-run
    exit code, and the report ERROR lines all apply unchanged.
    """
    checks: List[Dict[str, str]] = []
    for job_id, job_node in sorted(graph.jobs.items()):
        if not (job_node and job_node.job_def):
            continue
        settings = job_node.job_def.get("settings") or job_node.job_def
        name = settings.get("name") or f"dbx_job_{job_id}"
        for t in settings.get("tasks", []) or []:
            reason = task_conversion_refusal(t)
            if reason:
                checks.append({
                    "job_id": str(job_id),
                    "name": str(name),
                    "status": "WILL REFUSE",
                    "detail": reason,
                })
    return checks


def _workflow_override_preflight_checks(
    graph: Graph,
    root_job_id: str,
    workflow_parameter_overrides: Optional[Dict[str, str]],
) -> List[Dict[str, str]]:
    """Evaluate override semantics for every workflow without AIDP writes."""
    if not workflow_parameter_overrides:
        return []
    ordered = []
    root = graph.jobs.get(root_job_id)
    if root is not None:
        ordered.append((root_job_id, root))
    ordered.extend(
        (job_id, node) for job_id, node in sorted(graph.jobs.items())
        if job_id != root_job_id
    )
    checks: List[Dict[str, str]] = []
    for job_id, job_node in ordered:
        if not (job_node and job_node.job_def):
            continue
        settings = job_node.job_def.get("settings") or job_node.job_def
        name = settings.get("name") or f"dbx_job_{job_id}"
        try:
            warnings = validate_workflow_parameter_overrides(
                settings,
                workflow_parameter_overrides,
                _analyze_job_parameter_plan(graph, job_node, log=None),
            )
        except Exception as exc:
            checks.append({
                "job_id": str(job_id),
                "name": str(name),
                "status": "WILL REFUSE",
                "detail": str(exc),
            })
            continue
        for warning in warnings:
            checks.append({
                "job_id": str(job_id),
                "name": str(name),
                "status": "WARN",
                "detail": warning[len("WARN "):] if warning.startswith("WARN ") else warning,
            })
    return checks


def _find_resource_by_current_or_legacy_name(find_by_name, name, legacy_name, kind):
    """Prefer the current identifier; reuse exact pre-#268 resources on misses.

    Lookup errors propagate: an unavailable lookup is not proof of absence
    and must never lead to creating a duplicate. Both matches are exact.
    """
    existing = find_by_name(name)
    if existing or legacy_name == name:
        return existing, name
    existing = find_by_name(legacy_name)
    if existing:
        tprint(
            f"  [LEGACY] {kind} matched: {legacy_name} "
            f"(key={existing.get('key')}); new name={name}"
        )
        return existing, legacy_name
    return None, name


def _find_existing_cluster(aidp, source_name, prefix):
    existing, _ = _find_resource_by_current_or_legacy_name(
        aidp.find_cluster_by_name,
        sanitize_cluster_name(source_name, prefix),
        sanitizer.legacy_cluster_name(source_name, prefix),
        "cluster",
    )
    return existing


def phase3_create_clusters_and_workflows(
    aidp: AidpClient,
    dbx: DbxClient,
    graph: Graph,
    nb_target_paths: Dict[str, str],
    uri_to_aidp: Dict[str, str],
    export_output: str,
    prefix: str,
    overwrite_workflow: bool,
    cluster_override_key: Optional[str],
    report: Report,
    force_pause_schedule: bool = True,
    cfg=None,
    lib_repository_dir: str = DEFAULT_AIDP_LIB_DIR,
    workflow_parameter_overrides: Optional[Dict[str, str]] = None,
) -> None:
    """For each job in the graph, build cluster registry + workflow payload, create."""
    # Validate exact customer-pack artifacts before touching any target cluster.
    try:
        graph, pack_contracts = prepare_cluster_library_rules(
            aidp, graph, cfg, lib_repository_dir, uri_to_aidp, tprint, report,
        )
        ray_init_contracts = prepare_ray_cluster_init_script_rules(
            aidp, graph, cfg, tprint, report,
        )
    except (ValueError, OSError, AidpApiError) as exc:
        report.errors.append(f"ERROR customer-pack-cluster-library: {exc}")
        for job_id, node in graph.jobs.items():
            settings = (node.job_def or {}).get("settings") or node.job_def or {}
            report.jobs.append(JobResult(
                dbx_job_id=job_id, dbx_job_name=settings.get("name", "<unfetched>"),
                aidp_workflow_name=sanitize_workflow_name(settings.get("name", job_id), prefix),
                status="FAILED", failure_reason=str(exc),
            ))
        tprint(f"[customer-pack][ERROR] {exc}")
        return
    try:
        init_contracts = prepare_cluster_init_script_rules(
            aidp, graph, cfg, tprint, report,
        )
    except (ValueError, OSError, AidpApiError) as exc:
        report.errors.append(f"ERROR customer-pack-cluster-init-script: {exc}")
        for job_id, node in graph.jobs.items():
            settings = (node.job_def or {}).get("settings") or node.job_def or {}
            report.jobs.append(JobResult(
                dbx_job_id=job_id, dbx_job_name=settings.get("name", "<unfetched>"),
                aidp_workflow_name=sanitize_workflow_name(settings.get("name", job_id), prefix),
                status="FAILED", failure_reason=str(exc),
            ))
        tprint(f"[customer-pack][ERROR] {exc}")
        return
    for job_id, job_node in graph.jobs.items():
        if can_skip_absent_soft_match(job_node):
            # Only definitive absence permits a soft skip. Lookup failures
            # that cannot establish absence retain the ordinary FAILED path.
            tprint(f"  [SKIP] soft-matched job {job_id} confirmed absent "
                   f"({job_node.fetch_error or 'no_job_def'}); see WARN soft-match-job-unresolved")
            report.jobs.append(JobResult(
                dbx_job_id=job_id, dbx_job_name="<unfetched>",
                aidp_workflow_name="<n/a>", status="SKIPPED_SOFT_MATCH",
                failure_reason=job_node.fetch_error or "no_job_def",
            ))
            continue
        if job_node.job_def is None:
            tprint(f"  [SKIP] no job def for {job_id}")
            report.jobs.append(JobResult(
                dbx_job_id=job_id, dbx_job_name="<unfetched>",
                aidp_workflow_name="<n/a>", status="FAILED",
                failure_reason=job_node.fetch_error or "no_job_def",
            ))
            continue
        settings = job_node.job_def.get("settings") or job_node.job_def
        dbx_name = settings.get("name") or f"dbx_job_{job_id}"
        aidp_workflow_name = sanitize_workflow_name(dbx_name, prefix)

        if cluster_override_key and any(
            contract["dbx_job_id"] == str(job_id) for contract in pack_contracts.values()
        ):
            reason = "customer-pack library rule requires its actual job cluster; cluster override is forbidden"
            report.errors.append(f"ERROR customer-pack-cluster-library: {reason}")
            report.jobs.append(JobResult(
                dbx_job_id=job_id, dbx_job_name=dbx_name,
                aidp_workflow_name=aidp_workflow_name, status="FAILED", failure_reason=reason,
            ))
            tprint(f"[customer-pack][ERROR] {reason}")
            continue
        if cluster_override_key and any(key[0] == str(job_id) for key in init_contracts):
            reason = "customer-pack init script requires its actual job cluster; cluster override is forbidden"
            report.errors.append(f"ERROR customer-pack-cluster-init-script: {reason}")
            report.jobs.append(JobResult(
                dbx_job_id=job_id, dbx_job_name=dbx_name,
                aidp_workflow_name=aidp_workflow_name, status="FAILED", failure_reason=reason,
            ))
            tprint(f"[customer-pack][ERROR] {reason}")
            continue
        if cluster_override_key and any(key[0] == str(job_id) for key in ray_init_contracts):
            reason = "Ray init script requires its actual job cluster; cluster override is forbidden"
            report.errors.append(f"ERROR ray-cluster-init-script: {reason}")
            report.jobs.append(JobResult(
                dbx_job_id=job_id, dbx_job_name=dbx_name,
                aidp_workflow_name=aidp_workflow_name,
                status="FAILED", failure_reason=reason,
            ))
            tprint(f"[ray-cluster][ERROR] {reason}")
            continue

        # These refusals depend only on source metadata, the static parameter
        # plan, and explicit overrides. Validate before Step A so a refused
        # workflow cannot leave a cluster or baseline libraries behind.
        # Task-support refusals share that contract — same single source of
        # truth as conversion and the Phase-1 preflight (an unsupported task
        # previously surfaced only AFTER Step A provisioned clusters).
        task_refusals = [
            reason for reason in (
                task_conversion_refusal(t)
                for t in settings.get("tasks", []) or []
            ) if reason
        ]
        if task_refusals:
            reason = "; ".join(task_refusals)
            tprint(f"  [FAIL] workflow conversion {aidp_workflow_name}: {reason}")
            report.jobs.append(JobResult(
                dbx_job_id=job_id, dbx_job_name=dbx_name,
                aidp_workflow_name=aidp_workflow_name,
                status="FAILED", failure_reason=reason, cluster_status="",
            ))
            report.errors.append(
                f"ERROR workflow-conversion: {aidp_workflow_name}: {reason}"
            )
            continue
        try:
            param_plan = _analyze_job_parameter_plan(graph, job_node)
            validate_workflow_parameter_overrides(
                settings,
                workflow_parameter_overrides,
                param_plan,
            )
        except ValueError as exc:
            reason = str(exc)
            tprint(
                f"  [FAIL] workflow conversion {aidp_workflow_name}: {reason}"
            )
            report.jobs.append(JobResult(
                dbx_job_id=job_id,
                dbx_job_name=dbx_name,
                aidp_workflow_name=aidp_workflow_name,
                status="FAILED",
                failure_reason=reason,
                cluster_status="",
            ))
            report.errors.append(
                f"ERROR workflow-conversion: {aidp_workflow_name}: {reason}"
            )
            continue

        # Reject competing assemblies on reused clusters before any attachment.
        conflicting_pack = False
        for contract in pack_contracts.values():
            if contract["dbx_job_id"] != str(job_id):
                continue
            try:
                jc = next(c for c in settings["job_clusters"]
                          if c.get("job_cluster_key") == contract["job_cluster_key"])
                spec = jc.get("new_cluster") or {}
                display = jc.get("cluster_name") or spec.get("cluster_name") or jc["job_cluster_key"]
                existing_pack_cluster = _find_existing_cluster(aidp, display, prefix)
                if existing_pack_cluster:
                    check_cluster_library_conflicts(aidp, existing_pack_cluster["key"], contract)
            except (ValueError, AidpApiError) as exc:
                conflicting_pack = True
                report.errors.append(f"ERROR customer-pack-cluster-library: {exc}")
                report.jobs.append(JobResult(
                    dbx_job_id=job_id, dbx_job_name=dbx_name,
                    aidp_workflow_name=aidp_workflow_name, status="FAILED", failure_reason=str(exc),
                ))
                tprint(f"[customer-pack][ERROR] {exc}")
                break
        if conflicting_pack:
            continue

        # Validate every reused cluster covered by an init-script contract
        # before Step A creates or mutates any sibling cluster for this job.
        invalid_init_script = False
        for (contract_job_id, contract_cluster_key), init_rule in init_contracts.items():
            if contract_job_id != str(job_id):
                continue
            try:
                jc = next(
                    cluster for cluster in settings.get("job_clusters", [])
                    if cluster.get("job_cluster_key") == contract_cluster_key
                )
                spec = jc.get("new_cluster") or {}
                display = (
                    jc.get("cluster_name")
                    or spec.get("cluster_name")
                    or contract_cluster_key
                )
                existing_init_cluster = _find_existing_cluster(aidp, display, prefix)
                if existing_init_cluster:
                    verify_existing_cluster_init_script(
                        aidp, existing_init_cluster["key"], init_rule
                    )
            except (ValueError, AidpApiError) as exc:
                invalid_init_script = True
                failure_reason = (
                    f"{exc}; no cluster override/fallback for {contract_cluster_key}"
                )
                reason = f"customer-pack-cluster-init-script: {failure_reason}"
                report.errors.append(f"ERROR {reason}")
                report.jobs.append(JobResult(
                    dbx_job_id=job_id,
                    dbx_job_name=dbx_name,
                    aidp_workflow_name=aidp_workflow_name,
                    status="FAILED",
                    failure_reason=failure_reason,
                ))
                tprint(f"[customer-pack][ERROR] {failure_reason}")
                break
        if invalid_init_script:
            continue

        # Step A: clusters. Build dbx_cluster_key → aidp_cluster_key registry.
        dbx_key_to_aidp_key: Dict[str, str] = {}
        cluster_status_msgs = []
        cluster_create_failed = False
        ray_init_failure = None

        for jc in settings.get("job_clusters", []) or []:
            jc_key = jc.get("job_cluster_key")
            new_cluster = jc.get("new_cluster") or {}
            jar_context = f"job:{job_id}/job_cluster:{jc_key}"
            init_rule = init_contracts.get((str(job_id), jc_key))
            ray_context = f"job:{job_id}/job_cluster:{jc_key}"
            ray_init_rule = ray_init_contracts.get((str(job_id), ray_context))
            # Fallback order: explicit cluster_name on the JobCluster, then
            # on the new_cluster, then the job_cluster_key (already a unique
            # identifier inside the DBX spec — usually descriptive enough).
            cluster_display = (
                jc.get("cluster_name")
                or new_cluster.get("cluster_name")
                or jc_key
                or f"{dbx_name}_default"
            )
            target_cluster_name = sanitize_cluster_name(cluster_display, prefix)

            if cluster_override_key:
                jar_count = _install_context_jars_on_cluster(
                    aidp, cluster_override_key, graph, jar_context,
                    uri_to_aidp, tprint=tprint, report=report,
                )
                tprint(
                    f"    [jar-discovery] attached {jar_count} JAR(s) to "
                    f"override cluster {cluster_override_key}"
                )
                dbx_key_to_aidp_key[jc_key] = cluster_override_key
                cluster_status_msgs.append(f"{jc_key} → REUSED override {cluster_override_key}")
                continue

            # Reuse if the tool-owned cluster already exists. Reconcile only
            # notebook-discovered JARs; preserve its compute configuration.
            existing = _find_existing_cluster(aidp, cluster_display, prefix)
            if existing:
                target_cluster_name = existing.get("displayName") or target_cluster_name
                if ray_init_rule:
                    try:
                        verify_existing_ray_init_script(aidp, existing["key"], ray_init_rule)
                    except (ValueError, AidpApiError) as exc:
                        cluster_create_failed = True
                        reason = f"ray-cluster-init-script: {exc}"
                        ray_init_failure = reason
                        tprint(f"    [FAIL] {reason}")
                        report.errors.append(f"ERROR {reason}")
                        cluster_status_msgs.append(f"{jc_key} → FAILED ({reason})")
                        continue
                jar_count = _install_context_jars_on_cluster(
                    aidp, existing.get("key"), graph, jar_context,
                    uri_to_aidp, tprint=tprint, report=report,
                )
                tprint(
                    f"    [jar-discovery] attached {jar_count} JAR(s) to "
                    f"reused cluster {existing.get('key')}"
                )
                dbx_key_to_aidp_key[jc_key] = existing.get("key")
                cluster_status_msgs.append(f"{jc_key} → REUSED {target_cluster_name} ({existing.get('key')})")
                continue

            # Convert + create. Attach init scripts that were uploaded.
            try:
                conv = convert_dbx_cluster(
                    new_cluster=new_cluster,
                    cluster_display_name=cluster_display,
                    prefix=prefix,
                    cfg=cfg,
                )
            except ShapeCapacityError as e:
                # The DBX spec asks for more than the AIDP shape can provide.
                # Fail THIS cluster loudly (never clamp down — that defers
                # the failure to a runtime OOM) but keep exporting the rest.
                cluster_create_failed = True
                tprint(f"    [shape][ERROR] {jc_key}: {e}")
                cluster_status_msgs.append(f"{jc_key} → FAILED (shape capacity: {e})")
                report.errors.append(
                    f"ERROR shape-capacity: {aidp_workflow_name}/{jc_key}: {e}")
                continue
            if init_rule:
                try:
                    attach_cluster_init_script(conv.aidp_payload, init_rule)
                except ValueError as exc:
                    cluster_create_failed = True
                    reason = f"customer-pack-cluster-init-script: {exc}"
                    tprint(f"    [FAIL] {reason}")
                    report.errors.append(f"ERROR {reason}")
                    cluster_status_msgs.append(f"{jc_key} -> FAILED ({reason})")
                    continue
                tprint(f"    [init-scripts] attached customer-pack script {init_rule['destination']}")
            if ray_init_rule:
                try:
                    attach_ray_init_script(conv.aidp_payload, ray_init_rule)
                except ValueError as exc:
                    cluster_create_failed = True
                    reason = f"ray-cluster-init-script: {exc}"
                    ray_init_failure = reason
                    tprint(f"    [FAIL] {reason}")
                    report.errors.append(f"ERROR {reason}")
                    cluster_status_msgs.append(f"{jc_key} → FAILED ({reason})")
                    continue
                tprint(f"    [init-scripts] attached Ray script {ray_init_rule['destination']}")
            # PR #30: driver + worker shape-mapping decisions so operators see
            # DBX → AIDP node-type conversion before the cluster is created.
            # Ambiguous/family-suffix mappings are prefixed [shape][WARN] by
            # format_mapping_log_line().
            for _line in conv.shape_mapping_log_lines:
                tprint(f"    {_line}")
            for _res, _role in ((conv.driver_shape_res, "driver"),
                                (conv.worker_shape_res, "worker")):
                if _res is None:
                    continue
                _severity = "WARN" if (_res.ambiguous or _res.resolution != "catalog") else "INFO"
                report.errors.append(
                    f"{_severity} shape-mapping: {jc_key}/{_role}: "
                    f"{_res.dbx_node_type or '(none)'} ({_res.dbx_arch}, "
                    f"{_res.dbx_vcpu}vCPU/{_res.dbx_mem_gb}GB) → "
                    f"{_res.aidp_shape} ({_res.ocpus}OCPU/{_res.memory_gb}GB) "
                    f"[{_res.resolution}{'; ambiguous' if _res.ambiguous else ''}]"
                )
            # PR #31: nuanced drop reporting — [translate] for AQE-preserved
            # intent, [WARN] + report.errors for un-translatable tuning drops,
            # [drop] for benign DBX-only removals.
            _report_dropped_conf(conv, aidp_workflow_name, report)
            # Drop init script refs whose content became a no-op after
            # rewriting (pure JAR-copy scripts — covered by libraries API).
            conv.init_script_refs = [
                u for u in conv.init_script_refs
                if not (u in graph.files and graph.files[u].skip_attach)
            ]
            # ─── Init scripts: UPLOAD + LOG, but do NOT attach to cluster ───
            # Rationale: init scripts are highly environment-specific (S3 paths,
            # DBX-only /databricks/... targets, custom bootstrap steps) and
            # frequently fail on the target AIDP environment even after our
            # in-script rewrites. Historically attaching them made the freshly
            # created AIDP cluster go straight to FAILED state on init-script
            # error (observed on a pilot-customer AutoRefit export 2026-07-25 where
            # copy_resources_from_s3.sh exited code 1 and blocked all migration).
            # We now upload the scripts under <target-base>/init_scripts/ so the
            # operator can review + attach manually if needed, but leave the
            # cluster payload's clusterRuntimeConfig.initScripts = [] so cluster
            # creation is decoupled from init-script correctness.
            # See also: attach_init_scripts() still exists in cluster_converter.py
            # for callers that opt in explicitly (none in-tree at the moment).
            if conv.init_script_refs:
                tprint(f"    [init-scripts] {len(conv.init_script_refs)} script(s) uploaded to workspace but NOT attached to cluster")
                tprint(f"    [init-scripts] (environment-specific; review at target path and attach manually if needed)")
                for _uri in conv.init_script_refs:
                    _aidp = uri_to_aidp.get(_uri, "<not uploaded>")
                    tprint(f"    [init-scripts]   {_uri} -> {_aidp}")
            # Add init-script-discovered JARs/wheels to the cluster's libraries
            # so they get installed via the AIDP libraries API (the DBX-only
            # `cp ... /databricks/jars/` lines are commented out by the init
            # script rewriter — this is the AIDP-native replacement).
            _existing_lib_uris = set()
            for _lib in conv.library_refs:
                for _k in ("jar", "whl", "egg", "requirements"):
                    if _k in _lib and _lib[_k]:
                        _existing_lib_uris.add(_lib[_k])
            for _uri, _node in graph.files.items():
                if _node.kind not in ("jar", "whl") or _uri in _existing_lib_uris:
                    continue
                if not any(r.startswith("init_script:") for r in _node.referenced_by):
                    continue
                # Init-script artifacts are owned by the execution cluster(s)
                # resolved during jar discovery.  Do not spray a child-job
                # JAR onto every fresh root cluster merely because it exists in
                # the shared graph.
                if jar_context not in _node.referenced_by:
                    continue
                conv.library_refs.append({_node.kind: _uri})
                _aidp_path = uri_to_aidp.get(_uri, "<not uploaded>")
                tprint(f"    [lib-from-init] adding {_node.kind} to cluster libraries: {_aidp_path}")
            # Fix 7c: attach the synthetic requirements FileNode (generated
            # from notebook %pip install cells) to this cluster so its packages
            # are pre-installed at provision time — before ANY notebook cell
            # runs. Same shape as the init-script-driven lib pull above.
            # The URI prefix `/dbx_export_generated/pip_deps_` is the marker
            # (see discover_notebook_pip_deps for how the URI is constructed).
            _requirements_context = f"job:{job_id}/job_cluster:{jc_key}"
            _canonical_uri = _attach_only_consolidated_requirements(
                conv, graph, _requirements_context
            )
            _jar_uris = _attach_context_jars(conv, graph, jar_context)
            if _jar_uris:
                tprint(
                    f"    [jar-discovery] attaching {len(_jar_uris)} "
                    f"notebook-resolved JAR(s) for {jar_context}"
                )
            tprint(
                "    [requirements] attaching cluster-specific resolved file: "
                f"{uri_to_aidp.get(_canonical_uri, '<not uploaded>')}"
            )
            try:
                created = _create_cluster_with_intel_fallback(
                    aidp, conv.aidp_payload, tprint, report,
                )
                aidp_key = created.get("key")
                dbx_key_to_aidp_key[jc_key] = aidp_key
                cluster_status_msgs.append(f"{jc_key} → CREATED {target_cluster_name} ({aidp_key})")
                # Install file-backed libs (best effort; non-fatal).
                items = build_library_install_items(conv.library_refs, uri_to_aidp)
                if items and aidp_key:
                    try:
                        aidp.install_cluster_libraries(aidp_key, items)
                        tprint(f"    libraries installed on {aidp_key}: {len(items)} items")
                    except AidpApiError as e:
                        tprint(f"    [WARN] library install on {aidp_key} failed: {e}")
                        report.errors.append(f"library-install: {aidp_key}: {e}")

                # ── Fix 7k (2026-07-25): install baseline migration-tool libs ──
                # Every freshly-created cluster needs the migration-tool's
                # baseline JARs. Target Python dependencies are already in the
                # cluster-specific consolidated requirements file above.
                # Historically these were only installed when `job_migrate` (or
                # `job_migrate_from_workflow`) ran — via
                # `cluster_lifecycle.ensure_baseline_jars_installed()`. That
                # meant every fresh cluster came up without them; the operator
                # had to either invoke setup separately or wait for the
                # migration-start hook. Fix 7k moves the install to
                # cluster-creation time so a dbx_export cluster is READY
                # immediately.
                #
                # Idempotency: `cluster_lifecycle.ensure_baseline_jars_installed`
                # at migration start still runs — it checks current status and
                # SKIPs if the baseline is already INSTALLED/RESOLVED/
                # INSTALL_ON_RESTART. So no duplicate installs.
                #
                # JAR basenames are duplicated from
                # `scripts/cluster_lifecycle.py::BASELINE_JARS` — same values, kept in
                # sync manually because `scripts/` isn't a Python package
                # (no __init__.py) so cross-module import from dbx_export
                # would require sys.path manipulation. If the source ever
                # changes, update both spots. The DIRECTORY comes from the
                # YAML-configured lib_repository_dir (same value cluster_lifecycle
                # resolves), so both sides honor a customer-configured
                # repository. The runtime install is idempotent, so a drift is
                # self-correcting on next migration run.
                _BASELINE_TOOL_LIBS = [
                    f"{lib_repository_dir.rstrip('/')}/hudi-spark3.5-bundle_2.12-0.15.0-3975ca4.jar",
                    f"{lib_repository_dir.rstrip('/')}/scala-logging_2.12-3.9.5.jar",
                ]
                if aidp_key:
                    _baseline_items = [
                        {"operation": "INSTALL", "type": "WORKSPACE_FILE", "path": p}
                        for p in _BASELINE_TOOL_LIBS
                    ]
                    try:
                        aidp.install_cluster_libraries(aidp_key, _baseline_items)
                        tprint(f"    [baseline] tool baseline libs installed on {aidp_key}: "
                               f"{len(_baseline_items)} items ({', '.join(p.rsplit('/', 1)[-1] for p in _BASELINE_TOOL_LIBS)})")
                    except AidpApiError as e:
                        # Non-fatal — migration-start hook
                        # (ensure_baseline_jars_installed) will retry. But log
                        # loudly so the operator can spot a systemic issue.
                        tprint(f"    [baseline] [WARN] baseline lib install on {aidp_key} failed: {e}")
                        report.errors.append(f"baseline-lib-install: {aidp_key}: {e}")
            except ShapeCapacityError as e:
                # Self-heal parsed API-stated limits that prove the request
                # impossible for this shape — fail, never clamp down.
                cluster_create_failed = True
                tprint(f"    [shape][ERROR] {jc_key}: {e}")
                cluster_status_msgs.append(f"{jc_key} → FAILED (shape capacity: {e})")
                report.errors.append(
                    f"ERROR shape-capacity: {aidp_workflow_name}/{jc_key}: {e}")
                continue
            except AidpApiError as e:
                cluster_create_failed = True
                tprint(f"    [FAIL] cluster create {target_cluster_name}: {e}")
                cluster_status_msgs.append(f"{jc_key} → FAILED ({e})")

        # Step A1: task-level new_cluster definitions. Databricks permits a
        # cluster spec directly on a task instead of under job_clusters[].
        # Create the equivalent AIDP cluster and attach this task context's
        # canonical requirements file just like a normal job cluster.
        dbx_task_to_aidp_key: Dict[str, str] = {}
        for task in settings.get("tasks", []) or []:
            task_key = task.get("task_key")
            if not task_key or not task.get("new_cluster"):
                continue
            jar_context = f"job:{job_id}/task_cluster:{task_key}"
            ray_init_rule = ray_init_contracts.get((str(job_id), jar_context))
            if cluster_override_key:
                jar_count = _install_context_jars_on_cluster(
                    aidp, cluster_override_key, graph, jar_context,
                    uri_to_aidp, tprint=tprint, report=report,
                )
                tprint(
                    f"    [jar-discovery] attached {jar_count} JAR(s) to "
                    f"override cluster {cluster_override_key}"
                )
                dbx_task_to_aidp_key[task_key] = cluster_override_key
                cluster_status_msgs.append(
                    f"task_cluster {task_key} -> REUSED override {cluster_override_key}"
                )
                continue
            task_cluster_spec = dict(task.get("new_cluster") or {})
            task_cluster_spec["libraries"] = [
                *(task_cluster_spec.get("libraries") or []),
                *(task.get("libraries") or []),
            ]
            cluster_display = (
                task_cluster_spec.get("cluster_name")
                or f"{dbx_name}_{task_key}_cluster"
            )
            target_cluster_name = sanitize_cluster_name(cluster_display, prefix)
            existing = _find_existing_cluster(aidp, cluster_display, prefix)
            if existing:
                target_cluster_name = existing.get("displayName") or target_cluster_name
                existing_key = existing.get("key")
                if ray_init_rule:
                    try:
                        verify_existing_ray_init_script(
                            aidp, existing_key, ray_init_rule
                        )
                    except (ValueError, AidpApiError) as exc:
                        cluster_create_failed = True
                        reason = f"ray-cluster-init-script: {exc}"
                        ray_init_failure = reason
                        tprint(f"    [FAIL] {reason}")
                        report.errors.append(f"ERROR {reason}")
                        cluster_status_msgs.append(
                            f"task_cluster {task_key} -> FAILED ({reason})"
                        )
                        continue
                context = f"job:{job_id}/task_cluster:{task_key}"
                jar_count = _install_context_jars_on_cluster(
                    aidp, existing_key, graph, context, uri_to_aidp,
                    tprint=tprint, report=report,
                )
                tprint(
                    f"    [jar-discovery] attached {jar_count} JAR(s) to "
                    f"reused task cluster {existing_key}"
                )
                canonical = [
                    uri for uri, node in graph.files.items()
                    if node.kind == "requirements"
                    and uri.startswith(CANONICAL_REQUIREMENTS_PREFIX)
                    and context in node.referenced_by
                ]
                if len(canonical) != 1:
                    raise RequirementConsolidationError(
                        f"expected one consolidated requirements artifact for "
                        f"{context}, found {canonical}"
                    )
                canonical_path = uri_to_aidp.get(canonical[0])
                if not canonical_path:
                    raise RequirementConsolidationError(
                        f"canonical requirements for {context} were not uploaded: "
                        f"{canonical[0]}"
                    )
                try:
                    aidp.install_cluster_libraries(existing_key, [{
                        "operation": "INSTALL",
                        "type": "WORKSPACE_FILE",
                        "path": canonical_path,
                    }])
                except AidpApiError as exc:
                    cluster_create_failed = True
                    message = (
                        f"task cluster {task_key} reuse requirements attach "
                        f"failed on {existing_key}: {exc}"
                    )
                    tprint(f"    [FAIL] {message}")
                    cluster_status_msgs.append(
                        f"task_cluster {task_key} -> FAILED ({message})"
                    )
                    report.errors.append(f"library-install: {message}")
                    continue
                dbx_task_to_aidp_key[task_key] = existing_key
                cluster_status_msgs.append(
                    f"task_cluster {task_key} -> REUSED {target_cluster_name} "
                    f"({existing_key}); canonical requirements attached"
                )
                continue
            try:
                conv = convert_dbx_cluster(
                    new_cluster=task_cluster_spec,
                    cluster_display_name=cluster_display,
                    prefix=prefix,
                    cfg=cfg,
                )
                if ray_init_rule:
                    try:
                        attach_ray_init_script(conv.aidp_payload, ray_init_rule)
                    except ValueError as exc:
                        cluster_create_failed = True
                        reason = f"ray-cluster-init-script: {exc}"
                        ray_init_failure = reason
                        tprint(f"    [FAIL] {reason}")
                        report.errors.append(f"ERROR {reason}")
                        cluster_status_msgs.append(
                            f"task_cluster {task_key} -> FAILED ({reason})"
                        )
                        continue
                    tprint(
                        f"    [init-scripts] attached Ray script "
                        f"{ray_init_rule['destination']}"
                    )
                _report_dropped_conf(conv, aidp_workflow_name, report)
                context = f"job:{job_id}/task_cluster:{task_key}"
                canonical_uri = _attach_only_consolidated_requirements(
                    conv, graph, context
                )
                jar_uris = _attach_context_jars(conv, graph, context)
                if jar_uris:
                    tprint(
                        f"    [jar-discovery] attaching {len(jar_uris)} "
                        f"notebook-resolved JAR(s) for {context}"
                    )
                tprint(
                    "    [requirements] attaching task-cluster resolved file: "
                    f"{uri_to_aidp.get(canonical_uri, '<not uploaded>')}"
                )
                created = _create_cluster_with_intel_fallback(
                    aidp, conv.aidp_payload, tprint, report
                )
                aidp_key = created.get("key")
                dbx_task_to_aidp_key[task_key] = aidp_key
                items = build_library_install_items(conv.library_refs, uri_to_aidp)
                if items and aidp_key:
                    aidp.install_cluster_libraries(aidp_key, items)
                cluster_status_msgs.append(
                    f"task_cluster {task_key} -> CREATED {target_cluster_name} "
                    f"({aidp_key})"
                )
            except (ShapeCapacityError, AidpApiError, RequirementConsolidationError) as exc:
                cluster_create_failed = True
                tprint(f"    [FAIL] task cluster {task_key}: {exc}")
                cluster_status_msgs.append(
                    f"task_cluster {task_key} -> FAILED ({exc})"
                )
                report.errors.append(
                    f"task-cluster-create: {aidp_workflow_name}/{task_key}: {exc}"
                )

        # Step A2: existing_cluster_id references. DBX tasks can directly point
        # at a standalone (non-job-scoped) DBX cluster by ID. For each unique
        # one referenced by any task, fetch its spec from DBX, convert it, and
        # create / reuse on AIDP — then map the DBX ID to the AIDP cluster key
        # so convert_dbx_task can resolve them.
        dbx_existing_to_aidp_key: Dict[str, str] = {}
        existing_ids = sorted({
            t.get("existing_cluster_id")
            for t in settings.get("tasks", [])
            if t.get("existing_cluster_id")
        })
        for existing_id in existing_ids:
            jar_context = f"existing_cluster:{existing_id}"
            ray_init_rule = ray_init_contracts.get((str(job_id), jar_context))
            if cluster_override_key:
                jar_count = _install_context_jars_on_cluster(
                    aidp, cluster_override_key, graph, jar_context,
                    uri_to_aidp, tprint=tprint, report=report,
                )
                tprint(
                    f"    [jar-discovery] attached {jar_count} JAR(s) to "
                    f"override cluster {cluster_override_key}"
                )
                dbx_existing_to_aidp_key[existing_id] = cluster_override_key
                cluster_status_msgs.append(f"existing_cluster_id {existing_id} → REUSED override {cluster_override_key}")
                continue
            try:
                dbx_cluster = dbx.get_cluster(existing_id)
            except DbxExportError as e:
                tprint(f"    [WARN] could not fetch DBX cluster {existing_id}: {e.reason}")
                cluster_status_msgs.append(f"existing_cluster_id {existing_id} → FETCH_FAILED ({e.reason})")
                continue
            if not dbx_cluster:
                tprint(f"    [WARN] DBX cluster {existing_id} returned None")
                cluster_status_msgs.append(f"existing_cluster_id {existing_id} → NOT_FOUND")
                continue
            cluster_display = dbx_cluster.get("cluster_name") or f"existing_{existing_id}"
            target_cluster_name = sanitize_cluster_name(cluster_display, prefix)
            already_on_aidp = _find_existing_cluster(aidp, cluster_display, prefix)
            if already_on_aidp:
                target_cluster_name = already_on_aidp.get("displayName") or target_cluster_name
                if ray_init_rule:
                    try:
                        verify_existing_ray_init_script(
                            aidp, already_on_aidp.get("key"), ray_init_rule
                        )
                    except (ValueError, AidpApiError) as exc:
                        cluster_create_failed = True
                        reason = f"ray-cluster-init-script: {exc}"
                        ray_init_failure = reason
                        tprint(f"    [FAIL] {reason}")
                        report.errors.append(f"ERROR {reason}")
                        cluster_status_msgs.append(
                            f"existing_cluster_id {existing_id} -> FAILED ({reason})"
                        )
                        continue
                jar_count = _install_context_jars_on_cluster(
                    aidp, already_on_aidp.get("key"), graph, jar_context,
                    uri_to_aidp, tprint=tprint, report=report,
                )
                tprint(
                    f"    [jar-discovery] attached {jar_count} JAR(s) to "
                    f"reused cluster {already_on_aidp.get('key')}"
                )
                dbx_existing_to_aidp_key[existing_id] = already_on_aidp.get("key")
                cluster_status_msgs.append(
                    f"existing_cluster_id {existing_id} → REUSED {target_cluster_name} "
                    f"({already_on_aidp.get('key')})"
                )
                continue
            # The full clusters/get response has the same shape that convert_dbx_cluster
            # already understands (autoscale, num_workers, spark_conf, init_scripts, etc.)
            try:
                conv = convert_dbx_cluster(
                    new_cluster=dbx_cluster,
                    cluster_display_name=cluster_display,
                    prefix=prefix,
                    cfg=cfg,
                )
                if ray_init_rule:
                    try:
                        attach_ray_init_script(conv.aidp_payload, ray_init_rule)
                    except ValueError as exc:
                        cluster_create_failed = True
                        reason = f"ray-cluster-init-script: {exc}"
                        ray_init_failure = reason
                        tprint(f"    [FAIL] {reason}")
                        report.errors.append(f"ERROR {reason}")
                        cluster_status_msgs.append(
                            f"existing_cluster_id {existing_id} -> FAILED ({reason})"
                        )
                        continue
                    tprint(
                        f"    [init-scripts] attached Ray script "
                        f"{ray_init_rule['destination']}"
                    )
            except ShapeCapacityError as e:
                # Same treatment as the job_clusters loop: fail this cluster
                # loudly rather than clamp the spec down, keep exporting.
                tprint(f"    [shape][ERROR] existing_cluster_id {existing_id}: {e}")
                cluster_status_msgs.append(
                    f"existing_cluster_id {existing_id} → FAILED (shape capacity: {e})")
                report.errors.append(
                    f"ERROR shape-capacity: {aidp_workflow_name}/"
                    f"existing_cluster_id {existing_id}: {e}")
                continue
            # PR #30: shape mapping log lines (driver/worker decisions).
            for _line in conv.shape_mapping_log_lines:
                tprint(f"    {_line}")
            # PR #31: nuanced drop reporting (translate / WARN-drop / [drop]).
            _report_dropped_conf(conv, aidp_workflow_name, report)
            _requirements_context = f"existing_cluster:{existing_id}"
            _canonical_uri = _attach_only_consolidated_requirements(
                conv, graph, _requirements_context
            )
            _jar_uris = _attach_context_jars(
                conv, graph, _requirements_context
            )
            if _jar_uris:
                tprint(
                    f"    [jar-discovery] attaching {len(_jar_uris)} "
                    f"notebook-resolved JAR(s) for {_requirements_context}"
                )
            tprint(
                "    [requirements] attaching cluster-specific resolved file: "
                f"{uri_to_aidp.get(_canonical_uri, '<not uploaded>')}"
            )
            # ─── Init scripts: UPLOAD + LOG, but do NOT attach to cluster ───
            # (mirror of the same treatment for job_clusters above — see that
            # site for full rationale. Same principle: environment-specific,
            # operator reviews before attaching manually.)
            if conv.init_script_refs:
                tprint(f"    [init-scripts] {len(conv.init_script_refs)} script(s) uploaded to workspace but NOT attached to cluster")
                tprint(f"    [init-scripts] (environment-specific; review at target path and attach manually if needed)")
                for _uri in conv.init_script_refs:
                    _aidp = uri_to_aidp.get(_uri, "<not uploaded>")
                    tprint(f"    [init-scripts]   {_uri} -> {_aidp}")
            try:
                created = _create_cluster_with_intel_fallback(
                    aidp, conv.aidp_payload, tprint, report,
                )
                aidp_key = created.get("key")
                dbx_existing_to_aidp_key[existing_id] = aidp_key
                cluster_status_msgs.append(
                    f"existing_cluster_id {existing_id} → CREATED {target_cluster_name} ({aidp_key})"
                )
                items = build_library_install_items(conv.library_refs, uri_to_aidp)
                if items and aidp_key:
                    try:
                        aidp.install_cluster_libraries(aidp_key, items)
                        tprint(f"    libraries installed on {aidp_key}: {len(items)} items")
                    except AidpApiError as e:
                        tprint(f"    [WARN] library install on {aidp_key} failed: {e}")
                        report.errors.append(f"library-install: {aidp_key}: {e}")
            except ShapeCapacityError as e:
                # Self-heal parsed API-stated limits that prove the request
                # impossible for this shape — fail, never clamp down.
                tprint(f"    [shape][ERROR] existing_cluster_id {existing_id}: {e}")
                cluster_status_msgs.append(
                    f"existing_cluster_id {existing_id} → FAILED (shape capacity: {e})")
                report.errors.append(
                    f"ERROR shape-capacity: {aidp_workflow_name}/"
                    f"existing_cluster_id {existing_id}: {e}")
                continue
            except AidpApiError as e:
                tprint(f"    [FAIL] cluster create {target_cluster_name}: {e}")
                cluster_status_msgs.append(f"existing_cluster_id {existing_id} → FAILED ({e})")

        # Generic discovery is best-effort; a customer-pack contract is not.
        pack_failed = False
        for identity, rule in init_contracts.items():
            if identity[0] != str(job_id):
                continue
            if identity[1] not in dbx_key_to_aidp_key:
                pack_failed = True
                reason = ("customer-pack init script requires its actual job cluster; "
                          f"no cluster override/fallback for {identity[1]}")
                report.errors.append(f"ERROR customer-pack-cluster-init-script: {reason}")
                report.jobs.append(JobResult(
                    dbx_job_id=job_id, dbx_job_name=dbx_name,
                    aidp_workflow_name=aidp_workflow_name, status="FAILED",
                    failure_reason=reason, cluster_status="; ".join(cluster_status_msgs),
                ))
                tprint(f"[customer-pack][ERROR] {reason}")
                break
        if pack_failed:
            continue
        for context, contract in pack_contracts.items():
            if contract["dbx_job_id"] != str(job_id):
                continue
            try:
                cluster_key = dbx_key_to_aidp_key.get(contract["job_cluster_key"])
                if not cluster_key or cluster_override_key:
                    raise ValueError(
                        "customer-pack library rule requires its actual job cluster; "
                        "no cluster override/fallback"
                    )
                verify_cluster_library_contract(aidp, cluster_key, contract, tprint, report)
            except (ValueError, AidpApiError) as exc:
                pack_failed = True
                report.errors.append(f"ERROR customer-pack-cluster-library: {context}: {exc}")
                report.jobs.append(JobResult(
                    dbx_job_id=job_id, dbx_job_name=dbx_name,
                    aidp_workflow_name=aidp_workflow_name, status="FAILED",
                    failure_reason=str(exc), cluster_status="; ".join(cluster_status_msgs),
                ))
                tprint(f"[customer-pack][ERROR] {context}: {exc}")
                break
        if pack_failed:
            continue

        # A required Ray script mismatch is not an ordinary capacity failure:
        # never reuse a workflow or rebind the task onto another cluster.
        if ray_init_failure:
            report.jobs.append(JobResult(
                dbx_job_id=job_id, dbx_job_name=dbx_name,
                aidp_workflow_name=aidp_workflow_name, status="FAILED",
                failure_reason=ray_init_failure,
                cluster_status="; ".join(cluster_status_msgs),
            ))
            continue

        # Step B: workflow.
        existing_wf, matched_workflow_name = _find_resource_by_current_or_legacy_name(
            aidp.find_workflow_by_name,
            aidp_workflow_name,
            sanitizer.legacy_workflow_name(dbx_name, prefix),
            "workflow",
        )
        if existing_wf and not overwrite_workflow:
            tprint(f"  SKIP workflow (exists): {matched_workflow_name}")
            report.jobs.append(JobResult(
                dbx_job_id=job_id, dbx_job_name=dbx_name,
                aidp_workflow_name=matched_workflow_name,
                aidp_workflow_key=existing_wf.get("key"),
                status="SKIPPED_EXISTS",
                cluster_status="; ".join(cluster_status_msgs),
            ))
            continue

        default_cluster_key = cluster_override_key or (
            next(iter(dbx_key_to_aidp_key.values()), None)
            or next(iter(dbx_task_to_aidp_key.values()), None)
            or next(iter(dbx_existing_to_aidp_key.values()), None)
        )

        # Last-resort fallback: AIDP rejects workflow create when any task has
        # cluster=null. If neither the override path nor the DBX-side cluster
        # resolution produced anything (e.g. DBX clusters/get is not reachable
        # from this PAT), scan AIDP for any existing cluster whose displayName
        # starts with our prefix and reuse it. Keeps the isolation guarantee
        # (prefix-only) and lets the workflow get created — user can swap in a
        # different cluster manually after the fact if needed.
        if not default_cluster_key:
            prefix_match_key = None
            prefix_match_name = None
            try:
                for c in aidp.list_clusters():
                    dn = c.get("displayName") or ""
                    if dn.startswith(f"{prefix}_"):
                        prefix_match_key = c.get("key")
                        prefix_match_name = dn
                        break
            except AidpApiError as e:
                tprint(f"    [WARN] list_clusters failed during fallback lookup: {e}")
            if prefix_match_key:
                default_cluster_key = prefix_match_key
                tprint(
                    f"  [WARN] no DBX cluster could be resolved — falling back to existing "
                    f"AIDP cluster '{prefix_match_name}' ({prefix_match_key}). "
                    f"Override with --cluster-key to pick a different one."
                )
                cluster_status_msgs.append(
                    f"fallback → REUSED {prefix_match_name} ({prefix_match_key})"
                )
                # Also fill in dbx_existing_to_aidp_key so per-task resolution
                # in convert_dbx_task uses the same fallback consistently.
                for _eid in existing_ids:
                    dbx_existing_to_aidp_key.setdefault(_eid, prefix_match_key)

        if not default_cluster_key:
            tprint(
                f"  [FAIL] no AIDP cluster available for {aidp_workflow_name} "
                f"and no existing '{prefix}_*' cluster to fall back on — "
                f"pass --cluster-key <existing-aidp-cluster-key> to override"
            )
            report.jobs.append(JobResult(
                dbx_job_id=job_id, dbx_job_name=dbx_name,
                aidp_workflow_name=aidp_workflow_name,
                status="FAILED",
                failure_reason="no_cluster_available",
                cluster_status="; ".join(cluster_status_msgs),
            ))
            continue
        # A job_cluster failed to create but another cluster resolved
        # (default_cluster_key is truthy): convert_dbx_job will rebind tasks
        # bound to the failed cluster onto the default cluster. That rebinding
        # is otherwise silent, so warn loudly (the failure is also captured in
        # cluster_status). Not a hard-fail — tasks not using the failed cluster
        # are unaffected — but the operator must review before running.
        if cluster_create_failed and not cluster_override_key:
            tprint(f"  [WARN] one or more job_clusters failed to create for "
                   f"{aidp_workflow_name}; tasks bound to them will be rebound to "
                   f"the default cluster '{default_cluster_key}'. Review cluster_status.")
            report.errors.append(
                f"cluster-create-failed: {aidp_workflow_name}: tasks rebound to {default_cluster_key}")
        # Parameter-derived workspace files are exported through the notebook
        # asset path. Give the converter their exact source/target mapping so
        # a base parameter such as `/Workspace/Users/.../artifacts` is updated
        # to the corresponding exported AIDP directory.
        workspace_resource_path_map = {
            dbx_path: nb_target_paths[dbx_path]
            for dbx_path, resource_node in graph.notebooks.items()
            if (
                resource_node.role == "resource"
                and resource_node.export_error is None
                and dbx_path in nb_target_paths
            )
        }

        conversion_decisions: List[str] = []
        generated_notebooks: List = []
        try:
            payload = convert_dbx_job(
                job_data=job_node.job_def,
                export_output=export_output,
                prefix=prefix,
                dbx_cluster_key_to_aidp_key=dbx_key_to_aidp_key,
                dbx_existing_cluster_id_to_aidp_key=dbx_existing_to_aidp_key,
                dbx_task_key_to_aidp_key=dbx_task_to_aidp_key,
                aidp_default_cluster_key=default_cluster_key,
                force_pause_schedule=force_pause_schedule,
                param_plan=param_plan,
                workspace_resource_path_map=workspace_resource_path_map,
                workflow_parameter_overrides=workflow_parameter_overrides,
                conversion_decisions=conversion_decisions,
                generated_notebooks=generated_notebooks,
                notebook_path_map=nb_target_paths,
                file_path_map=uri_to_aidp,
            )
        except ValueError as exc:
            # Converter refusals are scoped to one workflow. Earlier workflows
            # may already have been created, so retain that state in the final
            # report and continue processing the remaining graph instead of
            # aborting Phase 3 before report.write().
            reason = str(exc)
            tprint(
                f"  [FAIL] workflow conversion {aidp_workflow_name}: {reason}"
            )
            report.jobs.append(JobResult(
                dbx_job_id=job_id,
                dbx_job_name=dbx_name,
                aidp_workflow_name=aidp_workflow_name,
                status="FAILED",
                failure_reason=reason,
                cluster_status="; ".join(cluster_status_msgs),
            ))
            report.errors.append(
                f"ERROR workflow-conversion: {aidp_workflow_name}: {reason}"
            )
            continue
        _record_workflow_conversion_decisions(
            aidp_workflow_name, conversion_decisions, report,
        )

        # FOREACH_BRIDGE: upload tool-generated driver notebooks BEFORE the
        # workflow references them. A failed driver upload fails the whole
        # workflow loudly — a workflow whose for-each task points at a missing
        # driver is exactly the incomplete-artifact class we refuse.
        # The driver path has job/task identity but NO workflow prefix, so a
        # second export of the same job under another prefix reaches the SAME
        # path: an identical artifact is reused; a DIFFERING one is written
        # only under explicit --overwrite, else refused — a silent PUT would
        # mutate the other workflow's executable arguments (review finding
        # 2026-10-14).
        def _driver_sources(nb_dict):
            # Cell TYPE participates in identity: identical text in a markdown
            # cell is not an executable driver (review finding 2026-10-14).
            return [(c.get("cell_type", "code"),
                     "".join(c.get("source", [])) if isinstance(c.get("source"), list)
                     else c.get("source", "")) for c in nb_dict.get("cells", [])]

        _driver_upload_failed = False
        for _driver_path, _driver_nb in generated_notebooks:
            try:
                _existing_driver = aidp.get_notebook(_driver_path)
                if _existing_driver is not None:
                    if _driver_sources(_existing_driver) == _driver_sources(_driver_nb):
                        tprint(f"  [foreach-driver] existing identical driver reused: {_driver_path}")
                        continue
                    if not overwrite_workflow:
                        raise AidpApiError(
                            "foreach_driver_conflict", 0,
                            f"existing driver at {_driver_path} differs from the "
                            "regenerated one (another workflow may reference it); "
                            "re-run with --overwrite to replace it deliberately",
                        )
                    tprint(f"  [foreach-driver][OVERWRITE] replacing differing driver: {_driver_path}")
                aidp.put_notebook(_driver_path, _driver_nb)
                tprint(f"  [foreach-driver] uploaded {_driver_path}")
            except AidpApiError as exc:
                _driver_upload_failed = True
                reason = f"for-each driver upload failed: {_driver_path}: {exc}"
                tprint(f"  [FAIL] {reason}")
                report.jobs.append(JobResult(
                    dbx_job_id=job_id, dbx_job_name=dbx_name,
                    aidp_workflow_name=aidp_workflow_name,
                    status="FAILED", failure_reason=reason,
                    cluster_status="; ".join(cluster_status_msgs),
                ))
                report.errors.append(
                    f"ERROR foreach-driver-upload: {aidp_workflow_name}: {reason}"
                )
                break
        if _driver_upload_failed:
            continue

        # Surface schedule handling: pausing an otherwise-active DBX cron is the
        # single most expensive thing to get wrong (AIDP would keep launching
        # failing runs), so log it explicitly.
        _dbx_sched = (job_node.job_def.get("settings") or job_node.job_def).get("schedule") or {}
        if _dbx_sched.get("quartz_cron_expression"):
            if force_pause_schedule and _dbx_sched.get("pause_status") != "PAUSED":
                tprint(f"  [SCHEDULE] forcing PAUSED for {aidp_workflow_name} "
                       f"(DBX was {_dbx_sched.get('pause_status', 'UNPAUSED')}; "
                       f"use --preserve-schedule-status to keep it active)")
            elif not force_pause_schedule and _dbx_sched.get("pause_status") != "PAUSED":
                tprint(f"  [SCHEDULE] preserving ACTIVE schedule for {aidp_workflow_name} "
                       f"— AIDP will run this on its cron")

        # Note: we intentionally do NOT pre-check that every task's
        # notebookPath exists on AIDP. AIDP accepts workflows with dangling
        # paths — those tasks just fail at runtime with a clear "notebook
        # not found" error. Pre-checking would waste GETs and block the
        # workflow creation unnecessarily.

        try:
            if existing_wf and overwrite_workflow:
                # Workflow exists + --overwrite: PUT the new payload directly
                # onto the existing key. AIDP doesn't have a delete-and-recreate
                # path we use (per never-delete-files policy), and POST would
                # 409 on the duplicate name.
                wf_key = existing_wf.get("key")
                tprint(f"  [OVERWRITE] updating existing workflow: {aidp_workflow_name}  (key={wf_key})")
                aidp.update_workflow(wf_key, payload)
                status = "UPDATED"
            else:
                # New workflow: POST shell, then PUT tasks.
                shell = aidp.create_workflow_shell(
                    name=aidp_workflow_name,
                    description=payload.get("description", ""),
                )
                wf_key = shell.get("key")
                aidp.update_workflow(wf_key, payload)
                tprint(f"  [OK] workflow created: {aidp_workflow_name}  (key={wf_key})")
                status = "CREATED"
            report.jobs.append(JobResult(
                dbx_job_id=job_id, dbx_job_name=dbx_name,
                aidp_workflow_name=aidp_workflow_name,
                aidp_workflow_key=wf_key,
                status=status,
                cluster_status="; ".join(cluster_status_msgs),
            ))
        except AidpApiError as e:
            tprint(f"  [FAIL] workflow create {aidp_workflow_name}: {e}")
            report.jobs.append(JobResult(
                dbx_job_id=job_id, dbx_job_name=dbx_name,
                aidp_workflow_name=aidp_workflow_name,
                status="FAILED", failure_reason=str(e),
                cluster_status="; ".join(cluster_status_msgs),
            ))


# ─── Pre-flight gate ────────────────────────────────────────────────


def pre_flight(
    graph: Graph,
    export_output: str,
    prefix: str,
    root_job_id: str,
    cfg=None,
    workflow_parameter_overrides: Optional[Dict[str, str]] = None,
    workflow_override_checks: Optional[List[Dict[str, str]]] = None,
) -> str:
    root_job = graph.jobs.get(root_job_id)
    if root_job and root_job.job_def:
        dbx_name = (root_job.job_def.get("settings") or root_job.job_def).get("name", f"dbx_job_{root_job_id}")
    else:
        dbx_name = f"dbx_job_{root_job_id}"
    workflow_name = sanitize_workflow_name(dbx_name, prefix)
    override_checks = (
        workflow_override_checks
        if workflow_override_checks is not None
        else _workflow_override_preflight_checks(
            graph,
            root_job_id,
            workflow_parameter_overrides,
        )
    )
    root_refusal = next((
        check["detail"] for check in override_checks
        if check["job_id"] == str(root_job_id)
        and check["status"] == "WILL REFUSE"
    ), None)
    workflow_check_lines = [
        f"[{check['status']}] {check['name']}: {check['detail']}"
        for check in override_checks
        if not (
            check["job_id"] == str(root_job_id)
            and check["status"] == "WILL REFUSE"
        )
    ]

    # Cluster summary + per-cluster DBX→AIDP shape mapping decisions.
    # We run convert_dbx_cluster() up-front here (pure Python, no writes) so
    # the operator can review the shape mapping in --dry-run BEFORE any AIDP
    # cluster is created. Same decisions are re-printed at real create time
    # in phase3, so the two logs tie together.
    #
    # Covers EVERY job in the graph, not just the root: phase3 creates one
    # cluster per job_clusters[] entry across all recursed jobs (it iterates
    # graph.jobs.items()), so a root-only summary under-reports both the
    # cluster count and the compute the export commits to.
    cluster_msgs = []
    cluster_shape_mappings: List[str] = []
    _n_clusters = 0
    _min_workers = _max_workers = 0
    _min_ocpu = _max_ocpu = 0.0

    def _ocpu(cfg: dict, shape_key: str) -> float:
        """OCPU count out of an AIDP driver/worker config block."""
        return float(((cfg or {}).get(shape_key) or {}).get("ocpus") or 0)

    # Root first so the most relevant clusters stay at the top, then children
    # in a stable order.
    _ordered = ([(root_job_id, root_job)] if root_job else [])
    _ordered += [(jid, jn) for jid, jn in sorted(graph.jobs.items())
                 if jid != root_job_id]

    for _jid, _jnode in _ordered:
        if not (_jnode and _jnode.job_def):
            continue
        _settings = _jnode.job_def.get("settings") or _jnode.job_def
        _owner = "root" if _jid == root_job_id else f"child job {_jid}"
        for jc in _settings.get("job_clusters", []) or []:
            _display = (jc.get("cluster_name")
                        or (jc.get("new_cluster") or {}).get("cluster_name")
                        or jc.get("job_cluster_key", "?"))
            _n_clusters += 1
            cluster_msgs.append(
                f"{sanitize_cluster_name(_display, prefix)}      [{_owner}]")
            # Compute the shape mapping for this job_cluster. Wrapped in
            # try/except so a malformed DBX cluster spec can't crash pre-flight
            # (the real phase3 create would surface the error clearly there).
            try:
                from .cluster_converter import convert_dbx_cluster
                _conv = convert_dbx_cluster(
                    new_cluster=jc.get("new_cluster") or {},
                    cluster_display_name=_display,
                    prefix=prefix,
                    cfg=cfg,
                )
                for _line in _conv.shape_mapping_log_lines:
                    cluster_shape_mappings.append(f"[{_display}] {_line}")
                # Accumulate the committed footprint from the payload phase3
                # would actually POST, so the total can't drift from reality.
                _pl = _conv.aidp_payload or {}
                _w = _pl.get("workerConfig") or {}
                _mn = int(_w.get("minWorkerCount") or 0)
                _mx = int(_w.get("maxWorkerCount") or _mn)
                _w_ocpu = _ocpu(_w, "workerShapeConfig")
                _d_ocpu = _ocpu(_pl.get("driverConfig"), "driverShapeConfig")
                _min_workers += _mn
                _max_workers += _mx
                _min_ocpu += _mn * _w_ocpu + _d_ocpu
                _max_ocpu += _mx * _w_ocpu + _d_ocpu
            except Exception as _e:
                cluster_shape_mappings.append(
                    f"[{_display}] shape mapping failed to compute: "
                    f"{type(_e).__name__}: {_e}")

    cluster_totals = None
    if _n_clusters:
        cluster_totals = (
            f"{_n_clusters} cluster(s) | workers min {_min_workers} max {_max_workers}"
            f" | OCPU min {_min_ocpu:g} max {_max_ocpu:g}")

    notebooks_real = sum(
        1 for n in graph.notebooks.values() if n.role == "task" and n.export_error is None
    )
    notebooks_failed = [
        {"dbx_path": n.dbx_path, "reason": n.export_error or "?"}
        for n in graph.notebooks.values()
        if n.role == "task" and n.export_error is not None
    ]
    run_dep_real = sum(
        1 for n in graph.notebooks.values() if n.role == "run_dep" and n.export_error is None
    )
    run_dep_failed = [
        {"dbx_path": n.dbx_path, "reason": n.export_error or "?"}
        for n in graph.notebooks.values()
        if n.role == "run_dep" and n.export_error is not None
    ]
    files_ok = sum(
        1 for f in graph.files.values()
        if (
            f.fetch_error is None
            or f.aidp_fallback_path is not None
            or bool(f.aidp_replacement_paths)
        )
    )
    files_failed = [
        {"dbx_source": uri, "reason": n.fetch_error or "?"}
        for uri, n in graph.files.items()
        if n.fetch_error not in (None, "external_uri_skipped")
        and n.aidp_fallback_path is None
        and not n.aidp_replacement_paths
    ]
    child_jobs = [j for j in graph.jobs if j != root_job_id]

    return render_pre_flight(
        export_output=export_output,
        workflow_name=workflow_name,
        cluster_summary=cluster_msgs,
        cluster_shape_mappings=cluster_shape_mappings,
        cluster_totals=cluster_totals,
        notebooks_real=notebooks_real,
        notebooks_failed=notebooks_failed,
        run_dep_real=run_dep_real,
        run_dep_failed=run_dep_failed,
        files_ok=files_ok,
        files_failed=files_failed,
        child_jobs=child_jobs,
        external_refs=graph.external_refs,
        skipped_loose_candidates=graph.skipped_loose_candidates,
        workflow_refusal=root_refusal,
        workflow_checks=workflow_check_lines,
    )


# ─── Main ───────────────────────────────────────────────────────────


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python3 -m dbx_export",
        description="Export a Databricks Job (notebooks + jobs + files + cluster spec) "
                    "to AIDP. Copies artifacts only — code-level migration runs later.",
    )
    p.add_argument("--job-id", help="Databricks job_id to export (required unless --probe)")
    p.add_argument("--probe", action="store_true",
                   help="Connectivity check only: hits DBX /api/2.2/jobs/list?limit=1 and exits.")
    p.add_argument(
        "--config", default=None,
        help="Path to aidp_migrator.yaml. Falls back to $AIDP_CONFIG_PATH, "
             "then ./aidp_migrator.yaml, then ~/aidp_migrator.yaml.",
    )
    p.add_argument(
        "--export-output", "--target-base", dest="export_output", default=None,
        help="AIDP path under which ALL exported artifacts land. "
             "Sourced from cfg.paths.export_output if not supplied. "
             "(--target-base is the pre-YAML-config flag name, kept as a "
             "back-compat alias.)",
    )
    p.add_argument(
        "--prefix", default="ai_tool",
        help="Prefix applied to AIDP workflow + cluster names. Default: ai_tool",
    )
    # DBX side
    p.add_argument(
        "--dbx-host", default=os.environ.get("DATABRICKS_HOST"),
        help="DBX host (env: DATABRICKS_HOST). Mutually exclusive with --dbx-proxy.",
    )
    p.add_argument(
        "--dbx-token", default=os.environ.get("DATABRICKS_TOKEN"),
        help="DBX PAT (env: DATABRICKS_TOKEN). Required if --dbx-host is used.",
    )
    p.add_argument(
        "--dbx-proxy", default=os.environ.get("DBX_PROXY"),
        help="DBX proxy host:port for cluster-side runs (env: DBX_PROXY).",
    )
    p.add_argument(
        "--proxy-basic-user", default=os.environ.get("DBX_PROXY_BASIC_USER"),
        help="HTTP Basic Auth username for --dbx-proxy (env: DBX_PROXY_BASIC_USER). "
             "Optional: supply both --proxy-basic-user and --proxy-basic-pass to "
             "authenticate, or neither to use the proxy without basic auth.",
    )
    p.add_argument(
        "--proxy-basic-pass", default=os.environ.get("DBX_PROXY_BASIC_PASS"),
        help="HTTP Basic Auth password for --dbx-proxy (env: DBX_PROXY_BASIC_PASS). "
             "Optional: supply both --proxy-basic-user and --proxy-basic-pass to "
             "authenticate, or neither to use the proxy without basic auth.",
    )
    p.add_argument(
        "--fetch-external-jars-via-proxy", action="store_true",
        help="Pull s3:// / http(s):// library jars (normally record-only) THROUGH "
             "the DBX proxy's egress so they upload to AIDP. Requires --dbx-proxy. "
             "Note: private S3 objects need AWS auth the proxy can't add — with "
             "this flag on, such fetches FAIL that jar (use the Production Jar "
             "folder for them, or leave the flag off to keep them record-only).",
    )
    # AIDP side — default None so YAML (via --config) is primary source.
    p.add_argument("--lake-ocid", default=None,
                   help="Override cfg.aidp.lake_ocid for this run only.")
    p.add_argument("--workspace-id", default=None,
                   help="Override cfg.aidp.workspace_id for this run only.")
    p.add_argument("--oci-profile", default=None,
                   help="Override cfg.aidp.oci_profile for this run only.")
    p.add_argument(
        "--oci-config-path", default=None,
        help="Path to OCI config file (LAPTOP ONLY; default ~/.oci/config or "
             "$OCI_CONFIG_FILE). Ignored on cluster — OCI credentials come "
             "from the AIDP Credential Store via aidputils (Phase 2).",
    )
    p.add_argument(
        "--cluster-key", default=None,
        help="Reuse an existing AIDP cluster key for ALL tasks instead of creating new ones.",
    )
    p.add_argument(
        "--shape-limits-path", default=None,
        help="Explicit shape-limits table (sets $AIDP_SHAPE_LIMITS_PATH). "
             "Default resolution: $AIDP_SHAPE_LIMITS_PATH, else the "
             "committed config/aidp_shape_limits.json.",
    )
    # Behavior
    p.add_argument("--overwrite", action="store_true",
                   help="Force re-upload of notebooks/files under <export_output> and re-create workflows.")
    p.add_argument("--strict", action="store_true",
                   help="Abort if any task notebook fails to export — refuses to create "
                        "a workflow with dangling notebookPath references.")
    p.add_argument("--strict-deps", action="store_true",
                   help="Abort if any %%run, local resource, or child job dependency "
                        "fails, including incomplete resource directory discovery.")
    p.add_argument(
        "--strict-jars",
        action="store_true",
        help=(
            "Abort when a JAR cannot be fetched, uploaded, or matched by "
            "checksum in the configured fallback directory. By default the "
            "export continues and returns exit code 12."
        ),
    )
    unresolved_jars = p.add_mutually_exclusive_group()
    unresolved_jars.add_argument(
        "--strict-unresolved-jars",
        dest="allow_unresolved_jars",
        action="store_false",
        help=(
            "Abort Phase 1 when notebook JVM/JAR references cannot be "
            "resolved uniquely. The default is to report them as warnings."
        ),
    )
    unresolved_jars.add_argument(
        "--allow-unresolved-jars",
        dest="allow_unresolved_jars",
        action="store_true",
        help=(
            "Report unresolved or ambiguous notebook JVM/JAR dependencies "
            "and continue Phase 1 (the default; retained for compatibility)."
        ),
    )
    p.set_defaults(allow_unresolved_jars=True)
    p.add_argument("--max-depth", type=int, default=10,
                   help="BFS depth cap for child-job recursion.")
    p.add_argument(
        "--param", action="append", default=[], metavar="KEY=VALUE",
        help="Parameter binding used during dependency discovery. Repeat for multiple "
             "values. Enables safe resolution of widget-based notebook.run paths.",
    )
    p.add_argument(
        "--workflow-param", action="append", default=[], metavar="KEY=VALUE",
        help="Declare or override an AIDP workflow parameter during export. "
             "Repeat for multiple values.",
    )
    p.add_argument("--dry-run", action="store_true",
                   help="Phase 1 + pre-flight only; no AIDP writes.")
    p.add_argument("--notebook-only", action="store_true",
                   help="Upload the notebook tree (task + %%run dependency notebooks/scripts) "
                        "only, then stop. Skips file/jar/init-script/wheel uploads AND Phase 3 "
                        "(cluster creation + workflow creation). Nothing cluster-related is touched.")
    p.add_argument("--preserve-schedule-status", action="store_true",
                   help="Carry over the DBX schedule pause_status as-is. By default every "
                        "exported workflow's schedule is forced to PAUSED so AIDP does not "
                        "auto-launch (and bill for) runs before the job is verified.")
    p.add_argument("--report-dir", default="reports/dbx_export",
                   help="Where to write the JSON report.")
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)

    # --prefix is CLI-only (default ai_tool), so reject it before config,
    # credential resolution or Phase 1 discovery/downloads. Library callers
    # retain the same validation in sanitize_workflow_name().
    try:
        sanitizer.validate_resource_prefix(args.prefix)
    except ValueError as exc:
        print(f"ERROR: invalid --prefix: {exc}", file=sys.stderr)
        return ExitCode.BAD_ARGS

    # Validate this self-contained CLI input before loading customer config or
    # performing I/O. A malformed override must fail for its own reason even
    # when the surrounding environment is not configured yet.
    workflow_parameter_overrides: Dict[str, str] = {}
    workflow_param_count = len(args.workflow_param)
    for position, raw in enumerate(args.workflow_param, start=1):
        key, separator, value = raw.partition("=")
        key = key.strip()
        if (
            not separator
            or not key
            or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) is None
        ):
            print(
                f"ERROR: invalid --workflow-param #{position} of "
                f"{workflow_param_count}; expected KEY=VALUE with "
                "KEY matching [A-Za-z_][A-Za-z0-9_]*. The supplied input "
                "is omitted because VALUE may contain a secret.",
                file=sys.stderr,
            )
            return ExitCode.BAD_ARGS
        if key in workflow_parameter_overrides:
            print(
                f"WARN: duplicate --workflow-param key {key!r} at position "
                f"{position}; the last value wins.",
                file=sys.stderr,
            )
        workflow_parameter_overrides[key] = value

    # Load customer YAML config; backfill any argparse args the user did not
    # pass on the CLI. Values ALREADY passed on the CLI win over YAML.
    # This is the single source of truth for customer-specific values; nothing
    # is hardcoded in this module.
    try:
        from aidp_config import load_config
        _cfg = load_config(cli_config_path=args.config)
    except Exception as e:
        print(f"ERROR: failed to load configuration: {e}", file=sys.stderr)
        return ExitCode.BAD_ARGS
    if not args.lake_ocid:
        args.lake_ocid = _cfg.aidp.lake_ocid
    if not args.workspace_id:
        args.workspace_id = _cfg.aidp.workspace_id
    # The AIDP client below targets args.lake_ocid / args.workspace_id; the
    # cluster env Phase 3 writes (recommended_env_vars, read by the migrated
    # job trigger at runtime) comes from _cfg. Give _cfg the same effective
    # coordinates so a CLI override cannot send the trigger to the YAML
    # workspace. Copies — the loaded config namespace is left untouched.
    _cfg = copy.copy(_cfg)
    _cfg.aidp = copy.copy(_cfg.aidp)
    _cfg.aidp.lake_ocid = args.lake_ocid
    _cfg.aidp.workspace_id = args.workspace_id
    if not args.oci_profile:
        args.oci_profile = _cfg.aidp.oci_profile
    if not args.export_output:
        args.export_output = _cfg.paths.export_output
    # dbx_source.host / dbx_source.proxy — mutually exclusive; either fills in
    # the CLI arg the user did not pass. The below-line check enforces "at
    # least one of host/proxy" (schema `anyOf` mirrors this at YAML level).
    if not args.dbx_host:
        args.dbx_host = getattr(_cfg.dbx_source, "host", None)
    if not args.dbx_proxy:
        args.dbx_proxy = getattr(_cfg.dbx_source, "proxy", None)
    # Proxy basic-auth creds are resolved as an ATOMIC pair. Sources (each may
    # be complete, partial, or absent): CLI/env, then YAML secrets (plaintext or
    # credential-store ref resolved cluster-side via aidputils — see
    # _resolve_proxy_secret for the laptop-side behavior). Precedence:
    #   complete CLI/env pair  >  complete YAML pair  >  none (unauthenticated).
    # A partial CLI/env pair never suppresses a complete YAML pair, and any
    # partial pair remaining AFTER resolution is a hard configuration error —
    # proceeding unauthenticated against an auth-requiring proxy reproduces the
    # exact 401-at-root-job-fetch this resolution exists to prevent.
    if args.dbx_proxy and not (args.proxy_basic_user and args.proxy_basic_pass):
        _cfg_user = _resolve_proxy_secret(
            getattr(getattr(_cfg, "secrets", None), "dbx_proxy_basic_user", None),
            "dbx_proxy_basic_user",
        )
        _cfg_pass = _resolve_proxy_secret(
            getattr(getattr(_cfg, "secrets", None), "dbx_proxy_basic_pass", None),
            "dbx_proxy_basic_pass",
        )
        if bool(_cfg_user) != bool(_cfg_pass):
            print(
                "ERROR: secrets.dbx_proxy_basic_user / secrets.dbx_proxy_basic_pass "
                "must be configured as a pair — only one of them is set/resolvable "
                "in the YAML. Fix the config (or the credential store entry) so "
                "both resolve, or remove both.",
                file=sys.stderr,
            )
            return ExitCode.BAD_ARGS
        if _cfg_user and _cfg_pass:
            if args.proxy_basic_user or args.proxy_basic_pass:
                tprint(
                    "[auth][WARN] partial --proxy-basic-user/--proxy-basic-pass "
                    "(CLI/env) ignored — using the complete credential pair from "
                    "config secrets instead"
                )
            args.proxy_basic_user = _cfg_user
            args.proxy_basic_pass = _cfg_pass
            tprint("[auth] proxy basic-auth credentials resolved from config secrets")
        elif args.proxy_basic_user or args.proxy_basic_pass:
            print(
                "ERROR: only one of --proxy-basic-user/--proxy-basic-pass (or env "
                "DBX_PROXY_BASIC_USER/DBX_PROXY_BASIC_PASS) is set and the config "
                "secrets provide no complete pair — proxy basic auth needs both. "
                "Supply both, or neither (for a proxy without basic auth).",
                file=sys.stderr,
            )
            return ExitCode.BAD_ARGS

    try:
        lib_repository_dir = configured_lib_repository_dir(args.config)
    except Exception as exc:
        print(f"ERROR: failed to load dependency-repository configuration: {exc}", file=sys.stderr)
        return ExitCode.BAD_ARGS
    # Keep the artifact-alias trust set in agreement with the configured
    # repository (PR #88 review round 4: a custom lib_repository_dir must not
    # leave the export client trusting only the default dirs).
    from dbx_export.aidp_client import set_artifact_alias_repository
    set_artifact_alias_repository(lib_repository_dir)

    parameter_bindings: Dict[str, str] = {}
    param_count = len(args.param)
    for position, raw in enumerate(args.param, start=1):
        key, separator, value = raw.partition("=")
        if not separator or not key.strip():
            print(
                f"ERROR: invalid --param #{position} of {param_count}; "
                "expected KEY=VALUE. The supplied input is omitted because "
                "VALUE may contain a secret.",
                file=sys.stderr,
            )
            return ExitCode.BAD_ARGS
        key = key.strip()
        if key in parameter_bindings:
            print(
                f"WARN: duplicate --param key {key!r} at position {position}; "
                "the last value wins.",
                file=sys.stderr,
            )
        parameter_bindings[key] = value

    # Validate target-base early (cheap, no I/O).
    try:
        export_output = validate_export_output(args.export_output)
    except sanitizer.TargetBaseViolation as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return ExitCode.BAD_ARGS

    if not args.dbx_host and not args.dbx_proxy:
        print("ERROR: one of --dbx-host (with --dbx-token) or --dbx-proxy is required", file=sys.stderr)
        return ExitCode.BAD_ARGS
    if args.dbx_host and not args.dbx_token:
        print("ERROR: --dbx-token (or env DATABRICKS_TOKEN) is required with --dbx-host", file=sys.stderr)
        return ExitCode.BAD_ARGS
    # Proxy basic auth remains OPTIONAL (`--dbx-proxy` alone is valid for an
    # unauthenticated proxy). Pair completeness is enforced ATOMICALLY in the
    # resolution block above (complete CLI/env pair > complete YAML pair >
    # none; any partial pair after resolution already returned BAD_ARGS), so
    # by this point creds are either a complete pair or entirely absent.
    if args.fetch_external_jars_via_proxy and not args.dbx_proxy:
        # In host+token mode there is no forward proxy: accepting the flag
        # would turn every benign EXTERNAL_RECORD_ONLY jar into a hard
        # no_forward_proxy FAILURE (and abort the export under --strict-jars).
        print("ERROR: --fetch-external-jars-via-proxy requires --dbx-proxy "
              "(host mode has no forward proxy; drop the flag to keep external "
              "jars as record-only)", file=sys.stderr)
        return ExitCode.BAD_ARGS

    # Build clients.
    dbx = DbxClient(DbxConfig(
        host=args.dbx_host, token=args.dbx_token, proxy=args.dbx_proxy,
        proxy_basic_user=args.proxy_basic_user, proxy_basic_pass=args.proxy_basic_pass,
        fetch_external_via_proxy=args.fetch_external_jars_via_proxy,
        # Optional customer knob (dbx_source.proxy_max_retries); when the YAML
        # omits it, fall back to the DbxConfig class default (single source of
        # truth) rather than re-hardcoding 20 here.
        proxy_max_retries=(getattr(_cfg.dbx_source, "proxy_max_retries", None)
                           or DbxConfig.proxy_max_retries),
    ))

    # ─── Probe mode: just connectivity, exit. ──────────────────────
    if args.probe:
        mode = "proxy" if args.dbx_proxy else "host"
        endpoint = args.dbx_proxy or args.dbx_host
        tprint(f"Probing DBX ({mode} mode) at {endpoint} …")
        ok, status, detail = dbx.probe()
        if ok:
            print(f"[OK] DBX reachable (HTTP {status}). {detail}")
            return ExitCode.OK
        print(f"[FAIL] DBX probe failed (HTTP {status}). {detail}", file=sys.stderr)
        return ExitCode.PROBE_FAILED

    if not args.job_id:
        print("ERROR: --job-id is required (unless --probe is passed)", file=sys.stderr)
        return ExitCode.BAD_ARGS

    _, signer = get_oci_signer(profile=args.oci_profile, config_path=args.oci_config_path)
    aidp_cfg = AidpConfig(
        lake_ocid=args.lake_ocid, workspace_id=args.workspace_id,
        export_output=export_output, prefix=args.prefix,
    )
    aidp = AidpClient(signer=signer, config=aidp_cfg)

    _gate_rc = _limits_startup_gate(args)
    if _gate_rc is not None:
        return _gate_rc

    run_id = ph_mod.new_run_id()
    report = Report(
        run_id=run_id, dbx_root_job_id=str(args.job_id),
        export_output=export_output, prefix=args.prefix,
    )

    # ─── Phase 1: discover + download ──────────────────────────────
    graph = phase1_discover(
        dbx, str(args.job_id), max_depth=args.max_depth,
        parameter_bindings=parameter_bindings,
    )
    if str(args.job_id) in graph.jobs:
        root_job = graph.jobs[str(args.job_id)]
        if root_job.fetch_error:
            print(f"ERROR: root job fetch failed: {root_job.fetch_error}", file=sys.stderr)
            return ExitCode.ROOT_JOB_FETCH_FAILED

    report.external_refs.extend(graph.external_refs)
    for node in graph.notebooks.values():
        for failure in node.resource_discovery_failures:
            if failure not in report.resource_discovery_failures:
                report.resource_discovery_failures.append(failure)
                severity = "ERROR" if args.strict_deps else "WARN"
                report.errors.append(
                    f"{severity} resource-discovery: {failure['reason']}: "
                    f"{failure['directory']} (from {failure['notebook']})"
                )
    if report.resource_discovery_failures:
        tprint("Resource discovery incomplete; see resource_discovery_failures in the report.")
        if args.strict_deps:
            path = report.write(args.report_dir)
            tprint(f"ERROR (--strict-deps): resource discovery failed. Report: {path}")
            return ExitCode.STRICT_DEPS_FAILED
    report.skipped_loose_candidates.extend(graph.skipped_loose_candidates)
    for candidate in graph.skipped_loose_candidates:
        report.errors.append(
            f"WARN loose-candidate-skip: {candidate['path']} "
            f"(from {candidate['referenced_by']}; {candidate['reason']})"
        )

    tprint("Phase 1: fetching library/init-script files…")
    # AIDP-first: index the shared dependency repository once, then prefer any
    # already-staged jar/wheel over a Databricks/proxy fetch.
    lib_repo = _LibRepoIndex(aidp, lib_repository_dir)
    phase1_discover_existing_cluster_libraries(dbx, graph)
    phase1_fetch_files(dbx, graph, lib_repo)
    phase1_discover_init_script_artifacts(dbx, graph, lib_repo)

    # Fix 7c: extract %pip install specs from every notebook and bundle into
    # a synthetic requirements FileNode. Ensures pip deps declared inside
    # notebooks (not in the cluster spec) get installed at cluster provision
    # time instead of racing against %pip execution mid-migration. See the
    # docstring on `discover_notebook_pip_deps` for the failure mode this
    # prevents (a pilot-customer AutoRefit brotli / matplotlib runtime-install churn).
    discover_notebook_pip_deps(
        graph, export_output, str(args.job_id), report=report,
        compatibility_policy_file=getattr(_cfg.paths, "compatibility_policy_file", None),
    )
    try:
        consolidate_cluster_requirements(
            graph,
            str(args.job_id),
            getattr(_cfg.paths, "target_runtime_requirements_file", None),
            getattr(_cfg.paths, "compatibility_policy_file", None),
            report=report,
        )
    except (RequirementConsolidationError, UnicodeDecodeError) as exc:
        message = f"requirements consolidation failed: {exc}"
        tprint(f"[requirements][ERROR] {message}")
        report.errors.append(f"ERROR requirements-conflict: {message}")
        return ExitCode.REQUIREMENTS_CONFLICT

    if report.pip_findings:
        summary = report.summary()
        tprint(f"[pip-discovery] unresolved findings: {summary['pip_unresolved']}; "
               f"notes: {summary['pip_notes']}. See pip_findings in the export report "
               "for notebook/cell context; unresolved discovery requires manual review.")

    # A source JAR may be unavailable or proxy-rate-limited even though the
    # approved binary is already staged in AIDP. Resolve that shared fallback
    # before pre-flight so the plan reports the dependency as available.
    resolve_failed_jar_fallbacks(aidp, graph, lib_repository_dir)

    # Databricks jobs can depend on JVM libraries installed outside the job
    # definition. With no historical source cluster available, scan all task
    # notebooks and transitive notebook dependencies, then resolve referenced
    # classes/packages against the configured AIDP fallback JAR directory.
    # Matched JARs are registered as FileNodes and flow through the normal
    # cluster-library installation path. Missing/ambiguous matches are reported
    # as warnings by default; --strict-unresolved-jars makes them blocking.
    jar_discovery = discover_notebook_jars(aidp, graph, lib_repository_dir)
    reconcile_failed_declared_jars(graph, jar_discovery)
    jar_discovery.allow_unresolved = args.allow_unresolved_jars
    report.jar_discovery = jar_discovery.to_dict()
    jar_report_path = jar_discovery.write(args.report_dir, str(args.job_id))
    for resolution in jar_discovery.resolutions:
        location = f"{resolution.notebook}:cell-{resolution.cell_index}"
        if resolution.status in {"MATCHED", "PRESENT"}:
            tprint(
                f"[jar-discovery][{resolution.status}] "
                f"{resolution.reference} -> {resolution.selected_jar} "
                f"({location})"
            )
        else:
            tprint(
                f"[jar-discovery][{resolution.status}] "
                f"{resolution.reference} ({location}); "
                f"candidates={resolution.candidates or 'none'}"
            )
    for fallback in jar_discovery.index_fallbacks:
        tprint(f"[jar-discovery][INDEX_FALLBACK] {fallback}")
    for error in jar_discovery.index_errors:
        tprint(f"[jar-discovery][INDEX_ERROR] {error}")
        report.errors.append(f"WARN jar-index: {error}")
    tprint(f"[jar-discovery] report: {jar_report_path}")
    if jar_discovery.blockers:
        summary = jar_discovery.to_dict()["summary"]
        message = (
            "JVM dependency discovery failed: "
            f"missing={summary['missing']}, ambiguous={summary['ambiguous']}, "
            f"index_errors={summary['index_errors']}. "
            f"See {jar_report_path}"
        )
        if args.allow_unresolved_jars:
            warning = (
                f"{message} Continuing with warning-only enforcement; use "
                "--strict-unresolved-jars to make unresolved references fatal."
            )
            report.errors.append(f"WARN jar-discovery-override: {warning}")
            tprint(f"[jar-discovery][WARN] {warning}")
        else:
            report.errors.append(f"ERROR jar-discovery: {message}")
            report_path = report.write(args.report_dir)
            print(f"ERROR: {message}", file=sys.stderr)
            print(f"Report: {report_path}", file=sys.stderr)
            return ExitCode.JAR_DISCOVERY_FAILED

    # ─── Pre-flight ────────────────────────────────────────────────
    path_plan = prepare_export_paths(graph, export_output, report, aidp)
    if path_plan.errors:
        for error in report.path_plan_errors:
            tprint(f"[ERROR] export destination refused: {error}")
        path = report.write(args.report_dir)
        print(f"ERROR: invalid or ambiguous export destinations; no AIDP writes performed. Report: {path}", file=sys.stderr)
        return ExitCode.PATH_PLAN_FAILED

    workflow_override_checks = _workflow_override_preflight_checks(
        graph,
        str(args.job_id),
        workflow_parameter_overrides,
    )
    # Task-support preflight: predict every Phase-3 task-conversion refusal
    # here so --dry-run (which exits before Phase 3) and the pre-upload plan
    # surface it instead of ending green. Same check Phase 3 enforces —
    # task_conversion_refusal is the single source of truth. (EXBRIDGE dry-run
    # 2026-10-13: a for_each_task job dry-ran "3 OK" with zero findings.)
    workflow_override_checks.extend(_task_support_preflight_checks(graph))
    plan_text = pre_flight(
        graph,
        export_output,
        args.prefix,
        str(args.job_id),
        cfg=_cfg,
        workflow_parameter_overrides=workflow_parameter_overrides,
        workflow_override_checks=workflow_override_checks,
    )
    print(plan_text)

    # Persist workflow-refusal findings before any strict-mode exit so the
    # JSON report always explains why Phase 3 would refuse a workflow. In a
    # normal run Phase 2 uploads are intentionally allowed to continue; the
    # refusal still occurs before that workflow's first cluster operation.
    refused_checks = [
        check for check in workflow_override_checks
        if check["status"] == "WILL REFUSE"
    ]
    for check in workflow_override_checks:
        severity = "ERROR" if check["status"] == "WILL REFUSE" else "WARN"
        report.errors.append(
            f"{severity} workflow-conversion-preflight: "
            f"{check['name']}: {check['detail']}"
        )
    if refused_checks and not args.dry_run:
        for check in refused_checks:
            tprint(
                f"[WARN] {check['name']} will be refused in Phase 3; "
                "Phase 2 uploads continue (use --dry-run to preview)"
            )

    # Disqualifying failures
    task_failed = [n for n in graph.notebooks.values() if n.role == "task" and n.export_error is not None]
    run_dep_failed = [n for n in graph.notebooks.values() if n.role == "run_dep" and n.export_error is not None]
    resource_failed = [
        n for n in graph.notebooks.values()
        if n.role == "resource" and n.export_error is not None
    ]
    file_failed = [
        n for n in graph.files.values()
        if n.fetch_error not in (None, "external_uri_skipped")
        and n.aidp_fallback_path is None
        and not n.aidp_replacement_paths
    ]
    _unfetched_children = [
        j for j in graph.jobs.values()
        if j.fetch_error is not None and j.dbx_job_id != str(args.job_id)
    ]
    # Provenance alone cannot establish absence. Apply the same predicate as
    # phase 3: only definitively absent soft candidates may be warned/skipped.
    child_job_failed = [j for j in _unfetched_children if not can_skip_absent_soft_match(j)]
    soft_unresolved = [j for j in _unfetched_children if can_skip_absent_soft_match(j)]
    for j in soft_unresolved:
        _msg = (f"job_id={j.dbx_job_id} found by regex only in "
                f"{', '.join(j.referenced_by) or '?'} was confirmed absent "
                f"({j.fetch_error}); skipped as a soft candidate. "
                "If this is a REST/wrapper child launch, that launch would fail "
                "because the referenced job does not exist; review the reference.")
        tprint(f"[WARN] soft-match-job-unresolved: {_msg}")
        report.errors.append(f"WARN soft-match-job-unresolved: {_msg}")

    if file_failed:
        # This exporter copies things from DBX to AIDP; library install / init
        # script attach happens later. Failed file fetches are recorded in the
        # report and the export continues. The cluster may then start without
        # those JARs/init-scripts attached — that's a runtime concern for the
        # user, not an export concern.
        msg = (f"{len(file_failed)} file(s) failed to fetch from DBX. "
               f"Export will continue without them; "
               f"check the report and library/init-script install on AIDP manually.")
        tprint(f"[WARN] {msg}")
        for f in file_failed:
            tprint(f"        - {f.dbx_source}: {f.fetch_error}: {f.error_detail or ''}")
        report.errors.append(f"WARN file-fetch-failed: {msg}")

    if args.strict and task_failed:
        print(f"\nERROR (--strict): {len(task_failed)} task notebook(s) failed; refusing to create incomplete workflow.", file=sys.stderr)
        path = report.write(args.report_dir)
        print(f"Report: {path}", file=sys.stderr)
        return ExitCode.STRICT_TASK_FAILED

    if args.strict_deps and (run_dep_failed or resource_failed or child_job_failed):
        print(
            "\nERROR (--strict-deps): %run, local resource, or child job "
            "dependency failure detected. Aborting.",
            file=sys.stderr,
        )
        path = report.write(args.report_dir)
        print(f"Report: {path}", file=sys.stderr)
        return ExitCode.STRICT_DEPS_FAILED

    if args.dry_run:
        path = report.write(args.report_dir)
        print(f"\n--dry-run: no AIDP writes performed. Report: {path}")
        return ExitCode.JOB_CREATE_FAILED if refused_checks else ExitCode.OK

    # ─── Phase 2: upload ──────────────────────────────────────────
    tprint("Phase 2: uploading notebooks…")
    nb_target_paths, failed_task_paths = phase2_upload_notebooks(
        aidp=aidp, graph=graph, export_output=export_output,
        overwrite=args.overwrite,
        run_id=run_id, root_job_id=str(args.job_id), report=report,
        path_plan=path_plan,
    )
    failed_resource_uploads = [
        item for item in report.notebooks
        if item.role == "resource" and item.status in (
            "EXPORT_FAILED", "FAILED", "FINGERPRINT_MISMATCH"
        )
    ]
    failed_shared_uploads = [
        item for item in report.notebooks
        if item.alias_of and item.status not in ("VALIDATED", "SKIPPED_EXISTS")
    ]
    if failed_shared_uploads or (args.strict_deps and failed_resource_uploads):
        # REF-294: accepting a source alias must never turn a failed shared
        # upload into a successful export, including without --strict-deps.
        failure = (f"{len(failed_resource_uploads)} local resource dependency upload(s) failed"
                   if args.strict_deps and failed_resource_uploads
                   else f"{len(failed_shared_uploads)} shared source artifact upload(s) failed")
        label = "ERROR (--strict-deps)" if args.strict_deps else "ERROR"
        print(f"\n{label}: {failure}. Aborting before Phase 3.", file=sys.stderr)
        path = report.write(args.report_dir)
        print(f"Report: {path}", file=sys.stderr)
        return ExitCode.STRICT_DEPS_FAILED if args.strict_deps else ExitCode.MANDATORY_FILE_UPLOAD_FAILED
    if failed_task_paths:
        # AIDP accepts workflows with dangling notebookPath references, so
        # we only abort here under --strict. Default: log + continue; the
        # workflow gets created with the canonical paths and tasks for
        # failed notebooks will fail at runtime with a clear AIDP error.
        if args.strict:
            print(f"\nERROR (--strict): {len(failed_task_paths)} task notebook(s) "
                  f"failed to export or upload. Aborting before Phase 3.", file=sys.stderr)
            for p in failed_task_paths:
                print(f"  - {p}", file=sys.stderr)
            path = report.write(args.report_dir)
            print(f"Report: {path}", file=sys.stderr)
            return ExitCode.STRICT_TASK_UPLOAD_FAILED
        tprint(f"[WARN] {len(failed_task_paths)} task notebook(s) failed to export/upload — "
               f"workflow will be created with dangling notebookPath references "
               f"(runtime failure, not export failure).")
        for p in failed_task_paths:
            tprint(f"        - {p}")

    if args.notebook_only:
        # Notebooks (and %run utility scripts) are uploaded. Stop here: skip
        # file/jar/init/wheel uploads and Phase 3 (clusters + workflow). Nothing
        # cluster-related is created or touched.
        path = report.write(args.report_dir)
        print("")
        print("=== --notebook-only: notebooks uploaded; skipped files, clusters, and workflow (Phase 3) ===")
        s = report.summary()
        print("=== SUMMARY ===")
        for k, v in s.items():
            print(f"  {k}: {v}")
        print(f"Report: {path}")
        # (--strict + failed task notebooks already aborted with rc=8 above.)
        return ExitCode.OK

    tprint("Phase 2: uploading files (jars / init scripts / wheels)…")
    try:
        uri_to_aidp, cluster_file_upload_failed = phase2_upload_files(
            aidp=aidp, graph=graph, export_output=export_output,
            lib_repository_dir=lib_repository_dir,
            overwrite=args.overwrite, report=report,
            strict_jars=args.strict_jars,
            path_plan=path_plan,
        )
    except RuntimeError as e:
        # A mandatory (non-JAR) file upload failed — fail loud. Persist the
        # This also covers a DBX JAR that is absent from the shared AIDP
        # fallback repository. Persist the report before aborting Phase 3.
        print(f"\nERROR: {e}", file=sys.stderr)
        report.errors.append(f"mandatory-file-upload-failed: {e}")
        path = report.write(args.report_dir)
        print(f"Report: {path}", file=sys.stderr)
        return ExitCode.MANDATORY_FILE_UPLOAD_FAILED
    if cluster_file_upload_failed:
        tprint("[WARN] some file uploads failed — export continues. "
               "Cluster libraries/init-scripts for those files will be missing; "
               "review the report and install them manually if needed.")
        report.errors.append("WARN file-upload-failed: see report.files for details")

    # ─── Phase 3: clusters + workflows ────────────────────────────
    tprint("Phase 3: creating clusters and workflows…")
    phase3_create_clusters_and_workflows(
        aidp=aidp, dbx=dbx, graph=graph,
        nb_target_paths=nb_target_paths, uri_to_aidp=uri_to_aidp,
        export_output=export_output, prefix=args.prefix,
        overwrite_workflow=args.overwrite,
        cluster_override_key=args.cluster_key,
        report=report,
        force_pause_schedule=not args.preserve_schedule_status,
        cfg=_cfg,
        lib_repository_dir=lib_repository_dir,
        workflow_parameter_overrides=workflow_parameter_overrides,
    )

    # ─── Done ─────────────────────────────────────────────────────
    path = report.write(args.report_dir)
    print("")
    s = report.summary()
    print("=== SUMMARY ===")
    for k, v in s.items():
        print(f"  {k}: {v}")
    print(f"Report: {path}")
    # Non-zero exit if any job creation failed…
    if s["jobs_failed"] != 0:
        return ExitCode.JOB_CREATE_FAILED
    # …or if a (JAR) file upload soft-failed. Distinct code so CI can catch a
    # missing JAR from $? without parsing logs. (Mandatory non-JAR upload
    # failures already hard-raise in phase2_upload_files.)
    if cluster_file_upload_failed:
        return ExitCode.OPTIONAL_FILE_UPLOAD_FAILED
    # …or if Spark tuning conf was silently dropped — operator must acknowledge
    # before running the workload (see [WARN] lines above).
    if any(e.startswith(SPARK_TUNING_DROP_MARKER) for e in report.errors):
        return ExitCode.SPARK_TUNING_DROPPED
    return ExitCode.OK


if __name__ == "__main__":
    sys.exit(main())
