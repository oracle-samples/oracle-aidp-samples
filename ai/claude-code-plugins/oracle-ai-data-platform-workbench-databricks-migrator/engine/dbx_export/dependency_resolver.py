"""BFS over notebook and job dependency graph.

Starting from a single DBX job_id, walks:
  - tasks[].notebook_task.notebook_path        → enqueue notebook
  - tasks[].libraries[]                        → enqueue library files
  - tasks[].new_cluster.init_scripts[]         → enqueue init scripts
  - job_clusters[].new_cluster.init_scripts[]  → same
  - tasks[].existing_cluster_id                → enqueue cluster fetch
  - inside each notebook source:
      %run <path>                              → enqueue notebook
      dbutils.notebook.run("path", ...)        → enqueue notebook
      oidlUtils.notebook.run("path", ...)      → enqueue notebook
      dbutils.jobs.runNow(job_id=N)            → enqueue child job (recurse)

All discovery results are recorded in a `Graph` dataclass. Actual downloads
happen later — this module only enumerates and reads notebook source bytes
(once each, cached).
"""

from __future__ import annotations

import ast
import itertools
import os
import posixpath
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from . import foreach_bridge
from .dbx_client import DbxClient, DbxExportError
from .sanitizer import rewrite_embedded_workspace_ref

MAX_DEPTH_DEFAULT = 10

_LOCAL_RESOURCE_EXTENSIONS = frozenset({
    ".py", ".yaml", ".yml", ".json", ".toml", ".ini", ".cfg", ".conf",
    ".properties", ".txt", ".csv", ".sql", ".sh",
})
_NON_NOTEBOOK_PATH_EXTENSIONS = (
    ".py", ".jar", ".whl", ".csv", ".json", ".pkl", ".pickle",
    ".parquet", ".avro", ".orc", ".delta", ".txt", ".tsv", ".log",
    ".yaml", ".yml", ".toml", ".ini", ".cfg", ".conf", ".properties",
    ".sh", ".sql", ".zip", ".tar", ".tar.gz", ".gz", ".bz2", ".lz4",
    ".snappy", ".html", ".md", ".xml", ".pdf", ".png", ".jpg",
    ".jpeg", ".gif", ".svg", ".ico", ".xlsx", ".xls", ".doc", ".docx",
    ".bin", ".dat", ".h5", ".hdf5", ".onnx", ".pt", ".pth", ".pb",
    ".ckpt", ".joblib", ".npy", ".npz", ".safetensors", ".model",
    ".arrow", ".feather", ".pem", ".key", ".crt", ".p12", ".env",
)
# Candidate heuristic for unqualified absolute filesystem paths, not a list
# of forbidden workspace folder names. Explicit /Workspace/... paths remain
# workspace candidates; the exporter's object-type check is authoritative.
# Storage roots: docs.databricks.com/aws/en/dbfs/root-locations and
# docs.databricks.com/aws/en/volumes#reserved-paths-for-volumes. Driver paths
# follow docs.databricks.com/aws/en/files/ and the existing runtime-prefix
# policy in cluster_converter.py. Match path segments, never root lookalikes.
_NON_NOTEBOOK_ABSOLUTE_ROOTS = (
    "/Volumes", "/volumes", "/Volume", "/volume", "/dbfs", "/mnt",
    "/FileStore", "/databricks-datasets", "/databricks-results",
    "/user/hive/warehouse", "/tmp", "/local_disk0", "/databricks",
)
_NON_NOTEBOOK_WORKSPACE_OBJECT_TYPES = frozenset({
    "DIRECTORY", "REPO", "FILE", "LIBRARY", "MLFLOW_EXPERIMENT", "DASHBOARD",
})
_RESOURCE_SCAN_LIMIT = 500
_WORKSPACE_CONTAINER_ROOTS = frozenset({
    "/Workspace", "/Users", "/Repos", "/Shared",
})
_MAX_STATIC_VALUES = 64


@dataclass
class NotebookNode:
    dbx_path: str
    # task is referenced by tasks[].notebook_task; the remaining roles are
    # discovered dependencies or repository companion resources.
    role: str  # "task" | "run_dep" | "py_sibling" | "resource"
    referenced_by: List[str] = field(default_factory=list)
    # True only while every reference came from the notebook-wide literal scan
    # or an unresolved selector's candidate values.
    loose_only: bool = False
    # A literal %run/notebook.run call names this path as a notebook. Static
    # candidates may be confirmed for export without proving a runtime call.
    explicit_run_ref: bool = False
    # Once an inferred target has been exported as a plain file, repeated
    # inference must not promote it back to a runnable notebook.
    inferred_plain_file: bool = False
    content: Optional[bytes] = None        # populated on first export
    export_error: Optional[str] = None     # if export failed
    error_detail: Optional[str] = None
    nb_dict: Optional[Dict[str, Any]] = None  # parsed (for fingerprint)
    # Git context — populated when this notebook is git-backed
    # (notebook_task.source == "GIT" in the DBX job spec, or inherited from
    # the parent notebook when discovered via %run). When set, fetch goes via
    # the GitHub proxy instead of workspace/export.
    git_owner: Optional[str] = None
    git_repo: Optional[str] = None
    git_branch: Optional[str] = None
    # After export: True if it's a real Databricks notebook (.ipynb on AIDP),
    # False if it's a plain Python/Scala utility module (preserve extension).
    is_notebook: bool = True
    final_extension: str = ".ipynb"
    export_method: Optional[str] = None
    resolved_git_path: Optional[str] = None
    # Static job/task/CLI values available while analysing this notebook.
    # Multiple tasks can reference the same notebook with different values;
    # in that case each key stores the bounded union of those values.
    parameter_bindings: Dict[str, Any] = field(default_factory=dict)
    resource_discovery_failures: List[Dict[str, str]] = field(default_factory=list)
    # Plain .py modules only: importing notebooks this module was already
    # scanned for (see _discover_module_resources).
    module_anchors_scanned: Set[str] = field(default_factory=set)
    # Resource hint names already searched from this notebook's folders. The
    # nearest match depends only on (notebook, hint), so imported modules
    # repeating the notebook's hints must not re-list the same directories.
    resource_hints_scanned: Set[str] = field(default_factory=set)
    # Exact repository-relative paths may equal hint basenames (e.g. config).
    resource_paths_scanned: Set[str] = field(default_factory=set)


def _fetched_location(node: NotebookNode, dbx_path: str) -> str:
    """Return dbx_path in the folder its Git file was fetched from.

    The Git fetcher's 404-only parent-whitespace fallback can return
    "Folder/Notebook.py" for the requested "Folder /Notebook". The requested
    folder does not exist, so discovery must list the folder the file came
    from. Every other path is returned unchanged.
    """
    if not (node.git_owner and node.git_repo and node.resolved_git_path and "/" in dbx_path):
        return dbx_path
    requested, name = dbx_path.rsplit("/", 1)
    fetched = node.resolved_git_path.rsplit("/", 1)[0] if "/" in node.resolved_git_path else ""
    if fetched == requested.lstrip("/"):
        return dbx_path
    prefix = "/" if dbx_path.startswith("/") else ""
    return f"{prefix}{fetched}/{name}" if fetched else f"{prefix}{name}"


def extract_local_resource_hints(nb_dict: Dict[str, Any]) -> Set[str]:
    """Return directory names that may hold local code/config resources.

    Union of import/constant-derived names (resource FILES may accompany the
    notebook) and enumeration-derived names (see
    extract_enumerated_resource_hints). Callers that promote directory
    contents to notebook DEPENDENCIES must use the enumerated subset only —
    an ordinary `import models` must never make models/draft.ipynb a
    mandatory dependency (review finding, 2026-10-13).
    """
    imports, enumerated = _resource_hint_sets(nb_dict)
    return imports | enumerated


def extract_enumerated_resource_hints(nb_dict: Dict[str, Any]) -> Set[str]:
    """Only the hints evidenced by directory ENUMERATION + dynamic read —
    the signal that the directory's contents are consumed at runtime."""
    return _resource_hint_sets(nb_dict)[1]


def _resource_hint_sets(nb_dict: Dict[str, Any]) -> tuple:
    hints: Set[str] = set()
    trees: List[ast.AST] = []
    for cell in nb_dict.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        source = cell.get("source", [])
        src = "".join(source) if isinstance(source, list) else (source or "")
        active = _active_source(src)
        try:
            tree = _parse_python_source(active)
        except SyntaxError:
            continue
        trees.append(tree)
    for tree in trees:
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                top = node.module.split(".", 1)[0]
                if top and top.isidentifier():
                    hints.add(top)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    top = alias.name.split(".", 1)[0]
                    if top and top.isidentifier():
                        hints.add(top)
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                value = node.value.strip().replace("\\", "/")
                if "://" in value or value.startswith("/"):
                    continue
                suffix = posixpath.splitext(value)[1].lower()
                parts = [p for p in value.split("/") if p not in ("", ".", "..")]
                if suffix in _LOCAL_RESOURCE_EXTENSIONS and len(parts) > 1:
                    hints.add(parts[0])

    # A Git notebook may enumerate a known repository directory via the
    # GitHub contents API, then open a dynamically selected file beneath it:
    # TARGET_FOLDER = "models"; url = f".../contents/{TARGET_FOLDER}?ref=..."
    # open("../" + item["yaml_path"], "r").  The file path itself cannot be
    # resolved statically, but the directory that must accompany the notebook
    # can. Require both the actual GET and a dynamic relative read; a GitHub
    # URL or a local filename mentioned in isolation is not a resource edge.
    if not trees:
        return hints, set()
    env = _static_environment(trees, {})
    github_urls: Dict[str, Set[str]] = {}

    def contents_roots(value: ast.AST) -> Set[str]:
        if not isinstance(value, ast.JoinedStr) or not value.values:
            return set()
        first = value.values[0]
        if not (isinstance(first, ast.Constant)
                and isinstance(first.value, str)
                and first.value.startswith("https://api.github.com/repos/")):
            return set()
        roots: Set[str] = set()
        for left, right in zip(value.values, value.values[1:]):
            if not (isinstance(left, ast.Constant)
                    and isinstance(left.value, str)
                    and left.value.endswith("/contents/")
                    and isinstance(right, ast.FormattedValue)):
                continue
            for candidate in _static_values(right.value, env, {}):
                if not isinstance(candidate, str):
                    continue
                path = candidate.strip().replace("\\", "/")
                if not path or path.startswith("/") or "://" in path:
                    continue
                root = path.split("/", 1)[0]
                if root not in ("", ".", ".."):
                    roots.add(root)
        return roots

    for tree in trees:
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                roots = contents_roots(node.value)
                for target in node.targets:
                    if roots and isinstance(target, ast.Name):
                        github_urls.setdefault(target.id, set()).update(roots)

    has_dynamic_relative_read = False
    requested_roots: Set[str] = set()

    def call_argument(call: ast.Call, position: int, name: str) -> Optional[ast.AST]:
        if len(call.args) > position:
            return call.args[position]
        return next((kw.value for kw in call.keywords if kw.arg == name), None)

    for tree in trees:
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            call_name = _dotted_name(node.func)
            if call_name in {"open", "builtins.open", "io.open"}:
                path_node = call_argument(node, 0, "file")
                if path_node is None or _static_values(path_node, env, {}):
                    continue
                prefix = None
                if (isinstance(path_node, ast.BinOp)
                        and isinstance(path_node.op, ast.Add)
                        and isinstance(path_node.left, ast.Constant)):
                    prefix = path_node.left.value
                elif isinstance(path_node, ast.JoinedStr) and path_node.values:
                    first = path_node.values[0]
                    prefix = first.value if isinstance(first, ast.Constant) else None
                if prefix in {"../", "./"}:
                    mode_node = call_argument(node, 1, "mode")
                    modes = (_static_values(mode_node, env, {})
                             if mode_node is not None else ["r"])
                    if any(isinstance(mode, str) and ("r" in mode or "+" in mode)
                           for mode in modes):
                        has_dynamic_relative_read = True
            elif call_name == "requests.get":
                url_node = call_argument(node, 0, "url")
                if url_node is None:
                    continue
                if isinstance(url_node, ast.Name):
                    requested_roots.update(github_urls.get(url_node.id, set()))
                else:
                    requested_roots.update(contents_roots(url_node))
    enumerated = set(requested_roots) if has_dynamic_relative_read else set()
    return hints, enumerated


def _parameter_values(value: Any) -> List[Any]:
    """Return the bounded value set represented by a parameter binding."""
    if isinstance(value, _StaticValueSet):
        return _merge_static_values([], value)
    if isinstance(value, list):
        return list(value[:_MAX_STATIC_VALUES])
    return [value]


def _merge_parameter_bindings(
    existing: Dict[str, Any], incoming: Optional[Dict[str, Any]],
) -> None:
    """Merge task-specific bindings without losing alternate task values."""
    for key, value in (incoming or {}).items():
        values = _merge_static_values(
            _parameter_values(existing[key]) if key in existing else [],
            _parameter_values(value),
        )
        existing[key] = (values[0] if len(values) == 1
                         and not isinstance(values, _StaticValueSet) else values)


def extract_workspace_resource_paths(
    nb_dict: Dict[str, Any],
    parameter_bindings: Optional[Dict[str, Any]] = None,
) -> Set[str]:
    """Resolve absolute Databricks workspace files read by notebook code.

    This deliberately covers file-reading APIs rather than every path-shaped
    string. It catches parameter-derived dependencies such as
    ``open(f"{widget_root}/config.csv")`` without treating output/model paths
    that merely appear as literals as export dependencies.
    """
    trees: List[ast.AST] = []
    for cell in nb_dict.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        source = cell.get("source", [])
        src = "".join(source) if isinstance(source, list) else (source or "")
        try:
            trees.append(_parse_python_source(_active_source(src)))
        except SyntaxError:
            continue

    bindings = dict(parameter_bindings or {})
    static_env = _static_environment(trees, bindings)
    paths: Set[str] = set()
    pandas_reads = {
        "pd.read_csv", "pandas.read_csv", "pd.read_json", "pandas.read_json",
        "pd.read_parquet", "pandas.read_parquet", "pd.read_pickle",
        "pandas.read_pickle", "pd.read_excel", "pandas.read_excel",
    }
    for tree in trees:
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not node.args:
                continue
            call_name = _dotted_name(node.func)
            is_open = call_name in {"open", "builtins.open", "io.open"}
            if not is_open and call_name not in pandas_reads:
                continue

            if is_open:
                mode_node = node.args[1] if len(node.args) > 1 else None
                for keyword in node.keywords:
                    if keyword.arg == "mode":
                        mode_node = keyword.value
                        break
                modes = (
                    _static_values(mode_node, static_env, bindings)
                    if mode_node is not None else ["r"]
                )
                if modes and not any(
                    isinstance(mode, str) and ("r" in mode or "+" in mode)
                    for mode in modes
                ):
                    continue

            for value in _static_values(node.args[0], static_env, bindings):
                if not isinstance(value, str):
                    continue
                path = value.strip().replace("\\", "/")
                if not path.startswith(
                    ("/Workspace/", "/Users/", "/Repos/", "/Shared/")
                ):
                    continue
                if posixpath.splitext(path)[1].lower() not in _LOCAL_RESOURCE_EXTENSIONS:
                    continue
                paths.add(posixpath.normpath(path))
    return paths


def extract_config_directory_paths(
    nb_dict: Dict[str, Any],
    parameter_bindings: Optional[Dict[str, Any]] = None,
) -> tuple[Set[str], bool]:
    """Resolve explicit load_config directory arguments, without executing code.

    REF-239: the bootstrap passes a directory to its imported loader; there is
    no concrete YAML filename in the notebook for file-read discovery to see.
    This explicitly supported reader contract does not infer the behavior of
    arbitrary customer functions or execute their implementations.
    """
    trees = []
    readers = {"load_config"}
    for cell in nb_dict.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        source = cell.get("source", "")
        source = "".join(source) if isinstance(source, list) else source
        try:
            tree = _parse_python_source(_active_source(source or ""))
        except SyntaxError:
            continue
        trees.append(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                readers.update(alias.asname or alias.name for alias in node.names
                               if alias.name == "load_config")
    bindings = dict(parameter_bindings or {})
    env = {str(key): _parameter_values(value) for key, value in bindings.items()}

    directories = set()
    unresolved = False
    widget_defaults = set()
    handled_calls = set()
    binding_states = {str(key): _StaticEvaluation.parameter(value) for key, value in bindings.items()}
    name_states = dict(binding_states)
    invalidated_containers = {}  # strong references prevent identity reuse

    def refresh(state):
        # A later operand can mutate an object captured by an earlier operand.
        # Refresh detached snapshots before assigning/consuming their values.
        pending, seen = [state], set()
        while pending:
            current = pending.pop()
            if id(current) in seen:
                continue
            seen.add(id(current))
            if invalidated_containers.keys() & current.containers.keys():
                current.covered = False
            pending.extend(child for fields in current.fields.values() for child in fields.values())

    def evaluate(node, *, effects=True):
        state = _StaticEvaluation(name_states, binding_states,
                                  on_unsupported=invalidate if effects else None)
        values = _static_values(node, env, bindings, config_paths=True, completeness=state)
        refresh(state)
        return values, state.snapshot()

    def is_reader(node):
        if not isinstance(node, ast.Call):
            return False
        name = _dotted_name(node.func)
        return name in readers or name.endswith(".load_config")

    def invalidate(statement, exposed=()):
        # Unsupported/unknown control flow must not leave confident values
        # behind. Supplied parameters retain precedence over widget defaults.
        for child in ast.walk(statement):
            if isinstance(child, ast.Call):
                api, _, operation = _dotted_name(child.func).rpartition(".")
                if api not in {"dbutils.widgets", "aidputils.widgets", "oidlUtils.widgets"}:
                    continue
                if operation not in {"removeAll", "remove", "text", "dropdown", "combobox", "multiselect"}:
                    continue
                argument = child.args[0] if child.args else next(
                    (kw.value for kw in child.keywords if kw.arg == "name"), None)
                names, state = evaluate(argument, effects=False)
                known_names = {name for name in names if isinstance(name, str)}
                unknown_name = argument is not None and not state.complete
                affected = (widget_defaults if operation == "removeAll" or not known_names or unknown_name
                            else widget_defaults.intersection(known_names))
                for name in affected:
                    bindings[name] = _StaticValueSet([])
                    binding_states.pop(name, None)
        # Unsupported calls may mutate containers exposed as receiver/arguments;
        # stores and in-place operators may mutate objects retained by aliases.
        # Reuse identity provenance rather than enumerating mutator method names.
        exposed = [] if is_reader(statement) else list(exposed)
        targets = []
        for child in ast.walk(statement):
            if isinstance(child, ast.Subscript) and isinstance(child.ctx, (ast.Store, ast.Del)):
                targets.append(evaluate(child.value, effects=False)[1])
            elif isinstance(child, ast.AugAssign):
                targets.append(evaluate(child.target, effects=False)[1])
        mutated = set()
        unknown_target = False
        pending = list(exposed)
        seen = set()
        containers = []
        while pending:
            state = pending.pop()
            if id(state) in seen:
                continue
            seen.add(id(state))
            unknown_target |= not state.complete
            containers.extend(state.containers.values())
            pending.extend(child for fields in state.fields.values() for child in fields.values())
        # Callees can reach nested containers through their argument/receiver.
        # This also covers raw parameter containers without per-field metadata.
        while containers:
            value = containers.pop()
            if id(value) in mutated:
                continue
            mutated.add(id(value))
            invalidated_containers[id(value)] = value
            children = value.values() if isinstance(value, dict) else value
            containers.extend(child for child in children if isinstance(child, (dict, list, tuple)))
        for state in targets:
            mutated.update(state.containers)
            invalidated_containers.update(state.containers)
            unknown_target |= not state.covered
        pending = list(name_states.values()) + list(binding_states.values())
        seen = set()
        while pending and (mutated or unknown_target):
            state = pending.pop()
            if id(state) in seen:
                continue
            seen.add(id(state))
            if (unknown_target and state.containers) or mutated.intersection(state.containers):
                state.covered = False
                invalidated_containers.update(state.containers)
            pending.extend(child for fields in state.fields.values() for child in fields.values())
        for child in ast.walk(statement):
            if isinstance(child, ast.Name) and isinstance(child.ctx, (ast.Store, ast.Del)):
                env[child.id] = []
                name_states.pop(child.id, None)

    def read_calls(statement):
        nonlocal unresolved
        for node in ast.walk(statement):
            if not is_reader(node):
                continue
            handled_calls.add(node)
            argument = node.args[0] if node.args else next(
                (kw.value for kw in node.keywords if kw.arg == "config_dir"), None)
            values, state = evaluate(argument)
            if not state.complete:
                unresolved = True
            for value in values:
                if not isinstance(value, str) or not value.strip():
                    unresolved = True
                    continue
                directories.add(value.strip().replace("\\", "/"))

    def assignments(statements):
        # The general evaluator unions repeated assignments; config roots need
        # Python's source order to avoid inventing repeated /Workspace prefixes.
        for statement in statements:
            if isinstance(statement, (ast.Assign, ast.AnnAssign)):
                if statement.value is None:  # annotation only; no runtime assignment
                    continue
                values, state = evaluate(statement.value)
                read_calls(statement.value)
                targets = statement.targets if isinstance(statement, ast.Assign) else [statement.target]
                for target in targets:
                    if isinstance(target, ast.Name):
                        refresh(state)
                        env[target.id] = values
                        name_states[target.id] = state
                    else:
                        evaluate(target)
                        invalidate(target)
            elif isinstance(statement, ast.Expr):
                call = statement.value
                # Widget registrations have their own supplied/default precedence.
                # Other expressions still need evaluation for unsupported effects.
                if (isinstance(call, ast.Call) and _dotted_name(call.func).rpartition(".")[0]
                        in {"dbutils.widgets", "aidputils.widgets", "oidlUtils.widgets"}):
                    for argument in list(call.args) + [kw.value for kw in call.keywords]:
                        evaluate(argument)
                else:
                    evaluate(call)
                if isinstance(call, ast.Call):
                    call_name = _dotted_name(call.func)
                    widget_api, _, operation = call_name.rpartition(".")
                    if widget_api in {"dbutils.widgets", "aidputils.widgets", "oidlUtils.widgets"}:
                        if operation == "removeAll":
                            for key in widget_defaults:
                                bindings.pop(key, None)
                                binding_states.pop(key, None)
                            widget_defaults.clear()
                        elif operation == "text" and len(call.args) >= 2:
                            names, names_state = evaluate(call.args[0])
                            defaults, default_state = evaluate(call.args[1])
                            for name in names:
                                if isinstance(name, str) and name not in bindings and defaults:
                                    bindings[name] = _StaticValueSet(defaults)
                                    widget_defaults.add(name)
                                    state = default_state.snapshot()
                                    state.covered &= names_state.complete
                                    binding_states[name] = state
                        else:
                            invalidate(statement)
                read_calls(statement)
            elif isinstance(statement, ast.If):
                tests, state = evaluate(statement.test)
                if not state.truth_complete:
                    tests = []
                if tests and (all(bool(value) for value in tests) or not any(bool(value) for value in tests)):
                    active, inactive = ((statement.body, statement.orelse) if all(bool(value) for value in tests)
                                        else (statement.orelse, statement.body))
                    # Proven unreachable calls do not make discovery incomplete.
                    handled_calls.update(child for branch in inactive for child in ast.walk(branch)
                                         if is_reader(child))
                    assignments(active)
                else:
                    evaluate(statement)
            else:
                evaluate(statement)

    for tree in trees:
        assignments(tree.body)
        # Every reader must either be resolved, rejected, or proven unreachable.
        # Compound statements/functions are not interpreted by this walker.
        if any(is_reader(node) and node not in handled_calls for node in ast.walk(tree)):
            unresolved = True
    return directories, unresolved


@dataclass
class FileNode:
    dbx_source: str            # e.g. "dbfs:/FileStore/jars/foo.jar"
    kind: str                  # "jar" | "whl" | "init_script" | "requirements"
    referenced_by: List[str] = field(default_factory=list)
    content: Optional[bytes] = None
    fetch_error: Optional[str] = None
    error_detail: Optional[str] = None
    # When a source JAR cannot be exported, Phase 1 probes the shared AIDP JAR
    # repository and records the matching path here. Phase 2 then attaches the
    # existing object through the normal cluster-library mapping.
    aidp_fallback_path: Optional[str] = None
    aidp_fallback_sha256: Optional[str] = None
    aidp_fallback_provenance: Optional[str] = None
    # AIDP-first hit: the shared dependency repository already holds a matching
    # jar/wheel (matched by name; exact version preferred, else highest). Set in
    # Phase 1 BEFORE any Databricks/proxy fetch — when set, the DBX fetch is
    # skipped entirely and Phase 2 attaches this existing object as-is.
    aidp_repo_path: Optional[str] = None
    aidp_repo_provenance: Optional[str] = None
    # A missing DBX-declared JAR may be superseded by one or more verified
    # class-index matches discovered from the notebook's actual JVM imports.
    # These replacement nodes are attached separately to the same cluster.
    aidp_replacement_paths: List[str] = field(default_factory=list)
    # Set True for init scripts whose only purpose was copying JARs to
    # /databricks/jars/. The JARs are installed via the AIDP libraries
    # API instead, and the script becomes a no-op — neither uploaded
    # nor attached to the cluster.
    skip_attach: bool = False


def can_skip_absent_soft_match(job_node) -> bool:
    """Shared gate; missing provenance/absence metadata always fails closed."""
    return (bool(getattr(job_node, "soft_match", False))
            and getattr(job_node, "job_def", None) is None
            and getattr(job_node, "fetch_error", None) is not None
            and bool(getattr(job_node, "fetch_error_is_not_found", False)))


@dataclass
class JobNode:
    dbx_job_id: str
    depth: int                 # 0 for root job
    job_def: Optional[Dict[str, Any]] = None
    fetch_error: Optional[str] = None
    referenced_by: List[str] = field(default_factory=list)
    # True when every notebook reference to this job came from the regex
    # fallback only (no dbutils.jobs.runNow/run_now call). Such an id may be an
    # unrelated ``job_id=`` literal (e.g. an HTTP payload), so an unfetchable
    # soft-matched job is skipped only on confirmed absence. Permission,
    # service and transport failures remain FAILED. A later hard match
    # clears the flag.
    soft_match: bool = False
    fetch_error_is_not_found: bool = False

    @property
    def can_skip_absent_soft_match(self) -> bool:
        return can_skip_absent_soft_match(self)


@dataclass
class Graph:
    jobs: Dict[str, JobNode] = field(default_factory=dict)
    notebooks: Dict[str, NotebookNode] = field(default_factory=dict)
    files: Dict[str, FileNode] = field(default_factory=dict)
    # Concrete workspace objects classified as non-notebook assets.  Keep this
    # separate from ``external_refs`` so a later dynamic-dispatch pass cannot
    # re-enqueue an object that the notebook BFS deliberately removed.
    skipped_assets: Set[str] = field(default_factory=set)
    external_refs: List[str] = field(default_factory=list)  # s3:// / http urls we don't fetch
    skipped_loose_candidates: List[Dict[str, str]] = field(default_factory=list)
    skipped_loose_paths: Set[str] = field(default_factory=set)
    # Source-cluster PyPI specifications that must be installed on the AIDP
    # execution context before they may suppress import inference.
    cluster_pypi_requirements: Dict[str, Set[str]] = field(default_factory=dict)


def task_cluster_context(job_id: str, task: Dict[str, Any]) -> Optional[str]:
    """Return the cluster context that actually executes *task*.

    Task-level libraries belong both to their task declaration and to this
    execution context.  Recording the latter lets reused-cluster installation
    consume a library that Databricks declared only under ``tasks[].libraries``.
    """
    if task.get("job_cluster_key"):
        return f"job:{job_id}/job_cluster:{task['job_cluster_key']}"
    if task.get("existing_cluster_id"):
        return f"existing_cluster:{task['existing_cluster_id']}"
    if task.get("new_cluster"):
        return f"job:{job_id}/task_cluster:{task.get('task_key', '?')}"
    return None


# ─── Source-level extraction ────────────────────────────────────────


_RUN_RE = re.compile(r'(?m)^[ \t]*%run[ \t]+([^\n#]+)')

# Language-agnostic match for dbutils/oidlUtils.notebook.run("path", ...).
# Works on Scala/R/SQL notebooks where ast.parse would raise SyntaxError and
# skip the cell entirely. \s* spans newlines so multi-line calls are caught.
_NOTEBOOK_RUN_RE = re.compile(
    r'(?:dbutils|oidlUtils|aidputils)\.notebook\.run\s*\(\s*(["\'])(.*?)\1'
)
# Detects a notebook.run( call to inspect its first argument — used to warn
# when the path is a runtime variable rather than a string literal.
_NOTEBOOK_RUN_CALL_RE = re.compile(
    r'(?:dbutils|oidlUtils|aidputils)\.notebook\.run\s*\(\s*(.)'
)

# Line-comment prefixes across the notebook languages we export:
# Python/R use '#', Scala/Java use '//', SQL uses '--'.
_COMMENT_PREFIXES = ("#", "//", "--")


def _active_source(src: str) -> str:
    """Drop blank lines and whole-line comments (Python/R '#', Scala '//',
    SQL '--') so commented-out %run / notebook.run calls aren't treated as
    real dependencies. Only strips lines that START with a comment marker —
    inline markers (e.g. 's3://...') are left untouched."""
    out = []
    for line in src.splitlines():
        stripped = line.lstrip()
        if not stripped:
            continue
        if stripped.startswith(_COMMENT_PREFIXES):
            continue
        out.append(line)
    return "\n".join(out)


def _python_parse_source(src: str, column_shifts: Optional[Dict[int, int]] = None) -> str:
    """Neutralize only magic lines rejected by Python's parser.

    Even in a cell containing %run, valid modulo/format continuations and
    percent-prefixed string contents must stay byte-for-byte unchanged.
    """
    # Position-aware docstring analysis preserves physical rows. Other callers
    # retain the original preparation, including opaque %python command bodies.
    lines = (src.replace("\r\n", "\n").replace("\r", "\n").split("\n")
             if column_shifts is not None else src.splitlines())
    for _ in range(len(lines)):
        candidate = "\n".join(lines)
        try:
            ast.parse(candidate)
            return candidate
        except SyntaxError as exc:
            index = (exc.lineno or 0) - 1
            if index < 0 or index >= len(lines) or not re.match(
                r"^\s*(?:%%?[A-Za-z_]\w*|![A-Za-z_][\w.-]*)",
                lines[index],
            ):
                return candidate
            python_magic = (re.match(r"^\s*%python(?=\s|$)", lines[index])
                            if column_shifts is not None else None)
            if python_magic:
                body = lines[index][python_magic.end():].lstrip(" \t")
                column_shifts[index + 1] = len(lines[index].encode("utf-8")) - len(body.encode("utf-8"))
                lines[index] = body
            else:
                lines[index] = "# [dependency discovery: notebook magic] " + lines[index]
    return "\n".join(lines)


def _parse_python_source(src: str) -> ast.AST:
    """Parse original Python first; only invalid syntax needs magic handling."""
    try:
        return ast.parse(src)
    except SyntaxError:
        return ast.parse(_python_parse_source(src))


def _percent_run_matches(src, nb_dict, cell):
    """Exclude %run in Python docstrings; preserve legacy discovery otherwise.

    Ordinary strings, including executable strings, remain candidates. Only
    the first string expression in a module/function/class is a docstring.
    Source the local interpreter cannot parse retains its existing fallback.
    """
    matches = list(_RUN_RE.finditer(_active_source(src)))
    if not matches:
        return []
    language = cell.get("metadata", {}).get("language") or nb_dict.get(
        "metadata", {}
    ).get("language_info", {}).get("name", "python")
    first_line = _active_source(src).partition("\n")[0].split(maxsplit=1)
    if first_line and first_line[0] in ("%python", "%scala", "%sql", "%r", "%sh"):
        language = first_line[0][1:]
    if language.lower() != "python":
        return matches
    src = src.replace("\r\n", "\n").replace("\r", "\n")
    column_shifts = {}
    try:
        tree = ast.parse(_python_parse_source(src, column_shifts))
    except SyntaxError:
        return matches
    # AST columns count UTF-8 bytes. Mask only the docstring expression span,
    # preserving rows and any other statements on its opening/closing lines.
    lines = src.encode("utf-8").split(b"\n")
    for scope in ast.walk(tree):
        if not isinstance(scope, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) or not scope.body:
            continue
        doc = scope.body[0]
        if not (isinstance(doc, ast.Expr) and isinstance(doc.value, ast.Constant)
                and isinstance(doc.value.value, str)):
            continue
        for row in range(doc.lineno, doc.end_lineno + 1):
            start = doc.col_offset + column_shifts.get(row, 0) if row == doc.lineno else 0
            end = doc.end_col_offset + column_shifts.get(row, 0) if row == doc.end_lineno else len(lines[row - 1])
            line = lines[row - 1]
            lines[row - 1] = line[:start] + b" " * (end - start) + line[end:]
    return list(_RUN_RE.finditer(_active_source(b"\n".join(lines).decode("utf-8"))))


def _strip_quotes(s: str) -> str:
    s = s.strip()
    if len(s) >= 2 and s[0] in ('"', "'") and s[-1] == s[0]:
        return s[1:-1]
    return s


def _percent_run_path(raw: str) -> str:
    """Parse a quoted or unquoted `%run` target without truncating spaces."""
    text = raw.strip()
    if not text:
        return ""
    if text[0] in ('"', "'"):
        quote = text[0]
        end = text.find(quote, 1)
        return text[1:end].strip().rstrip("/") if end > 0 else ""
    boundary = re.search(r"(?=\s+(?:\$[A-Za-z_]\w*|#))", text)
    target = text[:boundary.start()] if boundary else text
    return target.strip().rstrip("/")


def _resolve_run_target(raw: str, parent_dir: str) -> str:
    """Normalize a %run target to the canonical DBX REST path.

    Canonical form matches what DBX returns in task.notebook_task.notebook_path:
      /Users/foo@bar/notebook
      /Optimiser_Utils/Randomizer trigger
      /Repos/foo@bar/my_repo/notebook
    Strips any /Workspace/ prefix (DBX UI/FUSE convention) and any .ipynb
    extension. Resolves relative paths against parent_dir.

    parent_dir is the canonical DBX-form directory of the notebook being scanned.

    Divergence note (PR-34 review, 2026-07-27): a bare `Workspace/foo` input is
    canonicalized here to `/foo` (leading Workspace/ segment is stripped, then
    treated as absolute). This DISAGREES with job_migrate.py's `_fix_run`
    post-PR-34 (caller-relative) and with build_dag._normalize_ws_path
    (which prepends /Workspace/). The disagreement is intentional: this
    function produces the canonical export target that DBX REST would return;
    the other two functions are for different phases. If you change how bare
    `Workspace/` is interpreted anywhere, review all three sites together.
    """
    target = _percent_run_path(raw)
    # Strip $vars (e.g. "$path/foo")
    target = re.sub(r"\$\w+", "", target).strip()
    if not target:
        return ""
    # Strip /Workspace/ prefix if the caller wrote it.
    if target.startswith("/Workspace/"):
        target = target[len("/Workspace"):]   # keep leading '/'
    elif target.startswith("Workspace/"):
        target = "/" + target[len("Workspace/"):]
    # Resolve relative paths against parent.
    if target.startswith("./"):
        target = posixpath.join(parent_dir, target[2:])
    elif not target.startswith("/"):
        target = posixpath.join(parent_dir, target)
    # Strip .ipynb extension if present — DBX REST canonical paths have none.
    if target.endswith(".ipynb"):
        target = target[: -len(".ipynb")]
    return posixpath.normpath(target)


def _static_value_signature(value: Any):
    """Include provenance in dedup/fixpoint identity, not just Python value equality."""
    if isinstance(value, dict):
        provenance = getattr(value, "entry_provenance", {})
        return ("dict", tuple((repr(key), _static_value_signature(item),
                               _static_value_signature(provenance.get(key)))
                              for key, item in value.items()))
    if isinstance(value, (list, tuple)):
        return ("tuple" if isinstance(value, tuple) else "list",
                tuple(_static_value_signature(item) for item in value),
                isinstance(value, _StaticCandidates),
                repr(getattr(value, "dropped_values", [])),
                repr(getattr(value, "literal_candidates", [])))
    return repr(value)


def _dedup_values(values: List[Any], limit: int = _MAX_STATIC_VALUES) -> List[Any]:
    """Deduplicate bounded static-evaluation results without requiring hashability."""
    out: List[Any] = []
    seen: Set[str] = set()
    for value in values:
        marker = repr(_static_value_signature(value))
        if marker in seen:
            continue
        seen.add(marker)
        out.append(value)
        if len(out) >= limit:
            break
    return out


class _StaticValueSet(list):
    """Static values plus path literals from the notebook that supplied them."""

    def __init__(self, values=(), literal_candidates=()):
        super().__init__(values)
        self.literal_candidates = _dedup_values(list(literal_candidates))


class _StaticCandidates(_StaticValueSet):
    """List-compatible values whose selector could not be proven statically.

    Keep provenance with the value set so aliases and %run namespace unions
    cannot turn uncertain candidates into an apparently resolved dispatch.
    """

    def __init__(self, values=(), dropped_values=(), literal_candidates=()):
        super().__init__(values, literal_candidates)
        self.dropped_values = _dedup_values(list(dropped_values))


class _StaticMapping(dict):
    """Track selector provenance per field, so unrelated metadata cannot taint a path."""

    def __init__(self, values=(), entry_provenance=None):
        super().__init__(values)
        self.entry_provenance = dict(entry_provenance or {})


def _merge_static_values(left: List[Any], right: List[Any]) -> List[Any]:
    values = _dedup_values(list(left) + list(right))
    literals = getattr(left, "literal_candidates", []) + getattr(right, "literal_candidates", [])
    if isinstance(left, _StaticCandidates) or isinstance(right, _StaticCandidates):
        return _StaticCandidates(values, getattr(left, "dropped_values", [])
                                 + getattr(right, "dropped_values", []), literals)
    if literals:
        return _StaticValueSet(values, literals)
    return values


def _dotted_name(node: ast.AST) -> str:
    parts: List[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _bounded_product(groups: List[List[Any]]) -> List[tuple]:
    if not groups or any(not group for group in groups):
        return []
    out = []
    for values in itertools.product(*groups):
        out.append(values)
        if len(out) >= _MAX_STATIC_VALUES:
            break
    return out


class _StaticEvaluation:
    """Optional completeness channel, separate from additive candidate values.

    ``covered`` describes candidate alternatives; ``fields`` describes container
    contents independently. A known-key lookup need not consume an unrelated
    unknown field. Snapshots detach scope references before assignment/aliasing.
    """

    def __init__(self, names=None, bindings=None, on_unsupported=None):
        self.names = names if names is not None else {}
        self.on_unsupported = on_unsupported
        self.bindings = bindings if bindings is not None else {}
        self.values = []
        self.covered = True
        self.fields = {}
        self.open = set()
        self.children = []
        self.handled = False
        self.containers = {}
        self.evaluated = set()

    def remember_containers(self, values):
        # Preserve identities even when equal-valued candidates deduplicate.
        self.containers.update((id(value), value) for value in values
                               if isinstance(value, (dict, list, tuple)))

    @staticmethod
    def key(value):
        return repr(_static_value_signature(value))

    @classmethod
    def known(cls, values):
        state = cls()
        state.values = list(values)
        state.remember_containers(state.values)
        state.covered = bool(values)
        return state

    @classmethod
    def parameter(cls, value):
        state = cls.known(_parameter_values(value))
        if isinstance(value, list) and len(value) > _MAX_STATIC_VALUES:
            state.covered = False
        return state

    @property
    def truth_complete(self):
        # Contents do not change a nonempty container's truth value. Omitted
        # dict entries can change an apparently empty mapping's truth value.
        return self.covered and bool(self.values) and all(
            bool(value) or not isinstance(value, dict) or (
                self.key(value) not in self.open and all(
                    field in value for field in self.fields.get(self.key(value), {})))
            for value in self.values)

    @property
    def complete(self):
        return self.covered and bool(self.values) and all(
            self.key(value) not in self.open and all(
                child.complete for child in self.fields.get(self.key(value), {}).values())
            for value in self.values)

    def snapshot(self):
        state = _StaticEvaluation()
        state.values = list(self.values)
        state.covered = self.covered
        state.fields = {key: dict(fields) for key, fields in self.fields.items()}
        state.open = set(self.open)
        state.containers = dict(self.containers)
        return state

    def select(self, value):
        state = self.snapshot()
        key = self.key(value)
        state.values = [value]
        state.fields = {key: self.fields[key]} if key in self.fields else {}
        state.open.intersection_update({key})
        state.containers = {identity: container for identity, container in state.containers.items()
                            if self.key(container) == key}
        return state

    def merge_fields(self, key, fields):
        merged = dict(self.fields.get(key, {}))
        for field, child in fields.items():
            if field in merged:
                combined = merged[field].snapshot()
                combined.include(child)
                merged[field] = combined
            else:
                merged[field] = child
        self.fields[key] = merged

    def include(self, other):
        self.covered &= other.covered
        self.values = _dedup_values(self.values + other.values)
        self.containers.update(other.containers)
        for key, fields in other.fields.items():
            self.merge_fields(key, fields)
        self.open.update(other.open)

    def obscure(self, values):
        # An unknown key/**mapping may overwrite earlier entries. A later
        # explicit assignment can still establish its own field confidently.
        for value in values:
            key = self.key(value)
            self.open.add(key)
            fields = dict(self.fields.get(key, {}))
            for field in value:
                fields[field] = _StaticEvaluation.known([])
            self.fields[key] = fields

    def field(self, container, key, value):
        if isinstance(container, (list, tuple)) and isinstance(key, int) and key < 0:
            key += len(container)
        marker = self.key(container)
        fields = self.fields.get(marker, {})
        state = fields.get(key)
        if state is None:
            state = (_StaticEvaluation.known([]) if marker in self.open
                     else _StaticEvaluation.known([value]))
        result = state.snapshot()
        result.covered &= self.covered
        return result


def _static_values(
    node: ast.AST,
    env: Dict[str, List[Any]],
    parameter_bindings: Dict[str, Any],
    *,
    config_paths: bool = False,
    completeness=None,
) -> List[Any]:
    """Return existing candidates; optionally describe whether they are complete.

    The channel is opt-in. It follows the evaluator's actual recursive calls,
    including short-circuiting, rather than walking the syntax a second time.
    """
    values = _evaluate_static_values(node, env, parameter_bindings,
                                     config_paths=config_paths, completeness=completeness)
    if completeness is not None:
        if isinstance(node, ast.Call):
            # Arguments omitted by candidate inference still execute in Python,
            # even an unused getArgument fallback. Inspect only their effects;
            # do not mark them as consumed or change candidate completeness.
            if completeness.on_unsupported is not None:
                for argument in list(node.args) + [kw.value for kw in node.keywords]:
                    if id(argument) not in completeness.evaluated:
                        effects = _StaticEvaluation(completeness.names, completeness.bindings,
                                                    completeness.on_unsupported)
                        _static_values(argument, env, parameter_bindings,
                                       config_paths=config_paths, completeness=effects)
            # A modeled call cannot certify arguments it silently ignored.
            # Only explicit keywords actually consumed by evaluation qualify;
            # **kwargs remain unsupported without changing candidate values.
            completeness.covered &= all(keyword.arg is not None and
                                        id(keyword.value) in completeness.evaluated
                                        for keyword in node.keywords)
        if not completeness.handled:
            completeness.covered &= all(child.complete for child in completeness.children)
        completeness.covered &= bool(values)
        completeness.values = list(values)
        completeness.remember_containers(values)
        completeness.children = []
    return values


def _evaluate_static_values(
    node: ast.AST,
    env: Dict[str, List[Any]],
    parameter_bindings: Dict[str, Any],
    *,
    config_paths: bool = False,
    completeness=None,
) -> List[Any]:
    """Evaluate a small, side-effect-free subset of Python AST.

    This is dependency discovery, not notebook execution. The allowlist covers
    deterministic string/container construction commonly used for notebook
    paths. Unknown calls and expressions return no values.
    """
    # Keep the additional root-normalization operators local to REF-239's
    # directory reader. Existing notebook-run inference retains its contract.
    def evaluate(value, scope, bindings):
        if completeness is not None:
            completeness.evaluated.add(id(value))
        child = (_StaticEvaluation(completeness.names, completeness.bindings,
                                   completeness.on_unsupported)
                 if completeness is not None else None)
        values = _static_values(value, scope, bindings, config_paths=config_paths,
                                completeness=child)
        if completeness is not None:
            completeness.children.append(child)
        return values

    def child_state():
        return completeness.children[-1] if completeness is not None else None

    def finish(states):
        if completeness is not None:
            completeness.handled = True
            for state in states:
                completeness.include(state)

    def product(groups):
        if completeness is not None:
            size = 1
            for group in groups:
                size *= len(group)
            if size > _MAX_STATIC_VALUES:
                completeness.covered = False
        return _bounded_product(groups)

    def dedup(values):
        if completeness is not None:
            unique = {repr(_static_value_signature(value)) for value in values}
            if len(unique) > _MAX_STATIC_VALUES:
                completeness.covered = False
        return _dedup_values(values)

    if isinstance(node, ast.Constant):
        return [node.value]
    if isinstance(node, ast.Name):
        values = list(env.get(node.id, []))
        if completeness is not None:
            finish([completeness.names.get(node.id, _StaticEvaluation.known(values))])
        return values
    if isinstance(node, ast.FormattedValue):
        values = evaluate(node.value, env, parameter_bindings)
        if completeness is not None:
            # Candidates historically use str(value). Other conversions and
            # nonempty format specs are not modeled by that contract: keep
            # the candidates additive, but never certify them as exhaustive.
            completeness.covered &= node.conversion in {-1, ord("s")}
            spec = node.format_spec
            if spec is not None and not (isinstance(spec, ast.JoinedStr) and not spec.values):
                specs = evaluate(spec, env, parameter_bindings)
                completeness.covered &= bool(specs) and all(value == "" for value in specs)
        return [str(v) for v in values]
    if isinstance(node, ast.JoinedStr):
        groups: List[List[Any]] = []
        for value in node.values:
            vals = evaluate(value, env, parameter_bindings)
            if not vals:
                return []
            groups.append(vals)
        return dedup(["".join(str(v) for v in combo)
                              for combo in product(groups)])
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = evaluate(node.left, env, parameter_bindings)
        left_state = child_state()
        right = evaluate(node.right, env, parameter_bindings)
        right_state = child_state()
        if completeness is not None:
            completeness.handled = True
            completeness.covered &= left_state.covered and right_state.covered
        out = []
        for a, b in product([left, right]):
            if isinstance(a, str) and isinstance(b, str):
                out.append(a + b)
            elif isinstance(a, list) and isinstance(b, list):
                out.append(a + b)
            elif isinstance(a, tuple) and isinstance(b, tuple):
                out.append(a + b)
            elif completeness is not None:
                completeness.covered = False
            if completeness is not None and isinstance(a, (list, tuple)) and type(a) is type(b):
                fields = {index: left_state.field(a, index, value) for index, value in enumerate(a)}
                fields.update({len(a) + index: right_state.field(b, index, value)
                               for index, value in enumerate(b)})
                completeness.merge_fields(completeness.key(a + b), fields)
        return dedup(out)
    if config_paths and isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        values = evaluate(node.operand, env, parameter_bindings)
        if completeness is not None:
            completeness.handled = True
            completeness.covered &= child_state().truth_complete
        return [not value for value in values]
    if config_paths and isinstance(node, ast.BoolOp):
        # REF-239: repository roots use `(widgets.get(...) or "").strip()
        # and a default fallback. Honor Python short-circuiting only when the
        # operand is known; an unknown widget must not select a guessed default.
        result = []
        selected = []
        for index, operand in enumerate(node.values):
            values = evaluate(operand, env, parameter_bindings)
            if not values:
                return []
            state = child_state()
            if completeness is not None:
                completeness.covered &= (state.truth_complete if index < len(node.values) - 1
                                         else state.covered)
            continuing = False
            for value in values:
                short_circuits = (
                    bool(value) if isinstance(node.op, ast.Or) else not bool(value)
                )
                if short_circuits or index == len(node.values) - 1:
                    result.append(value)
                    if state is not None:
                        selected.append(state.select(value))
                else:
                    continuing = True
            if not continuing:
                break
        finish(selected)
        return dedup(result)
    if isinstance(node, ast.IfExp):
        if completeness is not None and completeness.on_unsupported is not None:
            # The predicate does not narrow additive candidates, but does execute.
            effects = _StaticEvaluation(completeness.names, completeness.bindings,
                                        completeness.on_unsupported)
            _static_values(node.test, env, parameter_bindings,
                           config_paths=config_paths, completeness=effects)
        # A runtime condition may select either target. Discovering both is
        # safer than dropping one; existence validation filters candidates.
        body = evaluate(node.body, env, parameter_bindings)
        body_state = child_state()
        alternative = evaluate(node.orelse, env, parameter_bindings)
        finish([body_state, child_state()] if completeness is not None else [])
        return dedup(body + alternative)
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        groups = [evaluate(elt, env, parameter_bindings) for elt in node.elts]
        combos = product(groups)
        factory = tuple if isinstance(node, ast.Tuple) else list
        values = [factory(combo) for combo in combos]
        if completeness is not None:
            states = completeness.children
            completeness.handled = True
            completeness.covered &= all(state.covered for state in states)
            for value, combo in zip(values, combos):
                completeness.fields[completeness.key(value)] = {
                    index: state.select(item) for index, (state, item)
                    in enumerate(zip(states, combo))}
        return values
    if isinstance(node, ast.Dict):
        # Keep the statically-known entries of a mixed config dictionary.
        # A notebook-path field is often accompanied by runtime-only values
        # (for example translate_path(...), np.arange(...), or a function).
        # One such value must not make the known child-notebook path disappear.
        entries: List[Dict[Any, Any]] = [{}]
        found_static_entry = False
        for key_node, value_node in zip(node.keys, node.values):
            if key_node is None:  # **mapping; unsupported static form
                if completeness is not None:
                    if completeness.on_unsupported is not None:
                        evaluate(value_node, env, parameter_bindings)
                    completeness.obscure(entries)
                continue
            keys = evaluate(key_node, env, parameter_bindings)
            key_state = child_state()
            values = evaluate(value_node, env, parameter_bindings)
            value_state = child_state()
            if completeness is not None:
                if not key_state.complete:
                    completeness.obscure(entries)
                if not keys or not values:
                    for entry in entries:
                        fields = completeness.fields.setdefault(completeness.key(entry), {})
                        for key in keys:
                            try:
                                fields[key] = value_state.snapshot()
                            except TypeError:
                                completeness.obscure([entry])
            if not keys or not values:
                continue
            expanded: List[Dict[Any, Any]] = []
            next_fields = {}
            next_open = set()
            for entry in entries:
                for key, value in product([keys, values]):
                    try:
                        candidate = _StaticMapping(entry, getattr(entry, "entry_provenance", {}))
                        candidate[key] = value
                        provenance = _selector_candidates(value_node, env, parameter_bindings)
                        if provenance is not None:
                            candidate.entry_provenance[key] = provenance
                    except TypeError:
                        if completeness is not None:
                            completeness.covered = False
                        continue
                    if completeness is not None:
                        source = completeness.key(entry)
                        target = completeness.key(candidate)
                        fields = dict(completeness.fields.get(source, {}))
                        field_state = value_state.select(value)
                        field_state.covered &= key_state.complete
                        fields[key] = field_state
                        combined = _StaticEvaluation()
                        combined.fields = next_fields
                        combined.merge_fields(target, fields)
                        next_fields = combined.fields
                        if source in completeness.open:
                            next_open.add(target)
                    expanded.append(candidate)
                    if len(expanded) >= _MAX_STATIC_VALUES:
                        break
                if len(expanded) >= _MAX_STATIC_VALUES:
                    break
            if completeness is not None and len(entries) * len(keys) * len(values) > _MAX_STATIC_VALUES:
                completeness.covered = False
            if expanded:
                if completeness is not None:
                    completeness.fields.update(next_fields)
                    completeness.open.update(next_open)
                entries = dedup(expanded)
                found_static_entry = True
        if completeness is not None:
            completeness.handled = True
        if found_static_entry or not node.keys:
            return dedup(entries)
        return []
    if isinstance(node, ast.Subscript):
        containers = evaluate(node.value, env, parameter_bindings)
        container_state = child_state()
        keys = evaluate(node.slice, env, parameter_bindings)
        key_state = child_state()
        out, selected = [], []
        if completeness is not None:
            completeness.handled = True
            completeness.covered &= container_state.covered
            if keys:
                completeness.covered &= key_state.complete
        if not keys:
            for container in containers:
                if not isinstance(container, dict):
                    if completeness is not None:
                        completeness.covered = False
                    continue
                if completeness is not None:
                    completeness.covered &= container_state.select(container).complete
                for key, value in container.items():
                    if isinstance(value, (dict, list, tuple, set)) or _looks_like_notebook_path(value):
                        out.append(value)
                        if completeness is not None:
                            selected.append(container_state.field(container, key, value))
                    elif completeness is not None:
                        completeness.covered = False
        for container, key in product([containers, keys]):
            try:
                value = container[key]
                out.append(value)
                if completeness is not None:
                    selected.append(container_state.field(container, key, value))
            except (KeyError, IndexError, TypeError):
                if completeness is not None:
                    completeness.covered = False
        finish(selected)
        return dedup(out)
    if not isinstance(node, ast.Call):
        if (isinstance(node, ast.AST) and completeness is not None
                and completeness.on_unsupported is not None):
            # Unsupported expressions/compound statements cannot leave writes
            # behind a confident binding. No customer implementation is executed.
            for child in ast.iter_child_nodes(node):
                evaluate(child, env, parameter_bindings)
            completeness.on_unsupported(node)
        return []

    call_name = _dotted_name(node.func)
    if call_name in {
        "dbutils.widgets.get", "dbutils.widgets.getArgument",
        "oidlUtils.widgets.get", "aidputils.widgets.get",
    } and node.args:
        names = evaluate(node.args[0], env, parameter_bindings)
        out: List[Any] = []
        selected = []
        name_state = child_state()
        for name in names:
            if isinstance(name, str) and name in parameter_bindings:
                values = _parameter_values(parameter_bindings[name])
                out.extend(values)
                if completeness is not None:
                    selected.append(completeness.bindings.get(name, _StaticEvaluation.parameter(parameter_bindings[name])))
            elif completeness is not None:
                selected.append(_StaticEvaluation.known([]))
        if not out and len(node.args) > 1:
            out.extend(evaluate(node.args[1], env, parameter_bindings))
            selected = [child_state()]  # Python uses the fallback only here.
        if completeness is not None:
            completeness.covered &= name_state.complete
            finish(selected)
        return dedup(out)

    if (config_paths and isinstance(node.func, ast.Attribute) and node.func.attr == "startswith"
            and len(node.args) == 1 and not node.keywords):
        strings = evaluate(node.func.value, env, parameter_bindings)
        prefixes = evaluate(node.args[0], env, parameter_bindings)
        if completeness is not None and any(not isinstance(v, str) for v in strings + prefixes):
            completeness.covered = False
        return dedup([
            value.startswith(prefix) for value, prefix in product([strings, prefixes])
            if isinstance(value, str) and isinstance(prefix, str)
        ])

    if call_name in {"os.path.join", "posixpath.join"}:
        groups = [evaluate(arg, env, parameter_bindings) for arg in node.args]
        return dedup([posixpath.join(*(str(v) for v in combo))
                              for combo in product(groups)])

    if call_name == "str" and len(node.args) == 1:
        return [str(v) for v in evaluate(node.args[0], env, parameter_bindings)]

    if (
        isinstance(node.func, ast.Attribute)
        and node.func.attr in {"strip", "lstrip", "rstrip"}
        and not node.args
        and not node.keywords
    ):
        values = evaluate(node.func.value, env, parameter_bindings)
        if completeness is not None and any(not isinstance(value, str) for value in values):
            completeness.covered = False
        return dedup([
            getattr(value, node.func.attr)()
            for value in values if isinstance(value, str)
        ])

    if isinstance(node.func, ast.Attribute) and node.func.attr == "format":
        templates = evaluate(node.func.value, env, parameter_bindings)
        arg_groups = [evaluate(arg, env, parameter_bindings) for arg in node.args]
        keyword_groups = {
            kw.arg: evaluate(kw.value, env, parameter_bindings)
            for kw in node.keywords if kw.arg
        }
        positional = product(arg_groups) if arg_groups else [()]
        keyword_names = list(keyword_groups)
        keyword_combos = (product([keyword_groups[k] for k in keyword_names])
                          if keyword_names else [()])
        out = []
        for template in templates:
            if not isinstance(template, str):
                if completeness is not None:
                    completeness.covered = False
                continue
            for args in positional:
                for values in keyword_combos:
                    try:
                        out.append(template.format(*args, **dict(zip(keyword_names, values))))
                    except (IndexError, KeyError, ValueError):
                        if completeness is not None:
                            completeness.covered = False
        return dedup(out)
    if completeness is not None and completeness.on_unsupported is not None:
        exposed = []
        if isinstance(node.func, ast.Attribute):
            evaluate(node.func.value, env, parameter_bindings)
            exposed.append(child_state())
        elif not isinstance(node.func, ast.Name):
            evaluate(node.func, env, parameter_bindings)
            exposed.append(child_state())
        for argument in list(node.args) + [kw.value for kw in node.keywords]:
            evaluate(argument, env, parameter_bindings)
            exposed.append(child_state())
        completeness.on_unsupported(node, exposed)
    return []


def _looks_like_notebook_path(value: Any) -> bool:
    """Whether a static string is safe to offer as a notebook candidate."""
    if not isinstance(value, str):
        return False
    candidate = value.strip()
    if not candidate:
        return False
    if not candidate.startswith(("/", "./", "../")) and not re.fullmatch(
        r"[A-Za-z0-9_ .-]+(/[^/]+)+", candidate,
    ):
        return False
    if "//" in candidate or candidate.endswith("/"):
        return False
    if "{" in candidate or "}" in candidate:
        return False
    if candidate.startswith("/"):
        # Canonical DBX workspace paths may use any root, including /Jobs or
        # a root-level notebook. Normalize before excluding known data roots
        # so /Jobs/../mnt/data and /tmp/../Jobs/child are classified correctly.
        absolute = posixpath.normpath(candidate)
        if absolute == "/" or any(
            absolute == root or absolute.startswith(root + "/")
            for root in _NON_NOTEBOOK_ABSOLUTE_ROOTS
        ):
            return False
    return not any(candidate.lower().endswith(ext)
                   for ext in _NON_NOTEBOOK_PATH_EXTENSIONS)


def _has_unknown_subscript(node: ast.AST, env: Dict[str, List[Any]],
                           parameter_bindings: Dict[str, Any]) -> bool:
    """Whether dispatch directly or indirectly uses an unknown selector."""
    return _selector_candidates(node, env, parameter_bindings) is not None


def _selector_candidates(node: ast.AST, env: Dict[str, List[Any]],
                         parameter_bindings: Dict[str, Any]):
    """Return unknown-selector provenance, including rejected scalar values."""
    unknown = False
    dropped: List[Any] = []
    literals: List[str] = []
    pending = [node]
    while pending:
        candidate = pending.pop()
        if isinstance(candidate, ast.Name):
            values = env.get(candidate.id, [])
            literals.extend(getattr(values, "literal_candidates", []))
            if isinstance(values, _StaticCandidates):
                unknown = True
                dropped.extend(values.dropped_values)
        elif isinstance(candidate, ast.Dict):
            # Each retained field carries its own provenance in _StaticMapping.
            # A known path key must not inherit an unrelated runtime field.
            continue
        elif isinstance(candidate, ast.Subscript):
            keys = _static_values(candidate.slice, env, parameter_bindings)
            unknown = unknown or not keys
            for container in _static_values(candidate.value, env, parameter_bindings):
                if isinstance(container, dict):
                    selected_keys = keys or list(container)
                    for key in selected_keys:
                        try:
                            value = container[key]
                        except (KeyError, TypeError):
                            continue
                        provenance = getattr(container, "entry_provenance", {}).get(key)
                        if provenance is not None:
                            unknown = True
                            dropped.extend(provenance.dropped_values)
                            literals.extend(provenance.literal_candidates)
                        if not keys and not isinstance(value, (dict, list, tuple, set)) \
                                and not _looks_like_notebook_path(value):
                            dropped.append(value)
            pending.extend([candidate.value, candidate.slice])
        else:
            pending.extend(ast.iter_child_nodes(candidate))
    return _StaticCandidates((), dropped, literals) if unknown else None


def _static_environment(
    trees: List[ast.AST], parameter_bindings: Dict[str, Any],
) -> Dict[str, List[Any]]:
    """Build a bounded union of values assigned to simple local variables."""
    env: Dict[str, List[Any]] = {
        str(name): _parameter_values(value)
        for name, value in parameter_bindings.items()
    }
    literal_candidates = _dedup_values([
        node.value for tree in trees for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and _looks_like_notebook_path(node.value)
    ])
    # Multiple passes let values flow across cells and short assignment chains.
    for _ in range(8):
        changed = False
        for tree in trees:
            for node in ast.walk(tree):
                targets: List[ast.AST] = []
                value_node: Optional[ast.AST] = None
                if isinstance(node, ast.Assign):
                    targets = list(node.targets)
                    value_node = node.value
                elif isinstance(node, ast.AnnAssign):
                    targets = [node.target]
                    value_node = node.value
                elif isinstance(node, ast.NamedExpr):
                    targets = [node.target]
                    value_node = node.value
                if value_node is not None:
                    values = _static_values(value_node, env, parameter_bindings)
                    imported_literals = [
                        literal for dependency in ast.walk(value_node)
                        if isinstance(dependency, ast.Name)
                        for literal in getattr(env.get(dependency.id, []), "literal_candidates", [])
                    ]
                    values = _StaticValueSet(values, literal_candidates + imported_literals)
                    provenance = _selector_candidates(value_node, env, parameter_bindings)
                    if provenance is not None:
                        values = _StaticCandidates(values, provenance.dropped_values,
                                                   values.literal_candidates + provenance.literal_candidates)
                    for target in targets:
                        if not isinstance(target, ast.Name) or (not values and provenance is None):
                            continue
                        previous = env.get(target.id, [])
                        merged = _merge_static_values(previous, values)
                        if _static_value_signature(merged) != _static_value_signature(previous):
                            env[target.id] = merged
                            changed = True
                if isinstance(node, ast.For) and isinstance(node.target, ast.Name):
                    iterables = _static_values(node.iter, env, parameter_bindings)
                    items = []
                    for iterable in iterables:
                        if isinstance(iterable, (list, tuple, set)):
                            items.extend(iterable)
                    provenance = _selector_candidates(node.iter, env, parameter_bindings)
                    if provenance is not None:
                        items = _StaticCandidates(items, provenance.dropped_values,
                                                  literal_candidates + provenance.literal_candidates)
                    if items or provenance is not None:
                        previous = env.get(node.target.id, [])
                        merged = _merge_static_values(previous, items)
                        if _static_value_signature(merged) != _static_value_signature(previous):
                            env[node.target.id] = merged
                            changed = True
        if not changed:
            break
    return env


def extract_run_targets(
    nb_dict: Dict[str, Any],
    notebook_dbx_path: str,
    log=None,
    parameter_bindings: Optional[Dict[str, Any]] = None,
    loose_only: Optional[Set[str]] = None,
    explicit_targets: Optional[Set[str]] = None,
) -> List[str]:
    """Find %run, dbutils.notebook.run, oidlUtils.notebook.run targets.

    Works across notebook languages: %run and notebook.run literals are pulled
    with regexes (language-agnostic), and an extra AST pass adds precision for
    Python cells. Known parameter bindings feed a bounded AST evaluator for
    widget reads, f-strings and simple containers/path construction. When
    `log` is provided, expressions outside that allowlist are warned about
    instead of being silently dropped.

    ``loose_only``, when supplied, receives paths inferred only from broad
    literal scanning or an unresolved selector. A direct/resolved reference to
    the same path takes precedence.

    ``explicit_targets`` receives paths appearing literally in a %run or
    notebook.run call. It is intentionally narrower than ``confirmed``: a
    statically selected work-list candidate can still be a guess.
    """
    parent_dir = posixpath.dirname(notebook_dbx_path)
    parameter_bindings = dict(parameter_bindings or {})
    out: List[str] = []
    confirmed: Set[str] = set()
    loose: Set[str] = set()

    # Parse all Python cells first so values can flow across cell boundaries.
    trees: List[ast.AST] = []
    for cell in nb_dict.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        src = "".join(cell.get("source", [])) if isinstance(cell.get("source"), list) else (cell.get("source") or "")
        active = _active_source(src)
        try:
            tree = _parse_python_source(active)
        except SyntaxError:
            continue
        trees.append(tree)
    static_env = _static_environment(trees, parameter_bindings)

    # The loose literal scan is intentionally a last resort. Enabling it for
    # every notebook caused ordinary output/model/image/MLflow paths to be
    # classified as notebooks. Activate it only when this notebook contains a
    # real notebook.run call whose first argument remains unresolved after the
    # bounded evaluator above. The flag is notebook-wide because dispatch
    # tables and the call that consumes them commonly live in different cells.
    has_unresolved_dynamic_notebook_run = False
    for tree in trees:
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not node.args:
                continue
            f = node.func
            if not (isinstance(f, ast.Attribute) and f.attr == "run"):
                continue
            owner = f.value
            if not (isinstance(owner, ast.Attribute) and owner.attr == "notebook"):
                continue
            base = owner.value
            if not (isinstance(base, ast.Name) and base.id in ("dbutils", "oidlUtils", "aidputils")):
                continue
            resolved = [value for value in _static_values(
                node.args[0], static_env, parameter_bindings
            ) if isinstance(value, str) and value.strip()]
            if not resolved or _has_unknown_subscript(
                node.args[0], static_env, parameter_bindings
            ):
                has_unresolved_dynamic_notebook_run = True
                break
        if has_unresolved_dynamic_notebook_run:
            break

    # Syntax-invalid-for-Python cells may be Scala/R/SQL. For those languages
    # use the comment-filtered source and require a non-quoted first argument.
    if not has_unresolved_dynamic_notebook_run:
        for cell in nb_dict.get("cells", []):
            if cell.get("cell_type") != "code":
                continue
            src = "".join(cell.get("source", [])) if isinstance(cell.get("source"), list) else (cell.get("source") or "")
            active = _active_source(src)
            try:
                _parse_python_source(active)
            except SyntaxError:
                if any(m.group(1) not in ('"', "'") for m in _NOTEBOOK_RUN_CALL_RE.finditer(active)):
                    has_unresolved_dynamic_notebook_run = True
                    break

    for cell in nb_dict.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        src = "".join(cell.get("source", [])) if isinstance(cell.get("source"), list) else (cell.get("source") or "")
        if not src.strip():
            continue
        active = _active_source(src)

        for m in _percent_run_matches(src, nb_dict, cell):
            t = _resolve_run_target(m.group(1), parent_dir)
            if t:
                out.append(t)
                confirmed.add(t)
                if explicit_targets is not None:
                    explicit_targets.add(t)
            elif log is not None:
                log(
                    f"  [warn] {notebook_dbx_path}: malformed or empty %run "
                    "target left unresolved."
                )

        # dbutils/oidlUtils.notebook.run("literal", ...) — language-agnostic
        # regex. Catches Scala/R/SQL cells where the AST pass below bails.
        for m in _NOTEBOOK_RUN_RE.finditer(active):
            t = _resolve_run_target(m.group(2), parent_dir)
            if t:
                out.append(t)
                confirmed.add(t)
                if explicit_targets is not None:
                    explicit_targets.add(t)

        # dbutils.notebook.run + oidlUtils.notebook.run — AST pass (Python).
        # Resolves: (a) hard-coded string-literal first args, and (b) a first
        # arg that is a variable name assigned string literal(s) elsewhere
        # (constant propagation). Warns only when the arg is genuinely
        # unresolvable (f-string, function call, computed path, etc.).
        try:
            tree = _parse_python_source(active)
        except SyntaxError:
            # Non-Python cell: can't AST. Warn on a non-literal first arg via
            # regex so a possibly-missed dep is still surfaced.
            if log is not None:
                for m in _NOTEBOOK_RUN_CALL_RE.finditer(active):
                    if m.group(1) not in ('"', "'"):
                        log(f"  [warn] {notebook_dbx_path}: notebook.run() with a "
                            f"non-literal path argument — cannot resolve statically; "
                            f"this dependency may be missed.")
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            f = node.func
            if not (isinstance(f, ast.Attribute) and f.attr == "run"):
                continue
            v = f.value
            if not (isinstance(v, ast.Attribute) and v.attr == "notebook"):
                continue
            base = v.value
            if not (isinstance(base, ast.Name) and base.id in ("dbutils", "oidlUtils", "aidputils")):
                continue
            if not node.args:
                continue
            arg0 = node.args[0]
            resolved_values = [v for v in _static_values(
                arg0, static_env, parameter_bindings
            ) if isinstance(v, str) and v.strip()]
            provenance = _selector_candidates(arg0, static_env, parameter_bindings)
            if provenance is not None:
                dropped = _dedup_values(provenance.dropped_values + [
                    value for value in resolved_values if not _looks_like_notebook_path(value)
                ])
                resolved_values = [value for value in resolved_values
                                   if _looks_like_notebook_path(value)]
                if log is not None:
                    expression = ast.unparse(arg0) if hasattr(ast, "unparse") else "<expression>"
                    log(f"  [warn] {notebook_dbx_path}: notebook.run({expression}): "
                        f"selector not statically known; taking {len(resolved_values)} "
                        f"candidate path(s): {', '.join(resolved_values) or 'none'}; "
                        f"dropped non-path values: {dropped!r}; loose scan remains enabled")
                for candidate in provenance.literal_candidates:
                    target = _resolve_run_target(candidate, parent_dir)
                    if target:
                        out.append(target)
                        loose.add(target)
                        if log is not None:
                            log(f"  [info] {notebook_dbx_path}: loose-scan discovered "
                                f"imported path-like literal {candidate!r} -> {target} "
                                "(inferred notebook.run target; will be verified at export time)")
            if resolved_values:
                for value in resolved_values:
                    t = _resolve_run_target(value, parent_dir)
                    if t:
                        out.append(t)
                        if provenance is None:
                            confirmed.add(t)
                        else:
                            # An unknown selector offers possible values, not
                            # evidence that any one is an actual run target.
                            loose.add(t)
                if log is not None and provenance is None and not (
                    isinstance(arg0, ast.Constant) and isinstance(arg0.value, str)
                ):
                    expression = ast.unparse(arg0) if hasattr(ast, "unparse") else "<expression>"
                    log(f"  [info] {notebook_dbx_path}: notebook.run({expression}) — "
                        f"resolved via bounded parameter/data-flow evaluation to "
                        f"{len(resolved_values)} path(s): {', '.join(resolved_values)}")
            elif provenance is None:
                if log is not None:
                    log(f"  [warn] {notebook_dbx_path}: notebook.run() with a "
                        f"non-literal path argument — cannot resolve statically; "
                        f"this dependency may be missed.")

        # ─── BEGIN: loose path-literal scan (ExampleVendor pattern) ─────────────
        # Introduced 2026-07-20 for the pilot deployment migration.
        #
        # The strict passes above resolve string literals ONLY when they appear
        # DIRECTLY as notebook.run(...) arg or as `var = "path"` (with var later
        # passed to notebook.run). But real jobs build work-lists like:
        #     _work_items = [(nm, "./NB1", "./NB2") for nm in models] + ...
        #     def _run_child(a, rand, norm):
        #         path = rand if condition else norm
        #         dbutils.notebook.run(path, 0, {...})
        # The path is 3+ levels of indirection from the literal — full trace
        # requires control-flow analysis we don't have.
        #
        # Fallback: when this notebook has an unresolved dynamic dispatch,
        # walk string Constants in this cell's AST and pick
        # up those with a notebook-path shape (relative directory/name or an
        # absolute path outside known data roots). Downstream (dbx_export)
        # verifies candidates using workspace object metadata before export.
        #
        # Filter tightly to reduce noise:
        #   - starts with a path prefix or contains a bare relative directory
        #   - not a URL (contains "://")
        #   - not a known non-notebook file extension
        #   - not trailing "/" (directory)
        #
        # To disable: comment out this block (search matching END marker).
        for _node in ast.walk(tree) if has_unresolved_dynamic_notebook_run else ():
            if not (isinstance(_node, ast.Constant) and isinstance(_node.value, str)):
                continue
            _v = _node.value.strip()
            if not _looks_like_notebook_path(_v):
                continue
            _t = _resolve_run_target(_v, parent_dir)
            if _t:
                out.append(_t)
                loose.add(_t)
                if log is not None:
                    log(f"  [info] {notebook_dbx_path}: loose-scan discovered "
                        f"path-like literal '{_v}' → {_t} "
                        f"(inferred notebook.run target; will be verified at export time)")
        # ─── END: loose path-literal scan ─────────────────────────────────

    # Dedup, preserve order.
    seen = set()
    uniq = []
    for t in out:
        if t and t not in seen:
            seen.add(t)
            uniq.append(t)
    if loose_only is not None:
        confirmed_paths = {rewrite_embedded_workspace_ref(t) for t in confirmed}
        loose_only.update(
            t for t in loose
            if rewrite_embedded_workspace_ref(t) not in confirmed_paths
        )
    return uniq


def _extract_percent_run_targets(
    nb_dict: Dict[str, Any], notebook_dbx_path: str,
) -> List[str]:
    """Return direct %run children resolved from their caller."""
    parent_dir = posixpath.dirname(notebook_dbx_path)
    targets: List[str] = []
    for cell in nb_dict.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        source = cell.get("source", [])
        src = "".join(source) if isinstance(source, list) else (source or "")
        for match in _percent_run_matches(src, nb_dict, cell):
            target = _resolve_run_target(match.group(1), parent_dir)
            if target:
                targets.append(target)
    return _dedup_values(targets)


def portable_run_extractor_source() -> str:
    """Return the dependency extractor as self-contained Python source.

    ``scripts/build_dag.py`` executes discovery inside a remote AIDP kernel.
    Importing :mod:`dbx_export` from that kernel is unsafe because the operator's
    checkout path is not guaranteed to exist there. Embed the exact extractor
    implementation instead so local and remote discovery stay in lock-step.
    """
    import inspect

    prelude = "\n".join([
        "import ast, itertools, posixpath, re",
        "from typing import Any, Dict, List, Optional, Set",
        f"_MAX_STATIC_VALUES = {_MAX_STATIC_VALUES!r}",
        f"_COMMENT_PREFIXES = {_COMMENT_PREFIXES!r}",
        f"_NON_NOTEBOOK_PATH_EXTENSIONS = {_NON_NOTEBOOK_PATH_EXTENSIONS!r}",
        f"_NON_NOTEBOOK_ABSOLUTE_ROOTS = {_NON_NOTEBOOK_ABSOLUTE_ROOTS!r}",
        f"_RUN_RE = re.compile({_RUN_RE.pattern!r})",
        f"_NOTEBOOK_RUN_RE = re.compile({_NOTEBOOK_RUN_RE.pattern!r})",
        f"_NOTEBOOK_RUN_CALL_RE = re.compile({_NOTEBOOK_RUN_CALL_RE.pattern!r})",
    ])
    helpers = (
        _active_source,
        _python_parse_source,
        _parse_python_source,
        _percent_run_matches,
        _strip_quotes,
        _percent_run_path,
        _resolve_run_target,
        _static_value_signature,
        _dedup_values,
        _StaticValueSet,
        _StaticCandidates,
        _StaticMapping,
        _merge_static_values,
        _dotted_name,
        _bounded_product,
        _parameter_values,
        _merge_parameter_bindings,
        _looks_like_notebook_path,
        _StaticEvaluation,
        _static_values,
        _evaluate_static_values,
        _has_unknown_subscript,
        _selector_candidates,
        _static_environment,
        rewrite_embedded_workspace_ref,
        extract_run_targets,
        _extract_percent_run_targets,
    )
    return prelude + "\n\n" + "\n\n".join(
        inspect.getsource(helper) for helper in helpers
    )


# REF-249: REST launchers commonly assign JOB_ID before building the payload.
# Casing must not change the existing soft-candidate discovery contract.
_JOB_ID_KW_RE = re.compile(r"\bjob_id\s*=\s*(['\"]?)(\d+)\1", re.IGNORECASE)


def extract_child_jobs_with_confidence(nb_dict: Dict[str, Any]) -> List[tuple]:
    """Find referenced Databricks job IDs, tagged with match confidence.

    Returns ``[(job_id, hard)]`` in first-seen order. ``hard`` is True for an
    AST match on ``<x>.runNow(...)`` / ``<x>.run_now(...)``. Ids found only by
    the ``job_id=NNNN`` regex fallback are soft: the regex ignores which
    function the kwarg belongs to, so it also matches unrelated literals such
    as an HTTP payload's ``job_id="11"`` (constructed reproduction, see
    tests/test_soft_match_child_job_integration.py). A hard match for an id
    always wins over a soft one.

    Fail closed: if the notebook contains ANY ``runNow``/``run_now`` call whose
    job id is not a literal (``runNow(job_id=jid)``, ``runNow(**kwargs)``), a
    real child job may be referenced only through a variable, and the regex id
    that may feed it (``job_id = 99``) cannot be told apart from an unrelated
    literal. Every regex id in that notebook is then treated as hard, so an
    unfetchable one still fails the export (and trips ``--strict-deps``).
    """
    found: Dict[str, bool] = {}
    regex_ids: List[str] = []
    ambiguous_call = False

    def _record(job_id: str, hard: bool) -> None:
        found[job_id] = found.get(job_id, False) or hard

    for cell in nb_dict.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        src = "".join(cell.get("source", [])) if isinstance(cell.get("source"), list) else (cell.get("source") or "")
        if not src.strip():
            continue
        active = _active_source(src)

        # AST: dbutils.jobs.runNow(job_id=N) or runNow(N) — hard
        try:
            tree = _parse_python_source(active)
        except SyntaxError:
            tree = None
        if tree is not None:
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                f = node.func
                if not (isinstance(f, ast.Attribute) and f.attr in ("runNow", "run_now")):
                    continue
                # Look for job_id kwarg or first positional int
                resolved = False
                for kw in node.keywords:
                    if kw.arg in ("job_id", "jobId") and isinstance(kw.value, ast.Constant):
                        _record(str(kw.value.value), True)
                        resolved = True
                if node.args and isinstance(node.args[0], ast.Constant):
                    v = node.args[0].value
                    if isinstance(v, (int, str)) and str(v).isdigit():
                        _record(str(v), True)
                        resolved = True
                if not resolved:
                    ambiguous_call = True

        # Regex: bare job_id=NNNN anywhere in code — soft (see docstring)
        for match in _JOB_ID_KW_RE.finditer(active):
            job_id = match.group(2)
            regex_ids.append(job_id)
            _record(job_id, False)

    if ambiguous_call:
        # Upgrade in place after all cells: retain discovery order even when
        # the ambiguous call appears after an earlier regex candidate.
        for job_id in regex_ids:
            _record(job_id, True)

    return list(found.items())


def extract_child_jobs(nb_dict: Dict[str, Any]) -> List[str]:
    """Flat list of referenced job IDs (hard and soft). See
    ``extract_child_jobs_with_confidence`` for the confidence tag."""
    return [job_id for job_id, _hard in extract_child_jobs_with_confidence(nb_dict)]


def extract_job_task_refs(job_def: Dict[str, Any]) -> List[str]:
    """Find child job_ids referenced as JOB_TASK in the workflow itself."""
    settings = job_def.get("settings") or job_def
    out = []
    for t in settings.get("tasks", []):
        rj = t.get("run_job_task") or t.get("runJobTask")
        if rj and rj.get("job_id"):
            out.append(str(rj["job_id"]))
    seen = set()
    return [x for x in out if not (x in seen or seen.add(x))]


# ─── Library/init-script extraction ──────────────────────────────────


def _classify_library_entry(lib: Dict[str, Any]):
    """Return (kind, source_uri) or (None, None) for ignored entry (pypi/maven).

    kind ∈ {"jar","whl","egg","requirements","cran"}.
    """
    if "jar" in lib:
        return "jar", lib["jar"]
    if "whl" in lib:
        return "whl", lib["whl"]
    if "egg" in lib:
        return "whl", lib["egg"]  # treat egg like whl bucket
    if "requirements" in lib:
        return "requirements", lib["requirements"]
    if "pypi" in lib or "maven" in lib or "cran" in lib:
        return None, None  # remote — record only, no download
    return None, None


def extract_cluster_artifacts(cluster_spec: Dict[str, Any]) -> List[Dict[str, str]]:
    """Pull file-backed libraries and init scripts from a cluster spec."""
    out = []
    for library in cluster_spec.get("libraries", []) or []:
        kind, uri = _classify_library_entry(library)
        if kind and uri:
            out.append({"kind": kind, "uri": uri})
    for item in cluster_spec.get("init_scripts", []) or []:
        for k in ("workspace", "dbfs", "s3", "abfss", "gcs", "file"):
            ref = item.get(k)
            if ref and isinstance(ref, dict) and ref.get("destination"):
                out.append({"kind": "init_script", "uri": ref["destination"]})
    return out


# ─── BFS driver ──────────────────────────────────────────────────────


class DependencyResolver:
    def __init__(self, dbx: DbxClient, max_depth: int = MAX_DEPTH_DEFAULT, log=print,
                 parameter_bindings: Optional[Dict[str, Any]] = None):
        self.dbx = dbx
        self.max_depth = max_depth
        self.log = log
        self.parameter_bindings = dict(parameter_bindings or {})

    def resolve(self, root_job_id: str) -> Graph:
        g = Graph()
        # Job BFS
        queue: List[tuple] = [(root_job_id, 0, "<root>")]
        while queue:
            job_id, depth, ref = queue.pop(0)
            if job_id in g.jobs:
                g.jobs[job_id].referenced_by.append(ref)
                continue
            if depth > self.max_depth:
                self.log(f"  [max-depth] skipping job {job_id} (depth {depth})")
                continue
            node = JobNode(dbx_job_id=job_id, depth=depth, referenced_by=[ref])
            g.jobs[job_id] = node
            try:
                node.job_def = self.dbx.get_job(job_id)
            except DbxExportError as e:
                node.fetch_error = f"{e.reason}: {e.detail}"
                node.fetch_error_is_not_found = e.is_job_not_found
                self.log(f"  [job-fetch-fail] {job_id}: {node.fetch_error}")
                continue

            # Enqueue notebook tasks + JOB_TASK refs + libraries + init scripts
            settings = node.job_def.get("settings") or node.job_def
            self._enqueue_task_notebooks(g, job_id, settings)

            self._enqueue_job_artifacts(g, job_id, settings)

            # Child jobs via JOB_TASK
            for child_id in extract_job_task_refs(node.job_def):
                queue.append((child_id, depth + 1, f"job:{job_id}"))

        # Notebook BFS — process queued notebooks, scanning each for sub-refs
        nb_queue = list(g.notebooks.keys())
        scanned_required_notebooks: Set[str] = set()
        while nb_queue or self._enqueue_percent_run_namespace_targets(g, nb_queue):
            p = nb_queue.pop(0)
            node = g.notebooks[p]
            if node.export_error is not None or p in scanned_required_notebooks:
                continue  # already scanned as required (or already failed)
            if node.nb_dict is not None and node.role == "py_sibling":
                continue  # copied for imports, not a notebook execution edge

            # A loose dynamic-dispatch fallback can only infer path-shaped
            # candidates. When Databricks identifies a run dependency as a
            # concrete non-notebook object, classify it as an external asset
            # instead of manufacturing a failed notebook dependency.
            if node.role == "run_dep" and not (node.git_owner and node.git_repo):
                get_status = getattr(self.dbx, "get_workspace_status", None)
                status = get_status(p) if callable(get_status) else None
                object_type = str((status or {}).get("object_type", "")).upper()
                if object_type in _NON_NOTEBOOK_WORKSPACE_OBJECT_TYPES:
                    ref = f"workspace_asset:{object_type}:{p}"
                    if ref not in g.external_refs:
                        g.external_refs.append(ref)
                    self.log(
                        f"  [asset-skip] {p}: Databricks object_type={object_type}; "
                        "not a runnable notebook"
                    )
                    g.skipped_assets.add(p)
                    del g.notebooks[p]
                    continue
            try:
                if node.content is not None:
                    # A copied sibling can later become a run target or task.
                    # Reuse downloaded bytes rather than fetching it again.
                    exp = None
                elif node.role == "resource":
                    if node.git_owner and node.git_repo:
                        exp = self.dbx.export_resource_from_git(
                            p.lstrip("/"), node.git_owner, node.git_repo,
                            node.git_branch or "main",
                        )
                    else:
                        exp = self.dbx.export_workspace_resource(p)
                elif node.git_owner and node.git_repo:
                    repo_path = p.lstrip("/")
                    exp = self.dbx.export_notebook_from_git(
                        repo_path, node.git_owner, node.git_repo, node.git_branch,
                    )
                else:
                    exp = self.dbx.export_notebook(p)
                if exp is not None:
                    node.content = exp.content
                    node.is_notebook = exp.is_notebook
                    node.final_extension = exp.extension
                    node.export_method = exp.export_method
                    node.resolved_git_path = exp.resolved_git_path
                    if exp.export_method in {
                        "workspace_jupyter_direct",
                        "workspace_jupyter_direct_source",
                        "workspace_source_direct",
                        "workspace_files_raw",
                    }:
                        self.log(f"  [export-fallback] {p}: {exp.export_method}")
                    if exp.is_notebook:
                        from .validator import parse_notebook_bytes
                        node.nb_dict = parse_notebook_bytes(exp.content)
                    else:
                        self.log(f"  [utility] {p}: plain {exp.extension} module — will export as-is")
            except DbxExportError as e:
                if node.role == "run_dep" and node.loose_only and e.status == 404:
                    finding = {"path": p, "referenced_by": ", ".join(node.referenced_by),
                               "reason": e.reason}
                    g.skipped_loose_candidates.append(finding)
                    g.skipped_loose_paths.add(p)
                    self.log(
                        f"  [WARN][loose-candidate-skip] {p}: {e.reason}; "
                        f"inferred from {finding['referenced_by']}; lookup returned 404, "
                        "not a confirmed notebook dependency"
                    )
                    del g.notebooks[p]
                    continue
                node.export_error = e.reason
                node.error_detail = e.detail
                self.log(f"  [nb-export-fail] {p}: {e.reason}")
                continue
            except ValueError as e:
                node.export_error = "malformed_notebook"
                node.error_detail = str(e)
                self.log(f"  [nb-malformed] {p}: {e}")
                continue

            # Plain utilities have no notebook execution edges. A .py sibling
            # with a Databricks notebook header is also incidental until a
            # required notebook actually references it.
            if not node.is_notebook:
                if node.role == "run_dep":
                    if node.explicit_run_ref:
                        node.export_error = "not_a_notebook"
                        node.error_detail = "Explicit notebook reference resolved to a plain file"
                        self.log(f"  [nb-export-fail] {p}: {node.error_detail}")
                    else:
                        # A path guessed during dynamic dispatch is a real
                        # Python file, not a failed notebook dependency. Keep
                        # its bytes for upload and surface the classification.
                        node.role = "py_sibling"
                        node.inferred_plain_file = True
                        ref = f"inferred_plain_file:{p}"
                        if ref not in g.external_refs:
                            g.external_refs.append(ref)
                        self.log(f"  [asset-skip] {p}: inferred notebook target is a plain file")
                for module_resource in self._discover_module_resources(g, node, p):
                    nb_queue.append(module_resource)
                continue
            if node.role == "py_sibling":
                self.log(f"  [sibling] {p}: copied without notebook traversal")
                continue
            scanned_required_notebooks.add(p)

            # ── Fix 7i (2026-07-25): discover sibling .py modules ────────
            # A notebook can reference a Python module that lives NEXT TO IT
            # on disk (star-import: `from HelperFunctions.constants import *`
            # or a fully-qualified `import` where the top segment is a
            # workspace-folder name). Those .py files are NOT reachable via
            # %run scanning — %run is for other NOTEBOOKS, not Python modules.
            # Without this discovery step, dbx_export silently drops them,
            # and at migration time the star-import fails on the AIDP cluster
            # (module not on sys.path) — producing NameErrors on any symbol
            # the module was supposed to provide. Root cause of the pilot deployment
            # AutoRefit APP_SOURCE_NAME cascade 2026-07-25.
            #
            # Strategy: for each notebook, list its parent DBX folder and
            # enqueue any FILE-type .py sibling as a new "py_sibling" node.
            # It gets exported through the same utility path as
            # workspace-referenced .py files (is_notebook=False), uploaded
            # to AIDP as-is by phase2_upload_notebooks, and lands next to
            # the notebook on AIDP so Python's implicit-import (workspace
            # folder on sys.path) finds it.
            new_siblings = self._discover_sibling_py_modules(g, node, p)
            for _sp in new_siblings:
                nb_queue.append(_sp)

            new_resources = self._discover_local_resources(g, node, p)
            for resource_path in new_resources:
                nb_queue.append(resource_path)

            direct_resources = self._discover_workspace_resources(g, node, p)
            for resource_path in direct_resources:
                nb_queue.append(resource_path)

            loose_only: Set[str] = set()
            explicit_targets: Set[str] = set()
            targets = extract_run_targets(
                node.nb_dict, p, log=self.log,
                parameter_bindings=node.parameter_bindings,
                loose_only=loose_only,
                explicit_targets=explicit_targets,
            )
            loose_only = {rewrite_embedded_workspace_ref(t) for t in loose_only}
            explicit_targets = {rewrite_embedded_workspace_ref(t) for t in explicit_targets}
            for target in targets:
                # %run children of a git-backed notebook are typically also
                # in the same repo — inherit the parent's git context. BUT
                # if the target is an absolute Databricks workspace path
                # (/Users/, /Repos/, /Shared/, /dbfs/, /mnt/), it's a
                # workspace reference — fetch it via workspace/export, NOT
                # via the GitHub proxy. Otherwise it'd be looked up in the
                # repo and 404, then surface as `git_path_unresolved`.
                # A %run target can arrive with an embedded workspace path:
                #   - a repo-relative path joined onto a git parent's dir,
                #     e.g. "Repo/HelperFunctions/Users/x@y.com/nb" (would 404
                #     on GitHub), or
                #   - a relative path joined onto a workspace parent's dir,
                #     producing a DOUBLED absolute path, e.g.
                #     "/Users/a@co/Users/b@co/nb" (would 404 on workspace/export).
                # Both embed the true absolute path after the LAST
                # /Users//Repos//Shared/ marker. Collapse unconditionally — the
                # rewrite is idempotent for clean single-marker paths — so the
                # doubled-absolute case (which already startswith /Users/) is
                # fixed too, not just the git-relative case.
                rewritten = rewrite_embedded_workspace_ref(target)
                if rewritten != target:
                    self.log(
                        f"  [rewrite] %run target had embedded/doubled "
                        f"workspace path: {target} → {rewritten}"
                    )
                    target = rewritten
                is_workspace_ref = target.startswith(
                    ("/Users/", "/Repos/", "/Shared/", "/dbfs/", "/mnt/")
                )
                if is_workspace_ref:
                    child_git_owner = None
                    child_git_repo = None
                    child_git_branch = None
                else:
                    child_git_owner = node.git_owner
                    child_git_repo = node.git_repo
                    child_git_branch = node.git_branch
                if self._skip_inferred_plain_target(g, target, p, explicit_targets):
                    continue
                if target not in g.notebooks:
                    if target in loose_only and target in g.skipped_loose_paths:
                        continue
                    self._enqueue_notebook(
                        g, target, role="run_dep", ref=p,
                        git_owner=child_git_owner,
                        git_repo=child_git_repo,
                        git_branch=child_git_branch,
                        parameter_bindings=node.parameter_bindings,
                        loose_only=target in loose_only,
                        explicit_run_ref=target in explicit_targets,
                    )
                    nb_queue.append(target)
                else:
                    was_incidental = g.notebooks[target].role == "py_sibling"
                    self._enqueue_notebook(
                        g, target, role="run_dep", ref=p,
                        git_owner=child_git_owner, git_repo=child_git_repo,
                        git_branch=child_git_branch,
                        parameter_bindings=node.parameter_bindings,
                        loose_only=target in loose_only,
                        explicit_run_ref=target in explicit_targets,
                    )
                    if was_incidental:
                        nb_queue.append(target)

            for child_job, hard in extract_child_jobs_with_confidence(node.nb_dict):
                if child_job in g.jobs:
                    # Already known: a later hard match upgrades an earlier soft one.
                    if hard:
                        g.jobs[child_job].soft_match = False
                else:
                    # Found from notebook source — depth based on the nearest job-ancestor.
                    kind = "" if hard else " (soft match: regex only, not a runNow call)"
                    self.log(f"  [discover] notebook {p} references job_id={child_job}{kind}")
                    before = {path: current.role for path, current in g.notebooks.items()}
                    self._enqueue_child_job(g, child_job, ref_notebook=p, soft=not hard)
                    # Queue newly discovered notebooks and sibling notebooks
                    # promoted to tasks; the latter may already have been
                    # downloaded and skipped by the BFS.
                    for path, current in g.notebooks.items():
                        if path not in before or (before[path] == "py_sibling" and current.role == "task"):
                            nb_queue.append(path)

        return g

    # ─── Enqueue helpers ─────────────────────────────────────────────

    def _discover_sibling_py_modules(
        self, g: Graph, notebook_node: NotebookNode, dbx_path: str,
    ) -> List[str]:
        """List the notebook's parent folder and enqueue any .py sibling files.

        Returns the list of newly-enqueued sibling paths so the caller can
        push them onto the BFS queue. Existing entries (already in
        g.notebooks) get a `referenced_by` update but aren't re-queued.

        Sibling files are enqueued as NotebookNode with role="py_sibling".
        Plain .py files are uploaded as files; Databricks SOURCE-format .py
        files are uploaded as notebooks. Neither kind contributes notebook
        execution edges until an explicit notebook reference promotes it.

        Failures listing the folder are non-fatal: a missing folder or a
        network hiccup just yields an empty list, and the notebook itself
        exports normally. Log so the operator can spot a pattern.

        Path resolution:
          - Git-backed nodes: parent folder in the GitHub repo. Use
            `list_github_dir` (already cached). Sibling DBX paths are
            constructed by replacing the notebook's basename with the .py
            filename — inheriting the notebook's parent path and git owner/
            repo/branch context.
          - Workspace-only nodes: parent folder in the workspace API. Use
            the new `list_workspace_folder` on DbxClient. Sibling DBX paths
            come directly from the API response.
        """
        # List the folder the notebook was fetched from; references below keep
        # the requested dbx_path.
        location = _fetched_location(notebook_node, dbx_path)
        if "/" not in location:
            return []
        parent_path = location.rsplit("/", 1)[0]
        # Root-level notebooks would list the whole workspace root — skip.
        # `""` from workspace-root split; `"/"` from a single-slash prefix.
        if not parent_path or parent_path == "/":
            return []

        # Cache scan-per-parent — some notebook folders host many notebooks,
        # and each would re-list the same folder without this guard.
        if not hasattr(self, "_sibling_parent_scanned"):
            self._sibling_parent_scanned = set()
        # Cache key includes git context so a git-backed and workspace-backed
        # notebook that happen to share a parent path (edge case) both scan.
        cache_key = (
            parent_path,
            notebook_node.git_owner,
            notebook_node.git_repo,
            notebook_node.git_branch,
        )
        if cache_key in self._sibling_parent_scanned:
            return []
        # Mark this parent scanned only AFTER a successful listing (done below,
        # after the if/else). Marking it here would cache a *transient* listing
        # failure — e.g. a proxy 429 that exhausted `proxy_max_retries` in
        # dbx_client — as "already scanned", so a later re-scan of the same
        # parent (another sibling notebook, or a re-run) would short-circuit to
        # [] and never retry, dropping siblings the retry could have recovered.
        # Deferring the add keeps a failed scan eligible for a later re-scan.
        # (Behaviour on a genuine listing failure is unchanged: still return []
        # — see the except branches — i.e. retry-then-fall-back-to-today.)

        py_sibling_paths: List[str] = []

        if notebook_node.git_owner and notebook_node.git_repo:
            # Git listing — `list_github_dir` returns {stem: filename}. Take
            # every filename whose full form ends in .py.
            try:
                entries = self.dbx.list_github_dir(
                    notebook_node.git_owner, notebook_node.git_repo,
                    parent_path, notebook_node.git_branch or "main",
                )
            except DbxExportError as e:
                # DELIBERATE, owner-approved deferral — do NOT "fix" to a graph
                # error without an owner sign-off (recurring review flag; see
                # PR #134 "## Review response"). The client (dbx_client
                # _github_get_with_retry) already retries transient 429/5xx up to
                # dbx_source.proxy_max_retries and raises loudly on exhaustion /
                # 401/403; here we catch and return [] because sibling discovery
                # is best-effort and non-fatal BY DESIGN (see this method's
                # docstring). Contract for this PR is "retry, then on exhaustion
                # fall back to today's behaviour" — and today (and on base) an
                # exhausted/failed listing yields []. cache-after-success (below)
                # keeps a *transient* failure retryable on a later re-scan.
                # Making a sustained-outage omission strict-gate-VISIBLE (a
                # Graph.discovery_errors channel surfaced under --strict-deps) is
                # a separate, pre-existing, UNFACED improvement (no observed run
                # has silently dropped a module this way) — tracked as a
                # follow-up, intentionally out of scope of the retry change.
                self.log(f"  [sibling-scan] git list failed for {parent_path}: {e}")
                return []
            for filename in set(entries.values()):
                if filename.endswith(".py"):
                    stem = filename[:-3]
                    # Sibling DBX path = parent + / + stem (no extension —
                    # matches how DBX addresses notebooks; export step will
                    # add the .py bytes verbatim).
                    py_sibling_paths.append(f"{parent_path}/{stem}")
        else:
            # Workspace listing — API returns {path, object_type} pairs.
            try:
                entries = self.dbx.list_workspace_folder(parent_path)
            except DbxExportError as e:
                self.log(f"  [sibling-scan] workspace list failed for {parent_path}: {e}")
                return []
            except Exception as e:
                # Defensive: any other client-side error (network, etc.) shouldn't
                # abort the whole export. Log and skip discovery for this folder.
                self.log(f"  [sibling-scan] workspace list error for {parent_path}: {e}")
                return []
            for entry in entries:
                obj_type = entry.get("object_type", "")
                sibling_path = entry.get("path", "")
                # DBX FILE type with a .py extension is a Python source
                # module. NOTEBOOK type on a .py path (rare) is a converted
                # SOURCE-format notebook and gets handled via the %run graph
                # already, so skip it here to avoid duplicates.
                if obj_type == "FILE" and sibling_path.endswith(".py"):
                    py_sibling_paths.append(sibling_path)

        # Listing succeeded (both failure branches above returned [] WITHOUT
        # reaching here, and without caching). Only now record this parent as
        # scanned, so other sibling notebooks sharing it skip the re-list while
        # a prior transient failure stays retryable.
        self._sibling_parent_scanned.add(cache_key)

        newly_enqueued: List[str] = []
        for sp in py_sibling_paths:
            if sp in g.notebooks:
                # Already known (referenced elsewhere). Just annotate the
                # ref so audit trails show the sibling relationship.
                g.notebooks[sp].referenced_by.append(f"sibling-of:{dbx_path}")
                continue
            node = NotebookNode(
                dbx_path=sp,
                role="py_sibling",
                referenced_by=[f"sibling-of:{dbx_path}"],
                git_owner=notebook_node.git_owner,
                git_repo=notebook_node.git_repo,
                git_branch=notebook_node.git_branch,
                # is_notebook defaults to True but will be overwritten by
                # export step to False for plain .py — that's fine.
            )
            g.notebooks[sp] = node
            newly_enqueued.append(sp)
            self.log(f"  [sibling] {sp} (sibling of {dbx_path.rsplit('/', 1)[-1]})")

        return newly_enqueued

    def _discover_workspace_resources(
        self,
        g: Graph,
        notebook_node: NotebookNode,
        dbx_path: str,
    ) -> List[str]:
        """Enqueue concrete workspace files read through task/job parameters."""
        newly_enqueued: List[str] = []
        for discovered_path in sorted(extract_workspace_resource_paths(
            notebook_node.nb_dict or {}, notebook_node.parameter_bindings,
        )):
            # /Workspace is an AIDP-style prefix; Databricks workspace APIs use
            # /Users, /Repos, and /Shared as their canonical roots.
            resource_path = (
                discovered_path[len("/Workspace"):]
                if discovered_path.startswith("/Workspace/")
                else discovered_path
            )
            ref = f"resource-of:{dbx_path}"
            if resource_path in g.notebooks:
                g.notebooks[resource_path].referenced_by.append(ref)
                continue
            g.notebooks[resource_path] = NotebookNode(
                dbx_path=resource_path,
                role="resource",
                referenced_by=[ref],
            )
            newly_enqueued.append(resource_path)
            self.log(
                f"  [resource-direct] {resource_path} "
                f"(parameter-derived file read by {posixpath.basename(dbx_path)})"
            )
        return newly_enqueued

    @staticmethod
    def _resource_search_ancestors(dbx_path: str) -> List[str]:
        """Return safe parent directories to probe, nearest first."""
        current = posixpath.dirname(dbx_path.rstrip("/"))
        ancestors: List[str] = []
        while current and current != "/":
            if current in _WORKSPACE_CONTAINER_ROOTS:
                break
            ancestors.append(current)
            parent = posixpath.dirname(current)
            if parent == current:
                break
            current = parent
        return ancestors

    def _record_resource_discovery_failure(self, node, directory, reason):
        # No fabricated file node: listing failed before filenames were known.
        failure = {"notebook": node.dbx_path, "directory": directory,
                   "reason": reason}
        if failure not in node.resource_discovery_failures:
            node.resource_discovery_failures.append(failure)

    def _list_resource_tree(
        self,
        notebook_node: NotebookNode,
        root_path: str,
        notebooks_out: Optional[List[str]] = None,
    ) -> List[str]:
        """Recursively list supported local resource files below ``root_path``.

        Returns the resource files (original contract). When ``notebooks_out``
        is provided, .ipynb entries are appended to it: notebook files in a
        discovered resource directory are dispatch targets, not raw resources
        — EXBRIDGE's ``generate_base`` runs ``notebook.run(f"../{target}")`` on a
        per-model notebook enumerated from the repo (live evidence 2026-10-13:
        0 of 30 model notebooks exported, every iteration would fail). They
        must export AS notebooks so the migrated tree resolves the dispatch.
        Without ``notebooks_out`` they are skipped, exactly as before.
        """
        files: List[str] = []
        notebooks: List[str] = notebooks_out if notebooks_out is not None else []
        pending = [root_path]
        seen: Set[str] = set()
        while pending and len(files) < _RESOURCE_SCAN_LIMIT:
            directory = pending.pop(0)
            if directory in seen:
                continue
            seen.add(directory)

            if notebook_node.git_owner and notebook_node.git_repo:
                try:
                    if hasattr(self.dbx, "list_github_dir_entries"):
                        typed_entries = self.dbx.list_github_dir_entries(
                            notebook_node.git_owner,
                            notebook_node.git_repo,
                            directory.lstrip("/"),
                            notebook_node.git_branch or "main",
                        )
                    else:
                        # Backward-compatible test/client fallback.
                        legacy = self.dbx.list_github_dir(
                            notebook_node.git_owner,
                            notebook_node.git_repo,
                            directory.lstrip("/"),
                            notebook_node.git_branch or "main",
                        )
                        typed_entries = [
                            {"name": name, "type": "file"}
                            for name in set(legacy.values())
                        ]
                except DbxExportError as exc:
                    self.log(f"  [resource-scan] git list failed for {directory}: {exc}")
                    self._record_resource_discovery_failure(
                        notebook_node, directory, "GIT_DIRECTORY_LIST_FAILED")
                    continue
                for entry in typed_entries:
                    filename = entry.get("name", "")
                    if not filename:
                        continue
                    child = posixpath.join(directory, filename)
                    if str(entry.get("type", "")).lower() == "dir":
                        pending.append(child)
                        continue
                    suffix = posixpath.splitext(filename)[1].lower()
                    if suffix == ".ipynb":
                        notebooks.append(child)
                    elif suffix in _LOCAL_RESOURCE_EXTENSIONS:
                        files.append(child)
            else:
                try:
                    entries = self.dbx.list_workspace_folder(directory)
                except Exception as exc:
                    self.log(f"  [resource-scan] workspace list failed for {directory}: {exc}")
                    self._record_resource_discovery_failure(
                        notebook_node, directory, "WORKSPACE_DIRECTORY_LIST_FAILED")
                    continue
                for entry in entries:
                    child = entry.get("path", "")
                    object_type = str(entry.get("object_type", "")).upper()
                    if not child:
                        continue
                    if object_type in ("DIRECTORY", "REPO"):
                        pending.append(child)
                    elif object_type == "NOTEBOOK":
                        notebooks.append(child)
                    elif object_type == "FILE":
                        suffix = posixpath.splitext(child)[1].lower()
                        if suffix == ".ipynb":
                            notebooks.append(child)
                        elif suffix in _LOCAL_RESOURCE_EXTENSIONS:
                            files.append(child)

        if pending:
            self._record_resource_discovery_failure(
                notebook_node, root_path, "RESOURCE_SCAN_LIMIT_REACHED")
            self.log(
                f"  [resource-scan] stopped at {_RESOURCE_SCAN_LIMIT} files below "
                f"{root_path}; narrow the imported resource directory"
            )
        return files

    def _discover_local_resources(
        self,
        g: Graph,
        notebook_node: NotebookNode,
        dbx_path: str,
    ) -> List[str]:
        """Find and enqueue local import/config directories for one notebook."""
        hints = extract_local_resource_hints(notebook_node.nb_dict or {})
        enumerated = extract_enumerated_resource_hints(notebook_node.nb_dict or {})
        directories, unresolved = extract_config_directory_paths(
            notebook_node.nb_dict or {}, notebook_node.parameter_bindings)
        if unresolved:
            self._record_resource_discovery_failure(
                notebook_node, "load_config(config_dir)", "CONFIG_DIRECTORY_UNRESOLVED")
            self.log(f"  [resource-config] {dbx_path}: load_config directory unresolved; "
                     "provide a concrete directory binding for export")
        exact_paths = set()
        is_git = bool(notebook_node.git_owner and notebook_node.git_repo)
        for directory in directories:
            if directory.startswith("/Workspace/"):
                directory = directory[len("/Workspace"):]
            if ("://" in directory or ":" in directory
                    or (directory.startswith("/") and (is_git or not directory.startswith(
                        ("/Users/", "/Repos/", "/Shared/"))))):
                self._record_resource_discovery_failure(
                    notebook_node, directory, "CONFIG_DIRECTORY_UNSUPPORTED")
                self.log(f"  [resource-config] {dbx_path}: unsupported config directory {directory!r}")
                continue
            path = posixpath.normpath(directory if directory.startswith("/") else posixpath.join(
                posixpath.dirname(_fetched_location(notebook_node, dbx_path)), directory))
            if (path in _WORKSPACE_CONTAINER_ROOTS or path in {"", ".", "/", ".."}
                    or path.startswith("../") or (not is_git and not path.startswith(
                        ("/Users/", "/Repos/", "/Shared/")))):
                self._record_resource_discovery_failure(
                    notebook_node, directory, "CONFIG_DIRECTORY_UNSUPPORTED")
                self.log(f"  [resource-config] {dbx_path}: refusing broad config directory {directory!r}")
                continue
            exact_paths.add(path)
        return self._enqueue_resource_directories(
            g, notebook_node, dbx_path, hints, enumerated, exact_paths=exact_paths)

    def _discover_module_resources(
        self,
        g: Graph,
        module_node: NotebookNode,
        module_path: str,
    ) -> List[str]:
        """Find and enqueue resource directories named inside an imported module.

        A notebook can read a file through a constant defined in a module it
        imports — ``pd.read_csv(ACCESS_CONTROL_CSV_RELATVE_PATH)`` with
        ``"database/access_control.csv"`` in ``imports/constants.py``
        (DataBricks_Access_Control, 2026-10: ``database/`` was never exported
        because only notebook cells were scanned). The module gets the same
        hint extraction as a notebook, anchored at each IMPORTING notebook:
        relative paths resolve against the notebook's working directory at
        runtime, not the module's folder. Module hints are import/constant
        derived, so they never promote notebooks to dependencies.
        """
        if module_node.content is None or module_node.final_extension.lower() != ".py":
            return []
        anchors = [
            reference.split(":", 1)[1]
            for reference in module_node.referenced_by
            if reference.startswith(("resource-of:", "sibling-of:"))
        ]
        anchors = [
            anchor for anchor in dict.fromkeys(anchors)
            if anchor in g.notebooks and anchor not in module_node.module_anchors_scanned
        ]
        if not anchors:
            return []

        source = ""
        failure = None
        try:
            source = module_node.content.decode("utf-8")
            _parse_python_source(_active_source(source))
        except UnicodeDecodeError:
            failure = "MODULE_DECODE_FAILED"
        except SyntaxError:
            failure = "MODULE_PARSE_FAILED"

        newly_enqueued: List[str] = []
        for anchor_path in anchors:
            module_node.module_anchors_scanned.add(anchor_path)
            anchor_node = g.notebooks[anchor_path]
            if failure:
                self.log(f"  [resource-module] {module_path}: {failure}; "
                         "resource folders it names were not scanned")
                self._record_resource_discovery_failure(anchor_node, module_path, failure)
                continue
            hints = extract_local_resource_hints(
                {"cells": [{"cell_type": "code", "source": source}]})
            newly_enqueued.extend(self._enqueue_resource_directories(
                g, anchor_node, anchor_path, hints, set(), via_module=module_path))
        return newly_enqueued

    def _enqueue_resource_directories(
        self,
        g: Graph,
        notebook_node: NotebookNode,
        dbx_path: str,
        hints: Set[str],
        enumerated: Set[str],
        via_module: Optional[str] = None,
        exact_paths: Optional[Set[str]] = None,
    ) -> List[str]:
        """Copy hinted directories or explicitly resolved config directories."""
        exact_paths = set(exact_paths or ()) - notebook_node.resource_paths_scanned
        hints = set(hints) - notebook_node.resource_hints_scanned
        if not hints and not exact_paths:
            return []
        notebook_node.resource_hints_scanned.update(hints)
        notebook_node.resource_paths_scanned.update(exact_paths)

        newly_enqueued: List[str] = []
        ancestors = self._resource_search_ancestors(_fetched_location(notebook_node, dbx_path))
        if notebook_node.git_owner and notebook_node.git_repo:
            ancestors.append("")  # repository root
        if exact_paths:
            ancestors.insert(0, None)  # explicit directories need no ancestor search
        for ancestor in ancestors:
            if ancestor is None:
                matches = sorted(exact_paths)
            elif notebook_node.git_owner and notebook_node.git_repo:
                try:
                    entries = self.dbx.list_github_dir(
                        notebook_node.git_owner,
                        notebook_node.git_repo,
                        ancestor.lstrip("/"),
                        notebook_node.git_branch or "main",
                    )
                except DbxExportError as e:
                    # Retain discovery uncertainty even when no file was found.
                    self.log(f"  [resource-scan] git list failed for {ancestor!r}: {e}")
                    self._record_resource_discovery_failure(
                        notebook_node, ancestor, "GIT_DIRECTORY_LIST_FAILED")
                    continue
                child_names = set(entries.values())
                matches = [hint for hint in hints if hint in child_names]
            else:
                try:
                    entries = self.dbx.list_workspace_folder(ancestor)
                except Exception as e:
                    self.log(f"  [resource-scan] workspace list failed for {ancestor!r}: {e}")
                    self._record_resource_discovery_failure(
                        notebook_node, ancestor, "WORKSPACE_DIRECTORY_LIST_FAILED")
                    continue
                directory_names = {
                    posixpath.basename(entry.get("path", "").rstrip("/"))
                    for entry in entries
                    if str(entry.get("object_type", "")).upper() in ("DIRECTORY", "REPO")
                }
                matches = [hint for hint in hints if hint in directory_names]

            for hint in matches:
                resource_root = hint if ancestor is None else posixpath.join(ancestor, hint)
                resource_notebooks: List[str] = []
                resource_files = self._list_resource_tree(
                    notebook_node, resource_root,
                    notebooks_out=resource_notebooks)
                if ancestor is None:
                    self.log(f"  [resource-config] {dbx_path} -> {resource_root}/ "
                             f"({len(resource_files)} supported file(s))")
                    if not resource_files and not any(
                        failure["directory"] == resource_root
                        for failure in notebook_node.resource_discovery_failures
                    ):
                        self._record_resource_discovery_failure(
                            notebook_node, resource_root, "CONFIG_DIRECTORY_EMPTY")
                # Dispatch-target notebooks ride the ordinary notebook export
                # (same GIT tree the runtime-relative notebook.run resolves
                # against), not the raw-resource upload. Only ENUMERATED hint
                # dirs qualify: an import-derived hint must not promote
                # unrelated notebooks to mandatory dependencies.
                for nb_path in (resource_notebooks if ancestor is not None and hint in enumerated else []):
                    enqueue_path = nb_path[:-len(".ipynb")] if nb_path.endswith(".ipynb") else nb_path
                    self._enqueue_notebook(
                        g, enqueue_path, role="run_dep",
                        ref=f"resource-notebook-of:{dbx_path}",
                        git_owner=notebook_node.git_owner,
                        git_repo=notebook_node.git_repo,
                        git_branch=notebook_node.git_branch,
                    )
                    newly_enqueued.append(enqueue_path)
                if via_module:
                    self.log(
                        f"  [resource-module] {via_module} -> {resource_root}/ "
                        f"({len(resource_files)} file(s)) for {posixpath.basename(dbx_path)}"
                    )
                for resource_path in resource_files:
                    ref = f"resource-of:{dbx_path}"
                    if resource_path in g.notebooks:
                        existing = g.notebooks[resource_path]
                        is_new_referrer = ref not in existing.referenced_by
                        existing.referenced_by.append(ref)
                        # A module already exported for another notebook must
                        # be rescanned from this notebook's folders as well.
                        if (is_new_referrer and existing.content is not None
                                and existing.final_extension.lower() == ".py"):
                            newly_enqueued.append(resource_path)
                        continue
                    g.notebooks[resource_path] = NotebookNode(
                        dbx_path=resource_path,
                        role="resource",
                        referenced_by=[ref],
                        git_owner=notebook_node.git_owner,
                        git_repo=notebook_node.git_repo,
                        git_branch=notebook_node.git_branch,
                    )
                    newly_enqueued.append(resource_path)
                    self.log(
                        f"  [resource] {resource_path} "
                        f"(import/config dependency of {posixpath.basename(dbx_path)})"
                    )
                # The closest matching directory wins. Do not accidentally
                # collect a same-named package from a broader workspace root.
                if ancestor is None:
                    exact_paths.discard(hint)
                else:
                    hints.discard(hint)
            if not hints and not exact_paths:
                break
        return newly_enqueued

    def _percent_run_namespace(
        self, g: Graph, node: NotebookNode, dbx_path: str,
        visiting: Optional[Set[str]] = None,
    ) -> Dict[str, Any]:
        """Return static variables supplied by direct %run children."""
        visiting = set(visiting or ())
        if dbx_path in visiting:
            return {}
        visiting.add(dbx_path)
        namespace: Dict[str, Any] = {}
        for child_path in _extract_percent_run_targets(node.nb_dict or {}, dbx_path):
            child_path = rewrite_embedded_workspace_ref(child_path)
            child = g.notebooks.get(child_path)
            if not child or not child.nb_dict:
                continue
            child_bindings = dict(child.parameter_bindings)
            _merge_parameter_bindings(
                child_bindings,
                self._percent_run_namespace(g, child, child_path, visiting),
            )
            child_trees: List[ast.AST] = []
            for cell in child.nb_dict.get("cells", []):
                if cell.get("cell_type") != "code":
                    continue
                source = cell.get("source", [])
                active = _active_source(
                    "".join(source) if isinstance(source, list) else (source or "")
                )
                try:
                    child_trees.append(_parse_python_source(active))
                except SyntaxError:
                    continue
            _merge_parameter_bindings(
                namespace, _static_environment(child_trees, child_bindings),
            )
        return namespace

    def _enqueue_percent_run_namespace_targets(
        self, g: Graph, nb_queue: List[str],
    ) -> bool:
        """Add dynamic notebook.run children resolved from shared %run config."""
        enqueued = False
        for dbx_path, node in list(g.notebooks.items()):
            if not node.nb_dict or not node.is_notebook or node.role == "py_sibling":
                continue
            # This second pass exists only to resolve values supplied by a
            # shared %run namespace.  Re-running ordinary dynamic-dispatch
            # discovery here would re-enqueue a non-notebook candidate after
            # the normal BFS had classified and removed it as an external
            # workspace asset.
            if not _extract_percent_run_targets(node.nb_dict, dbx_path):
                continue
            bindings = dict(node.parameter_bindings)
            _merge_parameter_bindings(
                bindings, self._percent_run_namespace(g, node, dbx_path),
            )
            loose_only: Set[str] = set()
            explicit_targets: Set[str] = set()
            targets = extract_run_targets(
                node.nb_dict, dbx_path, log=self.log, parameter_bindings=bindings,
                loose_only=loose_only,
                explicit_targets=explicit_targets,
            )
            loose_only = {rewrite_embedded_workspace_ref(t) for t in loose_only}
            explicit_targets = {rewrite_embedded_workspace_ref(t) for t in explicit_targets}
            for target in targets:
                target = rewrite_embedded_workspace_ref(target)
                if self._skip_inferred_plain_target(g, target, dbx_path, explicit_targets):
                    continue
                if target in g.notebooks:
                    existing = g.notebooks[target]
                    was_incidental = existing.role == "py_sibling"
                    if was_incidental:
                        self._enqueue_notebook(
                            g, target, role="run_dep", ref=dbx_path,
                            git_owner=node.git_owner, git_repo=node.git_repo,
                            git_branch=node.git_branch,
                            parameter_bindings=node.parameter_bindings,
                            loose_only=target in loose_only,
                            explicit_run_ref=target in explicit_targets,
                        )
                        nb_queue.append(target)
                        enqueued = True
                    else:
                        if target not in loose_only:
                            existing.loose_only = False
                        if target in explicit_targets:
                            existing.explicit_run_ref = True
                    continue
                if target in g.skipped_assets or (
                    target in loose_only and target in g.skipped_loose_paths
                ):
                    continue
                is_workspace_ref = target.startswith(
                    ("/Users/", "/Repos/", "/Shared/", "/dbfs/", "/mnt/")
                )
                self._enqueue_notebook(
                    g, target, role="run_dep", ref=dbx_path,
                    git_owner=None if is_workspace_ref else node.git_owner,
                    git_repo=None if is_workspace_ref else node.git_repo,
                    git_branch=None if is_workspace_ref else node.git_branch,
                    parameter_bindings=node.parameter_bindings,
                    loose_only=target in loose_only,
                    explicit_run_ref=target in explicit_targets,
                )
                nb_queue.append(target)
                self.log(
                    f"  [discover] {dbx_path}: notebook.run target {target} "
                    "discovered from %run namespace"
                )
                enqueued = True
        return enqueued

    def _skip_inferred_plain_target(
        self, g: Graph, target: str, ref: str, explicit_targets: Set[str],
    ) -> bool:
        """Retain provenance without requeuing a confirmed plain-file guess."""
        node = g.notebooks.get(target)
        if node is None or not node.inferred_plain_file or target in explicit_targets:
            return False
        if ref not in node.referenced_by:
            node.referenced_by.append(ref)
            self.log(f"  [asset-skip] {target}: already confirmed plain; inferred from {ref}")
        return True

    def _enqueue_notebook(
        self,
        g: Graph,
        dbx_path: str,
        role: str,
        ref: str,
        git_owner: Optional[str] = None,
        git_repo: Optional[str] = None,
        git_branch: Optional[str] = None,
        parameter_bindings: Optional[Dict[str, Any]] = None,
        loose_only: bool = False,
        explicit_run_ref: bool = False,
    ):
        if dbx_path in g.notebooks:
            existing = g.notebooks[dbx_path]
            existing.referenced_by.append(ref)
            if role == "task" or (role == "run_dep" and existing.role == "py_sibling"):
                existing.role = role
            if not loose_only:
                existing.loose_only = False
            if explicit_run_ref:
                existing.explicit_run_ref = True
            # Inherit git info if we now have it and didn't before.
            if git_owner and not existing.git_owner:
                existing.git_owner = git_owner
                existing.git_repo = git_repo
                existing.git_branch = git_branch
            _merge_parameter_bindings(existing.parameter_bindings, parameter_bindings)
            return
        if not loose_only and dbx_path in g.skipped_loose_paths:
            g.skipped_loose_paths.remove(dbx_path)
            g.skipped_loose_candidates = [
                candidate for candidate in g.skipped_loose_candidates
                if candidate["path"] != dbx_path
            ]
        g.notebooks[dbx_path] = NotebookNode(
            dbx_path=dbx_path, role=role, referenced_by=[ref],
            loose_only=loose_only,
            explicit_run_ref=explicit_run_ref,
            git_owner=git_owner, git_repo=git_repo, git_branch=git_branch,
            parameter_bindings=dict(parameter_bindings or {}),
        )

    def _enqueue_file(self, g: Graph, uri: str, kind: str, ref: str):
        if uri.startswith(("s3://", "http://", "https://")):
            g.external_refs.append(f"{kind}:{uri} (ref {ref})")
            return
        if uri in g.files:
            if ref not in g.files[uri].referenced_by:
                g.files[uri].referenced_by.append(ref)
            return
        g.files[uri] = FileNode(dbx_source=uri, kind=kind, referenced_by=[ref])

    def _enqueue_job_artifacts(
        self,
        g: Graph,
        job_id: str,
        settings: Dict[str, Any],
    ) -> None:
        """Attach every file-backed job artifact to its execution context.

        Databricks accepts file-backed dependencies both in ``tasks[].libraries``
        and inside task/job-cluster ``new_cluster`` specifications.  Root and
        notebook-discovered child jobs must use the same ownership traversal so
        fresh, reused, and override cluster paths see an identical contract.
        """
        for task in settings.get("tasks", []) or []:
            task_key = task.get("task_key", "<?>")
            task_ref = f"job:{job_id}/task:{task_key}"
            cluster_ref = task_cluster_context(job_id, task)
            artifacts: List[Dict[str, str]] = []
            for library in task.get("libraries", []) or []:
                kind, uri = _classify_library_entry(library)
                if kind and uri:
                    artifacts.append({"kind": kind, "uri": uri})
            if task.get("new_cluster"):
                artifacts.extend(extract_cluster_artifacts(task["new_cluster"]))

            references = [task_ref]
            if cluster_ref and cluster_ref != task_ref:
                references.append(cluster_ref)
            for artifact in artifacts:
                for reference in references:
                    self._enqueue_file(
                        g,
                        artifact["uri"],
                        artifact["kind"],
                        reference,
                    )

        for job_cluster in settings.get("job_clusters", []) or []:
            cluster_spec = job_cluster.get("new_cluster")
            if not cluster_spec:
                continue
            cluster_ref = (
                f"job:{job_id}/job_cluster:"
                f"{job_cluster.get('job_cluster_key', '?')}"
            )
            for artifact in extract_cluster_artifacts(cluster_spec):
                self._enqueue_file(
                    g,
                    artifact["uri"],
                    artifact["kind"],
                    cluster_ref,
                )

    def _enqueue_child_job(self, g: Graph, child_id: str, ref_notebook: str,
                           soft: bool = False):
        if child_id in g.jobs:
            g.jobs[child_id].referenced_by.append(ref_notebook)
            if not soft:
                g.jobs[child_id].soft_match = False
            return
        # Recurse into child by adding to outer queue would be nicer, but resolve()
        # already finished its job BFS. Fetch + analyze inline.
        depth = 1
        node = JobNode(dbx_job_id=child_id, depth=depth, referenced_by=[ref_notebook],
                       soft_match=soft)
        g.jobs[child_id] = node
        try:
            node.job_def = self.dbx.get_job(child_id)
        except DbxExportError as e:
            node.fetch_error = f"{e.reason}: {e.detail}"
            node.fetch_error_is_not_found = e.is_job_not_found
            return
        # Walk this job's notebooks/libs/init too — but DO NOT recurse further into
        # jobs-from-notebook for this child (would invert BFS into DFS).
        settings = node.job_def.get("settings") or node.job_def
        self._enqueue_task_notebooks(g, child_id, settings)
        self._enqueue_job_artifacts(g, child_id, settings)

    def _enqueue_task_notebooks(self, g: Graph, job_id: str, settings: Dict[str, Any]) -> None:
        """Enqueue every task notebook of one job definition.

        The SINGLE task-notebook enumeration for both discovery paths — the
        job BFS in resolve() and notebook-discovered child jobs in
        _enqueue_child_job(). Both previously carried their own flat copy of
        this loop, and both missed the task spec nested one level down in a
        ``for_each_task`` (EXBRIDGE dry-run 2026-10-13: the nested notebook was
        never discovered). The nested notebook is a task notebook in every
        sense — same GIT handling, same parameter bindings. This enumeration
        is PERMANENT: native AIDP for-each support will change the conversion
        (FOREACH_BRIDGE), not the need to see Databricks' nested structure.
        """
        # Job-level git_source — applies to every task whose source == "GIT"
        git_source = settings.get("git_source") or {}
        git_owner, git_repo = self.dbx.parse_github_owner_repo(git_source.get("git_url", ""))
        git_branch = (git_source.get("git_branch") or "main").strip()
        job_bindings = {
            str(item["name"]): item.get("default", item.get("value", ""))
            for item in settings.get("parameters", []) or []
            if isinstance(item, dict) and item.get("name") is not None
        }
        for t in settings.get("tasks", []):
            task_key = t.get("task_key", "<?>")
            ref_tag = f"job:{job_id}/task:{task_key}"
            nb_candidates = [(t.get("notebook_task"), ref_tag)]
            fe_nested = foreach_bridge.nested_notebook_task(t)
            if fe_nested is not None:
                nb_candidates.append(
                    (fe_nested.get("notebook_task"), ref_tag + "/for_each")
                )
            for nb_task, nb_ref in nb_candidates:
                if not (nb_task and nb_task.get("notebook_path")):
                    continue
                p = nb_task["notebook_path"].strip()
                task_bindings = dict(nb_task.get("base_parameters") or {})
                # Databricks job parameters are pushed down to notebook
                # tasks and override a same-named task parameter. Explicit
                # exporter bindings represent runtime overrides and win.
                task_bindings.update(job_bindings)
                task_bindings.update(self.parameter_bindings)
                # If the task declares source=GIT and the job has a valid
                # github git_source, fetch this notebook via the GitHub
                # proxy. Otherwise it's a workspace export (the default).
                is_git = nb_task.get("source") == "GIT" and git_owner and git_repo
                self._enqueue_notebook(
                    g, p, role="task", ref=nb_ref,
                    git_owner=git_owner if is_git else None,
                    git_repo=git_repo if is_git else None,
                    git_branch=git_branch if is_git else None,
                    parameter_bindings=task_bindings,
                )
