"""Conservative import audit for exported Python dependency graphs.

Only imports configured in the shared compatibility policy may create
requirements automatically.
Notebook ``%run`` dependencies are supplied by the resolver's reachable set;
this module additionally follows exported local ``.py`` imports recursively.
Everything else is classified as standard-library, local, AIDP-provided,
already explicit, or unknown. Unknown imports are reported, never installed.
"""

from __future__ import annotations

import ast
import os
import re
import sys
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import parse_qs, unquote, urlsplit

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import (
    InvalidWheelFilename,
    canonicalize_name,
    parse_wheel_filename,
)

from .dependency_resolver import (
    Graph,
    NotebookNode,
    _active_source,
    _parse_python_source,
)
from .requirements import CompatibilityPolicy, RequirementSource

_STRUCTURAL_PATH_PARTS = frozenset({
    "Workspace", "Users", "Repos", "Shared", "GIT", "notebooks",
})


@dataclass(frozen=True, order=True)
class ImportOccurrence:
    module: str
    dotted_module: str
    source_path: str
    statement: str
    dependency_chain: Tuple[str, ...]


@dataclass(frozen=True)
class ImportAuditResult:
    requirement_source: RequirementSource
    inferred: Tuple[Tuple[str, ImportOccurrence], ...] = ()
    unknown: Tuple[ImportOccurrence, ...] = ()
    managed_by_passthrough: Tuple[ImportOccurrence, ...] = ()
    alternate_provider: Tuple[ImportOccurrence, ...] = ()
    unparsed: Tuple[str, ...] = ()


@dataclass(frozen=True)
class _ExplicitRequirements:
    names: frozenset[str]
    passthrough_names: frozenset[str]
    unresolved_passthrough: bool


def _cell_source(cell: dict) -> str:
    source = cell.get("source", "")
    return "".join(source) if isinstance(source, list) else str(source or "")


def _imports_from_source(
    source: str,
    source_path: str,
    dependency_chain: Tuple[str, ...],
) -> Tuple[List[ImportOccurrence], Optional[str]]:
    """Parse imports with the dependency resolver's proven magic handling."""
    active = _active_source(source)
    if not active.strip():
        return [], None
    try:
        tree = _parse_python_source(active)
    except SyntaxError as exc:
        # Syntax validation remains owned by the notebook/module validator, but
        # the audit must never silently imply that it scanned an unparseable
        # cell. The caller emits one persistent WARN for this location.
        detail = f"{exc.msg} (line {exc.lineno or '?'})"
        return [], detail

    lines = active.splitlines()
    occurrences: List[ImportOccurrence] = []
    residuals: List[str] = []
    for node in ast.walk(tree):
        dotted_modules: List[str] = []
        if isinstance(node, ast.Import):
            dotted_modules = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                dotted_modules = [node.module]
            elif node.level:
                residuals.append(
                    f"relative import not traversed at line {node.lineno}"
                )
        if not dotted_modules:
            continue
        statement = (
            lines[node.lineno - 1].strip()
            if 0 < getattr(node, "lineno", 0) <= len(lines)
            else "import statement"
        )
        occurrences.extend(
            ImportOccurrence(
                dotted.split(".", 1)[0],
                dotted,
                source_path,
                statement,
                dependency_chain,
            )
            for dotted in dotted_modules
            if dotted
        )
    return occurrences, "; ".join(sorted(set(residuals))) or None


def _python_module_variants(path: str, node: NotebookNode) -> Set[str]:
    """Return dotted import suffixes represented by an exported ``.py`` node."""
    normalized = str(path).replace("\\", "/").rstrip("/")
    extension = os.path.splitext(normalized)[1].lower()
    if extension == ".py":
        normalized = normalized[:-3]
    elif extension:
        return set()
    elif str(getattr(node, "final_extension", "")).lower() != ".py":
        return set()

    parts = normalized.split("/")
    if parts and parts[-1] == "__init__":
        parts = parts[:-1]

    # Invalid identifiers (for example an email-address user folder) form a
    # natural boundary. Every valid suffix below that boundary is importable
    # when the corresponding project root is on sys.path.
    tail: List[str] = []
    for part in reversed(parts):
        if not part.isidentifier() or part in _STRUCTURAL_PATH_PARTS:
            if tail:
                break
            continue
        tail.append(part)
    ordered = list(reversed(tail))
    return {
        ".".join(ordered[index:])
        for index in range(len(ordered))
        if ordered[index:]
    }


def _local_python_index(
    graph: Graph,
    allowed_paths: Iterable[str],
) -> Dict[str, Tuple[str, ...]]:
    """Index only local modules reachable by the current cluster context."""
    allowed = set(allowed_paths)
    index: Dict[str, Set[str]] = {}
    for path, node in graph.notebooks.items():
        if path not in allowed:
            continue
        if node.is_notebook or node.content is None:
            continue
        for variant in _python_module_variants(path, node):
            index.setdefault(variant, set()).add(path)
    return {
        module: tuple(sorted(paths))
        for module, paths in index.items()
    }


def _passthrough_distribution_name(line: str) -> Optional[str]:
    """Best-effort distribution identity for pip forms outside PEP 508."""
    candidate = line.strip()
    if candidate.startswith("-e "):
        candidate = candidate[3:].strip()
    elif candidate.startswith("--editable "):
        candidate = candidate[len("--editable "):].strip()
    elif candidate.startswith("--editable="):
        candidate = candidate[len("--editable="):].strip()
    if len(candidate) >= 2 and candidate[0] in {'"', "'"}:
        if candidate[-1] == candidate[0]:
            candidate = candidate[1:-1]

    parsed = urlsplit(candidate)
    egg = parse_qs(parsed.fragment).get("egg")
    if egg and egg[0].strip():
        return canonicalize_name(egg[0].strip())

    path = unquote(parsed.path or candidate).replace("\\", "/")
    basename = PurePosixPath(path).name
    if basename.lower().endswith(".whl"):
        try:
            name, _, _, _ = parse_wheel_filename(basename)
        except InvalidWheelFilename:
            return None
        return canonicalize_name(name)

    if candidate.startswith((
        "git+", "hg+", "svn+", "bzr+",
    )):
        repository = basename.split("@", 1)[0]
        if repository.lower().endswith(".git"):
            repository = repository[:-4]
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", repository or ""):
            return canonicalize_name(repository)
    return None


def _explicit_distribution_names(
    sources: Sequence[RequirementSource],
) -> _ExplicitRequirements:
    names: Set[str] = set()
    passthrough_names: Set[str] = set()
    unresolved_passthrough = False
    for source in sources:
        unresolved_passthrough = (
            unresolved_passthrough
            or source.unresolved_passthrough
            or source.inference_barrier
        )
        names.update(canonicalize_name(name) for name in source.managed_names)
        for raw_line in source.content.splitlines():
            line = re.split(r"\s+#", raw_line, maxsplit=1)[0].strip()
            if not line or line.startswith("#"):
                continue
            try:
                names.add(canonicalize_name(Requirement(line).name))
            except InvalidRequirement:
                option = line.split(None, 1)[0].split("=", 1)[0].lower()
                if option in {
                    "--extra-index-url", "--find-links", "--index-url",
                    "-f", "-i", "--use-feature",
                    "--no-index", "--no-binary", "--only-binary", "--pre",
                    "--prefer-binary", "--require-hashes", "--trusted-host",
                }:
                    continue
                passthrough_name = _passthrough_distribution_name(line)
                if passthrough_name:
                    names.add(passthrough_name)
                    passthrough_names.add(passthrough_name)
                else:
                    unresolved_passthrough = True
    return _ExplicitRequirements(
        names=frozenset(names),
        passthrough_names=frozenset(passthrough_names),
        unresolved_passthrough=unresolved_passthrough,
    )


def _scan_occurrences(
    graph: Graph,
    notebook_paths: Iterable[str],
) -> Tuple[Set[ImportOccurrence], Set[str], Set[str]]:
    """Scan reachable notebooks and recursively scan imported local ``.py`` files."""
    reachable_paths = set(notebook_paths)
    local_index = _local_python_index(graph, reachable_paths)
    local_roots = {name.split(".", 1)[0] for name in local_index}
    occurrences: Set[ImportOccurrence] = set()
    unparsed: Set[str] = set()
    visited_sources: Set[str] = set()
    queue: List[Tuple[str, str, str, Tuple[str, ...], bool]] = []

    for path in sorted(reachable_paths):
        node = graph.notebooks.get(path)
        if not node or not node.is_notebook or node.nb_dict is None:
            continue
        for index, cell in enumerate(node.nb_dict.get("cells", [])):
            if cell.get("cell_type") == "code":
                queue.append((
                    path,
                    f"{path}:cell{index}",
                    _cell_source(cell),
                    (path,),
                    False,
                ))

    while queue:
        source_path, source_location, source, chain, is_local_file = queue.pop(0)
        if is_local_file:
            if source_path in visited_sources:
                continue
            visited_sources.add(source_path)

        found, parse_error = _imports_from_source(source, source_path, chain)
        if parse_error:
            unparsed.add(f"{source_location} | {parse_error}")
        occurrences.update(found)
        for occurrence in found:
            candidate_paths = local_index.get(occurrence.dotted_module, ())
            if not candidate_paths and occurrence.dotted_module == occurrence.module:
                # ``import package`` may refer to an exported package whose
                # concrete modules are indexed as ``package.child``. Package
                # __init__.py is scanned when present; otherwise classification
                # as local is sufficient and child imports provide the detail.
                candidate_paths = local_index.get(occurrence.module, ())
            for candidate_path in candidate_paths:
                if candidate_path in visited_sources:
                    continue
                node = graph.notebooks[candidate_path]
                queue.append((
                    candidate_path,
                    candidate_path,
                    node.content.decode("utf-8", errors="replace"),
                    chain + (candidate_path,),
                    True,
                ))

    return occurrences, local_roots, unparsed


def audit_notebook_imports(
    graph: Graph,
    notebook_paths: Iterable[str],
    explicit_sources: Sequence[RequirementSource],
    compatibility_policy: Optional[CompatibilityPolicy] = None,
) -> ImportAuditResult:
    """Return approved inferred requirements and unknown-import warnings."""
    explicit = _explicit_distribution_names(explicit_sources)
    occurrences, local_roots, unparsed = _scan_occurrences(
        graph, notebook_paths
    )
    policy = compatibility_policy
    import_map = dict(policy.import_to_distribution) if policy else {}
    distribution_aliases = (
        dict(policy.import_distribution_aliases) if policy else {}
    )
    stdlib_names = set(getattr(sys, "stdlib_module_names", ()))
    stdlib_names.update(policy.stdlib_imports if policy else ())
    stdlib_names.update({"__future__", "builtins"})
    provided_imports = set(policy.provided_imports if policy else ())
    if policy:
        provided_imports.update(
            name.replace("-", "_") for name in policy.runtime_provided
        )
    jvm_roots = set(policy.jvm_roots if policy else ())

    inferred_by_distribution: Dict[str, Tuple[str, ImportOccurrence]] = {}
    unknown_by_module: Dict[str, ImportOccurrence] = {}
    managed_by_passthrough: Dict[str, ImportOccurrence] = {}
    alternate_provider: Dict[str, ImportOccurrence] = {}
    for occurrence in sorted(occurrences):
        module = occurrence.module
        if (
            module in stdlib_names
            or module in local_roots
            or module in provided_imports
            or module in jvm_roots
        ):
            continue

        mapped_requirement = import_map.get(module)
        if mapped_requirement:
            mapped_spec = str(mapped_requirement)
            distribution = canonicalize_name(mapped_requirement.name)
            accepted_names = {distribution} | set(
                distribution_aliases.get(module, ())
            )
            explicit_matches = accepted_names & set(explicit.names)
            if explicit_matches:
                if explicit_matches & set(explicit.passthrough_names):
                    managed_by_passthrough.setdefault(module, occurrence)
            elif explicit.unresolved_passthrough:
                # An unidentifiable customer-selected artifact may provide this
                # namespace. Prefer an operator-visible conservative skip over
                # adding a second candidate to the same pip transaction.
                managed_by_passthrough.setdefault(module, occurrence)
            elif any(
                name.startswith(distribution + "-")
                or name.split("-", 1)[0] == distribution.split("-", 1)[0]
                for name in explicit.names
            ):
                # A validly parsed, but non-approved, provider from the same
                # distribution family is still a stronger signal than an
                # inferred public package (e.g. opencv-python-rolling for
                # ``import cv2``).  Do not install a second provider silently.
                alternate_provider.setdefault(module, occurrence)
            else:
                inferred_by_distribution.setdefault(
                    distribution, (mapped_spec, occurrence)
                )
            continue

        # A same-named explicit requirement proves that the import is already
        # managed, even though it is not approved for automatic inference.
        if canonicalize_name(module) in explicit.names:
            if canonicalize_name(module) in explicit.passthrough_names:
                managed_by_passthrough.setdefault(module, occurrence)
            continue
        unknown_by_module.setdefault(module, occurrence)

    inferred = tuple(
        value for _, value in sorted(inferred_by_distribution.items())
    )
    return ImportAuditResult(
        requirement_source=RequirementSource(
            "approved inferred Python imports",
            "\n".join(spec for spec, _ in inferred),
        ),
        inferred=inferred,
        unknown=tuple(unknown_by_module[key] for key in sorted(unknown_by_module)),
        managed_by_passthrough=tuple(
            managed_by_passthrough[key]
            for key in sorted(managed_by_passthrough)
        ),
        alternate_provider=tuple(
            alternate_provider[key] for key in sorted(alternate_provider)
        ),
        unparsed=tuple(sorted(unparsed)),
    )
