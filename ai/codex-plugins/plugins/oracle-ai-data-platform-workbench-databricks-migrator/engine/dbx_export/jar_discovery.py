"""Discover notebook JVM dependencies and resolve them from AIDP fallback JARs.

Databricks jobs frequently rely on JARs installed outside the job definition.
When the source cluster is unavailable, the only portable evidence is the JVM
package/class reference in the exported notebook.  This module scans every
task notebook and its transitive notebook dependencies, indexes the classes in
the configured AIDP fallback directory, and records deterministic matches.

Resolved fallback JARs are registered as ``FileNode`` objects so the existing
upload, cluster-library installation, restart, and verification paths can be
reused unchanged. Unresolved or ambiguous references are reported by default;
operators may opt into strict Phase-1 blocking.
"""

from __future__ import annotations

import hashlib
import io
import inspect
import json
import os
import re
import zipfile
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from typing import Any, BinaryIO, Dict, Iterable, Iterator, List, Optional, Set, Tuple, Union

from .auth import _get_aidputils_from_kernel
from .dependency_resolver import (
    FileNode,
    Graph,
    NotebookNode,
    task_cluster_context,
)


_PLATFORM_PREFIXES = (
    "java.",
    "javax.",
    "jdk.",
    "sun.",
    "scala.",
    "spark.",
    "org.apache.spark.",
    "org.apache.hadoop.",
    "org.slf4j.",
    "org.json4s.",
    "com.fasterxml.",
)

_LANGUAGE_ALIASES = {
    "python": "python",
    "python3": "python",
    "pyspark": "python",
    "scala": "scala",
}

_SCALA_IMPORT_RE = re.compile(
    r"(?m)^\s*import\s+((?:[A-Za-z_$][\w$]*\.)+[A-Za-z_$][\w$]*(?:\._|\.\*)?)"
)
_JVM_ATTRIBUTE_RE = re.compile(
    r"(?:\b_jvm|\bspark\._jvm|\bsc\._jvm)\."
    r"((?:[A-Za-z_$][\w$]*\.)+[A-Za-z_$][\w$]*)"
)
_CLASS_FOR_NAME_RE = re.compile(
    r"(?:Class\.forName|registerJavaFunction)\s*\([^\n]*?[\"']"
    r"((?:[A-Za-z_$][\w$]*\.)+[A-Za-z_$][\w$]*)[\"']"
)
_JAVA_IMPORT_RE = re.compile(
    r"\bjava_import\s*\([^,\n]+,\s*[\"']"
    r"((?:[A-Za-z_$][\w$]*\.)+[A-Za-z_$][\w$]*(?:\.\*)?)[\"']"
)
_FQCN_RE = re.compile(
    r"\b((?:com|org|io|net)\.(?:[A-Za-z_$][\w$]*\.)+"
    r"[A-Z][A-Za-z0-9_$]*)\b"
)

_GENERATED_JAR_PREFIX = "/dbx_export_generated/jar_fallback/"
MAX_FALLBACK_JARS = 512
MAX_JAR_BYTES = 256 * 1024 * 1024
MAX_TOTAL_JAR_BYTES = 2 * 1024 * 1024 * 1024


@dataclass(frozen=True)
class JvmReference:
    reference: str
    reference_type: str  # "class" | "package"
    notebook: str
    cell_index: int
    cluster_context: str


@dataclass
class JarResolution:
    reference: str
    reference_type: str
    notebook: str
    cell_index: int
    cluster_context: str
    status: str  # PRESENT | MATCHED | MISSING | AMBIGUOUS
    selected_jar: Optional[str] = None
    candidates: List[str] = field(default_factory=list)
    detail: Optional[str] = None


@dataclass
class JarDiscoveryResult:
    fallback_path: str
    allow_unresolved: bool = True
    resolutions: List[JarResolution] = field(default_factory=list)
    index_errors: List[str] = field(default_factory=list)
    index_fallbacks: List[str] = field(default_factory=list)

    @property
    def blockers(self) -> List[JarResolution]:
        return [
            item for item in self.resolutions
            if item.status in {"MISSING", "AMBIGUOUS"}
        ]

    @property
    def matched_jars(self) -> List[str]:
        return sorted({
            item.selected_jar for item in self.resolutions
            if item.status == "MATCHED" and item.selected_jar
        })

    def to_dict(self) -> Dict[str, Any]:
        return {
            "fallback_path": self.fallback_path,
            "allow_unresolved": self.allow_unresolved,
            "enforcement": "warn" if self.allow_unresolved else "block",
            "summary": {
                "references": len(self.resolutions),
                "present": sum(r.status == "PRESENT" for r in self.resolutions),
                "matched": sum(r.status == "MATCHED" for r in self.resolutions),
                "missing": sum(r.status == "MISSING" for r in self.resolutions),
                "ambiguous": sum(r.status == "AMBIGUOUS" for r in self.resolutions),
                "matched_jars": len(self.matched_jars),
                "index_errors": len(self.index_errors),
            },
            "resolutions": [asdict(item) for item in self.resolutions],
            "index_errors": list(self.index_errors),
            "index_fallbacks": list(self.index_fallbacks),
        }

    def write(self, report_dir: str, job_id: str) -> str:
        os.makedirs(report_dir, exist_ok=True)
        path = os.path.join(report_dir, f"dbx_job_{job_id}_jar_discovery.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(self.to_dict(), handle, indent=2, sort_keys=True)
        return path


def _active_source(source: str) -> str:
    """Remove whole-line comments before dependency scanning."""
    active: List[str] = []
    for line in source.splitlines():
        stripped = line.lstrip()
        if stripped.startswith(("#", "//", "--")):
            continue
        active.append(line)
    return "\n".join(active)


def _normalise_reference(reference: str) -> Tuple[str, str]:
    reference = reference.strip().rstrip(";")
    if reference.endswith(("._", ".*")):
        return reference[:-2], "package"
    tail = reference.rsplit(".", 1)[-1]
    return reference, "class" if tail[:1].isupper() else "package"


def _is_platform_reference(reference: str) -> bool:
    return reference.startswith(_PLATFORM_PREFIXES)


def _normalise_language(language: Optional[str]) -> Optional[str]:
    if not language:
        return None
    value = str(language).strip().lower()
    return _LANGUAGE_ALIASES.get(value, value)


def _source_language(source: str, default: Optional[str]) -> Optional[str]:
    """Return the effective language, honoring Databricks cell magics."""
    for line in source.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        magic = re.match(r"^%(python|scala|sql|r)\b", stripped, re.IGNORECASE)
        if magic:
            return _normalise_language(magic.group(1))
        break
    return _normalise_language(default)


def scan_source_for_jvm_references(
    source: str,
    language: Optional[str] = None,
) -> Set[Tuple[str, str]]:
    """Return ``(reference, type)`` pairs from one notebook cell."""
    source = _active_source(source)
    language = _source_language(source, language)
    found: Set[Tuple[str, str]] = set()

    # ``import x.y`` is valid in both Scala and Python.  Applying the Scala
    # regex to Python cells incorrectly turns imports such as
    # ``matplotlib.pyplot`` and ``pyspark.sql.functions`` into missing JARs.
    # Scan ordinary imports only for confirmed Scala cells. An ``import x.y``
    # statement is not evidence of a JVM dependency when notebook language is
    # Python or unknown; inferring from dotted names alone creates false JAR
    # failures for Python packages. Unknown-language cells still participate
    # through the explicit JVM bridge and FQCN evidence below.
    if language == "scala":
        import_matches = _SCALA_IMPORT_RE.finditer(source)
    else:
        import_matches = ()
    for match in import_matches:
        reference, reference_type = _normalise_reference(match.group(1))
        if not _is_platform_reference(reference):
            found.add((reference, reference_type))

    # Explicit JVM bridges and class-name literals remain valid evidence in
    # every notebook language, including Python.
    for regex in (_JVM_ATTRIBUTE_RE, _CLASS_FOR_NAME_RE, _JAVA_IMPORT_RE):
        for match in regex.finditer(source):
            reference, reference_type = _normalise_reference(match.group(1))
            if not _is_platform_reference(reference):
                found.add((reference, reference_type))
    if language == "scala":
        for match in _FQCN_RE.finditer(source):
            reference = match.group(1)
            if not _is_platform_reference(reference):
                found.add((reference, "class"))
    return found


def _notebook_language(node: NotebookNode) -> Optional[str]:
    notebook = node.nb_dict or {}
    metadata = notebook.get("metadata") or {}
    language_info = metadata.get("language_info") or {}
    kernelspec = metadata.get("kernelspec") or {}
    for candidate in (
        language_info.get("name") if isinstance(language_info, dict) else None,
        kernelspec.get("language") if isinstance(kernelspec, dict) else None,
        kernelspec.get("name") if isinstance(kernelspec, dict) else None,
        metadata.get("language"),
    ):
        language = _normalise_language(candidate)
        if language in {"python", "scala"}:
            return language
    extension = str(node.final_extension or "").lower()
    if extension == ".py":
        return "python"
    if extension == ".scala":
        return "scala"
    return None


def _notebook_sources(
    node: NotebookNode,
) -> Iterable[Tuple[int, str, Optional[str]]]:
    notebook_language = _notebook_language(node)
    if node.is_notebook and node.nb_dict is not None:
        for index, cell in enumerate(node.nb_dict.get("cells", [])):
            if cell.get("cell_type") != "code":
                continue
            source = cell.get("source", "")
            if isinstance(source, list):
                source = "".join(source)
            cell_metadata = cell.get("metadata") or {}
            cell_language = None
            if isinstance(cell_metadata, dict):
                cell_language = _normalise_language(
                    cell_metadata.get("language")
                    or cell_metadata.get("language_info")
                )
            yield index, str(source or ""), cell_language or notebook_language
        return
    if node.content:
        yield (
            0,
            node.content.decode("utf-8", errors="replace"),
            notebook_language,
        )


def _reachable_notebooks(graph: Graph, roots: Set[str]) -> Set[str]:
    reachable = set(roots)
    changed = True
    while changed:
        changed = False
        for path, node in graph.notebooks.items():
            if path in reachable:
                continue
            if any(parent in reachable for parent in node.referenced_by):
                reachable.add(path)
                changed = True
    return reachable


def collect_jvm_references(graph: Graph) -> List[JvmReference]:
    """Scan each cluster's task notebooks and transitive notebook deps."""
    contexts: Dict[str, Set[str]] = {}
    for job_id, job_node in graph.jobs.items():
        if not job_node.job_def:
            continue
        settings = job_node.job_def.get("settings") or job_node.job_def
        for task in settings.get("tasks", []) or []:
            context = task_cluster_context(job_id, task)
            notebook = (task.get("notebook_task") or {}).get("notebook_path")
            if context and notebook:
                contexts.setdefault(context, set()).add(notebook.strip())

    refs: Set[JvmReference] = set()
    for context, roots in contexts.items():
        for path in sorted(_reachable_notebooks(graph, roots)):
            node = graph.notebooks.get(path)
            if not node or node.export_error:
                continue
            for cell_index, source, language in _notebook_sources(node):
                for reference, reference_type in scan_source_for_jvm_references(
                    source, language=language
                ):
                    refs.add(JvmReference(
                        reference=reference,
                        reference_type=reference_type,
                        notebook=path,
                        cell_index=cell_index,
                        cluster_context=context,
                    ))
    return sorted(
        refs,
        key=lambda item: (
            item.cluster_context, item.notebook, item.cell_index, item.reference
        ),
    )


def _classes_from_jar(content: Union[bytes, BinaryIO]) -> Set[str]:
    classes: Set[str] = set()
    source = io.BytesIO(content) if isinstance(content, bytes) else content
    with zipfile.ZipFile(source) as archive:
        for name in archive.namelist():
            if not name.endswith(".class") or name.startswith("META-INF/"):
                continue
            class_name = name[:-6].replace("/", ".")
            # Scala emits Foo$.class and Foo$Nested.class.  Keep the concrete
            # name and its source-level outer-class spelling.
            classes.add(class_name)
            classes.add(class_name.split("$", 1)[0])
    return {name for name in classes if name}


def _fallback_jar_paths(aidp: Any, fallback_path: str) -> List[str]:
    items = aidp.list_workspace_objects(fallback_path)
    return sorted({
        str(item.get("path")) for item in items
        if str(item.get("path", "")).lower().endswith(".jar")
    })


def _download_jar_limited(aidp: Any, path: str) -> bytes:
    """Download one JAR with a production streaming limit and test-double fallback."""
    parameters = inspect.signature(aidp.download_file).parameters
    if "max_bytes" in parameters:
        content = aidp.download_file(path, max_bytes=MAX_JAR_BYTES)
    else:
        content = aidp.download_file(path)
    if len(content) > MAX_JAR_BYTES:
        raise ValueError(
            f"JAR exceeds {MAX_JAR_BYTES} byte per-file index limit: {path}"
        )
    return content


@contextmanager
def _open_repository_jar(
    aidp: Any, path: str, repository: str, fallbacks: List[str],
) -> Iterator[Tuple[BinaryIO, int]]:
    """Fall back to the same mounted repository object after a failed download.

    The 256 MiB cap bounds HTTP bodies held in memory. Mounted archives can
    exceed it because ZIP indexing seeks directly to their directory; the
    existing total-byte budget still bounds local indexing and checksum reads.
    Never substitute a laptop file for a remote workspace object.
    """
    try:
        content = _download_jar_limited(aidp, path)
    except Exception as download_error:
        try:
            if _get_aidputils_from_kernel() is None:
                raise OSError("mounted repository fallback requires an AIDP kernel")
            root = os.path.realpath(repository)
            local_path = os.path.realpath(path)
            if (not repository.startswith("/Workspace/")
                    or not path.startswith("/Workspace/")
                    or os.path.commonpath([root, local_path]) != root):
                raise OSError("JAR is outside the configured mounted repository")
            handle = open(local_path, "rb")
            try:
                size = os.fstat(handle.fileno()).st_size
                if size > MAX_TOTAL_JAR_BYTES:
                    raise ValueError(
                        f"mounted JAR exceeds {MAX_TOTAL_JAR_BYTES} byte index budget"
                    )
            except Exception:
                handle.close()
                raise
        except (OSError, ValueError) as local_error:
            raise OSError(
                f"download failed: {download_error}; "
                f"mounted repository fallback failed: {local_error}"
            ) from local_error
        note = (
            f"{path}: download failed ({download_error}); "
            "using mounted repository JAR"
        )
        if note not in fallbacks:
            fallbacks.append(note)
        with handle:
            yield handle, size
    else:
        with io.BytesIO(content) as handle:
            yield handle, len(content)


def _build_jar_index(
    aidp: Any,
    graph: Graph,
    fallback_path: str,
    index_fallbacks: List[str],
) -> Tuple[Dict[str, Set[str]], Set[str], List[str]]:
    index: Dict[str, Set[str]] = {}
    source_jars: Set[str] = set()
    staged_sources: Dict[str, List[str]] = {}
    errors: List[str] = []

    # Source-declared JARs take part in resolution too. If they contain the
    # class, the status is PRESENT and no fallback attachment is synthesised.
    for uri, node in graph.files.items():
        if node.kind == "jar" and node.content is not None and node.fetch_error is None:
            source_jars.add(uri)
            try:
                for class_name in _classes_from_jar(node.content):
                    index.setdefault(class_name, set()).add(uri)
            except (OSError, zipfile.BadZipFile) as exc:
                errors.append(f"cannot index {uri}: {exc}")
        elif node.kind == "jar" and node.aidp_repo_path and node.fetch_error is None:
            # AIDP-first deliberately skips the Databricks fetch, so index the
            # staged bytes under the original source URI. This keeps ownership
            # and installation attached to the declared FileNode. Only an exact
            # staged match is source-authoritative: a version substitution must
            # still compete with every provider found by the class index.
            staged_sources.setdefault(node.aidp_repo_path, []).append(uri)
            if _is_exact_aidp_first(node):
                source_jars.add(uri)

    try:
        fallback_jars = _fallback_jar_paths(aidp, fallback_path)
    except Exception as exc:
        fallback_jars = []
        errors.append(f"cannot list {fallback_path}: {exc}")

    fallback_jars = list(dict.fromkeys([*sorted(staged_sources), *fallback_jars]))

    if len(fallback_jars) > MAX_FALLBACK_JARS:
        errors.append(
            f"fallback JAR count {len(fallback_jars)} exceeds cap "
            f"{MAX_FALLBACK_JARS}; only the first capped set was indexed"
        )
        fallback_jars = fallback_jars[:MAX_FALLBACK_JARS]

    total_bytes = 0
    for path in fallback_jars:
        try:
            with _open_repository_jar(
                aidp, path, fallback_path, index_fallbacks,
            ) as (handle, size):
                total_bytes += size
                if total_bytes > MAX_TOTAL_JAR_BYTES:
                    errors.append(
                        f"fallback JAR bytes exceed cumulative cap "
                        f"{MAX_TOTAL_JAR_BYTES}; stopped before indexing {path}"
                    )
                    break
                classes = _classes_from_jar(handle)
        except Exception as exc:
            errors.append(f"cannot index {path}: {exc}")
            continue
        aliases = staged_sources.get(path)
        for class_name in classes:
            if aliases:
                index.setdefault(class_name, set()).update(aliases)
            else:
                index.setdefault(class_name, set()).add(path)
    return index, source_jars, errors


def _candidate_jars(
    reference: str,
    reference_type: str,
    index: Dict[str, Set[str]],
) -> List[str]:
    candidates: Set[str] = set()
    if reference_type == "class":
        candidates.update(index.get(reference, set()))
        for class_name, paths in index.items():
            if class_name.startswith(reference + "$"):
                candidates.update(paths)
    else:
        prefix = reference + "."
        for class_name, paths in index.items():
            if class_name.startswith(prefix):
                candidates.update(paths)
    return sorted(candidates)


def _select_candidate(candidates: List[str]) -> Tuple[Optional[str], Optional[str]]:
    if len(candidates) == 1:
        return candidates[0], None
    fixed = [
        path for path in candidates
        if "fixed" in os.path.basename(path).lower()
    ]
    if len(fixed) == 1:
        return fixed[0], "selected the sole reviewed '*fixed*' artifact"
    return None, None


def _is_exact_aidp_first(node: FileNode) -> bool:
    """True only for an exact source-declared AIDP repository match."""
    provenance = (node.aidp_repo_provenance or "").lower()
    return bool(
        node.aidp_repo_path
        and (
            "aidp-first: exact basename" in provenance
            or "aidp-first: exact version" in provenance
        )
    )


def _dedupe_source_aliases(graph: Graph, candidates: Iterable[str]) -> List[str]:
    """Collapse source URI aliases that resolve to the same staged artifact."""
    by_artifact: Dict[str, str] = {}
    for candidate in candidates:
        node = graph.files.get(candidate)
        artifact = node.aidp_repo_path if node and node.aidp_repo_path else candidate
        by_artifact.setdefault(artifact, candidate)
    return sorted(by_artifact.values())


def _select_source_candidate(
    graph: Graph,
    candidates: List[str],
) -> Tuple[Optional[str], Optional[str]]:
    """Prefer one exact staged declaration; preserve ambiguity otherwise."""
    candidates = _dedupe_source_aliases(graph, candidates)
    if len(candidates) <= 1:
        return _select_candidate(candidates)
    exact = [
        candidate for candidate in candidates
        if (graph.files.get(candidate) is not None
            and _is_exact_aidp_first(graph.files[candidate]))
    ]
    if len(exact) == 1:
        return exact[0], "preferred the exact AIDP-first declared artifact"
    return _select_candidate(candidates)


def _declared_contexts(
    graph: Graph,
    node: FileNode,
    task_to_cluster: Dict[str, str],
) -> Set[str]:
    """Expand task and init-script ownership to execution-cluster contexts."""
    contexts: Set[str] = set(node.referenced_by)
    for context in list(contexts):
        cluster = task_to_cluster.get(context)
        if cluster:
            contexts.add(cluster)
        if not context.startswith("init_script:"):
            continue
        script_uri = context[len("init_script:"):]
        script = graph.files.get(script_uri)
        if script is None:
            continue
        for script_context in script.referenced_by:
            contexts.add(script_context)
            cluster = task_to_cluster.get(script_context)
            if cluster:
                contexts.add(cluster)
    return contexts


def _propagate_init_script_library_contexts(
    graph: Graph,
    task_to_cluster: Dict[str, str],
) -> None:
    """Make init-script library ownership visible at execution-cluster boundaries.

    Init-script-discovered JARs/wheels/eggs are represented by an
    ``init_script:<uri>`` reference. They still need a concrete cluster context
    when the notebook contains no JVM reference: notebook-JAR discovery must
    not be the only place that expands ownership, because its reference scan
    can legitimately be empty. Keep the init-script marker and add the
    inherited task/cluster contexts so the existing upload/install paths see
    the artifact.
    """
    for node in graph.files.values():
        if node.kind not in ("jar", "whl", "egg") or not any(
            context.startswith("init_script:") for context in node.referenced_by
        ):
            continue
        for context in _declared_contexts(graph, node, task_to_cluster):
            if context not in node.referenced_by:
                node.referenced_by.append(context)


def _register_fallback_jar(
    graph: Graph,
    path: str,
    digest: str,
    context: str,
    reference: str,
) -> str:
    basename = os.path.basename(path)
    uri = f"{_GENERATED_JAR_PREFIX}{digest[:12]}_{basename}"
    existing = graph.files.get(uri)
    if existing:
        if context not in existing.referenced_by:
            existing.referenced_by.append(context)
        return uri
    graph.files[uri] = FileNode(
        dbx_source=uri,
        kind="jar",
        referenced_by=[context],
        fetch_error="not_declared_in_databricks",
        error_detail=f"resolved from notebook JVM reference {reference}",
        aidp_fallback_path=path,
        aidp_fallback_sha256=digest,
        aidp_fallback_provenance=(
            f"class-index match for notebook JVM reference {reference}"
        ),
    )
    return uri


def discover_notebook_jars(
    aidp: Any,
    graph: Graph,
    fallback_path: str,
) -> JarDiscoveryResult:
    """Resolve notebook JVM references and register matched fallback JARs."""
    task_to_cluster: Dict[str, str] = {}
    for job_id, job_node in graph.jobs.items():
        if not job_node.job_def:
            continue
        settings = job_node.job_def.get("settings") or job_node.job_def
        for task in settings.get("tasks", []) or []:
            cluster_context = task_cluster_context(job_id, task)
            task_key = task.get("task_key")
            if cluster_context and task_key:
                task_to_cluster[f"job:{job_id}/task:{task_key}"] = cluster_context

    # This ownership propagation is independent of JVM-reference matching.
    # A notebook may have no recognizable JVM reference while an init script
    # still stages a provider JAR that must be installed on its execution
    # cluster.
    _propagate_init_script_library_contexts(graph, task_to_cluster)

    references = collect_jvm_references(graph)
    result = JarDiscoveryResult(fallback_path=fallback_path)
    if not references:
        return result

    index, source_jars, index_errors = _build_jar_index(
        aidp, graph, fallback_path, result.index_fallbacks,
    )
    result.index_errors.extend(index_errors)

    declared_by_context: Dict[str, Dict[str, str]] = {}
    for uri, node in graph.files.items():
        if node.kind != "jar":
            continue
        if node.fetch_error is not None or not (
            node.content is not None or node.aidp_repo_path
        ):
            continue
        for effective_context in _declared_contexts(
            graph, node, task_to_cluster
        ):
            declared_by_context.setdefault(effective_context, {})[uri] = uri
    registered_fallbacks: Dict[str, str] = {}

    for item in references:
        candidates = _candidate_jars(item.reference, item.reference_type, index)
        declared_paths = declared_by_context.get(item.cluster_context, {})
        source_matches = [
            path for path in candidates
            if path in declared_paths and path in source_jars
        ]
        sibling_source = False
        if not source_matches:
            # A shared helper can use a provider JAR declared on a sibling
            # cluster, and init-script discovery can produce source JARs whose
            # original URI is not attached directly to a notebook task. Keep
            # declared-here preference, then preserve the pre-scoping source
            # fallback. Source URIs are never sent to AIDP downloadFileMeta.
            source_matches = [path for path in candidates if path in source_jars]
            sibling_source = bool(source_matches)
        if source_matches:
            source_matches = _dedupe_source_aliases(graph, source_matches)
            selected, detail = _select_source_candidate(graph, source_matches)
            displayed_matches = sorted({
                (graph.files[path].aidp_repo_path or path)
                if graph.files.get(path) is not None else path
                for path in source_matches
            })
            if selected is None:
                result.resolutions.append(JarResolution(
                    **asdict(item), status="AMBIGUOUS",
                    candidates=displayed_matches,
                    detail="multiple source-declared JARs contain this reference",
                ))
            else:
                source_uri = declared_paths.get(selected, selected)
                source_node = graph.files.get(source_uri)
                if source_node and item.cluster_context not in source_node.referenced_by:
                    source_node.referenced_by.append(item.cluster_context)
                if sibling_source:
                    detail = "indexed source JAR from another cluster"
                selected_artifact = (
                    source_node.aidp_repo_path
                    if source_node and source_node.aidp_repo_path
                    else selected
                )
                result.resolutions.append(JarResolution(
                    **asdict(item), status="PRESENT",
                    selected_jar=selected_artifact,
                    candidates=displayed_matches, detail=detail,
                ))
            continue

        # Non-exact staged declarations are fallback candidates, not
        # source-authoritative providers. Resolve their original DBX aliases to
        # the staged AIDP path so selection never tries to download a DBX URI
        # from AIDP and duplicate aliases collapse before ambiguity is judged.
        fallback_candidates = sorted({
            (
                graph.files[path].aidp_repo_path
                if graph.files.get(path) is not None
                and graph.files[path].aidp_repo_path
                else path
            )
            for path in candidates
            if path not in source_jars
        })
        selected, detail = _select_candidate(fallback_candidates)
        if selected:
            generated_uri = registered_fallbacks.get(selected)
            if generated_uri is None:
                try:
                    with _open_repository_jar(
                        aidp, selected, fallback_path, result.index_fallbacks,
                    ) as (handle, size):
                        digest = hashlib.sha256()
                        read_bytes = 0
                        while chunk := handle.read(1024 * 1024):
                            read_bytes += len(chunk)
                            if read_bytes > MAX_TOTAL_JAR_BYTES:
                                raise ValueError("JAR checksum read exceeds index budget")
                            digest.update(chunk)
                        if read_bytes != size:
                            raise OSError("JAR size changed during checksum read")
                except Exception as exc:
                    result.index_errors.append(
                        f"cannot read selected fallback {selected}: {exc}"
                    )
                    result.resolutions.append(JarResolution(
                        **asdict(item), status="MISSING", candidates=fallback_candidates,
                        detail=f"matched fallback became unreadable: {selected}",
                    ))
                    continue
                generated_uri = _register_fallback_jar(
                    graph, selected, digest.hexdigest(), item.cluster_context,
                    item.reference,
                )
                registered_fallbacks[selected] = generated_uri
            else:
                node = graph.files[generated_uri]
                if item.cluster_context not in node.referenced_by:
                    node.referenced_by.append(item.cluster_context)
            result.resolutions.append(JarResolution(
                **asdict(item), status="MATCHED", selected_jar=selected,
                candidates=fallback_candidates, detail=detail,
            ))
        elif fallback_candidates:
            result.resolutions.append(JarResolution(
                **asdict(item), status="AMBIGUOUS", candidates=fallback_candidates,
                detail="multiple fallback JARs contain this reference",
            ))
        else:
            result.resolutions.append(JarResolution(
                **asdict(item), status="MISSING", candidates=[],
                detail=f"no matching class/package under {fallback_path}",
            ))
    return result


def reconcile_failed_declared_jars(
    graph: Graph,
    result: JarDiscoveryResult,
) -> None:
    """Mark unavailable declared JARs covered by verified JVM references.

    A Databricks cluster can retain a stale DBFS JAR declaration after the
    artifact is deleted. If every JVM reference found for that cluster context
    was resolved to a present source JAR or a class-indexed shared AIDP JAR,
    the unavailable declaration is superseded rather than an export failure.
    The replacement JAR nodes remain responsible for cluster attachment.
    """
    resolutions_by_context: Dict[str, List[JarResolution]] = {}
    for resolution in result.resolutions:
        resolutions_by_context.setdefault(
            resolution.cluster_context, []
        ).append(resolution)

    for node in graph.files.values():
        if (
            node.kind != "jar"
            or node.fetch_error is None
            or node.aidp_fallback_path is not None
        ):
            continue

        replacements: Set[str] = set()
        covered = bool(node.referenced_by)
        for context in node.referenced_by:
            context_resolutions = resolutions_by_context.get(context, [])
            if not context_resolutions or any(
                item.status in {"MISSING", "AMBIGUOUS"}
                for item in context_resolutions
            ):
                covered = False
                break
            selected = {
                item.selected_jar
                for item in context_resolutions
                if item.status in {"MATCHED", "PRESENT"}
                and item.selected_jar
            }
            if not selected:
                covered = False
                break
            replacements.update(selected)

        if covered and replacements:
            node.aidp_replacement_paths = sorted(replacements)
            node.aidp_fallback_provenance = (
                "DBX-declared JAR unavailable; all discovered JVM references "
                "for its cluster context are covered by class-indexed "
                "replacement JARs"
            )
