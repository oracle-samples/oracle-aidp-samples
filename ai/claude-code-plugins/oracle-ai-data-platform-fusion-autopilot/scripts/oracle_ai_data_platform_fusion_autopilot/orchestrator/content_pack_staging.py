"""Content-pack file staging for REST dispatch.

The cluster has no access to the customer's local filesystem. A
``contentPack.path: ../../content_packs/foo`` relative path is
meaningless on the AIDP cluster. The notebook builder must carry every
file the runner needs (pack.yaml + node YAML + SQL templates +
dashboards) inline, materialise them to a cluster-side temp dir, and
hand the orchestrator a reconstructed ``ResolvedPack`` plus a
sibling-aware base resolver.

This module provides the two helpers that bracket the round trip:

* :func:`stage_pack_files` — runs on the laptop (orchestrator-side)
  before notebook build. Walks the merged ``ResolvedPack`` and produces
  ``(files_by_relpath, manifest)`` primitives that the dispatch layer
  can embed as base64+JSON in the notebook source.
* :func:`materialize_staged_pack` — runs on the cluster (orchestrator-
  side at the cluster). Writes the embedded files to a tempdir and
  returns ``(top_overlay_root, base_resolver)`` that the cluster-side
  ``load_full_chain`` call uses to reconstruct the same merged pack.

The two helpers MUST be designed for cluster-side importability — pure
stdlib + this package's own modules; no Spark dependency.

Error codes:

* AIDPF-1039 — path-traversal rejection during staging
* AIDPF-1040 — provenance inconsistency guardrail
"""

from __future__ import annotations

import re
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Mapping

if TYPE_CHECKING:
    from ..schema.medallion_pack import PackOverlayRef

    from .content_pack import ResolvedPack


# ---------------------------------------------------------------------------
# AIDPF error codes
# ---------------------------------------------------------------------------

AIDPF_1039_PATH_TRAVERSAL = "AIDPF-1039"
"""Content-pack SQL path escapes pack root (absolute, ``..``, symlink-out)."""

AIDPF_1040_PROVENANCE_INCONSISTENCY = "AIDPF-1040"
"""``resolved_pack.root_for(qid)`` is not in ``chain_roots`` — programmer-error guardrail."""


class ContentPackPathTraversalError(Exception):
    """Pack-relative SQL path escapes the layer root (AIDPF-1039)."""


class ContentPackProvenanceError(Exception):
    """``resolved_pack.root_for(qid)`` not present in ``chain_roots`` (AIDPF-1040)."""


# ---------------------------------------------------------------------------
# Public API — stage_pack_files
# ---------------------------------------------------------------------------


def stage_pack_files(
    resolved_pack: "ResolvedPack",
) -> tuple[dict[str, str], dict[str, Any]]:
    """Walk a merged ResolvedPack and produce stageable file primitives.

    Two-pass collection:

    1. **YAML manifests (glob)**: for each layer in ``chain_roots``, read
       ``pack.yaml``, ``bronze.yaml`` (if present), ``silver/*.yaml``,
       ``gold/*.yaml``, and ``dashboards/*.yaml`` as text.
    2. **SQL templates (declared-path based)**: walk every node in the
       merged ``resolved_pack.silver`` and ``resolved_pack.gold``. For
       each node, read the SQL from ``resolved_pack.root_for(qid) /
       node.implementation.sql`` (the merged effective path, which may
       differ from any single layer's node YAML — overrides). Map the
       source root back to its index in ``chain_roots`` and stage under
       ``__layer_N__/<node.implementation.sql>``. Root-bound
       normalisation rejects ``..`` / absolute paths / symlink-outs
       with AIDPF-1039.

    Args:
        resolved_pack: the merged ``ResolvedPack`` from
            :func:`load_full_chain`. Must have ``chain_roots`` populated
            (non-overlay packs get a single-element tuple containing the
            pack root).

    Returns:
        A 2-tuple:

        * ``files_by_relpath: dict[str, str]`` — keys are layer-relative
          paths (e.g. ``__layer_0__/pack.yaml``, ``__layer_2__/silver/dim_thing.sql``);
          values are file contents as text.
        * ``manifest: dict`` — JSON-serialisable, with ``chain_layers``
          ordered list and ``entry_layer_index`` pointing at the top
          overlay (the one the user pointed at via ``contentPack.path``).

    Raises:
        ContentPackPathTraversalError: AIDPF-1039 — declared SQL path
            escapes the layer root.
        ContentPackProvenanceError: AIDPF-1040 — ``resolved_pack.root_for(qid)``
            not in ``chain_roots`` (programmer-error guardrail).
    """
    chain_roots = _resolve_chain_roots(resolved_pack)

    files_by_relpath: dict[str, str] = {}
    chain_layers_meta: list[dict[str, Any]] = []

    # Pass 1: YAML manifests via globs (per layer).
    for index, layer_root in enumerate(chain_roots):
        layer_subdir = f"__layer_{index}__"
        _stage_yaml_manifests(layer_root, layer_subdir, files_by_relpath)
        # Track layer identity for the manifest.
        pack_yaml_path = layer_root / "pack.yaml"
        pack_id = _read_pack_id(pack_yaml_path)
        chain_layers_meta.append(
            {"index": index, "subdir": layer_subdir, "pack_id": pack_id}
        )

    # Pass 2: SQL templates driven by the merged ResolvedPack.
    _stage_sql_templates(resolved_pack, chain_roots, files_by_relpath)

    manifest = {
        "chain_layers": chain_layers_meta,
        "entry_layer_index": len(chain_roots) - 1,
    }
    return files_by_relpath, manifest


# ---------------------------------------------------------------------------
# Public API — materialize_staged_pack (cluster-side)
# ---------------------------------------------------------------------------


def materialize_staged_pack(
    files_by_relpath: Mapping[str, str],
    manifest: Mapping[str, Any],
) -> tuple[Path, Callable[["PackOverlayRef"], Path]]:
    """Materialise embedded files to a cluster-side tempdir.

    Writes every key in ``files_by_relpath`` to the corresponding path
    under a ``tempfile.mkdtemp(prefix="aidp-pack-")`` root. Returns the
    top-overlay root (the layer index named by
    ``manifest["entry_layer_index"]``) plus a closure-bound base
    resolver that handles ``extends:`` references by scanning the
    staged layer subdirs.

    The returned 2-tuple is what the cluster-side notebook hands to
    ``load_full_chain(top_overlay_root, base_resolver=resolver)``.

    Args:
        files_by_relpath: dict from staging (key: relative path,
            value: file content as text).
        manifest: companion manifest from :func:`stage_pack_files`.

    Returns:
        ``(top_overlay_root: Path, base_resolver: Callable)``.
    """
    tempdir = Path(tempfile.mkdtemp(prefix="aidp-pack-")).resolve()
    for relpath, content in files_by_relpath.items():
        target = tempdir / relpath
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")

    chain_layers = manifest.get("chain_layers", [])
    entry_index = manifest.get("entry_layer_index", len(chain_layers) - 1)
    if not chain_layers:
        # Degenerate fixture; return tempdir + no-op resolver.
        return tempdir, _no_op_resolver

    top_overlay_root = tempdir / chain_layers[entry_index]["subdir"]

    # Build a base_resolver closure that maps a PackOverlayRef -> path
    # under the staged tempdir by scanning the layer subdirs for the
    # matching pack id.
    def staging_base_resolver(ref) -> Path:
        for layer in chain_layers:
            candidate = tempdir / layer["subdir"]
            if not candidate.exists():
                continue
            if (candidate / "pack.yaml").exists():
                staged_pack_id = _read_pack_id(candidate / "pack.yaml")
                if staged_pack_id == ref.name:
                    return candidate.resolve()
        raise FileNotFoundError(
            f"staged base pack {ref.name!r} not found under {tempdir}. "
            f"Staged layers: {[layer['subdir'] for layer in chain_layers]!r}."
        )

    return top_overlay_root, staging_base_resolver


def _no_op_resolver(ref):  # pragma: no cover — only hit on degenerate empty manifest
    raise FileNotFoundError(f"no staged layers — cannot resolve {ref!r}")


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _resolve_chain_roots(resolved_pack: "ResolvedPack") -> tuple[Path, ...]:
    """Return ``chain_roots`` if present, else fall back to
    a single-element tuple from ``resolved_pack.root``.

    ResolvedPack carries ``chain_roots`` for full overlay-chain provenance.
    For backward compatibility, fall back to just ``resolved_pack.root`` when
    the attribute is absent or empty.
    """
    chain_roots = getattr(resolved_pack, "chain_roots", None)
    if chain_roots:
        return tuple(chain_roots)
    # No explicit chain_roots: derive from per-node source_roots so an
    # overlay's inherited base-pack nodes are staged too. Without this an
    # overlay falls back to its own root only and every inherited node raises
    # AIDPF-1040 at staging — i.e. no overlay (e.g. one extending the installed
    # fusion-finance-starter) could be seeded. merge_overlay seeds
    # source_roots base-first, so dict-insertion order is base -> overlay,
    # which matches the entry_layer_index = len(chain_roots) - 1 contract.
    source_roots = getattr(resolved_pack, "source_roots", None)
    if source_roots:
        ordered: dict[Path, None] = {}
        for r in source_roots.values():
            ordered.setdefault(r, None)
        if ordered:
            return tuple(ordered)
    return (resolved_pack.root,)


def _stage_yaml_manifests(
    layer_root: Path,
    layer_subdir: str,
    files_by_relpath: dict[str, str],
) -> None:
    """Stage YAML manifest files from a single layer root."""
    for relname in ("pack.yaml", "bronze.yaml"):
        path = layer_root / relname
        if path.exists():
            files_by_relpath[f"{layer_subdir}/{relname}"] = path.read_text(encoding="utf-8")

    # Bronze nodes may be per-file ``bronze/<id>.yaml`` entries. ``bronze``
    # must be globbed here or the cluster reconstructs an empty pack.bronze
    # while local ``load_full_chain`` succeeds.
    for subdir in ("bronze", "silver", "gold", "dashboards"):
        d = layer_root / subdir
        if not d.exists():
            continue
        for p in sorted(d.glob("*.yaml")):
            relpath = f"{layer_subdir}/{subdir}/{p.name}"
            files_by_relpath[relpath] = p.read_text(encoding="utf-8")


def _stage_sql_templates(
    resolved_pack: "ResolvedPack",
    chain_roots: tuple[Path, ...],
    files_by_relpath: dict[str, str],
) -> None:
    """Stage SQL templates driven by the merged ResolvedPack.

    For each node, resolve the source root via ``pack.root_for(qid)``
    and stage the SQL under the corresponding ``__layer_N__/`` subdir. This
    preserves per-node provenance for overlay SQL overrides.
    """
    all_nodes = {}
    for node_id, node in resolved_pack.silver.items():
        all_nodes[f"silver/{node_id}"] = node
    for node_id, node in resolved_pack.gold.items():
        all_nodes[f"gold/{node_id}"] = node

    for qid, node in all_nodes.items():
        impl = node.implementation
        sql_relpath_str = getattr(impl, "sql", None)
        if not sql_relpath_str:
            # Non-SQL implementation (builtin, bronze_extract) — no
            # SQL file to stage.
            continue

        source_root = resolved_pack.root_for(qid).resolve()
        _validate_sql_path_within_layer(source_root, sql_relpath_str)

        # Map source_root -> chain_roots index.
        layer_index: int | None = None
        for idx, layer_root in enumerate(chain_roots):
            if Path(layer_root).resolve() == source_root:
                layer_index = idx
                break
        if layer_index is None:
            raise ContentPackProvenanceError(
                f"{AIDPF_1040_PROVENANCE_INCONSISTENCY}: source root for node "
                f"{qid!r} is {source_root!r}, not found in chain_roots "
                f"{tuple(str(r) for r in chain_roots)!r}."
            )

        sql_abs_path = (source_root / sql_relpath_str).resolve()
        if not sql_abs_path.exists():
            # Don't stage what doesn't exist; downstream loader will
            # error clearly.
            continue

        stage_key = f"__layer_{layer_index}__/{sql_relpath_str}"
        files_by_relpath[stage_key] = sql_abs_path.read_text(encoding="utf-8")


def _validate_sql_path_within_layer(layer_root: Path, sql_path_str: str) -> None:
    """Root-bound normalisation — reject paths that escape the layer root."""
    if not sql_path_str:
        return
    sql_path = Path(sql_path_str)
    if sql_path.is_absolute():
        raise ContentPackPathTraversalError(
            f"{AIDPF_1039_PATH_TRAVERSAL}: SQL path {sql_path_str!r} is absolute; "
            f"only pack-relative paths are allowed."
        )
    if ".." in sql_path.parts:
        raise ContentPackPathTraversalError(
            f"{AIDPF_1039_PATH_TRAVERSAL}: SQL path {sql_path_str!r} contains "
            f"'..' segments which would escape the pack root."
        )
    resolved = (layer_root / sql_path).resolve()
    layer_resolved = Path(layer_root).resolve()
    try:
        resolved.relative_to(layer_resolved)
    except ValueError:
        raise ContentPackPathTraversalError(
            f"{AIDPF_1039_PATH_TRAVERSAL}: SQL path {sql_path_str!r} resolves to "
            f"{resolved!r} which is outside the pack root {layer_resolved!r}."
        )


_PACK_ID_LINE_RE = re.compile(r"^id:\s*([^\s#]+)", re.MULTILINE)


def _read_pack_id(pack_yaml_path: Path) -> str:
    """Lightweight pack-id extractor (no full Pydantic load).

    We only need the id field for the manifest's layer identification;
    parsing the full PackYaml here would pull pydantic v2 model
    construction onto a cluster path that already does the same thing
    immediately afterward via load_full_chain.
    """
    text = pack_yaml_path.read_text(encoding="utf-8")
    m = _PACK_ID_LINE_RE.search(text)
    return m.group(1) if m else ""
