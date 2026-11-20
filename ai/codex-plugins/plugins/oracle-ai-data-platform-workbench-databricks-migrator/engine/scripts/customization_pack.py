"""Customer customization pack — golden-notebook overrides (Layer 1).

Some notebooks need transformations no generic rule or LLM can produce: an
injected business map, a new capability (e.g. time-travel reads), an SDK swap
(boto3 -> OCI ObjectStorage), or an f-string path convention that encodes a
tenancy/namespace decision. For these, the customer maintains a pre-converted
"golden" notebook, and the migrator swaps it in for the original BEFORE
analysis — then still runs it through the normal execute+verify+fix loop, so a
golden that has drifted from the current runtime is repaired (minor) or fails
loudly in validation (major), never shipped blind.

The tool core stays generic: it only knows how to load a pack from the path in
`cfg.customizations.path`. The pack (overrides.yaml + notebooks/) lives OUTSIDE
the tool — e.g. `customer_packs/<customer>/` — so a new customer is a new pack
(or none), with nothing to strip from the tool. Remove a customer's overrides
by deleting their pack directory and the one config line.

overrides.yaml schema (v1):
    schema_version: 1
    overrides:
      - notebook: acme_reader.ipynb             # matched by BASENAME (any path)
        replacement: notebooks/acme_reader.ipynb      # relative to pack dir
        original_sha256: "<hash of the original this was converted from>"
        path_glob: "*/acme_pipeline/*"                # OPTIONAL disambiguator
        reason: "one-line explanation of why this needs a golden override"

Matching: by notebook basename (exact case). `original_sha256` is NOT a
matcher — it is the drift detector: a mismatch is reported as OVERRIDE_DRIFT
(the override still applies; the downstream verify+fix loop handles minor
drift), while an exact match reports OVERRIDE_APPLIED.

`path_glob`, when present, must also match the original's workspace path.
Matching is CASE-SENSITIVE and uses POSIX (forward-slash) separators on ALL
platforms: both the notebook path and the glob are normalized to forward
slashes and compared with fnmatch.fnmatchcase, so a pack authored on Windows
matches identically on Linux (and vice versa).

Safety: an entry WITHOUT a `path_glob` that matches more than one DISTINCT
notebook path within a run is ambiguous — two different notebooks sharing a
basename must never both be silently replaced by the same golden. The second
distinct match raises (surfaced as CUSTOMIZATION_FAILED by the caller) with an
"ambiguous override — add path_glob" message.

Fail-loud contract (repo convention, cf. aidp_config file references): an
EXPLICITLY configured pack that cannot be loaded — missing directory, missing
overrides.yaml, unreadable or malformed YAML — raises at load time. Only an
ABSENT `customizations` config section leaves the engine inert.
"""
from __future__ import annotations

import fnmatch
import hashlib
import json
import os
from typing import Any, Dict, List, Optional, Set

try:
    import yaml
except ImportError:  # pragma: no cover - yaml is a hard dep elsewhere
    yaml = None


_OVERRIDES: Optional[List[Dict[str, Any]]] = None  # loaded registry (None = not loaded)
_PACK_DIR: Optional[str] = None
# Run-scoped ambiguity tracker for entries WITHOUT a path_glob:
# entry index -> distinct normalized notebook paths it has matched this run.
_MATCHED_PATHS: Dict[int, Set[str]] = {}


def ray_init_script_destination() -> Optional[str]:
    """Return the configured Ray bootstrap path, if this pack enables it."""
    if _PACK_DIR is None:
        return None
    registry = os.path.join(_PACK_DIR, "overrides.yaml")
    with open(registry, encoding="utf-8") as stream:
        document = yaml.safe_load(stream)
    rule = (document or {}).get("ray_init_script") or {}
    return rule.get("destination")


def _posix_norm(path: str) -> str:
    """Normalize a workspace path/glob to forward slashes (no case folding)."""
    return path.replace("\\", "/").rstrip("/")


def load_pack(pack_path: Optional[str]) -> None:
    """Load the customer pack's overrides.yaml. Idempotent; no-op when unset.

    `pack_path` points at the pack directory (containing overrides.yaml).
    Fail-loud contract: an ABSENT config (pack_path None/empty) leaves the
    engine inert, but an EXPLICITLY configured pack that cannot be loaded —
    nonexistent directory (path typo, wrong cwd for a relative path), missing
    overrides.yaml, unreadable or malformed YAML — raises here. Silently
    disabling all overrides would ship the un-overridden originals, which is
    exactly what the pack exists to prevent.
    """
    global _OVERRIDES, _PACK_DIR
    _OVERRIDES, _PACK_DIR = [], None
    _MATCHED_PATHS.clear()
    if not pack_path:
        return
    pack_dir = os.path.abspath(os.path.expanduser(pack_path))
    if not os.path.isdir(pack_dir):
        raise FileNotFoundError(
            f"customization pack directory not found: {pack_dir} "
            f"(configured as customizations.path={pack_path!r} — fix the path, "
            f"launch from the repo root, or remove the customizations section)"
        )
    registry = os.path.join(pack_dir, "overrides.yaml")
    if not os.path.isfile(registry):
        raise FileNotFoundError(
            f"customization pack has no overrides.yaml: {registry} "
            f"(a configured pack must contain a registry; remove the "
            f"customizations section for a customer with no overrides)"
        )
    if yaml is None:
        raise RuntimeError("PyYAML required to load a customization pack")
    with open(registry) as fh:
        doc = yaml.safe_load(fh) or {}
    version = doc.get("schema_version")
    if version != 1:
        raise ValueError(
            f"customization pack {registry}: unsupported schema_version {version!r} (expected 1)"
        )
    overrides = doc.get("overrides") or []
    if not isinstance(overrides, list):
        raise ValueError(f"customization pack {registry}: 'overrides' must be a list")
    for entry in overrides:
        if not entry.get("notebook") or not entry.get("replacement"):
            raise ValueError(
                f"customization pack {registry}: each override needs 'notebook' and 'replacement' "
                f"(bad entry: {entry!r})"
            )
    _PACK_DIR = pack_dir
    _OVERRIDES = overrides


def _norm_code_hash(nb: Dict[str, Any]) -> str:
    """Stable hash of a notebook's code cells (whitespace-trimmed, order-kept).

    Ignores outputs/metadata/markdown so cosmetic re-saves don't trip drift.
    """
    srcs = [
        "".join(c.get("source", []))
        for c in nb.get("cells", [])
        if c.get("cell_type") == "code"
    ]
    joined = "\n<CELL>\n".join(s.strip() for s in srcs)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def match_override(notebook_path: str) -> Optional[Dict[str, Any]]:
    """Return the override entry for this notebook path, or None.

    Matched by basename (exact case); an entry's optional `path_glob` must
    also match the full workspace path. Glob matching is case-sensitive and
    posix-separator on all platforms (fnmatchcase over forward-slash-normalized
    forms). First matching entry wins (registry order).

    Ambiguity guard: an entry WITHOUT a path_glob that matches a SECOND
    distinct notebook path within this run raises ValueError — the caller
    surfaces it as CUSTOMIZATION_FAILED for that notebook. Never silently
    apply one golden to two different notebooks; add a path_glob instead.
    """
    if not _OVERRIDES:
        return None
    norm_path = _posix_norm(notebook_path)
    base = os.path.basename(norm_path)
    if not base.endswith(".ipynb"):
        base += ".ipynb"
    for idx, entry in enumerate(_OVERRIDES):
        want = entry["notebook"]
        if not want.endswith(".ipynb"):
            want += ".ipynb"
        if os.path.basename(want) != base:
            continue
        glob = entry.get("path_glob")
        if glob:
            if not fnmatch.fnmatchcase(norm_path, _posix_norm(glob)):
                continue
        else:
            # Globless entry: track distinct paths matched this run so two
            # different notebooks sharing a basename fail loud, not silently
            # both get overridden. Suffix-normalize so `a/nb` == `a/nb.ipynb`.
            canon = norm_path if norm_path.endswith(".ipynb") else norm_path + ".ipynb"
            seen = _MATCHED_PATHS.setdefault(idx, set())
            seen.add(canon)
            if len(seen) > 1:
                raise ValueError(
                    f"ambiguous override — add path_glob: override for "
                    f"{entry['notebook']!r} has no path_glob but matched multiple "
                    f"distinct notebooks in this run: {sorted(seen)}"
                )
        return entry
    return None


def apply_override(entry: Dict[str, Any], original_nb: Dict[str, Any]) -> Dict[str, Any]:
    """Load the golden replacement notebook and report drift vs the original.

    Returns {"cells", "status" (OVERRIDE_APPLIED|OVERRIDE_DRIFT), "reason",
             "replacement", "expected_sha256", "original_sha256"}.
    The caller substitutes `cells` into the working notebook and logs status.
    The golden is applied regardless of drift; the downstream execute+verify+fix
    loop repairs minor drift and fails loudly on major.
    """
    if _PACK_DIR is None:
        raise RuntimeError("apply_override called before load_pack")
    repl_path = os.path.join(_PACK_DIR, entry["replacement"])
    if not os.path.isfile(repl_path):
        raise FileNotFoundError(
            f"customization override replacement not found: {repl_path} "
            f"(override for {entry['notebook']!r})"
        )
    with open(repl_path) as fh:
        golden = json.load(fh)
    golden_cells = golden.get("cells")
    if not golden_cells:
        raise ValueError(f"customization override {repl_path} has no cells")

    original_sha = _norm_code_hash(original_nb)
    expected = entry.get("original_sha256")
    status = "OVERRIDE_APPLIED"
    if expected and expected != original_sha:
        status = "OVERRIDE_DRIFT"
    return {
        "cells": golden_cells,
        "status": status,
        "reason": entry.get("reason", ""),
        "replacement": entry["replacement"],
        "expected_sha256": expected,
        "original_sha256": original_sha,
    }
