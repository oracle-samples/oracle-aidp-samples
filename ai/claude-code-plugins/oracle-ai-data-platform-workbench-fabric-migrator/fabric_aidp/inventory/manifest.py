"""Combine per-source scans into one manifest plus a resolved catalog.

A scanner that raises records its error under that source's summary and the
rest of the scan continues — a single malformed item type must not cost the
operator the whole inventory.
"""
from __future__ import annotations

import time
from pathlib import Path

from fabric_aidp._atomic import write_json_atomic
from fabric_aidp.inventory import dataflow as dataflow_mod
from fabric_aidp.inventory import lakehouse as lakehouse_mod
from fabric_aidp.inventory import notebook as notebook_mod
from fabric_aidp.inventory import pipeline as pipeline_mod
from fabric_aidp.inventory import semanticmodel as semanticmodel_mod
from fabric_aidp.inventory import warehouse as warehouse_mod
from fabric_aidp.inventory.catalog import (
    TIER_ORDER, build_catalog, load_supplied_catalog,
)
from fabric_aidp.inventory.git_workspace import discover_items
from fabric_aidp.namespace import validated_namespace
from fabric_aidp.sources import ALL_SOURCES, ITEM_TYPES_BY_SOURCE

_SCANNERS = {
    "notebook": notebook_mod.scan,
    "warehouse": warehouse_mod.scan,
    "lakehouse": lakehouse_mod.scan,
    "pipeline": pipeline_mod.scan,
    "semanticmodel": semanticmodel_mod.scan,
    "dataflow": dataflow_mod.scan,
}
_EMPTY = {"summary": {}, "items": {}}

# `ITEM_TYPES_BY_SOURCE` says which Fabric item type each source scans; it
# lives in fabric_aidp/sources.py beside the two other views of the same list,
# because they drifted while they were apart. A Fabric workspace holds far
# more types than these -- Report, Environment, KQLDatabase, Eventhouse,
# SparkJobDefinition, MirroredDatabase, Reflex, MLModel -- and every one of
# them used to be discovered and then vanish: no source claimed it, so it
# reached neither the manifest nor the plan and nothing said it had been
# skipped. Measured on a nine-item export, eight items appeared nowhere in
# the manifest JSON and the plan had one asset.
_SOURCE_BY_ITEM_TYPE = {item_type.casefold(): source
                        for source, types in ITEM_TYPES_BY_SOURCE.items()
                        for item_type in types}


def _unsupported(items, selected) -> list:
    """Every discovered item no selected scanner will look at.

    Two reasons, kept apart because they need different actions from the
    operator: a type this tool has no scanner for at all, and one whose
    scanner simply was not asked for on this run.
    """
    wanted = {item_type.casefold()
              for source in selected
              for item_type in ITEM_TYPES_BY_SOURCE.get(source, ())}
    out = []
    for item in items:
        folded = item.item_type.casefold()
        if folded in wanted:
            continue
        owner = _SOURCE_BY_ITEM_TYPE.get(folded)
        out.append({
            "name": item.name,
            "item_type": item.item_type,
            "path": item.path.name,
            "reason": (
                f"the {owner!r} scanner was not selected for this run"
                if owner else
                f"this tool has no scanner for Fabric item type "
                f"{item.item_type!r}"),
        })
    return sorted(out, key=lambda row: (row["item_type"], row["name"]))


def build_manifest(root, sources=ALL_SOURCES, *, catalog_csv=None,
                   oci_namespace=None, log=None) -> dict:
    normalized = tuple(dict.fromkeys(sources))
    # Recorded, not used: nothing in a scan needs an OCI namespace. `plan`
    # reads it back, which is what `inventory --namespace` has always said it
    # does -- "recorded for plan" -- and never did. Validated here so a
    # mistyped namespace is refused at the verb the user typed it at.
    if oci_namespace is not None:
        validated_namespace(oci_namespace)
    unknown = [s for s in normalized if s not in _SCANNERS]
    if not normalized or unknown:
        detail = f"unsupported source(s): {unknown}" if unknown else "source list is empty"
        raise ValueError(f"{detail}; valid sources: {ALL_SOURCES}")

    root = Path(root)
    # An item whose identity file will not read has no type, so no scanner
    # can claim it and it used to fall out of the manifest with nothing
    # said. It is carried at the top level rather than under a source for
    # the same reason: there is no source it belongs to.
    unreadable: list = []
    items = discover_items(root, problems=unreadable)
    if log:
        log(f"found {len(items)} items in {root}")
        for problem in unreadable:
            log(f"  UNREADABLE {problem.path.name} — {problem.reason}")

    scanned: dict = {}
    for name in normalized:
        if log:
            log(f"  scanning {name}...")
        try:
            scanned[name] = _SCANNERS[name](items, log=log)
        except Exception as exc:  # one bad source must not sink the scan
            scanned[name] = {"summary": {"error": str(exc)}, "items": {}}
            if log:
                log(f"  {name}: failed — {exc}")

    unsupported = _unsupported(items, normalized)
    if log:
        for row in unsupported:
            log(f"  NOT MIGRATED {row['item_type']} {row['name']} — "
                f"{row['reason']}")

    supplied = load_supplied_catalog(catalog_csv) if catalog_csv else None
    return {
        "source": "fabric-git",
        "workspace_name": root.resolve().name,
        "workspace_path": str(root.resolve()),
        "scanned_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "sources_scanned": list(normalized),
        "oci_namespace": oci_namespace,
        "sources": scanned,
        # ITEM DIRECTORIES, not files. "Item" is Fabric's own word for a
        # workspace object's directory -- the thing with a `.platform` in it
        # -- and this list holds the ones `discover_items` could not read,
        # so no scanner ever saw inside them. `summarize()` prints it as
        # "item director(ies) could not be read and were not scanned", and
        # `planner._unreadable_assets` turns each entry into an asset of its
        # own with target `aidp_unmigrated`.
        #
        # A FILE that would not read INSIDE an item that did read is a
        # different thing and is deliberately NOT here. It is counted in its
        # own source's summary and carried on the object it belongs to:
        # `warehouse.summary.unreadable_object_count` with a `read_error` on
        # each object, `dataflow.summary.metadata_unreadable`,
        # `notebook.summary.parse_error_count`. Those objects already reach
        # the plan under their own ids, so listing them here would count one
        # file as two assets and report a readable item as unreadable.
        #
        # This comment exists because the name has been read the other way,
        # as "files that could not be read" -- and it is a comment rather
        # than a rename because the key is in every manifest already
        # written and `_unreadable_assets` looks it up by name, so renaming
        # it would drop those assets out of a plan built from an existing
        # inventory.json without a word. docs/RUNBOOK.md, "Step 2", carries
        # the operator's version of the same distinction.
        "unreadable_items": [
            {"name": problem.path.name,
             "path": _relative_to(problem.path, root),
             "reason": problem.reason}
            for problem in unreadable],
        "unsupported_items": unsupported,
        "resolved_catalog": build_catalog(scanned, supplied=supplied),
    }


def _relative_to(path: Path, root: Path) -> str:
    """`path` as a posix path under `root`, or absolute when it is not."""
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def write_manifest(manifest: dict, out) -> Path:
    path = Path(out)
    write_json_atomic(path, manifest)
    return path


def _summary_of(manifest: dict, source: str) -> dict:
    value = manifest.get("sources", {}).get(source, _EMPTY)
    summary = value.get("summary")
    return summary if isinstance(summary, dict) else {}


def summarize(manifest: dict) -> str:
    lines = [
        f"Fabric workspace: {manifest.get('workspace_name', '?')}",
        f"scanned at:       {manifest.get('scanned_at', '?')}",
        "",
    ]
    for source in manifest.get("sources_scanned", []):
        summary = _summary_of(manifest, source)
        if "error" in summary:
            lines.append(f"  {source:<14s} FAILED — {summary['error']}")
            continue
        parts = [f"{k}={v}" for k, v in summary.items() if not isinstance(v, dict)]
        lines.append(f"  {source:<14s} " + (", ".join(parts) if parts else "(none)"))

    # Tracking coverage — an untracked type must never read as zero.
    for lakehouse in (manifest.get("sources", {}).get("lakehouse", _EMPTY)
                      .get("items", {}).get("lakehouses", [])):
        tracking = lakehouse.get("tracking", {})
        count = lakehouse.get("shortcut_count")
        shown = f"{tracking.get('shortcuts', 'unknown')}"
        if count is not None:
            shown += f" ({count} found)"
        lines.append(f"    {lakehouse.get('name', '?')}.Lakehouse — shortcuts: {shown}"
                     f" · data access roles: {tracking.get('data_access_roles', 'unknown')}")
        # "unknown" on its own does not say whether tracking was off or the
        # file was unreadable, and only one of those needs acting on.
        if lakehouse.get("shortcuts_error"):
            lines.append(f"      {lakehouse['shortcuts_error']}")

    unreadable = manifest.get("unreadable_items") or []
    if unreadable:
        lines.extend(["", f"  {len(unreadable)} item director"
                          f"{'y' if len(unreadable) == 1 else 'ies'} could not "
                          f"be read and {'was' if len(unreadable) == 1 else 'were'} "
                          f"not scanned:"])
        for problem in unreadable:
            lines.append(f"    {problem.get('path', '?')} — "
                         f"{problem.get('reason', 'no reason recorded')}")

    unsupported = manifest.get("unsupported_items") or []
    if unsupported:
        by_type: dict = {}
        for row in unsupported:
            by_type.setdefault(row.get("item_type", "?"), []).append(
                row.get("name", "?"))
        lines.extend(["", f"  {len(unsupported)} item(s) of "
                          f"{len(by_type)} type(s) were found but not "
                          f"migrated:"])
        for item_type, names in sorted(by_type.items()):
            reason = next(r.get("reason", "") for r in unsupported
                          if r.get("item_type") == item_type)
            lines.append(f"    {item_type:<22s} {', '.join(sorted(names))}"
                         f"  — {reason}")

    catalog = manifest.get("resolved_catalog", {}).get("summary", {})
    if catalog:
        lines.extend(["", "  resolved catalog:"])
        for tier in TIER_ORDER:
            lines.append(f"    {catalog.get(tier, 0):4d}  {tier}")
        inferred = catalog.get("notebook_inferred", 0)
        if inferred:
            lines.append(f"    note: {inferred} table(s) inferred from notebook writes — "
                         f"name only, not confirmed to exist")
    return "\n".join(lines)
