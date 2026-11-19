"""Neutral plan resolver for dispatch-side dry-run plan rendering.

Walks a ``ResolvedPack`` instead of registry-metadata maps. Bronze +
silver + gold are all sourced from the resolved content pack; node
``dependsOn`` edges drive prerequisite discovery and topological sort.

The engine-side ``orchestrator.run`` calls
:func:`resolve_content_pack_plan` for runtime dispatch. The
dispatch-side ``dispatch.dispatch_via_rest`` dry-run path consumes
the DTOs (``PlanNode`` + ``PrereqNode``) this module produces.

Boundary contract: this module MUST NOT import from ``orchestrator/*``,
``dimensions/*``, ``transforms/*``, or ``extractors/*``. The pack is
loaded by the caller (``commands/run.py`` for both the inline and the
REST paths) and passed in; this preserves the dispatch import boundary that
``tests/unit/dispatch/test_imports.py`` enforces.
"""

from __future__ import annotations

from graphlib import TopologicalSorter
from typing import TYPE_CHECKING, Final, Literal

from .errors import MissingDependencyError
from .run_summary import PlanNode, PrereqNode

if TYPE_CHECKING:  # pragma: no cover
    from oracle_ai_data_platform_fusion_autopilot.config.paths import TablePaths

    from .bundle import Bundle
    from .medallion_pack import NodeYaml, ResolvedPack


# Closed set of layer values. Inlined here so the schema-layer
# resolver does not need to import from orchestrator (which would
# defeat the dispatch import boundary).
_VALID_LAYERS: Final[frozenset[str]] = frozenset({"bronze", "silver", "gold"})

# Mirrors orchestrator.content_pack_plan_resolver's strict-scope error
# code. Inlined here for the same boundary reason.
AIDPF_1042_STRICT_SCOPE_MISSING_DEPENDENCY: Final[str] = "AIDPF-1042"

# Mirrors orchestrator.content_pack_plan_resolver's empty-plan error
# code. Inlined here for the dispatch import boundary.
AIDPF_1045_LAYER_FILTER_EMPTIED_PLAN: Final[str] = "AIDPF-1045"

# Bundle-section names per layer — operator-facing remediation strings.
_BUNDLE_SECTION: Final[dict[str, str]] = {
    "bronze": "bundle.datasets",
    "silver": "bundle.dimensions.build",
    "gold":   "bundle.gold.marts",
}


def _bronze_ids_from_pack(pack: "ResolvedPack") -> set[str]:
    """Collect bronze node ids from the resolved pack.

    Honors both per-file ``pack.bronze`` and the legacy
    single-file ``pack.bronze_yaml`` (back-compat fallback).
    """
    ids: set[str] = set(pack.bronze.keys())
    bronze_yaml = getattr(pack, "bronze_yaml", None) or {}
    for ds in bronze_yaml.get("datasets", []) or []:
        if isinstance(ds, dict) and "id" in ds:
            ids.add(str(ds["id"]))
    return ids


def _node_layer(pack: "ResolvedPack", name: str) -> str | None:
    """Identify which layer a name belongs to in the resolved pack."""
    if name in pack.bronze:
        return "bronze"
    if name in pack.silver:
        return "silver"
    if name in pack.gold:
        return "gold"
    # Legacy bronze.yaml fallback.
    bronze_yaml = getattr(pack, "bronze_yaml", None) or {}
    for ds in bronze_yaml.get("datasets", []) or []:
        if isinstance(ds, dict) and ds.get("id") == name:
            return "bronze"
    return None


def _node_depends_on(
    pack: "ResolvedPack", layer: str, name: str,
) -> tuple[list[str], list[str]]:
    """Return (bronze_dep_ids, silver_dep_ids) for a pack node.

    Bronze nodes never have dependsOn entries. Silver nodes depend on
    bronze; gold nodes depend on bronze + silver.
    """
    bucket: dict[str, "NodeYaml"] | None = None
    if layer == "silver":
        bucket = pack.silver
    elif layer == "gold":
        bucket = pack.gold
    if not bucket or name not in bucket:
        return [], []
    node = bucket[name]
    deps = getattr(node, "depends_on", None)
    if deps is None:
        return [], []
    bronze_ids = [src.id for src in getattr(deps, "bronze", []) or []]
    silver_ids = [src.id for src in getattr(deps, "silver", []) or []]
    return bronze_ids, silver_ids


def _bronze_target(pack: "ResolvedPack", node_id: str) -> str:
    """Return the bronze node's ``target`` table name (single-segment).

    Honors per-file ``pack.bronze`` first; falls back to the legacy
    ``pack.bronze_yaml`` form (which carries the table name as the
    dataset's ``target`` or ``pvo`` key) — or finally to ``node_id``.
    """
    if node_id in pack.bronze:
        return pack.bronze[node_id].target
    bronze_yaml = getattr(pack, "bronze_yaml", None) or {}
    for ds in bronze_yaml.get("datasets", []) or []:
        if isinstance(ds, dict) and ds.get("id") == node_id:
            return str(ds.get("target") or ds.get("pvo") or node_id)
    return node_id


def resolve_dry_run_plan(
    pack: "ResolvedPack",
    bundle: "Bundle",
    paths: "TablePaths",
    *,
    datasets: list[str] | None,
    layers: list[str] | None,
    strict_scope: bool = False,
) -> tuple[tuple[PlanNode, ...], tuple[PrereqNode, ...]]:
    """Classify, filter, and topo-sort the pack plan for dry-run rendering.

    Mirrors the runtime
    ``orchestrator.content_pack_plan_resolver.resolve_content_pack_plan``
    contract byte-for-byte so dry-run is a faithful preview of what
    runtime will do.

    * Effective roots = ``set(datasets)`` when ``--datasets`` is
      given; else the union of ``bundle.datasets[]``,
      ``bundle.dimensions.build``, ``bundle.gold.marts`` (presence-
      aware via ``model_fields_set``-style classification done here).
    * ``--layers`` filters declared roots only; D-1 transitive deps
      remain in the plan regardless of layer.
    * ``strict_scope=False`` (D-1 default): walk
      ``dependsOn.bronze`` ∪ ``dependsOn.silver`` transitively and
      add every reachable dep to the plan, whether or not it was
      declared in the bundle.
    * ``strict_scope=True``: every transitive dep MUST be in
      ``effective_roots``. If a declared root has a dep that isn't
      also in effective_roots, raise ``AIDPF-1042`` — same as
      runtime.

    Returns
    -------
    ``(plan, prereqs)``:
      * ``plan`` — topo-sorted tuple of ``PlanNode``, containing the
        full D-1 closure of effective_roots (when
        ``strict_scope=False``) or effective_roots verbatim (when
        ``strict_scope=True``, since transitive deps must already
        be in the set).
      * ``prereqs`` — always an empty tuple under this design (every
        materializable dep lands in the plan). The slot is retained
        for back-compat with callers + the renderer; future work
        may re-purpose it for "expected-on-disk upstreams the
        engine will read but not write" if a use case emerges.

    Raises:
        MissingDependencyError: any bundle name unknown to the pack;
            any ``--datasets`` typo; any ``--layers`` typo; any node
            whose ``dependsOn`` references an id outside the pack;
            any ``strict_scope=True`` violation (AIDPF-1042).
    """
    bronze_ids_in_pack = _bronze_ids_from_pack(pack)

    # ------------------------------------------------------------------
    # 1. Classify every bundle root against the pack — same typo
    #    validation surface the prior resolver had. Honors
    #    DatasetSpec.enabled=false.
    # ------------------------------------------------------------------
    classes: dict[
        str,
        tuple[Literal["bronze", "silver", "gold"], Literal["eligible"], str | None],
    ] = {}
    disabled_datasets: set[str] = set()
    # Presence-aware bundle scope: the legacy blocks `dimensions:` and
    # `gold:` have non-empty Pydantic defaults
    # (DimensionsSpec.build = [dim_supplier, dim_account, dim_calendar,
    # dim_org]; GoldSpec.marts = [ar_aging, ap_aging, gl_balance,
    # po_backlog]) that fire even when the YAML omits the blocks. A
    # Bundle YAML declaring only `datasets:` would otherwise raise
    # MissingDependencyError for default dim/mart entries that are not
    # actually in scope. Mirror runtime's `model_fields_set` filter so
    # dry-run preview matches execution.
    bundle_fields_set = getattr(bundle, "model_fields_set", set()) or set()
    for ds in bundle.datasets:
        if not ds.enabled:
            disabled_datasets.add(ds.id)
            continue
        layer = _node_layer(pack, ds.id)
        if layer is None:
            raise MissingDependencyError(
                f"Unknown dataset {ds.id!r} in bundle.datasets. "
                f"Known pack ids: bronze={sorted(bronze_ids_in_pack)!r}, "
                f"silver={sorted(pack.silver)!r}, "
                f"gold={sorted(pack.gold)!r}."
            )
        classes[ds.id] = (layer, "eligible", None)
    if "dimensions" in bundle_fields_set:
        for dim_name in bundle.dimensions.build:
            if dim_name in pack.silver:
                classes[dim_name] = ("silver", "eligible", None)
            else:
                raise MissingDependencyError(
                    f"Unknown dim {dim_name!r} in bundle.dimensions.build. "
                    f"Known silver ids: {sorted(pack.silver)!r}."
                )
    if "gold" in bundle_fields_set:
        for mart_name in bundle.gold.marts:
            if mart_name in pack.gold:
                classes[mart_name] = ("gold", "eligible", None)
            else:
                raise MissingDependencyError(
                    f"Unknown mart {mart_name!r} in bundle.gold.marts. "
                    f"Known gold ids: {sorted(pack.gold)!r}."
                )

    # ------------------------------------------------------------------
    # 2. Validate filter inputs.
    # ------------------------------------------------------------------
    if datasets is not None:
        unknown_datasets = sorted(set(datasets) - set(classes))
        if unknown_datasets:
            disabled_in_filter = [
                d for d in unknown_datasets if d in disabled_datasets
            ]
            truly_unknown = [
                d for d in unknown_datasets if d not in disabled_datasets
            ]
            msg_parts: list[str] = []
            if disabled_in_filter:
                msg_parts.append(
                    f"--datasets references disabled name(s): "
                    f"{disabled_in_filter}. Either set `enabled: true` "
                    f"in bundle.datasets for those entries, or remove "
                    f"them from --datasets."
                )
            if truly_unknown:
                msg_parts.append(
                    f"--datasets contains name(s) not in the bundle plan: "
                    f"{truly_unknown}. Available names from bundle.yaml: "
                    f"{sorted(classes)}. --datasets is a filter over the "
                    f"bundle's declared datasets / dimensions / marts; "
                    f"to add a new name, edit bundle.yaml first."
                )
            raise MissingDependencyError("\n".join(msg_parts))
    if layers is not None:
        unknown_layers = sorted(set(layers) - _VALID_LAYERS)
        if unknown_layers:
            raise MissingDependencyError(
                f"--layers contains unknown layer(s): {unknown_layers}. "
                f"Valid layers: {sorted(_VALID_LAYERS)}."
            )

    # ------------------------------------------------------------------
    # 3. Compute effective_roots — the runtime contract: CLI datasets
    #    win, else the bundle scope. Layers filter is applied to
    #    declared roots only (transitive deps stay regardless of
    #    layer — matches runtime).
    # ------------------------------------------------------------------
    if datasets is not None:
        effective_roots: set[str] = set(datasets)
    else:
        effective_roots = set(classes.keys())

    if layers is not None:
        layer_set = {l.strip().lower() for l in layers}
        effective_roots = {
            r for r in effective_roots
            if classes[r][0] in layer_set
        }
        # Round-8 review fix: mirror runtime AIDPF-1045. When the
        # layer filter removes every declared root, runtime
        # immediately raises LayerFilterEmptiedPlanError; pre-fix
        # the dry-run resolver silently returned an empty plan,
        # letting REST dispatch report success on a run the cluster
        # will reject. Now both surfaces agree.
        if not effective_roots:
            raise MissingDependencyError(
                f"{AIDPF_1045_LAYER_FILTER_EMPTIED_PLAN}: --layers "
                f"{sorted(layer_set)!r} removed every declared root; "
                f"plan would be empty. Specify roots whose layer is "
                f"in the filter, or drop --layers."
            )

    # ------------------------------------------------------------------
    # 4. Helper: pack node existence check + dep walk.
    # ------------------------------------------------------------------
    def _check_dep_exists_or_raise(
        dep_name: str, dep_layer: str, consumer: str,
    ) -> None:
        if dep_layer == "bronze":
            if dep_name not in bronze_ids_in_pack:
                raise MissingDependencyError(
                    f"Consumer {consumer!r} depends on bronze "
                    f"{dep_name!r}, but that name is not in the pack's "
                    f"bronze layer. Add a content_packs/<pack>/bronze/"
                    f"<id>.yaml or a legacy bronze.yaml entry for it."
                )
        elif dep_layer == "silver":
            if dep_name not in pack.silver:
                raise MissingDependencyError(
                    f"Gold consumer {consumer!r} depends on silver "
                    f"{dep_name!r}, but that name is not in the pack's "
                    f"silver layer."
                )

    def _layer_of(name: str) -> Literal["bronze", "silver", "gold"]:
        if name in classes:
            return classes[name][0]
        # Auto-included node — derive from the pack.
        if name in pack.silver:
            return "silver"
        if name in pack.gold:
            return "gold"
        return "bronze"

    # ------------------------------------------------------------------
    # 5. strict_scope=True: walk one level of deps from each root; any
    #    dep not in effective_roots → AIDPF-1042 (mirrors runtime).
    # ------------------------------------------------------------------
    if strict_scope:
        missing_deps: list[tuple[str, Literal["bronze", "silver"], str]] = []
        for root in sorted(effective_roots):
            root_layer = _layer_of(root)
            if root_layer == "bronze":
                continue
            b_deps, s_deps = _node_depends_on(pack, root_layer, root)
            for b in b_deps:
                _check_dep_exists_or_raise(b, "bronze", root)
                if b not in effective_roots:
                    missing_deps.append((root, "bronze", b))
            if root_layer == "gold":
                for s in s_deps:
                    _check_dep_exists_or_raise(s, "silver", root)
                    if s not in effective_roots:
                        missing_deps.append((root, "silver", s))
        if missing_deps:
            lines = [
                f"{AIDPF_1042_STRICT_SCOPE_MISSING_DEPENDENCY}: "
                f"--strict-scope requires every transitive dep be in the "
                f"effective root set; {len(missing_deps)} missing:"
            ]
            for root, dep_layer, dep_name in missing_deps:
                if dep_name in disabled_datasets:
                    lines.append(
                        f"  • {dep_layer} {dep_name!r} (required by "
                        f"{root!r}) is disabled in bundle.datasets — set "
                        f"`enabled: true` or drop {root!r} from "
                        f"--datasets / {_BUNDLE_SECTION[_layer_of(root)]}"
                    )
                else:
                    lines.append(
                        f"  • {dep_layer} {dep_name!r} (required by "
                        f"{root!r}) — add it to --datasets or "
                        f"{_BUNDLE_SECTION[dep_layer]}"
                    )
            raise MissingDependencyError("\n".join(lines))
        plan_ids: set[str] = set(effective_roots)
    else:
        # --------------------------------------------------------------
        # 6. strict_scope=False: D-1 transitive closure. Walk
        #    dependsOn.bronze ∪ dependsOn.silver from each root and
        #    add every reachable pack node to plan_ids (regardless of
        #    whether it was declared in the bundle, and regardless of
        #    --layers).
        # --------------------------------------------------------------
        plan_ids = set(effective_roots)
        frontier: list[str] = list(effective_roots)
        while frontier:
            current = frontier.pop()
            cur_layer = _layer_of(current)
            if cur_layer == "bronze":
                continue
            b_deps, s_deps = _node_depends_on(pack, cur_layer, current)
            for b in b_deps:
                _check_dep_exists_or_raise(b, "bronze", current)
                if b not in plan_ids:
                    plan_ids.add(b)
                    frontier.append(b)
            if cur_layer == "gold":
                for s in s_deps:
                    _check_dep_exists_or_raise(s, "silver", current)
                    if s not in plan_ids:
                        plan_ids.add(s)
                        frontier.append(s)

    # ------------------------------------------------------------------
    # 7. Topo-sort plan_ids: bronze first, then silver, then gold,
    #    with intra-layer dependency edges from the pack.
    # ------------------------------------------------------------------
    ts: TopologicalSorter[str] = TopologicalSorter()
    for name in plan_ids:
        node_layer = _layer_of(name)
        deps_in_plan: set[str] = set()
        if node_layer == "silver":
            b_deps, _ = _node_depends_on(pack, "silver", name)
            deps_in_plan.update(d for d in b_deps if d in plan_ids)
        elif node_layer == "gold":
            b_deps, s_deps = _node_depends_on(pack, "gold", name)
            deps_in_plan.update(d for d in b_deps if d in plan_ids)
            deps_in_plan.update(d for d in s_deps if d in plan_ids)
        ts.add(name, *deps_in_plan)

    ordered_names = list(ts.static_order())

    # COA-first ordering parity (feature: fail-fast-seed-validation). Mirror the
    # exec loop's `order_coa_source_first` so the previewed order matches what
    # runtime dispatches: hoist COA-source bronze (consumed by an in-scope
    # silver/gold) to the front. Applicability mirrors
    # `node_preflight.coa_applicable_sources`.
    #
    # The COA role filter lives in the neutral schema layer
    # (``schema.coa_roles``), NOT ``orchestrator.node_preflight`` — importing
    # the engine-side module here would pull orchestrator/* into sys.modules
    # and break the dispatch import boundary (see the module docstring +
    # ``tests/unit/dispatch/test_imports.py``).
    from .coa_roles import coa_role_aliases

    _coa_srcs = {src for (_role, src) in coa_role_aliases(pack).values()}
    if _coa_srcs:
        _applicable: set[str] = set()
        for name in plan_ids:
            nl = _layer_of(name)
            if nl in ("silver", "gold"):
                b_deps, _s = _node_depends_on(pack, nl, name)
                _applicable.update(b for b in b_deps if b in _coa_srcs)
        if _applicable:
            _coa_first = [
                n for n in ordered_names
                if _layer_of(n) == "bronze" and n in _applicable
            ]
            _rest = [
                n for n in ordered_names
                if not (_layer_of(n) == "bronze" and n in _applicable)
            ]
            ordered_names = _coa_first + _rest

    plan_nodes = tuple(
        PlanNode(
            dataset_id=name,
            layer=_layer_of(name),
            status="eligible",
            reason=None,
        )
        for name in ordered_names
    )
    # Mark `paths` as referenced so static checkers / linters don't
    # flag it — kept in the signature for back-compat with callers
    # that wire TablePaths through even though the new plan-only
    # contract no longer needs them for prereq rendering.
    _ = paths
    return plan_nodes, ()


__all__ = ["resolve_dry_run_plan"]
