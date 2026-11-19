"""Content-pack DAG plan resolver.

Walks ``pack.bronze`` ∪ ``pack.silver`` ∪ ``pack.gold``. Bronze is a
first-class layer alongside silver/gold. When the operator declares a
high-level node (e.g. ``supplier_spend`` gold), the resolver
auto-includes the transitive bronze + silver dependencies needed to
materialize it. ``--strict-scope`` opts out of that auto-include.

The resolver enforces medallion correctness for dependency closure and the
multi-source primary/lookup contract.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..schema.medallion_pack import NodeYaml

    from .content_pack import ResolvedPack


# ---------------------------------------------------------------------------
# AIDPF error codes
# ---------------------------------------------------------------------------

AIDPF_1034_UNKNOWN_DATASET_FILTER = "AIDPF-1034"
"""Content-pack ``--datasets`` references a node id not in the pack."""

AIDPF_1042_STRICT_SCOPE_MISSING_DEPENDENCY = "AIDPF-1042"
"""``--strict-scope`` set and the declared roots have transitive deps
not in the operator's id set."""

AIDPF_1043_CLI_DATASET_OUTSIDE_BUNDLE_SCOPE = "AIDPF-1043"
"""CLI ``--datasets`` includes an id outside ``bundle.datasets[]`` scope."""

AIDPF_1045_LAYER_FILTER_EMPTIED_PLAN = "AIDPF-1045"
"""``--layers`` filter removed every declared root; plan would be empty."""


class UnknownDatasetFilterError(Exception):
    """`--datasets <id>` references a node id absent from the content pack."""


class StrictScopeMissingDependencyError(Exception):
    """``--strict-scope`` set and required deps not in declared roots."""


class CliDatasetOutsideBundleScopeError(Exception):
    """``--datasets`` smuggles in ids absent from ``bundle.datasets[]``."""


class LayerFilterEmptiedPlanError(Exception):
    """``--layers`` filter removed every declared root — plan would be empty."""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def resolve_content_pack_plan(
    pack: "ResolvedPack",
    *,
    datasets: list[str] | None = None,
    layers: list[str] | None = None,
    strict_scope: bool = False,
    bundle_scope: set[str] | None = None,
) -> list["NodeYaml"]:
    """Build a topologically-ordered list of nodes to execute.

    * ``pack.bronze`` is a first-class layer (alongside silver/gold).
    * The resolver computes ``effective_roots`` from
      ``cli_datasets`` ∩ ``bundle_scope`` (when ``bundle_scope`` is
      given); otherwise from ``cli_datasets`` ∪ ``bundle_scope`` ∪
      ``all_nodes``.
    * Implicit transitive include: walks ``dependsOn.bronze`` /
      ``dependsOn.silver`` to auto-include deps unless
      ``strict_scope`` is set.
    * ``layers`` filters declared roots; transitive deps remain.
    * Topological sort: bronze first, then silver, then gold.

    Args:
        pack: assembled ResolvedPack from :func:`load_full_chain`.
        datasets: optional CLI ``--datasets`` filter list. Each id
            must be one of the content-pack node ids; unknown ids
            raise :class:`UnknownDatasetFilterError` with AIDPF-1034.
            When ``bundle_scope`` is given, every id in this list
            must also appear in ``bundle_scope`` (or it raises
            ``AIDPF-1043``).
        layers: optional list of layer names to filter to (``bronze``
            / ``silver`` / ``gold``). Other values are silently
            ignored. Applies to declared ROOTS only — D-1 transitive
            deps remain regardless of layer.
        strict_scope: if True, transitive deps are NOT auto-included;
            a declared root whose deps aren't ALSO declared raises
            :class:`StrictScopeMissingDependencyError` (AIDPF-1042).
        bundle_scope: optional ``set[str]`` of dataset ids declared
            in ``bundle.yaml::datasets[]``. When given, narrows the
            resolver's universe — ``cli_datasets`` must be a subset.

    Returns:
        List of ``NodeYaml`` objects in dependency order.

    Raises:
        UnknownDatasetFilterError: AIDPF-1034.
        StrictScopeMissingDependencyError: AIDPF-1042.
        CliDatasetOutsideBundleScopeError: AIDPF-1043.
        LayerFilterEmptiedPlanError: AIDPF-1045.
    """
    # Build the candidate node universe — bronze + silver + gold.
    all_nodes: dict[str, "NodeYaml"] = {}
    for node_id, node in pack.bronze.items():
        all_nodes[node_id] = node
    for node_id, node in pack.silver.items():
        all_nodes[node_id] = node
    for node_id, node in pack.gold.items():
        all_nodes[node_id] = node

    # CLI dataset filter validation.
    if datasets is not None:
        unknown = [d for d in datasets if d not in all_nodes]
        if unknown:
            raise UnknownDatasetFilterError(
                f"{AIDPF_1034_UNKNOWN_DATASET_FILTER}: --datasets references "
                f"node id(s) not in content pack: {unknown!r}. Available: "
                f"{sorted(all_nodes.keys())!r}."
            )
        if bundle_scope is not None:
            outside = [d for d in datasets if d not in bundle_scope]
            if outside:
                raise CliDatasetOutsideBundleScopeError(
                    f"{AIDPF_1043_CLI_DATASET_OUTSIDE_BUNDLE_SCOPE}: "
                    f"--datasets includes id(s) not in bundle.datasets[] "
                    f"scope: {outside!r}. Bundle scope: "
                    f"{sorted(bundle_scope)!r}."
                )

    # effective_roots: CLI > bundle_scope > all_nodes.
    if datasets is not None:
        effective_roots = set(datasets)
    elif bundle_scope is not None:
        effective_roots = set(bundle_scope) & set(all_nodes.keys())
    else:
        effective_roots = set(all_nodes.keys())

    # Layer filter applies to declared roots only.
    if layers is not None:
        layer_set = {l.strip().lower() for l in layers}
        effective_roots = {
            r for r in effective_roots
            if r in all_nodes and all_nodes[r].layer in layer_set
        }
        if not effective_roots:
            raise LayerFilterEmptiedPlanError(
                f"{AIDPF_1045_LAYER_FILTER_EMPTIED_PLAN}: --layers "
                f"{sorted(layer_set)!r} removed every declared root; "
                f"plan would be empty. Specify roots whose layer is in "
                f"the filter, or drop --layers."
            )

    # Transitively walk dependsOn.bronze + dependsOn.silver to build
    # the executable closure. strict_scope disables auto-include.
    if strict_scope:
        missing_deps: list[tuple[str, str]] = []
        for root in effective_roots:
            node = all_nodes[root]
            deps = getattr(node, "depends_on", None)
            if deps:
                for src in list(getattr(deps, "bronze", []) or []) + list(
                    getattr(deps, "silver", []) or []
                ):
                    if src.id not in effective_roots and src.id in all_nodes:
                        missing_deps.append((root, src.id))
        if missing_deps:
            details = ", ".join(f"{r}→{d}" for r, d in missing_deps)
            raise StrictScopeMissingDependencyError(
                f"{AIDPF_1042_STRICT_SCOPE_MISSING_DEPENDENCY}: --strict-scope "
                f"requires every transitive dep be declared; missing: {details}"
            )
        plan_ids = set(effective_roots)
    else:
        plan_ids = set(effective_roots)
        frontier = list(effective_roots)
        while frontier:
            current = frontier.pop()
            node = all_nodes.get(current)
            if node is None:
                continue
            deps = getattr(node, "depends_on", None)
            if not deps:
                continue
            for src in list(getattr(deps, "bronze", []) or []) + list(
                getattr(deps, "silver", []) or []
            ):
                if src.id in all_nodes and src.id not in plan_ids:
                    plan_ids.add(src.id)
                    frontier.append(src.id)

    # Topological sort: bronze first, then silver, then gold, with
    # intra-layer dependency ordering via DFS post-order.
    in_plan = {nid: all_nodes[nid] for nid in plan_ids if nid in all_nodes}
    ordered: list["NodeYaml"] = []
    visited: set[str] = set()
    in_progress: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in visited:
            return
        if node_id in in_progress:
            raise ValueError(
                f"resolve_content_pack_plan: dependency cycle at {node_id!r}."
            )
        in_progress.add(node_id)
        node = in_plan.get(node_id)
        if node is not None:
            deps = getattr(node, "depends_on", None)
            if deps:
                for src in list(getattr(deps, "bronze", []) or []) + list(
                    getattr(deps, "silver", []) or []
                ):
                    if src.id in in_plan:
                        visit(src.id)
            ordered.append(node)
        in_progress.remove(node_id)
        visited.add(node_id)

    # Layer-priority outer loop ensures bronze-before-silver-before-gold.
    for layer_priority in ("bronze", "silver", "gold"):
        for node_id in sorted(in_plan.keys()):
            if in_plan[node_id].layer == layer_priority:
                visit(node_id)

    return ordered


__all__ = [
    "AIDPF_1034_UNKNOWN_DATASET_FILTER",
    "AIDPF_1042_STRICT_SCOPE_MISSING_DEPENDENCY",
    "AIDPF_1043_CLI_DATASET_OUTSIDE_BUNDLE_SCOPE",
    "AIDPF_1045_LAYER_FILTER_EMPTIED_PLAN",
    "UnknownDatasetFilterError",
    "StrictScopeMissingDependencyError",
    "CliDatasetOutsideBundleScopeError",
    "LayerFilterEmptiedPlanError",
    "resolve_content_pack_plan",
]
