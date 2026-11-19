"""Static content validators for content packs.

Distinct from the Pydantic schema validation in :mod:`schema.medallion_pack`:
these validators need access to the filesystem (SQL files), the assembled
pack after overlay merge, and cross-references between packs and dashboards.
Operator-facing behavior is documented in ``docs/content_pack_execution.md``.

Validators implemented (one error code per failure mode):

    * :func:`validate_sql_paths` → AIDPF-2003
    * :func:`validate_template_variables` → AIDPF-5002, AIDPF-5003
    * :func:`validate_dag` → AIDPF-2040, AIDPF-2041
    * :func:`validate_column_contracts` → AIDPF-2045
    * :func:`validate_dashboard_requires` → AIDPF-7001, AIDPF-7003

:func:`validate_pack_full` aggregates the above into a single
:class:`ValidationReport`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Iterable

from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import ResolvedPack
from oracle_ai_data_platform_fusion_autopilot.orchestrator.required_column_resolver import (
    resolve_required_column_entries,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_references import (
    extract_upstream_reads,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.spark_types import (
    _normalise_spark_type,
)
from oracle_ai_data_platform_fusion_autopilot.schema.dashboard_pack import DashboardYaml

if TYPE_CHECKING:
    from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import NodeYaml
    from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import TenantProfile

# Error codes surfaced by content-pack validation.
AIDPF_2003_SQL_FILE_MISSING = "AIDPF-2003"
AIDPF_2005_RESERVED_NODE_ID = "AIDPF-2005"
"""A real content-pack node id begins with the reserved ``__`` prefix. That
namespace is reserved for synthetic state rows (``__run_manifest__``,
``__coa_gate__``, ``__bronze_readiness_gate__``, ``__fusion_pvo_drift_gate__``,
``__fingerprint_skip__``); a real node colliding with it would be dropped from
resume scope reconstruction or masquerade as a synthetic row. Fail closed."""
AIDPF_2040_DAG_CYCLE = "AIDPF-2040"
AIDPF_2041_UNRESOLVED_DEPENDENCY = "AIDPF-2041"
AIDPF_2045_COLUMN_CONTRACT_MISMATCH = "AIDPF-2045"
AIDPF_2084_UNDECLARED_INPUT = "AIDPF-2084"
"""A silver/gold SQL reads an upstream column not declared in its
``requiredColumns`` — including a `SELECT *` / `<alias>.*` wildcard from a
declared upstream, which is unverifiable and therefore fails closed. The
declared-inputs companion to AIDPF-2045 (SQL reads ⊆ requiredColumns)."""
AIDPF_2085_UNQUALIFIED_UPSTREAM_COLUMN = "AIDPF-2085"
"""WARN-only: a bare (unqualified) identifier in a block with an upstream source
matches that upstream's `outputSchema`. Qualify it with the table alias so the
declared-inputs gate can verify it. Warn (not error) because a bare name may be
CTE-derived."""
"""A silver/gold node demands a column that is missing from -- or
type-incompatible with -- an upstream node's declared `outputSchema`. A
design-time, source-independent producer/consumer contract gate (no live PVO):
the runtime AIDPF-4070/4071 gates compare the contract against live Fusion; this
compares declared consumer-demand against the declared upstream contract."""
AIDPF_5002_UNKNOWN_TEMPLATE_VAR = "AIDPF-5002"
AIDPF_5003_UNDECLARED_VARIATION_POINT = "AIDPF-5003"
AIDPF_5010_POST_RENDER_REJECTED = "AIDPF-5010"
"""Design-time surface of the renderer's post-render rejection: a TEMPLATE
containing a comment marker (``--`` / ``/*``) or ``;`` would fail every
render with AIDPF-5010 cluster-side — surface it offline instead
(live-observed render_failed on an overlay template, 2026-08-06)."""
AIDPF_7001_DASHBOARD_MISSING_NODE = "AIDPF-7001"
AIDPF_7003_DASHBOARD_TYPE_MISMATCH = "AIDPF-7003"
AIDPF_7004_DASHBOARD_PACK_INCOMPATIBLE = "AIDPF-7004"
AIDPF_7005_ALLOWED_COLUMNS_NOT_REQUIRED = "AIDPF-7005"
AIDPF_8002_PII_HIGH_DASHBOARD_EXPOSURE = "AIDPF-8002"

# Bronze extract nodes are validated against the Fusion catalog when possible.
AIDPF_2080_BRONZE_EXTRACT_PVO_NOT_IN_CATALOG = "AIDPF-2080"

# COA semantic-role validation (feature coa-role-segment-resolution).
AIDPF_2014_COA_ROLE_AS_EXISTENCE_ALIAS = "AIDPF-2014"
"""A known COA role is modeled as a bare column-existence alias (no
`resolution: semanticRole`) -- the existence-auto-match anti-pattern."""
AIDPF_2015_COA_BINDING_OUT_OF_CONTRACT = "AIDPF-2015"
"""A COA role candidate / chartOfAccounts mapping names a column the gl_coa
bronze `outputSchema` does not guarantee."""

AIDPF_2019_COA_SEGMENT_OUT_OF_RANGE = "AIDPF-2019"
"""A COA role candidate is not a ``CodeCombinationSegment<N>`` with N in 1..30
(Fusion's GL key-flexfield max). Catches typos like Segment31 / a non-segment
column before the contract check."""

# Fusion GL key-flexfield supports up to 30 segments. A COA role may only bind
# CodeCombinationSegment1..CodeCombinationSegment30. Canonical definition now
# lives in the neutral schema layer (shared with the metadata-arm derivation);
# re-exported here under the module-private name this file already uses.
from ..schema.coa_roles import COA_SEGMENT_RE as _COA_SEGMENT_RE  # noqa: E402

# Pack alias names conventionally used for COA roles -- a single-candidate
# existence alias on one of these without `resolution: semanticRole` is the
# anti-pattern the feature exists to prevent.
_COA_ROLE_ALIAS_NAMES = {
    "coa_balancing_segment",
    "coa_cost_center_segment",
    "coa_natural_account_segment",
}


# ---------------------------------------------------------------------------
# Validation report dataclasses
# ---------------------------------------------------------------------------


@dataclass
class ValidationError:
    code: str
    message: str
    location: str | None = None  # e.g., "silver/dim_supplier" or "dashboard/executive_cfo"


@dataclass
class ValidationReport:
    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationError] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def extend(self, other: "ValidationReport") -> None:
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)

    def merge_errors(self, errors: Iterable[ValidationError]) -> None:
        self.errors.extend(errors)


# ---------------------------------------------------------------------------
# Allowlisted SQL template variables.
# ---------------------------------------------------------------------------

_BASE_TEMPLATE_VARS = {
    "catalog",
    "bronze_schema",
    "silver_schema",
    "gold_schema",
    "run_id_literal",
    "watermark_predicate",
    "snapshot_date",  # Dedicated ISO-date token (AIDPF-5013).
}

# `{{ profile.<key> }}` / `{{ column.<name> }}` / `{{ semantic.<name> }}`
# are parsed and the suffix is validated against pack content.
_NAMESPACED_PREFIXES = ("profile", "column", "semantic")

_TEMPLATE_TOKEN_RE = re.compile(r"\{\{\s*([^}\s]+(?:\.[^}\s]+)*)\s*\}\}")


# ---------------------------------------------------------------------------
# validate_sql_paths (AIDPF-2003)
# ---------------------------------------------------------------------------


def validate_sql_paths(pack: ResolvedPack) -> list[ValidationError]:
    """For every node with `implementation.type: sql`, confirm the SQL file exists.

    Uses ``pack.root_for(qualified_id)`` so inherited base nodes resolve against
    the base pack's root (not the overlay's), and overlay-overridden / overlay-
    added nodes resolve against the overlay's root.
    """
    errors: list[ValidationError] = []
    for layer_name, nodes in (("silver", pack.silver), ("gold", pack.gold)):
        for node_id, node in nodes.items():
            if node.implementation.type != "sql":
                continue
            qualified = f"{layer_name}/{node_id}"
            sql_path = pack.root_for(qualified) / node.implementation.sql
            if not sql_path.exists():
                errors.append(
                    ValidationError(
                        code=AIDPF_2003_SQL_FILE_MISSING,
                        message=(
                            f"{AIDPF_2003_SQL_FILE_MISSING}: node "
                            f"`{qualified}` declares "
                            f"`implementation.sql: {node.implementation.sql}` but "
                            f"file does not exist at {sql_path}."
                        ),
                        location=qualified,
                    )
                )
    return errors


# ---------------------------------------------------------------------------
# validate_template_variables (AIDPF-5002, AIDPF-5003)
# ---------------------------------------------------------------------------


def validate_template_variables(pack: ResolvedPack) -> list[ValidationError]:
    """Confirm every `{{ ... }}` token in pack SQL files is allowed and declared.

    Allowlisted tokens:
        * Bare names in `_BASE_TEMPLATE_VARS`.
        * `profile.<key>` — resolved against `pack.profiles[<active>].<key>`
          at render time. This validator checks only that the namespace exists
          in the pack because profile key depth can vary.
        * `column.<name>` — must match a declared `columnAliases.<name>`.
        * `semantic.<name>` — must match a declared `semanticVariants.<name>`.
    """
    errors: list[ValidationError] = []
    declared_columns = set(pack.pack.column_aliases)
    declared_semantics = set(pack.pack.semantic_variants)
    has_profiles = bool(pack.pack.profiles)

    for layer_name, nodes in (("silver", pack.silver), ("gold", pack.gold)):
        for node_id, node in nodes.items():
            if node.implementation.type != "sql":
                continue
            qualified = f"{layer_name}/{node_id}"
            sql_path = pack.root_for(qualified) / node.implementation.sql
            if not sql_path.exists():
                # validate_sql_paths surfaces the AIDPF-2003 error; skip token
                # scanning to avoid noisy duplicate errors.
                continue
            content = sql_path.read_text(encoding="utf-8")
            tokens = _TEMPLATE_TOKEN_RE.findall(content)
            for token in tokens:
                parts = token.split(".")
                head = parts[0]
                if head in _BASE_TEMPLATE_VARS and len(parts) == 1:
                    continue
                if head == "profile":
                    if not has_profiles:
                        errors.append(
                            ValidationError(
                                code=AIDPF_5003_UNDECLARED_VARIATION_POINT,
                                message=(
                                    f"{AIDPF_5003_UNDECLARED_VARIATION_POINT}: "
                                    f"node `{qualified}` references "
                                    f"`{{{{ {token} }}}}` but pack declares no profiles."
                                ),
                                location=qualified,
                            )
                        )
                    continue
                if head == "column":
                    name = parts[1] if len(parts) > 1 else ""
                    if name not in declared_columns:
                        errors.append(
                            ValidationError(
                                code=AIDPF_5003_UNDECLARED_VARIATION_POINT,
                                message=(
                                    f"{AIDPF_5003_UNDECLARED_VARIATION_POINT}: "
                                    f"node `{qualified}` references "
                                    f"`{{{{ {token} }}}}` but `columnAliases.{name}` "
                                    f"is not declared. Known: {sorted(declared_columns)!r}."
                                ),
                                location=qualified,
                            )
                        )
                    continue
                if head == "coa":
                    role = parts[1] if len(parts) > 1 else ""
                    if role not in {"balancing", "cost_center", "natural_account"}:
                        errors.append(
                            ValidationError(
                                code=AIDPF_5003_UNDECLARED_VARIATION_POINT,
                                message=(
                                    f"{AIDPF_5003_UNDECLARED_VARIATION_POINT}: node "
                                    f"`{qualified}` references `{{{{ {token} }}}}` but "
                                    f"`{role}` is not a known COA role. Known: "
                                    f"balancing, cost_center, natural_account."
                                ),
                                location=qualified,
                            )
                        )
                    continue
                if head == "semantic":
                    name = parts[1] if len(parts) > 1 else ""
                    if name not in declared_semantics:
                        errors.append(
                            ValidationError(
                                code=AIDPF_5003_UNDECLARED_VARIATION_POINT,
                                message=(
                                    f"{AIDPF_5003_UNDECLARED_VARIATION_POINT}: "
                                    f"node `{qualified}` references "
                                    f"`{{{{ {token} }}}}` but `semanticVariants.{name}` "
                                    f"is not declared. Known: {sorted(declared_semantics)!r}."
                                ),
                                location=qualified,
                            )
                        )
                    continue
                # Unknown top-level namespace.
                errors.append(
                    ValidationError(
                        code=AIDPF_5002_UNKNOWN_TEMPLATE_VAR,
                        message=(
                            f"{AIDPF_5002_UNKNOWN_TEMPLATE_VAR}: node "
                            f"`{qualified}` references unknown "
                            f"template variable `{{{{ {token} }}}}`. "
                            f"Allowed: {sorted(_BASE_TEMPLATE_VARS) + ['profile.<key>', 'column.<name>', 'semantic.<name>']}."
                        ),
                        location=qualified,
                    )
                )
    return errors


# ---------------------------------------------------------------------------
# validate_sql_template_hygiene (AIDPF-5010, design-time twin)
# ---------------------------------------------------------------------------


def validate_sql_template_hygiene(pack: ResolvedPack) -> list[ValidationError]:
    """Reject templates that can NEVER render: the renderer's post-render
    check (``sql_renderer._check_post_render``) rejects any rendered SQL
    containing a comment marker or ``;`` (comment-smuggling / multi-statement
    defense, AIDPF-5010). A marker present in the TEMPLATE itself therefore
    fails every render, but only cluster-side at run time — surface it here,
    offline, with the authoring remedy. Single-sourced from the renderer's
    ``_DISALLOWED_AFTER_RENDER`` list."""
    from .sql_renderer import _DISALLOWED_AFTER_RENDER

    forbidden = tuple(_DISALLOWED_AFTER_RENDER) + (";",)
    errors: list[ValidationError] = []
    for layer_name, nodes in (("silver", pack.silver), ("gold", pack.gold)):
        for node_id, node in nodes.items():
            if node.implementation.type != "sql":
                continue
            qualified = f"{layer_name}/{node_id}"
            sql_path = pack.root_for(qualified) / node.implementation.sql
            if not sql_path.exists():
                # validate_sql_paths surfaces AIDPF-2003; avoid duplicates.
                continue
            content = sql_path.read_text(encoding="utf-8")
            hits = [m for m in forbidden if m in content]
            if hits:
                errors.append(
                    ValidationError(
                        code=AIDPF_5010_POST_RENDER_REJECTED,
                        message=(
                            f"{AIDPF_5010_POST_RENDER_REJECTED}: SQL template for "
                            f"node `{qualified}` contains {hits!r} — the renderer "
                            f"rejects comment markers and `;` in RENDERED SQL "
                            f"(comment-smuggling / multi-statement defense), so "
                            f"this template can never render. Templates must be "
                            f"comment-free; put commentary in the node YAML."
                        ),
                        location=qualified,
                    )
                )
    return errors


# ---------------------------------------------------------------------------
# validate_dag (AIDPF-2040, AIDPF-2041)
# ---------------------------------------------------------------------------


def validate_dag(pack: ResolvedPack) -> list[ValidationError]:
    """Confirm node `dependsOn` references resolve and form a DAG."""
    errors: list[ValidationError] = []

    # Build the set of declared source ids:
    #   - bronze datasets from bronze.yaml
    #   - silver nodes by id
    declared_bronze: set[str] = set()
    # Per-file pack.bronze is the source of truth; legacy pack.bronze_yaml is
    # retained for backwards compatibility.
    declared_bronze.update(pack.bronze.keys())
    for ds in pack.bronze_yaml.get("datasets", []) or []:
        if isinstance(ds, dict) and "id" in ds:
            declared_bronze.add(ds["id"])
    declared_silver = set(pack.silver)
    declared_gold = set(pack.gold)

    # Build adjacency: node -> set(node ids it depends on, restricted to
    # silver/gold-shape nodes for cycle detection — bronze deps are leaves).
    graph: dict[str, set[str]] = {}

    all_nodes = {
        f"silver/{nid}": node for nid, node in pack.silver.items()
    } | {f"gold/{nid}": node for nid, node in pack.gold.items()}

    for full_id, node in all_nodes.items():
        deps: set[str] = set()

        for src in node.depends_on.bronze:
            if src.id not in declared_bronze:
                errors.append(
                    ValidationError(
                        code=AIDPF_2041_UNRESOLVED_DEPENDENCY,
                        message=(
                            f"{AIDPF_2041_UNRESOLVED_DEPENDENCY}: node "
                            f"`{full_id}` depends on bronze `{src.id}` which "
                            f"is not declared in bronze.yaml. Known bronze "
                            f"datasets: {sorted(declared_bronze)!r}."
                        ),
                        location=full_id,
                    )
                )
        for src in node.depends_on.silver:
            if src.id not in declared_silver:
                errors.append(
                    ValidationError(
                        code=AIDPF_2041_UNRESOLVED_DEPENDENCY,
                        message=(
                            f"{AIDPF_2041_UNRESOLVED_DEPENDENCY}: node "
                            f"`{full_id}` depends on silver `{src.id}` which "
                            f"is not a declared silver node. Known: "
                            f"{sorted(declared_silver)!r}."
                        ),
                        location=full_id,
                    )
                )
                continue
            # Map silver dependency id to its full qualified name.
            deps.add(f"silver/{src.id}")
        graph[full_id] = deps

    # Cycle detection via DFS.
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = {n: WHITE for n in graph}

    def dfs(node: str, path: list[str]) -> None:
        color[node] = GRAY
        path.append(node)
        for nxt in graph.get(node, set()):
            if color.get(nxt, WHITE) == GRAY:
                cycle_repr = " -> ".join(path[path.index(nxt):] + [nxt])
                errors.append(
                    ValidationError(
                        code=AIDPF_2040_DAG_CYCLE,
                        message=(
                            f"{AIDPF_2040_DAG_CYCLE}: dependency cycle in pack "
                            f"DAG: {cycle_repr}."
                        ),
                        location=node,
                    )
                )
                return
            if color.get(nxt, WHITE) == WHITE:
                dfs(nxt, path)
        path.pop()
        color[node] = BLACK

    for node in graph:
        if color[node] == WHITE:
            dfs(node, [])

    return errors


# ---------------------------------------------------------------------------
# validate_column_contracts (AIDPF-2045)
# ---------------------------------------------------------------------------


def _contract_columns(node: "NodeYaml | None") -> dict[str, tuple[str, str]]:
    """Map lowercased column name -> (original_name, declared_type) for a
    producer node's `outputSchema`, or ``{}`` when it has none."""
    if node is None or node.output_schema is None:
        return {}
    return {c.name.lower(): (c.name, c.type) for c in node.output_schema.columns}


def validate_column_contracts(
    pack: ResolvedPack, *, profile: "TenantProfile | None" = None
) -> list[ValidationError]:
    """Design-time producer/consumer column-contract consistency gate (AIDPF-2045).

    For every silver/gold node, resolve the columns it *demands* from each
    upstream source (declared ``requiredColumns`` + the incremental
    ``watermark.column`` against its ``watermark.source``) and assert each is
    **present** in — and, for pass-through columns, **type-compatible** with —
    that upstream node's declared ``outputSchema`` *contract*.

    Source-independent: no live PVO probe. ``$column.*`` / ``$coa.*`` demands
    are resolved via the shared resolver against ``profile``; when ``profile``
    is ``None`` those refs drop silently (literals + watermark still gate),
    matching :func:`required_column_resolver.resolve_required_column_entries`.

    Scope notes (see ``docs/features/bronze-contract-consumer-consistency``):

    * **Type** expectation is read from the *consumer's own* ``outputSchema``
      (``requiredColumns`` carries names only). A demanded column re-declared
      by the same case-insensitive name in the consumer's ``outputSchema`` is a
      pass-through and gets a type check; otherwise presence-only (inferring a
      renamed/derived column's type would need SQL parsing — out of scope).
    * ``naturalKey`` is intentionally **excluded** — it is the node's own merge
      key, not an upstream demand.
    * Edges whose upstream id is not a declared producer are skipped here;
      :func:`validate_dag` already reports them as AIDPF-2041.
    """
    errors: list[ValidationError] = []

    for layer_name, nodes in (("silver", pack.silver), ("gold", pack.gold)):
        for nid, node in nodes.items():
            full_id = f"{layer_name}/{nid}"
            consumer_types = {
                c.name.lower(): c.type
                for c in (node.output_schema.columns if node.output_schema else [])
            }

            # Resolve demand, attributed per upstream source id.
            demand: dict[str, set[str]] = {}
            for src_id, entries in (node.required_columns or {}).items():
                resolved = resolve_required_column_entries(
                    entries, resolved_pack=pack, tenant_profile=profile
                )
                if resolved:
                    demand.setdefault(src_id, set()).update(resolved)
            inc = node.refresh.incremental if node.refresh else None
            wm = inc.watermark if inc else None
            if wm is not None:
                demand.setdefault(wm.source, set()).add(wm.column)

            for src_id in sorted(demand):
                upstream = pack.bronze.get(src_id) or pack.silver.get(src_id)
                if upstream is None:
                    # Unknown producer — AIDPF-2041 (validate_dag) owns this.
                    continue
                contract = _contract_columns(upstream)
                if not contract:
                    continue
                up_layer = "bronze" if src_id in pack.bronze else "silver"
                known = sorted(orig for orig, _ in contract.values())
                for col in sorted(demand[src_id]):
                    entry = contract.get(col.lower())
                    if entry is None:
                        errors.append(
                            ValidationError(
                                code=AIDPF_2045_COLUMN_CONTRACT_MISMATCH,
                                message=(
                                    f"{AIDPF_2045_COLUMN_CONTRACT_MISMATCH}: node "
                                    f"`{full_id}` requires column `{col}` from "
                                    f"upstream `{up_layer}/{src_id}`, but it is not "
                                    f"in that node's declared outputSchema. Known "
                                    f"upstream columns: {known!r}. Extend the "
                                    f"upstream outputSchema or fix requiredColumns."
                                ),
                                location=full_id,
                            )
                        )
                        continue
                    # Pass-through type check: only when the consumer re-declares
                    # the same column name in its own outputSchema.
                    consumer_type = consumer_types.get(col.lower())
                    if consumer_type is None:
                        continue
                    _, contract_type = entry
                    if _normalise_spark_type(consumer_type) != _normalise_spark_type(
                        contract_type
                    ):
                        errors.append(
                            ValidationError(
                                code=AIDPF_2045_COLUMN_CONTRACT_MISMATCH,
                                message=(
                                    f"{AIDPF_2045_COLUMN_CONTRACT_MISMATCH}: node "
                                    f"`{full_id}` declares column `{col}` as type "
                                    f"`{consumer_type}`, but upstream "
                                    f"`{up_layer}/{src_id}` declares it as "
                                    f"`{contract_type}`. Align the declared types."
                                ),
                                location=full_id,
                            )
                        )

    return errors


# ---------------------------------------------------------------------------
# validate_declared_inputs (AIDPF-2084) + warnings (AIDPF-2085)
# ---------------------------------------------------------------------------


def _node_depends_on_ids(node: "NodeYaml") -> set[str]:
    """Declared upstream ids (bronze + silver) for a node."""
    deps: set[str] = set()
    dep = node.depends_on
    if dep is not None:
        deps.update(s.id for s in dep.bronze)
        deps.update(s.id for s in dep.silver)
    return deps


def _read_node_sql(pack: ResolvedPack, qualified: str, node: "NodeYaml") -> str | None:
    """Read a node's pre-render SQL, or ``None`` if it isn't a SQL node / missing."""
    if node.implementation.type != "sql":
        return None
    sql_path = pack.root_for(qualified) / node.implementation.sql
    if not sql_path.exists():
        return None  # validate_sql_paths owns the AIDPF-2003 error
    try:
        return sql_path.read_text(encoding="utf-8")
    except OSError:
        return None


def _symbol_satisfied(
    symbol: str,
    declared_raw: list[str],
    *,
    pack: ResolvedPack,
    profile: "TenantProfile | None",
) -> bool:
    """Is a SQL-read ``symbol`` declared in a source's raw ``requiredColumns``?

    Symbol-level (profile-INDEPENDENT) for the common cases:
    * ``$column.<key>`` / ``$coa.<role>`` → must appear verbatim in the raw list.
    * literal ``<Col>`` → case-insensitive literal match in the raw list.
    Cross-plane (profile-DEPENDENT, only when a profile is in scope): a literal
    read satisfied by a ``$column.*`` / ``$coa.*`` entry that resolves to it.
    """
    if symbol.startswith("$"):
        return symbol in declared_raw
    low = symbol.lower()
    if any(not e.startswith("$") and e.lower() == low for e in declared_raw):
        return True
    if profile is not None:
        resolved = resolve_required_column_entries(
            declared_raw, resolved_pack=pack, tenant_profile=profile
        )
        if any(r.lower() == low for r in resolved):
            return True
    return False


def validate_declared_inputs(
    pack: ResolvedPack, *, profile: "TenantProfile | None" = None
) -> list[ValidationError]:
    """Declared-inputs gate (AIDPF-2084): SQL reads ⊆ declared ``requiredColumns``.

    For each silver/gold SQL node, extract the upstream columns its SQL reads
    (conservative, block-scoped — see :mod:`orchestrator.sql_references`) and
    assert each is declared in the node's ``requiredColumns`` for that source.
    Matching is at the **author symbol level** (literal / ``$column.<key>`` /
    ``$coa.<role>``), so the gate is profile-independent and fires even on the
    profile-less run-start validation path; ``profile`` is used only for the
    cross-plane literal↔alias case. A ``SELECT *`` / ``<alias>.*`` read from a
    declared upstream is a hard error (unverifiable). Companion to AIDPF-2045.
    """
    errors: list[ValidationError] = []

    for layer_name, nodes in (("silver", pack.silver), ("gold", pack.gold)):
        for nid, node in nodes.items():
            qualified = f"{layer_name}/{nid}"
            sql = _read_node_sql(pack, qualified, node)
            if sql is None:
                continue
            deps = _node_depends_on_ids(node)
            if not deps:
                continue
            reads = extract_upstream_reads(sql, depends_on_ids=deps)
            req = node.required_columns or {}

            # Wildcard from a declared upstream → hard, unverifiable.
            for src_id in sorted(reads.wildcard_sources):
                up_layer = "bronze" if src_id in pack.bronze else "silver"
                errors.append(
                    ValidationError(
                        code=AIDPF_2084_UNDECLARED_INPUT,
                        message=(
                            f"{AIDPF_2084_UNDECLARED_INPUT}: node `{qualified}` reads "
                            f"`SELECT *` / `<alias>.*` from upstream "
                            f"`{up_layer}/{src_id}`, which cannot be proven declared. "
                            f"Project explicit alias-qualified columns and declare "
                            f"them in requiredColumns[{src_id}]."
                        ),
                        location=qualified,
                    )
                )

            # Attributed per-source demands → must be declared symbol-for-symbol.
            for src_id in sorted(reads.demands):
                declared_raw = list(req.get(src_id, []))
                up_layer = "bronze" if src_id in pack.bronze else "silver"
                for sym in sorted(reads.demands[src_id]):
                    if not _symbol_satisfied(
                        sym, declared_raw, pack=pack, profile=profile
                    ):
                        errors.append(
                            ValidationError(
                                code=AIDPF_2084_UNDECLARED_INPUT,
                                message=(
                                    f"{AIDPF_2084_UNDECLARED_INPUT}: node "
                                    f"`{qualified}` reads `{sym}` from upstream "
                                    f"`{up_layer}/{src_id}` but it is not declared in "
                                    f"requiredColumns[{src_id}] "
                                    f"(declared: {sorted(declared_raw)!r}). Add it."
                                ),
                                location=qualified,
                            )
                        )

            # Role-like reads (standalone `{{ coa.<role> }}` / `{{ semantic.<key> }}`)
            # must be declared on the SOURCE the token is read from — the extractor
            # attributes each to the direct upstream(s) of its block (or, for a
            # derived-block token, all referenced upstreams). We check
            # `req[<that source>]` only — not the union of every requiredColumns key
            # — so a `$coa.<role>` / `$semantic.<key>` declared under a
            # non-dependency or an unrelated upstream cannot spuriously satisfy the
            # read. Candidates are intersected with the declared dependencies.
            for role_sym in sorted(reads.role_sources):
                cands = reads.role_sources[role_sym] & deps
                if not any(role_sym in (req.get(src) or []) for src in cands):
                    errors.append(
                        ValidationError(
                            code=AIDPF_2084_UNDECLARED_INPUT,
                            message=(
                                f"{AIDPF_2084_UNDECLARED_INPUT}: node `{qualified}` "
                                f"reads `{role_sym}` (via a `{{{{ … }}}}` token) but it "
                                f"is not declared in the requiredColumns of the source "
                                f"it reads from (candidate sources: {sorted(cands)!r}). "
                                f"Add `{role_sym}` to that source's requiredColumns."
                            ),
                            location=qualified,
                        )
                    )

    return errors


def collect_declared_input_warnings(pack: ResolvedPack) -> list[ValidationError]:
    """Warn-only (AIDPF-2085): bare unqualified identifiers that match an upstream
    ``outputSchema`` column — they should be alias-qualified so the declared-inputs
    gate can verify them. Profile-agnostic: matches physical names against
    physical ``outputSchema`` columns; no token resolution needed.
    """
    warnings: list[ValidationError] = []
    for layer_name, nodes in (("silver", pack.silver), ("gold", pack.gold)):
        for nid, node in nodes.items():
            qualified = f"{layer_name}/{nid}"
            sql = _read_node_sql(pack, qualified, node)
            if sql is None:
                continue
            deps = _node_depends_on_ids(node)
            if not deps:
                continue
            reads = extract_upstream_reads(sql, depends_on_ids=deps)
            if not reads.bare_identifiers:
                continue
            # Union of all declared upstreams' outputSchema column names (lower).
            upstream_cols: set[str] = set()
            for src_id in deps:
                upstream = pack.bronze.get(src_id) or pack.silver.get(src_id)
                upstream_cols |= set(_contract_columns(upstream))  # lc names
            for ident in sorted(reads.bare_identifiers):
                if ident.lower() in upstream_cols:
                    warnings.append(
                        ValidationError(
                            code=AIDPF_2085_UNQUALIFIED_UPSTREAM_COLUMN,
                            message=(
                                f"{AIDPF_2085_UNQUALIFIED_UPSTREAM_COLUMN}: node "
                                f"`{qualified}` reads bare column `{ident}` which "
                                f"matches an upstream `outputSchema`. Qualify it with "
                                f"its table alias so declared-inputs (AIDPF-2084) can "
                                f"verify it."
                            ),
                            location=qualified,
                        )
                    )
    return warnings


# ---------------------------------------------------------------------------
# validate_dashboard_requires (AIDPF-7001, AIDPF-7003)
# ---------------------------------------------------------------------------


def validate_dashboard_requires(
    pack: ResolvedPack, dashboard: DashboardYaml
) -> list[ValidationError]:
    """Confirm dashboard requires.{tables,columns} resolve against pack gold nodes."""
    errors: list[ValidationError] = []

    # Build a lookup from `gold.<table>` -> gold node.
    gold_by_target = {f"gold.{node.target}": node for node in pack.gold.values()}

    for table_ref in dashboard.requires.tables:
        if table_ref not in gold_by_target:
            errors.append(
                ValidationError(
                    code=AIDPF_7001_DASHBOARD_MISSING_NODE,
                    message=(
                        f"{AIDPF_7001_DASHBOARD_MISSING_NODE}: dashboard "
                        f"`{dashboard.id}` requires `{table_ref}` which is not "
                        f"declared as a gold node in the pack. Known gold "
                        f"tables: {sorted(gold_by_target)!r}."
                    ),
                    location=f"dashboard/{dashboard.id}",
                )
            )

    # Tables that appear in requires.tables were checked in the loop above.
    # Tables that appear in requires.columns must ALSO resolve to a gold node;
    # a typo present only in requires.columns (e.g. `gold.gl_balnace` instead
    # of `gold.gl_balance`) would otherwise slip through silently.
    required_tables_set = set(dashboard.requires.tables)
    for table_ref, columns in dashboard.requires.columns.items():
        if table_ref not in gold_by_target:
            # Distinguish two flavours of failure for clearer remediation:
            #   * Already reported via requires.tables loop above → silent skip
            #     would be acceptable, but we still want the dashboard author
            #     to see one error per failing table so we report it again here.
            #   * Table only in requires.columns (typo / forgot requires.tables):
            #     must surface explicitly; otherwise the rest of the column
            #     checks and the PII firewall skip the table altogether.
            already_reported = table_ref in required_tables_set
            extra_hint = (
                "" if already_reported else
                " (this table is referenced only by `requires.columns`; "
                "every column-table key must also appear in `requires.tables` "
                "AND resolve to a declared gold node)"
            )
            errors.append(
                ValidationError(
                    code=AIDPF_7001_DASHBOARD_MISSING_NODE,
                    message=(
                        f"{AIDPF_7001_DASHBOARD_MISSING_NODE}: dashboard "
                        f"`{dashboard.id}` references `{table_ref}` in "
                        f"`requires.columns` which is not declared as a gold "
                        f"node in the pack{extra_hint}. Known gold tables: "
                        f"{sorted(gold_by_target)!r}."
                    ),
                    location=f"dashboard/{dashboard.id}",
                )
            )
            continue
        node = gold_by_target[table_ref]
        node_columns_by_name = {c.name: c for c in node.output_schema.columns}
        for required_col in columns:
            if required_col.name not in node_columns_by_name:
                errors.append(
                    ValidationError(
                        code=AIDPF_7001_DASHBOARD_MISSING_NODE,
                        message=(
                            f"{AIDPF_7001_DASHBOARD_MISSING_NODE}: dashboard "
                            f"`{dashboard.id}` requires column `{required_col.name}` "
                            f"on `{table_ref}` which is not in the gold "
                            f"node's `outputSchema.columns`."
                        ),
                        location=f"dashboard/{dashboard.id}",
                    )
                )
                continue
            actual = node_columns_by_name[required_col.name]
            if actual.type != required_col.type:
                errors.append(
                    ValidationError(
                        code=AIDPF_7003_DASHBOARD_TYPE_MISMATCH,
                        message=(
                            f"{AIDPF_7003_DASHBOARD_TYPE_MISMATCH}: dashboard "
                            f"`{dashboard.id}` requires `{table_ref}.{required_col.name}: "
                            f"{required_col.type}` but the gold node declares "
                            f"`{actual.type}`."
                        ),
                        location=f"dashboard/{dashboard.id}",
                    )
                )

    return errors


# ---------------------------------------------------------------------------
# validate_dashboard_security_and_compat (AIDPF-7004, AIDPF-7005, AIDPF-8002)
# ---------------------------------------------------------------------------


def _semver_tuple(v: str) -> tuple[int, ...]:
    """Best-effort SemVer to comparable tuple. Ignores pre-release / build."""
    core = v.split("-")[0].split("+")[0]
    try:
        return tuple(int(p) for p in core.split("."))
    except ValueError:
        return ()


def validate_dashboard_security_and_compat(
    pack: ResolvedPack, dashboard: DashboardYaml
) -> list[ValidationError]:
    """Pack-version compatibility + PII firewall + allowedColumns subset check.

    Three rules:

    * **AIDPF-7004** — ``requires.pack.id`` must equal ``pack.pack.id``; if
      ``requires.pack.minVersion`` is set, ``pack.pack.version`` must be
      >= it; if ``requires.pack.maxVersion`` is set, ``pack.pack.version``
      must be <= it.
    * **AIDPF-7005** — every entry in ``security.allowedColumns[table]``
      must already appear in ``requires.columns[table]`` for the same
      table. Prevents "I allow X for display but never required it" drift.
    * **AIDPF-8002** — any column in ``requires.columns`` OR
      ``security.allowedColumns`` whose gold ``outputSchema`` declares
      ``pii: high`` is rejected. High-PII columns must not be reachable
      via OAC dataset/RPD.
    """
    errors: list[ValidationError] = []
    where = f"dashboard/{dashboard.id}"

    # --- AIDPF-7004: pack compatibility ----------------------------------
    req_pack = dashboard.requires.pack
    if req_pack.id != pack.pack.id:
        errors.append(
            ValidationError(
                code=AIDPF_7004_DASHBOARD_PACK_INCOMPATIBLE,
                message=(
                    f"{AIDPF_7004_DASHBOARD_PACK_INCOMPATIBLE}: dashboard "
                    f"`{dashboard.id}` requires pack `{req_pack.id}` but "
                    f"active pack is `{pack.pack.id}`."
                ),
                location=where,
            )
        )
    else:
        pack_v = _semver_tuple(pack.pack.version)
        min_v = _semver_tuple(req_pack.min_version) if req_pack.min_version else None
        max_v = _semver_tuple(req_pack.max_version) if req_pack.max_version else None
        if min_v and pack_v and pack_v < min_v:
            errors.append(
                ValidationError(
                    code=AIDPF_7004_DASHBOARD_PACK_INCOMPATIBLE,
                    message=(
                        f"{AIDPF_7004_DASHBOARD_PACK_INCOMPATIBLE}: dashboard "
                        f"`{dashboard.id}` requires pack `{req_pack.id}` "
                        f">= {req_pack.min_version} but active pack is "
                        f"{pack.pack.version}."
                    ),
                    location=where,
                )
            )
        if max_v and pack_v and pack_v > max_v:
            errors.append(
                ValidationError(
                    code=AIDPF_7004_DASHBOARD_PACK_INCOMPATIBLE,
                    message=(
                        f"{AIDPF_7004_DASHBOARD_PACK_INCOMPATIBLE}: dashboard "
                        f"`{dashboard.id}` requires pack `{req_pack.id}` "
                        f"<= {req_pack.max_version} but active pack is "
                        f"{pack.pack.version}."
                    ),
                    location=where,
                )
            )

    # Look up gold nodes keyed by qualified `gold.<target>` for column ↔ PII checks.
    gold_by_target = {f"gold.{node.target}": node for node in pack.gold.values()}

    # --- AIDPF-7005: allowed_columns ⊆ requires.columns ------------------
    required_cols_by_table: dict[str, set[str]] = {
        table: {c.name for c in cols}
        for table, cols in dashboard.requires.columns.items()
    }
    for table, allowed_names in dashboard.security.allowed_columns.items():
        required_set = required_cols_by_table.get(table, set())
        unrequired = [name for name in allowed_names if name not in required_set]
        if unrequired:
            errors.append(
                ValidationError(
                    code=AIDPF_7005_ALLOWED_COLUMNS_NOT_REQUIRED,
                    message=(
                        f"{AIDPF_7005_ALLOWED_COLUMNS_NOT_REQUIRED}: dashboard "
                        f"`{dashboard.id}` declares `allowedColumns[{table}]` "
                        f"entries that are not present in `requires.columns[{table}]`: "
                        f"{sorted(unrequired)!r}."
                    ),
                    location=where,
                )
            )

    # --- AIDPF-8002: PII high firewall -----------------------------------
    # Collect every (table, column) pair the dashboard references via
    # requires.columns OR security.allowedColumns, then check the gold
    # node's outputSchema for pii=='high' on each.
    references: dict[str, set[str]] = {}
    for table, cols in dashboard.requires.columns.items():
        references.setdefault(table, set()).update(c.name for c in cols)
    for table, allowed_names in dashboard.security.allowed_columns.items():
        references.setdefault(table, set()).update(allowed_names)

    for table, col_names in references.items():
        node = gold_by_target.get(table)
        if node is None:
            # validate_dashboard_requires reports the missing-table error;
            # don't surface a duplicate PII complaint here.
            continue
        cols_by_name = {c.name: c for c in node.output_schema.columns}
        for col_name in col_names:
            col = cols_by_name.get(col_name)
            if col is None:
                # Missing column already reported by validate_dashboard_requires.
                continue
            if col.pii == "high":
                errors.append(
                    ValidationError(
                        code=AIDPF_8002_PII_HIGH_DASHBOARD_EXPOSURE,
                        message=(
                            f"{AIDPF_8002_PII_HIGH_DASHBOARD_EXPOSURE}: dashboard "
                            f"`{dashboard.id}` references `{table}.{col_name}` "
                            f"which is declared `pii: high` in the gold node's "
                            f"`outputSchema`. High-PII columns must not be "
                            f"reachable via OAC dataset/RPD. "
                            f"Remove from `requires.columns` / `allowedColumns` "
                            f"or downgrade the column's pii classification."
                        ),
                        location=where,
                    )
                )

    return errors


# ---------------------------------------------------------------------------
# validate_pack_full
# ---------------------------------------------------------------------------


def validate_bronze_pvo_catalog(pack: ResolvedPack) -> list[ValidationError]:
    """WARN when a bronze_extract node's ``pvo_id`` is not in the catalog.

    WARN-only: pack loads cleanly; the BICC drift gate (``AIDPF-2072``)
    catches typo'd PVOs at extract-preflight time. This preserves the customer
    extension story: customers can author overlay-pack YAMLs for new PVOs
    without a plugin release.

    Missing ``pvo_id`` entirely produces NO WARN — there is nothing to
    cross-reference.
    """
    from ..schema.fusion_catalog import CATALOG

    warnings: list[ValidationError] = []
    curated_pvo_ids = {entry.datastore for entry in CATALOG.values()}
    for node_id, node in pack.bronze.items():
        impl = node.implementation
        if impl.type != "bronze_extract":
            continue
        pvo_id = getattr(impl, "pvo_id", None)
        if pvo_id is None:
            continue
        # Cross-reference against either the curated PvoEntry.datastore
        # (full AM-hierarchy) or the curated id keys themselves.
        if pvo_id in curated_pvo_ids or pvo_id in CATALOG:
            continue
        warnings.append(
            ValidationError(
                code=AIDPF_2080_BRONZE_EXTRACT_PVO_NOT_IN_CATALOG,
                message=(
                    f"{AIDPF_2080_BRONZE_EXTRACT_PVO_NOT_IN_CATALOG}: bronze "
                    f"node `bronze/{node_id}` references pvo_id "
                    f"{pvo_id!r} which is not in the curated fusion_catalog. "
                    f"Pack loads cleanly; the BICC drift gate "
                    f"(AIDPF-2072) catches typos at extract-preflight time. "
                    f"Customer overlay packs commonly hit this WARN."
                ),
                location=f"bronze/{node_id}",
            )
        )
    return warnings


def _gl_coa_contract_columns(pack: ResolvedPack) -> set[str] | None:
    """Lowercased column names the gl_coa bronze `outputSchema` guarantees, or
    None when there is no gl_coa node (so the check no-ops for other packs)."""
    node = pack.bronze.get("gl_coa")
    if node is None or node.output_schema is None:
        return None
    return {c.name.lower() for c in node.output_schema.columns}


def validate_coa_semantic_roles(pack: ResolvedPack) -> list[ValidationError]:
    """COA semantic-role guards (AIDPF-2014, AIDPF-2015).

    * **AIDPF-2014** -- a known COA role alias modeled as a bare
      column-existence alias (no `resolution: semanticRole`) is rejected: a
      business role must not be resolved by column existence.
    * **AIDPF-2015** -- a `semanticRole` COA candidate (its allowed domain) or
      a `profiles.<p>.chartOfAccounts` mapping that names a column the gl_coa
      bronze contract does not guarantee is rejected, routing to a bronze
      contract-extension.
    """
    errors: list[ValidationError] = []
    contract = _gl_coa_contract_columns(pack)

    for name, spec in pack.pack.column_aliases.items():
        is_coa_role = name in _COA_ROLE_ALIAS_NAMES or (spec.role or "").startswith(
            "coa."
        )
        if not is_coa_role:
            continue
        if spec.resolution != "semanticRole":
            errors.append(
                ValidationError(
                    code=AIDPF_2014_COA_ROLE_AS_EXISTENCE_ALIAS,
                    message=(
                        f"{AIDPF_2014_COA_ROLE_AS_EXISTENCE_ALIAS}: columnAlias "
                        f"`{name}` is a COA business role but is modeled as a "
                        f"column-existence alias. Declare `resolution: semanticRole` "
                        f"+ `role: coa.<role>` so it resolves from explicit "
                        f"`profile.chartOfAccounts`, not column existence."
                    ),
                    location=f"columnAliases/{name}",
                )
            )
            continue
        # AIDPF-2019: candidate must be a valid COA segment (1..30) -- catches
        # Segment31+ / non-segment typos before the contract check.
        for cand in spec.candidates:
            if not _COA_SEGMENT_RE.match(cand):
                errors.append(
                    ValidationError(
                        code=AIDPF_2019_COA_SEGMENT_OUT_OF_RANGE,
                        message=(
                            f"{AIDPF_2019_COA_SEGMENT_OUT_OF_RANGE}: COA role "
                            f"`{name}` allows candidate `{cand}` which is not a "
                            f"`CodeCombinationSegment<N>` with N in 1..30 (Fusion GL "
                            f"flexfield max). Fix the candidate name."
                        ),
                        location=f"columnAliases/{name}",
                    )
                )
        # AIDPF-2015: candidate (allowed domain) must be within the contract.
        if contract is not None:
            for cand in spec.candidates:
                if cand.lower() not in contract:
                    errors.append(
                        ValidationError(
                            code=AIDPF_2015_COA_BINDING_OUT_OF_CONTRACT,
                            message=(
                                f"{AIDPF_2015_COA_BINDING_OUT_OF_CONTRACT}: COA role "
                                f"`{name}` allows candidate `{cand}` which the "
                                f"`gl_coa` bronze outputSchema does not guarantee. "
                                f"Extend the gl_coa bronze contract first."
                            ),
                            location=f"columnAliases/{name}",
                        )
                    )

    # AIDPF-2015: pack-default chartOfAccounts mappings must be in-contract too.
    if contract is not None:
        for pname, prof in (pack.pack.profiles or {}).items():
            coa = prof.chart_of_accounts
            if coa is None:
                continue
            mapped: set[str] = set()
            default = coa.resolved_default()
            if default is not None:
                mapped.update(default.columns().values())
            for arm in (coa.by_chart or {}).values():
                mapped.update(arm.columns().values())
            for col in sorted(mapped):
                if col.lower() not in contract:
                    errors.append(
                        ValidationError(
                            code=AIDPF_2015_COA_BINDING_OUT_OF_CONTRACT,
                            message=(
                                f"{AIDPF_2015_COA_BINDING_OUT_OF_CONTRACT}: "
                                f"profiles.{pname}.chartOfAccounts binds `{col}` which "
                                f"the `gl_coa` bronze outputSchema does not guarantee. "
                                f"Extend the gl_coa bronze contract first."
                            ),
                            location=f"profiles/{pname}/chartOfAccounts",
                        )
                    )
    return errors


def validate_reserved_node_ids(pack: ResolvedPack) -> list[ValidationError]:
    """Reject any real node id in the reserved ``__``-prefixed namespace (AIDPF-2005).

    Synthetic state rows (``__run_manifest__``, ``__coa_gate__``, the gate
    markers) live in this namespace and the resume reader EXCLUDES ``__*__`` ids
    from its succeeded/scope node sets. A real node named ``__…__`` would either
    collide with a synthetic row or be silently dropped from scope
    reconstruction → perpetual retries / omitted nodes / an ambiguous manifest
    read. Covers bronze (per-file + legacy datasets[]), silver, and gold.
    """
    errors: list[ValidationError] = []
    seen: set[tuple[str, str]] = set()

    def _check(layer: str, node_id: str) -> None:
        key = (layer, node_id)
        if key in seen:
            return
        seen.add(key)
        if node_id.startswith("__"):
            errors.append(
                ValidationError(
                    code=AIDPF_2005_RESERVED_NODE_ID,
                    message=(
                        f"{AIDPF_2005_RESERVED_NODE_ID}: node id "
                        f"`{layer}/{node_id}` uses the reserved `__` prefix. That "
                        f"namespace is reserved for synthetic state rows "
                        f"(e.g. `__run_manifest__`, `__coa_gate__`); a real node "
                        f"there breaks resume scope reconstruction. Rename it."
                    ),
                    location=f"{layer}/{node_id}",
                )
            )

    for node_id in pack.bronze:
        _check("bronze", node_id)
    for ds in pack.bronze_yaml.get("datasets", []) or []:
        if isinstance(ds, dict) and isinstance(ds.get("id"), str):
            _check("bronze", ds["id"])
    for node_id in pack.silver:
        _check("silver", node_id)
    for node_id in pack.gold:
        _check("gold", node_id)
    return errors


def validate_pack_full(
    pack: ResolvedPack, *, profile: "TenantProfile | None" = None
) -> ValidationReport:
    """Run every validator over the assembled pack; aggregate into a report.

    ``profile`` (optional) is threaded into the design-time column gates:
    * AIDPF-2045 (``validate_column_contracts``) — resolves its ``$column.*`` /
      ``$coa.*`` demands against the profile; when ``None`` those alias demands
      drop (literals + watermark still gate).
    * AIDPF-2084 (``validate_declared_inputs``) — matches SQL reads against
      declared ``requiredColumns`` **at the author symbol level**, so it runs
      regardless of ``profile``; the profile is used only for its cross-plane
      literal↔alias case. The run-start path calls this with ``profile=None``
      (profile not yet loaded) and the gate still catches token reads.
    AIDPF-2085 (``collect_declared_input_warnings``) is profile-agnostic, warn-only.
    """
    report = ValidationReport()
    report.merge_errors(validate_reserved_node_ids(pack))
    report.merge_errors(validate_sql_paths(pack))
    report.merge_errors(validate_template_variables(pack))
    report.merge_errors(validate_sql_template_hygiene(pack))
    report.merge_errors(validate_dag(pack))
    report.merge_errors(validate_column_contracts(pack, profile=profile))
    report.merge_errors(validate_declared_inputs(pack, profile=profile))
    report.merge_errors(validate_coa_semantic_roles(pack))
    # AIDPF-2080 / AIDPF-2085 are WARN-only.
    report.warnings.extend(validate_bronze_pvo_catalog(pack))
    report.warnings.extend(collect_declared_input_warnings(pack))
    for dashboard in pack.dashboards.values():
        report.merge_errors(validate_dashboard_requires(pack, dashboard))
        report.merge_errors(validate_dashboard_security_and_compat(pack, dashboard))
    return report
