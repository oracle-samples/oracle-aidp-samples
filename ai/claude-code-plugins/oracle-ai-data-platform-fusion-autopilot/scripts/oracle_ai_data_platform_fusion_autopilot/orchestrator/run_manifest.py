"""Durable pre-execution run manifest + resume drift/mode resolution.

Feature: fail-fast-seed-validation. The manifest records the run's INTENT
(resolver inputs + canonical topology + per-node semantic fingerprints + mode +
execution identity + pack fingerprint + profile hash + exec policy) BEFORE the
first node dispatches, so a resume replays the exact original scope instead of
INFERRING it from surviving state rows (which a mid-run failure makes
unreliable). This module is PURE — no Spark, no I/O — so every rule is
unit-testable. The orchestrator composes it: it builds the manifest, writes the
one reserved ``__run_manifest__`` state row, and on resume reads it back and runs
these guards before any node dispatch.

Reserved synthetic node id for the manifest row.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

from ..schema.errors import OrchestratorConfigError

if TYPE_CHECKING:
    from ..schema.medallion_pack import NodeYaml
    from .content_pack import ResolvedPack

RUN_MANIFEST_DATASET_ID = "__run_manifest__"
"""Reserved ``dataset_id`` of the single durable manifest row."""

MANIFEST_SCHEMA_VERSION = 2
"""Bumped only on a breaking manifest-shape change. The reader fails closed on
an unknown version (never legacy-reconstructs when a manifest row exists).

v2 (feature: incremental-coa-chart-onboarding) adds ``coa_projection`` and
``non_coa_semantic_hash`` so an additive chart-of-accounts change can be proven
on resume/incremental. A v1 manifest (written before this feature) is still
accepted — it simply lacks the COA baseline, so the additive fast path is
unavailable and the conservative seed-only behaviour applies."""

SUPPORTED_MANIFEST_SCHEMA_VERSIONS = (1, 2)
"""Manifest versions this reader accepts. A version outside this set fails
closed (AIDPF-4022) — never legacy-reconstructed."""

EXECUTION_MODES = ("seed", "incremental")
"""The only ``mode`` values that count as an EXECUTION row. Audit modes
(``plan_hash_repin``, ``fingerprint_skip``) and the manifest row's own status
are excluded from every derived resume set."""

# --- Error codes -----------------------------------------------------------
AIDPF_4022_MANIFEST_COMMIT_FAILED = "AIDPF-4022"
AIDPF_1044_RESUME_TOPOLOGY_DRIFT = "AIDPF-1044"
AIDPF_1046_RESUME_MODE_CONFLICT = "AIDPF-1046"
AIDPF_1047_RESUME_SCOPE_CONFLICT = "AIDPF-1047"
AIDPF_1048_RESUME_IDENTITY_PROFILE_DRIFT = "AIDPF-1048"
AIDPF_1049_RESUME_NODE_DEFINITION_DRIFT = "AIDPF-1049"


class ManifestError(OrchestratorConfigError):
    """Base for manifest / resume-guard failures (each carries an AIDPF code).

    Inherits ``OrchestratorConfigError`` so the CLI boundary
    (``commands/run.py``) maps every AIDPF-4022 / AIDPF-104x manifest failure to
    the established clean exit-code-2 path instead of leaking a traceback."""


class ManifestInvalidError(ManifestError):
    """AIDPF-4022 — the manifest row is malformed / unknown-version / missing a
    required field. Non-resumable; remediate with a fresh ``--mode seed``."""


class ResumeModeConflictError(ManifestError):
    """AIDPF-1046 — an explicit ``--mode`` conflicts with the manifest / legacy
    inferred mode, OR the legacy execution history is MIXED (non-resumable)."""


class ResumeScopeConflictError(ManifestError):
    """AIDPF-1047 — an explicit scope filter mismatches the manifest scope."""


class ResumeTopologyDriftError(ManifestError):
    """AIDPF-1044 — the replayed plan's topology (nodes or edges) differs from
    the manifest's."""


class ResumeIdentityProfileDriftError(ManifestError):
    """AIDPF-1048 — execution identity, profile hash, or exec policy changed on
    resume; apply the change via a fresh ``--mode seed``, not a resume."""


class ResumeNodeDefinitionDriftError(ManifestError):
    """AIDPF-1049 — a node's semantic fingerprint or the pack fingerprint
    changed on resume (a definition edit under an unchanged topology)."""


# ---------------------------------------------------------------------------
# Fingerprints (pure)
# ---------------------------------------------------------------------------


def compute_node_sem(
    node: NodeYaml,
    *,
    sql_bytes: bytes | None = None,
    schema_override: str | None = None,
) -> str:
    """Per-node, profile-INDEPENDENT semantic fingerprint.

    Covers the complete authored execution contract that a resume must not
    silently mix across the seed↔skip boundary: the full ``NodeYaml``
    (``model_dump(by_alias=True, mode="json")`` — layer, target, the entire
    ``implementation`` payload, refresh, requiredColumns, outputSchema incl. pii,
    quality), the EXACT ``.sql`` template BYTES (SQL nodes), the redundant-safe
    ``compute_contract_fingerprint``, and the per-node
    ``bundle.fusion.schemaOverrides`` entry (a physical-table repoint that never
    touches NodeYaml). Excludes profile-rendered SQL / profile_hash / tenant +
    bronze fingerprints — those belong to the separate scoped-profile guard, so a
    COA-only profile refresh does NOT trip AIDPF-1049 while a genuine definition
    edit does.
    """
    from .sql_renderer import compute_contract_fingerprint

    h = hashlib.sha256()
    dumped = node.model_dump(by_alias=True, mode="json")
    h.update(json.dumps(dumped, sort_keys=True, default=str).encode("utf-8"))
    h.update(b"\x00sql\x00")
    if sql_bytes is not None:
        h.update(hashlib.sha256(sql_bytes).digest())
    h.update(b"\x00contract\x00")
    h.update(compute_contract_fingerprint(node).encode("utf-8"))
    h.update(b"\x00override\x00")
    h.update((schema_override or "").encode("utf-8"))
    return h.hexdigest()


def compute_pack_fingerprint(
    pack: ResolvedPack, active_profile_name: str | None
) -> str:
    """Manifest-level PACK execution fingerprint.

    Covers pack-level inputs that no per-node ``sem`` covers:

    * pack ``id`` + ``version``;
    * the ``columnAliases`` block (each alias's ``resolution`` / ``role`` /
      ``appliesTo`` / ``candidates`` — a COA alias role or ``appliesTo`` edit
      must invalidate);
    * **every ``semanticVariants`` candidate** (full ``model_dump``) — a
      ``{{ semantic.* }}`` fragment edit that leaves a node's SQL token
      unchanged still changes the RENDERED SQL, so it must invalidate here
      (per-node ``sem`` hashes only the raw template bytes and would miss it);
    * the **active** pack-profile ``calendar`` defaults consumed by
      ``dim_calendar_adapter`` — keyed by ``active_profile_name`` (the real
      active key is ``bundle.contentPack.profile``; the ``Pack`` model has NO
      ``active_profile`` attribute, so it MUST be passed in — a wrong/None key
      would hash the calendar as null and miss a calendar mutation).

    Any change → AIDPF-1049 on resume.
    """
    payload: dict[str, Any] = {
        "id": pack.pack.id,
        "version": pack.pack.version,
        "columnAliases": {},
        "semanticVariants": {},
    }
    for name, spec in sorted(pack.pack.column_aliases.items()):
        payload["columnAliases"][name] = {
            "resolution": getattr(spec, "resolution", None),
            "role": getattr(spec, "role", None),
            "appliesTo": getattr(spec, "appliesTo", None),
            "candidates": list(getattr(spec, "candidates", []) or []),
        }
    # Full semanticVariants block — any referenced fragment edit invalidates.
    for name, variant in sorted(
        (getattr(pack.pack, "semantic_variants", None) or {}).items()
    ):
        payload["semanticVariants"][name] = (
            variant.model_dump(by_alias=True, mode="json")
            if hasattr(variant, "model_dump")
            else variant
        )
    # Active pack-profile calendar defaults (dim_calendar_adapter input),
    # resolved by the REAL active profile key.
    profiles = getattr(pack.pack, "profiles", None) or {}
    cal: Any = None
    prof = (
        profiles.get(active_profile_name)
        if isinstance(profiles, dict) and active_profile_name
        else None
    )
    if prof is not None:
        cal = getattr(prof, "calendar", None)
        if cal is not None and hasattr(cal, "model_dump"):
            cal = cal.model_dump(by_alias=True, mode="json")
    payload["calendar"] = cal
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def canonical_topology(
    plan: list[NodeYaml],
    *,
    sem_by_id: dict[str, str],
) -> list[dict[str, Any]]:
    """Canonical, order-independent topology: per node ``(id, layer, sorted
    in-closure dependsOn ids, sem)``, sorted by id. Edge-aware (an edge change
    with an unchanged node set still differs)."""
    out: list[dict[str, Any]] = []
    for node in plan:
        deps = _node_dep_ids(node)
        out.append(
            {
                "id": node.id,
                "layer": node.layer,
                "deps": sorted(deps),
                "sem": sem_by_id.get(node.id, ""),
            }
        )
    return sorted(out, key=lambda e: e["id"])


def _node_dep_ids(node: NodeYaml) -> list[str]:
    deps = getattr(node, "depends_on", None)
    if deps is None:
        return []
    ids: list[str] = []
    for attr in ("bronze", "silver", "gold"):
        for dep in getattr(deps, attr, None) or []:
            dep_id = getattr(dep, "id", None)
            if dep_id:
                ids.append(dep_id)
    return ids


# ---------------------------------------------------------------------------
# Manifest build / (de)serialize (pure)
# ---------------------------------------------------------------------------


def build_manifest(
    *,
    datasets: list[str] | None,
    layers: list[str] | None,
    strict_scope: bool,
    topology: list[dict[str, Any]],
    mode: str,
    identity: dict[str, str],
    pack_fingerprint: str,
    profile_hash: str,
    allow_unprovable_coa: bool,
    coa_projection: dict[str, Any],
    non_coa_semantic_hash: str,
) -> dict[str, Any]:
    """Assemble the immutable manifest dict (stored verbatim as JSON).

    ``coa_projection`` is the raw ``{default, byChart, singletonAccepted}`` COA
    mapping (normalised via :func:`coa_change.coa_projection_of`), and
    ``non_coa_semantic_hash`` is the allowlist semantic hash of the non-COA
    profile identity (:func:`coa_change.non_coa_semantic_hash`). Both are
    computed by the caller (orchestrator) so this function stays pure. They are
    the durable baseline the additive-COA fast path proves against on a later
    resume/incremental.
    """
    return {
        "schemaVersion": MANIFEST_SCHEMA_VERSION,
        "resolver_inputs": {
            "datasets": datasets,
            "layers": layers,
            "strict_scope": bool(strict_scope),
        },
        "topology": topology,
        "mode": mode,
        "identity": identity,
        "pack_fingerprint": pack_fingerprint,
        "profile_hash": profile_hash,
        "exec_policy": {"allowUnprovableCOA": bool(allow_unprovable_coa)},
        # v2 (incremental-coa-chart-onboarding) COA baseline.
        "coa_projection": coa_projection,
        "non_coa_semantic_hash": non_coa_semantic_hash,
    }


def serialize_manifest(manifest: dict[str, Any]) -> str:
    return json.dumps(manifest, sort_keys=True, default=str)


# Base fields present in every manifest version.
_REQUIRED_FIELDS_BASE = (
    "schemaVersion",
    "resolver_inputs",
    "topology",
    "mode",
    "identity",
    "pack_fingerprint",
    "profile_hash",
    "exec_policy",
)
# Per-version required-field sets. v2 adds the COA baseline. A v1 manifest MUST
# NOT be required to carry the v2 fields (it predates the feature), so the
# reader selects the required set by the manifest's own schemaVersion — read
# BEFORE the field-presence check.
_REQUIRED_FIELDS_BY_VERSION: dict[int, tuple[str, ...]] = {
    1: _REQUIRED_FIELDS_BASE,
    2: _REQUIRED_FIELDS_BASE + ("coa_projection", "non_coa_semantic_hash"),
}


def parse_manifest(raw: str | None) -> dict[str, Any]:
    """Strict-parse a manifest JSON string; fail closed (AIDPF-4022) on a
    malformed / unknown-version / missing-field payload. Never returns a
    partially-valid manifest."""
    if not raw:
        raise ManifestInvalidError(
            f"{AIDPF_4022_MANIFEST_COMMIT_FAILED}: manifest row is empty."
        )
    try:
        data = json.loads(raw)
    except (ValueError, TypeError) as exc:
        raise ManifestInvalidError(
            f"{AIDPF_4022_MANIFEST_COMMIT_FAILED}: manifest is not valid JSON "
            f"({exc})."
        ) from exc
    if not isinstance(data, dict):
        raise ManifestInvalidError(
            f"{AIDPF_4022_MANIFEST_COMMIT_FAILED}: manifest is not an object."
        )
    # Read the version FIRST — the required-field set is version-specific, so a
    # v1 manifest must not be rejected for lacking the v2 COA fields.
    if "schemaVersion" not in data:
        raise ManifestInvalidError(
            f"{AIDPF_4022_MANIFEST_COMMIT_FAILED}: manifest missing required "
            f"field 'schemaVersion'."
        )
    version = data["schemaVersion"]
    if version not in SUPPORTED_MANIFEST_SCHEMA_VERSIONS:
        raise ManifestInvalidError(
            f"{AIDPF_4022_MANIFEST_COMMIT_FAILED}: manifest schemaVersion "
            f"{version!r} not in supported {SUPPORTED_MANIFEST_SCHEMA_VERSIONS!r}."
        )
    missing = [f for f in _REQUIRED_FIELDS_BY_VERSION[version] if f not in data]
    if missing:
        raise ManifestInvalidError(
            f"{AIDPF_4022_MANIFEST_COMMIT_FAILED}: v{version} manifest missing "
            f"required field(s) {missing!r}."
        )

    # Validate NESTED shape + types — top-level presence alone is not enough. A
    # payload like ``resolver_inputs: []`` would pass presence then crash on a
    # later ``.get`` in the guards. Fail closed HERE instead (AIDPF-4022).
    def _require(cond: bool, detail: str) -> None:
        if not cond:
            raise ManifestInvalidError(
                f"{AIDPF_4022_MANIFEST_COMMIT_FAILED}: {detail}"
            )

    ri = data["resolver_inputs"]
    _require(isinstance(ri, dict), "resolver_inputs is not an object.")
    _require(
        ri.get("datasets") is None or _is_str_list(ri.get("datasets")),
        "resolver_inputs.datasets is not null or a list of strings.",
    )
    _require(
        ri.get("layers") is None or _is_str_list(ri.get("layers")),
        "resolver_inputs.layers is not null or a list of strings.",
    )
    _require(
        isinstance(ri.get("strict_scope"), bool),
        "resolver_inputs.strict_scope is not a bool.",
    )
    _require(
        data["mode"] in EXECUTION_MODES,
        f"mode {data['mode']!r} is not one of {EXECUTION_MODES!r}.",
    )
    _require(isinstance(data["identity"], dict), "identity is not an object.")
    _require(
        isinstance(data["pack_fingerprint"], str),
        "pack_fingerprint is not a string.",
    )
    _require(isinstance(data["profile_hash"], str), "profile_hash is not a string.")
    _require(isinstance(data["exec_policy"], dict), "exec_policy is not an object.")
    # allowUnprovableCOA must be present AND a NATIVE bool — a persisted
    # coercible value like "false" would otherwise be read as truthy by a later
    # bool(...) and silently resume with the correctness hatch ENABLED.
    _require(
        isinstance(data["exec_policy"].get("allowUnprovableCOA"), bool),
        "exec_policy.allowUnprovableCOA is missing or not a native bool.",
    )

    topo = data["topology"]
    _require(isinstance(topo, list), "topology is not a list.")
    for i, entry in enumerate(topo):
        _require(isinstance(entry, dict), f"topology[{i}] is not an object.")
        _require(
            isinstance(entry.get("id"), str),
            f"topology[{i}].id is not a string.",
        )
        _require(
            isinstance(entry.get("layer"), str),
            f"topology[{i}].layer is not a string.",
        )
        _require(
            _is_str_list(entry.get("deps")),
            f"topology[{i}].deps is not a list of strings.",
        )
        _require(
            isinstance(entry.get("sem"), str),
            f"topology[{i}].sem is not a string.",
        )

    # v2 COA baseline shape (only present/required for v2+; a v1 manifest has
    # no COA baseline and takes the conservative no-additive-path branch).
    if version >= 2:
        _require(
            isinstance(data["coa_projection"], dict),
            "coa_projection is not an object.",
        )
        _require(
            isinstance(data["non_coa_semantic_hash"], str),
            "non_coa_semantic_hash is not a string.",
        )
    return data


def _is_str_list(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(v, str) for v in value)


# ---------------------------------------------------------------------------
# Mode resolution (Blocker 2) — pure
# ---------------------------------------------------------------------------


def resolve_run_mode(
    explicit_mode: str | None,
    *,
    is_resume: bool,
    manifest_mode: str | None = None,
    historical_exec_modes: list[str] | None = None,
) -> str:
    """Resolve the effective run mode, enforcing the seed/incremental contract.

    * **Fresh run** (not a resume): ``explicit_mode or "seed"`` (unchanged
      default).
    * **Resume, manifest present**: adopt ``manifest_mode`` when
      ``explicit_mode`` is None; an explicit match is fine; an explicit conflict
      → AIDPF-1046.
    * **Resume, NO manifest (legacy)**: inspect ALL historical EXECUTION modes.
      ``> 1`` distinct → the physical baseline is already MIXED → non-resumable
      regardless of an explicit ``--mode`` (AIDPF-1046; remediate with a fresh
      seed). Exactly one → adopt it (validate an explicit ``--mode`` against it).
      None carry a mode + explicit omitted → reject asking for an explicit
      ``--mode`` (never silently ``seed`` — that is the destructive-overwrite
      bug).
    """
    if not is_resume:
        return explicit_mode or "seed"

    if manifest_mode is not None:
        if explicit_mode is None or explicit_mode == manifest_mode:
            return manifest_mode
        raise ResumeModeConflictError(
            f"{AIDPF_1046_RESUME_MODE_CONFLICT}: --mode {explicit_mode!r} "
            f"conflicts with the run manifest's mode {manifest_mode!r}. Drop "
            f"--mode to adopt the manifest, or start a fresh scoped run."
        )

    distinct = sorted({m for m in (historical_exec_modes or []) if m in EXECUTION_MODES})
    if len(distinct) > 1:
        raise ResumeModeConflictError(
            f"{AIDPF_1046_RESUME_MODE_CONFLICT}: the run's execution history is "
            f"MIXED (modes {distinct!r}) — the physical baseline is incoherent "
            f"and no --mode can repair it. Remediate with a fresh `--mode seed`."
        )
    if len(distinct) == 1:
        adopted = distinct[0]
        if explicit_mode is None or explicit_mode == adopted:
            return adopted
        raise ResumeModeConflictError(
            f"{AIDPF_1046_RESUME_MODE_CONFLICT}: --mode {explicit_mode!r} "
            f"conflicts with the resumed run's recorded mode {adopted!r}."
        )
    # No execution row carries a mode.
    if explicit_mode is None:
        raise ResumeModeConflictError(
            f"{AIDPF_1046_RESUME_MODE_CONFLICT}: cannot infer the mode of this "
            f"legacy run (no execution rows carry a mode). Pass --mode "
            f"explicitly (seed|incremental)."
        )
    return explicit_mode


# ---------------------------------------------------------------------------
# Scope + drift guards (pure) — each raises its typed AIDPF error
# ---------------------------------------------------------------------------


def check_scope_conflict(
    explicit_datasets: list[str] | None,
    explicit_layers: list[str] | None,
    explicit_strict_scope: bool | None,
    *,
    manifest_inputs: dict[str, Any],
) -> None:
    """AIDPF-1047 — an explicit scope filter on a manifest-backed resume that
    does not EXACTLY equal the manifest scope. Omitted (None) filters adopt the
    manifest; a mismatch never silently narrows."""
    m_datasets = manifest_inputs.get("datasets")
    m_layers = manifest_inputs.get("layers")
    m_strict = bool(manifest_inputs.get("strict_scope"))

    def _norm(v: list[str] | None) -> set[str] | None:
        return None if v is None else set(v)

    conflicts: list[str] = []
    if explicit_datasets is not None and _norm(explicit_datasets) != _norm(m_datasets):
        conflicts.append(f"--datasets {explicit_datasets!r} != {m_datasets!r}")
    if explicit_layers is not None and _norm(explicit_layers) != _norm(m_layers):
        conflicts.append(f"--layers {explicit_layers!r} != {m_layers!r}")
    if explicit_strict_scope is not None and explicit_strict_scope != m_strict:
        conflicts.append(f"--strict-scope {explicit_strict_scope!r} != {m_strict!r}")
    if conflicts:
        raise ResumeScopeConflictError(
            f"{AIDPF_1047_RESUME_SCOPE_CONFLICT}: explicit scope conflicts with "
            f"the run manifest ({'; '.join(conflicts)}). Drop the filters to "
            f"resume the original scope, or start a fresh scoped run."
        )


def check_topology_drift(
    replayed_topology: list[dict[str, Any]],
    *,
    manifest_topology: list[dict[str, Any]],
) -> None:
    """AIDPF-1044 — the replayed plan's canonical topology (id, layer, sorted
    deps) differs from the manifest's. Edge-aware (an edge change with an
    unchanged node set still fires). Compares ONLY topology (id/layer/deps), NOT
    ``sem`` (that is the node-definition guard)."""

    def _topo_only(entries: list[dict[str, Any]]) -> list[tuple]:
        return sorted(
            (e["id"], e["layer"], tuple(e["deps"])) for e in entries
        )

    if _topo_only(replayed_topology) != _topo_only(manifest_topology):
        raise ResumeTopologyDriftError(
            f"{AIDPF_1044_RESUME_TOPOLOGY_DRIFT}: the plan topology changed since "
            f"the manifest was written (node added/removed or an edge changed). "
            f"Resume aborted — start a fresh `--mode seed`."
        )


def check_node_definition_drift(
    replayed_topology: list[dict[str, Any]],
    replayed_pack_fingerprint: str,
    *,
    manifest_topology: list[dict[str, Any]],
    manifest_pack_fingerprint: str,
) -> None:
    """AIDPF-1049 — any node's ``sem`` differs, or the pack fingerprint differs
    (a definition edit under an unchanged topology)."""
    if replayed_pack_fingerprint != manifest_pack_fingerprint:
        raise ResumeNodeDefinitionDriftError(
            f"{AIDPF_1049_RESUME_NODE_DEFINITION_DRIFT}: the pack execution "
            f"fingerprint changed since the manifest was written. Resume aborted "
            f"— start a fresh `--mode seed`."
        )
    sem_by_id = {e["id"]: e["sem"] for e in manifest_topology}
    for e in replayed_topology:
        if sem_by_id.get(e["id"]) != e["sem"]:
            raise ResumeNodeDefinitionDriftError(
                f"{AIDPF_1049_RESUME_NODE_DEFINITION_DRIFT}: node {e['id']!r} "
                f"definition changed since the manifest was written (SQL / "
                f"schema / refresh / requiredColumns / schemaOverride). Resume "
                f"aborted — start a fresh `--mode seed`."
            )


def check_identity_profile_drift(
    *,
    current_identity: dict[str, str],
    current_profile_hash: str,
    current_allow_unprovable_coa: bool,
    manifest: dict[str, Any],
    current_non_coa_semantic_hash: str | None = None,
    coa_verdict: str | None = None,
) -> None:
    """AIDPF-1048 — execution identity, profile, or exec policy changed on resume.

    Identity and exec-policy changes always route to a fresh ``--mode seed``.
    A **profile** change is split (feature: incremental-coa-chart-onboarding):

    * ``current_non_coa_semantic_hash`` differs from the manifest's → a real
      (non-COA) profile change → **AIDPF-1048** (unchanged behaviour).
    * non-COA identical, so the delta is confined to the COA mapping (or to
      volatile refresh metadata excluded from the semantic hash) → honour the
      precomputed ``coa_verdict``: ``additive`` / ``identical`` → allow the
      resume; ``mutating`` → **AIDPF-1048**.

    Both ``current_non_coa_semantic_hash`` and ``coa_verdict`` are computed
    Spark-side by the orchestrator (the additive verdict needs a live
    ``dim_account`` read for ``protected_charts``; a failed read must arrive here
    as ``coa_verdict=None`` so this gate stays fail-closed). When either is
    ``None`` — a v1 manifest with no COA baseline, or an unavailable read — the
    gate falls back to the conservative whole-``profile_hash`` compare.
    """
    if current_identity != manifest.get("identity"):
        raise ResumeIdentityProfileDriftError(
            f"{AIDPF_1048_RESUME_IDENTITY_PROFILE_DRIFT}: execution identity "
            f"(endpoint / principal / schema / plugin version) changed since the "
            f"manifest was written. Apply via a fresh `--mode seed`."
        )
    if current_profile_hash != manifest.get("profile_hash"):
        manifest_non_coa = manifest.get("non_coa_semantic_hash")
        # Conservative fallback: no v2 COA baseline, or the verdict / non-COA
        # hash could not be computed (fail-closed) → treat any profile change as
        # a fresh-seed change, exactly as before this feature.
        if (
            manifest_non_coa is None
            or coa_verdict is None
            or current_non_coa_semantic_hash is None
        ):
            raise ResumeIdentityProfileDriftError(
                f"{AIDPF_1048_RESUME_IDENTITY_PROFILE_DRIFT}: the tenant profile "
                f"changed since the manifest was written. Apply via a fresh "
                f"`--mode seed`, not a resume."
            )
        if current_non_coa_semantic_hash != manifest_non_coa:
            raise ResumeIdentityProfileDriftError(
                f"{AIDPF_1048_RESUME_IDENTITY_PROFILE_DRIFT}: a non-COA part of "
                f"the tenant profile changed since the manifest was written. "
                f"Apply via a fresh `--mode seed`, not a resume."
            )
        # Non-COA identical → the delta is COA-only (or volatile-metadata-only).
        if coa_verdict == "mutating":
            raise ResumeIdentityProfileDriftError(
                f"{AIDPF_1048_RESUME_IDENTITY_PROFILE_DRIFT}: an existing chart "
                f"of accounts' mapping changed (mutating COA change). An "
                f"incremental would leave already-classified rows stale — apply "
                f"via a fresh `--mode seed`."
            )
        # coa_verdict in ("additive", "identical") → safe to resume.
    # Compare the stored native bool DIRECTLY — no bool() coercion. parse_manifest
    # already guaranteed exec_policy.allowUnprovableCOA is a native bool, so a
    # corrupt "false" string never reaches here; comparing directly means we
    # never silently coerce a malformed value into a hatch-enabled resume.
    m_policy = manifest["exec_policy"]["allowUnprovableCOA"]
    if current_allow_unprovable_coa is not m_policy:
        raise ResumeIdentityProfileDriftError(
            f"{AIDPF_1048_RESUME_IDENTITY_PROFILE_DRIFT}: the COA exec policy "
            f"(allowUnprovableCOA) changed since the manifest was written; a "
            f"resume cannot silently weaken a hard COA block. Fresh `--mode seed`."
        )


__all__ = [
    "AIDPF_1044_RESUME_TOPOLOGY_DRIFT",
    "AIDPF_1046_RESUME_MODE_CONFLICT",
    "AIDPF_1047_RESUME_SCOPE_CONFLICT",
    "AIDPF_1048_RESUME_IDENTITY_PROFILE_DRIFT",
    "AIDPF_1049_RESUME_NODE_DEFINITION_DRIFT",
    "AIDPF_4022_MANIFEST_COMMIT_FAILED",
    "EXECUTION_MODES",
    "MANIFEST_SCHEMA_VERSION",
    "RUN_MANIFEST_DATASET_ID",
    "ManifestError",
    "ManifestInvalidError",
    "ResumeIdentityProfileDriftError",
    "ResumeModeConflictError",
    "ResumeNodeDefinitionDriftError",
    "ResumeScopeConflictError",
    "ResumeTopologyDriftError",
    "build_manifest",
    "canonical_topology",
    "check_identity_profile_drift",
    "check_node_definition_drift",
    "check_scope_conflict",
    "check_topology_drift",
    "compute_node_sem",
    "compute_pack_fingerprint",
    "parse_manifest",
    "resolve_run_mode",
    "serialize_manifest",
]
