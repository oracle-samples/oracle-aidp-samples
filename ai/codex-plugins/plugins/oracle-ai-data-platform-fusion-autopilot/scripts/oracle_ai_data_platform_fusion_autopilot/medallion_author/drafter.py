"""Overlay drafter for the medallion-author skill.

Emits:

* ``overlays/<overlay-name>/pack.yaml`` — content-pack overlay with
  ``extends:`` pointing at the starter pack and ``columnAliases`` /
  ``semanticVariants`` extended with operator-approved candidates.
  Full provenance block stamped for audit.
* ``overlays/<overlay-name>/resolutions.json`` — **conditional**;
  only emitted when MultiMatch picks or refresh-AutoResolved-change
  picks need scripted operator approval at commit time. Initial
  AIDPF-2010 onboarding skips this file (would fail feature #2's
  validator).
* ``overlays/<overlay-name>/skill-evidence.json`` — the skill's own
  audit trail (model id, reasoning per proposal, cost estimates,
  operator decisions).

The drafter NEVER emits SQL templates. ``validate_overlay`` rejects any
overlay that introduces a node or override block, and ``write_overlay``
confines all I/O to ``<workdir>/overlays/<overlay-name>/``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import yaml

from ..schema.diagnostic_artifact import BronzeTypeMismatchV1
from ..schema.incremental_impact import IncrementalImpact
from ..schema.medallion_pack import (
    ColumnAlias,
    PackProvenance,
    PackYaml,
    SemanticVariant,
    SemanticVariantCandidate,
    SemanticVariantDetect,
    SkillProposalRecord,
)
from ..schema.path_segment import (
    UnsafePathSegmentError,
    assert_within_root,
    validate_path_segment,
)
from ..schema.resolutions_input import ResolutionsInputV1
from . import SKILL_ID, SKILL_VERSION


PickOutcome = Literal["AutoResolved", "MultiMatch", "RefreshChange"]


@dataclass(frozen=True)
class ProposedCandidate:
    """One operator-approved candidate to add to a VP's list."""

    vp_name: str
    kind: Literal["columnAliases", "semanticVariants"]
    applies_to: str
    """``bronze.<dataset_id>`` (from the diagnostic artifact's
    ``variationPoint.appliesTo``)."""

    candidate: str
    """For columnAliases: the column name. For semanticVariants: the
    candidate ID (e.g. ``cancelled_date_short``)."""

    confidence: str | None = None
    reasoning: str | None = None

    # semanticVariants-only — required to make the overlay valid.
    detect_column: str | None = None
    """The ``detect.columnExists`` value (semanticVariants only)."""

    fragment: str | None = None
    """SQL fragment substituted at render (semanticVariants only)."""

    outcome: PickOutcome = "AutoResolved"
    """Walker outcome label — drives whether ``write_resolutions`` emits
    an entry for this pick."""

    incremental_impact: IncrementalImpact | None = None
    """Per-VP impact analysis (refresh promotions only)."""


@dataclass(frozen=True)
class OverlayDraft:
    """In-memory overlay representation prior to disk write."""

    overlay_name: str
    base_pack_id: str
    base_pack_version: str
    diagnostic_run_id: str
    model_id: str

    proposed: tuple[ProposedCandidate, ...]
    """Operator-approved proposals to write into the overlay."""

    pack_yaml: PackYaml
    """Pydantic-validated PackYaml ready to serialise."""

    skill_evidence: dict[str, Any] = field(default_factory=dict)
    """Skill's own audit-trail payload (model id, reasoning per
    proposal, cost estimates). Written separately to
    ``skill-evidence.json``."""


class OverlayValidationError(ValueError):
    """Raised when a drafted overlay violates skill-authored overlay rules."""


# ---------------------------------------------------------------------------
# draft_overlay
# ---------------------------------------------------------------------------


def draft_overlay(
    *,
    overlay_name: str,
    base_pack_id: str,
    base_pack_version: str,
    base_column_aliases: dict[str, ColumnAlias],
    base_semantic_variants: dict[str, SemanticVariant],
    proposed: list[ProposedCandidate],
    diagnostic_run_id: str,
    model_id: str,
    skill_evidence: dict[str, Any] | None = None,
) -> OverlayDraft:
    """Assemble an :class:`OverlayDraft` from operator-approved proposals.

    Each ``proposed`` candidate appends to the base pack's existing
    candidate list — overlay's ``columnAliases.<vp>.candidates`` is
    ``[<base candidates...>, <new candidate>]``. Validated via
    :func:`validate_overlay` before return; raises
    :class:`OverlayValidationError` on any overlay-rule violation.
    """
    validate_path_segment(overlay_name, field="overlay.name")
    validate_path_segment(diagnostic_run_id, field="overlay.diagnosticRunId")

    if not proposed:
        raise OverlayValidationError(
            "draft_overlay called with empty `proposed` list — nothing to draft."
        )

    column_aliases: dict[str, dict[str, Any]] = {}
    semantic_variants: dict[str, dict[str, Any]] = {}

    proposals_map: dict[str, SkillProposalRecord] = {}
    impact_map: dict[str, IncrementalImpact] = {}

    for p in proposed:
        # Provenance entries (per VP) for the operator-facing audit.
        proposals_map[p.vp_name] = SkillProposalRecord(
            candidateAdded=p.candidate,
            confidence=p.confidence,
            reasoning=p.reasoning,
        )
        if p.incremental_impact is not None:
            impact_map[p.vp_name] = p.incremental_impact

        if p.kind == "columnAliases":
            base = base_column_aliases.get(p.vp_name)
            inherited = list(base.candidates) if base is not None else []
            column_aliases[p.vp_name] = {
                "appliesTo": p.applies_to,
                "required": base.required if base is not None else True,
                "candidates": [*inherited, p.candidate],
            }
        else:  # semanticVariants
            if p.detect_column is None or p.fragment is None:
                raise OverlayValidationError(
                    f"semanticVariants proposal for {p.vp_name!r} missing "
                    f"detect_column or fragment; both are required."
                )
            base = base_semantic_variants.get(p.vp_name)
            inherited: list[dict[str, Any]] = []
            if base is not None:
                inherited = [c.model_dump(by_alias=True) for c in base.candidates]
            semantic_variants[p.vp_name] = {
                "appliesTo": p.applies_to,
                "required": base.required if base is not None else True,
                "candidates": [
                    *inherited,
                    {
                        "id": p.candidate,
                        "detect": {"columnExists": p.detect_column},
                        "fragment": p.fragment,
                    },
                ],
            }

    pack_data: dict[str, Any] = {
        "id": overlay_name,
        "version": "0.1.0",
        "extends": f"{base_pack_id}@{base_pack_version}",
        "compatibility": {
            "pluginMinVersion": "0.3.0",
            "fusionFamilies": ["ERP"],
            "aidp": {"requiresDelta": True},
        },
        "provenance": _build_provenance(
            diagnostic_run_id=diagnostic_run_id,
            model_id=model_id,
            proposals=proposals_map,
            incremental_impact=impact_map,
        ),
    }
    if column_aliases:
        pack_data["columnAliases"] = column_aliases
    if semantic_variants:
        pack_data["semanticVariants"] = semantic_variants

    pack_yaml = PackYaml.model_validate(pack_data)
    draft = OverlayDraft(
        overlay_name=overlay_name,
        base_pack_id=base_pack_id,
        base_pack_version=base_pack_version,
        diagnostic_run_id=diagnostic_run_id,
        model_id=model_id,
        proposed=tuple(proposed),
        pack_yaml=pack_yaml,
        skill_evidence=skill_evidence or {},
    )
    validate_overlay(draft)
    return draft


def draft_type_overlay(
    *,
    overlay_name: str,
    base_pack_id: str,
    base_pack_version: str,
    mismatch: "BronzeTypeMismatchV1",
    diagnostic_run_id: str,
    model_id: str,
    skill_evidence: dict[str, Any] | None = None,
) -> OverlayDraft:
    """Draft a bronze type-overlay from an AIDPF-4070 diagnostic.

    Emits an ``overrides: { bronze/<node>: { outputSchema: { columns: [...] }}}``
    block that retypes each mismatched column to its live (``materialised``)
    type — for operator approval (never auto-applied). Validated via
    :func:`validate_overlay` before return.
    """
    validate_path_segment(overlay_name, field="overlay.name")
    validate_path_segment(diagnostic_run_id, field="overlay.diagnosticRunId")

    if not mismatch.type_mismatches:
        raise OverlayValidationError(
            "draft_type_overlay called with no type mismatches — nothing to draft."
        )

    columns = [
        {"name": m.column, "type": m.materialised} for m in mismatch.type_mismatches
    ]
    pack_data: dict[str, Any] = {
        "id": overlay_name,
        "version": "0.1.0",
        "extends": f"{base_pack_id}@{base_pack_version}",
        "compatibility": {
            "pluginMinVersion": "0.3.0",
            "fusionFamilies": ["ERP"],
            "aidp": {"requiresDelta": True},
        },
        "provenance": _build_provenance(
            diagnostic_run_id=diagnostic_run_id,
            model_id=model_id,
            proposals={},
            incremental_impact={},
        ),
        "overrides": {
            f"bronze/{mismatch.node}": {"outputSchema": {"columns": columns}}
        },
    }
    pack_yaml = PackYaml.model_validate(pack_data)
    draft = OverlayDraft(
        overlay_name=overlay_name,
        base_pack_id=base_pack_id,
        base_pack_version=base_pack_version,
        diagnostic_run_id=diagnostic_run_id,
        model_id=model_id,
        proposed=(),
        pack_yaml=pack_yaml,
        skill_evidence=skill_evidence or {},
    )
    validate_overlay(draft)
    return draft


_COA_ROLE_TO_ALIAS = {
    "balancing": ("coa_balancing_segment", "coa.balancing"),
    "cost_center": ("coa_cost_center_segment", "coa.cost_center"),
    "natural_account": ("coa_natural_account_segment", "coa.natural_account"),
}


def draft_coa_depth_overlay(
    *,
    overlay_name: str,
    base_pack_id: str,
    base_pack_version: str,
    base_column_aliases: dict[str, ColumnAlias],
    segments: list[int],
    operator_input_id: str,
    model_id: str,
    tenant: str | None = None,
    roles: dict[str, str] | None = None,
    skill_evidence: dict[str, Any] | None = None,
) -> OverlayDraft:
    """Draft a COA-depth overlay from **operator input** (no runtime diagnostic).

    Extends, in ONE coordinated overlay:
      * the three ``coa_*`` semantic-role candidate lists (inherit + the deep
        ``CodeCombinationSegment<N>`` columns), and
      * the ``gl_coa`` bronze ``outputSchema`` (``extendColumns`` the same deep
        columns) — so the binding is contract-backed (avoids AIDPF-2015).

    Provenance uses operator-input mode (``operatorInputId`` + ``trigger:
    operator_input``), never a faked ``diagnosticRunId`` — the canonical entry
    for an ``AIDPF-2015`` content-pack-validate failure, which writes no
    diagnostic artifact. ``requiredColumns`` is intentionally NOT touched
    (overlay support for it is the separate ``bronze-required-columns-overlay``).
    """
    validate_path_segment(overlay_name, field="overlay.name")
    validate_path_segment(operator_input_id, field="overlay.operatorInputId")

    bad = [n for n in segments if n < 1 or n > 30]
    if bad:
        raise OverlayValidationError(
            f"COA-depth segments out of range (must be 1..30): {bad!r}."
        )
    if not segments:
        raise OverlayValidationError("draft_coa_depth_overlay needs >=1 segment.")

    deep_cols = [f"CodeCombinationSegment{n}" for n in sorted(set(segments))]

    # Extend each COA role's candidate domain: inherit + deep columns.
    column_aliases: dict[str, dict[str, Any]] = {}
    for _role, (alias_name, role_token) in _COA_ROLE_TO_ALIAS.items():
        base = base_column_aliases.get(alias_name)
        column_aliases[alias_name] = {
            "appliesTo": base.appliesTo if base is not None else "bronze.gl_coa",
            "required": base.required if base is not None else True,
            "resolution": "semanticRole",
            "role": role_token,
            "candidates": ["inherit", *deep_cols],
        }

    # Extend the gl_coa bronze contract (extendColumns the deep segments).
    overrides = {
        "bronze/gl_coa": {
            "outputSchema": {
                "extendColumns": True,
                "columns": [
                    {"name": c, "type": "string", "pii": "none"} for c in deep_cols
                ],
            }
        }
    }

    evidence = {
        "trigger": "operator_input",
        "tenant": tenant,
        "segments": sorted(set(segments)),
        "roles": roles or {},
    }
    pack_data: dict[str, Any] = {
        "id": overlay_name,
        "version": "0.1.0",
        "extends": f"{base_pack_id}@{base_pack_version}",
        "compatibility": {
            "pluginMinVersion": "0.3.0",
            "fusionFamilies": ["ERP"],
            "aidp": {"requiresDelta": True},
        },
        "provenance": _build_provenance(
            operator_input_id=operator_input_id,
            model_id=model_id,
            proposals={},
            incremental_impact={},
            evidence=evidence,
        ),
        "columnAliases": column_aliases,
        "overrides": overrides,
    }
    pack_yaml = PackYaml.model_validate(pack_data)
    draft = OverlayDraft(
        overlay_name=overlay_name,
        base_pack_id=base_pack_id,
        base_pack_version=base_pack_version,
        diagnostic_run_id=operator_input_id,  # audit label (operator-input)
        model_id=model_id,
        proposed=(),
        pack_yaml=pack_yaml,
        skill_evidence=skill_evidence or {},
    )
    validate_overlay(draft)
    return draft


def validate_overlay(draft: OverlayDraft) -> None:
    """Enforce skill-authored overlay restrictions.

    * Overlay MUST NOT introduce any silver/gold node definitions
      (skill never authors SQL templates).
    * Overlay ``overrides`` may carry ONLY one of two sanctioned shapes:

      (a) a **bronze** override (target ``bronze/<id>``) carrying **at least one**
          of the three sanctioned bronze keys — ``outputSchema`` (type-overlay),
          ``requiredColumns`` (add a source-column assertion), and/or
          ``relaxRequiredColumns`` (acknowledged removal of a required column, each
          entry with a mandatory ``reason``). Any combination of the three is
          allowed; a bronze entry carrying none of them is a no-op and is rejected.
          These three are exactly the bronze mechanisms the medallion-author skill
          is documented to author (see ``skills/medallion-author/SKILL.md`` "Bronze
          requiredColumns overlay", and the ``/medallion-author`` routing in
          mart-author / fusion-drift-doctor / aidpf-error-triage). The
          ``relaxRequiredColumns`` safety is enforced by the schema (mandatory
          ``reason``) and the engine (AIDPF-2063), not here.

      (b) a **silver/gold** guarded full replacement (target ``silver/<id>`` /
          ``gold/<id>`` carrying ONLY a ``replaceNode`` block).

      Forbidden in every case: ``sql`` / ``profile`` / ``quality`` (free-form
      SQL-template authoring) and a bronze ``replaceNode`` (silver/gold-only).
    * Overlay MUST carry a non-empty ``provenance.skill_id``,
      ``skill_version``, ``model_id``, ``diagnostic_run_id``.
    """
    pack = draft.pack_yaml
    for key, entry in (pack.overrides or {}).items():
        # (a) Bronze override: at least one of the three sanctioned bronze keys,
        # and none of the forbidden ones (sql/profile/quality/replaceNode).
        sanctioned_bronze = (
            key.startswith("bronze/")
            and (
                entry.output_schema is not None
                or entry.required_columns is not None
                or entry.relax_required_columns is not None
            )
            and entry.sql is None
            and entry.profile is None
            and entry.quality is None
            and entry.replace_node is None
        )
        # (b) Silver/gold guarded full replacement: a `replaceNode` block (and
        # nothing else — the schema already enforces replaceNode mutual-exclusion).
        sanctioned_replace = (
            (key.startswith("silver/") or key.startswith("gold/"))
            and entry.replace_node is not None
        )
        if not (sanctioned_bronze or sanctioned_replace):
            raise OverlayValidationError(
                f"Skill-authored overlay override {key!r} is not a sanctioned shape. "
                f"Allowed: a bronze override (`bronze/<id>`) carrying at least one of "
                f"`outputSchema` / `requiredColumns` / `relaxRequiredColumns`, or a "
                f"silver/gold `replaceNode` full replacement (`silver|gold/<id>`). "
                f"Forbidden: `sql` / `profile` / `quality` (free-form SQL authoring), "
                f"a bronze `replaceNode` (silver/gold-only), and an empty bronze "
                f"override carrying none of the three bronze keys."
            )
    if pack.provenance is None:
        raise OverlayValidationError("Overlay missing `provenance` block.")
    missing = [
        n
        for n, v in (
            ("skill_id", pack.provenance.skill_id),
            ("skill_version", pack.provenance.skill_version),
            ("model_id", pack.provenance.model_id),
        )
        if not v
    ]
    if missing:
        raise OverlayValidationError(
            f"Overlay provenance missing required fields: {missing}"
        )
    # Exactly one trigger id: a runtime diagnostic run id (diagnostic-driven
    # path) XOR an operator-input id (e.g. COA-depth from AIDPF-2015, which
    # writes no diagnostic). Neither → can't audit the trigger; both →
    # ambiguous / a faked diagnostic id.
    has_diag = bool(pack.provenance.diagnostic_run_id)
    has_op = bool(pack.provenance.operator_input_id)
    if has_diag == has_op:
        raise OverlayValidationError(
            "Overlay provenance must carry exactly one of `diagnosticRunId` "
            "(diagnostic-driven) or `operatorInputId` (operator-input mode), "
            f"not {'both' if has_diag else 'neither'}."
        )
    if pack.provenance.skill_id != SKILL_ID:
        raise OverlayValidationError(
            f"Overlay provenance.skill_id={pack.provenance.skill_id!r} does "
            f"not match the medallion-author skill_id ({SKILL_ID!r})."
        )


def _build_provenance(
    *,
    diagnostic_run_id: str | None = None,
    operator_input_id: str | None = None,
    model_id: str,
    proposals: dict[str, SkillProposalRecord],
    incremental_impact: dict[str, IncrementalImpact],
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    block: dict[str, Any] = {
        "skillId": SKILL_ID,
        "skillVersion": SKILL_VERSION,
        "modelId": model_id,
        "generatedAt": datetime.now(tz=timezone.utc).isoformat(),
        "proposals": {
            name: {
                "candidateAdded": p.candidate_added,
                "confidence": p.confidence,
                "reasoning": p.reasoning,
            }
            for name, p in proposals.items()
        },
        "incrementalImpact": {
            name: i.model_dump(by_alias=True)
            for name, i in incremental_impact.items()
        }
        if incremental_impact
        else None,
    }
    # Exactly one trigger id (validate_overlay enforces the XOR).
    if operator_input_id is not None:
        block["operatorInputId"] = operator_input_id
        block["trigger"] = "operator_input"
    else:
        block["diagnosticRunId"] = diagnostic_run_id
        block["trigger"] = "diagnostic"
    if evidence:
        block["evidence"] = evidence
    return block


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------


def write_overlay(
    draft: OverlayDraft,
    *,
    workdir: Path,
    overwrite: bool = False,
) -> Path:
    """Write the validated overlay YAML to
    ``<workdir>/overlays/<overlay_name>/pack.yaml``.

    Args:
        draft: validated :class:`OverlayDraft`.
        workdir: persistence-root anchor; the skill passes
            ``bundle_path.resolve().parent``.
        overwrite: when ``False`` (default), refuse to write into an
            existing overlay directory (operator's prior draft must
            be reviewed / removed before overwriting).

    Returns:
        Absolute path to the written ``pack.yaml``.
    """
    workdir_resolved = workdir.resolve()
    overlays_root = workdir_resolved / "overlays"
    overlay_dir = overlays_root / draft.overlay_name
    assert_within_root(overlay_dir, overlays_root, field="overlay.name")

    if overlay_dir.exists() and not overwrite:
        raise FileExistsError(
            f"overlay directory already exists: {overlay_dir}. "
            f"Pass overwrite=True to replace it."
        )

    overlay_dir.mkdir(parents=True, exist_ok=True)
    target = overlay_dir / "pack.yaml"
    assert_within_root(target, overlay_dir, field="overlay.pack.yaml")

    payload = draft.pack_yaml.model_dump(mode="json", by_alias=True, exclude_none=True)
    rendered = yaml.safe_dump(payload, sort_keys=False, default_flow_style=False)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(rendered, encoding="utf-8")
    tmp.replace(target)
    return target


def write_resolutions(
    draft: OverlayDraft,
    *,
    workdir: Path,
    tenant: str,
) -> Path | None:
    """Conditionally write ``resolutions.json``.

    Emits ONLY for picks whose ``outcome`` is ``MultiMatch`` or
    ``RefreshChange``. Initial AutoResolved picks would fail feature
    #2's validator (Rule 7 — extraneous entry) so this function
    returns ``None`` when no such picks exist.

    Args:
        draft: the validated overlay draft.
        workdir: persistence-root anchor.
        tenant: ``bundle.contentPack.profile``. Stamped into the
            resolutions file so feature #2's validator accepts it.

    Returns:
        Absolute path to the written file, OR ``None`` if no
        scripted entries are needed.
    """
    scripted = [
        p
        for p in draft.proposed
        if p.outcome in ("MultiMatch", "RefreshChange")
    ]
    if not scripted:
        return None

    validate_path_segment(tenant, field="resolutions.tenant")

    resolutions = ResolutionsInputV1.model_validate(
        {
            "schemaVersion": 1,
            "tenant": tenant,
            "resolutions": [
                {
                    "name": p.vp_name,
                    "kind": p.kind,
                    "chosenCandidate": p.candidate,
                }
                for p in scripted
            ],
        }
    )

    workdir_resolved = workdir.resolve()
    overlays_root = workdir_resolved / "overlays"
    overlay_dir = overlays_root / draft.overlay_name
    assert_within_root(overlay_dir, overlays_root, field="overlay.name")
    target = overlay_dir / "resolutions.json"
    assert_within_root(target, overlay_dir, field="overlay.resolutions")

    overlay_dir.mkdir(parents=True, exist_ok=True)
    payload = resolutions.model_dump_json(by_alias=True, indent=2) + "\n"
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(target)
    return target


def write_skill_evidence(
    draft: OverlayDraft,
    *,
    workdir: Path,
) -> Path:
    """Write the skill's own audit trail to ``skill-evidence.json``.

    Separate from feature #2's evidence snapshot — this file captures
    the skill's reasoning + operator decisions for ops-side review;
    feature #2 only records the final pinned values + the
    `mechanism: skill_proposed` mechanism + the `skill_version` for
    correlation.
    """
    workdir_resolved = workdir.resolve()
    overlay_dir = workdir_resolved / "overlays" / draft.overlay_name
    assert_within_root(
        overlay_dir, workdir_resolved / "overlays", field="overlay.name"
    )
    target = overlay_dir / "skill-evidence.json"

    payload: dict[str, Any] = {
        "schemaVersion": 1,
        "skillId": SKILL_ID,
        "skillVersion": SKILL_VERSION,
        "modelId": draft.model_id,
        "diagnosticRunId": draft.diagnostic_run_id,
        "generatedAt": datetime.now(tz=timezone.utc).isoformat(),
        "overlayName": draft.overlay_name,
        "basedOnPack": f"{draft.base_pack_id}@{draft.base_pack_version}",
        "proposed": [
            {
                "vpName": p.vp_name,
                "kind": p.kind,
                "candidate": p.candidate,
                "confidence": p.confidence,
                "reasoning": p.reasoning,
                "outcome": p.outcome,
            }
            for p in draft.proposed
        ],
        "extras": draft.skill_evidence,
    }
    overlay_dir.mkdir(parents=True, exist_ok=True)
    import json

    rendered = json.dumps(payload, indent=2, sort_keys=False) + "\n"
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(rendered, encoding="utf-8")
    tmp.replace(target)
    return target


__all__ = [
    "OverlayDraft",
    "OverlayValidationError",
    "PickOutcome",
    "ProposedCandidate",
    "draft_overlay",
    "draft_type_overlay",
    "validate_overlay",
    "write_overlay",
    "write_resolutions",
    "write_skill_evidence",
]
