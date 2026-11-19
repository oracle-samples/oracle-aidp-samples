"""Shared `IncrementalImpact` Pydantic model.

Lives here rather than in :mod:`evidence_snapshot` or
:mod:`medallion_pack` because BOTH modules need to reference it:

* :class:`schema.evidence_snapshot.ResolvedVariationPoint` carries an
  optional ``incremental_impact`` field that bootstrap populates on
  ``--refresh`` commit (and on initial commit when the overlay is
  skill-authored).
* :class:`schema.medallion_pack.PackProvenance` carries an optional
  ``incremental_impact`` map (per-VP) so the medallion-author skill
  can record the impact analysis in the overlay it drafts. Bootstrap
  reads from there at commit time.

Defining the model here keeps the import direction
``evidence_snapshot.py → incremental_impact.py`` and ``medallion_pack.py
→ incremental_impact.py`` — no circular dependency.

Schema version 1 is **additive-only** going forward per the
forward-compat rule.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


ChangeKind = Literal["initial", "promotion", "demotion"]
"""
* ``initial`` — first time this VP is resolved (no ``priorPinned``).
* ``promotion`` — refresh found a higher-priority candidate than the
  one previously pinned.
* ``demotion`` — refresh found that the previously-pinned candidate
  is no longer on bronze; walker fell back to a lower-priority
  candidate.
"""


RiskLabel = Literal["likely-rename", "likely-different-semantics", "unknown"]
"""LLM-classified risk:

* ``likely-rename`` — old + new column names are semantically near-
  identical (e.g. ``ApInvoicesCurrencyCode`` → ``ApInvoicesInvoiceCurrencyCode``
  is a Fusion rename, same data).
* ``likely-different-semantics`` — names suggest different data;
  re-MERGE of affected silver rows recommended.
* ``unknown`` — LLM can't tell from names alone; operator must inspect
  sample values.
"""


RemediationOption = Literal["A", "B", "C", "D", "E"]
"""Remediation menu:

* ``A`` — no action (rename only).
* ``B`` — surgical backfill MERGE (skill-drafted SQL; operator review).
* ``C`` — watermark rewind. **DEFERRED to v0.4** — requires a real
  `aidp-fusion-autopilot rewind` verb that knows both legacy and
  content-pack state schemas. The runbook drafter raises
  :class:`medallion_author.runbook.OptionDeferredError` when
  requested.
* ``D`` — targeted re-seed of affected nodes (the v0.3 default).
  Reuses the engine's existing tested seed path.
* ``E`` — full re-seed (audit-baseline reset, rare).
"""


class RemediationRecord(BaseModel):
    """The remediation choice recorded for one variation-point change."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    recommended: RemediationOption
    """The skill's recommended option based on risk + cost estimates."""

    operator_chose: RemediationOption = Field(alias="operatorChose")
    """What the operator actually picked (may differ from recommended)."""

    rationale: str
    """Human-readable explanation of the recommendation + the operator's
    choice. Shown in `remediation.md` and persisted in the audit trail."""


class IncrementalImpact(BaseModel):
    """Per-VP impact analysis recorded on both the overlay and the
    evidence snapshot.

    The medallion-author skill populates this when drafting an overlay;
    bootstrap mirrors it into the per-resolution evidence on the
    ``--refresh`` commit and on initial commit when the overlay is
    skill-authored.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    change_kind: ChangeKind = Field(alias="changeKind")
    prior_pinned: str | None = Field(default=None, alias="priorPinned")
    """The previously-pinned candidate value. ``None`` for ``initial``
    change kind."""

    new_candidate: str = Field(alias="newCandidate")
    """The candidate the skill proposed (and the operator approved)."""

    risk_label: RiskLabel = Field(alias="riskLabel")

    affected_nodes: list[str] = Field(default_factory=list, alias="affectedNodes")
    """Qualified silver/gold node IDs (e.g.
    ``["silver.supplier_spend", "gold.ap_aging"]``) the VP change
    cascades through. Populated by
    :func:`medallion_author.affected_nodes.compute_affected_nodes`."""

    remediation: RemediationRecord


__all__ = [
    "ChangeKind",
    "IncrementalImpact",
    "RemediationOption",
    "RemediationRecord",
    "RiskLabel",
]
