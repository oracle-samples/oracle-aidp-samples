"""medallion_author — the operator-side Tier-2 overlay-drafting helpers.

The Claude Code plugin skill at ``skills/medallion-author/``
imports from this package via thin re-export shims (sibling pattern to
``aidp-rest``'s ``client.py``). All implementation, validation, and unit
tests live here so the package is pip-installable + testable without
Claude Code.

The skill is **operator-initiated**; the engine has zero LLM dependency at
runtime. This package contains pure Python that reads diagnostic artifacts,
reasons over the observed bronze schema, and drafts overlay YAML for operator
review.

Public surface:

* :mod:`reader` — diagnostic artifact parsing (consumes
  ``schema.diagnostic_artifact``).
* :mod:`affected_nodes` — VP-to-consuming-nodes map by scanning
  pack SQL templates for ``{{ column.X }}`` / ``{{ semantic.X }}``
  tokens.
* :mod:`reasoner` — pure-Python candidate scoring + incremental-risk
  classification. The "LLM-driven" reasoning lives in the Claude Code
  chat loop; this module exposes deterministic ranking primitives the
  chat loop calls.
* :mod:`drafter` — overlay + resolutions + skill-evidence emission.
* :mod:`runbook` — Option A/B/D/E remediation drafter. Option C raises
  :class:`runbook.OptionDeferredError`.
* :mod:`knowledge_base` — loader for ``known-deltas.yaml``.

Skill version is loose-coupled with runtime execution: bumped on every
``known-deltas.yaml`` addition, recorded in evidence-snapshot provenance, but
NOT a plan-hash input.
"""

from __future__ import annotations

SKILL_ID = "aidp-fusion-medallion-author"
"""Stamped into every overlay's ``provenance.skillId``. Bootstrap reads
this to detect skill-authored overlays and record ``mechanism:
skill_proposed`` on the resolutions they drive."""

SKILL_VERSION = "0.1.0"
"""Bumped on every ``known-deltas.yaml`` change (patch for new entries,
minor for new variation-point kinds). Recorded in evidence snapshots
via :class:`schema.evidence_snapshot.SnapshotProvenance.skill_version`."""


__all__ = ["SKILL_ID", "SKILL_VERSION"]
