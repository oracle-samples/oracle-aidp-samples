"""Evidence snapshot schema.

Every successful bootstrap run writes one snapshot file at
``<workdir>/evidence/<tenant>/<ISO-ts>.yaml`` recording exactly which
variation-point candidates matched, which one won, and the audit
metadata around the approval. The directory accumulates per the
audit rule: old snapshots are NEVER deleted on a ``--refresh``; the history
grows monotonically and provides the SOX-floor audit trail.

Schema shape:

```
schemaVersion: 1
tenant: <name>
generatedAt: <iso-ts>
runId: <run-id>
bronzeSchemaFingerprint: sha256:...
provenance:
  approvedBy:
    operator: <name>
    timestamp: <iso-ts>
    mechanism: auto_resolve | terminal_prompt | non_interactive | cli_flag | skill_proposed
  skillVersion: <ver-or-null>
  evidence:
    snapshots:
      - snapshotId: <ulid-or-iso-ts>
        capturedAt: <iso-ts>
        resolutions:
          - name: <vp-name>
            kind: columnAliases | semanticVariants
            chosenCandidate: <chosen>
            candidatesConsidered:
              - candidate: <c>
                outcome: matched | column_not_found | detect_clause_failed
            evidence: {...}
```

One file == one bootstrap run; the nested ``snapshots`` list is a
single entry per file because the directory accumulates one file per
run. The list-of-entries shape (rather than a single entry) lets
feature #3's skill batch-append multiple snapshots when committing
overlays in a single profile-update transaction.

The ``mechanism: auto_resolve`` value covers the single-candidate case where
no prompt fired. Readers treat unknown mechanism values as opaque audit
metadata for forward compatibility.
"""

from __future__ import annotations

import errno
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field

from .incremental_impact import IncrementalImpact


# ---------------------------------------------------------------------------
# Approval / provenance sub-models
# ---------------------------------------------------------------------------


ApprovalMechanism = Literal[
    "auto_resolve",
    "terminal_prompt",
    "non_interactive",
    "cli_flag",
    "skill_proposed",
    # Semantic-role (COA) mechanisms -- a semantic role is NEVER auto_resolve.
    "config_resolved",
    "operator_confirmed",
    "defaulted_convention",
    "legacy_unverified",
    "metadata_resolved",
]
"""Recorded under ``provenance.approvedBy.mechanism``. The ``auto_resolve``
value covers the no-prompt single-candidate physical-alias path.

The semantic-role values are used for COA role resolution and must never be
``auto_resolve`` (existence cannot prove a business meaning):

* ``config_resolved`` -- pinned from explicit ``profile.chartOfAccounts`` / CLI.
* ``operator_confirmed`` -- operator confirmed an interactive prompt / evidence.
* ``defaulted_convention`` -- pack conventional default, *explicitly accepted*.
* ``legacy_unverified`` -- back-derived from a legacy silent pin on ``--refresh``;
  correctness NOT established (emits a remediation warning, never silently upgraded).
* ``metadata_resolved`` -- reserved for Fusion flexfield-qualifier metadata (spike)."""


class ApprovedBy(BaseModel):
    """Operator-identity + approval-mechanism record."""

    model_config = ConfigDict(extra="forbid")

    operator: str
    timestamp: datetime
    mechanism: ApprovalMechanism


class CandidateConsidered(BaseModel):
    """Per-candidate walker outcome captured during a resolution."""

    model_config = ConfigDict(extra="forbid")

    candidate: str
    outcome: Literal["matched", "column_not_found", "detect_clause_failed"]
    detail: str | None = None


class ResolvedVariationPoint(BaseModel):
    """One variation-point resolution captured in a snapshot entry."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    name: str
    kind: Literal["columnAliases", "semanticVariants"]
    chosen_candidate: str = Field(alias="chosenCandidate")
    candidates_considered: list[CandidateConsidered] = Field(alias="candidatesConsidered")
    evidence: dict[str, Any] = Field(default_factory=dict)
    """Free-form measured evidence — row counts, non-null counts, whatever
    the prompt showed the operator. Skill consumes this for context."""

    incremental_impact: IncrementalImpact | None = Field(
        default=None, alias="incrementalImpact"
    )
    """Populated when the resolution came via a skill-authored overlay
    carrying ``provenance.incrementalImpact``.
    ``None`` for non-skill paths (preserves backwards-compat with
    pre-Phase-3b evidence files). See
    :class:`schema.incremental_impact.IncrementalImpact`."""


class SnapshotEntry(BaseModel):
    """One snapshot entry — corresponds to a single bootstrap run."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    snapshot_id: str = Field(alias="snapshotId")
    """ULID or ISO-8601 timestamp; matches the file's ``<ISO-ts>`` stem."""

    captured_at: datetime = Field(alias="capturedAt")
    resolutions: list[ResolvedVariationPoint]


class EvidenceContainer(BaseModel):
    """Holds the list of snapshot entries (one per run in this file)."""

    model_config = ConfigDict(extra="forbid")

    snapshots: list[SnapshotEntry]


class SnapshotProvenance(BaseModel):
    """Provenance block — approval metadata + nested evidence container."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    approved_by: ApprovedBy = Field(alias="approvedBy")
    skill_version: str | None = Field(default=None, alias="skillVersion")
    """``None`` for Tier-1 mechanical resolutions; populated by feature #3's
    skill on Tier-2 commit."""

    evidence: EvidenceContainer


# ---------------------------------------------------------------------------
# Top-level snapshot model
# ---------------------------------------------------------------------------


class EvidenceSnapshotV1(BaseModel):
    """One evidence snapshot file written by a successful bootstrap run."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    schema_version: Literal[1] = Field(default=1, alias="schemaVersion")
    tenant: str
    generated_at: datetime = Field(alias="generatedAt")
    run_id: str = Field(alias="runId")
    bronze_schema_fingerprint: str = Field(alias="bronzeSchemaFingerprint")
    provenance: SnapshotProvenance


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------


class EvidenceSnapshotAlreadyExistsError(FileExistsError):
    """Raised when a snapshot file with the same ``<ISO-ts>.yaml`` stem
    already exists. Indicates two bootstrap runs at the exact same
    timestamp — a caller bug given microsecond ISO timestamps."""


def _evidence_dir(workdir: Path, tenant: str) -> Path:
    return workdir / "evidence" / tenant


def _iso_timestamp_filename(generated_at: datetime) -> str:
    """Render ``generated_at`` as a filesystem-safe ISO-8601 stem.

    Replaces colons with hyphens so the filename is portable across
    Windows shares (which reject ``:`` in filenames). Example:
    ``2026-06-05T12-00-00.123456+00-00``.
    """
    iso = generated_at.isoformat()
    # Replace colon-bearing components but keep ``T`` + ``+``/``-`` separators.
    return iso.replace(":", "-")


def write_evidence_snapshot(
    workdir: Path,
    snapshot: EvidenceSnapshotV1,
) -> Path:
    """Write an evidence snapshot YAML file.

    Args:
        workdir: persistence-root anchor — pass
            ``bundle_path.resolve().parent``.
        snapshot: the snapshot payload. ``snapshot.tenant`` drives the
            sub-directory; ``snapshot.generated_at`` drives the filename.

    Returns:
        The absolute path the snapshot was written to.

    Raises:
        EvidenceSnapshotAlreadyExistsError: a snapshot file with the
            same ISO-timestamp stem already exists. Tests rely on this
            for the "preserves old snapshots" invariant — the writer
            cannot silently overwrite history.
        UnsafePathSegmentError: ``snapshot.tenant`` is not a safe
            filesystem segment, or the resolved target escapes
            ``<workdir>/evidence/``.
    """
    from .path_segment import assert_within_root, validate_path_segment

    validate_path_segment(snapshot.tenant, field="evidence.tenant")
    workdir_resolved = workdir.resolve()
    evidence_root = workdir_resolved / "evidence"
    tenant_dir = (evidence_root / snapshot.tenant).resolve()
    assert_within_root(tenant_dir, evidence_root, field="evidence.tenant")
    tenant_dir.mkdir(parents=True, exist_ok=True)
    target = tenant_dir / (_iso_timestamp_filename(snapshot.generated_at) + ".yaml")
    # Defence-in-depth: assert the timestamp-driven filename stays
    # under the tenant dir.
    assert_within_root(target, tenant_dir, field="evidence.snapshot")
    if target.exists():
        raise EvidenceSnapshotAlreadyExistsError(
            errno.EEXIST,
            "refusing to overwrite existing evidence snapshot",
            str(target),
        )

    # Render via Pydantic JSON dump → load → YAML dump to get deterministic
    # ordering with by-alias camelCase keys. Pydantic's ``model_dump`` with
    # ``mode="json"`` handles datetime / enum serialisation correctly.
    payload = snapshot.model_dump(mode="json", by_alias=True)
    rendered = yaml.safe_dump(payload, sort_keys=False, default_flow_style=False)

    fd, tmp_path = tempfile.mkstemp(
        prefix=target.name + ".",
        suffix=".tmp",
        dir=str(target.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(rendered)
        os.replace(tmp_path, target)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    return target


__all__ = [
    "ApprovalMechanism",
    "ApprovedBy",
    "CandidateConsidered",
    "EvidenceContainer",
    "EvidenceSnapshotAlreadyExistsError",
    "EvidenceSnapshotV1",
    "ResolvedVariationPoint",
    "SnapshotEntry",
    "SnapshotProvenance",
    "write_evidence_snapshot",
]
