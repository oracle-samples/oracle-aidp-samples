"""Diagnostic artifact schema.

Bootstrap writes one file per failing concern under
``<workdir>/.aidp/diagnostics/<run_id>/`` when mechanical resolution
cannot proceed. ``medallion-author`` consumes these files to draft overlays;
other tools can consume the same contract because Pydantic models plus a
documented schema version are the public surface.

Path-naming uses a per-failure discriminator so a single bootstrap run
can produce multiple no-match artifacts without collision:

```
.aidp/diagnostics/<run_id>/
  AIDPF-1020.json                     # identity gate (one per run)
  AIDPF-2010__<vp-name>.json          # one per failing columnAlias
  AIDPF-2011__<vp-name>.json          # one per failing semanticVariant
```

Bootstrap collects ALL failures across the walk loop before exiting
(no early-exit on first failure); skill reads the whole directory to
assemble full recovery context.

Bootstrap's ``--refresh`` resolves drift, emitting 2010 / 2011 only when
re-walk fails. Runtime preflight emits ``AIDPF-2012`` for schema drift using
the same diagnostic directory.

Schema-version forward compatibility: consumers ignore unknown top-level
fields; a future schemaVersion=2 model can add fields without breaking v1
consumers.
"""

from __future__ import annotations

import errno
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Error codes
# ---------------------------------------------------------------------------

AIDPF_1020_OPERATOR_IDENTITY_UNRESOLVED = "AIDPF-1020"
"""Operator identity cannot be resolved from --operator / AIDP_OPERATOR / $USER."""

AIDPF_2010_COLUMN_ALIAS_UNRESOLVED = "AIDPF-2010"
"""``required: true`` ``columnAliases.<name>`` has no matching candidate on the tenant's bronze."""

AIDPF_2011_SEMANTIC_VARIANT_UNRESOLVED = "AIDPF-2011"
"""``required: true`` ``semanticVariants.<name>`` has no matching detect clause on the tenant's bronze."""

AIDPF_2012_SCHEMA_DRIFT_DETECTED = "AIDPF-2012"
"""Bronze schema fingerprint drift detected at runtime preflight.
Live bronze fingerprint differs from the value pinned in the tenant profile;
the run blocks until the operator runs ``aidp-fusion-autopilot bootstrap --refresh``."""

AIDPF_4070_BRONZE_TYPE_MISMATCH = "AIDPF-4070"
"""A bronze node's declared outputSchema type differs from the live PVO type,
detected by the pre-extract / post-write type gate. Skill-recoverable: draft a
bronze type-overlay retyping each column to the live (materialised) type."""

AIDPF_4071_BRONZE_SOURCE_COLUMN_MISSING = "AIDPF-4071"
"""A column the pack declares for a bronze node is absent from the live PVO,
detected by the pre-ingest source-schema gate (sql_runner Step 3). Diagnostic
at ``<workdir>/.aidp/diagnostics/<run_id>/AIDPF-4071__<node>.json`` carries the
missing column(s) + the live PVO column list (name+type). Consumed by
``medallion-author``: a present-but-renamed column → columnAlias overlay; a
present-but-wrong-type column is caught instead by AIDPF-4070 post-write."""

AIDPF_2047_CLUSTER_BOOTSTRAP_PREDISPATCH = "AIDPF-2047"
"""Cluster-mode bootstrap pre-dispatch readiness failure.
CLI-level only (no artifact). Sub-reason in the message: ``missing_config`` /
``aidp_rest_probe_failed`` / ``conflicting_flags``. Operator fixes the
CLI / config and reruns; not a skill-recoverable condition."""

AIDPF_2048_CLUSTER_BOOTSTRAP_DISPATCH_FAILED = "AIDPF-2048"
"""Cluster-mode probe dispatch failed before producing a valid marker.
Diagnostic at ``<workdir>/.aidp/diagnostics/<run_id>/AIDPF-2048.json`` carries
the failed step and cause; operator-actionable (re-auth, fix cluster, retry);
NOT consumed by ``medallion-author`` skill."""

AIDPF_2049_CLUSTER_BOOTSTRAP_MARKER_INVALID = "AIDPF-2049"
"""Cluster ran but the laptop could not use the marker
(envelope missing, cluster reported error, marker-version mismatch,
validation failure). Diagnostic at ``<workdir>/.aidp/diagnostics/<run_id>/AIDPF-2049.json``
+ companion ``cluster_stdout.log`` carries the full cluster output.
Operator-actionable; NOT consumed by ``medallion-author`` skill."""


# ---------------------------------------------------------------------------
# Failure payload sub-models
# ---------------------------------------------------------------------------


class CandidateProbeOutcome(BaseModel):
    """Per-candidate probe result captured during a walker no-match.

    Skill (feature #3) reads each outcome to understand WHY the candidate
    failed — was the column simply absent, or did its detect-clause fail
    for a semantic variant?
    """

    model_config = ConfigDict(extra="forbid")

    candidate: str
    """The candidate's logical id (column name for columnAliases; candidate
    id like ``cancelled_date`` for semanticVariants)."""

    outcome: Literal["column_not_found", "detect_clause_failed"]
    """Why this candidate was rejected. ``column_not_found`` is the
    columnAlias case (physical column doesn't exist). ``detect_clause_failed``
    is the semanticVariant case where the detect clause's required column
    is absent."""

    detail: str | None = None
    """Human-readable extension — e.g. ``"detect.columnExists=ApInvoicesCancelledFlag"``
    when the failing candidate was a semantic variant."""


class ObservedColumn(BaseModel):
    """One column observed on the tenant's bronze schema."""

    model_config = ConfigDict(extra="forbid")

    name: str
    type: str
    nullable: bool = True


class VariationPointFailure(BaseModel):
    """The structured failure context for ``AIDPF-2010`` / ``AIDPF-2011``.

    Skill reads this to author an overlay extending the candidate list,
    or to surface the failure to the operator in human-readable form.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    name: str
    """Variation-point name (e.g. ``invoice_currency_code``)."""

    kind: Literal["columnAliases", "semanticVariants"]
    """Which variation-point family this name belongs to."""

    applies_to: str = Field(alias="appliesTo")
    """The bronze table the variation point targets (e.g. ``bronze.ap_invoices``)."""

    candidates_tried: list[CandidateProbeOutcome] = Field(alias="candidatesTried")
    """Per-candidate walker result, in priority order."""

    observed_bronze_schema: list[ObservedColumn] = Field(alias="observedBronzeSchema")
    """Columns present in the tenant's bronze table at probe time. Skill uses
    these to suggest a candidate to add to an overlay."""

    prior_pinned: str | None = Field(default=None, alias="priorPinned")
    """Value from the prior profile when running ``--refresh``; ``None`` on
    initial onboarding."""


class IdentityProbeFailure(BaseModel):
    """Structured failure context for ``AIDPF-1020``.

    Records what env-var lookups bootstrap probed; skill can advise the
    operator which one to set.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    probed_sources: list[str] = Field(alias="probedSources")
    """Identity sources probed in precedence order:
    ``"--operator"``, ``"AIDP_OPERATOR"``, ``"USER"``."""

    non_empty_sources: list[str] = Field(alias="nonEmptySources", default_factory=list)
    """Subset of ``probed_sources`` that were set to a non-empty / non-whitespace
    value but were still rejected (currently always empty — bootstrap accepts
    any non-empty value; reserved for future stricter validation)."""


# ---------------------------------------------------------------------------
# Artifact models
# ---------------------------------------------------------------------------


class DiagnosticArtifactBase(BaseModel):
    """Shared header for every diagnostic artifact."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    schema_version: Literal[1] = Field(default=1, alias="schemaVersion")
    """Bootstrap-diagnostic schema version. Bumped on breaking changes."""

    run_id: str = Field(alias="runId")
    """Bootstrap-run identifier; matches the
    ``.aidp/diagnostics/<run_id>/`` directory."""

    tenant: str | None
    """Tenant identifier. ``None`` only on ``AIDPF-1020`` — identity gate
    fires before tenant context is loaded."""

    error_code: str = Field(alias="errorCode")
    """One of ``AIDPF-1020`` / ``AIDPF-2010`` / ``AIDPF-2011``."""

    error_message: str = Field(alias="errorMessage")
    """Human-readable explanation of the failure."""

    generated_at: datetime = Field(alias="generatedAt")
    """UTC timestamp of artifact creation."""


class VariationPointDiagnosticV1(DiagnosticArtifactBase):
    """Diagnostic artifact for one unresolved variation point."""

    error_code: Literal["AIDPF-2010", "AIDPF-2011"] = Field(alias="errorCode")
    variation_point: VariationPointFailure = Field(alias="variationPoint")


class IdentityDiagnosticV1(DiagnosticArtifactBase):
    """Diagnostic artifact for an unresolved operator identity (AIDPF-1020)."""

    error_code: Literal["AIDPF-1020"] = Field(alias="errorCode")
    tenant: None = None
    identity_probe: IdentityProbeFailure = Field(alias="identityProbe")


class BronzeSourceColumnMissingV1(DiagnosticArtifactBase):
    """Diagnostic for a bronze node declaring a column the live PVO lacks (AIDPF-4071).

    Emitted by the pre-ingest source-schema gate. ``medallion-author``
    reads it to author a columnAlias overlay: the missing column is almost
    always a *renamed* column present in ``pvo_columns`` under a different
    physical name (e.g. ``ApPayHistDist…`` → ``ApPaymentHistDists…``).
    """

    error_code: Literal["AIDPF-4071"] = Field(alias="errorCode")
    node: str
    """Bronze node id (e.g. ``ap_payments``)."""

    datastore: str
    """Full BICC PVO datastore path the node extracts from."""

    missing_columns: list[str] = Field(alias="missingColumns")
    """Declared columns absent from the live PVO (case-insensitive)."""

    pvo_columns: list[ObservedColumn] = Field(alias="pvoColumns")
    """Every column the live PVO exposes (name + type) — the candidate set
    for resolving each missing column to its real physical name."""


class TypeMismatch(BaseModel):
    """One column whose declared type differs from the live PVO type."""

    model_config = ConfigDict(extra="forbid")

    column: str
    declared: str
    """The pack-declared Spark type (e.g. ``decimal(38,30)``)."""
    materialised: str
    """The live PVO / materialised type (e.g. ``decimal(18,0)``)."""


class BronzeTypeMismatchV1(DiagnosticArtifactBase):
    """Diagnostic for a bronze node whose declared outputSchema type differs
    from the live PVO type (AIDPF-4070).

    Emitted by the pre-extract / post-write type gate. ``medallion-author``
    reads it to draft a bronze type-overlay (an ``outputSchema`` block override
    or a same-id bronze file) retyping each mismatched column to ``materialised``.
    """

    error_code: Literal["AIDPF-4070"] = Field(alias="errorCode")
    node: str
    """Bronze node id (e.g. ``erp_suppliers``)."""

    datastore: str
    """Full BICC PVO datastore path the node extracts from."""

    type_mismatches: list[TypeMismatch] = Field(alias="typeMismatches")
    """Declared-vs-live type mismatches; the type-overlay retypes each to
    ``materialised``."""

    pvo_columns: list[ObservedColumn] = Field(alias="pvoColumns")
    """Every column the live PVO exposes (name + type)."""


# ---------------------------------------------------------------------------
# Schema-drift artifact (AIDPF-2012)
# ---------------------------------------------------------------------------


class ColumnTypeChange(BaseModel):
    """One observed column whose type changed since pin time.

    Populated only when a pinned schema snapshot is available. The model exists
    in schemaVersion=1 so richer drift artifacts can be emitted without a
    schema-version bump.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    name: str
    prior_type: str = Field(alias="priorType")
    current_type: str = Field(alias="currentType")


class DatasetSchemaDelta(BaseModel):
    """Per-dataset description of what changed since pin time.

    All three lists are optional/default-empty. When no pinned schema snapshot
    is present, only ``affectedVariationPoints`` is computable from the pinned
    profile and live observation.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    dataset_id: str = Field(alias="datasetId")
    added_columns: list[ObservedColumn] = Field(
        default_factory=list, alias="addedColumns"
    )
    removed_columns: list[ObservedColumn] = Field(
        default_factory=list, alias="removedColumns"
    )
    type_changed_columns: list[ColumnTypeChange] = Field(
        default_factory=list, alias="typeChangedColumns"
    )


class AffectedVariationPoint(BaseModel):
    """Per-pinned-VP impact summary for the drift artifact.

    Computed by diffing each ``profile.resolved.{column,semantic}.<name>``
    pinned candidate against the live observed bronze schema. ``True``
    means the pinned column still exists on bronze (drift cause is
    elsewhere); ``False`` means the pinned column was dropped /
    renamed (skill recovery target).
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    name: str
    kind: Literal["columnAliases", "semanticVariants"]
    pinned_candidate: str = Field(alias="pinnedCandidate")
    still_exists_on_bronze: bool = Field(alias="stillExistsOnBronze")


class SchemaDriftFailure(BaseModel):
    """Structured drift-context payload for the AIDPF-2012 artifact."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    prior_fingerprint: str = Field(alias="priorFingerprint")
    current_fingerprint: str = Field(alias="currentFingerprint")
    pinned_at: datetime = Field(alias="pinnedAt")
    """When bootstrap pinned the prior fingerprint (from the profile)."""

    dataset_deltas: list[DatasetSchemaDelta] = Field(
        default_factory=list, alias="datasetDeltas"
    )
    """Per-dataset column-level deltas. Empty when no pinned schema snapshot is
    available because the fingerprint hash is one-way."""

    affected_variation_points: list[AffectedVariationPoint] = Field(
        default_factory=list, alias="affectedVariationPoints"
    )
    """Per-pinned-VP impact. Computable from pinned profile + live
    observation; populated unconditionally on drift. ``medallion-author``
    consumes this to decide which VPs need re-resolution."""


class ClusterDispatchFailure(BaseModel):
    """Payload of ``AIDPF-2048`` cluster dispatch failure.

    Captures which step of the dispatch chain raised + the typed
    exception's cause + the cluster/workspace coords for operator
    triage. Operator-actionable, not skill-actionable.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    failed_step: Literal[
        "build_wheel",
        "stage_pack",
        "upload_notebook",
        "create_notebook_job",
        "submit_run",
        "poll_run",
        "fetch_output",
    ] = Field(alias="failedStep")
    """Stable artifact-side enum derived from the dispatch helper's
    typed-exception ``.code`` ClassVar (see
    ``commands.cluster_bootstrap_probe._dispatch_step_from_code``)."""

    cause_type: str = Field(alias="causeType")
    """``type(exc).__name__`` of the underlying exception."""

    cause_message: str = Field(alias="causeMessage")
    """``str(exc)[:2000]`` — bounded to keep the artifact small."""

    workspace_path: str | None = Field(default=None, alias="workspacePath")
    """Server-side notebook path the helper attempted to upload to.
    ``None`` when the failure happened before path construction (wheel
    build / pack staging)."""

    cluster_key: str | None = Field(default=None, alias="clusterKey")
    """The cluster UUID dispatch was targeting. ``None`` for very-early
    failures."""

    run_state: str | None = Field(default=None, alias="runState")
    """Terminal state from ``poll_run`` when the failure was a RUN_FAILED
    outcome (one of ``FAILED`` / ``CANCELED`` / ``TIMED_OUT``)."""

    poll_elapsed_seconds: float | None = Field(
        default=None, alias="pollElapsedSeconds"
    )


class ClusterDispatchDiagnosticV1(DiagnosticArtifactBase):
    """Diagnostic artifact for ``AIDPF-2048``.

    One file per dispatch-failure at
    ``<workdir>/.aidp/diagnostics/<run_id>/AIDPF-2048.json``. No
    discriminator — bootstrap attempts one cluster dispatch per run,
    so at most one 2048 artifact exists per run."""

    error_code: Literal["AIDPF-2048"] = Field(alias="errorCode")
    tenant: str
    cluster_dispatch: ClusterDispatchFailure = Field(alias="clusterDispatch")


class ClusterMarkerFailure(BaseModel):
    """Payload of ``AIDPF-2049`` cluster marker invalid failure."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    kind: Literal[
        "envelope_missing",
        "cluster_reported_error",
        "marker_version_unsupported",
        "validation_failed",
    ]
    """Discriminator across the four marker-failure flavours.

    * ``envelope_missing`` — no ``MARKER_BEGIN/END`` envelope in the
      executed-notebook stdout.
    * ``cluster_reported_error`` — envelope present with ``ok=false`` —
      ``cluster_error_*`` fields carry the in-cell exception details.
    * ``marker_version_unsupported`` — envelope present, inner marker
      ``markerVersion`` ≠ 1.
    * ``validation_failed`` — envelope or marker failed Pydantic
      validation for any other reason; ``validation_errors`` carries
      the Pydantic error string(s).
    """

    cluster_error_type: str | None = Field(
        default=None, alias="clusterErrorType"
    )
    """Set iff ``kind == "cluster_reported_error"``."""

    cluster_error_message: str | None = Field(
        default=None, alias="clusterErrorMessage"
    )

    cluster_traceback: str | None = Field(
        default=None, alias="clusterTraceback"
    )

    validation_errors: list[str] = Field(
        default_factory=list, alias="validationErrors"
    )
    """Pydantic error strings — populated for ``validation_failed`` and
    ``marker_version_unsupported`` kinds."""

    stdout_excerpt: str = Field(alias="stdoutExcerpt")
    """Last ~4 KiB of the cluster's stdout — what goes inside the
    artifact JSON. Full stdout is mirrored to the companion
    ``cluster_stdout.log`` file."""

    stdout_log_path: str = Field(alias="stdoutLogPath")
    """Relative path (from the diagnostics dir) to the companion
    ``cluster_stdout.log`` file. Always set — the writer guarantees
    the file exists alongside the JSON."""


class ClusterMarkerDiagnosticV1(DiagnosticArtifactBase):
    """Diagnostic artifact for ``AIDPF-2049``.

    One file per marker-failure at
    ``<workdir>/.aidp/diagnostics/<run_id>/AIDPF-2049.json``. Mutually
    exclusive with ``AIDPF-2048.json`` — marker parsing only runs on
    a successful fetch. A companion ``cluster_stdout.log`` lives in
    the same directory."""

    error_code: Literal["AIDPF-2049"] = Field(alias="errorCode")
    tenant: str
    cluster_marker: ClusterMarkerFailure = Field(alias="clusterMarker")


class SchemaDriftDiagnosticV1(DiagnosticArtifactBase):
    """Diagnostic artifact for AIDPF-2012 schema-fingerprint drift.

    One file per drifted run at
    ``<workdir>/.aidp/diagnostics/<run_id>/AIDPF-2012.json`` — no
    discriminator (one fingerprint per run, not per-VP).
    """

    error_code: Literal["AIDPF-2012"] = Field(alias="errorCode")
    tenant: str
    """Drift always happens in a known-tenant context (unlike 1020
    where tenant is unknown at gate-fire time). Override the base's
    ``str | None`` to require a value."""

    schema_drift: SchemaDriftFailure = Field(alias="schemaDrift")


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------


class DiagnosticArtifactAlreadyExistsError(FileExistsError):
    """Raised when two bootstrap calls reuse the same ``run_id`` and
    target the same artifact path.

    Inherits from ``FileExistsError`` so callers can also catch the
    stdlib-typed exception (e.g. broad exception handlers in test
    harnesses)."""


def _diagnostics_dir(workdir: Path, run_id: str) -> Path:
    return workdir / ".aidp" / "diagnostics" / run_id


def _atomic_write_json(path: Path, payload: str) -> None:
    """Write ``payload`` to ``path`` atomically.

    Refuses to overwrite an existing file — two bootstrap runs reusing
    the same ``run_id`` indicates a caller bug or operator error, and a
    silent overwrite would destroy the prior run's evidence.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise DiagnosticArtifactAlreadyExistsError(
            errno.EEXIST,
            f"refusing to overwrite existing diagnostic artifact",
            str(path),
        )
    # Write to a sibling temp file in the same directory so os.replace is atomic.
    fd, tmp_path = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(payload)
        os.replace(tmp_path, path)
    except BaseException:
        # Best-effort cleanup of the temp file if anything went wrong.
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def write_variation_diagnostic(
    workdir: Path,
    run_id: str,
    artifact: VariationPointDiagnosticV1,
) -> Path:
    """Write a variation-point diagnostic artifact.

    Path = ``<workdir>/.aidp/diagnostics/<run_id>/<errorCode>__<vpName>.json``.
    The ``<vpName>`` discriminator ensures multiple no-match failures in
    one bootstrap run produce distinct files.

    Args:
        workdir: persistence-root anchor; bootstrap passes
            ``bundle_path.resolve().parent``.
        run_id: bootstrap-run identifier.
        artifact: the diagnostic payload.

    Returns:
        The absolute path the artifact was written to.

    Raises:
        DiagnosticArtifactAlreadyExistsError: a file already exists at
            the target path (the ``run_id``/``vpName`` combination has
            been used before).
        UnsafePathSegmentError: ``run_id`` or
            ``artifact.variation_point.name`` is not a safe filesystem
            segment, or the resolved target escapes
            ``<workdir>/.aidp/diagnostics/<run_id>/``.
    """
    from .path_segment import assert_within_root, validate_path_segment

    validate_path_segment(run_id, field="run_id")
    validate_path_segment(
        artifact.variation_point.name, field="variationPoint.name"
    )
    diag_dir = _diagnostics_dir(workdir, run_id).resolve()
    target = diag_dir / (
        f"{artifact.error_code}__{artifact.variation_point.name}.json"
    )
    assert_within_root(target, diag_dir, field="variationPoint.name")
    payload = artifact.model_dump_json(by_alias=True, indent=2) + "\n"
    _atomic_write_json(target, payload)
    return target


def write_identity_diagnostic(
    workdir: Path,
    run_id: str,
    artifact: IdentityDiagnosticV1,
) -> Path:
    """Write an identity-gate diagnostic artifact.

    Path = ``<workdir>/.aidp/diagnostics/<run_id>/AIDPF-1020.json``.
    Only one ``AIDPF-1020`` artifact per run (no discriminator); identity
    gate fires once.

    Raises:
        UnsafePathSegmentError: ``run_id`` is not a safe filesystem
            segment.
    """
    from .path_segment import assert_within_root, validate_path_segment

    validate_path_segment(run_id, field="run_id")
    diag_dir = _diagnostics_dir(workdir, run_id).resolve()
    target = diag_dir / "AIDPF-1020.json"
    assert_within_root(target, diag_dir, field="run_id")
    payload = artifact.model_dump_json(by_alias=True, indent=2) + "\n"
    _atomic_write_json(target, payload)
    return target


def write_schema_drift_diagnostic(
    workdir: Path,
    run_id: str,
    artifact: SchemaDriftDiagnosticV1,
) -> Path:
    """Write an AIDPF-2012 schema-drift diagnostic artifact.

    Path = ``<workdir>/.aidp/diagnostics/<run_id>/AIDPF-2012.json``.
    Only one ``AIDPF-2012`` artifact per run (no discriminator); drift
    is detected once at preflight.

    Raises:
        UnsafePathSegmentError: ``run_id`` is not a safe filesystem
            segment.
    """
    from .path_segment import assert_within_root, validate_path_segment

    validate_path_segment(run_id, field="run_id")
    diag_dir = _diagnostics_dir(workdir, run_id).resolve()
    target = diag_dir / "AIDPF-2012.json"
    assert_within_root(target, diag_dir, field="run_id")
    payload = artifact.model_dump_json(by_alias=True, indent=2) + "\n"
    _atomic_write_json(target, payload)
    return target


def write_bronze_source_column_missing_diagnostic(
    workdir: Path,
    run_id: str,
    artifact: BronzeSourceColumnMissingV1,
) -> Path:
    """Write an AIDPF-4071 bronze source-column-missing diagnostic.

    Path = ``<workdir>/.aidp/diagnostics/<run_id>/AIDPF-4071__<node>.json``.
    One artifact per failing bronze node (the node id discriminates), so a
    run that hits the gate on several nodes leaves one file each for
    ``medallion-author`` to resolve independently.

    Raises:
        UnsafePathSegmentError: ``run_id`` or ``artifact.node`` is not a
            safe filesystem segment.
    """
    from .path_segment import assert_within_root, validate_path_segment

    validate_path_segment(run_id, field="run_id")
    validate_path_segment(artifact.node, field="node")
    diag_dir = _diagnostics_dir(workdir, run_id).resolve()
    target = diag_dir / f"AIDPF-4071__{artifact.node}.json"
    assert_within_root(target, diag_dir, field="node")
    payload = artifact.model_dump_json(by_alias=True, indent=2) + "\n"
    _atomic_write_json(target, payload)
    return target


def write_bronze_type_mismatch_diagnostic(
    workdir: Path,
    run_id: str,
    artifact: BronzeTypeMismatchV1,
) -> Path:
    """Write an AIDPF-4070 bronze type-mismatch diagnostic.

    Path = ``<workdir>/.aidp/diagnostics/<run_id>/AIDPF-4070__<node>.json``.
    One artifact per failing bronze node so ``medallion-author`` can draft a
    type-overlay per node independently. Mirrors
    :func:`write_bronze_source_column_missing_diagnostic` (AIDPF-4071).

    Raises:
        UnsafePathSegmentError: ``run_id`` or ``artifact.node`` is not a safe
            filesystem segment.
    """
    from .path_segment import assert_within_root, validate_path_segment

    validate_path_segment(run_id, field="run_id")
    validate_path_segment(artifact.node, field="node")
    diag_dir = _diagnostics_dir(workdir, run_id).resolve()
    target = diag_dir / f"AIDPF-4070__{artifact.node}.json"
    assert_within_root(target, diag_dir, field="node")
    payload = artifact.model_dump_json(by_alias=True, indent=2) + "\n"
    _atomic_write_json(target, payload)
    return target


def write_cluster_dispatch_diagnostic(
    workdir: Path,
    run_id: str,
    artifact: ClusterDispatchDiagnosticV1,
) -> Path:
    """Write an AIDPF-2048 cluster-dispatch diagnostic artifact.

    Path = ``<workdir>/.aidp/diagnostics/<run_id>/AIDPF-2048.json``.
    Mutually exclusive with ``AIDPF-2049.json`` in the same run
    directory — dispatch failures fire before marker parsing.

    Raises:
        UnsafePathSegmentError: ``run_id`` is not a safe filesystem segment.
        DiagnosticArtifactAlreadyExistsError: a file already exists at
            the target path.
    """
    from .path_segment import assert_within_root, validate_path_segment

    validate_path_segment(run_id, field="run_id")
    diag_dir = _diagnostics_dir(workdir, run_id).resolve()
    target = diag_dir / "AIDPF-2048.json"
    assert_within_root(target, diag_dir, field="run_id")
    payload = artifact.model_dump_json(by_alias=True, indent=2) + "\n"
    _atomic_write_json(target, payload)
    return target


def write_cluster_marker_diagnostic(
    workdir: Path,
    run_id: str,
    artifact: ClusterMarkerDiagnosticV1,
    *,
    stdout_full: str,
) -> Path:
    """Write an AIDPF-2049 cluster-marker diagnostic artifact.

    Also writes the companion ``cluster_stdout.log`` file.

    Two files written in one call so the operator inspecting
    ``AIDPF-2049.json`` always has the matching log next to it:

    * ``<workdir>/.aidp/diagnostics/<run_id>/AIDPF-2049.json``
    * ``<workdir>/.aidp/diagnostics/<run_id>/cluster_stdout.log``

    The artifact's ``cluster_marker.stdoutLogPath`` field MUST equal
    ``"cluster_stdout.log"`` (the writer enforces this — programmer
    error if the caller passes a different value).

    Args:
        workdir: persistence-root anchor.
        run_id: bootstrap-run identifier.
        artifact: the diagnostic payload.
        stdout_full: untruncated cluster stdout. Written to the
            companion log file verbatim; the artifact JSON carries
            only the last ~4 KiB excerpt.

    Returns:
        The absolute path of the JSON artifact (the companion log
        sits next to it).
    """
    from .path_segment import assert_within_root, validate_path_segment

    validate_path_segment(run_id, field="run_id")
    diag_dir = _diagnostics_dir(workdir, run_id).resolve()
    if artifact.cluster_marker.stdout_log_path != "cluster_stdout.log":
        raise ValueError(
            f"ClusterMarkerFailure.stdoutLogPath must be "
            f"'cluster_stdout.log' (artifact + log are siblings); got "
            f"{artifact.cluster_marker.stdout_log_path!r}"
        )

    target = diag_dir / "AIDPF-2049.json"
    log_target = diag_dir / "cluster_stdout.log"
    assert_within_root(target, diag_dir, field="run_id")
    assert_within_root(log_target, diag_dir, field="run_id")

    payload = artifact.model_dump_json(by_alias=True, indent=2) + "\n"
    _atomic_write_json(target, payload)
    # Write the companion log separately — same atomic-write pattern.
    _atomic_write_json(log_target, stdout_full)
    return target


__all__ = [
    "AIDPF_1020_OPERATOR_IDENTITY_UNRESOLVED",
    "AIDPF_2010_COLUMN_ALIAS_UNRESOLVED",
    "AIDPF_2011_SEMANTIC_VARIANT_UNRESOLVED",
    "AIDPF_2012_SCHEMA_DRIFT_DETECTED",
    "AIDPF_2047_CLUSTER_BOOTSTRAP_PREDISPATCH",
    "AIDPF_2048_CLUSTER_BOOTSTRAP_DISPATCH_FAILED",
    "AIDPF_2049_CLUSTER_BOOTSTRAP_MARKER_INVALID",
    "AffectedVariationPoint",
    "CandidateProbeOutcome",
    "ClusterDispatchDiagnosticV1",
    "ClusterDispatchFailure",
    "ClusterMarkerDiagnosticV1",
    "ClusterMarkerFailure",
    "ColumnTypeChange",
    "DatasetSchemaDelta",
    "DiagnosticArtifactAlreadyExistsError",
    "DiagnosticArtifactBase",
    "IdentityDiagnosticV1",
    "IdentityProbeFailure",
    "ObservedColumn",
    "SchemaDriftDiagnosticV1",
    "SchemaDriftFailure",
    "VariationPointDiagnosticV1",
    "VariationPointFailure",
    "write_cluster_dispatch_diagnostic",
    "write_cluster_marker_diagnostic",
    "write_coa_gate_diagnostic",
    "write_identity_diagnostic",
    "write_schema_drift_diagnostic",
    "write_variation_diagnostic",
    "persist_run_diagnostics",
    "write_coa_metadata_diagnostic",
]


def write_coa_gate_diagnostic(
    workdir: Path,
    run_id: str,
    payload: dict,
) -> Path:
    """Write the structured COA gate-abort diagnostic (FR-15.9).

    Path = ``<workdir>/.aidp/diagnostics/<run_id>/AIDPF-<primary>__coa-gate.json``
    — keyed on the checkpoint's ACTUAL primary code (``payload["errorCode"]``),
    never a hard-coded 2018.

    Raises:
        UnsafePathSegmentError: ``run_id`` is not a safe filesystem segment.
    """
    from .path_segment import assert_within_root, validate_path_segment

    validate_path_segment(run_id, field="run_id")
    diag_dir = _diagnostics_dir(workdir, run_id).resolve()
    code = str(payload.get("errorCode") or "AIDPF-0000")
    target = diag_dir / f"{code}__coa-gate.json"
    assert_within_root(target, diag_dir, field="errorCode")
    _atomic_write_json(target, json.dumps(payload, indent=2) + "\n")
    return target


def write_schema_patch_provenance(
    workdir: Path,
    run_id: str,
    applied: dict,
) -> Path:
    """Persist the run's EFFECTIVE ``schemaPatches`` provenance (feature
    bronze-extract-schema-patch, FR-9).

    Path = ``<workdir>/.aidp/diagnostics/<run_id>/schema-patches__<run_id>.json``
    with the versioned shape
    ``{"schemaVersion": 1, "runId": …, "generatedAt": …,
    "appliedSchemaPatches": {dataset_id: [column, …]}}``.
    """
    from datetime import datetime, timezone

    from .path_segment import assert_within_root, validate_path_segment

    validate_path_segment(run_id, field="run_id")
    diag_dir = _diagnostics_dir(workdir, run_id).resolve()
    target = diag_dir / f"schema-patches__{run_id}.json"
    assert_within_root(target, diag_dir, field="run_id")
    payload = {
        "schemaVersion": 1,
        "runId": run_id,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "appliedSchemaPatches": {k: list(v) for k, v in applied.items()},
    }
    _atomic_write_json(target, json.dumps(payload, indent=2) + "\n")
    return target


def persist_run_diagnostics(workdir: Path, summary, *, log=None) -> None:
    """Persist ``RunSummary.diagnostics`` to ``.aidp/diagnostics/<run_id>/``.

    THE shared persister for BOTH execution paths (design D-14): the
    REST-dispatch path historically owned this loop while ``--inline`` runs
    never wrote diagnostics at all — leaving the remediation loop's artifact
    unreachable for inline aborts (a pre-existing gap for the 4071/4070
    artifacts too). Best-effort per entry: a malformed diagnostic must never
    mask the run's summary.
    """
    _log = log or (lambda msg: None)
    # schemaPatches provenance (FR-9) — one artifact per run when any
    # patched dataset landed (best-effort like every diagnostics write).
    applied = getattr(summary, "applied_schema_patches", None)
    if applied:
        try:
            path = write_schema_patch_provenance(
                workdir, summary.run_id, dict(applied)
            )
            _log(f"wrote diagnostic {path}")
        except Exception:  # noqa: BLE001 — best-effort
            pass
    for diag in getattr(summary, "diagnostics", ()) or ():
        try:
            code = diag.get("errorCode")
            if diag.get("kind") == "coa-gate":
                path = write_coa_gate_diagnostic(workdir, summary.run_id, dict(diag))
            elif code == "AIDPF-4071":
                artifact = BronzeSourceColumnMissingV1.model_validate(diag)
                path = write_bronze_source_column_missing_diagnostic(
                    workdir, summary.run_id, artifact
                )
            elif code == "AIDPF-4070":
                artifact = BronzeTypeMismatchV1.model_validate(diag)
                path = write_bronze_type_mismatch_diagnostic(
                    workdir, summary.run_id, artifact
                )
            else:
                continue
            _log(f"wrote diagnostic {path}")
        except Exception:  # noqa: BLE001 — diagnostic write is best-effort
            continue


def write_coa_metadata_diagnostic(
    workdir: Path,
    run_id: str,
    code: str,
    payload: dict,
) -> Path:
    """Write a COA metadata-resolution diagnostic (AIDPF-2021/2022/2023).

    Path = ``<workdir>/.aidp/diagnostics/<run_id>/<code>__coa-metadata.json``.
    """
    from .path_segment import assert_within_root, validate_path_segment

    validate_path_segment(run_id, field="run_id")
    diag_dir = _diagnostics_dir(workdir, run_id).resolve()
    target = diag_dir / f"{code}__coa-metadata.json"
    assert_within_root(target, diag_dir, field="code")
    _atomic_write_json(target, json.dumps(payload, indent=2) + "\n")
    return target
