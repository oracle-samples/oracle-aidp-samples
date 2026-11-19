"""Diagnostic-artifact reader for the medallion-author skill.

Reads the artifact files bootstrap writes under
``<workdir>/.aidp/diagnostics/<run_id>/`` and surfaces them as parsed
Pydantic models the skill can reason over.

Refuse-to-proceed gates:

* ``AIDPF-1020.json`` present → identity gate failed; overlay drafting
  is the wrong response.
* Unknown ``schemaVersion`` → forward-compat rule says readers ignore
  unknown fields but MUST refuse on unknown major versions.
* Diagnostics directory missing / empty → nothing to draft.

The reader does NOT modify the artifacts; it's read-only.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from ..schema.diagnostic_artifact import (
    AIDPF_1020_OPERATOR_IDENTITY_UNRESOLVED,
    AIDPF_2010_COLUMN_ALIAS_UNRESOLVED,
    AIDPF_2011_SEMANTIC_VARIANT_UNRESOLVED,
    AIDPF_2012_SCHEMA_DRIFT_DETECTED,
    AIDPF_2048_CLUSTER_BOOTSTRAP_DISPATCH_FAILED,
    AIDPF_2049_CLUSTER_BOOTSTRAP_MARKER_INVALID,
    AIDPF_4070_BRONZE_TYPE_MISMATCH,
    AIDPF_4071_BRONZE_SOURCE_COLUMN_MISSING,
    BronzeSourceColumnMissingV1,
    BronzeTypeMismatchV1,
    IdentityDiagnosticV1,
    SchemaDriftDiagnosticV1,
    VariationPointDiagnosticV1,
)

# Cluster-dispatch failures are operator-actionable, not skill-recoverable.
# The reader recognises them by errorCode and surfaces them in a dedicated
# field so the `/medallion-author` skill can refuse with a targeted message.
_CLUSTER_DISPATCH_INFRASTRUCTURE_CODES = frozenset(
    {
        AIDPF_2048_CLUSTER_BOOTSTRAP_DISPATCH_FAILED,
        AIDPF_2049_CLUSTER_BOOTSTRAP_MARKER_INVALID,
    }
)

SUPPORTED_SCHEMA_VERSIONS: tuple[int, ...] = (1,)
"""Major schemaVersions the reader can parse. Bumped when the artifact
contract changes incompatibly (skill version bumps in lockstep)."""


@dataclass(frozen=True)
class DiagnosticReadResult:
    """Result of scanning a single ``<run_id>`` diagnostics directory.

    The skill reads this dataclass to decide:

    * Should we refuse? (identity gate, unknown version, empty dir)
    * If proceeding: which variation-point failures to draft for?
    """

    run_id: str
    """Bootstrap-run id parsed from the directory name."""

    run_dir: Path
    """Absolute path to ``<workdir>/.aidp/diagnostics/<run_id>/``."""

    variation_failures: list[VariationPointDiagnosticV1] = field(default_factory=list)
    """One entry per ``AIDPF-2010__<vp>.json`` or
    ``AIDPF-2011__<vp>.json`` found in the directory."""

    source_column_failures: list[BronzeSourceColumnMissingV1] = field(default_factory=list)
    """One entry per ``AIDPF-4071__<node>.json`` — a bronze node declaring
    a column the live PVO lacks. Skill-recoverable: resolve each by
    authoring a columnAlias overlay mapping the declared name to the
    renamed physical column found in the diagnostic's ``pvoColumns``."""

    type_mismatch_failures: list[BronzeTypeMismatchV1] = field(default_factory=list)
    """One entry per ``AIDPF-4070__<node>.json`` — a bronze node whose declared
    outputSchema type differs from the live PVO type. Skill-recoverable: draft a
    bronze type-overlay retyping each column to the diagnostic's
    ``materialised`` type (block override or same-id bronze file)."""

    identity_failure: IdentityDiagnosticV1 | None = None
    """Set if ``AIDPF-1020.json`` is present. Skill refuses to draft
    when this is non-None — identity gate must be fixed first."""

    schema_drift_failure: SchemaDriftDiagnosticV1 | None = None
    """Set if ``AIDPF-2012.json`` is present. Drift is bootstrap's domain; if
    a drift artifact is present without any 2010/2011 in the same directory,
    the skill refuses with a "wrong recovery flow" message (operator should
    run ``bootstrap --refresh`` first). If 2012 + 2010/2011 both present, the
    skill surfaces both and proceeds on the 2010/2011 work."""

    unknown_schema_paths: list[Path] = field(default_factory=list)
    """Artifact files whose ``schemaVersion`` is not in
    :data:`SUPPORTED_SCHEMA_VERSIONS`. Skill refuses to draft when
    non-empty as a forward-compat rule on major versions."""

    malformed_paths: list[Path] = field(default_factory=list)
    """Artifact files that failed JSON parse or Pydantic validation.
    Surface to the operator with the path so they can inspect."""

    cluster_dispatch_skipped_paths: list[Path] = field(default_factory=list)
    """Artifact files the reader recognised as cluster-dispatch failures
    (``AIDPF-2048`` / ``AIDPF-2049``) and deliberately did NOT parse into
    skill-actionable state. These failures are operator-actionable (re-auth,
    fix cluster config, retry) and not skill-recoverable; the skill refuses to
    draft when this list is non-empty, with reason
    ``cluster_dispatch_failure_not_skill_recoverable`` and a hand-off to the
    bootstrap diagnostic.

    Distinct from ``malformed_paths`` — these files are well-formed and
    semantically valid; they're just outside the skill's scope. Keeping
    the two lists separate means the skill can give the operator a
    targeted message instead of a vague "your diagnostics are broken"."""

    @property
    def has_identity_failure(self) -> bool:
        """``AIDPF-1020`` present → refuse to draft."""
        return self.identity_failure is not None

    @property
    def has_unknown_schema_version(self) -> bool:
        """Any artifact at an unsupported schemaVersion → refuse."""
        return bool(self.unknown_schema_paths)

    @property
    def has_malformed_artifacts(self) -> bool:
        return bool(self.malformed_paths)

    @property
    def has_cluster_dispatch_failures(self) -> bool:
        """``True`` iff the run dir carries any ``AIDPF-2048`` or
        ``AIDPF-2049`` artifact. Skill refuses to draft when this is set;
        cluster-dispatch failures are operator-actionable and outside the
        skill's scope."""
        return bool(self.cluster_dispatch_skipped_paths)

    @property
    def is_empty(self) -> bool:
        """No variation-point failures + no identity failure → nothing
        to draft (the operator may have pointed at the wrong run_id, or
        feature #2 exited cleanly on this run)."""
        return (
            not self.variation_failures
            and not self.source_column_failures
            and not self.type_mismatch_failures
            and self.identity_failure is None
            and not self.unknown_schema_paths
            and not self.malformed_paths
            and not self.cluster_dispatch_skipped_paths
        )

    @property
    def has_drift_only(self) -> bool:
        """Drift artifact present BUT no skill-actionable failures to act on.

        Drift artifacts are produced by ``run``-time preflight; the
        recovery is ``bootstrap --refresh``, not a skill-drafted
        overlay. A drift-only directory means the operator is in
        the wrong recovery flow — skill refuses with a clear
        hand-off message. A directory with a 4070 type-mismatch (or any
        skill-recoverable failure) is NOT drift-only.
        """
        return (
            self.schema_drift_failure is not None
            and not self.variation_failures
            and not self.source_column_failures
            and not self.type_mismatch_failures
        )

    def can_proceed(self) -> bool:
        """``True`` iff the skill should proceed to the propose phase.

        Drift-only directories refuse (operator should run
        ``bootstrap --refresh`` first). Drift + 2010/2011 in the same
        directory proceeds on the 2010/2011 work; the drift artifact is
        surfaced as context.
        """
        return (
            (
                bool(self.variation_failures)
                or bool(self.source_column_failures)
                or bool(self.type_mismatch_failures)
            )
            and not self.has_identity_failure
            and not self.has_unknown_schema_version
            and not self.has_malformed_artifacts
            and not self.has_cluster_dispatch_failures
        )


def read_run(
    diagnostics_root: Path,
    run_id: str | None = None,
) -> DiagnosticReadResult:
    """Scan a single bootstrap-run's diagnostics directory.

    Args:
        diagnostics_root: ``<workdir>/.aidp/diagnostics/``. The skill
            resolves ``workdir`` from the bundle path before calling.
        run_id: bootstrap-run id (matches the directory name). If
            ``None``, the reader auto-discovers the most-recent run
            (lexicographic max of subdirectory names — feature #2's
            run-ids are ISO-prefixed so this is chronological).

    Returns:
        :class:`DiagnosticReadResult`. Always returns — the gates
        surface as fields, not exceptions, so the skill can present a
        coherent refusal message to the operator instead of crashing.
    """
    if not diagnostics_root.is_dir():
        return DiagnosticReadResult(
            run_id=run_id or "",
            run_dir=diagnostics_root / (run_id or ""),
        )

    resolved_run_id, run_dir = _resolve_run_dir(diagnostics_root, run_id)
    if run_dir is None or not run_dir.is_dir():
        return DiagnosticReadResult(run_id=resolved_run_id, run_dir=diagnostics_root)

    variation_failures: list[VariationPointDiagnosticV1] = []
    source_column_failures: list[BronzeSourceColumnMissingV1] = []
    type_mismatch_failures: list[BronzeTypeMismatchV1] = []
    identity_failure: IdentityDiagnosticV1 | None = None
    schema_drift_failure: SchemaDriftDiagnosticV1 | None = None
    unknown_schema_paths: list[Path] = []
    malformed_paths: list[Path] = []
    cluster_dispatch_skipped_paths: list[Path] = []

    for artifact_path in sorted(run_dir.glob("*.json")):
        try:
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            malformed_paths.append(artifact_path)
            continue

        schema_version = payload.get("schemaVersion")
        if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
            unknown_schema_paths.append(artifact_path)
            continue

        error_code = payload.get("errorCode")
        # Cluster-dispatch artifacts get recognised but NOT parsed into skill
        # state. Operator-actionable, not skill-recoverable.
        if error_code in _CLUSTER_DISPATCH_INFRASTRUCTURE_CODES:
            cluster_dispatch_skipped_paths.append(artifact_path)
            continue

        try:
            if error_code == AIDPF_1020_OPERATOR_IDENTITY_UNRESOLVED:
                identity_failure = IdentityDiagnosticV1.model_validate(payload)
            elif error_code in (
                AIDPF_2010_COLUMN_ALIAS_UNRESOLVED,
                AIDPF_2011_SEMANTIC_VARIANT_UNRESOLVED,
            ):
                variation_failures.append(
                    VariationPointDiagnosticV1.model_validate(payload)
                )
            elif error_code == AIDPF_2012_SCHEMA_DRIFT_DETECTED:
                # Runtime drift artifact. Surfaced to the operator but doesn't
                # make the skill proceed: drift recovery is `bootstrap
                # --refresh`, not a skill draft.
                schema_drift_failure = SchemaDriftDiagnosticV1.model_validate(payload)
            elif error_code == AIDPF_4071_BRONZE_SOURCE_COLUMN_MISSING:
                # A bronze node wants a column the live PVO lacks — almost
                # always a rename. Skill-recoverable: draft a columnAlias
                # overlay mapping declared → real physical name (from
                # the diagnostic's pvoColumns).
                source_column_failures.append(
                    BronzeSourceColumnMissingV1.model_validate(payload)
                )
            elif error_code == AIDPF_4070_BRONZE_TYPE_MISMATCH:
                # A bronze node's declared type differs from the live PVO.
                # Skill-recoverable: draft a bronze type-overlay retyping each
                # mismatched column to the diagnostic's `materialised` type.
                type_mismatch_failures.append(
                    BronzeTypeMismatchV1.model_validate(payload)
                )
            else:
                malformed_paths.append(artifact_path)
        except Exception:  # noqa: BLE001 — Pydantic raises a variety of types
            malformed_paths.append(artifact_path)

    return DiagnosticReadResult(
        run_id=resolved_run_id,
        run_dir=run_dir,
        variation_failures=variation_failures,
        source_column_failures=source_column_failures,
        type_mismatch_failures=type_mismatch_failures,
        identity_failure=identity_failure,
        schema_drift_failure=schema_drift_failure,
        unknown_schema_paths=unknown_schema_paths,
        malformed_paths=malformed_paths,
        cluster_dispatch_skipped_paths=cluster_dispatch_skipped_paths,
    )


def _resolve_run_dir(
    diagnostics_root: Path,
    run_id: str | None,
) -> tuple[str, Path | None]:
    """Pick the target run directory.

    With an explicit ``run_id``, point at that directory (don't probe
    other runs — operator's intent is specific). With ``None``,
    auto-discover the latest run: feature #2's run-ids are formatted
    ``YYYYMMDDTHHMMSSZ-<uuid8>`` so lexicographic max == chronological
    latest.
    """
    if run_id is not None:
        return run_id, diagnostics_root / run_id

    candidates = [p for p in diagnostics_root.iterdir() if p.is_dir()]
    if not candidates:
        return "", None
    latest = max(candidates, key=lambda p: p.name)
    return latest.name, latest


__all__ = [
    "SUPPORTED_SCHEMA_VERSIONS",
    "DiagnosticReadResult",
    "read_run",
]
