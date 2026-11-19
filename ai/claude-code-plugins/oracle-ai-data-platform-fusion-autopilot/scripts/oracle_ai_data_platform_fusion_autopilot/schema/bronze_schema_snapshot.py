"""Pinned bronze-schema snapshot.

Bootstrap writes one file at
``<bundle.yaml.parent>/profiles/<profile>.schema-snapshot.yaml`` at the
same instant it computes ``bronzeSchemaFingerprint``. The
snapshot is the un-hashed input that drives the fingerprint —
serialising it lets the runtime drift gate populate
:attr:`SchemaDriftFailure.dataset_deltas` with column-level
add/remove/type-changed entries.

Layout / lifecycle:

* Co-located with the tenant profile (both are bootstrap-pinned
  artefacts; ``.aidp/`` is reserved for runtime-emitted artefacts).
* Single current snapshot per tenant — bootstrap overwrites atomically
  on each successful pin or ``--refresh``. History stays in the
  evidence trail under ``evidence/<tenant>/<ISO-ts>.yaml``.
* Snapshot carries its own ``bronzeSchemaFingerprint`` field for
  cross-check at read time — preflight recomputes the fingerprint
  over the snapshot's dataset/column lists and confirms it matches
  both the snapshot's own metadata AND the value pinned in
  ``profiles/<tenant>.yaml``. Mismatch → degrade to empty
  ``datasetDeltas`` + WARN, never block the drift signal.

Diff key canonicalisation MUST mirror
``schema.bronze_fingerprint.compute_bronze_fingerprint`` exactly:
``name.strip().lower()`` for column keys, ``type.strip().lower()`` for
type comparisons. Original casing is preserved on the surfaced delta
entries (operator-facing).

Forward-compat: ``schemaVersion`` is currently always ``1``. Future
``2+`` files are rejected by the loader with a typed error; preflight
treats them as a degraded-mode path (empty ``datasetDeltas`` + WARN)
the same way it treats a missing snapshot.
"""

from __future__ import annotations

import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .bronze_fingerprint import ColumnInfo


# ---------------------------------------------------------------------------
# Pydantic models — wire shape
# ---------------------------------------------------------------------------


class SnapshotColumn(BaseModel):
    """One bronze column captured at pin time."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    name: str
    type: str


class SnapshotDataset(BaseModel):
    """All columns of one bronze dataset captured at pin time."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    dataset_id: str = Field(alias="datasetId")
    columns: list[SnapshotColumn]


class BronzeSchemaSnapshotV1(BaseModel):
    """Top-level snapshot file.

    Carries enough metadata for preflight to validate the snapshot in
    isolation (without re-reading the profile):

    * ``schemaVersion`` — locked to ``1``. Future bumps are a
      separate feature.
    * ``tenant`` — paired with the profile; preflight cross-checks at
      read time but does not require equality (the profile's tenant
      is authoritative — they're written atomically by the same
      bootstrap commit).
    * ``pinnedAt`` — same moment as
      ``EvidenceSnapshotV1.generated_at``; recorded for forensic
      timeline reconstruction.
    * ``bronzeSchemaFingerprint`` — the hash computed over
      ``datasets`` at pin time. Preflight recomputes and confirms
      this matches the snapshot's own value (catches hand-edits) and
      matches the value pinned in ``profiles/<tenant>.yaml``
      (catches profile-snapshot desync from partial-failure
      histories).
    * ``datasets`` — the un-hashed input. Each entry is the same
      ``(name, type)`` projection
      :func:`schema.bronze_fingerprint.compute_bronze_fingerprint`
      hashes.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    schema_version: Literal[1] = Field(default=1, alias="schemaVersion")
    tenant: str
    pinned_at: datetime = Field(alias="pinnedAt")
    bronze_schema_fingerprint: str = Field(alias="bronzeSchemaFingerprint")
    datasets: list[SnapshotDataset]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class BronzeSchemaSnapshotSchemaError(ValueError):
    """Raised when a snapshot YAML file fails to load or validate.

    Carries the source path so preflight's WARN log can surface
    actionable context — operator's remediation is
    ``bootstrap --refresh`` to repin both the profile fingerprint and
    the snapshot atomically.
    """

    def __init__(self, *, path: Path | None, reason: str) -> None:
        self.path = path
        self.reason = reason
        suffix = f" ({path})" if path is not None else ""
        super().__init__(f"bronze schema snapshot invalid: {reason}{suffix}")


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------


def resolve_snapshot_path(bundle_path: Path, profile_name: str) -> Path:
    """Return the on-disk path for a tenant's pinned schema snapshot.

    Mirrors :func:`schema.tenant_profile.resolve_profile_path` —
    profiles and snapshots are co-located under
    ``<bundle.yaml.parent>/profiles/``.

    Args:
        bundle_path: path to the ``bundle.yaml`` file.
        profile_name: profile name without extension
            (``bundle.contentPack.profile``). Validated as a safe
            filesystem segment.

    Returns:
        ``<bundle.yaml.parent>/profiles/<profile_name>.schema-snapshot.yaml``
        (absolute after ``.resolve()``).

    Raises:
        UnsafePathSegmentError: ``profile_name`` is not a safe
            filesystem segment.
    """
    from .path_segment import assert_within_root, validate_path_segment

    validate_path_segment(profile_name, field="contentPack.profile")
    bundle_root = bundle_path.parent.resolve()
    target = (
        bundle_root / "profiles" / f"{profile_name}.schema-snapshot.yaml"
    ).resolve()
    assert_within_root(
        target, bundle_root / "profiles", field="contentPack.profile"
    )
    return target


# ---------------------------------------------------------------------------
# Build helper — used by bootstrap at pin time
# ---------------------------------------------------------------------------


def from_observed(
    *,
    tenant: str,
    pinned_at: datetime,
    fingerprint: str,
    observed: dict[str, list[ColumnInfo]],
) -> BronzeSchemaSnapshotV1:
    """Build a snapshot model from the same ``observed`` dict
    :func:`compute_bronze_fingerprint` consumed.

    Preserves dataset / column ordering from the probe so the
    cross-check at read time can recompute the same fingerprint.

    Args:
        tenant: tenant identifier (matches the profile's tenant).
        pinned_at: pin timestamp (matches the profile's ``pinnedAt``).
        fingerprint: the value
            :func:`compute_bronze_fingerprint` produced for
            ``observed``.
        observed: ``{dataset_id: [ColumnInfo, ...]}`` mapping from
            :func:`commands.bronze_probe.describe_bronze`.

    Returns:
        A validated :class:`BronzeSchemaSnapshotV1`.
    """
    # Strip bronze audit columns BEFORE building the snapshot — same
    # axis as `compute_bronze_fingerprint`. The snapshot represents the
    # BICC PVO contract (compared against live BICC at runtime by the
    # AIDPF-2072 drift gate); audit columns are appended post-extract by
    # the bronze adapter and are NOT part of that contract. Without
    # this strip, `DESCRIBE TABLE`-sourced `observed` dicts (the
    # bootstrap path) leak audit names into the snapshot YAML and
    # AIDPF-2072 fires false-positive on every subsequent run.
    from .bronze_fingerprint import strip_audit_columns
    stripped = strip_audit_columns(observed)
    datasets = [
        SnapshotDataset(
            datasetId=dataset_id,
            columns=[
                SnapshotColumn(name=col.name, type=col.type)
                for col in stripped[dataset_id]
            ],
        )
        for dataset_id in stripped
    ]
    return BronzeSchemaSnapshotV1(
        tenant=tenant,
        pinnedAt=pinned_at,
        bronzeSchemaFingerprint=fingerprint,
        datasets=datasets,
    )


def snapshot_to_observed(
    snapshot: BronzeSchemaSnapshotV1,
) -> dict[str, list[ColumnInfo]]:
    """Inverse of :func:`from_observed` — produce a
    ``{dataset_id: [ColumnInfo, ...]}`` dict from the snapshot.

    Used by preflight + bootstrap-refresh to recompute the fingerprint
    over the snapshot's contents and compare against the snapshot's
    own ``bronze_schema_fingerprint`` field (catches hand-edits where
    the dataset list was modified but the metadata fingerprint wasn't
    updated to match).

    Pure function — no Spark, no I/O.
    """
    return {
        dataset.dataset_id: [
            ColumnInfo(name=col.name, type=col.type) for col in dataset.columns
        ]
        for dataset in snapshot.datasets
    }


# ---------------------------------------------------------------------------
# Writers / loaders
# ---------------------------------------------------------------------------


def write_bronze_schema_snapshot(
    workdir: Path,
    profile_name: str,
    snapshot: BronzeSchemaSnapshotV1,
) -> Path:
    """Write a snapshot YAML file atomically.

    Overwrites an existing snapshot on success (mirrors how
    ``profiles/<tenant>.yaml`` is rewritten by
    ``bootstrap --refresh``). The temp file is sibling-co-located so
    ``os.replace`` is atomic across the same filesystem.

    Args:
        workdir: persistence-root anchor — pass
            ``bundle_path.resolve().parent``.
        profile_name: same value used to resolve
            ``profiles/<profile_name>.yaml``. Validated as a safe
            filesystem segment by :func:`resolve_snapshot_path`.
        snapshot: the snapshot payload.

    Returns:
        The absolute path the snapshot was written to.

    Raises:
        UnsafePathSegmentError: ``profile_name`` is not a safe
            filesystem segment, or the resolved target escapes
            ``<workdir>/profiles/``.
    """
    from .path_segment import assert_within_root, validate_path_segment

    validate_path_segment(profile_name, field="contentPack.profile")
    workdir_resolved = workdir.resolve()
    profiles_dir = workdir_resolved / "profiles"
    target = (profiles_dir / f"{profile_name}.schema-snapshot.yaml").resolve()
    assert_within_root(
        target, profiles_dir, field="contentPack.profile"
    )
    profiles_dir.mkdir(parents=True, exist_ok=True)

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


def load_bronze_schema_snapshot(path: Path) -> BronzeSchemaSnapshotV1:
    """Load a snapshot YAML file.

    Args:
        path: filesystem path to the snapshot file.

    Returns:
        Validated :class:`BronzeSchemaSnapshotV1`.

    Raises:
        BronzeSchemaSnapshotSchemaError: parse or validation failure
            (malformed YAML, unsupported ``schemaVersion``, Pydantic
            validation error, missing keys, etc.).
    """
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise BronzeSchemaSnapshotSchemaError(
            path=path, reason=f"read failed: {type(exc).__name__}: {exc}"
        ) from exc
    return load_bronze_schema_snapshot_from_string(text, source=path)


def load_bronze_schema_snapshot_from_string(
    text: str, *, source: Path | None = None
) -> BronzeSchemaSnapshotV1:
    """Load a snapshot from a YAML string.

    Used by the cluster-side notebook bootstrap which receives the
    snapshot YAML as an inlined base64-encoded payload. The cluster
    cannot ``read_text`` from the customer's filesystem, so this
    string-shaped variant is the cluster-safe entry point.

    Args:
        text: YAML contents.
        source: optional file path for error messages.

    Returns:
        Validated :class:`BronzeSchemaSnapshotV1`.

    Raises:
        BronzeSchemaSnapshotSchemaError: parse or validation failure.
    """
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise BronzeSchemaSnapshotSchemaError(
            path=source,
            reason=f"malformed YAML: {getattr(exc, 'problem', str(exc))}",
        ) from exc

    if not isinstance(raw, dict):
        raise BronzeSchemaSnapshotSchemaError(
            path=source,
            reason=f"top-level YAML must be a mapping, got {type(raw).__name__}",
        )

    declared_version = raw.get("schemaVersion")
    if declared_version is not None and declared_version != 1:
        raise BronzeSchemaSnapshotSchemaError(
            path=source,
            reason=(
                f"unsupported schemaVersion={declared_version!r}; "
                f"this plugin supports version 1 only"
            ),
        )

    try:
        return BronzeSchemaSnapshotV1.model_validate(raw)
    except ValidationError as exc:
        raise BronzeSchemaSnapshotSchemaError(
            path=source,
            reason=f"Pydantic validation failed: {exc}",
        ) from exc


__all__ = [
    "BronzeSchemaSnapshotSchemaError",
    "BronzeSchemaSnapshotV1",
    "SnapshotColumn",
    "SnapshotDataset",
    "from_observed",
    "load_bronze_schema_snapshot",
    "load_bronze_schema_snapshot_from_string",
    "resolve_snapshot_path",
    "snapshot_to_observed",
    "write_bronze_schema_snapshot",
]
