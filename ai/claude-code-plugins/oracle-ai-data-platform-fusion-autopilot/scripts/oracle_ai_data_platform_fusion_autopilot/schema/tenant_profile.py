"""Tenant profile schema + loader.

A *tenant profile* is the customer-specific values frozen at bootstrap
time for a given Fusion pod: calendar settings, COA semantic segment
map, resolved variation-point picks (which column-alias / semantic
candidate matches this tenant's PVO shapes), bronze schema fingerprint
for drift detection.

The profile YAML lives **beside** ``bundle.yaml`` at
``<bundle.yaml.parent>/profiles/<name>.yaml`` — NEVER inside the pack
directory. One file per tenant; the active one is named in
``bundle.contentPack.profile``.

Bootstrap-time profile *creation* performs interactive variation-point
resolution and evidence snapshotting. At runtime, the orchestrator
loads the file and threads the resolved values into the SQL renderer.

This layer supersedes runtime detection by pinning column-alias and
semantic-variant choices at bootstrap time.

Variation-point choices are resolved at bootstrap, stored beside the bundle,
and reused deterministically at runtime.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError


# ---------------------------------------------------------------------------
# AIDPF error codes for tenant profile loading.
# ---------------------------------------------------------------------------
# Note: AIDPF-1033 (profile file not found) is registered with the bundle-
# resolution codes in schema/bundle.py because it fires at the CLI/run
# boundary, not at the loader itself.

AIDPF_1050_TENANT_PROFILE_SCHEMA_MISMATCH = "AIDPF-1050"
"""Tenant profile YAML failed Pydantic validation (unknown keys, wrong types, missing fields)."""

AIDPF_1051_TENANT_PROFILE_UNSUPPORTED_VERSION = "AIDPF-1051"
"""Tenant profile declares a ``schemaVersion`` this plugin doesn't understand."""


class TenantProfileSchemaError(Exception):
    """Raised when a tenant profile YAML fails to load or validate."""


# Supported schema versions. Bump this list (not the literal) when a
# breaking change ships; new readers should still accept old files when
# possible — additive changes don't need a version bump.
SUPPORTED_PROFILE_SCHEMA_VERSIONS: tuple[int, ...] = (1,)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class ResolvedVariationPoints(BaseModel):
    """Tenant-specific picks for ``columnAliases`` and ``semanticVariants``.

    Populated at bootstrap time once the interactive resolver picks a
    candidate (or auto-resolves when exactly one matches). Each map is
    keyed by the variation-point ``name`` declared in ``pack.yaml``;
    the value is the chosen candidate's logical id.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    column: dict[str, str] = Field(default_factory=dict)
    """Variation-point ``columnAliases.<name>`` → candidate column name."""

    semantic: dict[str, str] = Field(default_factory=dict)
    """Variation-point ``semanticVariants.<name>`` → candidate id (e.g. ``cancelled_date``)."""


class TenantProfile(BaseModel):
    """Frozen tenant-specific values referenced by SQL templates.

    The renderer substitutes ``{{ profile.<key> }}`` references from
    this model's nested attributes (e.g. ``{{ profile.calendar.fiscalStartMonth }}``
    looks up ``profile.calendar.fiscal_start_month`` via Pydantic alias
    normalisation).

    ``profile_hash`` is included in the content-pack plan-hash; any change to
    this file's contents changes the hash and blocks unsafe resume.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    schema_version: int = Field(alias="schemaVersion")
    """Tenant profile YAML schema version. Currently always ``1``."""

    tenant: str
    """Tenant identifier — usually the Fusion pod short name (e.g. ``acme-prod``)."""

    pinned_at: datetime = Field(alias="pinnedAt")
    """When bootstrap pinned this profile (used in evidence snapshots)."""

    bronze_schema_fingerprint: str | None = Field(
        default=None, alias="bronzeSchemaFingerprint"
    )
    """Fingerprint of the bronze schema this profile was resolved against.

    Runtime drift detection compares this against the live bronze
    schema each run; mismatch blocks the run until a re-bootstrap.

    Optional with ``default=None`` so legacy profiles load without the
    field. ``check_bronze_fingerprint_drift`` treats ``None`` and the
    placeholder sentinel pattern (e.g. ``sha256:placeholder-...``) as
    "no real pin" and emits a single WARN log per run before
    proceeding without the drift gate. ``bootstrap --refresh`` pins a
    real value; from then on the gate fires.
    """

    resolved: ResolvedVariationPoints = Field(default_factory=ResolvedVariationPoints)
    """Variation-point picks. Empty for packs that declare no variation points."""

    profile: dict[str, Any] = Field(default_factory=dict)
    """Free-form ``{{ profile.<key> }}`` substitution values (calendar, COA, etc.).

    Renderer enforces per-value type allowlist (str/int/float/bool/date/datetime).
    Nested dicts are allowed; the renderer walks dotted lookups (max depth 4).
    """

    provenance: dict[str, Any] = Field(default_factory=dict)
    """Optional bootstrap-time metadata (run id, host, source files, etc.). Free-form."""


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


def resolve_profile_path(bundle_path: Path, profile_name: str) -> Path:
    """Return the expected on-disk path for a named tenant profile.

    Profiles live at ``<bundle.yaml.parent>/profiles/<profile_name>.yaml`` —
    never inside the pack directory. This helper does NOT check the file's
    existence; callers raise ``AIDPF-1033`` if the file is missing.

    Args:
        bundle_path: path to the ``bundle.yaml`` file (typically the
            CLI ``--bundle`` argument).
        profile_name: profile name without extension (``bundle.contentPack.profile``).

    Returns:
        ``<bundle.yaml.parent>/profiles/<profile_name>.yaml`` (absolute
        after ``.resolve()``).

    Raises:
        UnsafePathSegmentError: ``profile_name`` is not a safe filesystem
            segment (contains separators, ``..``, whitespace, etc.).
            Defence against malformed / untrusted bundle YAML — a
            profile name of ``../../outside`` would otherwise produce
            ``profiles/../../outside.yaml`` which collapses to a path
            outside the bundle's persistence root after ``.resolve()``.
    """
    # Import here to avoid a circular import at module load time.
    from .path_segment import assert_within_root, validate_path_segment

    validate_path_segment(profile_name, field="contentPack.profile")
    bundle_root = bundle_path.parent.resolve()
    target = (bundle_root / "profiles" / f"{profile_name}.yaml").resolve()
    # Defence-in-depth: even with the segment validator, assert the final
    # resolved path lives under the bundle's profiles/ directory.
    assert_within_root(target, bundle_root / "profiles", field="contentPack.profile")
    return target


def load_tenant_profile(path: Path) -> TenantProfile:
    """Load and validate a tenant profile YAML from disk.

    Args:
        path: filesystem path to the profile YAML. Caller is responsible
            for the file's existence (raise ``AIDPF-1033`` first if missing).

    Returns:
        Validated ``TenantProfile``.

    Raises:
        TenantProfileSchemaError: YAML parse failed, unsupported
            ``schemaVersion``, or Pydantic validation failed. The
            exception's message carries the appropriate AIDPF code.
    """
    with path.open("r", encoding="utf-8") as f:
        return load_tenant_profile_from_string(f.read(), source=str(path))


def load_tenant_profile_from_string(yaml_text: str, *, source: str = "<string>") -> TenantProfile:
    """Load a tenant profile from a YAML string.

    Used by the cluster-side notebook bootstrap, which receives the
    profile YAML as an inlined base64-encoded JSON token. The cluster cannot
    ``read_text`` from the
    customer's filesystem, so this string-shaped variant is the
    cluster-safe entry point.

    Args:
        yaml_text: profile YAML contents.
        source: identifier (filename or ``"<string>"``) used in error
            messages for traceability.

    Returns:
        Validated ``TenantProfile``.

    Raises:
        TenantProfileSchemaError: parse or validation failure; message
            includes the AIDPF code.
    """
    try:
        raw = yaml.safe_load(yaml_text)
    except yaml.YAMLError as e:
        mark = getattr(e, "problem_mark", None)
        loc = f" at line {mark.line + 1} col {mark.column + 1}" if mark else ""
        raise TenantProfileSchemaError(
            f"{AIDPF_1050_TENANT_PROFILE_SCHEMA_MISMATCH}: malformed YAML in "
            f"{source}{loc}: {getattr(e, 'problem', str(e))}"
        ) from e

    if not isinstance(raw, dict):
        raise TenantProfileSchemaError(
            f"{AIDPF_1050_TENANT_PROFILE_SCHEMA_MISMATCH}: profile {source} must "
            f"be a YAML mapping at the top level, got {type(raw).__name__}"
        )

    # Reject unsupported schema versions BEFORE Pydantic — gives a clean
    # diagnostic instead of an opaque "schemaVersion: 2 not in (1,)" error
    # from the Literal validator that Pydantic would otherwise raise.
    declared_version = raw.get("schemaVersion")
    if declared_version is not None and declared_version not in SUPPORTED_PROFILE_SCHEMA_VERSIONS:
        raise TenantProfileSchemaError(
            f"{AIDPF_1051_TENANT_PROFILE_UNSUPPORTED_VERSION}: profile {source} "
            f"declares schemaVersion={declared_version!r}; this plugin supports "
            f"versions {SUPPORTED_PROFILE_SCHEMA_VERSIONS}."
        )

    try:
        return TenantProfile.model_validate(raw)
    except ValidationError as e:
        details = "\n".join(
            f"  - {'.'.join(str(p) for p in err['loc'])}: {err['msg']}"
            for err in e.errors()
        )
        raise TenantProfileSchemaError(
            f"{AIDPF_1050_TENANT_PROFILE_SCHEMA_MISMATCH}: profile {source} failed "
            f"schema validation:\n{details}"
        ) from e


# ---------------------------------------------------------------------------
# Profile hashing — feeds into the content-pack plan-hash
# ---------------------------------------------------------------------------


def compute_profile_hash(profile: TenantProfile) -> str:
    """Deterministic sha256 over a tenant profile's canonical JSON form.

    Used by ``compute_content_pack_plan_hash`` so any change to the
    profile (a renamed column resolution, a new calendar start date, a
    swapped semantic variant) changes the plan-hash and blocks unsafe resume.

    Cosmetic changes to the YAML file (whitespace, key ordering, comments)
    do NOT change the hash because Pydantic round-trips the model and
    ``json.dumps(..., sort_keys=True)`` canonicalises the output. Semantic
    changes (any value-flip) DO.

    Args:
        profile: a validated ``TenantProfile`` instance.

    Returns:
        Hex sha256 string.
    """
    payload = profile.model_dump(mode="json", by_alias=True)
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()
