"""Pydantic models for content-pack YAML.

This module defines the typed representation of ``pack.yaml`` plus bronze,
silver, gold, and dashboard node YAML that ``aidp-fusion-autopilot content-pack
validate`` parses and the engine consumes at run time.

Error codes raised here are documented in ``docs/aidpf-error-codes.md``. Codes
used in this module's top-level models:

    * AIDPF-2002 -- pack version not SemVer-valid

Node-level validation rules and their codes are implemented in the
``NodeYaml`` model and content-pack validators.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal

from .incremental_impact import IncrementalImpact

if TYPE_CHECKING:  # pragma: no cover
    from .dashboard_pack import DashboardYaml

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    ValidationInfo,
    field_validator,
    model_validator,
)

# ---------------------------------------------------------------------------
# Error code constants
# ---------------------------------------------------------------------------
# Centralized here so module-level validators reference symbols rather than
# bare strings. The public registry lives in docs/aidpf-error-codes.md.

AIDPF_2002_INVALID_SEMVER = "AIDPF-2002"
AIDPF_2001_ORPHAN_OVERRIDE = "AIDPF-2001"  # used by overlay merger (Step 5)

# Node-level validation rule codes.
AIDPF_2020_MERGE_NO_NATURAL_KEY = "AIDPF-2020"      # R1
AIDPF_2030_OUTPUT_SCHEMA_NO_PII = "AIDPF-2030"      # R12
AIDPF_2050_MERGE_NO_WATERMARK = "AIDPF-2050"        # R2
AIDPF_2051_MERGE_ZERO_PRIMARY = "AIDPF-2051"        # R3
AIDPF_2052_MERGE_MULTI_PRIMARY = "AIDPF-2052"       # R4
AIDPF_2053_MERGE_MULTI_BRONZE_NO_ROLE = "AIDPF-2053"  # R5
AIDPF_2054_REPLACE_PARTITION_NO_COLUMNS = "AIDPF-2054"  # R6
AIDPF_2055_REPLACE_PARTITION_MULTI_PRIMARY = "AIDPF-2055"  # R7
AIDPF_2056_APPEND_UNIQUE_NO_KEY = "AIDPF-2056"      # R8
AIDPF_2057_AGGREGATE_MERGE_DEFERRED = "AIDPF-2057"  # R9
AIDPF_2058_SNAPSHOT_NO_UNIQUE_TEST = "AIDPF-2058"   # R10
AIDPF_2059_SCD2_NO_TRACKED_COLUMNS = "AIDPF-2059"   # R11
# AIDPF-2060 (python_legacy deprecated invariant) and AIDPF-2061
# (python_legacy callable spec) are retained as historical codes only; the
# python_legacy implementation type is no longer accepted.

# Bronze content-pack node type.
AIDPF_2080_BRONZE_EXTRACT_PVO_NOT_IN_CATALOG = "AIDPF-2080"
# WARN-only: pack YAML's implementation.pvo_id is not in the curated
# fusion_catalog.py. Pack loads cleanly; BICC drift gate (AIDPF-2072) catches
# typos at extract-preflight time. Customers can author overlay-pack YAMLs
# for new PVOs without a plugin release.

AIDPF_2081_BUNDLE_DATASET_NOT_IN_PACK = "AIDPF-2081"
# RAISE: bundle.yaml::datasets[].id does not resolve in any pack layer
# (bronze ∪ silver ∪ gold). Replaces the old fusion_catalog.CATALOG check.

AIDPF_2082_INVALID_SQL_IDENTIFIER = "AIDPF-2082"
# RAISE: a column name declared in naturalKey / partitionColumns /
# trackedColumns / watermark.column is not a safe unquoted SQL identifier.
# These names interpolate directly into MERGE ON / partition / watermark SQL,
# so they are validated at pack-load to reject both injection and cryptic
# Spark parse errors from typos.

AIDPF_2083_INVALID_CALENDAR_DATE = "AIDPF-2083"
# RAISE: a CalendarProfile startDate/endDate is not an ISO-8601 (YYYY-MM-DD)
# date. These values interpolate into the dim_calendar `sequence(DATE'...')`
# SQL, so they are validated at pack-load.


# ---------------------------------------------------------------------------
# SemVer validation
# ---------------------------------------------------------------------------
# Canonical SemVer 2.0.0 regex (https://semver.org/). Accepts:
#   1.0.0
#   0.1.0-alpha.1
#   1.2.3-rc.1+build.42
# Rejects bare numbers, leading zeros, trailing whitespace.

_SEMVER_RE = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
)


def _validate_semver(value: str) -> str:
    """Validate a SemVer string; raise ValueError with AIDPF-2002 on failure."""
    if not isinstance(value, str) or not _SEMVER_RE.match(value):
        raise ValueError(
            f"{AIDPF_2002_INVALID_SEMVER}: pack version not SemVer-valid: {value!r}. "
            f"Use a SemVer string in pack.yaml's `version:` field (e.g., `0.1.0`)."
        )
    return value


SemVerStr = Annotated[str, Field(description="SemVer 2.0.0 version string (e.g., 0.1.0)")]


# ---------------------------------------------------------------------------
# SQL-identifier and calendar-date validation
# ---------------------------------------------------------------------------
# Column names declared in naturalKey / partitionColumns / trackedColumns /
# watermark.column are interpolated unquoted into MERGE ON predicates,
# partition clauses, and watermark SQL. CalendarProfile dates are interpolated
# into ``sequence(DATE'...')``. Validating both at pack-load keeps untrusted /
# typo'd values from ever reaching the renderer. Same identifier rule as
# ``config.paths._SQL_IDENTIFIER_RE``.

_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _validate_sql_identifier_list(field_name: str, values: list[str]) -> list[str]:
    """Reject any column name that wouldn't safely interpolate into Spark SQL."""
    for value in values:
        if not isinstance(value, str) or not _SQL_IDENTIFIER_RE.match(value):
            raise ValueError(
                f"{AIDPF_2082_INVALID_SQL_IDENTIFIER}: {field_name} entry "
                f"{value!r} is not a valid unquoted SQL identifier — must match "
                r"^[A-Za-z_][A-Za-z0-9_]*$. These names interpolate directly "
                "into MERGE / partition / watermark SQL."
            )
    return values


def _validate_sql_identifier(field_name: str, value: str) -> str:
    """Reject a single column name that wouldn't safely interpolate into SQL."""
    if not isinstance(value, str) or not _SQL_IDENTIFIER_RE.match(value):
        raise ValueError(
            f"{AIDPF_2082_INVALID_SQL_IDENTIFIER}: {field_name}={value!r} is not "
            r"a valid unquoted SQL identifier — must match ^[A-Za-z_][A-Za-z0-9_]*$."
        )
    return value


def _validate_iso_date(field_name: str, value: str) -> str:
    """Validate an ISO-8601 (YYYY-MM-DD) date string; raise AIDPF-2083 on failure.

    Checks both the literal shape and that it is a real calendar date (so
    ``2020-13-40`` is rejected, not just non-numeric junk).
    """
    from datetime import date

    if not isinstance(value, str) or not _ISO_DATE_RE.match(value):
        raise ValueError(
            f"{AIDPF_2083_INVALID_CALENDAR_DATE}: {field_name}={value!r} is not an "
            "ISO-8601 date (YYYY-MM-DD)."
        )
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"{AIDPF_2083_INVALID_CALENDAR_DATE}: {field_name}={value!r} is not a "
            f"valid calendar date ({exc})."
        ) from exc
    return value


# ---------------------------------------------------------------------------
# Identity, compatibility, defaults
# ---------------------------------------------------------------------------


class PackCompatibilityAidp(BaseModel):
    """AIDP runtime capability requirements declared by the pack."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    requires_delta: bool = Field(default=True, alias="requiresDelta")
    """Whether the pack requires Delta Lake tables. v0.3 packs always do."""


class PackCompatibility(BaseModel):
    """Minimum environment requirements declared by the pack."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    plugin_min_version: SemVerStr = Field(alias="pluginMinVersion")
    """Minimum installed plugin version that satisfies this pack."""

    fusion_families: list[Literal["ERP", "HCM", "SCM"]] = Field(
        default_factory=list, alias="fusionFamilies"
    )
    """Fusion module families this pack targets (informational)."""

    aidp: PackCompatibilityAidp = Field(default_factory=PackCompatibilityAidp)

    @field_validator("plugin_min_version")
    @classmethod
    def _check_plugin_min_version(cls, v: str) -> str:
        return _validate_semver(v)


class RunIdColumnDefaults(BaseModel):
    """Audit column names per medallion layer."""

    model_config = ConfigDict(extra="forbid")

    bronze: str = "_run_id"
    silver: str = "silver_run_id"
    gold: str = "gold_run_id"


class PackDefaults(BaseModel):
    """Pack-wide defaults applied to every node unless overridden."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    sql_dialect: Literal["spark-sql"] = Field(default="spark-sql", alias="sqlDialect")
    """SQL flavour the templates target. v0.3 supports spark-sql only."""

    identifier_policy: Literal["validated-three-part"] = Field(
        default="validated-three-part", alias="identifierPolicy"
    )
    """Identifier validation policy for `{{ catalog }}.{{ schema }}.<table>` substitutions."""

    run_id_column: RunIdColumnDefaults = Field(
        default_factory=RunIdColumnDefaults, alias="runIdColumn"
    )


# ---------------------------------------------------------------------------
# Tenant profile defaults
# ---------------------------------------------------------------------------


class CalendarProfile(BaseModel):
    """Default calendar dimensions for the dim_calendar builtin (ADR-0011)."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    start_date: str = Field(alias="startDate")
    """ISO-8601 date (YYYY-MM-DD)."""

    end_date: str = Field(alias="endDate")

    fiscal_start_month: int = Field(default=1, alias="fiscalStartMonth", ge=1, le=12)

    @field_validator("start_date", "end_date")
    @classmethod
    def _check_iso_date(cls, v: str) -> str:
        return _validate_iso_date("calendar date", v)


class CoaRoleMapping(BaseModel):
    """One chart's COA role -> physical-column-name binding.

    Values are physical column names (e.g. ``CodeCombinationSegment4``), not
    integer positions -- so the binding survives non-conventional column
    naming and can be existence-validated against the bronze contract.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    balancing_segment: str = Field(alias="balancingSegment")
    cost_center_segment: str = Field(alias="costCenterSegment")
    natural_account_segment: str = Field(alias="naturalAccountSegment")

    def columns(self) -> dict[str, str]:
        """role-name -> physical column, keyed by the canonical role tokens."""
        return {
            "balancing": self.balancing_segment,
            "cost_center": self.cost_center_segment,
            "natural_account": self.natural_account_segment,
        }


class ChartOfAccountsProfile(BaseModel):
    """COA segment role->column mapping for a tenant.

    Two shapes are accepted:

    * **Flat / legacy** -- ``balancingSegment`` / ``costCenterSegment`` /
      ``naturalAccountSegment`` directly on this object (the pre-feature pack
      default shape, kept back-compat-parseable).
    * **Nested** -- ``default`` (a :class:`CoaRoleMapping`) plus optional
      per-chart ``byChart`` arms keyed by ``CodeCombinationChartOfAccountsId``
      (the multi-COA serving shape).

    The flat and nested ``default`` shapes are mutually exclusive (declaring
    both is an error). Use :meth:`resolved_default` to read the effective
    default regardless of which shape was authored.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    # Flat / legacy shape (optional now -- a profile may use the nested shape).
    balancing_segment: str | None = Field(default=None, alias="balancingSegment")
    cost_center_segment: str | None = Field(default=None, alias="costCenterSegment")
    natural_account_segment: str | None = Field(
        default=None, alias="naturalAccountSegment"
    )

    # Nested shape.
    default: CoaRoleMapping | None = None
    by_chart: dict[str, CoaRoleMapping] | None = Field(default=None, alias="byChart")

    # Operator opt-in: all active charts share the default layout (set via
    # `bootstrap --accept-singleton-coa`). Lets the multi-COA gate pass.
    #
    # StrictBool (not plain bool): a hand-edited `singletonAccepted: "false"`
    # STRING must NOT coerce to True and silently bypass AIDPF-2018. Native
    # true/false only; the structural COA gate relies on this to reject a
    # non-bool value pre-extraction.
    singleton_accepted: StrictBool = Field(default=False, alias="singletonAccepted")

    @model_validator(mode="after")
    def _check_shape(self) -> "ChartOfAccountsProfile":
        flat_present = any(
            v is not None
            for v in (
                self.balancing_segment,
                self.cost_center_segment,
                self.natural_account_segment,
            )
        )
        if flat_present and self.default is not None:
            raise ValueError(
                "chartOfAccounts: declare EITHER the flat "
                "balancing/costCenter/naturalAccount fields OR a nested "
                "`default` mapping, not both."
            )
        if flat_present:
            missing = [
                name
                for name, v in (
                    ("balancingSegment", self.balancing_segment),
                    ("costCenterSegment", self.cost_center_segment),
                    ("naturalAccountSegment", self.natural_account_segment),
                )
                if v is None
            ]
            if missing:
                raise ValueError(
                    f"chartOfAccounts flat shape is missing required fields: {missing}"
                )
        if self.by_chart is not None and not flat_present and self.default is None:
            raise ValueError(
                "chartOfAccounts: `byChart` requires a `default` mapping (or flat "
                "fields) for charts not covered by an explicit arm."
            )
        return self

    def resolved_default(self) -> CoaRoleMapping | None:
        """The effective default mapping, from either the nested or flat shape."""
        if self.default is not None:
            return self.default
        if self.balancing_segment is not None:
            return CoaRoleMapping(
                balancingSegment=self.balancing_segment,
                costCenterSegment=self.cost_center_segment,  # type: ignore[arg-type]
                naturalAccountSegment=self.natural_account_segment,  # type: ignore[arg-type]
            )
        return None

    def arms(self) -> dict[str, CoaRoleMapping]:
        """All effective mappings keyed by arm id (``default`` + each chart id)."""
        out: dict[str, CoaRoleMapping] = {}
        default = self.resolved_default()
        if default is not None:
            out["default"] = default
        for chart_id, mapping in (self.by_chart or {}).items():
            out[chart_id] = mapping
        return out

    def referenced_columns(self) -> set[str]:
        """Union of every physical column referenced by any arm (existence scope)."""
        cols: set[str] = set()
        for mapping in self.arms().values():
            cols.update(mapping.columns().values())
        return cols


class PackProfileDefaults(BaseModel):
    """One named profile's defaults declared by the pack.

    The pack ships sensible defaults; tenant profiles (``profiles/<tenant>.yaml``)
    override per-key during bootstrap. Schema is intentionally extensible --
    arbitrary typed knobs can be added under nested dicts.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    calendar: CalendarProfile | None = None
    chart_of_accounts: ChartOfAccountsProfile | None = Field(
        default=None, alias="chartOfAccounts"
    )


# ---------------------------------------------------------------------------
# Variation points
# ---------------------------------------------------------------------------


class ColumnAlias(BaseModel):
    """Same logical column, different physical names across tenants.

    Bootstrap walks ``candidates`` in priority order and pins the first that
    exists on the tenant.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    appliesTo: str
    """Fully-qualified bronze table this variation point applies to (e.g. `bronze.ap_invoices`)."""

    required: bool = True
    """If true and zero candidates match, bootstrap fails with AIDPF-2010."""

    candidates: list[str] = Field(min_length=1)
    """Priority-ordered list of physical column names. May include the literal `inherit`
    in overlay packs to extend the base pack's candidates (resolved at overlay merge).

    For ``resolution: semanticRole`` entries this is the *allowed domain* (the set of
    physical columns a role may bind), not a priority walk."""

    resolution: Literal["columnExistence", "semanticRole"] = "columnExistence"
    """Resolution strategy. ``columnExistence`` (default) keeps the existing
    physical-alias auto-match. ``semanticRole`` marks a tenant-specific business role
    (e.g. COA balancing segment) resolved from explicit ``profile.chartOfAccounts``
    config, not column existence -- existence cannot prove *meaning*."""

    role: str | None = None
    """For ``semanticRole`` entries: the semantic role this alias binds, e.g.
    ``coa.balancing``. Maps the alias to its ``profile.chartOfAccounts`` mapping."""

    @model_validator(mode="after")
    def _check_resolution(self) -> "ColumnAlias":
        if self.resolution == "semanticRole" and not self.role:
            raise ValueError(
                f"columnAlias on {self.appliesTo!r}: `resolution: semanticRole` "
                "requires a `role:` binding (e.g. `role: coa.balancing`)."
            )
        if self.resolution == "columnExistence" and self.role is not None:
            raise ValueError(
                f"columnAlias on {self.appliesTo!r}: `role:` is only valid with "
                "`resolution: semanticRole`."
            )
        return self


class SemanticVariantDetect(BaseModel):
    """How to recognise that this candidate applies to a tenant."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    column_exists: str = Field(alias="columnExists")
    """Column that must exist on the tenant for this candidate to match."""


class SemanticVariantCandidate(BaseModel):
    """One semantic-shape candidate."""

    model_config = ConfigDict(extra="forbid")

    id: str
    """Stable identifier for this candidate (e.g., `cancelled_date`, `cancelled_flag`)."""

    detect: SemanticVariantDetect

    fragment: str
    """SQL boolean fragment substituted into `{{ semantic.<name> }}`.
    Must conform to the semantic-fragment grammar."""


class SemanticVariant(BaseModel):
    """Same logical concept, different SQL shape across tenants."""

    model_config = ConfigDict(extra="forbid")

    appliesTo: str
    required: bool = True
    candidates: list[SemanticVariantCandidate] = Field(min_length=1)


# ---------------------------------------------------------------------------
# Overlay reference + override entries
# ---------------------------------------------------------------------------


_OVERLAY_REF_RE = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*?)@([^\s]+)$")


class PackOverlayRef(BaseModel):
    """Parsed ``extends: <pack-id>@<semver>`` reference.

    Constructed from a plain string by ``PackYaml.extends``; serialises back
    to the same string form. Provides typed access to ``name`` and ``version``
    for the overlay merger.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    version: SemVerStr

    @classmethod
    def parse(cls, raw: str) -> "PackOverlayRef":
        m = _OVERLAY_REF_RE.match(raw or "")
        if not m:
            raise ValueError(
                f"{AIDPF_2002_INVALID_SEMVER}: `extends:` must be in the form "
                f"`<pack-id>@<semver>`; got {raw!r}."
            )
        name, version = m.group(1), m.group(2)
        _validate_semver(version)  # may raise AIDPF-2002
        return cls(name=name, version=version)

    def to_string(self) -> str:
        return f"{self.name}@{self.version}"


class RelaxRequiredColumn(BaseModel):
    """One acknowledged removal of a bronze node's ``requiredColumns`` entry.

    Removing a required column *weakens* a live safety gate (the per-node
    preflight assertion + PVO-drift watch stop covering it), so it is allowed
    only behind an explicit, audited acknowledgement: the ``reason`` is
    mandatory and must be non-blank — it is the *only* control on the
    gate-weakening op. A missing key, ``""``, or whitespace-only string all fail
    closed (a bare ``min_length`` would accept ``"   "``).
    """

    model_config = ConfigDict(extra="forbid")

    column: str = Field(min_length=1)
    """The base ``requiredColumns`` entry to remove (literal or ``$column.*`` /
    ``$coa.*`` reference, matched verbatim)."""

    reason: str = Field(min_length=1)
    """Operator-facing justification for relaxing the assertion. Mandatory,
    non-blank — the acknowledgement of a deliberate gate relaxation."""

    @field_validator("column", "reason")
    @classmethod
    def _reject_blank(cls, v: str, info) -> str:
        if not v.strip():
            raise ValueError(
                f"{AIDPF_2001_ORPHAN_OVERRIDE}: relaxRequiredColumns `{info.field_name}` "
                f"must be a non-blank string; whitespace-only is not a valid "
                f"acknowledgement."
            )
        return v


class ForkedFrom(BaseModel):
    """Provenance fingerprints a ``replaceNode`` overlay was forked from.

    A same-id silver/gold full replacement *shadows* the base ``<id>.sql`` and
    the base node YAML contract; silver/gold have no drift gate against the base,
    so a later starter-pack fix to the base mart would be silently missed. These
    fingerprints, recomputed at ``content-pack validate``, convert that *silent*
    staleness into a *loud* one (``AIDPF-2064``):

    * ``sql_sha256`` — the base SQL template text + the referenced
      ``{{ semantic.* }}`` candidate lists (pack-owned render inputs).
    * ``contract_sha256`` — the base node YAML contract (``outputSchema`` incl.
      ``pii``, ``requiredColumns``, ``quality.tests``). A base change that touches
      only the YAML (e.g. a PII reclassification) trips ``AIDPF-2064`` too.
    * ``pack_version`` — the base pack version at fork time (recorded provenance).

    Both fingerprints are profile-independent and validate-time computable. Keys
    are camelCase in YAML (``sqlSha256`` / ``contractSha256`` / ``packVersion``),
    matching ``outputSchema`` / ``requiredColumns``.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    sql_sha256: str = Field(min_length=1, alias="sqlSha256")
    contract_sha256: str = Field(min_length=1, alias="contractSha256")
    pack_version: str = Field(min_length=1, alias="packVersion")

    @field_validator("sql_sha256", "contract_sha256", "pack_version")
    @classmethod
    def _reject_blank(cls, v: str, info) -> str:
        if not v.strip():
            raise ValueError(
                f"{AIDPF_2001_ORPHAN_OVERRIDE}: forkedFrom `{info.field_name}` "
                f"must be a non-blank string; whitespace-only is not a valid "
                f"fingerprint."
            )
        return v


class ReplaceNode(BaseModel):
    """Acknowledged same-id silver/gold full replacement (`.yaml` + `.sql`).

    Same-id silver/gold replacement is rejected by default (``AIDPF-2001``); a
    ``replaceNode`` block makes it legal — opt-in and audited, mirroring the
    ``relaxRequiredColumns`` precedent. The overlay ships a complete new
    ``<layer>/<id>.yaml`` + ``<id>.sql`` for a shipped SQL mart, keeping its id so
    downstream ``dependsOn`` consumers are not re-pointed. Add, remove, and
    rewrite of columns/logic all go through this one shape; only identity changes
    (``layer`` / ``target`` / ``dependsOn`` edges / ``refresh``) remain new-mart-id.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    reason: str = Field(min_length=1)
    """Mandatory, non-blank operator justification — the acknowledgement of a
    deliberate, staleness-incurring fork."""

    forked_from: ForkedFrom = Field(alias="forkedFrom")

    @field_validator("reason")
    @classmethod
    def _reject_blank_reason(cls, v: str) -> str:
        if not v.strip():
            raise ValueError(
                f"{AIDPF_2001_ORPHAN_OVERRIDE}: replaceNode `reason` must be a "
                f"non-blank string; whitespace-only is not a valid acknowledgement."
            )
        return v


class OverrideEntry(BaseModel):
    """Per-node override declared by an overlay pack.

    Merge rules:

    * ``profile:`` -- scalar replace.
    * ``sql:`` -- full-file replace; the named SQL path lives in the overlay.
    * ``quality:`` -- nested ``tests:`` list extends base.
    * ``outputSchema:`` -- name-keyed partial merge of a bronze node's
      output columns (retype matched columns; append with ``extendColumns``).
      Bronze targets only; enforced at merge.
    * ``requiredColumns:`` -- additive per-source union into a bronze node's
      required columns (adds only; never removes). Bronze targets only.
    * ``relaxRequiredColumns:`` -- acknowledged per-source REMOVAL of a bronze
      node's required columns (each entry carries a mandatory ``reason``).
      Bronze targets only; this is the *only* sanctioned removal path.
    * ``replaceNode:`` -- acknowledged same-id silver/gold full replacement
      (`.yaml` + `.sql`). Silver/gold targets only; mutually exclusive with every
      other key (the replacement files carry the new schema/SQL). See
      :class:`ReplaceNode`.

    Unknown keys fail closed (``extra="forbid"``) with an actionable
    AIDPF-2001 message.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    profile: str | None = None
    sql: str | None = None
    quality: dict[str, Any] | None = None
    output_schema: "OutputSchemaOverride | None" = Field(
        default=None, alias="outputSchema"
    )
    required_columns: dict[str, list[str]] | None = Field(
        default=None, alias="requiredColumns"
    )
    """Additive: per-source-id lists unioned into the base node's
    ``requiredColumns``. Cannot express a removal (use ``relaxRequiredColumns``)."""
    relax_required_columns: dict[str, list[RelaxRequiredColumn]] | None = Field(
        default=None, alias="relaxRequiredColumns"
    )
    """Acknowledged removal: per-source-id lists of columns to drop from the base
    node's ``requiredColumns``, each with a mandatory ``reason``."""
    replace_node: "ReplaceNode | None" = Field(default=None, alias="replaceNode")
    """Acknowledged same-id silver/gold full replacement. Silver/gold only;
    mutually exclusive with all other override keys."""

    @model_validator(mode="before")
    @classmethod
    def _reject_unknown_keys_actionably(cls, data: Any) -> Any:
        """Raise an AIDPF-2001-family, actionable error for unknown override
        keys *before* the generic ``extra="forbid"`` Pydantic error fires."""
        if not isinstance(data, dict):
            return data
        known = {
            "profile", "sql", "quality", "output_schema", "outputSchema",
            "required_columns", "requiredColumns",
            "relax_required_columns", "relaxRequiredColumns",
            "replace_node", "replaceNode",
        }
        unknown = [k for k in data if k not in known]
        if unknown:
            raise ValueError(
                f"{AIDPF_2001_ORPHAN_OVERRIDE}: unsupported override key(s) "
                f"{sorted(unknown)!r}. Allowed: profile, sql, quality, "
                f"outputSchema, requiredColumns, relaxRequiredColumns, replaceNode."
            )
        return data

    @model_validator(mode="after")
    def _replace_node_is_exclusive(self) -> "OverrideEntry":
        """``replaceNode`` carries its delta in the replacement files, so it must
        be the ONLY key on the entry — it cannot be combined with
        ``sql``/``outputSchema``/``requiredColumns``/``relaxRequiredColumns``/
        ``quality``/``profile``."""
        if self.replace_node is None:
            return self
        siblings = {
            "profile": self.profile,
            "sql": self.sql,
            "quality": self.quality,
            "outputSchema": self.output_schema,
            "requiredColumns": self.required_columns,
            "relaxRequiredColumns": self.relax_required_columns,
        }
        present = [k for k, v in siblings.items() if v is not None]
        if present:
            raise ValueError(
                f"{AIDPF_2001_ORPHAN_OVERRIDE}: `replaceNode` is mutually "
                f"exclusive with other override keys, but found {sorted(present)!r} "
                f"on the same entry. A full replacement carries its new schema/SQL "
                f"in the replacement `<layer>/<id>.yaml` + `<id>.sql` files; declare "
                f"only `replaceNode`."
            )
        return self


# ---------------------------------------------------------------------------
# Provenance (ADR-0019)
# ---------------------------------------------------------------------------


class SkillProposalRecord(BaseModel):
    """One per-VP proposal the medallion-author skill captured at
    overlay-draft time.

    Bootstrap reads ``candidate_added`` to detect AutoResolved-on-skill-
    proposed-candidate at commit time. The initial-onboarding flow must
    record ``mechanism: skill_proposed`` when the walker AutoResolves on a
    candidate the skill added.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    candidate_added: str = Field(alias="candidateAdded")
    """The candidate value the skill appended to the VP's list (e.g.
    ``ApInvoicesXCurrCode``)."""

    confidence: str | None = None
    """``high`` | ``medium`` | ``low`` — LLM's confidence in the
    proposal. Optional for audit only."""

    reasoning: str | None = None
    """Operator-facing rationale paragraph from the propose phase."""


class PackProvenance(BaseModel):
    """Optional provenance block stamped by the medallion-author skill (ADR-0019).

    Skill-authored packs (overlays, in particular) record the skill version,
    model identity, generation timestamp, and reason. Hand-authored packs may
    omit this block entirely.

    Skill-specific fields use explicit camelCase ``Field(alias=...)`` values
    so overlay YAML keys parse consistently and bootstrap can detect
    skill-authored proposals.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    generated_by: str | None = Field(default=None, alias="generatedBy")
    skill_version: str | None = Field(default=None, alias="skillVersion")
    model_id: str | None = Field(default=None, alias="modelId")
    generated_at: str | None = Field(default=None, alias="generatedAt")
    reason: str | None = None
    evidence: dict[str, Any] | None = None

    # Skill-authored overlay metadata.

    skill_id: str | None = Field(default=None, alias="skillId")
    """Stable identifier of the skill that drafted the overlay. The
    medallion-author skill stamps ``aidp-fusion-medallion-author``;
    bootstrap detects skill-authored overlays via this field and
    records ``mechanism: skill_proposed`` on the resolutions they
    drive."""

    diagnostic_run_id: str | None = Field(default=None, alias="diagnosticRunId")
    """The bootstrap run_id whose diagnostic artifacts triggered the
    skill invocation. Threads the audit trail from failure → draft →
    commit. Mutually exclusive with ``operator_input_id`` — exactly one
    identifies how the overlay was triggered."""

    operator_input_id: str | None = Field(default=None, alias="operatorInputId")
    """Set for overlays drafted from explicit operator input (no runtime
    diagnostic) — e.g. the COA-depth mode triggered by an ``AIDPF-2015``
    content-pack-validate failure, which writes no diagnostic artifact. A
    path-safe synthetic id like ``operator-input-<id>`` (never a fake bootstrap
    run id). Mutually exclusive with ``diagnostic_run_id``."""

    trigger: str | None = None
    """How the overlay was triggered: ``"diagnostic"`` (a runtime
    ``.aidp/diagnostics`` artifact) or ``"operator_input"`` (explicit operator
    command, no diagnostic)."""

    proposals: dict[str, SkillProposalRecord] | None = Field(
        default=None, alias="proposals"
    )
    """Per-VP candidate proposals the skill captured at draft time.
    Keyed by VP name (e.g. ``invoice_currency_code``). Bootstrap
    reads this to detect AutoResolved-on-skill-added-candidate at
    commit time."""

    incremental_impact: dict[str, IncrementalImpact] | None = Field(
        default=None, alias="incrementalImpact"
    )
    """Per-VP impact analysis (change kind, risk label, affected
    nodes, remediation choice). Bootstrap mirrors this into the
    per-resolution evidence snapshot on commit. See
    :class:`schema.incremental_impact.IncrementalImpact`."""


# ---------------------------------------------------------------------------
# Top-level pack.yaml
# ---------------------------------------------------------------------------


class PackYaml(BaseModel):
    """Top-level schema for ``pack.yaml``.

    Node definitions live in separate per-node YAML files under
    ``silver/`` and ``gold/`` and are validated by :class:`NodeYaml`
    when the content pack is loaded.
    ``PackYaml`` covers pack identity, compatibility, defaults, profile
    defaults, variation-point declarations, overlay coordination, and
    provenance.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    # Identity
    id: str
    """Stable pack identifier (e.g., ``fusion-finance-starter``).
    Dashes are allowed and conventional; the Python package data directory
    uses an underscore equivalent (``content_packs/<id-with-underscores>/``)."""

    version: SemVerStr
    description: str | None = None

    # Compatibility constraints
    compatibility: PackCompatibility

    # Pack-wide defaults
    defaults: PackDefaults = Field(default_factory=PackDefaults)

    # Tenant-customisation knobs
    profiles: dict[str, PackProfileDefaults] = Field(default_factory=dict)

    # Variation points
    column_aliases: dict[str, ColumnAlias] = Field(
        default_factory=dict, alias="columnAliases"
    )
    semantic_variants: dict[str, SemanticVariant] = Field(
        default_factory=dict, alias="semanticVariants"
    )

    # Overlay coordination
    extends: str | None = None
    """For overlay packs, the base pack reference in the form ``<id>@<version>``.
    ``None`` for base packs themselves."""

    overrides: dict[str, OverrideEntry] = Field(default_factory=dict)
    """For overlay packs: per-node-id override entries. Empty for base packs."""

    # Provenance (skill-authored packs)
    provenance: PackProvenance | None = None

    @field_validator("version")
    @classmethod
    def _check_version(cls, v: str) -> str:
        return _validate_semver(v)

    @field_validator("extends")
    @classmethod
    def _check_extends_syntax(cls, v: str | None) -> str | None:
        if v is None:
            return None
        # Parse and validate; discard the result (we store the string form).
        PackOverlayRef.parse(v)
        return v

    @model_validator(mode="after")
    def _base_packs_have_no_overrides(self) -> "PackYaml":
        """A base pack (``extends: null``) must not have ``overrides:`` content.

        Overrides are an overlay-only concept; populating them on a base pack
        is a logic error in pack authoring.
        """
        if self.extends is None and self.overrides:
            raise ValueError(
                f"{AIDPF_2001_ORPHAN_OVERRIDE}: base packs (with no `extends:`) "
                f"must not declare `overrides:`. Found overrides for: "
                f"{sorted(self.overrides.keys())!r}."
            )
        return self

    def parsed_extends(self) -> PackOverlayRef | None:
        """Return the parsed ``extends:`` reference, or ``None`` for base packs."""
        return PackOverlayRef.parse(self.extends) if self.extends else None


# ---------------------------------------------------------------------------
# Node-level models (silver / gold YAML per-node files)
# ---------------------------------------------------------------------------


# Dependency source reference (one entry in dependsOn.bronze or dependsOn.silver).

PiiLevel = Literal["high", "medium", "low", "none"]
Role = Literal["primary", "lookup"]
SeedStrategy = Literal["replace", "merge", "append", "replace_partition", "custom"]
IncrementalStrategy = Literal[
    "replace",
    "merge",
    "append",
    "replace_partition",
    "custom",
    # Deferred strategies; accepted by the type but rejected by validation.
    "aggregate_merge",
    "snapshot",
    "scd2",
]
NodeLayer = Literal["bronze", "silver", "gold"]
NodeImplType = Literal["sql", "builtin", "bronze_extract"]


class WatermarkSpec(BaseModel):
    """Per-source watermark column for incremental filtering."""

    model_config = ConfigDict(extra="forbid")

    column: str
    """Bronze/silver column whose monotonic values drive the watermark predicate."""

    @field_validator("column")
    @classmethod
    def _check_column(cls, v: str) -> str:
        return _validate_sql_identifier("watermark.column", v)


class SourceRef(BaseModel):
    """One entry in ``dependsOn.bronze[]`` or ``dependsOn.silver[]``.

    ``role`` declares whether the dependency is the primary watermark source
    or a lookup source.
    """

    model_config = ConfigDict(extra="forbid")

    id: str
    """Dataset/node id; must resolve in bronze.yaml or the pack's silver node set."""

    role: Role | None = None
    """Optional in YAML for single-source nodes (default applies); required for multi-bronze."""

    watermark: WatermarkSpec | None = None


class DependsOn(BaseModel):
    """Per-node upstream dependencies."""

    model_config = ConfigDict(extra="forbid")

    bronze: list[SourceRef] = Field(default_factory=list)
    silver: list[SourceRef] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Refresh strategy specs
# ---------------------------------------------------------------------------


class RefreshSeed(BaseModel):
    """Seed-mode refresh strategy. Typically `replace` for all v0.3 nodes."""

    model_config = ConfigDict(extra="forbid")

    strategy: SeedStrategy


class IncrementalWatermark(BaseModel):
    """Watermark configuration for incremental-mode merge / replace_partition."""

    model_config = ConfigDict(extra="forbid")

    source: str
    """ID of the primary source whose watermark drives this node."""

    column: str

    @field_validator("column")
    @classmethod
    def _check_column(cls, v: str) -> str:
        return _validate_sql_identifier("incremental.watermark.column", v)


class AffectedPartitionsFrom(BaseModel):
    """Maps source delta to target partitions (replace_partition strategy)."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    source: str
    source_columns: list[str] = Field(alias="sourceColumns", min_length=1)


class RefreshIncremental(BaseModel):
    """Incremental-mode refresh strategy."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    strategy: IncrementalStrategy
    watermark: IncrementalWatermark | None = None
    natural_key: list[str] = Field(default_factory=list, alias="naturalKey")
    partition_columns: list[str] = Field(default_factory=list, alias="partitionColumns")
    affected_partitions_from: AffectedPartitionsFrom | None = Field(
        default=None, alias="affectedPartitionsFrom"
    )
    tracked_columns: list[str] = Field(default_factory=list, alias="trackedColumns")
    """For scd2: columns whose changes close a record (deferred — schema only)."""
    reason: str | None = None

    @field_validator("natural_key", "partition_columns", "tracked_columns")
    @classmethod
    def _check_identifier_lists(cls, v: list[str], info: "ValidationInfo") -> list[str]:
        return _validate_sql_identifier_list(f"incremental.{info.field_name}", v)


class RefreshSpec(BaseModel):
    """Both seed and incremental refresh strategies."""

    model_config = ConfigDict(extra="forbid")

    seed: RefreshSeed
    incremental: RefreshIncremental | None = None


# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------


class OutputSchemaColumn(BaseModel):
    """One column in a node's declared output schema."""

    model_config = ConfigDict(extra="forbid")

    name: str
    type: str
    """Spark SQL type string (e.g., `bigint`, `string`, `decimal(28,8)`)."""

    nullable: bool = True
    pii: PiiLevel
    """Required PII classification. Missing values raise AIDPF-2030."""


class OutputSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    columns: list[OutputSchemaColumn] = Field(min_length=1)

    @model_validator(mode="after")
    def _no_duplicate_column_names(self) -> "OutputSchema":
        """Reject case-insensitive duplicate column names.

        Spark/Delta resolve column names case-insensitively, so ``VENDORID``
        and ``vendorid`` in one schema are ambiguous (and make the overlay
        name-keyed merge non-deterministic). Fail closed.
        """
        seen: dict[str, str] = {}
        for col in self.columns:
            key = col.name.lower()
            if key in seen:
                raise ValueError(
                    f"{AIDPF_2001_ORPHAN_OVERRIDE}: duplicate column name "
                    f"(case-insensitive): {col.name!r} collides with "
                    f"{seen[key]!r}. Column names must be unique."
                )
            seen[key] = col.name
        return self


class OutputSchemaColumnOverride(BaseModel):
    """One column entry in an overlay's ``outputSchema`` override.

    Distinct from :class:`OutputSchemaColumn`: ``name`` is the merge key and is
    required, while ``type``/``nullable``/``pii`` are **optional** so a *matched*
    (retype) column may override only the fields it changes and inherit the rest
    from the base column. A *matched* column must still set at least one mutable
    field (a name-only entry is a no-op and is rejected). Appended columns (a name
    absent from the base) must carry full ``type`` + ``pii`` — enforced in the
    merge step, where base-vs-overlay membership is known.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    type: str | None = None
    nullable: bool | None = None
    pii: PiiLevel | None = None

    @model_validator(mode="after")
    def _require_at_least_one_mutable_field(self) -> "OutputSchemaColumnOverride":
        if self.type is None and self.nullable is None and self.pii is None:
            raise ValueError(
                f"{AIDPF_2001_ORPHAN_OVERRIDE}: override column {self.name!r} "
                f"provides no mutable field — a name-only override is a no-op. "
                f"Set at least one of `type`, `nullable`, or `pii`."
            )
        return self


class OutputSchemaOverride(BaseModel):
    """An overlay's ``outputSchema`` override: a name-keyed, partial merge into
    the base node's columns.

    Declare only the columns you change. Matched columns are retyped; with
    ``extendColumns: true`` a new column may be appended (full ``type`` + ``pii``
    required, enforced at merge). Base columns not listed are preserved.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    columns: list[OutputSchemaColumnOverride] = Field(min_length=1)
    extend_columns: bool = Field(default=False, alias="extendColumns")

    @model_validator(mode="after")
    def _no_duplicate_override_names(self) -> "OutputSchemaOverride":
        seen: dict[str, str] = {}
        for col in self.columns:
            key = col.name.lower()
            if key in seen:
                raise ValueError(
                    f"{AIDPF_2001_ORPHAN_OVERRIDE}: duplicate override column "
                    f"name (case-insensitive): {col.name!r} collides with "
                    f"{seen[key]!r}."
                )
            seen[key] = col.name
        return self


# ---------------------------------------------------------------------------
# Quality tests (discriminated union over `type`)
# ---------------------------------------------------------------------------


class _QualityTestBase(BaseModel):
    """Common base for quality test entries. Field shapes vary by type."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class QualityTestNotNull(_QualityTestBase):
    type: Literal["not_null"] = "not_null"
    columns: list[str] = Field(min_length=1)


class QualityTestUnique(_QualityTestBase):
    type: Literal["unique"] = "unique"
    columns: list[str] = Field(min_length=1)


class QualityTestAcceptedValues(_QualityTestBase):
    type: Literal["accepted_values"] = "accepted_values"
    column: str
    values: list[Any] = Field(min_length=1)


class QualityTestRowCountMin(_QualityTestBase):
    type: Literal["row_count_min"] = "row_count_min"
    min: int = Field(ge=0)
    when_source_non_empty: str | None = Field(default=None, alias="whenSourceNonEmpty")


class QualityTestRowCountDelta(_QualityTestBase):
    type: Literal["row_count_delta"] = "row_count_delta"
    tolerance_pct: float = Field(alias="tolerancePct", ge=0)


class QualityTestFreshness(_QualityTestBase):
    type: Literal["freshness"] = "freshness"
    column: str
    max_age_hours: int = Field(alias="maxAgeHours", ge=1)


class QualityTestReconcileTo(_QualityTestBase):
    type: Literal["reconcile_to"] = "reconcile_to"
    source: str
    aggregate: str
    tolerance: float = Field(default=0.0, ge=0)


class QualityTestReferentialIntegrity(_QualityTestBase):
    type: Literal["referential_integrity"] = "referential_integrity"
    column: str
    references: str


class QualityTestCustom(_QualityTestBase):
    """Third-party quality test."""

    type: Literal["custom"] = "custom"
    implementation: str
    args: dict[str, Any] = Field(default_factory=dict)


QualityTest = Annotated[
    (
        QualityTestNotNull
        | QualityTestUnique
        | QualityTestAcceptedValues
        | QualityTestRowCountMin
        | QualityTestRowCountDelta
        | QualityTestFreshness
        | QualityTestReconcileTo
        | QualityTestReferentialIntegrity
        | QualityTestCustom
    ),
    Field(discriminator="type"),
]


class QualitySection(BaseModel):
    """Per-node `quality:` block."""

    model_config = ConfigDict(extra="forbid")

    tests: list[QualityTest] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Node implementation (discriminated union over `type`)
# ---------------------------------------------------------------------------


class SqlImpl(BaseModel):
    """`implementation.type: sql` — a SQL template file."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["sql"] = "sql"
    sql: str
    """Pack-relative path to the SQL template file (e.g., `silver/dim_supplier.sql`)."""


class BuiltinImpl(BaseModel):
    """`implementation.type: builtin` — engine-owned helper (e.g., `dim_calendar`, ADR-0011)."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["builtin"] = "builtin"
    callable: str
    """Importable Python callable, `<module>:<func>` form."""


class BronzeExtractImpl(BaseModel):
    """``implementation.type: bronze_extract`` -- BICC PVO extraction.

    Declares a content-pack-driven bronze node. Carries everything the
    runtime needs to construct a BICC ``PvoEntry``-equivalent descriptor
    WITHOUT depending on the curated ``fusion_catalog.py`` — pack YAML is
    self-contained so customer overlay packs can declare new PVOs without
    a plugin release.

    PVO resolution contract:

    1. The adapter constructs a ``PvoEntry``-equivalent descriptor directly
       from the YAML fields (``datastore``, ``bicc_schema``, ``natural_key``
       from ``refresh.incremental``, ``incremental_capable``).
    2. The descriptor is passed to ``extract_pvo()`` unchanged.
    3. At pack-load time, validators cross-reference
       ``implementation.pvo_id`` against the curated catalog for a WARN
       only. The BICC drift gate (``AIDPF-2072``) catches typo'd PVOs at
       extract-preflight time.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    type: Literal["bronze_extract"] = "bronze_extract"

    datastore: str
    """BICC datastore identifier (e.g. ``SupplierExtractPVO``). What BICC
    actually uses as the PVO handle. **Required.**"""

    pvo_id: str | None = None
    """Optional full AM-hierarchy path
    (``FscmTopModelAM.PrcExtractAM.PozBiccExtractAM.SupplierExtractPVO``).
    Documentation field; used to cross-reference the curated catalog and
    emit an ``AIDPF-2080`` WARN when not found. Missing entirely → no
    WARN (nothing to cross-reference)."""

    bicc_schema: str = Field(alias="biccSchema")
    """BICC offering schema (``Financial`` / ``HCM`` / ``SCM``).
    **Required.** Pack YAML always specifies; runtime never auto-discovers
    behind the operator's back."""

    schema_override: str | None = Field(default=None, alias="schemaOverride")
    """Optional per-tenant BICC offering schema override. Overrides
    ``bicc_schema`` at runtime and matches
    ``bundle.fusion.schemaOverrides.<id>`` semantics."""

    incremental_capable: bool = Field(default=True, alias="incrementalCapable")
    """Whether ``LastUpdateDate`` tracks meaningful change events.

    PVOs such as ``gl_period_balances`` can revise historical period-end
    snapshots retroactively; set this to False when an incremental BICC
    predicate would miss valid changes.

    Runtime behavior:

    * ``mode=seed``: full BICC pull + ``replace`` strategy (independent of
      this flag).
    * ``mode=incremental`` + ``incremental_capable=False``: no BICC
      ``fusion.initial.extract-date`` pushdown (full pull) +
      payload-diff-gated MERGE (content-hash predicate on the source
      side; unchanged rows keep their ``_extract_ts``/``_run_id``).
    """

    audit_columns_mode: Literal["bronze_v1"] = Field(
        default="bronze_v1", alias="auditColumnsMode"
    )
    """Reserved for future audit-column-shape variants. v0.3 uses the
    ``bronze_v1`` shape (``_extract_ts``, ``_source_pvo``, ``_run_id``,
    ``_watermark_used``)."""


NodeImplementation = Annotated[
    SqlImpl | BuiltinImpl | BronzeExtractImpl,
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# NodeYaml (silver/gold per-node YAML file)
# ---------------------------------------------------------------------------


class NodeYaml(BaseModel):
    """Schema for ``silver/<name>.yaml`` and ``gold/<name>.yaml`` files.

    Enforces the strategy validation matrix. Each rule violation raises a
    `ValueError` with a specific AIDPF code so the CLI's
    `content-pack validate` surfaces actionable remediation.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    id: str
    layer: NodeLayer
    implementation: NodeImplementation
    target: str
    """Target table name (resolved against `{{ silver_schema }}` / `{{ gold_schema }}`)."""

    depends_on: DependsOn = Field(default_factory=DependsOn, alias="dependsOn")

    refresh: RefreshSpec

    required_columns: dict[str, list[str]] = Field(
        default_factory=dict, alias="requiredColumns"
    )
    """Per-source required column lists. Keys are source IDs, values are column lists."""

    output_schema: OutputSchema = Field(alias="outputSchema")

    quality: QualitySection = Field(default_factory=QualitySection)

    # Strategy validation matrix.

    @model_validator(mode="after")
    def _validate_strategy_matrix(self) -> "NodeYaml":
        # Bronze nodes are content-pack-driven. They permit seed=replace plus
        # incremental=merge when watermark and naturalKey are declared.
        # Silver/gold nodes use the broader matrix below.
        if self.layer == "bronze":
            seed_strategy = self.refresh.seed.strategy
            if seed_strategy != "replace":
                raise ValueError(
                    f"bronze node {self.id!r}: refresh.seed.strategy must be "
                    f"'replace' (got {seed_strategy!r}). Bronze seed is always "
                    f"a full BICC pull with overwriteSchema=true."
                )
            inc = self.refresh.incremental
            if inc is not None:
                if inc.strategy != "merge":
                    raise ValueError(
                        f"bronze node {self.id!r}: refresh.incremental.strategy "
                        f"must be 'merge' (got {inc.strategy!r}). Bronze does "
                        f"not support replace_partition / aggregate_merge / etc."
                    )
                if not inc.natural_key:
                    raise ValueError(
                        f"{AIDPF_2020_MERGE_NO_NATURAL_KEY}: bronze node "
                        f"{self.id!r} with incremental=merge requires "
                        f"naturalKey on refresh.incremental."
                    )
                if inc.watermark is None:
                    raise ValueError(
                        f"{AIDPF_2050_MERGE_NO_WATERMARK}: bronze node "
                        f"{self.id!r} with incremental=merge requires "
                        f"incremental.watermark.{{source,column}}."
                    )
            return self

        inc = self.refresh.incremental
        if inc is None:
            # No incremental block declared; nothing else to validate.
            return self

        bronze_sources = self.depends_on.bronze
        primary_sources = [s for s in bronze_sources if s.role == "primary"]
        # Determine the primary set. A single bronze source with no explicit
        # role is treated as the implicit primary source.
        implicit_primary = (
            len(bronze_sources) == 1
            and bronze_sources[0].role is None
            and not self.depends_on.silver
        )
        primary_count = (
            len(primary_sources) if not implicit_primary else 1
        )

        s = inc.strategy

        # R9: aggregate_merge is deferred to v0.4+.
        if s == "aggregate_merge":
            raise ValueError(
                f"{AIDPF_2057_AGGREGATE_MERGE_DEFERRED}: strategy `aggregate_merge` "
                "is deferred to v0.4+. Use `replace` in v0.3."
            )

        # R10: snapshot itself is deferred; if declared, must have a
        # unique-on-(natural_key, snapshot_date) quality test.
        if s == "snapshot":
            has_unique_snapshot_test = any(
                isinstance(t, QualityTestUnique)
                and "snapshot_date" in t.columns
                and any(k in t.columns for k in (inc.natural_key or []))
                for t in self.quality.tests
            )
            if not has_unique_snapshot_test:
                raise ValueError(
                    f"{AIDPF_2058_SNAPSHOT_NO_UNIQUE_TEST}: strategy `snapshot` "
                    "(deferred) requires a `unique` quality test on "
                    "(natural_key, snapshot_date)."
                )

        # R11: scd2 itself is deferred; if declared, must have trackedColumns.
        if s == "scd2" and not inc.tracked_columns:
            raise ValueError(
                f"{AIDPF_2059_SCD2_NO_TRACKED_COLUMNS}: strategy `scd2` "
                "(deferred) requires `trackedColumns:`."
            )

        # R1: merge requires naturalKey.
        if s == "merge" and not inc.natural_key:
            raise ValueError(
                f"{AIDPF_2020_MERGE_NO_NATURAL_KEY}: strategy `merge` requires "
                "`naturalKey:` on the node's `refresh.incremental` block."
            )

        # R2: merge requires watermark config.
        if s == "merge" and inc.watermark is None:
            raise ValueError(
                f"{AIDPF_2050_MERGE_NO_WATERMARK}: strategy `merge` requires "
                "`incremental.watermark.{source,column}`."
            )

        # R5: merge with multiple bronze deps and no explicit role classification.
        if s == "merge" and len(bronze_sources) > 1:
            unroled = [src.id for src in bronze_sources if src.role is None]
            if unroled:
                raise ValueError(
                    f"{AIDPF_2053_MERGE_MULTI_BRONZE_NO_ROLE}: strategy `merge` "
                    "with multiple bronze deps requires explicit `role:` "
                    f"(primary|lookup) on every source. Missing role on: {unroled!r}."
                )

        # R3: merge with zero role:primary.
        if s == "merge" and primary_count == 0:
            raise ValueError(
                f"{AIDPF_2051_MERGE_ZERO_PRIMARY}: strategy `merge` requires "
                "exactly one source marked `role: primary`."
            )

        # R4: merge with multiple role:primary (multi-primary deferred).
        if s == "merge" and primary_count > 1:
            raise ValueError(
                f"{AIDPF_2052_MERGE_MULTI_PRIMARY}: strategy `merge` with multiple "
                "`role: primary` sources is deferred to v0.4+. "
                "Collapse to a single primary or switch to `replace`."
            )

        # R6: replace_partition requires partitionColumns or affectedPartitionsFrom.
        if s == "replace_partition" and not (
            inc.partition_columns or inc.affected_partitions_from
        ):
            raise ValueError(
                f"{AIDPF_2054_REPLACE_PARTITION_NO_COLUMNS}: strategy "
                "`replace_partition` requires `partitionColumns:` or "
                "`affectedPartitionsFrom:`."
            )

        # R7: replace_partition with multi-source primary.
        if s == "replace_partition" and primary_count > 1:
            raise ValueError(
                f"{AIDPF_2055_REPLACE_PARTITION_MULTI_PRIMARY}: strategy "
                "`replace_partition` requires a single `role: primary`. "
                "Partition derivation requires an unambiguous primary."
            )

        # R8: append with `unique` quality test but no naturalKey.
        if s == "append" and not inc.natural_key:
            has_unique_test = any(isinstance(t, QualityTestUnique) for t in self.quality.tests)
            if has_unique_test:
                raise ValueError(
                    f"{AIDPF_2056_APPEND_UNIQUE_NO_KEY}: strategy `append` with "
                    "a `unique` quality test must declare `naturalKey:` (or "
                    "remove the test)."
                )

        return self


# ---------------------------------------------------------------------------
# ResolvedPack
# ---------------------------------------------------------------------------
#
# The dispatch package should not import ``orchestrator/*``. Keeping this
# dataclass under ``schema/`` lets the dry-run plan resolver walk content packs
# without crossing that import boundary. ``orchestrator/content_pack.py``
# re-exports ``ResolvedPack`` for backwards compatibility with existing
# consumers.


def _canonicalise(value: Any) -> Any:
    """Recursively sort dict keys for deterministic hashing."""
    if isinstance(value, dict):
        return {k: _canonicalise(value[k]) for k in sorted(value)}
    if isinstance(value, list):
        return [_canonicalise(v) for v in value]
    return value


@dataclass(frozen=True)
class ResolvedPack:
    """A fully-loaded content pack (post-overlay-merge).

    Attributes:
        root: filesystem path to the pack root directory. For merged packs,
            this is the overlay root (top of the chain). Use ``source_roots``
            for per-node path resolution.
        pack: parsed ``pack.yaml`` (top-level).
        silver: per-node-id mapping of silver nodes (parsed from
            ``silver/*.yaml``).
        gold: per-node-id mapping of gold nodes (parsed from ``gold/*.yaml``).
        dashboards: per-dashboard-id mapping (parsed from
            ``dashboards/*.yaml``).
        bronze: per-node-id mapping of bronze nodes parsed from
            ``bronze/*.yaml``. Each carries
            ``implementation.type: bronze_extract`` or a builtin/sql variant.
        bronze_yaml: deprecated legacy single-file ``bronze.yaml``
            declaration. Retained for backwards compatibility with packs that
            have not migrated to per-file ``bronze/<id>.yaml``.
        is_merged: True if this is the result of a merge_overlay call.
        chain: list of pack ids in load order (base first, overlays after).
        source_roots: per-artifact pack-root provenance.
        chain_roots: filesystem roots of every pack in the chain, base-first
            (``(base.root, …, overlay.root)``). Distinct from ``source_roots``
            (which root each *artifact* resolves against): ``chain_roots`` is
            *which packs get staged*, so a pure-metadata overlay that owns no
            artifact is still staged. Accumulated like ``chain``.
    """

    root: Path
    pack: "PackYaml"
    silver: dict[str, "NodeYaml"] = field(default_factory=dict)
    gold: dict[str, "NodeYaml"] = field(default_factory=dict)
    dashboards: dict[str, "DashboardYaml"] = field(default_factory=dict)
    bronze: dict[str, "NodeYaml"] = field(default_factory=dict)
    bronze_yaml: dict[str, Any] = field(default_factory=dict)
    is_merged: bool = False
    chain: tuple[str, ...] = ()
    source_roots: dict[str, Path] = field(default_factory=dict)
    chain_roots: tuple[Path, ...] = ()

    def all_nodes(self) -> dict[str, "NodeYaml"]:
        """Convenience: bronze, silver, and gold nodes combined."""
        return {**self.bronze, **self.silver, **self.gold}

    def root_for(self, qualified_id: str) -> Path:
        """Return the source-pack root for an artifact id, falling back to
        ``root``.

        ``qualified_id`` examples: ``"bronze/erp_suppliers"``,
        ``"silver/dim_supplier"``, ``"gold/gl_balance"``,
        ``"dashboards/executive_cfo"``, ``"bronze.yaml"``.
        """
        return self.source_roots.get(qualified_id, self.root)

    def compute_hash(self) -> str:
        """Stable sha256 of the pack's canonical serialised form.

        Used by plan-hash drift detection. Deterministic across runs: keys
        sorted, no unstable ordering.
        """
        payload: dict[str, Any] = {
            "pack": self.pack.model_dump(mode="json", by_alias=True),
            "bronze": {
                k: v.model_dump(mode="json", by_alias=True)
                for k, v in sorted(self.bronze.items())
            },
            "silver": {
                k: v.model_dump(mode="json", by_alias=True)
                for k, v in sorted(self.silver.items())
            },
            "gold": {
                k: v.model_dump(mode="json", by_alias=True)
                for k, v in sorted(self.gold.items())
            },
            "dashboards": {
                k: v.model_dump(mode="json", by_alias=True)
                for k, v in sorted(self.dashboards.items())
            },
            "bronze_yaml": _canonicalise(self.bronze_yaml),
        }
        blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
        return hashlib.sha256(blob).hexdigest()
