"""Pydantic models for dashboard pack YAML.

A dashboard pack pairs a YAML contract (this module) with an OAC `.bar`
snapshot. The YAML declares the gold-table dependencies, PII rules, and
validation queries the dashboard needs; the `.bar` is the opaque OAC content
that gets uploaded at install time.

The engine never parses `.bar` files. The dashboard YAML's `requires.columns`
block is a hand-authored declaration; OAC catches any drift between `.bar` and
YAML at import time.

Error codes raised here:

    * AIDPF-7001 -- dashboard requires.tables references undeclared gold node
                    (raised at validation time, not schema time — see
                    orchestrator/content_pack_validators.py)
    * AIDPF-8002 -- pii: high column in requires.columns or allowedColumns
                    (raised by validators with access to gold outputSchema)

Schema-level rules:

    * `severity:` on validation queries restricted to `error | warning`.
    * `delivery.type` restricted to `oac-snapshot` for v0.3.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Delivery (OAC snapshot install)
# ---------------------------------------------------------------------------


class DashboardDeliveryOac(BaseModel):
    """OAC-specific delivery options."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    project_name: str = Field(alias="projectName")
    folder_path: str = Field(alias="folderPath")
    connection_name: str = Field(alias="connectionName")
    """Logical OAC connection name; bundle.yaml maps it to the tenant's actual connection."""

    refresh_mode: Literal["live", "cached", "on-demand"] = Field(
        default="live", alias="refreshMode"
    )
    import_mode: Literal["replace", "merge", "preserve-customizations"] = Field(
        default="replace", alias="importMode"
    )


class DashboardDelivery(BaseModel):
    """How the dashboard reaches OAC."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    type: Literal["oac-snapshot"] = "oac-snapshot"
    bar_object: str = Field(alias="barObject")
    """Pack-relative path to the .bar snapshot file."""

    oac: DashboardDeliveryOac


# ---------------------------------------------------------------------------
# Requires (gold contract dependencies)
# ---------------------------------------------------------------------------


class DashboardRequiresPack(BaseModel):
    """Pack-version compatibility for this dashboard."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    id: str
    min_version: str = Field(alias="minVersion")
    max_version: str | None = Field(default=None, alias="maxVersion")


class RequiredColumn(BaseModel):
    """One column the dashboard depends on, with its expected Spark type."""

    model_config = ConfigDict(extra="forbid")

    name: str
    type: str


class FreshnessRequirement(BaseModel):
    """How fresh a required table must be at install time."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    max_age_hours: int = Field(alias="maxAgeHours", ge=1)


class DashboardRequires(BaseModel):
    """The dashboard's gold contract requirements."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    pack: DashboardRequiresPack
    tables: list[str] = Field(min_length=1)
    """Fully-qualified gold table names (e.g., `gold.gl_balance`)."""

    columns: dict[str, list[RequiredColumn]] = Field(default_factory=dict)
    """Keyed by table name; each value is the list of required columns."""

    freshness: dict[str, FreshnessRequirement] = Field(default_factory=dict)
    """Optional per-table freshness SLOs."""


# ---------------------------------------------------------------------------
# Validation queries
# ---------------------------------------------------------------------------


class ValidationQueryExpectation(BaseModel):
    """Numeric expectations for a validation query result."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    row_count_min: int | None = Field(default=None, alias="row_count_min")
    row_count_max: int | None = Field(default=None, alias="row_count_max")
    value_min: float | None = Field(default=None, alias="value_min")
    value_max: float | None = Field(default=None, alias="value_max")


class ValidationQuery(BaseModel):
    """One pre-install sanity-check SQL query."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    id: str
    sql: str
    expect: ValidationQueryExpectation
    severity: Literal["error", "warning"] = "error"


# ---------------------------------------------------------------------------
# Security / PII firewall
# ---------------------------------------------------------------------------


class DashboardSecurity(BaseModel):
    """PII firewall for the dashboard.

    `allowedColumns` declares which gold columns the dashboard intends to
    display. Validators (orchestrator/content_pack_validators.py) cross-check
    each entry against the source gold node's `outputSchema.pii` field and
    reject `pii: high` columns with AIDPF-8002.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    allowed_columns: dict[str, list[str]] = Field(
        default_factory=dict, alias="allowedColumns"
    )


# ---------------------------------------------------------------------------
# Metadata + connection
# ---------------------------------------------------------------------------


class DashboardWorkbook(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    pages: list[str] = Field(default_factory=list)


class DashboardRefreshSchedule(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    suggested_cron: str = Field(alias="suggestedCron")
    rationale: str | None = None


class DashboardMetadata(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    audience: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    workbooks: list[DashboardWorkbook] = Field(default_factory=list)
    refresh_schedule: DashboardRefreshSchedule | None = Field(
        default=None, alias="refreshSchedule"
    )


class DashboardConnectionSource(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    type: Literal["aidp-delta"] = "aidp-delta"
    schema_pattern: str = Field(alias="schemaPattern")
    required_privileges: list[str] = Field(default_factory=list, alias="requiredPrivileges")


class DashboardConnection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source: DashboardConnectionSource


# ---------------------------------------------------------------------------
# Top-level DashboardYaml
# ---------------------------------------------------------------------------


class DashboardYaml(BaseModel):
    """Top-level schema for a dashboard pack YAML file."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    id: str
    title: str
    version: str
    description: str | None = None

    delivery: DashboardDelivery
    requires: DashboardRequires
    validation_queries: list[ValidationQuery] = Field(
        default_factory=list, alias="validationQueries"
    )
    security: DashboardSecurity = Field(default_factory=DashboardSecurity)
    metadata: DashboardMetadata = Field(default_factory=DashboardMetadata)
    connection: DashboardConnection | None = None
