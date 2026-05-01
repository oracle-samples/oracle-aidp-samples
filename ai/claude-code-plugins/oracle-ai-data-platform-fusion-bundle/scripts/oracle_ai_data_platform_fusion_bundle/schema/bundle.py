"""Pydantic v2 models for ``bundle.yaml`` and ``aidp.config.yaml``."""

from __future__ import annotations

from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


# ---------------------------------------------------------------------------
# aidp.config.yaml  (workspace coords + env mapping)
# ---------------------------------------------------------------------------


class AuthSpec(BaseModel):
    """How the bundle authenticates to Fusion + AIDP for a given environment."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["profile", "vault"] = "profile"
    """``profile`` = local OCI session token; ``vault`` = CI runner with secrets in OCI Vault."""

    api_key_ocid: str | None = Field(default=None, alias="apiKeyOcid")
    """Vault secret OCID containing the API key JSON (mode=vault only)."""

    private_key_ocid: str | None = Field(default=None, alias="privateKeyOcid")
    """Vault secret OCID containing the base64 PEM private key (mode=vault only)."""


class EnvSpec(BaseModel):
    """One named environment block (e.g. ``dev``, ``prod``)."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    workspace_key: str = Field(alias="workspaceKey")
    data_lake_ocid: str | None = Field(default=None, alias="dataLakeOcid")
    region: str | None = None
    oci_profile: str | None = Field(default="DEFAULT", alias="ociProfile")
    auth: AuthSpec = AuthSpec()


class Defaults(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    region: str = "us-ashburn-1"
    api_base: str | None = Field(default=None, alias="apiBase")
    workspace_root: str = Field(default="Shared", alias="workspaceRoot")


class AidpConfig(BaseModel):
    """Top-level ``aidp.config.yaml`` schema."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    api_version: Literal["aidp-fusion-bundle/v1"] = Field(alias="apiVersion")
    project: str
    defaults: Defaults = Defaults()
    environments: dict[str, EnvSpec]


# ---------------------------------------------------------------------------
# bundle.yaml  (datasets, dimensions, gold marts, OAC dashboards)
# ---------------------------------------------------------------------------


class FusionConn(BaseModel):
    """Fusion connection block under ``fusion:`` in bundle.yaml."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    service_url: str = Field(alias="serviceUrl")
    username: str
    password: str
    """May contain a ``${vault:OCID}`` reference; resolved at orchestrator startup."""

    external_storage: str = Field(alias="externalStorage")
    """The BICC console External Storage profile name (set up once by an admin in BICC's "Configure External Storage" tab — there is no parallel AIDP-side registration)."""


class AidpRefs(BaseModel):
    """AIDP-side targets for bronze/silver/gold tables."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    catalog: str = "fusion_catalog"
    bronze_schema: str = Field(default="bronze", alias="bronzeSchema")
    silver_schema: str = Field(default="silver", alias="silverSchema")
    gold_schema: str = Field(default="gold", alias="goldSchema")
    storage_format: Literal["delta", "iceberg"] = Field(default="delta", alias="storageFormat")


class DatasetSpec(BaseModel):
    """One dataset entry (corresponds to a curated PVO)."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    id: str
    """Bundle's logical id (e.g. ``erp_suppliers``); maps to ``schema/fusion_catalog.py``."""

    mode: Literal["incremental", "full", "seed"] = "incremental"
    schedule: str | None = None
    """Cron expression for AIDP-side scheduling. Optional."""

    enabled: bool = True


class DimensionsSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    build: list[str] = Field(default_factory=lambda: ["dim_account", "dim_calendar", "dim_org"])


class GoldSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    marts: list[str] = Field(default_factory=lambda: ["ar_aging", "ap_aging", "gl_balance", "po_backlog"])


class NotificationsSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    on_failure: list[str] = Field(default_factory=list, alias="onFailure")


class OacSnapshotSpec(BaseModel):
    """Where the bundle's ``.bar`` snapshot lives in the customer's OCI tenancy."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    bucket: str | None = None
    """OCI Object Storage bucket name containing the bundle's .bar."""

    uri: str | None = None
    """Object name (relative path) of the .bar within the bucket."""

    password: str | None = None
    """Optional BAR password. May be a ``${vault:OCID}`` reference."""

    snapshot_name: str = Field(default="aidp-fusion-bundle", alias="snapshotName")
    """Display name for the registered snapshot (visible in OAC Console -> Snapshots)."""


class OacDashboardSpec(BaseModel):
    """OAC integration block under ``oac:`` in bundle.yaml.

    Architecture (TC10h-2 refactor, 2026-05-01) — strictly Oracle-documented endpoints:
      1. POST /api/20210901/catalog/connections             (create AIDP connection)
      2. POST /api/20210901/snapshots                       (register customer-uploaded .bar)
      3. POST /api/20210901/system/actions/restoreSnapshot  (async restore)
      4. GET  /api/20210901/workRequests/{id}               (poll until SUCCEEDED)

    The bundle ships a single ``.bar`` snapshot as a release artifact (built
    once via ``bundle build-bar`` from a clean dev OAC). Customer uploads it
    to their own OCI Object Storage bucket and grants OAC's Resource Principal
    read access. See ``docs/oac_rest_api_setup.md``.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    enabled: bool = True
    url: str | None = None
    """OAC instance URL. May be supplied via CLI flag too."""

    data_source_name: str = Field(default="aidp_fusion_jdbc", alias="dataSourceName")

    # Workbook content delivery via snapshot
    snapshot: OacSnapshotSpec = Field(default_factory=OacSnapshotSpec)
    """Where the bundle's .bar snapshot lives. Set ``snapshot.bucket`` + ``snapshot.uri``
    to enable workbook restore; omit for connection-only install."""

    # ---- IDCS OAuth (one-time admin setup; see docs/oac_rest_api_setup.md) ----
    idcs_url: str | None = Field(default=None, alias="idcsUrl")
    """IDCS stripe URL, e.g. ``https://idcs-<stripe>.identity.oraclecloud.com``."""

    oauth_client_id: str | None = Field(default=None, alias="oauthClientId")
    oauth_client_secret: str | None = Field(default=None, alias="oauthClientSecret")
    """May be a ``${vault:OCID}`` reference."""

    oauth_scope: str | None = Field(
        default=None,
        alias="oauthScope",
    )
    """Override the auto-derived scope. Default: auto-discover the IDCS audience
    from the OAC ``/ui/`` redirect, then build ``<audience>urn:opc:resource:consumer::all offline_access``."""

    # ---- AIDP JDBC connection params (the 6-key JSON OAC's connector needs) ----
    api_key_user_ocid: str | None = Field(default=None, alias="apiKeyUserOcid")
    """OCID of the OCI user that owns the registered API key."""

    tenancy_ocid: str | None = Field(default=None, alias="tenancyOcid")
    api_key_fingerprint: str | None = Field(default=None, alias="apiKeyFingerprint")
    cluster_key: str | None = Field(default=None, alias="clusterKey")
    """AIDP cluster key (UUID-like) used in the JDBC ``httpPath=cliservice/<key>``."""

    catalog: str = "default"
    """Default JDBC catalog (OAC sees all catalogs in the schema tree once connected)."""


class Bundle(BaseModel):
    """Top-level ``bundle.yaml`` schema."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    api_version: Literal["aidp-fusion-bundle/v1"] = Field(alias="apiVersion")
    project: str
    variables: dict[str, str] = Field(default_factory=dict)
    fusion: FusionConn
    aidp: AidpRefs = AidpRefs()
    datasets: list[DatasetSpec]
    dimensions: DimensionsSpec = DimensionsSpec()
    gold: GoldSpec = GoldSpec()
    oac: OacDashboardSpec | None = None
    notifications: NotificationsSpec = NotificationsSpec()

    @model_validator(mode="after")
    def _validate_unique_dataset_ids(self) -> Self:
        seen: set[str] = set()
        for ds in self.datasets:
            if ds.id in seen:
                raise ValueError(f"duplicate dataset id: {ds.id}")
            seen.add(ds.id)
        return self
