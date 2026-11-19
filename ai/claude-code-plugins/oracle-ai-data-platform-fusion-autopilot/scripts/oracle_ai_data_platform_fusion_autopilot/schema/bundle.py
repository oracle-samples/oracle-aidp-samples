"""Pydantic v2 models for ``bundle.yaml`` and ``aidp.config.yaml``.

The schema layer also owns ``load_bundle`` so CLI, dispatch, and dry-run code
can parse configuration without importing orchestrator runtime internals.
``orchestrator.runtime`` re-exports the loader for compatibility with older
internal callers.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    ValidationError,
    model_validator,
)

from .errors import BundleLoadError, BundleVersionMismatchError
from .refs import render_vars


# ---------------------------------------------------------------------------
# Content-pack bundle AIDPF error codes
# ---------------------------------------------------------------------------
# Surfaced via the schema layer because bundle-shape gates such as a missing
# ``contentPack`` block or an unresolvable pack path are pure config concerns.

AIDPF_1030_PROFILE_MISSING = "AIDPF-1030"
"""`bundle.yaml`'s `contentPack.profile` field is missing when content-pack backend selected."""

AIDPF_1031_CONTENT_PACK_MISSING = "AIDPF-1031"
"""`bundle.yaml` has no `contentPack` block at all when content-pack backend selected."""

AIDPF_1032_RESUME_NOT_SUPPORTED = "AIDPF-1032"
"""`--resume` is not supported with content-pack backend in v0.3."""

AIDPF_1033_PROFILE_FILE_NOT_FOUND = "AIDPF-1033"
"""Resolved profile file (`<bundle>/profiles/<name>.yaml`) does not exist."""

AIDPF_1037_INSTALLED_PACK_NOT_FOUND = "AIDPF-1037"
"""Installed content pack `<name>` not found when `contentPack.path` is None."""

AIDPF_1038_RESOLVED_ROOT_NO_PACK_YAML = "AIDPF-1038"
"""Resolved content-pack root does not contain `pack.yaml` at the resolved path."""

AIDPF_1036_PACK_VALIDATION_FAILED = "AIDPF-1036"
"""Content-pack failed `validate_pack_full(...)` at run-start. The transport
code at the CLI/run boundary; per-error AIDPF codes from the pack validators
carry the specific problems (orphan overrides, DAG cycles, unresolved
variation points, etc.)."""


class ContentPackRootNotFoundError(Exception):
    """Installed pack not found at `<plugin>/content_packs/<name>/`. AIDPF-1037."""


class ContentPackRootInvalidError(Exception):
    """Resolved content-pack root exists but contains no `pack.yaml`. AIDPF-1038."""


class ContentPackValidationFailedError(Exception):
    """Resolved pack failed `validate_pack_full(...)` at run-start. Carries
    the full validation report so operators see every per-error code
    (AIDPF-2003, 2040, 2041, 5002, 5003, 7001, 7003, 7004, 7005, 8002, etc.)
    in addition to the aggregate AIDPF-1036 transport code."""

    def __init__(self, *, report: "Any") -> None:
        self.report = report
        per_error = "\n".join(
            f"  - {e.code} [{e.location}]: {e.message}"
            for e in (report.errors if hasattr(report, "errors") else [])
        )
        super().__init__(
            f"{AIDPF_1036_PACK_VALIDATION_FAILED}: content pack failed "
            f"validate_pack_full at run-start. Refusing to execute or stage "
            f"the pack. Per-error report:\n{per_error}"
        )


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

    # Dispatch coordinates for ``aidp-fusion-autopilot run``. Optional on the
    # model because validate/bootstrap/status do not need them; dispatch
    # preflight enforces presence when they are required.
    ai_data_platform_id: str | None = Field(default=None, alias="aiDataPlatformId")
    cluster_key: str | None = Field(default=None, alias="clusterKey")
    cluster_name: str | None = Field(default=None, alias="clusterName")
    bicc_secret_name: str = Field(default="fusion_bicc_password", alias="biccSecretName")
    bicc_secret_key: str = Field(default="password", alias="biccSecretKey")


class Defaults(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    region: str = "us-ashburn-1"
    api_base: str | None = Field(default=None, alias="apiBase")
    workspace_root: str = Field(default="Shared", alias="workspaceRoot")

    workspace_dir: str | None = Field(default=None, alias="workspaceDir")
    """Server-side notebook upload root for cluster-mode bootstrap.
    ``None`` ⇒ dispatch derives
    ``/Workspace/{workspace_root}/fusion-autopilot-bootstrap`` at call time.
    Only consulted by ``aidp-fusion-autopilot bootstrap --dispatch-mode=cluster``;
    local-mode bootstrap + the existing ``run`` dispatcher both ignore
    it (the run dispatcher builds its own ``/Workspace/{workspace_root}/aidp-fusion-autopilot-{project}``
    path at ``dispatch/__init__.py:243``)."""


class AidpConfig(BaseModel):
    """Top-level ``aidp.config.yaml`` schema."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    api_version: Literal["aidp-fusion-autopilot/v1"] = Field(alias="apiVersion")
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

    schema_overrides: dict[str, str] = Field(
        default_factory=dict, alias="schemaOverrides"
    )
    """Per-PVO BICC offering schema overrides.

    Wins over catalog default + auto-discovery. Key: bundle pvo id —
    matches ``DatasetSpec.id`` / the bronze ``NodeYaml.id`` (the
    customer-facing bundle id). Value: BICC offering schema name as
    it appears in ``/biacm/rest/meta/datastores``.

    Tenant-dependent — use sparingly. The orchestrator's preflight
    auto-discovers the correct schema for ~80% of mismatch cases via
    a one-time ``/biacm/rest/meta/datastores`` probe; this field is
    the escape hatch for the ambiguous case (PVO appears in multiple
    BICC offerings on this tenant) and the operator-known
    short-circuit (skip the probe entirely when the answer is
    already in bundle.yaml).
    """


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

    build: list[str] = Field(
        default_factory=lambda: ["dim_supplier", "dim_account", "dim_calendar", "dim_org"]
    )
    """Default dimension build list for clean-checkout runs.

    ``dim_org`` is retained as a default opt-in but does not ship a
    content-pack node today; the resolver emits ``RunStep(status='deferred')``
    for it instead of crashing.
    """


class GoldSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    marts: list[str] = Field(default_factory=lambda: ["ar_aging", "ap_aging", "gl_balance", "po_backlog"])


class NotificationsSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    on_failure: list[str] = Field(default_factory=list, alias="onFailure")


class IncrementalConfig(BaseModel):
    """Configuration for ``--mode incremental`` runs.

    Today carries only ``watermark_safety_window_seconds``.

    ``watermark_safety_window_seconds`` is the gap subtracted from
    ``extract_started_at`` when persisting the bronze cursor. Absorbs
    AIDP-vs-Fusion clock skew so the next incremental BICC filter
    doesn't drop rows at the boundary. Default ``3600`` (1h) — wider
    than typical NTP-synced drift between OCI-hosted AIDP and Fusion
    Cloud; matches the runtime module's default ``WATERMARK_SAFETY_WINDOW``.
    Validated ``gt=0`` because zero would erase the buffer and a
    negative value would move the cursor INTO THE FUTURE relative to
    ``extract_started_at`` (BICC returns zero rows even when data exists).
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    watermark_safety_window_seconds: int = Field(
        default=3600,
        gt=0,
        alias="watermarkSafetyWindowSeconds",
        description=(
            "Safety window (seconds) subtracted from extract_started_at when "
            "persisting the bronze watermark cursor. Default 3600 (1h)."
        ),
    )


class OacSnapshotSpec(BaseModel):
    """Where the bundle's ``.bar`` snapshot lives in the customer's OCI tenancy."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    bucket: str | None = None
    """OCI Object Storage bucket name containing the bundle's .bar."""

    uri: str | None = None
    """Object name (relative path) of the .bar within the bucket."""

    password: str | None = None
    """Optional BAR password. May be a ``${vault:OCID}`` reference."""

    snapshot_name: str = Field(default="aidp-fusion-autopilot", alias="snapshotName")
    """Display name for the registered snapshot (visible in OAC Console -> Snapshots)."""


class OacDashboardSpec(BaseModel):
    """OAC integration block under ``oac:`` in bundle.yaml.

    Architecture — strictly Oracle-documented endpoints:
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


class ContentPackSpec(BaseModel):
    """Declare which content pack and tenant profile this bundle runs.

    Three resolution shapes for the pack directory, handled by
    :func:`resolve_content_pack_root` (not by Pydantic):

    * ``path`` absent → installed Oracle-shipped pack at
      ``<plugin>/content_packs/<name>/``.
    * ``path`` absolute → used as-is.
    * ``path`` relative → resolved against ``bundle.yaml``'s parent
      directory (NOT CWD — bundle-relative resolution survives `cd`).

    The profile YAML always lives beside the bundle at
    ``<bundle.yaml.parent>/profiles/<profile>.yaml``; profiles do not live
    inside the content-pack directory.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    name: str
    """Pack id (e.g. ``fusion-finance-starter``). Required."""

    path: Path | None = None
    """Override pack location (relative or absolute). ``None`` → installed-pack lookup."""

    profile: str | None = None
    """Active tenant profile name. Required at runtime under content-pack backend."""

    allow_unprovable_coa: StrictBool = Field(
        default=False, alias="allowUnprovableCOA"
    )
    """Opt-in escape hatch for the COA gate: when ``true``, a COA correctness
    PROBE that cannot EXECUTE (constrained Spark session) downgrades to a loud
    logged WARN instead of a hard AIDPF-2074 abort — but ONLY when no COA
    VIOLATION was retained (a real 5001/2016/2042/2018/2017 violation still
    hard-blocks). Default ``false`` (correctness first).

    ``StrictBool`` on purpose: a plain ``bool`` would coerce ``1``/``"yes"``/
    ``"on"``/``"true"`` and risk an ACCIDENTAL enable of a correctness bypass;
    ``StrictBool`` accepts only native ``true``/``false``."""


class Bundle(BaseModel):
    """Top-level ``bundle.yaml`` schema."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    api_version: Literal["aidp-fusion-autopilot/v1"] = Field(alias="apiVersion")
    version: Literal["0.2.0"] = "0.2.0"
    """Bundle schema version.

    Default `"0.2.0"` so existing bundles continue to load implicitly. When a
    breaking schema change ships in v0.3, this Literal widens to
    `Literal["0.2.0", "0.3.0"]` and a migration helper rewrites v0.2 → v0.3 in
    place via `aidp-fusion-autopilot migrate-bundle`. `load_bundle()` re-raises
    version-specific Pydantic `ValidationError`s as `BundleVersionMismatchError`
    with a migration hint so operators see a remediation-specific message.
    """
    project: str
    variables: dict[str, str] = Field(default_factory=dict)
    fusion: FusionConn
    aidp: AidpRefs = AidpRefs()
    datasets: list[DatasetSpec]
    dimensions: DimensionsSpec = DimensionsSpec()
    gold: GoldSpec = GoldSpec()
    oac: OacDashboardSpec | None = None
    notifications: NotificationsSpec = NotificationsSpec()
    incremental: IncrementalConfig = Field(default_factory=IncrementalConfig)
    content_pack: ContentPackSpec | None = Field(default=None, alias="contentPack")
    """Active content-pack declaration. Current customer bundles should set it."""

    @model_validator(mode="after")
    def _validate_unique_dataset_ids(self) -> Self:
        seen: set[str] = set()
        for ds in self.datasets:
            if ds.id in seen:
                raise ValueError(f"duplicate dataset id: {ds.id}")
            seen.add(ds.id)
        return self


# ---------------------------------------------------------------------------
# Bundle loader
# ---------------------------------------------------------------------------
#
# Lives at the schema layer because every step is a pure schema-level
# operation: YAML parse, generic ``${VAR}`` env-var rendering, Pydantic
# validation, TablePaths identifier check. ``${env:VAR}`` and
# ``${vault:OCID}`` references are preserved literally — they're credential
# markers resolved later by ``orchestrator.runtime._resolve_password``.


def _render_env_vars(node: Any) -> Any:
    """Recursively expand ``${VAR}`` env-var refs in a parsed-YAML structure.

    Leaves ``${vault:OCID}`` and ``${env:VAR}`` references untouched (handled
    by the negative-lookahead in ``schema.refs.render_vars``). Raises
    ``BundleLoadError`` naming the missing variable when an env-var ref
    cannot be resolved — bare ``KeyError`` never bubbles through.
    """
    if isinstance(node, dict):
        return {k: _render_env_vars(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_render_env_vars(v) for v in node]
    if isinstance(node, str):
        try:
            return render_vars(node)
        except KeyError as e:
            raise BundleLoadError(
                f"Missing env var {e.args[0]!r} referenced in bundle.yaml. "
                f"Set it before running, or override on the CLI."
            ) from e
    return node  # int, float, bool, None — pass through


def load_bundle(bundle_path: Path) -> tuple["Bundle", "TablePaths"]:
    """Load and validate a bundle.yaml, returning the parsed model + resolved paths.

    Single entry point that wraps EVERY config-load failure mode into
    ``BundleLoadError`` so the CLI's exit-2 path catches them all (no
    bare tracebacks for malformed YAML, missing env var, schema
    violations, or bad ``aidp.*`` identifiers).

    Failure modes:
      1. File-not-found / permission / IsADirectoryError / OSError
      2. yaml.YAMLError (malformed YAML)
      3. _render_env_vars KeyError (missing env var) — already wrapped
      4. pydantic.ValidationError (schema violation) — version-specific
         re-raised as ``BundleVersionMismatchError``
      5. TypeError/ValueError from TablePaths._validate_identifier

    Exception chain preserved via ``raise ... from e``.
    """
    # Local import to avoid an import cycle if anyone ever points
    # ``config.paths`` at schema-level helpers. TablePaths is pure
    # (no engine imports), so this is purely a hygiene measure.
    from oracle_ai_data_platform_fusion_autopilot.config.paths import TablePaths

    bundle_path = Path(bundle_path)

    # 1. File read.
    try:
        text = bundle_path.read_text(encoding="utf-8")
    except FileNotFoundError as e:
        raise BundleLoadError(f"Bundle file not found: {bundle_path}") from e
    except IsADirectoryError as e:
        raise BundleLoadError(
            f"Bundle path is a directory, not a file: {bundle_path}"
        ) from e
    except PermissionError as e:
        raise BundleLoadError(
            f"Cannot read bundle {bundle_path}: permission denied"
        ) from e
    except OSError as e:
        raise BundleLoadError(
            f"Cannot read bundle {bundle_path}: {e.strerror or e}"
        ) from e

    # 2. YAML parse.
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as e:
        mark = getattr(e, "problem_mark", None)
        loc = f" at line {mark.line + 1} col {mark.column + 1}" if mark else ""
        problem = getattr(e, "problem", str(e))
        raise BundleLoadError(
            f"Malformed YAML in {bundle_path}{loc}: {problem}"
        ) from e

    if not isinstance(raw, dict):
        raise BundleLoadError(
            f"Bundle {bundle_path} must be a YAML mapping at the top level, "
            f"got {type(raw).__name__}"
        )

    # 3. Env-var expansion.
    rendered = _render_env_vars(raw)

    # 4. Pydantic validation — hoist version errors into the specific class.
    try:
        bundle = Bundle.model_validate(rendered)
    except ValidationError as e:
        version_errs = [err for err in e.errors() if err["loc"] == ("version",)]
        if version_errs:
            offending = version_errs[0].get("input", "<unknown>")
            raise BundleVersionMismatchError(
                f"Bundle {bundle_path} declares version={offending!r}; "
                f"this plugin supports version='0.2.0'. "
                f"Run `aidp-fusion-autopilot migrate-bundle "
                f"--from {offending} --to 0.2.0`."
            ) from e
        details = "\n".join(
            f"  - {'.'.join(str(p) for p in err['loc'])}: {err['msg']}"
            for err in e.errors()
        )
        raise BundleLoadError(
            f"Bundle {bundle_path} failed schema validation:\n{details}"
        ) from e

    # 5. TablePaths identifier validation.
    try:
        paths = TablePaths.from_bundle(bundle.model_dump(by_alias=True))
    except (TypeError, ValueError) as e:
        raise BundleLoadError(
            f"Bundle {bundle_path} has invalid aidp.* identifier: {e}"
        ) from e

    return bundle, paths


# ---------------------------------------------------------------------------
# Content-pack root resolution
# ---------------------------------------------------------------------------


def resolve_content_pack_root(bundle_path: Path, spec: ContentPackSpec) -> Path:
    """Resolve the on-disk root of a ContentPackSpec.

    Three cases:

    * ``spec.path is None`` — installed-pack lookup at
      ``<plugin>/content_packs/<spec.name>/``. Missing → AIDPF-1037.
    * ``spec.path.is_absolute()`` — used as-is.
    * ``spec.path`` relative — resolved against ``bundle_path.parent``
      (NOT cwd; bundle-relative resolution survives ``cd``).

    In every case, the returned path must contain a ``pack.yaml`` file
    or :class:`ContentPackRootInvalidError` is raised (AIDPF-1038).

    Args:
        bundle_path: path to the ``bundle.yaml`` file (its parent dir
            is the anchor for relative ``spec.path``).
        spec: parsed ``ContentPackSpec`` from the bundle.

    Returns:
        Resolved absolute path to the pack root directory.

    Raises:
        ContentPackRootNotFoundError: installed pack lookup miss
            (AIDPF-1037). Carries ``spec.name`` in the message.
        ContentPackRootInvalidError: resolved root has no ``pack.yaml``
            (AIDPF-1038). Carries the resolved path in the message.
    """
    if spec.path is None:
        # Installed-pack lookup — import lazily to avoid pulling
        # commands/content_pack.py into the schema layer at import time.
        from ..commands.content_pack import INSTALLED_CONTENT_PACKS_DIR
        candidate = (INSTALLED_CONTENT_PACKS_DIR / spec.name).resolve()
        if not candidate.exists():
            raise ContentPackRootNotFoundError(
                f"{AIDPF_1037_INSTALLED_PACK_NOT_FOUND}: installed content pack "
                f"{spec.name!r} not found at {candidate}. Check spelling or set "
                f"`contentPack.path` to a local directory."
            )
    elif spec.path.is_absolute():
        candidate = spec.path.resolve()
    else:
        candidate = (bundle_path.parent / spec.path).resolve()

    if not (candidate / "pack.yaml").exists():
        raise ContentPackRootInvalidError(
            f"{AIDPF_1038_RESOLVED_ROOT_NO_PACK_YAML}: resolved content-pack root "
            f"{candidate} contains no pack.yaml file."
        )
    return candidate
