"""Implementation of ``aidp-fusion-autopilot bootstrap``.

Two phases:

1. **Pre-onboarding probes** (existing) — validate ``bundle.yaml`` +
   ``aidp.config.yaml``, ping BICC + AIDP REST, reconcile the catalog,
   optionally probe IAM. Skippable via ``--skip-preonboarding-probes``
   for ``--refresh`` flows.
2. **Variation-resolution phase** — when
   ``bundle.content_pack`` is set, walk the pack's declared
   ``columnAliases`` / ``semanticVariants`` against the tenant's live
   bronze schema, pin chosen values to ``profiles/<tenant>.yaml``,
   write an evidence snapshot. ``--refresh`` re-walks every variation
   point against a possibly-drifted bronze.

v1 bundles (no ``contentPack:`` block) skip variation resolution; their
existing ``bootstrap`` behaviour is unchanged.

Probes performed in phase 1:
  1. ``bundle.yaml + aidp.config.yaml`` schema validate
  2. ``GET <pod>/biacm/rest/meta/datastores`` reachable + auth works
  3. Catalog reconciliation (every dataset id resolves to a live datastore)
  4. AIDP REST API reachable using the current OCI session token
  5. (Optional, ``--check-iam``) AIDP RP IAM policies cover the workspace
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import requests
import yaml
from pydantic import ValidationError
from rich.console import Console
from rich.table import Table

from ..schema.bundle import AidpConfig, Bundle
from ..schema.fusion_catalog import CATALOG
from ..schema.refs import render_tree
from .variation_phase import (
    RefreshRequiresConfirmation,
    VariationPhaseOptions,
    VariationPhaseOutcome,
    run_variation_phase,
)


class _ProbeResult:
    __slots__ = ("name", "status", "detail", "remediation")

    def __init__(
        self,
        name: str,
        status: str,
        detail: str = "",
        remediation: str = "",
    ) -> None:
        self.name = name
        self.status = status  # "PASS" | "FAIL" | "SKIP"
        self.detail = detail
        self.remediation = remediation


# ---------------------------------------------------------------------------
# Cluster-dispatch config resolution
# ---------------------------------------------------------------------------


# Env var names the CLI consults when the corresponding flag is unset.
# Operator wires these into their `.envrc` to avoid retyping cluster
# coords on every bootstrap.
_ENV_VAR_CLUSTER_KEY = "AIDP_FUSION_CLUSTER_KEY"
_ENV_VAR_CLUSTER_NAME = "AIDP_FUSION_CLUSTER_NAME"
_ENV_VAR_WORKSPACE_DIR = "AIDP_FUSION_WORKSPACE_DIR"


@dataclass(frozen=True)
class ResolvedClusterDispatchConfig:
    """Final cluster-dispatch coordinates after applying the override chain.

    Construction always succeeds; field values may be ``None`` when a
    required input is unresolved. AIDPF-2047 detects those
    nulls in cluster mode and fails-closed.

    Resolution chain per field: CLI flag → env var → ``EnvSpec.<field>``
    (or ``Defaults.<field>`` for ``workspace_dir``) → ``None``. The CLI
    flag wins because it's per-invocation; the env var is per-shell;
    the YAML is per-bundle.
    """

    aidp_id: str | None
    workspace_key: str
    cluster_key: str | None
    cluster_name: str | None
    region: str
    oci_profile: str
    workspace_dir: str

    def missing_fields(self) -> list[str]:
        """Return the list of fields that must be set for cluster-mode
        dispatch but were not resolvable. Step 9 surfaces these as the
        ``AIDPF-2047 (reason=missing_config)`` remediation hint."""
        missing: list[str] = []
        if not self.aidp_id:
            missing.append("aiDataPlatformId")
        if not self.cluster_key:
            missing.append("clusterKey")
        if not self.cluster_name:
            missing.append("clusterName")
        # workspace_dir always has a derived default; workspace_key is
        # already required by EnvSpec at parse time, so we never check
        # them here.
        return missing


def _resolve_cluster_dispatch_config(
    env,  # EnvSpec — typed loosely to avoid a forward-import cycle
    defaults,  # Defaults
    *,
    cluster_key_override: str | None = None,
    cluster_name_override: str | None = None,
    workspace_dir_override: str | None = None,
) -> ResolvedClusterDispatchConfig:
    """Apply CLI → env-var → YAML resolution to the cluster-dispatch coords.

    Pure function. Does NOT raise on missing fields — callers inspect
    :meth:`ResolvedClusterDispatchConfig.missing_fields` and decide how
    to react (Step 9 emits AIDPF-2047 in cluster mode; local mode
    ignores the result).
    """

    def _resolve(
        cli_override: str | None,
        env_var: str,
        yaml_value: str | None,
    ) -> str | None:
        if cli_override:
            return cli_override
        env_value = os.environ.get(env_var)
        if env_value:
            return env_value
        return yaml_value

    workspace_dir = _resolve(
        workspace_dir_override,
        _ENV_VAR_WORKSPACE_DIR,
        defaults.workspace_dir,
    )
    if not workspace_dir:
        # Step 4a "derived default" — no missing-fields entry needed
        # since we always have a value.
        workspace_root = (defaults.workspace_root or "Shared").strip("/")
        workspace_dir = f"/Workspace/{workspace_root}/fusion-autopilot-bootstrap"

    return ResolvedClusterDispatchConfig(
        aidp_id=env.ai_data_platform_id,
        workspace_key=env.workspace_key,
        cluster_key=_resolve(
            cluster_key_override, _ENV_VAR_CLUSTER_KEY, env.cluster_key
        ),
        cluster_name=_resolve(
            cluster_name_override, _ENV_VAR_CLUSTER_NAME, env.cluster_name
        ),
        region=env.region or defaults.region or "us-ashburn-1",
        oci_profile=env.oci_profile or "DEFAULT",
        workspace_dir=workspace_dir,
    )


def bootstrap(
    bundle_path: Path,
    config_path: Path,
    env_name: str,
    *,
    check_iam: bool = False,
    console: Console | None = None,
    # --- Variation-resolution flags ---
    refresh: bool = False,
    operator: str | None = None,
    non_interactive: bool = False,
    accept_coa_convention: bool = False,
    accept_singleton_coa: bool = False,
    resolve_coa_from_metadata: bool = False,
    repin_coa_from_metadata: bool = False,
    resolutions_path: Path | None = None,
    skip_preonboarding_probes: bool = False,
    spark_session=None,
    # --- Cluster-side bootstrap dispatcher flags ---
    dispatch_mode: Literal["cluster", "local"] = "cluster",
    cluster_key_override: str | None = None,
    cluster_name_override: str | None = None,
    workspace_dir_override: str | None = None,
) -> int:
    """Run pre-onboarding probes + variation-resolution phase.

    Returns process exit code (0 on success, non-zero on any failure).

    Args:
        bundle_path: path to ``bundle.yaml``.
        config_path: path to ``aidp.config.yaml``.
        env_name: environment key from ``aidp.config.yaml``.
        check_iam: optional IAM probe.
        console: Rich console for output.
        refresh: re-walk against possibly-drifted bronze.
        operator: explicit ``--operator`` value.
        non_interactive: multi-match auto-pick mode.
        resolutions_path: scripted multi-match resolutions JSON.
        skip_preonboarding_probes: skip pre-onboarding probes (power-user knob for
            ``--refresh`` flows that already passed them on initial
            onboarding). Incompatible with ``dispatch_mode="cluster"`` —
            the conflict surfaces as ``AIDPF-2047 (reason=conflicting_flags)``
            in Step 9.
        spark_session: caller-injected Spark session (tests). Only used
            in ``dispatch_mode="local"`` — cluster mode never
            instantiates a local Spark session.
        dispatch_mode: ``"cluster"`` (default) dispatches
            the variation-phase bronze probe to the AIDP cluster via
            a notebook. ``"local"`` keeps the legacy laptop-Spark
            behaviour for backward compat with unit tests / laptop POC.
        cluster_key_override: per-invocation override of
            ``EnvSpec.cluster_key``. CLI flag → env var
            ``AIDP_FUSION_CLUSTER_KEY`` → ``EnvSpec``.
        cluster_name_override: per-invocation override of
            ``EnvSpec.cluster_name`` (same chain).
        workspace_dir_override: per-invocation override of
            ``Defaults.workspace_dir`` / env var
            ``AIDP_FUSION_WORKSPACE_DIR`` (same chain). When fully
            unresolved, derives
            ``/Workspace/{workspace_root}/fusion-autopilot-bootstrap``.
    """
    console = console or Console()
    results: list[_ProbeResult] = []

    # ---- AIDPF-2047 (conflicting_flags) gate ----
    # `--dispatch-mode=cluster` + `--skip-preonboarding-probes` is
    # incompatible: cluster dispatch needs the aidp-rest probe to
    # confirm auth before paying the wheel-build + notebook round-trip
    # cost. Fail-closed at CLI parse time, no artifact (AIDPF-2047 is
    # CLI-level per Step 8).
    if dispatch_mode == "cluster" and skip_preonboarding_probes:
        console.print(
            "[red]AIDPF-2047 (reason=conflicting_flags): "
            "--dispatch-mode=cluster is incompatible with "
            "--skip-preonboarding-probes (the aidp-rest probe is "
            "load-bearing in cluster mode). Drop the skip flag, or "
            "pass --dispatch-mode=local if you genuinely want to "
            "skip the probe and use laptop-local Spark.[/red]"
        )
        return 1

    bundle, config = _load(bundle_path, config_path, results)
    if bundle and config:
        env = config.environments.get(env_name)
        if env is None:
            results.append(_ProbeResult(
                "env-lookup", "FAIL",
                f"env '{env_name}' not in aidp.config.yaml",
                f"add '{env_name}:' under environments: in aidp.config.yaml",
            ))
        elif not skip_preonboarding_probes:
            _probe_bicc(bundle, results)
            _probe_aidp(env, results)
            if check_iam:
                results.append(_ProbeResult(
                    "iam-policy", "SKIP",
                    "policy probe requires AIDP RP credentials and is not auto-discoverable",
                    "verify manually that AIDP RP can read the BICC External Storage bucket",
                ))
        elif skip_preonboarding_probes:
            results.append(_ProbeResult(
                "preonboarding-probes", "SKIP",
                "skipped via --skip-preonboarding-probes",
                "re-run without the flag to validate pod / AIDP reachability",
            ))

    _render(results, console)
    phase1_failed = any(r.status == "FAIL" for r in results)
    if phase1_failed:
        return 1

    # ---- AIDPF-2047 (aidp_rest_probe_failed) gate ----
    # In cluster mode the aidp-rest probe MUST pass — a SKIP (oci SDK
    # missing) is just as fatal as a FAIL because cluster dispatch
    # needs the OCI signer. Locally we just printed PASS/FAIL/SKIP;
    # cluster mode promotes SKIP to non-zero exit with the AIDPF-2047
    # framing.
    if dispatch_mode == "cluster" and bundle is not None and env is not None:
        aidp_rest = next(
            (r for r in results if r.name == "aidp-rest"), None
        )
        if aidp_rest is not None and aidp_rest.status == "SKIP":
            console.print(
                f"[red]AIDPF-2047 (reason=aidp_rest_probe_failed): "
                f"aidp-rest probe SKIPPED — {aidp_rest.detail}. "
                f"Cluster dispatch requires the OCI SDK; install it or "
                f"pass --dispatch-mode=local.[/red]"
            )
            return 1

    # ----- Variation resolution -----
    if bundle is None or bundle.content_pack is None:
        # v1 bundle — phase 1 result is the only signal.
        return 0

    # Resolve the cluster-dispatch config when entering cluster mode.
    resolved_dispatch_config = None
    if dispatch_mode == "cluster" and bundle is not None and env is not None:
        resolved_dispatch_config = _resolve_cluster_dispatch_config(
            env,
            config.defaults if config is not None else None,
            cluster_key_override=cluster_key_override,
            cluster_name_override=cluster_name_override,
            workspace_dir_override=workspace_dir_override,
        )
        # ---- AIDPF-2047 (missing_config) gate ----
        missing = resolved_dispatch_config.missing_fields()
        if missing:
            console.print(
                f"[red]AIDPF-2047 (reason=missing_config): cluster-mode "
                f"bootstrap requires {missing!r}. Set the field(s) in the "
                f"active environment under "
                f"aidp.config.yaml::environments.{env_name!r} (or Defaults "
                f"for workspaceDir), or pass --cluster-key= / "
                f"--cluster-name= / --workspace-dir=.[/red]"
            )
            return 1

    if repin_coa_from_metadata and not resolve_coa_from_metadata:
        console.print(
            "[red]AIDPF-2047 (reason=conflicting_flags): "
            "--repin-coa-from-metadata refines --resolve-coa-from-metadata "
            "and cannot be used without it — pass both.[/red]"
        )
        return 1

    options = VariationPhaseOptions(
        refresh=refresh,
        operator=operator,
        non_interactive=non_interactive,
        accept_coa_convention=accept_coa_convention,
        accept_singleton_coa=accept_singleton_coa,
        resolve_coa_from_metadata=resolve_coa_from_metadata,
        repin_coa_from_metadata=repin_coa_from_metadata,
        resolutions_path=resolutions_path,
        spark_session=spark_session,
        dispatch_mode=dispatch_mode,
        dispatch_config=resolved_dispatch_config,
        env=env if dispatch_mode == "cluster" else None,
    )
    try:
        outcome: VariationPhaseOutcome = run_variation_phase(
            bundle, bundle_path, options=options, console=console
        )
    except RefreshRequiresConfirmation as exc:
        console.print(f"[red]{exc}[/red]")
        return 1

    return outcome.exit_code


def _load(
    bundle_path: Path, config_path: Path, results: list[_ProbeResult]
) -> tuple[Bundle | None, AidpConfig | None]:
    bundle = None
    config = None
    try:
        if bundle_path.exists():
            raw = render_tree(yaml.safe_load(bundle_path.read_text(encoding="utf-8")))
            bundle = Bundle.model_validate(raw)
            results.append(_ProbeResult("bundle.yaml", "PASS", f"{len(bundle.datasets)} datasets"))
        else:
            results.append(_ProbeResult(
                "bundle.yaml", "FAIL", f"{bundle_path} not found",
                "run [cyan]aidp-fusion-autopilot init[/cyan]",
            ))
    except (ValidationError, yaml.YAMLError) as exc:
        results.append(_ProbeResult("bundle.yaml", "FAIL", str(exc).splitlines()[0]))
    try:
        if config_path.exists():
            raw = render_tree(yaml.safe_load(config_path.read_text(encoding="utf-8")))
            config = AidpConfig.model_validate(raw)
            results.append(_ProbeResult(
                "aidp.config.yaml", "PASS",
                f"environments: {sorted(config.environments.keys())}",
            ))
        else:
            results.append(_ProbeResult(
                "aidp.config.yaml", "FAIL", f"{config_path} not found",
                "run [cyan]aidp-fusion-autopilot init[/cyan]",
            ))
    except (ValidationError, yaml.YAMLError) as exc:
        results.append(_ProbeResult("aidp.config.yaml", "FAIL", str(exc).splitlines()[0]))
    return bundle, config


def _probe_bicc(bundle: Bundle, results: list[_ProbeResult]) -> None:
    user = os.environ.get("FUSION_BICC_USER")
    pwd = os.environ.get("FUSION_BICC_PASSWORD")
    if not (user and pwd):
        results.append(_ProbeResult(
            "bicc-auth", "SKIP",
            "FUSION_BICC_USER / FUSION_BICC_PASSWORD env vars not set",
            "export FUSION_BICC_USER + FUSION_BICC_PASSWORD before running bootstrap",
        ))
        return

    if _is_aidp_secret_ref(pwd):
        results.append(_ProbeResult(
            "bicc-auth", "SKIP",
            "FUSION_BICC_PASSWORD points to an AIDP credential-store secret",
            "set a local plaintext FUSION_BICC_PASSWORD only if you want the laptop-side BICC auth probe",
        ))
        return

    pod_url = bundle.fusion.service_url.rstrip("/")
    if "$" in pod_url:
        results.append(_ProbeResult(
            "bicc-auth", "SKIP",
            f"unresolved variable in fusion.serviceUrl: {pod_url}",
            "set FUSION_BICC_BASE_URL or substitute the value directly in bundle.yaml",
        ))
        return

    url = pod_url + "/biacm/rest/meta/datastores"
    try:
        response = requests.get(url, auth=(user, pwd), timeout=120)
    except requests.RequestException as exc:
        results.append(_ProbeResult("bicc-auth", "FAIL", f"network error: {exc}"))
        return

    if response.status_code != 200:
        results.append(_ProbeResult(
            "bicc-auth", "FAIL",
            f"HTTP {response.status_code} from /biacm/rest/meta/datastores",
            "verify user has BIAdmin role; check pod URL",
        ))
        return

    live = _extract_datastore_names(response.json())
    results.append(_ProbeResult(
        "bicc-auth", "PASS", f"{len(live)} datastores visible",
    ))

    missing = [
        ds.id for ds in bundle.datasets
        if ds.id in CATALOG and CATALOG[ds.id].datastore not in live
    ]
    if missing:
        results.append(_ProbeResult(
            "bicc-catalog-reconcile", "FAIL",
            f"datasets with no matching datastore on this pod: {missing}",
            "run [cyan]aidp-fusion-autopilot catalog probe --pod " + pod_url + "[/cyan] to inspect",
        ))
    else:
        results.append(_ProbeResult(
            "bicc-catalog-reconcile", "PASS",
            f"all {len([d for d in bundle.datasets if d.id in CATALOG])} datasets reconcile",
        ))


def _is_aidp_secret_ref(value: str) -> bool:
    return value.startswith("${aidp:secret:") and value.endswith("}")


def _probe_aidp(env, results: list[_ProbeResult]) -> None:
    # Reuse AidpRestClient as the single source of truth for the AIDP REST
    # host and path construction.
    try:
        from ..dispatch.rest_client import AidpRestClient, AidpRestError
    except ImportError as exc:
        results.append(_ProbeResult(
            "aidp-rest", "SKIP",
            f"AidpRestClient not importable: {exc}",
            "pip install oci",
        ))
        return

    workspace_key = env.workspace_key
    aidp_id = getattr(env, "ai_data_platform_id", None)
    if not aidp_id:
        results.append(_ProbeResult(
            "aidp-rest", "FAIL",
            "aiDataPlatformId is not set on the environment",
            "set `aiDataPlatformId:` for this environment in aidp.config.yaml",
        ))
        return

    region = env.region or "us-ashburn-1"
    oci_profile = env.oci_profile or "DEFAULT"
    try:
        client = AidpRestClient(
            region=region,
            aidp_id=aidp_id,
            workspace_key=workspace_key,
            oci_profile=oci_profile,
        )
        workspaces = client.list_workspaces()
    except AidpRestError as exc:
        results.append(_ProbeResult("aidp-rest", "FAIL", f"AIDP REST error: {exc}"))
        return
    except Exception as exc:  # signer construction, oci config, network
        results.append(_ProbeResult("aidp-rest", "FAIL", f"client init/request error: {exc}"))
        return

    if any(ws.key == workspace_key for ws in workspaces):
        results.append(_ProbeResult(
            "aidp-rest", "PASS",
            f"workspace {workspace_key} reachable in {region}",
        ))
    else:
        visible = ", ".join(ws.key for ws in workspaces[:5]) or "<none>"
        results.append(_ProbeResult(
            "aidp-rest", "FAIL",
            f"workspace {workspace_key!r} not found in aiDataPlatform "
            f"{aidp_id!r}; visible: {visible}",
            "verify workspaceKey + aiDataPlatformId in aidp.config.yaml",
        ))


def _extract_datastore_names(body) -> set[str]:
    names: set[str] = set()

    def visit(node):
        if isinstance(node, dict):
            for key in ("name", "datastoreName", "viewObjectName", "dataStoreName"):
                val = node.get(key)
                if isinstance(val, str):
                    names.add(val)
            for key in ("dataStores", "datastores"):
                val = node.get(key)
                if isinstance(val, list):
                    for item in val:
                        if isinstance(item, str):
                            names.add(item)
            for v in node.values():
                visit(v)
        elif isinstance(node, list):
            for item in node:
                visit(item)

    visit(body)
    return names


def _render(results: list[_ProbeResult], console: Console) -> None:
    table = Table(title="bootstrap probes")
    table.add_column("probe", style="cyan", no_wrap=True)
    table.add_column("status")
    table.add_column("detail", overflow="fold")
    for r in results:
        color = {"PASS": "green", "FAIL": "red", "SKIP": "yellow"}[r.status]
        table.add_row(r.name, f"[{color}]{r.status}[/{color}]", r.detail)
    console.print(table)
    failures = [r for r in results if r.status == "FAIL"]
    if failures:
        console.print("\n[bold red]Remediation steps:[/bold red]")
        for r in failures:
            if r.remediation:
                console.print(f"  - [cyan]{r.name}[/cyan]: {r.remediation}")


__all__ = ["bootstrap"]
