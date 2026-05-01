"""Implementation of ``aidp-fusion-bundle bootstrap``.

Probes live prerequisites against the Fusion pod + AIDP workspace listed
in ``aidp.config.yaml``. Each probe is independent and reports
PASS / FAIL / SKIP with the exact remediation step. Overall exit code:
0 if every probe passed (or was intentionally skipped), 1 otherwise.

Probes performed:
  1. ``bundle.yaml + aidp.config.yaml`` schema validate
  2. ``GET <pod>/biacm/rest/meta/datastores`` reachable + auth works
  3. Catalog reconciliation (every dataset id resolves to a live datastore)
  4. AIDP REST API reachable using the current OCI session token
  5. (Optional, ``--check-iam``) AIDP RP IAM policies cover the workspace
"""

from __future__ import annotations

import os
from pathlib import Path

import requests
import yaml
from pydantic import ValidationError
from rich.console import Console
from rich.table import Table

from ..schema.bundle import AidpConfig, Bundle
from ..schema.fusion_catalog import CATALOG


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


def bootstrap(
    bundle_path: Path,
    config_path: Path,
    env_name: str,
    *,
    check_iam: bool = False,
    console: Console | None = None,
) -> int:
    """Probe all prereqs; return process exit code (0 if all PASS, 1 otherwise)."""
    console = console or Console()
    results: list[_ProbeResult] = []

    bundle, config = _load(bundle_path, config_path, results)
    if bundle and config:
        env = config.environments.get(env_name)
        if env is None:
            results.append(_ProbeResult(
                "env-lookup", "FAIL",
                f"env '{env_name}' not in aidp.config.yaml",
                f"add '{env_name}:' under environments: in aidp.config.yaml",
            ))
        else:
            _probe_bicc(bundle, results)
            _probe_aidp(env, results)
            if check_iam:
                results.append(_ProbeResult(
                    "iam-policy", "SKIP",
                    "policy probe requires AIDP RP credentials and is not auto-discoverable",
                    "verify manually that AIDP RP can read the BICC External Storage bucket",
                ))

    _render(results, console)
    return 0 if all(r.status == "PASS" or r.status == "SKIP" for r in results) else 1


def _load(
    bundle_path: Path, config_path: Path, results: list[_ProbeResult]
) -> tuple[Bundle | None, AidpConfig | None]:
    bundle = None
    config = None
    try:
        if bundle_path.exists():
            bundle = Bundle.model_validate(yaml.safe_load(bundle_path.read_text(encoding="utf-8")))
            results.append(_ProbeResult("bundle.yaml", "PASS", f"{len(bundle.datasets)} datasets"))
        else:
            results.append(_ProbeResult(
                "bundle.yaml", "FAIL", f"{bundle_path} not found",
                "run [cyan]aidp-fusion-bundle init[/cyan]",
            ))
    except (ValidationError, yaml.YAMLError) as exc:
        results.append(_ProbeResult("bundle.yaml", "FAIL", str(exc).splitlines()[0]))
    try:
        if config_path.exists():
            config = AidpConfig.model_validate(yaml.safe_load(config_path.read_text(encoding="utf-8")))
            results.append(_ProbeResult(
                "aidp.config.yaml", "PASS",
                f"environments: {sorted(config.environments.keys())}",
            ))
        else:
            results.append(_ProbeResult(
                "aidp.config.yaml", "FAIL", f"{config_path} not found",
                "run [cyan]aidp-fusion-bundle init[/cyan]",
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
        response = requests.get(url, auth=(user, pwd), timeout=30)
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
            "run [cyan]aidp-fusion-bundle catalog probe --pod " + pod_url + "[/cyan] to inspect",
        ))
    else:
        results.append(_ProbeResult(
            "bicc-catalog-reconcile", "PASS",
            f"all {len([d for d in bundle.datasets if d.id in CATALOG])} datasets reconcile",
        ))


def _probe_aidp(env, results: list[_ProbeResult]) -> None:
    try:
        import oci  # type: ignore[import-not-found]
    except ImportError:
        results.append(_ProbeResult(
            "aidp-rest", "SKIP",
            "oci SDK not importable",
            "pip install oci",
        ))
        return

    profile = env.oci_profile or "DEFAULT"
    try:
        config = oci.config.from_file(profile_name=profile)
    except Exception as exc:
        results.append(_ProbeResult(
            "aidp-rest", "FAIL", f"oci profile '{profile}': {exc}",
            "oci session authenticate --profile " + profile,
        ))
        return

    region = env.region or config.get("region", "us-ashburn-1")
    workspace_key = env.workspace_key
    api_base = f"https://{workspace_key}.aidataplatform.{region}.oci.oraclecloud.com"
    try:
        signer = oci.signer.Signer(
            tenancy=config["tenancy"],
            user=config["user"],
            fingerprint=config["fingerprint"],
            private_key_file_location=config.get("key_file"),
            pass_phrase=config.get("pass_phrase"),
        )
        response = requests.get(
            f"{api_base}/api/v1/workspaces/{workspace_key}",
            auth=signer,
            timeout=30,
        )
    except Exception as exc:
        results.append(_ProbeResult("aidp-rest", "FAIL", f"signer/request error: {exc}"))
        return

    if response.status_code == 200:
        results.append(_ProbeResult(
            "aidp-rest", "PASS",
            f"workspace {workspace_key} reachable in {region}",
        ))
    else:
        results.append(_ProbeResult(
            "aidp-rest", "FAIL",
            f"HTTP {response.status_code} from /api/v1/workspaces/{workspace_key}",
            "verify workspaceKey + dataLakeOcid in aidp.config.yaml",
        ))


def _extract_datastore_names(body) -> set[str]:
    names: set[str] = set()

    def visit(node):
        if isinstance(node, dict):
            for key in ("name", "datastoreName", "viewObjectName", "dataStoreName"):
                val = node.get(key)
                if isinstance(val, str):
                    names.add(val)
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
