"""``bronze diagnose-encode`` — offline culprit diagnosis for AIDPF-2093.

Bisects a PVO's columns against a cache-forcing probe to name exactly which
column(s) carry connector value/declared-type mismatches, probes each
culprit's RUNTIME type with a patched-read ladder, and prints the
ready-to-paste ``schemaPatches`` entry. The procedure is the live one that
isolated ``ItemBasePEOMaterialCost`` on ItemExtractPVO (19 probes over 400
columns, 2026-08-06).

Pure cores here (``bisect_encode_culprits`` / ``probe_runtime_type`` over
an injected probe callable) are unit-tested with fakes; the CLI wires them
into a dispatched notebook that installs the plugin wheel (single source —
the cell imports THIS module) and reports through the standard base64
marker envelope.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Sequence

#: Patched-read type ladder for probing a culprit's runtime type. Order is
#: live-informed: Long is the observed Fusion case; double catches float
#: payloads; string is the last resort that accepts only string values.
RUNTIME_TYPE_LADDER: tuple[str, ...] = ("bigint", "double", "string")

DEFAULT_MAX_PROBES = 30


@dataclass(frozen=True)
class EncodeDiagnosis:
    """Outcome of a bisection run."""

    culprits: tuple[str, ...]
    runtime_types: dict[str, str | None]
    """culprit -> probed runtime type (``None`` = no uniform type found —
    vendor-connector-fix territory, never force a patch)."""
    probes_used: int
    exhausted: bool
    """True when ``max_probes`` ran out with unexplored column groups —
    the culprit list may be incomplete."""

    @property
    def suggestion_yaml(self) -> str:
        """Ready-to-paste bundle snippet for the resolvable culprits."""
        resolvable = {
            c: t for c, t in self.runtime_types.items() if t is not None
        }
        if not resolvable:
            return ""
        lines = ["    schemaPatches:"]
        for col in sorted(resolvable):
            lines.append(f"      {col}: {resolvable[col]}")
        return "\n".join(lines)


def bisect_encode_culprits(
    probe: Callable[[Sequence[str]], bool],
    columns: Sequence[str],
    *,
    max_probes: int = DEFAULT_MAX_PROBES,
) -> tuple[list[str], int, bool]:
    """Deterministic bisection: ``probe(cols) -> True`` = the projection
    encodes cleanly. Returns ``(culprits, probes_used, exhausted)``.

    LIFO over halves of failing groups — the live procedure. A passing
    group is cleared wholesale; a failing singleton is a culprit. Bounded
    by ``max_probes``; leftover unexplored groups set ``exhausted``.
    """
    culprits: list[str] = []
    stack: list[list[str]] = [list(columns)]
    probes_used = 0
    while stack and probes_used < max_probes:
        group = stack.pop()
        if not group:
            continue
        probes_used += 1
        if probe(group):
            continue
        if len(group) == 1:
            culprits.append(group[0])
            continue
        mid = len(group) // 2
        stack.append(group[:mid])
        stack.append(group[mid:])
    exhausted = bool(stack) and probes_used >= max_probes
    return culprits, probes_used, exhausted


def probe_runtime_type(
    patched_probe: Callable[[str, str], bool],
    column: str,
    *,
    ladder: Sequence[str] = RUNTIME_TYPE_LADDER,
) -> str | None:
    """Probe a culprit's runtime type: ``patched_probe(column, type)`` =
    read ONLY that column with the type patched in, cache+count. First
    ladder type that encodes cleanly wins; ``None`` = no uniform type
    (mixed runtime values) — the honest vendor-ticket outcome."""
    for candidate in ladder:
        if patched_probe(column, candidate):
            return candidate
    return None


def diagnose(
    probe: Callable[[Sequence[str]], bool],
    patched_probe: Callable[[str, str], bool],
    columns: Sequence[str],
    *,
    max_probes: int = DEFAULT_MAX_PROBES,
) -> EncodeDiagnosis:
    """Full diagnosis: full-width sanity probe, bisection, type ladder."""
    probes_used = 0
    if probe(list(columns)):
        return EncodeDiagnosis(
            culprits=(), runtime_types={}, probes_used=1, exhausted=False
        )
    probes_used += 1
    remaining = max(1, max_probes - probes_used)
    culprits, used, exhausted = bisect_encode_culprits(
        probe, columns, max_probes=remaining
    )
    probes_used += used
    runtime_types: dict[str, str | None] = {}
    for culprit in culprits:
        runtime_types[culprit] = probe_runtime_type(patched_probe, culprit)
    return EncodeDiagnosis(
        culprits=tuple(culprits),
        runtime_types=runtime_types,
        probes_used=probes_used,
        exhausted=exhausted,
    )


__all__ = [
    "DEFAULT_MAX_PROBES",
    "EncodeDiagnosis",
    "RUNTIME_TYPE_LADDER",
    "bisect_encode_culprits",
    "diagnose",
    "probe_runtime_type",
]


# ---------------------------------------------------------------------------
# CLI implementation — `bronze diagnose-encode`
# ---------------------------------------------------------------------------


_MARKER_BEGIN = "AIDPF_DIAG_ENCODE_BEGIN"
_MARKER_END = "AIDPF_DIAG_ENCODE_END"


def _build_diag_cell(
    *,
    service_url: str,
    username: str,
    external_storage: str,
    offering_schema: str,
    datastore: str,
    bicc_secret_name: str,
    bicc_secret_key: str,
    max_probes: int,
) -> str:
    """The cluster diagnosis cell. Connection literals are the same
    NON-SECRET values every run notebook embeds; the password comes from
    the AIDP credential store in-cell (never embedded, never printed).
    Imports the pure cores from the installed wheel — single source."""
    return f'''import base64 as _b64, json as _json

from oracle_ai_data_platform_fusion_autopilot.commands.bronze_diagnose import diagnose
from oracle_ai_data_platform_fusion_autopilot.orchestrator.builtins.bronze_extract_adapter import (
    _spark_type_for_patch,
)

_PW = aidputils.secrets.get(name={bicc_secret_name!r}, key={bicc_secret_key!r})  # noqa: F821
assert _PW, "AIDP credential store returned an empty BICC password"
_BASE = {{
    "type": "FUSION_BICC",
    "fusion.service.url": {service_url!r},
    "user.name": {username!r},
    "password": _PW,
    "schema": {offering_schema!r},
    "fusion.external.storage": {external_storage!r},
    "datastore": {datastore!r},
}}

def _load(user_schema=None):
    reader = spark.read.format("aidataplatform").options(**_BASE)  # noqa: F821
    if user_schema is not None:
        reader = reader.schema(user_schema)
    return reader.load()

_base_schema = _load().schema

def _probe(cols):
    df = None
    try:
        df = _load().select(*list(cols))
        df.cache()
        df.count()
        return True
    except Exception:
        return False
    finally:
        if df is not None:
            try:
                df.unpersist()
            except Exception:
                pass

def _patched_probe(column, patch_type):
    from pyspark.sql.types import StructField, StructType
    patched = StructType([
        (
            StructField(f.name, _spark_type_for_patch(patch_type), f.nullable,
                        dict(f.metadata or {{}}))
            if f.name == column
            else f
        )
        for f in _base_schema.fields
    ])
    df = None
    try:
        df = _load(user_schema=patched).select(column)
        df.cache()
        df.count()
        return True
    except Exception:
        return False
    finally:
        if df is not None:
            try:
                df.unpersist()
            except Exception:
                pass

_d = diagnose(
    _probe, _patched_probe, [f.name for f in _base_schema.fields],
    max_probes={max_probes},
)
_payload = {{
    "culprits": list(_d.culprits),
    "runtimeTypes": dict(_d.runtime_types),
    "probesUsed": _d.probes_used,
    "exhausted": _d.exhausted,
    "suggestionYaml": _d.suggestion_yaml,
    "totalColumns": len(_base_schema.fields),
}}
print("{_MARKER_BEGIN}")
print(_b64.b64encode(_json.dumps(_payload).encode("utf-8")).decode("ascii"))
print("{_MARKER_END}")
'''


def run_diagnose_encode(
    bundle_path,
    config_path,
    env_name: str,
    *,
    dataset: str,
    max_probes: int = DEFAULT_MAX_PROBES,
    poll_timeout_s: int = 3600,
    console=None,
) -> int:
    """Dispatch the diagnosis notebook and print the schemaPatches
    suggestion. Exit codes: 0 = diagnosis complete (culprits or clean);
    1 = incomplete (probe budget exhausted) or an unresolvable culprit
    (no uniform runtime type — vendor-ticket territory); 2 = dispatch or
    configuration failure."""
    import json
    import os
    import time
    import uuid
    from pathlib import Path

    from rich.console import Console

    from ..dispatch.notebook_builder import _build_install_cell
    from ..dispatch.rest_client import AidpRestClient
    from ..dispatch.wheel_builder import build_wheel
    from ..orchestrator.builtins.bronze_extract_adapter import (
        _resolve_effective_schema,
    )
    from ..orchestrator.content_pack import (
        load_full_chain,
        make_filesystem_base_resolver,
    )
    from ..schema.bundle import load_bundle, resolve_content_pack_root
    from ._config_helpers import env_or_error, load_aidp_config
    from .cluster_bootstrap_probe import _find_plugin_checkout

    console = console or Console()
    bundle_path = Path(bundle_path)
    try:
        bundle, _paths = load_bundle(bundle_path)
        config = load_aidp_config(Path(config_path))
        env = env_or_error(config, env_name)
        pack_root = resolve_content_pack_root(bundle_path, bundle.content_pack)
        pack = load_full_chain(
            pack_root, base_resolver=make_filesystem_base_resolver(pack_root)
        )
        node = pack.bronze.get(dataset)
        if node is None or node.implementation.type != "bronze_extract":
            console.print(
                f"[red]bronze diagnose-encode: {dataset!r} is not a "
                f"bronze_extract node of the resolved pack. Bronze nodes: "
                f"{sorted(pack.bronze)}[/red]"
            )
            return 2
        wheel_path = build_wheel(
            plugin_checkout=_find_plugin_checkout(bundle_path),
            cache_dir=bundle_path.parent / ".aidp" / "wheel-cache",
            log=lambda msg: console.print(f"[dim][wheel] {msg}[/dim]"),
        )
        client = AidpRestClient(
            region=env.region or config.defaults.region,
            aidp_id=env.ai_data_platform_id or "",
            workspace_key=env.workspace_key,
            oci_profile=env.oci_profile or "DEFAULT",
            log=lambda stage, **kw: console.print(
                f"[dim][rest] {stage} "
                + " ".join(f"{k}={v}" for k, v in kw.items())
                + "[/dim]"
            ),
        )
        diag_cell = _build_diag_cell(
            service_url=bundle.fusion.service_url,
            username=bundle.fusion.username,
            external_storage=bundle.fusion.external_storage,
            offering_schema=_resolve_effective_schema(node, bundle),
            datastore=node.implementation.datastore,
            bicc_secret_name=getattr(
                env, "bicc_secret_name", None
            ) or "fusion_bicc_password",
            bicc_secret_key=getattr(env, "bicc_secret_key", None) or "password",
            max_probes=max_probes,
        )
        nb = {
            "nbformat": 4,
            "nbformat_minor": 5,
            "metadata": {"language_info": {"name": "python"}},
            "cells": [
                {
                    "cell_type": "code",
                    "source": src,
                    "metadata": {},
                    "outputs": [],
                    "execution_count": None,
                }
                for src in (_build_install_cell(wheel_path), diag_cell)
            ],
        }
        stamp = f"{int(time.time())}_{uuid.uuid4().hex[:6]}"
        nb_path = (
            f"Workspace/Shared/aidp-fusion-autopilot-{bundle.project}"
            f"/diagnose-encode-{dataset}.ipynb"
        )
        client.upload_notebook(nb_path, nb)
        job_key = client.create_notebook_job(
            name=f"aidpf_diag_encode_{dataset}_{stamp}",
            description=(
                f"bronze diagnose-encode for {dataset} (AIDPF-2093 bisection)"
            ),
            notebook_path="/" + nb_path,
            cluster_key=env.cluster_key,
            cluster_name=env.cluster_name,
            task_key="diagnose",
        )
        run_key = client.submit_run(job_key)
        result = client.poll_run(
            run_key, timeout_s=poll_timeout_s, interval_s=20
        )
        if result.status != "SUCCESS":
            console.print(
                f"[red]diagnosis job reached {result.status!r} — see the "
                f"AIDP console (jobRunKey={run_key}).[/red]"
            )
            return 2
        run = client.get_run(run_key)
        task_run_key = list((run.get("taskToTaskRunMap") or {}).values())[0]
        executed = json.loads(client.fetch_output(task_run_key, timeout=120))
        payload = client.parse_marker(
            executed, begin=_MARKER_BEGIN, end=_MARKER_END, decode_base64=True
        )
        if payload is None:
            console.print(
                "[red]diagnosis notebook produced no marker — open the "
                "executed notebook in the AIDP console.[/red]"
            )
            return 2
    except Exception as exc:  # noqa: BLE001 — CLI boundary
        console.print(f"[red]bronze diagnose-encode failed: {exc}[/red]")
        return 2

    culprits = payload.get("culprits") or []
    runtime_types = payload.get("runtimeTypes") or {}
    exhausted = bool(payload.get("exhausted"))
    console.print(
        f"[bold]diagnose-encode {dataset}[/bold]: "
        f"{payload.get('totalColumns')} columns, "
        f"{payload.get('probesUsed')} probes"
    )
    if not culprits and not exhausted:
        console.print(
            "[green]no encode culprits — the full-width read encodes "
            "cleanly; no schemaPatches needed.[/green]"
        )
        return 0
    for col in culprits:
        rt = runtime_types.get(col)
        if rt:
            console.print(
                f"[yellow]culprit: {col} — runtime type {rt!r} "
                f"(declared type mismatched by the connector)[/yellow]"
            )
        else:
            console.print(
                f"[red]culprit: {col} — NO uniform runtime type (mixed "
                f"values); a schemaPatches entry cannot fix this column — "
                f"file the connector defect (AIDPF-2093).[/red]"
            )
    suggestion = payload.get("suggestionYaml") or ""
    if suggestion:
        console.print(
            "\n[bold]Add to the bundle dataset entry:[/bold]\n" + suggestion
        )
    if exhausted:
        console.print(
            f"[red]probe budget exhausted before full coverage — culprit "
            f"list may be incomplete; re-run with a higher "
            f"--max-probes.[/red]"
        )
        return 1
    if any(runtime_types.get(c) is None for c in culprits):
        return 1
    return 0
