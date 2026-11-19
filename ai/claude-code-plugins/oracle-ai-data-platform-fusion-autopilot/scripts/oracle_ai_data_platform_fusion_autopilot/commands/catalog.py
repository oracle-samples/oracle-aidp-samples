"""Implementation of ``aidp-fusion-autopilot catalog list`` and ``catalog probe``.

* ``list`` — pretty-print the bundle's curated PVO catalog.
* ``probe`` — hit the live BICC ``/biacm/rest/meta/datastores`` endpoint
  and reconcile each catalog entry against what the customer's pod
  actually exposes.
"""

from __future__ import annotations

from typing import Any

import requests
from rich.console import Console
from rich.table import Table

from ..config.paths import DEFAULT_PATHS
from ..schema.fusion_catalog import CATALOG, PvoEntry, PvoKind

# Display markers for the three PvoKind values. Should-fix-4 (2026-05-17):
# SAAS_BATCH distinct from ExtractPVO + OTBI so operators see at a glance
# which entries are eligible for a content-pack bronze node vs which are
# deferred (no extractor yet) or refused outright (OTBI).
_KIND_MARKERS: dict[PvoKind, str] = {
    PvoKind.EXTRACT_PVO: "ExtractPVO",
    PvoKind.OTBI:        "[red]OTBI[/red]",
    PvoKind.SAAS_BATCH:  "[yellow]SaasBatch[/yellow]",
}


def list_catalog(*, console: Console | None = None) -> int:
    """Pretty-print the catalog as a table."""
    console = console or Console()
    table = Table(title=f"PVO catalog ({len(CATALOG)} entries)", show_lines=False)
    table.add_column("id", no_wrap=True, style="cyan")
    table.add_column("datastore", overflow="fold")
    table.add_column("schema")
    table.add_column("bronze table", style="green")
    table.add_column("kind")
    table.add_column("✓", justify="center")

    for entry in sorted(CATALOG.values(), key=lambda e: (e.schema, e.id)):
        kind_marker = _KIND_MARKERS[entry.kind]
        confirmed = "[green]ok[/green]" if entry.confirmed else "[yellow]?[/yellow]"
        # Render the default-tenant 3-part name so the display matches what
        # most customers see; tenants with overridden aidp.catalog /
        # bronzeSchema get a different prefix, but the bare name is the same.
        table.add_row(
            entry.id,
            entry.datastore,
            entry.schema,
            DEFAULT_PATHS.bronze(entry.bronze_table_name),
            kind_marker,
            confirmed,
        )
    console.print(table)
    confirmed_count = sum(1 for e in CATALOG.values() if e.confirmed)
    console.print(
        f"\n[green]{confirmed_count}[/green] verbatim-from-Oracle, "
        f"[yellow]{len(CATALOG) - confirmed_count}[/yellow] need live verification "
        f"([dim]run [/dim][cyan]catalog probe --pod <url>[/cyan][dim] to reconcile[/dim])"
    )
    return 0


def probe_catalog(
    pod: str,
    *,
    username: str | None = None,
    password: str | None = None,
    console: Console | None = None,
) -> int:
    """Hit ``GET {pod}/biacm/rest/meta/datastores`` and reconcile against CATALOG.

    Args:
        pod: Fusion pod URL (e.g. ``https://my-pod.fa.<region>.oraclecloud.com``).
        username: HTTP Basic username (BIAdmin role required to read datastores).
            If absent, falls back to env var ``FUSION_BICC_USER``.
        password: HTTP Basic password. Falls back to env var ``FUSION_BICC_PASSWORD``.

    Returns:
        Process exit code: 0 if every catalog entry resolves to a live datastore,
        1 if any are missing or the BICC API call failed.
    """
    import os
    console = console or Console()
    user = username or os.environ.get("FUSION_BICC_USER")
    pwd = password or os.environ.get("FUSION_BICC_PASSWORD")
    if not (user and pwd):
        console.print(
            "[red]missing creds:[/red] pass --user/--password or set "
            "FUSION_BICC_USER + FUSION_BICC_PASSWORD env vars"
        )
        return 2

    url = pod.rstrip("/") + "/biacm/rest/meta/datastores"
    console.print(f"GET [cyan]{url}[/cyan] ...")
    try:
        response = requests.get(url, auth=(user, pwd), timeout=60)
    except requests.RequestException as exc:
        console.print(f"[red]network error:[/red] {exc}")
        return 1
    if response.status_code != 200:
        console.print(
            f"[red]HTTP {response.status_code}:[/red] {response.text[:200]}"
        )
        return 1

    body = response.json()
    live_datastores = _extract_datastore_names(body)
    console.print(f"  [green]{len(live_datastores)}[/green] datastores in catalog")

    table = Table(title="Catalog reconcile", show_lines=False)
    table.add_column("id", style="cyan")
    table.add_column("datastore", overflow="fold")
    table.add_column("status")
    missing: list[PvoEntry] = []
    skipped_count = 0
    for entry in sorted(CATALOG.values(), key=lambda e: e.id):
        # Should-fix-4 (2026-05-17): skip non-EXTRACT_PVO kinds. SAAS_BATCH
        # entries (e.g. hcm_worker_assignments) use a REST endpoint, not
        # BICC's /biacm/rest/meta/datastores — probing them would always
        # surface as MISSING with a misleading "not in BICC catalog"
        # message. OTBI entries are documentation-skip per the existing
        # refuse-by-default contract.
        if entry.kind != PvoKind.EXTRACT_PVO:
            table.add_row(
                entry.id,
                entry.datastore,
                f"[dim]SKIPPED kind={entry.kind.value}[/dim]",
            )
            skipped_count += 1
            continue
        live = entry.datastore in live_datastores
        if live:
            table.add_row(entry.id, entry.datastore, "[green]LIVE[/green]")
        else:
            table.add_row(entry.id, entry.datastore, "[red]MISSING[/red]")
            missing.append(entry)
    console.print(table)

    if missing:
        console.print(
            f"\n[red]{len(missing)} catalog entries missing on this pod:[/red]"
        )
        for e in missing:
            console.print(f"  - {e.id}: {e.datastore}")
        return 1
    extract_count = len(CATALOG) - skipped_count
    console.print(
        f"\n[green]all {extract_count} EXTRACT_PVO entries reconcile against {pod}[/green]"
        + (f" ({skipped_count} non-BICC entries skipped)" if skipped_count else "")
    )
    return 0


def _extract_datastore_names(body: Any) -> set[str]:
    """Pull datastore names out of BICC's response (shape varies by release)."""
    names: set[str] = set()

    def visit(node: Any) -> None:
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


# ---------------------------------------------------------------------------
# probe-pvo --emit-pack-yaml
# ---------------------------------------------------------------------------


def _spark_type_to_yaml(spark_type: str) -> str:
    """Map a Spark DDL type string (e.g. ``StringType``, ``LongType``,
    ``DecimalType(28,8)``) to the YAML form used in pack outputSchema."""
    t = spark_type.strip()
    # Strip the `()` from `Type()` if present, then lowercase the head.
    if t.endswith("Type"):
        head = t[:-4]
        return head.lower()  # StringType → string, LongType → long, ...
    if t.endswith("Type()"):
        head = t[:-6]
        return head.lower()
    # Parameterised: `DecimalType(28,8)` → `decimal(28,8)`.
    if t.startswith("DecimalType("):
        # Pull out the parens content.
        inside = t[len("DecimalType("):-1]
        return f"decimal({inside})"
    # Already a DDL-style literal like `string` / `long` / `timestamp` / etc.
    return t.lower()


def probe_pvo_emit_pack_yaml(
    *,
    dataset_id: str,
    datastore: str,
    bicc_schema: str,
    pvo_id: str | None,
    incremental_capable: bool,
    emit_pack_yaml: "str",
    bundle_path: "str | None" = None,
    config_path: "str | None" = None,
    env_name: str = "dev",
    console: Console | None = None,
) -> int:
    """Probe a BICC PVO and emit a draft content-pack bronze YAML.

    Runs a metadata-only ``extract_pvo().schema`` roundtrip (no row pull),
    translates the discovered ``StructType`` to an ``outputSchema.columns``
    list (plus the standard audit columns), and writes a draft YAML to
    ``emit_pack_yaml``. ``refresh.incremental`` is BLOCK-COMMENTED with
    explicit ``TODO`` markers so the operator MUST review before enabling
    incremental.

    Returns process exit code (0 on success).
    """
    from pathlib import Path

    console = console or Console()

    # Load bundle (if provided) so we can resolve BICC credentials +
    # service URL the same way the orchestrator does.
    if bundle_path is None:
        console.print(
            "[red]probe-pvo requires a bundle for BICC connectivity; "
            "pass --bundle path/to/bundle.yaml[/red]"
        )
        return 2

    try:
        from pyspark.sql import SparkSession
    except ImportError:
        console.print(
            "[red]probe-pvo requires PySpark + the aidataplatform connector; "
            "run inside an AIDP notebook or a Spark-enabled environment.[/red]"
        )
        return 2

    # Build the descriptor inline (mirrors the bronze adapter's path —
    # NEVER calls fusion_catalog.get(); pack YAMLs are self-contained).
    entry = PvoEntry(
        id=dataset_id,
        datastore=datastore,
        schema=bicc_schema,
        bronze_table_name=dataset_id,
        description=f"probe-pvo draft for {dataset_id}",
        kind=PvoKind.EXTRACT_PVO,
        confirmed=False,
        incremental_capable=incremental_capable,
        natural_key="",
    )

    # Resolve bundle + credentials.
    import yaml as _yaml
    from ..schema.bundle import Bundle
    bundle_obj = Bundle.model_validate(
        _yaml.safe_load(Path(bundle_path).read_text(encoding="utf-8"))
    )
    from ..orchestrator.runtime import _resolve_password
    resolved_password = _resolve_password(bundle_obj.fusion.password)

    from ..extractors import bicc as bicc_extractor

    spark = SparkSession.builder.getOrCreate()
    df = bicc_extractor.extract_pvo(
        spark, entry,
        fusion_service_url=bundle_obj.fusion.service_url,
        username=bundle_obj.fusion.username,
        password=resolved_password.get_secret_value(),
        fusion_external_storage=bundle_obj.fusion.external_storage,
        schema=bicc_schema,
    )
    schema = df.schema  # Triggers BICC inferSchema roundtrip.

    columns_yaml_lines: list[str] = []
    for field in schema.fields:
        ttype = _spark_type_to_yaml(str(field.dataType))
        nullable = "true" if field.nullable else "false"
        col_type = f'"{ttype}"' if "," in ttype else ttype
        columns_yaml_lines.append(
            f"    - {{ name: {field.name}, type: {col_type}, "
            f"nullable: {nullable}, pii: none }}"
        )

    # Append standard audit columns.
    columns_yaml_lines.extend([
        "    - { name: _extract_ts, type: timestamp, nullable: false, pii: none }",
        "    - { name: _source_pvo, type: string, nullable: false, pii: none }",
        "    - { name: _run_id, type: string, nullable: false, pii: none }",
        "    - { name: _watermark_used, type: timestamp, nullable: true, pii: none }",
    ])

    pvo_id_line = ""
    if pvo_id is not None:
        # Only emit the pvo_id key when a value was supplied — empty
        # would trip a FALSE AIDPF-2080 ("not in catalog").
        pvo_id_line = f"  pvo_id: {pvo_id}\n"

    yaml_body = f"""id: {dataset_id}
layer: bronze
implementation:
  type: bronze_extract
  datastore: {datastore}
{pvo_id_line}  biccSchema: {bicc_schema}
  schemaOverride: null
  incrementalCapable: {str(incremental_capable).lower()}
  auditColumnsMode: bronze_v1
target: {dataset_id}
dependsOn:
  bronze: []
  silver: []
refresh:
  seed:
    strategy: replace
  # incremental:                  # TODO: uncomment after filling in below
  #   strategy: merge
  #   watermark:
  #     source: {dataset_id}
  #     column: TODO_WATERMARK_COLUMN
  #   naturalKey: [TODO_NATURAL_KEY]
requiredColumns:
  {dataset_id}: []                  # TODO: list cols downstream silver/gold reads
outputSchema:
  columns:
{chr(10).join(columns_yaml_lines)}
quality:
  tests: []
"""

    out_path = Path(emit_pack_yaml).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(yaml_body, encoding="utf-8")

    console.print(
        f"[green]Draft pack YAML emitted at {out_path}.[/green]\n"
        f"Review + fill TODOs ([cyan]incremental.naturalKey[/cyan], "
        f"[cyan]watermark.column[/cyan], [cyan]requiredColumns[/cyan], "
        f"[cyan]pii[/cyan] classifications). Commit. Then add "
        f"[cyan]{dataset_id}[/cyan] to [cyan]bundle.yaml::datasets[][/cyan] "
        f"to enable."
    )
    return 0


__all__ = ["list_catalog", "probe_catalog", "probe_pvo_emit_pack_yaml"]
