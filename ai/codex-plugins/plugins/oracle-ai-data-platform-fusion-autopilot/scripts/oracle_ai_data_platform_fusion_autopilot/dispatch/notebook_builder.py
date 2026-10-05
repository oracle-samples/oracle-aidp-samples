"""4-cell ipynb generator for REST dispatch.

Builds the notebook uploaded by the laptop dispatcher. The generated
notebook is intentionally self-contained: it installs the local wheel,
loads AIDP secrets, stages the bundle/content pack, runs the orchestrator,
and emits a machine-readable run marker.

- The run-cell emits the FULL ``RunSummary`` payload via
  ``summary.to_marker_dict()`` (NOT a hand-rolled subset dict —
  hand-rolled dicts drift from the schema and break
  ``RunSummary.from_marker_dict`` laptop-side).
- ``resume_run_id`` is threaded into the cluster-side orchestrator call
  when a run is resumed from the laptop CLI.

The notebook contract is the **only** boundary between the laptop-side
dispatcher and the cluster-side orchestrator. Adding a new orchestrator
flag means adding it to ``build_notebook``'s signature and threading it
into the run-cell template — no changes anywhere else in the dispatch
package.
"""

from __future__ import annotations

import base64
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

# Marker delimiters — must match ``schema.run_summary.RunSummary``'s
# serialization contract. The dispatch package's ``parse_marker`` looks
# for the same strings.
MARKER_BEGIN = "AIDP_LIVE_TEST_RESULT_BEGIN"
MARKER_END = "AIDP_LIVE_TEST_RESULT_END"
_SENSITIVE_ENV_KEY_PARTS = ("PASSWORD", "SECRET", "TOKEN")


def _code_cell(source: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": source.splitlines(keepends=True),
    }


def _markdown_cell(source: str) -> dict:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": [source if source.endswith("\n") else source + "\n"],
    }


def _build_install_cell(wheel_path: Path) -> str:
    wheel_b64 = base64.b64encode(wheel_path.read_bytes()).decode()
    wheel_filename = wheel_path.name
    return (
        f"import base64, subprocess, sys, tempfile, pathlib\n"
        f'WHEEL_B64 = """{wheel_b64}"""\n'
        f'_stage = pathlib.Path(tempfile.mkdtemp(prefix="aidp_fusion_autopilot_"))\n'
        f'_whl = _stage / "{wheel_filename}"\n'
        f"_whl.write_bytes(base64.b64decode(WHEEL_B64))\n"
        f'_target = _stage / "site-packages"\n'
        f"_target.mkdir()\n"
        f"res = subprocess.run(\n"
        f'    [sys.executable, "-m", "pip", "install", "--quiet", "--no-deps",\n'
        f'     "--target", str(_target), str(_whl)],\n'
        f"    capture_output=True, text=True, timeout=180,\n"
        f")\n"
        f'print(f"pip rc={{res.returncode}}")\n'
        f"if res.returncode != 0:\n"
        f'    print("STDOUT:", res.stdout[-2000:])\n'
        f'    print("STDERR:", res.stderr[-2000:])\n'
        f'    raise RuntimeError("plugin wheel install failed")\n'
        f"sys.path.insert(0, str(_target))\n"
        f'print(f"plugin installed to {{_target}}")\n'
    )


def _build_creds_cell(
    *,
    bundle_yaml: str,
    bicc_secret_name: str,
    bicc_secret_key: str,
    env_vars: Mapping[str, str] | None = None,
) -> str:
    safe_env_vars = {
        str(k): str(v)
        for k, v in sorted((env_vars or {}).items())
        if not any(part in str(k).upper() for part in _SENSITIVE_ENV_KEY_PARTS)
    }
    return (
        f"import os\n"
        f"from pathlib import Path\n"
        f"_RUNTIME_ENV = {safe_env_vars!r}\n"
        f"for _k, _v in _RUNTIME_ENV.items():\n"
        f"    os.environ[_k] = _v\n"
        f"if _RUNTIME_ENV:\n"
        f'    print("runtime env loaded: " + ", ".join(sorted(_RUNTIME_ENV)))\n'
        f'os.environ["FUSION_BICC_PASSWORD"] = aidputils.secrets.get(  # noqa: F821\n'
        f"    name={bicc_secret_name!r}, key={bicc_secret_key!r}\n"
        f")\n"
        f'assert os.environ["FUSION_BICC_PASSWORD"], (\n'
        f'    f"AIDP credential store returned empty value for "\n'
        f'    f"name={bicc_secret_name!r} key={bicc_secret_key!r}"\n'
        f")\n"
        f'print("FUSION_BICC_PASSWORD loaded")\n'
        f'BUNDLE_PATH = Path("bundle.yaml")\n'
        f"BUNDLE_PATH.write_text({bundle_yaml!r})\n"
        f"from oracle_ai_data_platform_fusion_autopilot import orchestrator\n"
        f'print("orchestrator loaded")\n'
    )


def _build_run_cell(
    *,
    mode: Literal["seed", "incremental"] | None,
    datasets: list[str] | None,
    layers: list[str] | None,
    execution_backend: str = "legacy-python",
    force_fingerprint_skip: bool = False,
    repin_plan_hash: bool = False,
    resume_run_id: str | None = None,
    # ``--strict-scope`` opts out of implicit transitive include. Emit it
    # as ``strict_scope=...`` so the cluster honors the operator's choice.
    strict_scope: bool | None = None,
) -> str:
    # For content-pack execution, the previous bootstrap cell set up
    # _resolved_pack and _tenant_profile; thread them into orchestrator.run.
    if execution_backend == "content-pack":
        # 8-space indent — sits inside `try:` block + inside `orchestrator.run(` call.
        backend_kwargs = (
            f'        execution_backend="content-pack",\n'
            f"        resolved_pack=_resolved_pack,  # noqa: F821 — bootstrap cell\n"
            f"        tenant_profile=_tenant_profile,  # noqa: F821 — bootstrap cell\n"
        )
    else:
        backend_kwargs = f'        execution_backend="legacy-python",\n'

    return (
        f"import json, time\n"
        f"from oracle_ai_data_platform_fusion_autopilot.schema.errors import (\n"
        f"    SchemaDriftDetectedError,\n"
        f")\n"
        f"_tstart = time.time()\n"
        f"try:\n"
        f"    summary = orchestrator.run(  # noqa: F821\n"
        f"        bundle_path=BUNDLE_PATH,  # noqa: F821\n"
        f"        spark=spark,  # noqa: F821\n"
        f"        mode={mode!r},\n"
        f"        datasets={datasets!r},\n"
        f"        layers={layers!r},\n"
        f"        dry_run=False,\n"
        f"        resume_run_id={resume_run_id!r},\n"
        f"        force_fingerprint_skip={force_fingerprint_skip!r},\n"
        f"        repin_plan_hash={repin_plan_hash!r},\n"
        f"        strict_scope={strict_scope!r},\n"
        f"{backend_kwargs}"
        f"    )\n"
        f"except SchemaDriftDetectedError as _drift_exc:\n"
        f"    # Emit drift marker (artifact_json carries the full\n"
        f"    # AIDPF-2012 payload so the laptop dispatcher can\n"
        f"    # reconstruct the diagnostic locally + raise\n"
        f"    # SchemaDriftDetectedError on the operator's machine).\n"
        f"    _drift_artifact_json = _drift_exc.diagnostic_path.read_text(\n"
        f"        encoding='utf-8'\n"
        f"    )\n"
        f"    _drift_payload = {{\n"
        f"        '_kind': 'schema_drift',\n"
        f"        'run_id': _drift_exc.run_id,\n"
        f"        'summary': _drift_exc.summary,\n"
        f"        'prior_fingerprint': _drift_exc.prior_fingerprint,\n"
        f"        'current_fingerprint': _drift_exc.current_fingerprint,\n"
        f"        'artifact_json': _drift_artifact_json,\n"
        f"    }}\n"
        f"    import base64 as _b64d\n"
        f"    _b64_drift = _b64d.b64encode(json.dumps(_drift_payload).encode('utf-8')).decode('ascii')\n"
        f"    print({MARKER_BEGIN!r}, _b64_drift, {MARKER_END!r})\n"
        f"    raise\n"
        f"_twall = time.time() - _tstart\n"
        f'print(f"run_id={{summary.run_id}}")\n'
        f'print(f"steps: {{summary.succeeded}} ok, {{summary.failed}} failed, "\n'
        f'      f"{{summary.skipped}} skipped, {{summary.deferred}} deferred "\n'
        f'      f"({{summary.total_duration_seconds:.1f}}s reported / {{_twall:.1f}}s wall)")\n'
        f"for step in summary.steps:\n"
        f'    _skip_tag = f" [{{step.skip_reason}}]" if step.skip_reason else ""\n'
        f'    _rc = step.row_count if step.row_count is not None else "-"\n'
        f'    _err = (\n'
        f'        f" err={{step.error_message[:80]}}"\n'
        f'        if step.error_message and step.status == "failed"\n'
        f'        else ""\n'
        f"    )\n"
        f'    print(\n'
        f'        f"  {{step.layer:6s}}  {{step.dataset_id:24s}}  "\n'
        f'        f"{{step.status:10s}}{{_skip_tag:12s}}  rows={{str(_rc):>10s}}  "\n'
        f'        f"dur={{step.duration_seconds:.2f}}s{{_err}}"\n'
        f"    )\n"
        f"# Marker emit — use to_marker_dict() so the laptop-side dispatcher\n"
        f"# can round-trip via RunSummary.from_marker_dict.\n"
        f"# base64-wrap the JSON: AIDP's Jupyter captures stdout as\n"
        f"# display_data text/plain and strips JSON-escape backslashes,\n"
        f"# corrupting any payload with quotes/reprs (e.g. a failed step's\n"
        f"# error_message) — which silently degraded the run summary to a\n"
        f"# regex-recovered run_id only. The base64 alphabet has no chars\n"
        f"# the text/plain formatter touches, so it round-trips intact.\n"
        f"_payload = summary.to_marker_dict()\n"
        f"import base64 as _b64\n"
        f"_b64_marker = _b64.b64encode(json.dumps(_payload).encode('utf-8')).decode('ascii')\n"
        f'print({MARKER_BEGIN!r}, _b64_marker, {MARKER_END!r})\n'
    )


def _encode_payload_b64(obj: Any) -> str:
    """base64(json) encoding for arbitrary dict/list payloads.

    Pure-ASCII opaque token — safe to splice into the generated
    notebook source as a Python string literal. ``sort_keys=True``
    makes the encoded form deterministic for snapshot tests;
    ``ensure_ascii=True`` guarantees no non-ASCII chars in the token.
    """
    import base64
    import json as _json
    raw = _json.dumps(obj, ensure_ascii=True, sort_keys=True).encode("utf-8")
    return base64.b64encode(raw).decode("ascii")


def _encode_text_b64(text: str) -> str:
    """base64 encoding for arbitrary text payloads (e.g. profile YAML).

    Same safety guarantees as ``_encode_payload_b64`` but for plain
    string content. No JSON wrapping — the cluster-side decoder calls
    ``base64.b64decode(...).decode('utf-8')`` and gets the original
    text back byte-for-byte.
    """
    import base64
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def _build_content_pack_bootstrap_cell(
    *,
    profile_yaml: str,
    pack_files: Mapping[str, str],
    pack_manifest: dict[str, Any],
    schema_snapshot_yaml: str | None = None,
) -> str:
    """Cell that materialises the staged pack + reconstructs ResolvedPack on the cluster.

    Emits Python source that:

    1. Imports the orchestrator helpers (load_full_chain,
       materialize_staged_pack, load_tenant_profile_from_string).
    2. Decodes the embedded base64+json payloads to get the staged
       files dict + manifest + profile YAML text.
    3. Materialises the files to a tempdir + builds the staging-aware
       base resolver.
    4. Reconstructs ResolvedPack via load_full_chain(top_root,
       base_resolver=...).
    5. Reconstructs TenantProfile via load_tenant_profile_from_string.
    6. When ``schema_snapshot_yaml`` is provided, materialises the snapshot to
       ``<BUNDLE_PATH.parent>/profiles/<profile>.schema-snapshot.yaml``
       — the same path the laptop-side bootstrap writes, resolved on
       the cluster via the shared ``resolve_snapshot_path`` helper.
       Without this step, preflight on the cluster would never find
       the snapshot and would silently degrade to empty
       ``datasetDeltas``.

    The orchestrator.run call in the run cell consumes
    ``_resolved_pack`` + ``_tenant_profile`` from this cell's namespace.
    """
    pack_files_b64 = _encode_payload_b64(dict(pack_files))
    pack_manifest_b64 = _encode_payload_b64(pack_manifest)
    profile_yaml_b64 = _encode_text_b64(profile_yaml)

    if schema_snapshot_yaml is None:
        snapshot_stage = ""
    else:
        snapshot_yaml_b64 = _encode_text_b64(schema_snapshot_yaml)
        # Key the cluster-side snapshot path by `bundle.contentPack.profile`
        # — the SAME key bootstrap writes under on the laptop. NOT
        # `_tenant_profile.tenant`: a pre-3d profile YAML may carry a
        # hand-authored `tenant:` field that differs from the active
        # profile name; using the YAML field as the path key would
        # write to (and later read from) the wrong file.
        snapshot_stage = (
            f"from oracle_ai_data_platform_fusion_autopilot.schema.bronze_schema_snapshot import resolve_snapshot_path\n"
            f"from oracle_ai_data_platform_fusion_autopilot.schema.bundle import load_bundle as _load_bundle_for_snapshot\n"
            f"_SCHEMA_SNAPSHOT_YAML = _b64.b64decode({snapshot_yaml_b64!r}).decode('utf-8')\n"
            f"_bundle_for_snapshot, _ = _load_bundle_for_snapshot(BUNDLE_PATH)  # noqa: F821\n"
            f"_snapshot_profile_name = (\n"
            f"    _bundle_for_snapshot.content_pack.profile\n"
            f"    or _bundle_for_snapshot.content_pack.name\n"
            f")\n"
            f"_snapshot_path = resolve_snapshot_path(BUNDLE_PATH, _snapshot_profile_name)  # noqa: F821\n"
            f"_snapshot_path.parent.mkdir(parents=True, exist_ok=True)\n"
            f"_snapshot_path.write_text(_SCHEMA_SNAPSHOT_YAML, encoding='utf-8')\n"
            f'print(f"schema snapshot staged at {{_snapshot_path}}")\n'
        )

    return (
        f"import base64 as _b64\n"
        f"import json as _json\n"
        f"from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_staging import materialize_staged_pack\n"
        f"from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_full_chain\n"
        f"from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import load_tenant_profile_from_string\n"
        f"_PACK_FILES = _json.loads(_b64.b64decode({pack_files_b64!r}).decode('utf-8'))\n"
        f"_PACK_MANIFEST = _json.loads(_b64.b64decode({pack_manifest_b64!r}).decode('utf-8'))\n"
        f"_PROFILE_YAML = _b64.b64decode({profile_yaml_b64!r}).decode('utf-8')\n"
        f"_top_overlay_root, _base_resolver = materialize_staged_pack(_PACK_FILES, _PACK_MANIFEST)\n"
        f"_resolved_pack = load_full_chain(_top_overlay_root, base_resolver=_base_resolver)\n"
        f"_tenant_profile = load_tenant_profile_from_string(_PROFILE_YAML)\n"
        f"{snapshot_stage}"
        f'print(f"content-pack bootstrap: pack={{_resolved_pack.pack.id}}@{{_resolved_pack.pack.version}} tenant={{_tenant_profile.tenant}}")\n'
    )


def _build_verify_cell() -> str:
    return (
        "from oracle_ai_data_platform_fusion_autopilot.schema.bundle import load_bundle\n"
        "_bundle, _paths = load_bundle(BUNDLE_PATH)  # noqa: F821\n"
        '_state_table = _paths.bronze("fusion_autopilot_state")\n'
        "spark.sql(  # noqa: F821\n"
        '    f"""SELECT dataset_id, layer, mode, status, row_count, '
        "skip_reason, duration_seconds FROM (SELECT *, ROW_NUMBER() OVER "
        "(PARTITION BY dataset_id ORDER BY last_run_at DESC) AS rn FROM "
        "{_state_table} WHERE run_id = '{summary.run_id}') t WHERE rn=1 "
        'ORDER BY layer, dataset_id"""\n'
        ").show(200, truncate=False)\n"
        'for _layer in ("silver", "gold"):\n'
        '    _rc_col = f"{_layer}_run_id"\n'
        "    _candidate = next(\n"
        "        (s for s in summary.steps  # noqa: F821\n"
        '         if s.layer == _layer and s.status == "success"),\n'
        "        None,\n"
        "    )\n"
        "    if _candidate is None:\n"
        '        print(f"  (no successful {_layer} rows)")\n'
        "        continue\n"
        "    _table = (\n"
        "        _paths.silver(_candidate.dataset_id)\n"
        '        if _layer == "silver"\n'
        "        else _paths.gold(_candidate.dataset_id)\n"
        "    )\n"
        "    _n = spark.sql(  # noqa: F821\n"
        '        f"SELECT COUNT(*) AS n FROM {_table} WHERE {_rc_col} = '
        "'{summary.run_id}'\"\n"
        "    ).collect()[0].n\n"
        "    _total = spark.sql(  # noqa: F821\n"
        '        f"SELECT COUNT(*) AS n FROM {_table}"\n'
        "    ).collect()[0].n\n"
        "    print(\n"
        '        f"SOX-trail {_layer:6s} {_candidate.dataset_id:20s}: "\n'
        '        f"{_rc_col} matches on {_n}/{_total} rows"\n'
        "    )\n"
    )


def build_notebook(
    *,
    wheel_path: Path,
    bundle_yaml: str,
    mode: Literal["seed", "incremental"] | None,
    datasets: list[str] | None,
    layers: list[str] | None,
    bicc_secret_name: str = "fusion_bicc_password",
    bicc_secret_key: str = "password",
    title: str = "AIDP Fusion Autopilot dispatch",
    # Primitives only; this module must not import orchestrator types.
    execution_backend: str = "legacy-python",
    profile_yaml: str | None = None,
    pack_files: Mapping[str, str] | None = None,
    pack_manifest: dict[str, Any] | None = None,
    # Splices into orchestrator.run kwargs so the cluster-side gate honors
    # the break-glass intent.
    force_fingerprint_skip: bool = False,
    # Splices ``repin_plan_hash=...`` into the run-cell orchestrator.run
    # call so the cluster-side AIDPF-4040 gate honors --repin-plan-hash.
    repin_plan_hash: bool = False,
    # When provided, the bootstrap cell materializes the pinned bronze-schema
    # snapshot at the cluster-side resolved path so preflight can populate
    # `datasetDeltas` on drift. ``None`` means snapshot absent.
    schema_snapshot_yaml: str | None = None,
    # Splices into the run-cell orchestrator.run kwargs so the cluster-side
    # run adopts the supplied resume run_id. ``None`` starts a fresh run.
    resume_run_id: str | None = None,
    # Non-secret runtime env needed by bundle.yaml placeholders on the cluster.
    # Secrets continue to resolve through AIDP credential store in the creds cell.
    env_vars: Mapping[str, str] | None = None,
    # ``--strict-scope`` opts out of implicit transitive include. Threaded
    # into the generated orchestrator.run() call as a literal kwarg.
    strict_scope: bool | None = None,
) -> dict:
    """Build the 4-cell ipynb dict that runs the orchestrator on the cluster.

    Cells:
      1. **install** — base64-decode the wheel, ``pip install --target``,
         ``sys.path.insert``.
      2. **creds + bundle** — load ``FUSION_BICC_PASSWORD`` from
         ``aidputils.secrets``, write ``bundle.yaml``, import orchestrator.
      3. **run** — ``orchestrator.run(...)``, per-step print, marker emit
         via ``summary.to_marker_dict()``.
      4. **verify** — query ``fusion_autopilot_state`` + count silver/gold
         audit-col matches for the run_id.

    The run cell injects ``mode`` / ``datasets`` / ``layers`` as literals
    (via ``repr()``), and threads ``resume_run_id`` when supplied.

    Returns an nbformat-4 dict ready to pass to
    :meth:`AidpRestClient.upload_notebook`.
    """
    # Content-pack is the CLI backend, but this boundary retains backend
    # dispatch for tests that lock the staging primitive contract. Keep the
    # kwargs asserted symmetrically.
    if execution_backend == "content-pack":
        assert profile_yaml is not None, (
            "build_notebook(execution_backend='content-pack', ...) requires profile_yaml"
        )
        assert pack_files is not None, (
            "build_notebook(execution_backend='content-pack', ...) requires pack_files"
        )
        assert pack_manifest is not None, (
            "build_notebook(execution_backend='content-pack', ...) requires pack_manifest"
        )
    elif execution_backend == "legacy-python":
        assert profile_yaml is None and pack_files is None and pack_manifest is None, (
            "build_notebook(execution_backend='legacy-python', ...) must pass "
            "profile_yaml/pack_files/pack_manifest as None"
        )
        assert schema_snapshot_yaml is None, (
            "build_notebook(execution_backend='legacy-python', ...) must "
            "pass schema_snapshot_yaml as None"
        )

    cells = [
        _markdown_cell(f"# {title}\nSelf-contained dispatch from `aidp-fusion-autopilot run`."),
        _code_cell(_build_install_cell(wheel_path)),
        _code_cell(
            _build_creds_cell(
                bundle_yaml=bundle_yaml,
                bicc_secret_name=bicc_secret_name,
                bicc_secret_key=bicc_secret_key,
                env_vars=env_vars,
            )
        ),
    ]

    if execution_backend == "content-pack":
        cells.append(
            _code_cell(
                _build_content_pack_bootstrap_cell(
                    profile_yaml=profile_yaml,
                    pack_files=pack_files,
                    pack_manifest=pack_manifest,
                    schema_snapshot_yaml=schema_snapshot_yaml,
                )
            )
        )

    cells.extend([
        _code_cell(
            _build_run_cell(
                mode=mode, datasets=datasets, layers=layers,
                execution_backend=execution_backend,
                force_fingerprint_skip=force_fingerprint_skip,
                repin_plan_hash=repin_plan_hash,
                resume_run_id=resume_run_id,
                strict_scope=strict_scope,
            )
        ),
        _code_cell(_build_verify_cell()),
    ])
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


__all__ = ["MARKER_BEGIN", "MARKER_END", "build_notebook"]
