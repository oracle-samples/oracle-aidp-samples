"""Phase 2 unit tests for ``dispatch/notebook_builder.py``.

Covers:
* build_notebook returns an nbformat-4 dict (NOT a string) — contract preserved.
* emits the new bootstrap cell with
  base64-encoded primitives; raw values never appear in cell source.
* omits the bootstrap cell — cell list
  shape identical to Phase 1 baseline.
* Invariant: content-pack requires all three primitives non-None;
  legacy-python forbids them.
* Adversarial round-trip: profile YAML containing triple-quotes /
  backslashes / SQL-injection content survives base64 round-trip
  byte-for-byte and never appears as a raw substring of the notebook source.
"""

from __future__ import annotations

import base64
import json
import pathlib
import re

import pytest

from oracle_ai_data_platform_fusion_autopilot.dispatch.notebook_builder import (
    _build_content_pack_bootstrap_cell,
    build_notebook,
)


@pytest.fixture
def tmp_wheel(tmp_path: pathlib.Path) -> pathlib.Path:
    """build_notebook reads the wheel file to base64-encode it. Provide a
    minimal placeholder so the install cell builds without I/O errors."""
    wheel = tmp_path / "fake.whl"
    wheel.write_bytes(b"PK\x03\x04\x00\x00\x00\x00")
    return wheel


def _minimal_args(wheel_path: pathlib.Path, **overrides) -> dict:
    base = dict(
        wheel_path=wheel_path,
        bundle_yaml="apiVersion: aidp-fusion-autopilot/v1\nproject: x\n",
        mode="seed",
        datasets=None,
        layers=None,
    )
    base.update(overrides)
    # Infer execution_backend from the presence of content-pack staging
    # primitives — the regex sweep stripped the explicit kwarg, so the
    # helper now picks the right default.
    if "execution_backend" not in base:
        if any(
            overrides.get(k) is not None
            for k in ("profile_yaml", "pack_files", "pack_manifest")
        ):
            base["execution_backend"] = "content-pack"
        else:
            base["execution_backend"] = "legacy-python"
    return base


# ---------------------------------------------------------------------------
# Return type contract: nbformat-4 dict (NOT a string)
# ---------------------------------------------------------------------------


class TestReturnType:
    def test_returns_dict_legacy_backend(self, tmp_wheel) -> None:
        result = build_notebook(**_minimal_args(tmp_wheel))
        assert isinstance(result, dict)
        assert result["nbformat"] == 4
        assert "cells" in result

    def test_returns_dict_content_pack_backend(self, tmp_wheel) -> None:
        result = build_notebook(
            **_minimal_args(
                tmp_wheel,
                profile_yaml="schemaVersion: 1\ntenant: x\n",
                pack_files={"__layer_0__/pack.yaml": "id: x\nversion: 1.0.0\n"},
                pack_manifest={"chain_layers": [], "entry_layer_index": 0},
            )
        )
        assert isinstance(result, dict)
        assert result["nbformat"] == 4


# ---------------------------------------------------------------------------
# Cell-list shape
# ---------------------------------------------------------------------------


class TestCellListShape:
    def test_legacy_backend_omits_bootstrap_cell(self, tmp_wheel) -> None:
        nb = build_notebook(**_minimal_args(tmp_wheel))
        # markdown + install + creds + run + verify = 5 cells.
        assert len(nb["cells"]) == 5

    def test_content_pack_backend_inserts_bootstrap_cell(self, tmp_wheel) -> None:
        nb = build_notebook(
            **_minimal_args(
                tmp_wheel,
                profile_yaml="schemaVersion: 1\ntenant: x\n",
                pack_files={"__layer_0__/pack.yaml": "id: x\nversion: 1.0.0\n"},
                pack_manifest={"chain_layers": [], "entry_layer_index": 0},
            )
        )
        # markdown + install + creds + bootstrap + run + verify = 6 cells.
        assert len(nb["cells"]) == 6


# ---------------------------------------------------------------------------
# Invariant: content-pack requires primitives; legacy-python forbids them
# ---------------------------------------------------------------------------


class TestInvariantChecks:
    def test_content_pack_missing_profile_yaml_raises(self, tmp_wheel) -> None:
        with pytest.raises(AssertionError):
            build_notebook(
                **_minimal_args(
                    tmp_wheel,
                    profile_yaml=None,
                    pack_files={"x": "y"},
                    pack_manifest={"a": 1},
                )
            )

    def test_content_pack_missing_pack_files_raises(self, tmp_wheel) -> None:
        with pytest.raises(AssertionError):
            build_notebook(
                **_minimal_args(
                    tmp_wheel,
                    profile_yaml="x",
                    pack_files=None,
                    pack_manifest={"a": 1},
                )
            )

    def test_legacy_python_with_pack_files_raises(self, tmp_wheel) -> None:
        with pytest.raises(AssertionError):
            build_notebook(
                **_minimal_args(
                    tmp_wheel,
                    pack_files={"x": "y"},
                )
            )


# ---------------------------------------------------------------------------
# Visible literal: orchestrator.run kwarg
# ---------------------------------------------------------------------------


class TestRunCellLiteral:
    def test_run_cell_contains_execution_backend_literal(self, tmp_wheel) -> None:
        nb = build_notebook(
            **_minimal_args(
                tmp_wheel,
                profile_yaml="schemaVersion: 1\ntenant: x\n",
                pack_files={"x": "y"},
                pack_manifest={"a": 1},
            )
        )
        all_sources = "".join(
            "".join(c["source"]) if isinstance(c["source"], list) else c["source"]
            for c in nb["cells"] if c["cell_type"] == "code"
        )
        assert '' in all_sources

    def test_legacy_backend_run_cell_has_legacy_python_literal(self, tmp_wheel) -> None:
        nb = build_notebook(**_minimal_args(tmp_wheel))
        all_sources = "".join(
            "".join(c["source"]) if isinstance(c["source"], list) else c["source"]
            for c in nb["cells"] if c["cell_type"] == "code"
        )
        assert '' in all_sources


# ---------------------------------------------------------------------------
# Adversarial round-trip — no raw payload leakage
# ---------------------------------------------------------------------------


class TestAdversarialRoundTrip:
    def test_profile_yaml_round_trips_via_base64(self) -> None:
        """Round-trip: take a profile YAML containing triple-quotes and
        backslashes and SQL-injection style values, encode it, embed in
        cell source, decode it back — equality holds byte-for-byte. The
        raw payload MUST NOT appear as a substring of the cell source.
        """
        adversarial = (
            'schemaVersion: 1\n'
            'tenant: adversarial\n'
            'pinnedAt: 2026-01-01T00:00:00+00:00\n'
            'bronzeSchemaFingerprint: "sha256:abc"\n'
            'malicious_value: "\'; DROP TABLE evil; --"\n'
            'backslash: "C:\\\\path"\n'
        )
        bootstrap = _build_content_pack_bootstrap_cell(
            profile_yaml=adversarial,
            pack_files={"__layer_0__/pack.yaml": "id: p\nversion: 1.0.0\n"},
            pack_manifest={"chain_layers": [], "entry_layer_index": 0},
        )

        # Extract the _PROFILE_YAML base64 token.
        m = re.search(r"_PROFILE_YAML = _b64\.b64decode\(['\"]([^'\"]+)['\"]\)", bootstrap)
        assert m is not None
        token = m.group(1)

        decoded = base64.b64decode(token).decode("utf-8")
        assert decoded == adversarial

        # No-raw-payload-leakage canaries.
        assert "DROP TABLE" not in bootstrap
        assert "malicious_value" not in bootstrap

    def test_generated_run_cell_uses_kwargs_orchestrator_run_accepts(self, tmp_wheel) -> None:
        """Lock the round-12 review finding: the generated notebook's
        orchestrator.run(...) call must use kwargs that the real
        function signature accepts. Otherwise the notebook raises
        TypeError before any node executes.
        """
        import inspect

        from oracle_ai_data_platform_fusion_autopilot import orchestrator

        # Pull the real orchestrator.run signature.
        run_sig = inspect.signature(orchestrator.run)
        accepted_kwargs = set(run_sig.parameters)

        # Build a content-pack notebook and extract the run cell.
        nb = build_notebook(
            **_minimal_args(
                tmp_wheel,
                profile_yaml="schemaVersion: 1\ntenant: x\n",
                pack_files={"x": "y"},
                pack_manifest={"a": 1},
            )
        )
        run_cells = [c for c in nb["cells"] if c["cell_type"] == "code"]
        all_sources = "\n".join(
            "".join(c["source"]) if isinstance(c["source"], list) else c["source"]
            for c in run_cells
        )

        # Find every kwarg the run cell passes to orchestrator.run(...).
        run_call_match = re.search(
            r"orchestrator\.run\(([^)]*)\)", all_sources, re.DOTALL,
        )
        assert run_call_match is not None
        args_block = run_call_match.group(1)
        used_kwargs = set(re.findall(r"^\s*(\w+)\s*=", args_block, re.MULTILINE))

        # Every used kwarg must be accepted by the real function. If a
        # future change emits an unsupported kwarg, this test fails
        # BEFORE the notebook is ever uploaded.
        unsupported = used_kwargs - accepted_kwargs
        assert not unsupported, (
            f"Generated notebook's orchestrator.run(...) uses kwargs "
            f"the real function doesn't accept: {unsupported!r}. "
            f"Accepted: {sorted(accepted_kwargs)!r}."
        )

    def test_legacy_backend_run_cell_also_uses_accepted_kwargs(self, tmp_wheel) -> None:
        """Same check for the legacy-python branch — must not emit
        unsupported kwargs."""
        import inspect

        from oracle_ai_data_platform_fusion_autopilot import orchestrator

        accepted_kwargs = set(inspect.signature(orchestrator.run).parameters)

        nb = build_notebook(**_minimal_args(tmp_wheel))
        all_sources = "\n".join(
            "".join(c["source"]) if isinstance(c["source"], list) else c["source"]
            for c in nb["cells"] if c["cell_type"] == "code"
        )
        run_call_match = re.search(r"orchestrator\.run\(([^)]*)\)", all_sources, re.DOTALL)
        assert run_call_match is not None
        used_kwargs = set(re.findall(r"^\s*(\w+)\s*=", run_call_match.group(1), re.MULTILINE))
        unsupported = used_kwargs - accepted_kwargs
        assert not unsupported, (
            f"Legacy-backend notebook emits unsupported kwargs: {unsupported!r}"
        )

    def test_pack_files_dict_round_trips_via_base64(self) -> None:
        pack_files = {
            "__layer_0__/pack.yaml": "id: malicious-pack\nversion: 1.0.0\n",
            "__layer_0__/silver/dim_x.sql": "MERGE INTO target USING src ON 1=1 WHEN MATCHED THEN DELETE",
        }
        bootstrap = _build_content_pack_bootstrap_cell(
            profile_yaml="x: 1\n",
            pack_files=pack_files,
            pack_manifest={"chain_layers": [], "entry_layer_index": 0},
        )

        m = re.search(
            r"_PACK_FILES = _json\.loads\(_b64\.b64decode\(['\"]([^'\"]+)['\"]\)",
            bootstrap,
        )
        assert m is not None
        token = m.group(1)
        decoded = json.loads(base64.b64decode(token).decode("utf-8"))
        assert decoded == pack_files

        # Raw SQL must NOT appear as substring.
        assert "MERGE INTO target" not in bootstrap


# ---------------------------------------------------------------------------
# Phase 3d — schema snapshot staging
# ---------------------------------------------------------------------------


class TestPhase3dSchemaSnapshotStaging:
    _MINIMAL_SNAPSHOT_YAML = (
        "schemaVersion: 1\n"
        "tenant: acme\n"
        "pinnedAt: 2026-06-06T12:00:00+00:00\n"
        "bronzeSchemaFingerprint: sha256:" + "a" * 64 + "\n"
        "datasets:\n"
        "  - datasetId: ap_invoices\n"
        "    columns:\n"
        "      - name: A\n"
        "        type: bigint\n"
    )

    def test_content_pack_with_snapshot_materialises_on_cluster(
        self, tmp_wheel
    ) -> None:
        nb = build_notebook(
            **_minimal_args(
                tmp_wheel,
                profile_yaml="schemaVersion: 1\ntenant: acme\n",
                pack_files={"__layer_0__/pack.yaml": "id: x\nversion: 1.0.0\n"},
                pack_manifest={"chain_layers": [], "entry_layer_index": 0},
                schema_snapshot_yaml=self._MINIMAL_SNAPSHOT_YAML,
            )
        )
        all_sources = "\n".join(
            "".join(c["source"]) if isinstance(c["source"], list) else c["source"]
            for c in nb["cells"]
            if c["cell_type"] == "code"
        )
        # Bootstrap cell decodes + writes the snapshot to the resolved path.
        assert "_SCHEMA_SNAPSHOT_YAML" in all_sources
        assert "resolve_snapshot_path" in all_sources
        assert "schema-snapshot.yaml" not in all_sources  # not raw — resolved on cluster
        # The decoded payload round-trips byte-for-byte.
        m = re.search(
            r"_SCHEMA_SNAPSHOT_YAML = _b64\.b64decode\(['\"]([^'\"]+)['\"]\)",
            all_sources,
        )
        assert m is not None
        decoded = base64.b64decode(m.group(1)).decode("utf-8")
        assert decoded == self._MINIMAL_SNAPSHOT_YAML

    def test_content_pack_without_snapshot_omits_materialisation(
        self, tmp_wheel
    ) -> None:
        nb = build_notebook(
            **_minimal_args(
                tmp_wheel,
                profile_yaml="schemaVersion: 1\ntenant: acme\n",
                pack_files={"x": "y"},
                pack_manifest={"a": 1},
                schema_snapshot_yaml=None,
            )
        )
        all_sources = "\n".join(
            "".join(c["source"]) if isinstance(c["source"], list) else c["source"]
            for c in nb["cells"]
            if c["cell_type"] == "code"
        )
        assert "_SCHEMA_SNAPSHOT_YAML" not in all_sources
        assert "resolve_snapshot_path" not in all_sources

    def test_legacy_python_with_snapshot_raises(self, tmp_wheel) -> None:
        with pytest.raises(AssertionError):
            build_notebook(
                **_minimal_args(
                    tmp_wheel,
                    schema_snapshot_yaml=self._MINIMAL_SNAPSHOT_YAML,
                )
            )

    def test_dispatch_via_rest_threads_kwarg_through(
        self, tmp_wheel, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``dispatch_via_rest`` must thread ``schema_snapshot_yaml``
        into ``build_notebook``. Mirrors the
        ``force_fingerprint_skip`` round-1 thread-through test."""
        from unittest.mock import MagicMock

        import oracle_ai_data_platform_fusion_autopilot.dispatch as dispatch_mod

        # Capture the kwargs that ``build_notebook`` is called with.
        captured: dict = {}

        def _spy_build_notebook(**kwargs):
            captured.update(kwargs)
            return {"cells": [], "nbformat": 4, "nbformat_minor": 5}

        monkeypatch.setattr(dispatch_mod, "build_notebook", _spy_build_notebook)
        monkeypatch.setattr(
            dispatch_mod, "build_wheel", lambda **_: pathlib.Path("/tmp/x.whl")
        )

        # Stub preflight + REST client so dispatch reaches build_notebook.
        from oracle_ai_data_platform_fusion_autopilot.dispatch.preflight import (
            PreflightResult,
        )

        monkeypatch.setattr(
            dispatch_mod,
            "run_local_preflight",
            lambda **_: [PreflightResult(name="x", status="PASS", detail="")],
        )
        monkeypatch.setattr(
            dispatch_mod,
            "run_remote_preflight",
            lambda **_: [PreflightResult(name="x", status="PASS", detail="")],
        )

        rest_client = MagicMock(name="AidpRestClient")
        rest_client.upload_notebook.return_value = "/Workspace/x.ipynb"
        rest_client.create_notebook_job.return_value = "job-1"
        rest_client.submit_run.return_value = "run-1"
        from oracle_ai_data_platform_fusion_autopilot.dispatch.rest_client import (
            RunResult,
        )

        raw = {"taskToTaskRunMap": {"orchestrator_run": "task-1"}}
        rest_client.poll_run.return_value = RunResult(status="SUCCESS", raw=raw)
        # Return a notebook with a parseable success marker so dispatch
        # builds a RunSummary without raising. Use the real
        # ``RunSummary.empty(...)`` shape via ``to_marker_dict``.
        from oracle_ai_data_platform_fusion_autopilot.schema.run_summary import (
            RunSummary,
        )

        marker_dict = RunSummary.empty(
            bundle_project="x", mode="incremental"
        ).to_marker_dict()
        marker_text = (
            f"AIDP_LIVE_TEST_RESULT_BEGIN {json.dumps(marker_dict)} "
            f"AIDP_LIVE_TEST_RESULT_END"
        )
        rest_client.fetch_output.return_value = json.dumps(
            {
                "cells": [
                    {
                        "cell_type": "code",
                        "outputs": [
                            {
                                "output_type": "stream",
                                "name": "stdout",
                                "text": marker_text,
                            }
                        ],
                    }
                ]
            }
        )
        from oracle_ai_data_platform_fusion_autopilot.dispatch.rest_client import (
            AidpRestClient as RealAidpRestClient,
        )

        factory = MagicMock(return_value=rest_client)
        factory.parse_marker = RealAidpRestClient.parse_marker
        factory.resolve_task_run_key = RealAidpRestClient.resolve_task_run_key
        monkeypatch.setattr(dispatch_mod, "AidpRestClient", factory)

        bundle_path = tmp_path / "bundle.yaml"
        bundle_path.write_text(
            "apiVersion: aidp-fusion-autopilot/v1\nproject: x\n", encoding="utf-8"
        )

        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import (
            AidpConfig,
            EnvSpec,
        )

        env = EnvSpec.model_validate(
            {
                "workspaceKey": "wk",
                "aiDataPlatformId": "ocid1.x",
                "clusterKey": "ck",
                "clusterName": "cn",
                "ociProfile": "DEFAULT",
            }
        )
        cfg = AidpConfig.model_validate(
            {
                "apiVersion": "aidp-fusion-autopilot/v1",
                "project": "x",
                "environments": {"dev": env.model_dump(by_alias=True)},
            }
        )

        dispatch_mod.dispatch_via_rest(
            bundle_path=bundle_path,
            config=cfg,
            env=env,
            env_name="dev",
            mode="incremental",
            datasets=None,
            layers=None,
            profile_yaml="schemaVersion: 1\ntenant: x\n",
            pack_files={"x": "y"},
            pack_manifest={"a": 1},
            schema_snapshot_yaml=self._MINIMAL_SNAPSHOT_YAML,
        )

        assert captured.get("schema_snapshot_yaml") == self._MINIMAL_SNAPSHOT_YAML
