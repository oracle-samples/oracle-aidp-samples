"""Unit tests for ``commands.cluster_bootstrap_probe`` (Phase 4.1 / D3).

The dispatch helper itself is exercised in
``tests/unit/dispatch/test_notebook_dispatch.py``. Here we mock the
helper and verify the bootstrap-specific assembly: build wheel → stage
pack → compose notebook → invoke helper → validate envelope → return
``ClusterProbeMarker``. Failure modes map to the typed
``ClusterDispatchError`` / ``ClusterMarkerError`` with the right
``failure_context``.
"""

from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from oracle_ai_data_platform_fusion_autopilot.commands import cluster_bootstrap_probe as cbp
from oracle_ai_data_platform_fusion_autopilot.commands.bootstrap import (
    ResolvedClusterDispatchConfig,
)
from oracle_ai_data_platform_fusion_autopilot.dispatch.errors import (
    DispatchJobSubmitError,
    DispatchMarkerDecodeError,
    DispatchMarkerEnvelopeMissing,
    DispatchUploadError,
)
from oracle_ai_data_platform_fusion_autopilot.schema.cluster_probe_marker import (
    ClusterProbeEnvelope,
    ClusterProbeMarker,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _dispatch_config() -> ResolvedClusterDispatchConfig:
    return ResolvedClusterDispatchConfig(
        aidp_id="ocid1.aidp.test",
        workspace_key="ws-1",
        cluster_key="cluster-uuid",
        cluster_name="cluster_dev",
        region="us-ashburn-1",
        oci_profile="DEFAULT",
        workspace_dir="/Workspace/Shared/fusion-autopilot-bootstrap",
    )


def _happy_marker_payload() -> dict:
    marker = ClusterProbeMarker(
        markerVersion=1,
        tenant="demo-pod",
        bronzeFingerprint="sha256:happy",
        observedSchema={"erp_suppliers": [{"name": "Segment1", "type": "string"}]},
        walkerResults=[
            {
                "name": "supplier_natural_key",
                "kind": "columnAliases",
                "outcome": "auto_resolved",
                "chosen": "Segment1",
            }
        ],
        dispatchedAt=datetime(2026, 6, 7, 12, 0, tzinfo=timezone.utc),
    )
    envelope = ClusterProbeEnvelope(ok=True, marker=marker)
    return envelope.model_dump(by_alias=True, mode="json")


def _error_envelope_payload(error_type: str = "RuntimeError") -> dict:
    envelope = ClusterProbeEnvelope(
        ok=False,
        error_type=error_type,
        error_message="bronze table missing",
        traceback="Traceback...",
    )
    return envelope.model_dump(by_alias=True, mode="json")


def _patch_chain(
    monkeypatch, tmp_path: Path, *, helper_return=None, helper_exc=None
) -> MagicMock:
    """Patch wheel build / pack staging / dispatch helper so the unit
    test exercises only the bootstrap-specific assembly."""
    fake_wheel = tmp_path / "fake-1.0-py3-none-any.whl"
    # Real bytes so _build_install_cell's base64-encode of the wheel
    # contents succeeds. Content is arbitrary — the install cell
    # itself isn't executed in this test.
    fake_wheel.write_bytes(b"PK\x03\x04not-a-real-wheel")
    monkeypatch.setattr(cbp, "build_wheel", lambda **kw: fake_wheel)
    monkeypatch.setattr(
        cbp,
        "stage_pack_files",
        lambda pack: (
            {"__layer_0__/pack.yaml": "id: starter"},
            {"chain_layers": [], "entry_layer_index": 0},
        ),
    )
    monkeypatch.setattr(
        cbp, "_find_plugin_checkout", lambda bundle_path: Path("/repo")
    )
    helper_mock = MagicMock(name="dispatch_notebook_and_fetch_marker")
    if helper_exc is not None:
        helper_mock.side_effect = helper_exc
    else:
        helper_mock.return_value = helper_return
    monkeypatch.setattr(cbp, "dispatch_notebook_and_fetch_marker", helper_mock)
    return helper_mock


def _call(monkeypatch, tmp_path: Path, **overrides):
    bundle_path = tmp_path / "bundle.yaml"
    bundle_path.write_text("apiVersion: x\n", encoding="utf-8")
    kwargs = dict(
        env=MagicMock(name="EnvSpec"),
        bundle=MagicMock(name="Bundle"),
        bundle_path=bundle_path,
        pack=MagicMock(name="ResolvedPack"),
        dispatch_config=_dispatch_config(),
        tenant="demo-pod",
        client_factory=lambda: MagicMock(name="AidpRestClient"),
    )
    kwargs.update(overrides)
    return cbp.dispatch_cluster_probe(**kwargs)


@pytest.fixture
def fake_wheel(tmp_path: Path) -> Path:
    """A tiny on-disk file usable by ``_build_install_cell`` (it
    base64-encodes the bytes — content is irrelevant)."""
    p = tmp_path / "fake-1.0-py3-none-any.whl"
    p.write_bytes(b"PK\x03\x04not-a-real-wheel")
    return p


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestPluginCheckoutResolution:
    def test_external_customer_bundle_uses_installed_module_checkout(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("AIDP_FUSION_PLUGIN_CHECKOUT", raising=False)
        customer_dir = tmp_path / "demo-fusion-cfo"
        customer_dir.mkdir()
        bundle_path = customer_dir / "bundle.yaml"
        bundle_path.write_text("apiVersion: x\n", encoding="utf-8")

        checkout = cbp._find_plugin_checkout(bundle_path)

        assert (checkout / "pyproject.toml").exists()
        assert (checkout / "scripts" / "oracle_ai_data_platform_fusion_autopilot").exists()

    def test_env_override_must_point_to_plugin_checkout(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AIDP_FUSION_PLUGIN_CHECKOUT", str(tmp_path))
        bundle_path = tmp_path / "customer" / "bundle.yaml"
        bundle_path.parent.mkdir()
        bundle_path.write_text("apiVersion: x\n", encoding="utf-8")

        with pytest.raises(FileNotFoundError, match="AIDP_FUSION_PLUGIN_CHECKOUT"):
            cbp._find_plugin_checkout(bundle_path)


class TestHappyPath:
    def test_returns_validated_cluster_probe_marker(self, monkeypatch, tmp_path) -> None:
        _patch_chain(monkeypatch, tmp_path, helper_return=_happy_marker_payload())
        marker = _call(monkeypatch, tmp_path)
        assert isinstance(marker, ClusterProbeMarker)
        assert marker.tenant == "demo-pod"
        assert marker.bronze_fingerprint == "sha256:happy"

    def test_helper_called_with_bootstrap_marker_constants(
        self, monkeypatch, tmp_path
    ) -> None:
        helper_mock = _patch_chain(monkeypatch, tmp_path, helper_return=_happy_marker_payload())
        _call(monkeypatch, tmp_path)
        kw = helper_mock.call_args.kwargs
        # Distinct from the run dispatcher's MARKER_BEGIN — operator
        # who points at the wrong notebook gets a clear envelope-missing.
        assert kw["marker_begin"] == "AIDP_BOOTSTRAP_PROBE_MARKER_BEGIN"
        assert kw["marker_end"] == "AIDP_BOOTSTRAP_PROBE_MARKER_END"
        assert kw["marker_b64"] is True

    def test_cluster_coords_thread_through(self, monkeypatch, tmp_path) -> None:
        helper_mock = _patch_chain(monkeypatch, tmp_path, helper_return=_happy_marker_payload())
        _call(monkeypatch, tmp_path)
        kw = helper_mock.call_args.kwargs
        assert kw["cluster_key"] == "cluster-uuid"
        assert kw["cluster_name"] == "cluster_dev"
        assert kw["workspace_path"].startswith("/Workspace/Shared/fusion-autopilot-bootstrap/probe-")

    def test_job_name_is_aidp_safe(self, monkeypatch, tmp_path) -> None:
        helper_mock = _patch_chain(monkeypatch, tmp_path, helper_return=_happy_marker_payload())
        _call(monkeypatch, tmp_path)
        job_name = helper_mock.call_args.kwargs["job_name"]
        # AIDP rule: letters / underscores / slashes only — no hyphens, no dots.
        assert "-" not in job_name
        assert "." not in job_name
        assert job_name.startswith("aidp_fusion_autopilot_bootstrap_")


# ---------------------------------------------------------------------------
# Pre-dispatch failures → AIDPF-2048 territory
# ---------------------------------------------------------------------------


class TestClusterDispatchFailures:
    def test_upload_error_maps_to_cluster_dispatch_error(
        self, monkeypatch, tmp_path
    ) -> None:
        exc = DispatchUploadError("HTTP 500 upload")
        _patch_chain(monkeypatch, tmp_path, helper_exc=exc)
        with pytest.raises(cbp.ClusterDispatchError) as raised:
            _call(monkeypatch, tmp_path)
        ctx = raised.value.failure_context
        assert ctx.failed_step == "upload_notebook"
        assert ctx.cause_type == "DispatchUploadError"
        assert "HTTP 500 upload" in ctx.cause_message
        assert ctx.cluster_key == "cluster-uuid"

    def test_submit_error_maps_to_cluster_dispatch_error(
        self, monkeypatch, tmp_path
    ) -> None:
        exc = DispatchJobSubmitError("HTTP 400 submit")
        _patch_chain(monkeypatch, tmp_path, helper_exc=exc)
        with pytest.raises(cbp.ClusterDispatchError) as raised:
            _call(monkeypatch, tmp_path)
        assert raised.value.failure_context.failed_step == "create_notebook_job"


# ---------------------------------------------------------------------------
# Marker failures → AIDPF-2049 territory
# ---------------------------------------------------------------------------


class TestClusterMarkerFailures:
    def test_envelope_missing_maps_to_cluster_marker_error(
        self, monkeypatch, tmp_path
    ) -> None:
        exc = DispatchMarkerEnvelopeMissing(
            "no envelope",
            executed_notebook={"cells": [{"outputs": [{"text": "no marker"}]}]},
            stdout_excerpt="no marker",
        )
        _patch_chain(monkeypatch, tmp_path, helper_exc=exc)
        with pytest.raises(cbp.ClusterMarkerError) as raised:
            _call(monkeypatch, tmp_path)
        ctx = raised.value.failure_context
        assert ctx.kind == "envelope_missing"
        assert ctx.stdout_excerpt == "no marker"
        # The full stdout for the companion cluster_stdout.log file is
        # reconstructed from the executed notebook payload.
        assert "no marker" in ctx.stdout_full
        # Executed notebook is preserved on the exception for diagnostic
        # writers in Step 8.
        assert raised.value.executed_notebook is not None

    def test_decode_error_maps_to_validation_failed(
        self, monkeypatch, tmp_path
    ) -> None:
        exc = DispatchMarkerDecodeError(
            "base64 decode failed",
            executed_notebook={"cells": []},
            stdout_excerpt="garbage",
        )
        _patch_chain(monkeypatch, tmp_path, helper_exc=exc)
        with pytest.raises(cbp.ClusterMarkerError) as raised:
            _call(monkeypatch, tmp_path)
        assert raised.value.failure_context.kind == "validation_failed"

    def test_cluster_reported_error_envelope_surfaces_traceback(
        self, monkeypatch, tmp_path
    ) -> None:
        payload = _error_envelope_payload(error_type="ValueError")
        _patch_chain(monkeypatch, tmp_path, helper_return=payload)
        with pytest.raises(cbp.ClusterMarkerError) as raised:
            _call(monkeypatch, tmp_path)
        ctx = raised.value.failure_context
        assert ctx.kind == "cluster_reported_error"
        assert ctx.cluster_error_type == "ValueError"
        assert ctx.cluster_error_message == "bronze table missing"
        assert ctx.cluster_traceback == "Traceback..."

    def test_marker_version_2_raises_validation_failed(
        self, monkeypatch, tmp_path
    ) -> None:
        # Future cluster emits markerVersion: 2 — Literal[1] rejects.
        bad_payload = {
            "ok": True,
            "marker": {
                "markerVersion": 2,
                "tenant": "x",
                "bronzeFingerprint": "sha256:y",
                "observedSchema": {},
                "walkerResults": [],
                "dispatchedAt": "2026-06-07T12:00:00+00:00",
            },
        }
        _patch_chain(monkeypatch, tmp_path, helper_return=bad_payload)
        with pytest.raises(cbp.ClusterMarkerError) as raised:
            _call(monkeypatch, tmp_path)
        assert raised.value.failure_context.kind == "validation_failed"


# ---------------------------------------------------------------------------
# Notebook builder shape
# ---------------------------------------------------------------------------


class TestNotebookBuilder:
    @staticmethod
    def _nb(fake_wheel, **overrides):
        kwargs = dict(
            wheel_path=fake_wheel,
            bundle_yaml="apiVersion: x\n",
            pack_files={"pack.yaml": "id: starter"},
            pack_manifest={"chain_layers": [], "entry_layer_index": 0},
            tenant="demo-pod",
            bicc_secret_name="fusion_bicc_password",
            bicc_secret_key="password",
        )
        kwargs.update(overrides)
        return cbp._build_notebook(**kwargs)

    def test_four_cells_plus_markdown(self, fake_wheel) -> None:
        nb = self._nb(fake_wheel)
        # 1 markdown + 4 code cells (install / creds / stage / probe).
        assert len(nb["cells"]) == 5
        assert nb["cells"][0]["cell_type"] == "markdown"
        assert all(c["cell_type"] == "code" for c in nb["cells"][1:])
        assert nb["nbformat"] == 4

    def test_probe_cell_in_cell_try_except(self, fake_wheel) -> None:
        nb = self._nb(fake_wheel)
        probe_src = "".join(nb["cells"][4]["source"])
        # The whole point of plan.md Step 5: try/except is INSIDE the
        # probe cell, not split across cells.
        assert "try:" in probe_src
        assert "except Exception as exc:" in probe_src
        # The cell emits a marker via MARKER_BEGIN/END regardless of
        # ok/error branch.
        assert "AIDP_BOOTSTRAP_PROBE_MARKER_BEGIN" in probe_src
        assert "AIDP_BOOTSTRAP_PROBE_MARKER_END" in probe_src

    def test_probe_cell_renders_bundle_and_uses_source_helper(self, fake_wheel) -> None:
        nb = self._nb(fake_wheel, pack_files={})
        probe_src = "".join(nb["cells"][4]["source"])
        # Renders the bundle in-cell (resolves ${VAR} fusion fields) rather
        # than the old raw yaml.safe_load — Risk R7.
        assert "load_bundle(BUNDLE_PATH)" in probe_src
        assert "yaml.safe_load" not in probe_src
        # Uses the shared producer-selection helper (landed-vs-source,
        # physical node.target) — Risk R8.
        assert "resolve_observed(" in probe_src
        assert "describe_bronze(" not in probe_src
        # Resolves the password in-cell and rejects unsupported secret refs.
        assert "_resolve_password(" in probe_src
        assert "${aidp:secret:" in probe_src
        assert "MARKER_BEGIN =" in probe_src
        assert "MARKER_END =" in probe_src

    def test_creds_cell_loads_secret_no_plaintext(self, fake_wheel) -> None:
        nb = self._nb(
            fake_wheel,
            bicc_secret_name="my_secret",
            bicc_secret_key="pw",
        )
        creds_src = "".join(nb["cells"][2]["source"])
        # Fetches the BICC password from the AIDP credential store cluster-side
        # using the configured name/key (Step 5 credential contract).
        assert "aidputils.secrets.get(" in creds_src
        assert "my_secret" in creds_src
        assert "pw" in creds_src
        assert 'os.environ["FUSION_BICC_PASSWORD"]' in creds_src
        # The whole notebook payload must never carry a plaintext password.
        whole = "".join(
            "".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code"
        )
        assert "aidputils.secrets.get(" in whole  # fetched, not embedded

    def test_staging_cell_uses_two_arg_materialize_helper(self, fake_wheel) -> None:
        nb = self._nb(fake_wheel)
        staging_src = "".join(nb["cells"][3]["source"])
        # plan.md Step 5 high-severity fix #2 — real signature is
        # (files, manifest) — helper makes its own tempdir.
        assert "materialize_staged_pack(\n    _PACK_FILES, _PACK_MANIFEST\n)" in staging_src
