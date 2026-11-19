"""P1.5ε §Step 1d — schema/bundle.load_bundle move smoke + back-compat tests.

Most of the bundle-loader's behavior is exercised by the pre-existing
``tests/unit/test_orchestrator_runtime.py::TestLoadBundle`` block — those
tests keep working unchanged because they import via the back-compat
re-export at ``orchestrator.runtime.load_bundle``. This file adds:

- identity tests (the re-export points at the schema-level function)
- focused coverage of the two contracts the move had to preserve:
  ``${VAR}`` generic env-var rendering vs ``${env:VAR}`` / ``${vault:OCID}``
  password-marker preservation.
"""

from __future__ import annotations

from pathlib import Path

import pytest


_MINIMAL_BUNDLE_TEMPLATE = """\
apiVersion: aidp-fusion-autopilot/v1
project: test-load-bundle
fusion:
  serviceUrl: https://fusion.example.com
  username: fusion_user
  password: {password}
  externalStorage: storage-profile-1
datasets:
  - id: erp_suppliers
"""


def _write_bundle(tmp_path: Path, password: str) -> Path:
    p = tmp_path / "bundle.yaml"
    p.write_text(_MINIMAL_BUNDLE_TEMPLATE.format(password=password), encoding="utf-8")
    return p


class TestLoadBundleIdentity:
    def test_runtime_re_export_is_schema_function(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.runtime import (
            load_bundle as from_runtime,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import (
            load_bundle as from_schema,
        )
        assert from_runtime is from_schema

    def test_render_env_vars_re_export_identity(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.runtime import (
            _render_env_vars as from_runtime,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import (
            _render_env_vars as from_schema,
        )
        assert from_runtime is from_schema


class TestVarContracts:
    """The two-contract env-var handling that the move had to preserve."""

    def test_generic_var_expanded_at_load(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # ${VAR} is a generic substitution — rendered immediately by
        # load_bundle. A real password value is what lands on the model.
        monkeypatch.setenv("FUSION_BICC_PASSWORD", "actual-password")
        bundle_path = _write_bundle(tmp_path, "${FUSION_BICC_PASSWORD}")
        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import load_bundle

        bundle, _ = load_bundle(bundle_path)
        assert bundle.fusion.password == "actual-password"

    def test_generic_var_unset_raises_bundle_load_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("FUSION_BICC_PASSWORD", raising=False)
        bundle_path = _write_bundle(tmp_path, "${FUSION_BICC_PASSWORD}")
        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import load_bundle
        from oracle_ai_data_platform_fusion_autopilot.schema.errors import BundleLoadError

        with pytest.raises(BundleLoadError, match="FUSION_BICC_PASSWORD"):
            load_bundle(bundle_path)

    def test_env_ref_preserved_through_load(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # ${env:VAR} is a password marker — preserved literally by load_bundle
        # even when the env var isn't set. Credential resolution is a
        # separate (later) phase via _resolve_password.
        monkeypatch.delenv("UNSET_AT_LOAD_TIME", raising=False)
        bundle_path = _write_bundle(tmp_path, "${env:UNSET_AT_LOAD_TIME}")
        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import load_bundle

        bundle, _ = load_bundle(bundle_path)
        assert bundle.fusion.password == "${env:UNSET_AT_LOAD_TIME}"

    def test_vault_ref_preserved_through_load(self, tmp_path: Path) -> None:
        # ${vault:OCID} is a vault marker — preserved literally by load_bundle.
        bundle_path = _write_bundle(
            tmp_path, "${vault:ocid1.vaultsecret.oc1.iad.example}"
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import load_bundle

        bundle, _ = load_bundle(bundle_path)
        assert (
            bundle.fusion.password == "${vault:ocid1.vaultsecret.oc1.iad.example}"
        )


class TestFailureMessageWrapping:
    def test_missing_file_wrapped_as_bundle_load_error(self, tmp_path: Path) -> None:
        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import load_bundle
        from oracle_ai_data_platform_fusion_autopilot.schema.errors import BundleLoadError

        with pytest.raises(BundleLoadError, match="not found"):
            load_bundle(tmp_path / "does-not-exist.yaml")

    def test_malformed_yaml_wrapped_as_bundle_load_error(
        self, tmp_path: Path
    ) -> None:
        bundle_path = tmp_path / "bundle.yaml"
        bundle_path.write_text("not: valid: yaml: at: all", encoding="utf-8")
        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import load_bundle
        from oracle_ai_data_platform_fusion_autopilot.schema.errors import BundleLoadError

        with pytest.raises(BundleLoadError, match="Malformed YAML"):
            load_bundle(bundle_path)
