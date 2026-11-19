"""Unit tests for the Pydantic v2 bundle.yaml schema."""

from __future__ import annotations

import pathlib

import pytest
import yaml
from pydantic import ValidationError

from oracle_ai_data_platform_fusion_autopilot.schema.bundle import AidpConfig, Bundle, Defaults, EnvSpec


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
EXAMPLES = REPO_ROOT / "examples"


class TestBundleSchema:
    def test_minimal_example_parses(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # The example uses ${VAR} substitutions; we expect parse to succeed even
        # before substitution (Bundle accepts strings as-is — substitution is a
        # separate phase that uses schema/refs.py).
        raw = (EXAMPLES / "minimal_gl_only.yaml").read_text(encoding="utf-8")
        data = yaml.safe_load(raw)
        bundle = Bundle.model_validate(data)
        assert bundle.api_version == "aidp-fusion-autopilot/v1"
        assert bundle.project == "cecl-finance-lake"
        assert bundle.aidp.catalog == "fusion_catalog"
        assert bundle.aidp.storage_format == "delta"
        ids = {d.id for d in bundle.datasets}
        assert ids == {"gl_journal_lines", "gl_period_balances", "gl_coa"}

    def test_full_finance_example_parses(self) -> None:
        raw = (EXAMPLES / "full_finance.yaml").read_text(encoding="utf-8")
        data = yaml.safe_load(raw)
        bundle = Bundle.model_validate(data)
        assert bundle.oac is not None
        assert bundle.oac.enabled is True
        # Confirmed PVOs from blogs are in the example:
        ids = {d.id for d in bundle.datasets}
        assert {"erp_suppliers", "po_orders", "scm_items"} <= ids

    def test_duplicate_dataset_id_rejected(self) -> None:
        data = {
            "apiVersion": "aidp-fusion-autopilot/v1",
            "project": "test",
            "fusion": {
                "serviceUrl": "https://x",
                "username": "u",
                "password": "p",
                "externalStorage": "s",
            },
            "datasets": [
                {"id": "gl_journal_lines"},
                {"id": "gl_journal_lines"},
            ],
        }
        with pytest.raises(ValidationError, match="duplicate dataset id"):
            Bundle.model_validate(data)

    def test_extra_fields_rejected(self) -> None:
        data = {
            "apiVersion": "aidp-fusion-autopilot/v1",
            "project": "test",
            "fusion": {
                "serviceUrl": "https://x",
                "username": "u",
                "password": "p",
                "externalStorage": "s",
            },
            "datasets": [{"id": "gl_journal_lines"}],
            "unexpected_key": "should fail",
        }
        with pytest.raises(ValidationError):
            Bundle.model_validate(data)

    def test_default_storage_format_is_delta(self) -> None:
        data = {
            "apiVersion": "aidp-fusion-autopilot/v1",
            "project": "test",
            "fusion": {
                "serviceUrl": "https://x",
                "username": "u",
                "password": "p",
                "externalStorage": "s",
            },
            "datasets": [{"id": "gl_journal_lines"}],
        }
        bundle = Bundle.model_validate(data)
        assert bundle.aidp.storage_format == "delta"

    def test_dataset_enabled_false_roundtrips_cleanly(self) -> None:
        """P1.5α-fix15: `enabled: false` must parse cleanly AND survive a
        re-serialize. The orchestrator honors this field at
        ``resolve_plan`` (skipping the dataset from ``all_specs``); if the
        schema silently dropped the field, the orchestrator would treat
        the disabled dataset as enabled and the fix15 contract would
        regress without anyone noticing.
        """
        data = {
            "apiVersion": "aidp-fusion-autopilot/v1",
            "project": "test-fix15-roundtrip",
            "fusion": {
                "serviceUrl": "https://x",
                "username": "u",
                "password": "p",
                "externalStorage": "s",
            },
            "datasets": [
                {"id": "ap_invoices", "enabled": False},
                {"id": "erp_suppliers"},  # default: enabled=True
            ],
        }
        bundle = Bundle.model_validate(data)
        # Field parses to the expected values
        assert len(bundle.datasets) == 2
        ds_by_id = {d.id: d for d in bundle.datasets}
        assert ds_by_id["ap_invoices"].enabled is False, (
            "explicit `enabled: false` must be honored"
        )
        assert ds_by_id["erp_suppliers"].enabled is True, (
            "default `enabled: True` must be applied when omitted"
        )

        # Round-trip: re-serialize + re-parse must preserve the field
        dumped = bundle.model_dump(by_alias=True)
        reparsed = Bundle.model_validate(dumped)
        reparsed_by_id = {d.id: d for d in reparsed.datasets}
        assert reparsed_by_id["ap_invoices"].enabled is False, (
            "`enabled: false` must survive a re-serialize — if the schema "
            "drops it silently, the orchestrator's fix15 honor-check at "
            "resolve_plan stops working"
        )

    def test_fusion_schema_overrides_roundtrips_cleanly(self) -> None:
        """P1.5α-fix19: `schemaOverrides` on FusionConn must parse cleanly AND
        survive a re-serialize. The orchestrator's preflight reads this field
        as the tier-1 override source; a silent schema regression would break
        the override path without anyone noticing.
        """
        data = {
            "apiVersion": "aidp-fusion-autopilot/v1",
            "project": "test-fix19-roundtrip",
            "fusion": {
                "serviceUrl": "https://x",
                "username": "u",
                "password": "p",
                "externalStorage": "s",
                "schemaOverrides": {
                    "po_receipts": "Financial",
                    "scm_items": "SCM",
                },
            },
            "datasets": [{"id": "gl_journal_lines"}],
        }
        bundle = Bundle.model_validate(data)
        assert bundle.fusion.schema_overrides == {
            "po_receipts": "Financial",
            "scm_items": "SCM",
        }

        # Default (omitted) → {}
        data_no_override = {
            **data,
            "fusion": {k: v for k, v in data["fusion"].items() if k != "schemaOverrides"},
        }
        bundle_default = Bundle.model_validate(data_no_override)
        assert bundle_default.fusion.schema_overrides == {}, (
            "omitting schemaOverrides must default to {} so existing bundles parse"
        )

        # Round-trip: re-serialize + re-parse preserves the field
        dumped = bundle.model_dump(by_alias=True)
        reparsed = Bundle.model_validate(dumped)
        assert reparsed.fusion.schema_overrides == {
            "po_receipts": "Financial",
            "scm_items": "SCM",
        }, (
            "schemaOverrides must survive a re-serialize — if the schema "
            "drops it silently, fix19's preflight tier-1 override path stops "
            "working"
        )


class TestAidpConfigSchema:
    def test_example_parses(self) -> None:
        raw = (EXAMPLES / "aidp.config.example.yaml").read_text(encoding="utf-8")
        data = yaml.safe_load(raw)
        config = AidpConfig.model_validate(data)
        assert config.api_version == "aidp-fusion-autopilot/v1"
        assert "dev" in config.environments
        assert "prod" in config.environments
        # P1.5ε: both envs use profile mode now — vault mode is rejected
        # at preflight until P1.5ε-fix6 (cloud-side signer support) ships.
        assert config.environments["dev"].auth.mode == "profile"
        assert config.environments["prod"].auth.mode == "profile"
        # P1.5ε: both envs declare dispatch coords (aiDataPlatformId,
        # clusterKey, clusterName) so `aidp-fusion-autopilot run` (no --inline)
        # works once the operator fills in the placeholders.
        assert config.environments["dev"].ai_data_platform_id is not None
        assert config.environments["dev"].cluster_key is not None
        assert config.environments["dev"].cluster_name == "fusion_autopilot_dev"
        assert config.environments["prod"].cluster_name == "fusion_autopilot_prod"

    def test_envspec_dispatch_fields_optional(self) -> None:
        # P1.5ε — operators who only run --inline don't have to populate
        # the dispatch coords. EnvSpec parses with just the legacy fields.
        env = EnvSpec.model_validate({"workspaceKey": "wk-123"})
        assert env.ai_data_platform_id is None
        assert env.cluster_key is None
        assert env.cluster_name is None
        # Secret-name fields have defaults — they're always present.
        assert env.bicc_secret_name == "fusion_bicc_password"
        assert env.bicc_secret_key == "password"

    def test_envspec_dispatch_fields_accepted(self) -> None:
        # P1.5ε — all five new fields round-trip via their aliases.
        env = EnvSpec.model_validate(
            {
                "workspaceKey": "wk-123",
                "aiDataPlatformId": "ocid1.datalake.oc1.iad.example",
                "clusterKey": "cluster-uuid",
                "clusterName": "fusion_autopilot_dev",
                "biccSecretName": "custom_secret",
                "biccSecretKey": "secret_key",
            }
        )
        assert env.ai_data_platform_id == "ocid1.datalake.oc1.iad.example"
        assert env.cluster_key == "cluster-uuid"
        assert env.cluster_name == "fusion_autopilot_dev"
        assert env.bicc_secret_name == "custom_secret"
        assert env.bicc_secret_key == "secret_key"

    def test_envspec_rejects_unknown_field(self) -> None:
        # extra="forbid" must still hold — guards against typos like
        # ``clustreKey`` slipping through silently.
        with pytest.raises(ValidationError):
            EnvSpec.model_validate({"workspaceKey": "wk-123", "clustreKey": "typo"})


class TestDefaultsWorkspaceDir:
    """Phase 4.1 / D3 — ``Defaults.workspaceDir`` covers the server-side
    notebook upload root for cluster-mode bootstrap. Optional; absent on
    legacy configs."""

    def test_workspace_dir_default_none(self) -> None:
        # Pre-Phase-4.1 configs have no `workspaceDir` field; Defaults
        # must still load.
        defaults = Defaults.model_validate({})
        assert defaults.workspace_dir is None
        # workspace_root default unchanged from before — the new field
        # doesn't perturb existing semantics.
        assert defaults.workspace_root == "Shared"

    def test_workspace_dir_round_trips_via_alias(self) -> None:
        defaults = Defaults.model_validate(
            {"workspaceDir": "/Workspace/Shared/fusion-autopilot-bootstrap"}
        )
        assert defaults.workspace_dir == "/Workspace/Shared/fusion-autopilot-bootstrap"

    def test_workspace_dir_round_trips_via_python_name(self) -> None:
        defaults = Defaults.model_validate(
            {"workspace_dir": "/Workspace/Team/custom-bootstrap"}
        )
        assert defaults.workspace_dir == "/Workspace/Team/custom-bootstrap"

    def test_defaults_still_rejects_unknown_field(self) -> None:
        # extra="forbid" guards against typos like `worksapceDir`.
        with pytest.raises(ValidationError):
            Defaults.model_validate({"worksapceDir": "/typo/path"})
