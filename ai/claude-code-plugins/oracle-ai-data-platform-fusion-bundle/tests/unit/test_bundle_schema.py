"""Unit tests for the Pydantic v2 bundle.yaml schema."""

from __future__ import annotations

import pathlib

import pytest
import yaml
from pydantic import ValidationError

from oracle_ai_data_platform_fusion_bundle.schema.bundle import AidpConfig, Bundle


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
        assert bundle.api_version == "aidp-fusion-bundle/v1"
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
            "apiVersion": "aidp-fusion-bundle/v1",
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
            "apiVersion": "aidp-fusion-bundle/v1",
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
            "apiVersion": "aidp-fusion-bundle/v1",
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


class TestAidpConfigSchema:
    def test_example_parses(self) -> None:
        raw = (EXAMPLES / "aidp.config.example.yaml").read_text(encoding="utf-8")
        data = yaml.safe_load(raw)
        config = AidpConfig.model_validate(data)
        assert config.api_version == "aidp-fusion-bundle/v1"
        assert "dev" in config.environments
        assert "prod" in config.environments
        # dev uses profile mode (default), prod uses vault mode
        assert config.environments["dev"].auth.mode == "profile"
        assert config.environments["prod"].auth.mode == "vault"
