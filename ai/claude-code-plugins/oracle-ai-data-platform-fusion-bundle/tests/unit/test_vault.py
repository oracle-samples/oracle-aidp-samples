"""Unit tests for the OCI Vault helper."""

from __future__ import annotations

import pytest

from oracle_ai_data_platform_fusion_bundle.utils import vault


class TestVaultRefMatching:
    def test_is_vault_ref_recognises_format(self) -> None:
        assert vault.is_vault_ref("${vault:ocid1.vaultsecret.oc1.iad.aaa}") is True

    def test_plain_string_not_a_ref(self) -> None:
        assert vault.is_vault_ref("plain-secret") is False

    def test_empty_not_a_ref(self) -> None:
        assert vault.is_vault_ref("") is False

    def test_parse_ocid_round_trip(self) -> None:
        assert vault.parse_ocid("${vault:ocid1.vaultsecret.oc1.iad.xyz}") == \
            "ocid1.vaultsecret.oc1.iad.xyz"


class TestMockMode:
    def test_resolves_via_mock_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(
            "AIDP_FUSION_BUNDLE_MOCK_SECRETS",
            "ocid1.vaultsecret.oc1.iad.alpha=hello;ocid1.vaultsecret.oc1.iad.beta=world",
        )
        assert vault.resolve("${vault:ocid1.vaultsecret.oc1.iad.alpha}") == "hello"
        assert vault.resolve("${vault:ocid1.vaultsecret.oc1.iad.beta}") == "world"

    def test_passthrough_for_plain_string(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AIDP_FUSION_BUNDLE_MOCK_SECRETS", "ocid1.vaultsecret.oc1.iad.x=v")
        assert vault.resolve("plain-secret") == "plain-secret"

    def test_unknown_ocid_in_mock_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AIDP_FUSION_BUNDLE_MOCK_SECRETS", "ocid1.vaultsecret.oc1.iad.x=v")
        with pytest.raises(KeyError):
            vault.resolve("${vault:ocid1.vaultsecret.oc1.iad.unknown}")
