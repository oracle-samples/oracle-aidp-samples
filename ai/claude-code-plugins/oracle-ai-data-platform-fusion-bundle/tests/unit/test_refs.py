"""Unit tests for the variable + Vault-reference resolver."""

from __future__ import annotations

import pytest

from oracle_ai_data_platform_fusion_bundle.schema.refs import (
    VaultRef,
    find_vault_refs,
    render_vars,
    replace_vault_refs,
)


class TestRenderVars:
    def test_renders_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("FUSION_BICC_USER", "Casey.Brown")
        assert render_vars("user=${FUSION_BICC_USER}") == "user=Casey.Brown"

    def test_extra_overrides_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("env", "prod")
        assert render_vars("path=Shared/${env}", extra={"env": "dev"}) == "path=Shared/dev"

    def test_leaves_vault_refs_untouched(self) -> None:
        s = "pwd=${vault:ocid1.vaultsecret.oc1.iad.aaa}"
        assert render_vars(s) == s

    def test_unresolved_var_raises(self) -> None:
        with pytest.raises(KeyError, match="DOES_NOT_EXIST_DEFINITELY_NOT"):
            render_vars("v=${DOES_NOT_EXIST_DEFINITELY_NOT}")

    def test_multiple_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("A", "1")
        monkeypatch.setenv("B", "2")
        assert render_vars("${A}-${B}") == "1-2"


class TestFindVaultRefs:
    def test_finds_single_ref(self) -> None:
        s = "pwd=${vault:ocid1.vaultsecret.oc1.iad.aaa}"
        refs = list(find_vault_refs(s))
        assert refs == [VaultRef(ocid="ocid1.vaultsecret.oc1.iad.aaa")]

    def test_finds_multiple_refs(self) -> None:
        s = "${vault:ocid1.vaultsecret.oc1.iad.aaa} and ${vault:ocid1.vaultsecret.oc1.iad.bbb}"
        refs = list(find_vault_refs(s))
        assert {r.ocid for r in refs} == {
            "ocid1.vaultsecret.oc1.iad.aaa",
            "ocid1.vaultsecret.oc1.iad.bbb",
        }

    def test_no_refs(self) -> None:
        assert list(find_vault_refs("plain string")) == []

    def test_ignores_non_vaultsecret_ocid(self) -> None:
        # Only ocid1.vaultsecret.* matches (other OCID resource types are not Vault refs)
        s = "user=${vault:ocid1.user.oc1..aaa}"
        assert list(find_vault_refs(s)) == []


class TestReplaceVaultRefs:
    def test_replaces_one_ref(self) -> None:
        s = "pwd=${vault:ocid1.vaultsecret.oc1.iad.aaa}"
        out = replace_vault_refs(s, {"ocid1.vaultsecret.oc1.iad.aaa": "secret"})
        assert out == "pwd=secret"

    def test_missing_ref_raises(self) -> None:
        with pytest.raises(KeyError):
            replace_vault_refs("${vault:ocid1.vaultsecret.oc1.iad.aaa}", {})

    def test_preserves_other_text(self) -> None:
        s = "user=Casey.Brown pwd=${vault:ocid1.vaultsecret.oc1.iad.aaa} other=stuff"
        out = replace_vault_refs(s, {"ocid1.vaultsecret.oc1.iad.aaa": "S3cret!"})
        assert out == "user=Casey.Brown pwd=S3cret! other=stuff"
