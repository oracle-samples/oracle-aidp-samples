"""Unit tests for the dashboard install flow.

The bundle ships a hybrid install design (verified TC10h, 2026-05-01):

    * Connection: always writes the 6-key JSON via ``render_template`` so the
      admin can upload through OAC's UI (3-min one-time step). The AIDP
      connectionType for OAC's REST is undocumented by Oracle.
    * Workbooks: imported via REST using Auth Code + PKCE + Refresh Token.
      Skipped if ``--skip-workbooks`` is set or if no ``oac/workbooks/*.dva``
      files exist.

Tests mock ``OacOauthFlow`` (the new auth class) and ``OacRestClient`` to
isolate from network calls.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from oracle_ai_data_platform_fusion_bundle.oac.install import InstallParams, install


def _params(tmp_path: Path, **overrides) -> InstallParams:
    pem = tmp_path / "key.pem"; pem.write_text("pem")
    defaults = {
        "oac_url": "https://oac.example.com",
        "connection_name": "aidp_fusion_jdbc",
        "region": "us-ashburn-1",
        "user_ocid": "ocid1.user.oc1..u",
        "tenancy_ocid": "ocid1.tenancy.oc1..t",
        "fingerprint": "fp",
        "idl_ocid": "ocid1.aidataplatform.oc1.iad..d",
        "cluster_key": "ck",
        "catalog": "default",
        "idcs_url": None,
        "client_id": None,
        "client_secret": None,
        "oauth_scope": "https://oac.example.comurn:opc:resource:consumer::all offline_access",
        "auth_flow": "auth_code",
        "private_key_pem_path": pem,
        "workbooks_dir": tmp_path / "workbooks",
        "print_only": False,
        "skip_workbooks": False,
    }
    defaults.update(overrides)
    return InstallParams(**defaults)


class TestPrintOnly:
    def test_writes_json_template(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """``--print-only`` writes the 6-key JSON to oac/data_source/."""
        monkeypatch.chdir(tmp_path)
        result = install(_params(tmp_path, print_only=True))
        assert result.json_template_path is not None
        assert result.json_template_path.exists()
        loaded = json.loads(result.json_template_path.read_text(encoding="utf-8"))
        assert loaded["username"] == "ocid1.user.oc1..u"
        assert loaded["idl-ocid"] == "ocid1.aidataplatform.oc1.iad..d"
        assert loaded["dsn"].startswith("jdbc:spark://gateway.aidp.us-ashburn-1.oci.oraclecloud.com")

    def test_print_only_makes_no_oauth_or_rest_calls(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``--print-only`` short-circuits before any auth or REST."""
        monkeypatch.chdir(tmp_path)
        with patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacOauthFlow"
        ) as oauth_cls, patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacRestClient"
        ) as client_cls:
            install(_params(tmp_path, print_only=True))
            oauth_cls.assert_not_called()
            client_cls.assert_not_called()


class TestHybridInstall:
    """Default mode: always write connection JSON; import workbooks via REST when present."""

    def test_writes_connection_json_then_imports_workbooks(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        wb_dir = tmp_path / "workbooks"; wb_dir.mkdir()
        (wb_dir / "supplier_spend.dva").write_bytes(b"PK\x03\x04fake")

        with patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacOauthFlow"
        ) as oauth_cls, patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacRestClient"
        ) as client_cls:
            client_inst = client_cls.return_value
            client_inst.import_workbook.return_value = {"id": "wb-7"}

            params = _params(
                tmp_path,
                workbooks_dir=wb_dir,
                idcs_url="https://idcs-x.identity.oraclecloud.com",
                client_id="cid",
                client_secret="csec",
            )
            result = install(params)

        # Connection JSON is written even in hybrid mode (admin uploads via UI)
        assert result.json_template_path is not None
        assert result.json_template_path.exists()

        # Workbook REST flow runs
        oauth_cls.assert_called_once()
        client_cls.assert_called_once()
        client_inst.import_workbook.assert_called_once()
        assert result.imported_workbooks == ["supplier_spend.dva"]

    def test_requires_idcs_creds_when_workbooks_present(self, tmp_path: Path) -> None:
        """Hybrid install needs IDCS creds when workbooks are to be imported via REST."""
        wb_dir = tmp_path / "workbooks"; wb_dir.mkdir()
        (wb_dir / "x.dva").write_bytes(b"x")
        with pytest.raises(ValueError, match="--idcs-url"):
            install(_params(tmp_path, workbooks_dir=wb_dir))  # no idcs_url / client_id / secret

    def test_skip_workbooks_short_circuits_after_json(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``--skip-workbooks`` writes connection JSON and stops — no auth, no REST."""
        monkeypatch.chdir(tmp_path)
        wb_dir = tmp_path / "workbooks"; wb_dir.mkdir()
        (wb_dir / "supplier_spend.dva").write_bytes(b"x")
        with patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacOauthFlow"
        ) as oauth_cls, patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacRestClient"
        ) as client_cls:
            params = _params(
                tmp_path,
                workbooks_dir=wb_dir,
                idcs_url="https://idcs-x.identity.oraclecloud.com",
                client_id="cid",
                client_secret="csec",
                skip_workbooks=True,
            )
            result = install(params)
        oauth_cls.assert_not_called()
        client_cls.assert_not_called()
        assert result.json_template_path is not None
        assert result.imported_workbooks == []

    def test_workbook_import_failure_does_not_crash_install(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failing workbook import is reported; install continues / returns."""
        from oracle_ai_data_platform_fusion_bundle.oac.rest import OacRestError

        monkeypatch.chdir(tmp_path)
        wb_dir = tmp_path / "workbooks"; wb_dir.mkdir()
        (wb_dir / "broken.dva").write_bytes(b"x")
        with patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacOauthFlow"
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacRestClient"
        ) as client_cls:
            client_inst = client_cls.return_value
            client_inst.import_workbook.side_effect = OacRestError("boom")
            params = _params(
                tmp_path,
                workbooks_dir=wb_dir,
                idcs_url="https://idcs-x.identity.oraclecloud.com",
                client_id="cid",
                client_secret="csec",
            )
            result = install(params)
        assert result.imported_workbooks == []  # broken.dva did not land
