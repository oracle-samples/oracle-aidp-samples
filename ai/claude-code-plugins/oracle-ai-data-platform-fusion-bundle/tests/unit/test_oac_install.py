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


class TestRestInstall:
    """Default mode: try REST connection POST; fall back to print-only JSON on schema-level 4xx; import workbooks via REST."""

    def test_creates_connection_via_rest_then_imports_workbooks(
        self, tmp_path: Path
    ) -> None:
        wb_dir = tmp_path / "workbooks"; wb_dir.mkdir()
        (wb_dir / "supplier_spend.dva").write_bytes(b"PK\x03\x04fake")

        with patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacOauthFlow"
        ) as oauth_cls, patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacRestClient"
        ) as client_cls:
            client_inst = client_cls.return_value
            client_inst.find_connection.return_value = None  # not yet present
            client_inst.create_connection.return_value = {"id": "conn-99"}
            client_inst.import_workbook.return_value = {"id": "wb-7"}

            params = _params(
                tmp_path,
                workbooks_dir=wb_dir,
                idcs_url="https://idcs-x.identity.oraclecloud.com",
                client_id="cid",
                client_secret="csec",
            )
            result = install(params)

        oauth_cls.assert_called_once()
        client_cls.assert_called_once()
        client_inst.create_connection.assert_called_once()
        client_inst.import_workbook.assert_called_once()
        assert result.connection_id == "conn-99"
        assert result.imported_workbooks == ["supplier_spend.dva"]
        assert result.json_template_path is None  # REST succeeded; no print-only fallback needed

    def test_falls_back_to_print_only_when_rest_post_returns_4xx(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If OAC rejects the connection POST (e.g. schema mismatch), write the
        JSON for the admin to upload via UI but continue with workbook imports."""
        from oracle_ai_data_platform_fusion_bundle.oac.rest import OacRestError
        monkeypatch.chdir(tmp_path)
        wb_dir = tmp_path / "workbooks"; wb_dir.mkdir()
        (wb_dir / "supplier_spend.dva").write_bytes(b"x")

        with patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacOauthFlow"
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacRestClient"
        ) as client_cls:
            client_inst = client_cls.return_value
            client_inst.find_connection.return_value = None
            client_inst.create_connection.side_effect = OacRestError("HTTP 400 Invalid connectionType")
            client_inst.import_workbook.return_value = {"id": "wb-1"}

            params = _params(
                tmp_path,
                workbooks_dir=wb_dir,
                idcs_url="https://idcs-x.identity.oraclecloud.com",
                client_id="cid",
                client_secret="csec",
            )
            result = install(params)

        # JSON template was written for manual UI upload
        assert result.json_template_path is not None
        assert result.json_template_path.exists()
        assert result.connection_id is None
        # Workbook import still ran
        assert result.imported_workbooks == ["supplier_spend.dva"]

    def test_skips_create_when_connection_already_present(
        self, tmp_path: Path
    ) -> None:
        with patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacOauthFlow"
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacRestClient"
        ) as client_cls:
            client_inst = client_cls.return_value
            client_inst.find_connection.return_value = {"id": "existing-1"}
            params = _params(
                tmp_path,
                idcs_url="https://idcs-x.identity.oraclecloud.com",
                client_id="cid",
                client_secret="csec",
                skip_workbooks=True,
            )
            result = install(params)
        client_inst.create_connection.assert_not_called()
        assert result.connection_id == "existing-1"

    def test_requires_idcs_creds(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="--idcs-url"):
            install(_params(tmp_path))  # no idcs_url / client_id / secret

    def test_skip_workbooks_flag(self, tmp_path: Path) -> None:
        wb_dir = tmp_path / "workbooks"; wb_dir.mkdir()
        (wb_dir / "supplier_spend.dva").write_bytes(b"x")
        with patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacOauthFlow"
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacRestClient"
        ) as client_cls:
            client_inst = client_cls.return_value
            client_inst.find_connection.return_value = None
            client_inst.create_connection.return_value = {"id": "c"}
            params = _params(
                tmp_path,
                workbooks_dir=wb_dir,
                idcs_url="https://idcs-x.identity.oraclecloud.com",
                client_id="cid",
                client_secret="csec",
                skip_workbooks=True,
            )
            install(params)
        client_inst.import_workbook.assert_not_called()

    def test_workbook_import_failure_does_not_crash_install(
        self, tmp_path: Path
    ) -> None:
        """A failing workbook import is reported; install continues / returns."""
        from oracle_ai_data_platform_fusion_bundle.oac.rest import OacRestError
        wb_dir = tmp_path / "workbooks"; wb_dir.mkdir()
        (wb_dir / "broken.dva").write_bytes(b"x")
        with patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacOauthFlow"
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacRestClient"
        ) as client_cls:
            client_inst = client_cls.return_value
            client_inst.find_connection.return_value = None
            client_inst.create_connection.return_value = {"id": "c"}
            client_inst.import_workbook.side_effect = OacRestError("boom")
            params = _params(
                tmp_path,
                workbooks_dir=wb_dir,
                idcs_url="https://idcs-x.identity.oraclecloud.com",
                client_id="cid",
                client_secret="csec",
            )
            result = install(params)
        assert result.imported_workbooks == []
