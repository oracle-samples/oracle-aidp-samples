"""Unit tests for the dashboard install flow.

The bundle ships a hybrid install design (TC10h-2 refactor, 2026-05-01) that uses
ONLY Oracle-documented public REST endpoints:

    1. POST /catalog/connections                     (create AIDP connection)
    2. POST /snapshots                               (register customer-uploaded .bar)
    3. POST /system/actions/restoreSnapshot          (async restore)
    4. GET  /workRequests/{id}                       (poll until SUCCEEDED)

There is NO public REST endpoint for workbook .dva imports — that path was
removed in this refactor.
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
        "prompt_login": False,
        "private_key_pem_path": pem,
        "bar_bucket": None,
        "bar_uri": None,
        "bar_password": None,
        "snapshot_name": "aidp-fusion-bundle",
        "print_only": False,
        "skip_workbooks": False,
        "overwrite_connection": False,
    }
    defaults.update(overrides)
    return InstallParams(**defaults)


class TestPrintOnly:
    def test_writes_json_template(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
    """Connection POST + snapshot register/restore against fully-specified params."""

    def test_creates_connection_and_restores_snapshot(self, tmp_path: Path) -> None:
        with patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacOauthFlow"
        ) as oauth_cls, patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacRestClient"
        ) as client_cls:
            client_inst = client_cls.return_value
            client_inst.find_connection.return_value = None
            client_inst.create_connection.return_value = {"connectionId": "conn-99"}
            client_inst.register_snapshot.return_value = {"id": "snap-7"}
            client_inst.restore_snapshot.return_value = "wr-42"
            client_inst.poll_work_request.return_value = {"status": "SUCCEEDED"}

            params = _params(
                tmp_path,
                idcs_url="https://idcs-x.identity.oraclecloud.com",
                client_id="cid",
                client_secret="csec",
                bar_bucket="customer-bucket",
                bar_uri="bundles/v1.bar",
                bar_password="hunter2",
            )
            result = install(params)

        oauth_cls.assert_called_once()
        client_cls.assert_called_once()
        client_inst.create_connection.assert_called_once()
        client_inst.register_snapshot.assert_called_once()
        client_inst.restore_snapshot.assert_called_once_with("snap-7", password="hunter2")
        client_inst.poll_work_request.assert_called_once()
        assert result.connection_id == "conn-99"
        assert result.snapshot_id == "snap-7"
        assert result.work_request_id == "wr-42"
        assert result.work_request_status == "SUCCEEDED"

    def test_skips_create_when_connection_already_present(self, tmp_path: Path) -> None:
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

    def test_overwrite_connection_deletes_then_creates(self, tmp_path: Path) -> None:
        with patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacOauthFlow"
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacRestClient"
        ) as client_cls:
            client_inst = client_cls.return_value
            client_inst.find_connection.return_value = {"id": "old", "owner": "admin"}
            client_inst.create_connection.return_value = {"connectionId": "new"}
            params = _params(
                tmp_path,
                idcs_url="https://idcs-x.identity.oraclecloud.com",
                client_id="cid",
                client_secret="csec",
                skip_workbooks=True,
                overwrite_connection=True,
            )
            result = install(params)
        client_inst.delete_connection.assert_called_once()
        client_inst.create_connection.assert_called_once()
        assert result.connection_id == "new"

    def test_requires_idcs_creds(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="--idcs-url"):
            install(_params(tmp_path))  # no idcs_url / client_id / secret

    def test_skip_workbooks_skips_snapshot_phase(self, tmp_path: Path) -> None:
        with patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacOauthFlow"
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacRestClient"
        ) as client_cls:
            client_inst = client_cls.return_value
            client_inst.find_connection.return_value = None
            client_inst.create_connection.return_value = {"connectionId": "c"}
            params = _params(
                tmp_path,
                idcs_url="https://idcs-x.identity.oraclecloud.com",
                client_id="cid",
                client_secret="csec",
                bar_bucket="b",
                bar_uri="x.bar",
                skip_workbooks=True,
            )
            install(params)
        client_inst.register_snapshot.assert_not_called()
        client_inst.restore_snapshot.assert_not_called()

    def test_no_bar_args_skips_snapshot_with_warning(self, tmp_path: Path) -> None:
        """If --bar-bucket/--bar-uri aren't set, we install the connection only."""
        with patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacOauthFlow"
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacRestClient"
        ) as client_cls:
            client_inst = client_cls.return_value
            client_inst.find_connection.return_value = None
            client_inst.create_connection.return_value = {"connectionId": "c"}
            params = _params(
                tmp_path,
                idcs_url="https://idcs-x.identity.oraclecloud.com",
                client_id="cid",
                client_secret="csec",
            )
            result = install(params)
        client_inst.register_snapshot.assert_not_called()
        assert result.connection_id == "c"
        assert result.snapshot_id is None

    def test_failed_work_request_records_status(self, tmp_path: Path) -> None:
        with patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacOauthFlow"
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.oac.install.OacRestClient"
        ) as client_cls:
            client_inst = client_cls.return_value
            client_inst.find_connection.return_value = None
            client_inst.create_connection.return_value = {"connectionId": "c"}
            client_inst.register_snapshot.return_value = {"id": "s"}
            client_inst.restore_snapshot.return_value = "wr"
            client_inst.poll_work_request.return_value = {"status": "FAILED", "error": "boom"}

            params = _params(
                tmp_path,
                idcs_url="https://idcs-x.identity.oraclecloud.com",
                client_id="cid",
                client_secret="csec",
                bar_bucket="b",
                bar_uri="x.bar",
            )
            result = install(params)
        assert result.work_request_status == "FAILED"
