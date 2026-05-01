"""Unit tests for OacRestClient."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_bundle.oac.rest.client import OacRestClient, OacRestError
from oracle_ai_data_platform_fusion_bundle.oac.rest.connection import build_payload


def _fetcher(token: str = "tok") -> MagicMock:
    f = MagicMock()
    f.get_token.return_value = token
    return f


class TestListConnections:
    def test_returns_list_directly(self) -> None:
        s = MagicMock()
        resp = MagicMock(status_code=200)
        resp.json.return_value = [{"name": "a"}, {"name": "b"}]
        s.request.return_value = resp
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        out = client.list_connections()
        assert [c["name"] for c in out] == ["a", "b"]
        # Verify URL + headers
        call = s.request.call_args
        assert call.kwargs["method"] == "GET"
        assert call.kwargs["url"] == "https://oac.example.com/api/20210901/catalog/connections"
        assert call.kwargs["headers"]["Authorization"] == "Bearer tok"

    def test_dict_with_items_key(self) -> None:
        s = MagicMock()
        resp = MagicMock(status_code=200)
        resp.json.return_value = {"items": [{"name": "x"}]}
        s.request.return_value = resp
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        assert [c["name"] for c in client.list_connections()] == ["x"]

    def test_raises_on_non_200(self) -> None:
        s = MagicMock()
        s.request.return_value = MagicMock(status_code=500, text="oops")
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        with pytest.raises(OacRestError, match="HTTP 500"):
            client.list_connections()


class TestFindConnection:
    def test_match_by_name(self) -> None:
        s = MagicMock()
        resp = MagicMock(status_code=200)
        resp.json.return_value = [{"name": "aidp_fusion_jdbc"}, {"name": "other"}]
        s.request.return_value = resp
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        match = client.find_connection("aidp_fusion_jdbc")
        assert match is not None
        assert match["name"] == "aidp_fusion_jdbc"

    def test_returns_none_when_missing(self) -> None:
        s = MagicMock()
        resp = MagicMock(status_code=200)
        resp.json.return_value = [{"name": "other"}]
        s.request.return_value = resp
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        assert client.find_connection("aidp_fusion_jdbc") is None


class TestCreateConnection:
    def test_posts_multipart_with_pem(self, tmp_path: Path) -> None:
        pem = tmp_path / "key.pem"
        pem.write_bytes(b"-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----\n")
        s = MagicMock()
        resp = MagicMock(status_code=201)
        resp.json.return_value = {"id": "conn-42", "name": "aidp_fusion_jdbc"}
        s.request.return_value = resp

        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        payload = build_payload(
            user_ocid="u", tenancy_ocid="t", region="us-ashburn-1",
            fingerprint="fp", idl_ocid="idl", cluster_key="ck",
        )
        result = client.create_connection(
            name="aidp_fusion_jdbc",
            payload=payload,
            private_key_pem_path=pem,
            description="bundle install",
        )
        assert result["id"] == "conn-42"

        call = s.request.call_args
        assert call.kwargs["method"] == "POST"
        assert call.kwargs["url"] == "https://oac.example.com/api/20210901/catalog/connections"
        files = call.kwargs["files"]
        # New schema (verified live TC10h, 2026-05-01): documented field names
        # are ``connectionParams`` (JSON envelope) and ``cert`` (PEM file).
        assert "connectionParams" in files
        assert "cert" in files
        assert files["connectionParams"][2] == "text/plain"
        assert files["cert"][2] == "text/plain"

        import json as _json
        envelope = _json.loads(files["connectionParams"][1])
        assert envelope["version"] == "2.0.0"
        assert envelope["type"] == "connection"
        assert envelope["name"] == "aidp_fusion_jdbc"
        # The 6-key AIDP payload lives under content.connectionParams alongside
        # the connectionType discriminator.
        cp = envelope["content"]["connectionParams"]
        assert cp["connectionType"] == "oracle-ai-data-platform"
        assert cp["username"] == "u"
        assert cp["idl-ocid"] == "idl"
        assert envelope["description"] == "bundle install"

    def test_raises_when_pem_missing(self, tmp_path: Path) -> None:
        client = OacRestClient("https://oac.example.com", _fetcher(), session=MagicMock())
        payload = build_payload(
            user_ocid="u", tenancy_ocid="t", region="us-ashburn-1",
            fingerprint="fp", idl_ocid="idl", cluster_key="ck",
        )
        with pytest.raises(FileNotFoundError):
            client.create_connection(
                name="x", payload=payload, private_key_pem_path=tmp_path / "absent.pem",
            )

    def test_raises_on_4xx(self, tmp_path: Path) -> None:
        pem = tmp_path / "k.pem"; pem.write_text("pem")
        s = MagicMock()
        s.request.return_value = MagicMock(status_code=409, text='{"error":"already exists"}')
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        payload = build_payload(
            user_ocid="u", tenancy_ocid="t", region="us-ashburn-1",
            fingerprint="fp", idl_ocid="idl", cluster_key="ck",
        )
        with pytest.raises(OacRestError, match="HTTP 409"):
            client.create_connection(name="x", payload=payload, private_key_pem_path=pem)


class TestDeleteConnection:
    def test_204_returns_true(self) -> None:
        s = MagicMock()
        s.request.return_value = MagicMock(status_code=204)
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        assert client.delete_connection("aidp_fusion_jdbc") is True

    def test_404_returns_false(self) -> None:
        s = MagicMock()
        s.request.return_value = MagicMock(status_code=404)
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        assert client.delete_connection("missing") is False

    def test_500_raises(self) -> None:
        s = MagicMock()
        s.request.return_value = MagicMock(status_code=500, text="oops")
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        with pytest.raises(OacRestError):
            client.delete_connection("x")


class TestImportWorkbook:
    def test_uploads_dva_multipart(self, tmp_path: Path) -> None:
        dva = tmp_path / "wb.dva"; dva.write_bytes(b"\x50\x4b\x03\x04fake")
        s = MagicMock()
        resp = MagicMock(status_code=201, text='{"id":"wb-1"}')
        resp.json.return_value = {"id": "wb-1"}
        s.request.return_value = resp
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        out = client.import_workbook(dva, target_folder="/users/me")
        assert out["id"] == "wb-1"
        call = s.request.call_args
        assert call.kwargs["url"] == "https://oac.example.com/api/20210901/catalog/workbooks/imports"
        assert call.kwargs["data"]["folder"] == "/users/me"
        assert "file" in call.kwargs["files"]


class TestTokenRetryOn401:
    def test_refreshes_token_once_on_401(self, tmp_path: Path) -> None:
        s = MagicMock()
        first = MagicMock(status_code=401, text="expired")
        second = MagicMock(status_code=200)
        second.json.return_value = []
        s.request.side_effect = [first, second]
        fetcher = _fetcher()
        client = OacRestClient("https://oac.example.com", fetcher, session=s)
        result = client.list_connections()
        assert result == []
        # token fetched twice — once initial, once force-refresh
        assert fetcher.get_token.call_count == 2
        assert fetcher.get_token.call_args_list[1].kwargs == {"force_refresh": True}
