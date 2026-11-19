"""Unit tests for OacRestClient (TC10h-2 refactor: snapshot-based, public-API only)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_autopilot.oac.rest.client import (
    OacRestClient,
    OacRestError,
    WorkRequestStatus,
    encode_catalog_id,
)
from oracle_ai_data_platform_fusion_autopilot.oac.rest.connection import build_payload


def _fetcher(token: str = "tok") -> MagicMock:
    f = MagicMock()
    f.get_token.return_value = token
    return f


# ----------------------------------------------------------------- helpers
class TestEncodeCatalogId:
    def test_oracle_doc_example(self) -> None:
        """Doc-cited example: 'admin'.'oracle_ailakehouse_walletless' -> known base64url."""
        plain = "'admin'.'oracle_ailakehouse_walletless'"
        encoded = encode_catalog_id(plain)
        # Base64URL has no padding and uses -_ instead of +/
        assert "=" not in encoded
        assert "+" not in encoded
        assert "/" not in encoded
        # And it's reversible
        import base64
        padding = "=" * (-len(encoded) % 4)
        assert base64.urlsafe_b64decode(encoded + padding).decode() == plain


# ------------------------------------------------------------ connections
class TestListConnections:
    def test_returns_list_directly(self) -> None:
        s = MagicMock()
        resp = MagicMock(status_code=200)
        resp.json.return_value = [{"name": "a"}, {"name": "b"}]
        s.request.return_value = resp
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        out = client.list_connections()
        assert [c["name"] for c in out] == ["a", "b"]

        # Verify URL + params
        call = s.request.call_args
        assert call.kwargs["method"] == "GET"
        # Per Oracle's openapi.json, list connections is `/catalog?type=connections`
        # NOT `/catalog/connections` (which is POST-only).
        assert call.kwargs["url"] == "https://oac.example.com/api/20210901/catalog"
        assert call.kwargs["params"]["type"] == "connections"
        # search defaults to "*" (otherwise OAC returns a TypeInfo header, not items).
        assert call.kwargs["params"]["search"] == "*"
        assert call.kwargs["headers"]["Authorization"] == "Bearer tok"

    def test_search_query_param(self) -> None:
        s = MagicMock()
        s.request.return_value = MagicMock(status_code=200, json=lambda: [])
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        client.list_connections(search="aidp_fusion")
        params = s.request.call_args.kwargs["params"]
        assert params == {"type": "connections", "search": "aidp_fusion"}

    def test_default_search_is_wildcard(self) -> None:
        """Without an explicit search, helper passes ``search=*`` (live OAC requires this)."""
        s = MagicMock()
        s.request.return_value = MagicMock(status_code=200, json=lambda: [])
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        client.list_connections()
        assert s.request.call_args.kwargs["params"]["search"] == "*"

    def test_dict_with_items_key(self) -> None:
        s = MagicMock()
        resp = MagicMock(status_code=200)
        resp.json.return_value = {"items": [{"name": "x"}]}
        s.request.return_value = resp
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        assert [c["name"] for c in client.list_connections()] == ["x"]

    def test_drops_typeinfo_header_rows(self) -> None:
        """OAC sometimes mixes a ``[{"type": "connections"}]`` header with no name —
        drop those rows so callers see real records only."""
        s = MagicMock()
        resp = MagicMock(status_code=200)
        resp.json.return_value = [
            {"type": "connections"},  # TypeInfo header, no name
            {"name": "real_conn", "id": "abc", "type": "connections"},
        ]
        s.request.return_value = resp
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        out = client.list_connections()
        assert len(out) == 1
        assert out[0]["name"] == "real_conn"

    def test_raises_on_non_200(self) -> None:
        s = MagicMock()
        s.request.return_value = MagicMock(status_code=500, text="oops")
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        with pytest.raises(OacRestError, match="HTTP 500"):
            client.list_connections()


class TestFindConnection:
    def test_match_by_name(self) -> None:
        s = MagicMock()
        s.request.return_value = MagicMock(
            status_code=200,
            json=lambda: [{"name": "aidp_fusion_jdbc", "id": "abc"}, {"name": "other", "id": "def"}],
        )
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        out = client.find_connection("aidp_fusion_jdbc")
        assert out is not None
        assert out["id"] == "abc"

    def test_passes_name_as_search_term(self) -> None:
        """find_connection narrows server-side via search=<name>."""
        s = MagicMock()
        s.request.return_value = MagicMock(status_code=200, json=lambda: [])
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        client.find_connection("aidp_fusion_jdbc")
        assert s.request.call_args.kwargs["params"]["search"] == "aidp_fusion_jdbc"

    def test_substring_search_does_not_false_match(self) -> None:
        """OAC's search is substring; helper enforces exact-match client-side."""
        s = MagicMock()
        # Server returns multiple substring hits — only the exact name should be picked.
        s.request.return_value = MagicMock(
            status_code=200,
            json=lambda: [
                {"name": "aidp_fusion_jdbc_v2", "id": "wrong"},
                {"name": "other_aidp_fusion_jdbc_clone", "id": "also_wrong"},
            ],
        )
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        assert client.find_connection("aidp_fusion_jdbc") is None

    def test_no_match(self) -> None:
        s = MagicMock()
        s.request.return_value = MagicMock(status_code=200, json=lambda: [])
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        assert client.find_connection("missing") is None


class TestCreateConnection:
    def test_posts_envelope_with_idljdbc_discriminator(self, tmp_path: Path) -> None:
        pem = tmp_path / "key.pem"
        pem.write_bytes(b"-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----\n")
        s = MagicMock()
        resp = MagicMock(status_code=201, text='{"connectionId":"abc123"}')
        resp.json.return_value = {"connectionId": "abc123"}
        s.request.return_value = resp

        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        payload = build_payload(
            user_ocid="u", tenancy_ocid="t", region="us-ashburn-1",
            fingerprint="fp", idl_ocid="idl", cluster_key="ck",
        )
        out = client.create_connection(
            name="aidp_fusion_jdbc",
            payload=payload,
            private_key_pem_path=pem,
            description="bundle install",
        )
        assert out["connectionId"] == "abc123"

        call = s.request.call_args
        assert call.kwargs["method"] == "POST"
        assert call.kwargs["url"] == "https://oac.example.com/api/20210901/catalog/connections"

        envelope = call.kwargs["json"]
        assert envelope["version"] == "2.0.0"
        assert envelope["type"] == "connection"
        assert envelope["name"] == "aidp_fusion_jdbc"
        assert envelope["description"] == "bundle install"

        cp = envelope["content"]["connectionParams"]
        # OAC's discriminator for AIDP is "idljdbc" (from UI capture, TC10h)
        assert cp["connectionType"] == "idljdbc"
        assert cp["provider-name"] == "idljdbc"
        # Field-name traps:
        assert cp["username"] == "u"
        assert cp["idlocid"] == "idl"          # NOT "idl-ocid"
        assert cp["auth-type"] == "APIKey"
        assert cp["catalog"] == "fusion_catalog"
        # PEM is inlined
        assert cp["private-key"].startswith("-----BEGIN PRIVATE KEY-----")
        assert cp["private-key"].endswith("-----END PRIVATE KEY-----")

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
        # Pass a base64url-shaped id directly
        assert client.delete_connection("J2FkbWluJy4nYWlkcF9mdXNpb25famRiYyc") is True

    def test_404_returns_false(self) -> None:
        s = MagicMock()
        s.request.return_value = MagicMock(status_code=404)
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        assert client.delete_connection("J2FkbWluJy4nbWlzc2luZyc") is False

    def test_500_raises(self) -> None:
        s = MagicMock()
        s.request.return_value = MagicMock(status_code=500, text="oops")
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        with pytest.raises(OacRestError):
            client.delete_connection("anything")

    def test_encodes_owner_dot_name_when_owner_provided(self) -> None:
        """delete_connection(name, owner='admin') should encode 'admin'.'name' as base64url."""
        s = MagicMock()
        s.request.return_value = MagicMock(status_code=204)
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        client.delete_connection("aidp_fusion_jdbc", owner="admin")
        url = s.request.call_args.kwargs["url"]
        # The path should contain the base64url of "'admin'.'aidp_fusion_jdbc'"
        expected_id = encode_catalog_id("'admin'.'aidp_fusion_jdbc'")
        assert url.endswith(f"/catalog/connections/{expected_id}")


# -------------------------------------------------------------- snapshots
class TestRegisterSnapshot:
    def test_register_via_oci_object_storage(self) -> None:
        """POST body shape — uses wait=False to skip the async work-request poll."""
        s = MagicMock()
        resp = MagicMock(status_code=202, text='{"workRequestId":"wr-1"}')
        resp.json.return_value = {"workRequestId": "wr-1"}
        s.request.return_value = resp
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)

        out = client.register_snapshot(
            name="fusion-autopilot",
            bucket="customer-bucket",
            bar_uri="bundles/fusion-v1.bar",
            password="hunter2",
            wait=False,
        )
        assert out == {"workRequestId": "wr-1"}

        call = s.request.call_args
        assert call.kwargs["method"] == "POST"
        assert call.kwargs["url"] == "https://oac.example.com/api/20210901/snapshots"

        body = call.kwargs["json"]
        assert body["type"] == "REGISTER"
        assert body["name"] == "fusion-autopilot"
        assert body["storage"]["type"] == "OCI_NATIVE"
        assert body["storage"]["bucket"] == "customer-bucket"
        assert body["storage"]["auth"]["type"] == "OCI_RESOURCE_PRINCIPAL"
        assert body["bar"]["uri"] == "bundles/fusion-v1.bar"
        assert body["password"] == "hunter2"

    def test_no_password_when_none(self) -> None:
        s = MagicMock()
        s.request.return_value = MagicMock(status_code=202, text='{"workRequestId":"wr-x"}', json=lambda: {"workRequestId": "wr-x"})
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        client.register_snapshot(name="x", bucket="b", bar_uri="b.bar", wait=False)
        body = s.request.call_args.kwargs["json"]
        assert "password" not in body

    def test_4xx_raises(self) -> None:
        s = MagicMock()
        s.request.return_value = MagicMock(status_code=400, text="bad")
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        with pytest.raises(OacRestError, match="HTTP 400"):
            client.register_snapshot(name="x", bucket="b", bar_uri="b.bar", wait=False)

    def test_wait_true_polls_then_returns_snapshot_record(self) -> None:
        """When wait=True (default): POST → poll workRequest → look up snapshot by name."""
        s = MagicMock()
        post_resp = MagicMock(status_code=202, text='{"workRequestId":"wr-1"}', headers={})
        post_resp.json.return_value = {"workRequestId": "wr-1"}

        wr_resp = MagicMock(status_code=200, text='{"id":"wr-1","status":"SUCCEEDED"}')
        wr_resp.json.return_value = {"id": "wr-1", "status": "SUCCEEDED", "resources": []}

        list_resp = MagicMock(status_code=200, text='[]')
        list_resp.json.return_value = [
            {"id": "snap-1", "name": "fusion-autopilot"},
            {"id": "snap-2", "name": "other"},
        ]

        s.request.side_effect = [post_resp, wr_resp, list_resp]
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)

        out = client.register_snapshot(
            name="fusion-autopilot",
            bucket="b",
            bar_uri="b.bar",
            poll_interval=0,
        )
        assert out == {"id": "snap-1", "name": "fusion-autopilot"}

    def test_wait_true_uses_workrequest_resources_id_when_present(self) -> None:
        """If the work-request payload exposes the snapshot id directly, fetch by id."""
        s = MagicMock()
        post_resp = MagicMock(status_code=202, text='{"workRequestId":"wr-2"}', headers={})
        post_resp.json.return_value = {"workRequestId": "wr-2"}

        wr_resp = MagicMock(status_code=200, text='{}')
        wr_resp.json.return_value = {
            "id": "wr-2",
            "status": "SUCCEEDED",
            "resources": [{"entityType": "snapshot", "identifier": "snap-77"}],
        }

        get_resp = MagicMock(status_code=200, text='{}')
        get_resp.json.return_value = {"id": "snap-77", "name": "fusion-autopilot"}

        s.request.side_effect = [post_resp, wr_resp, get_resp]
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)

        out = client.register_snapshot(
            name="fusion-autopilot",
            bucket="b",
            bar_uri="b.bar",
            poll_interval=0,
        )
        assert out == {"id": "snap-77", "name": "fusion-autopilot"}

    def test_wait_true_raises_if_workrequest_failed(self) -> None:
        s = MagicMock()
        post_resp = MagicMock(status_code=202, text='{"workRequestId":"wr-x"}', headers={})
        post_resp.json.return_value = {"workRequestId": "wr-x"}

        wr_resp = MagicMock(status_code=200, text='{}')
        wr_resp.json.return_value = {"id": "wr-x", "status": "FAILED"}

        s.request.side_effect = [post_resp, wr_resp]
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)

        with pytest.raises(OacRestError, match="terminated as FAILED"):
            client.register_snapshot(
                name="fusion-autopilot", bucket="b", bar_uri="b.bar", poll_interval=0,
            )


class TestRestoreSnapshot:
    def test_returns_work_request_id_from_header(self) -> None:
        s = MagicMock()
        resp = MagicMock(status_code=202, text="", headers={"oa-work-request-id": "wr-99"})
        s.request.return_value = resp
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        wr = client.restore_snapshot("snap-1", password="hunter2")
        assert wr == "wr-99"

        body = s.request.call_args.kwargs["json"]
        assert body["snapshot"]["id"] == "snap-1"
        assert body["snapshot"]["password"] == "hunter2"
        url = s.request.call_args.kwargs["url"]
        assert url.endswith("/system/actions/restoreSnapshot")

    def test_falls_back_to_location_header(self) -> None:
        s = MagicMock()
        resp = MagicMock(status_code=202, text="", headers={"Location": "/api/20210901/workRequests/wr-loc"})
        s.request.return_value = resp
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        assert client.restore_snapshot("snap-1") == "wr-loc"

    def test_raises_when_no_work_request_id(self) -> None:
        s = MagicMock()
        s.request.return_value = MagicMock(status_code=202, text="", headers={})
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        with pytest.raises(RuntimeError, match="oa-work-request-id"):
            client.restore_snapshot("snap-1")


class TestPollWorkRequest:
    def test_returns_when_succeeded(self) -> None:
        s = MagicMock()
        responses = [
            MagicMock(status_code=200, json=lambda: {"status": "IN_PROGRESS"}),
            MagicMock(status_code=200, json=lambda: {"status": "SUCCEEDED"}),
        ]
        s.request.side_effect = responses
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        out = client.poll_work_request("wr-1", timeout=10, poll_interval=0)
        assert out["status"] == WorkRequestStatus.SUCCEEDED

    def test_returns_when_failed(self) -> None:
        s = MagicMock()
        s.request.return_value = MagicMock(status_code=200, json=lambda: {"status": "FAILED", "error": "boom"})
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        out = client.poll_work_request("wr-1", timeout=10, poll_interval=0)
        assert out["status"] == "FAILED"
        assert out["error"] == "boom"

    def test_timeout_raises(self) -> None:
        s = MagicMock()
        s.request.return_value = MagicMock(status_code=200, json=lambda: {"status": "IN_PROGRESS"})
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        with pytest.raises(TimeoutError):
            client.poll_work_request("wr-1", timeout=0, poll_interval=0)


class TestDeleteSnapshot:
    def test_204_returns_true(self) -> None:
        s = MagicMock()
        s.request.return_value = MagicMock(status_code=204)
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        assert client.delete_snapshot("snap-1") is True

    def test_404_returns_false(self) -> None:
        s = MagicMock()
        s.request.return_value = MagicMock(status_code=404)
        client = OacRestClient("https://oac.example.com", _fetcher(), session=s)
        assert client.delete_snapshot("missing") is False


class TestTokenRetryOn401:
    def test_refreshes_token_once_on_401(self, tmp_path: Path) -> None:
        s = MagicMock()
        first = MagicMock(status_code=401, text="expired")
        second = MagicMock(status_code=200, json=lambda: [])
        s.request.side_effect = [first, second]
        fetcher = _fetcher()
        client = OacRestClient("https://oac.example.com", fetcher, session=s)
        result = client.list_connections()
        assert result == []
        # Second call should be made with force_refresh=True
        assert fetcher.get_token.call_count == 2
        assert fetcher.get_token.call_args_list[1].kwargs["force_refresh"] is True
