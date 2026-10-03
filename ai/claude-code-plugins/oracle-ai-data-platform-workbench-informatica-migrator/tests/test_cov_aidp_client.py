"""AIDPClient against a scripted fake ``requests`` session -- no network.

tests/test_deployer.py pins the deployer's calls against a fake *client*;
this file pins the client itself: the URL and headers each call sends, the
retry policy (which statuses, how long, and that a non-idempotent POST is
not replayed after a 500), pagination of the jobs list, the three-step PAR
upload, and OCI signer selection. ``time.sleep`` is patched out and
recorded, the PAR ``PUT`` goes to a fake, and ``oci`` is a stub module.
"""
from __future__ import annotations

import base64
import json
import sys
import types
from email.utils import format_datetime
from datetime import datetime, timedelta, timezone
from hashlib import sha256

import pytest
import requests

from infa2aidp.deployer import aidp_client as client_mod
from infa2aidp.deployer.aidp_client import AIDPClient, _OCIRequestAuth, make_signer


class FakeResponse:
    def __init__(self, status=200, body=None, headers=None, text=None):
        self.status_code = status
        self._body = body if body is not None else {}
        self.headers = {k.lower(): v for k, v in (headers or {}).items()}
        self.text = text if text is not None else json.dumps(self._body)

    @property
    def ok(self):
        return 200 <= self.status_code < 400

    def json(self):
        return self._body

    def raise_for_status(self):
        if not self.ok:
            raise requests.HTTPError(f"{self.status_code}", response=self)


class FakeSession:
    """Returns queued responses in order and records each request."""

    def __init__(self, *responses):
        self.queue = list(responses)
        self.calls: list[dict] = []
        self.auth = None

    def request(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        if not self.queue:
            raise AssertionError(f"unexpected request {method} {url}")
        return self.queue.pop(0)


@pytest.fixture
def sleeps(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr(client_mod.time, "sleep", lambda s: slept.append(s))
    return slept


def _client(*responses, ws="ws-1"):
    c = AIDPClient("us-ashburn-1", "ocid1.datalake.x", signer=object(), workspace_key=ws)
    c._session = FakeSession(*responses)
    return c


BASE = "https://aidp.us-ashburn-1.oci.oraclecloud.com/20240831/dataLakes/ocid1.datalake.x"


# ---------------------------------------------------------------------------
# Construction and simple reads
# ---------------------------------------------------------------------------

class TestBasics:
    def test_base_url_and_signing_adapter(self):
        c = AIDPClient("eu-frankfurt-1", "dl1", signer="SIG")
        assert c.base_url == "https://aidp.eu-frankfurt-1.oci.oraclecloud.com/20240831/dataLakes/dl1"
        assert isinstance(c._session.auth, _OCIRequestAuth)

    def test_list_workspaces(self, sleeps):
        c = _client(FakeResponse(200, {"items": [{"key": "w"}]}))
        assert c.list_workspaces() == [{"key": "w"}]
        assert c._session.calls[0]["url"] == BASE + "/workspaces"
        assert c._session.calls[0]["method"] == "GET"

    def test_list_clusters_uses_default_workspace(self, sleeps):
        c = _client(FakeResponse(200, {"items": [1]}))
        assert c.list_clusters() == [1]
        assert c._session.calls[0]["url"] == BASE + "/workspaces/ws-1/clusters"

    def test_list_notebooks_encodes_the_path(self, sleeps):
        c = _client(FakeResponse(200, {"content": ["nb"]}), FakeResponse(404))
        assert c.list_notebooks(path="Workspace/My Folder") == ["nb"]
        assert c._session.calls[0]["url"] == (
            BASE + "/workspaces/ws-1/notebook/api/contents/Workspace%2FMy%20Folder"
                   "?type=directory&content=1")
        assert c.list_notebooks(workspace_key="other") == []
        assert "/workspaces/other/" in c._session.calls[1]["url"]

    def test_connection_test_true_and_false(self, sleeps):
        assert _client(FakeResponse(200, {"items": []})).test_connection() is True
        assert _client(FakeResponse(401, text="NotAuthenticated")).test_connection() is False

    def test_cluster_ref_is_key_only(self):
        assert AIDPClient.cluster_ref("ck") == {"clusterKey": "ck"}


# ---------------------------------------------------------------------------
# Retry policy
# ---------------------------------------------------------------------------

class TestRetry:
    @pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
    def test_get_retries_transient_statuses(self, sleeps, status):
        c = _client(FakeResponse(status), FakeResponse(200, {"items": ["ok"]}))
        assert c.list_workspaces() == ["ok"]
        assert len(c._session.calls) == 2
        assert sleeps == [1.0]                       # 2 ** 0

    def test_backoff_is_exponential_and_gives_up_after_four_tries(self, sleeps):
        c = _client(*[FakeResponse(503, text="busy")] * 4)
        resp = c._request("GET", "/x")
        assert resp.status_code == 503
        assert len(c._session.calls) == 4
        assert sleeps == [1.0, 2.0, 4.0]

    @pytest.mark.parametrize("status", [500, 502, 504])
    def test_post_is_not_replayed_after_a_possibly_applied_5xx(self, sleeps, status):
        c = _client(FakeResponse(status, text="boom"))
        with pytest.raises(requests.HTTPError):
            c.create_job("ws-1", {"name": "j"})
        assert len(c._session.calls) == 1 and sleeps == []

    @pytest.mark.parametrize("status", [429, 503])
    def test_post_is_retried_when_the_server_did_not_act(self, sleeps, status):
        c = _client(FakeResponse(status), FakeResponse(200, {"key": "job-1"}))
        assert c.create_job(None, {"name": "j"}) == {"key": "job-1"}
        assert len(c._session.calls) == 2
        assert c._session.calls[1]["json"] == {"name": "j"}
        assert c._session.calls[1]["url"] == BASE + "/workspaces/ws-1/jobs"

    def test_non_retryable_error_returns_immediately(self, sleeps):
        c = _client(FakeResponse(400, text="bad"))
        assert c._request("GET", "/x").status_code == 400
        assert sleeps == []

    def test_retry_after_seconds_is_honoured_and_capped_at_30(self, sleeps):
        c = _client(FakeResponse(429, headers={"Retry-After": "7"}),
                    FakeResponse(429, headers={"Retry-After": "120"}),
                    FakeResponse(200))
        c._request("GET", "/x")
        assert sleeps == [7.0, 30]

    def test_retry_after_http_date(self):
        when = datetime.now(timezone.utc) + timedelta(seconds=20)
        resp = FakeResponse(429, headers={"Retry-After": format_datetime(when, usegmt=True)})
        assert 15 <= AIDPClient._retry_delay(resp, 0) <= 20

    def test_retry_after_in_the_past_or_garbage(self):
        past = FakeResponse(429, headers={"Retry-After": "Mon, 01 Jan 2001 00:00:00 GMT"})
        assert AIDPClient._retry_delay(past, 0) == 0.0
        assert AIDPClient._retry_delay(FakeResponse(429, headers={"Retry-After": "soon"}), 2) == 4.0
        assert AIDPClient._retry_delay(FakeResponse(429, headers={"Retry-After": "-5"}), 0) == 0.0


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

class TestJobs:
    def test_list_jobs_follows_every_page(self, sleeps):
        c = _client(
            FakeResponse(200, {"items": [{"name": "a"}]}, headers={"opc-next-page": "p2"}),
            FakeResponse(200, {"items": [{"name": "b"}]}, headers={"opc-next-page": "p3"}),
            FakeResponse(200, {"items": [{"name": "c"}]}),
        )
        assert [j["name"] for j in c.list_jobs()] == ["a", "b", "c"]
        params = [call["params"] for call in c._session.calls]
        assert params == [{"limit": 100}, {"limit": 100, "page": "p2"}, {"limit": 100, "page": "p3"}]

    def test_find_job_by_name_exact_match_across_pages(self, sleeps):
        c = _client(
            FakeResponse(200, {"items": [{"name": "wf_a.job", "key": 1}]}, headers={"opc-next-page": "n"}),
            FakeResponse(200, {"items": [{"name": "wf_a", "key": 2}]}),
        )
        assert c.find_job_by_name("wf_a") == {"name": "wf_a", "key": 2}
        assert _client(FakeResponse(200, {"items": []})).find_job_by_name("x") is None

    def test_update_job_is_a_full_put(self, sleeps):
        c = _client(FakeResponse(200, {"key": "k1"}))
        assert c.update_job("ws-9", "k1", {"name": "n"}) == {"key": "k1"}
        call = c._session.calls[0]
        assert (call["method"], call["url"], call["json"]) == (
            "PUT", BASE + "/workspaces/ws-9/jobs/k1", {"name": "n"})

    def test_list_jobs_error_raises(self, sleeps):
        with pytest.raises(requests.HTTPError):
            _client(FakeResponse(403, text="denied")).list_jobs()


# ---------------------------------------------------------------------------
# Workspace objects
# ---------------------------------------------------------------------------

class TestWorkspaceObjects:
    @pytest.mark.parametrize("path,rel", [
        ("/Workspace/Migrated/x.ipynb", "Migrated/x.ipynb"),
        ("/Workspace", ""),
        ("  /Shared/a  ", "Shared/a"),
        ("Migrated", "Migrated"),
    ])
    def test_workspace_relative(self, path, rel):
        assert AIDPClient.workspace_relative(path) == rel

    def test_mkdir_posts_relative_path(self, sleeps):
        c = _client(FakeResponse(200))
        c.mkdir("/Workspace/Migrated/sub")
        call = c._session.calls[0]
        assert call["url"] == BASE + "/workspaces/ws-1/actions/mkdir"
        assert call["json"] == {"path": "Migrated/sub", "description": None}

    @pytest.mark.parametrize("resp", [
        FakeResponse(409, text="conflict"),
        FakeResponse(400, text="Folder already EXISTS"),
    ])
    def test_mkdir_existing_folder_is_not_an_error(self, sleeps, resp):
        _client(resp).mkdir("/Workspace/a")

    def test_mkdir_other_failure_raises(self, sleeps):
        with pytest.raises(requests.HTTPError):
            _client(FakeResponse(400, text="invalid name")).mkdir("/Workspace/a")

    def test_mkdir_root_is_a_no_op(self, sleeps):
        c = _client()
        c.mkdir("/Workspace")
        assert c._session.calls == []

    def test_list_objects_items_root_and_missing(self, sleeps):
        c = _client(FakeResponse(200, {"items": [{"name": "a"}]}),
                    FakeResponse(200, {"items": []}),
                    FakeResponse(404))
        assert c.list_objects("/Workspace/Migrated") == [{"name": "a"}]
        assert c._session.calls[0]["params"] == {"path": "Migrated"}
        assert c.list_objects("") == []
        assert c._session.calls[1]["params"] == {"path": "/"}
        assert c.list_objects("/Workspace/missing") == []

    def test_list_objects_accepts_a_bare_list_response(self, sleeps):
        c = _client(FakeResponse(200, [{"name": "b"}]))
        assert c.list_objects("/Workspace/Migrated") == [{"name": "b"}]

    def test_object_exists_by_name_or_path(self, sleeps):
        c = _client(
            FakeResponse(200, {"items": [{"displayName": "x.ipynb"}]}),
            FakeResponse(200, {"items": [{"name": "other", "path": "/Workspace/Migrated/y.ipynb"}]}),
            FakeResponse(200, {"items": [{"name": "z.ipynb"}]}),
        )
        assert c.object_exists("/Workspace/Migrated/x.ipynb") is True
        assert c._session.calls[0]["params"] == {"path": "Migrated"}
        assert c.object_exists("/Workspace/Migrated/y.ipynb") is True
        assert c.object_exists("/Workspace/Migrated/nope.ipynb") is False


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

class TestUpload:
    def test_three_step_par_upload(self, sleeps, tmp_path, monkeypatch):
        nb = tmp_path / "nb_orders.ipynb"
        nb.write_bytes(b'{"cells": []}')
        puts = []

        def fake_put(url, data=None, headers=None):
            puts.append((url, data.read(), headers))
            return FakeResponse(200, headers={"etag": "E1"})
        monkeypatch.setattr(client_mod.requests, "put", fake_put)

        c = _client(FakeResponse(200, {"parUrl": "https://par/abc"}), FakeResponse(200))
        c.upload_file("ws-1", str(nb), "/Workspace/Migrated/nb_orders.ipynb", overwrite=False)

        create, confirm = c._session.calls
        for call in (create, confirm):
            assert call["method"] == "POST"
            assert call["url"] == BASE + "/workspaces/ws-1/actions/uploadFileMeta"
            assert call["headers"] == {"Content-Type": "application/json",
                                       "Path": "Migrated/nb_orders.ipynb", "Type": "FILE"}
            assert call["params"] == {"isOverwrite": "false", "objectDescription": "nb_orders.ipynb"}
        assert create["json"] == {"action": "CREATE"}
        assert confirm["json"] == {"action": "UPDATE", "eTag": "E1", "size": 13}
        assert puts == [("https://par/abc", b'{"cells": []}', {"Content-Length": "13"})]

    def test_missing_par_url_is_an_error(self, sleeps, tmp_path):
        nb = tmp_path / "a.ipynb"
        nb.write_text("{}", encoding="utf-8")
        with pytest.raises(ValueError, match="No PAR URL"):
            _client(FakeResponse(200, {"other": 1})).upload_file("ws", str(nb), "Migrated/a.ipynb")

    def test_upload_notebook_requires_a_workspace(self, tmp_path):
        with pytest.raises(ValueError, match="workspace_key required"):
            _client(ws="").upload_notebook(str(tmp_path / "a.ipynb"), "/Workspace/a.ipynb")

    def test_upload_notebook_delegates_with_default_workspace(self, monkeypatch):
        c = _client()
        seen = []
        monkeypatch.setattr(c, "upload_file", lambda ws, lp, rp: seen.append((ws, lp, rp)))
        c.upload_notebook("a.ipynb", "/Workspace/a.ipynb")
        assert seen == [("ws-1", "a.ipynb", "/Workspace/a.ipynb")]


# ---------------------------------------------------------------------------
# Request signing adapter
# ---------------------------------------------------------------------------

class RecordingSigner:
    def __init__(self):
        self.signed = []

    def do_request_sign(self, req):
        self.signed.append(dict(req.headers))


class TestRequestAuth:
    def _prepared(self, method, body=None, headers=None):
        return requests.Request(method, BASE + "/workspaces", data=body,
                                headers=headers or {}).prepare()

    def test_body_gets_digest_length_and_json_content_type(self):
        signer = RecordingSigner()
        req = self._prepared("POST", body='{"a": 1}')
        req.headers.pop("Content-Type", None)
        _OCIRequestAuth(signer)(req)
        h = signer.signed[0]
        assert h["x-content-sha256"] == base64.b64encode(sha256(b'{"a": 1}').digest()).decode()
        assert h["content-length"] == "8"
        assert h["content-type"] == "application/json"
        assert h["host"] == "aidp.us-ashburn-1.oci.oraclecloud.com"
        assert h["date"].endswith(" GMT")

    def test_existing_content_type_is_kept_and_bytes_body_hashed(self):
        signer = RecordingSigner()
        req = self._prepared("PUT", body=b"\x00\x01", headers={"content-type": "application/octet-stream"})
        _OCIRequestAuth(signer)(req)
        assert signer.signed[0]["content-type"] == "application/octet-stream"
        assert signer.signed[0]["content-length"] == "2"

    def test_get_without_body_is_signed_without_digest(self):
        signer = RecordingSigner()
        _OCIRequestAuth(signer)(self._prepared("GET"))
        assert "x-content-sha256" not in {k.lower() for k in signer.signed[0]}


# ---------------------------------------------------------------------------
# Signer selection and from_config
# ---------------------------------------------------------------------------

def _fake_oci(config: dict):
    oci = types.ModuleType("oci")
    signers = types.SimpleNamespace(
        get_resource_principals_signer=lambda: "RP",
        InstancePrincipalsSecurityTokenSigner=lambda: "IP",
        SecurityTokenSigner=lambda token, key: ("ST", token, key),
    )
    oci.auth = types.SimpleNamespace(signers=signers)
    oci.config = types.SimpleNamespace(from_file=lambda path, profile: dict(config, _profile=profile))
    oci.signer = types.SimpleNamespace(load_private_key_from_file=lambda p: f"KEY({p})")
    oci.Signer = lambda **kw: ("API", kw)
    return oci


class TestMakeSigner:
    @pytest.fixture(autouse=True)
    def _clean_env(self, monkeypatch):
        monkeypatch.delenv("OCI_RESOURCE_PRINCIPAL_VERSION", raising=False)
        monkeypatch.delenv("OCI_INSTANCE_PRINCIPAL", raising=False)

    def test_resource_principal_first(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "oci", _fake_oci({}))
        monkeypatch.setenv("OCI_RESOURCE_PRINCIPAL_VERSION", "2.2")
        monkeypatch.setenv("OCI_INSTANCE_PRINCIPAL", "1")
        assert make_signer() == "RP"

    def test_instance_principal_second(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "oci", _fake_oci({}))
        monkeypatch.setenv("OCI_INSTANCE_PRINCIPAL", "1")
        assert make_signer() == "IP"

    def test_session_token_profile(self, monkeypatch, tmp_path):
        tok = tmp_path / "token"
        tok.write_text("  tok-123\n", encoding="utf-8")
        monkeypatch.setitem(sys.modules, "oci", _fake_oci(
            {"security_token_file": str(tok), "key_file": "k.pem"}))
        assert make_signer("AIDP_SESSION", str(tmp_path / "cfg")) == ("ST", "tok-123", "KEY(k.pem)")

    def test_api_key_profile(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "oci", _fake_oci(
            {"tenancy": "t", "user": "u", "fingerprint": "f", "key_file": "k.pem"}))
        kind, kw = make_signer()
        assert kind == "API"
        assert kw == {"tenancy": "t", "user": "u", "fingerprint": "f",
                      "private_key_file_location": "k.pem", "pass_phrase": None}

    def test_missing_oci_package_names_it(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "oci", None)
        with pytest.raises(ImportError, match="oci package required"):
            make_signer()


class TestFromConfig:
    @pytest.fixture(autouse=True)
    def _isolate(self, monkeypatch):
        from infa2aidp import config as cfg
        for name in ("AIDP_REGION", "AIDP_INSTANCE_ID", "AIDP_WORKSPACE_KEY", "OCI_PROFILE"):
            monkeypatch.setattr(cfg, name, "", raising=False)
            monkeypatch.delenv(name, raising=False)
        self.signer_args = []
        monkeypatch.setattr(client_mod, "make_signer",
                            lambda profile, config_file: self.signer_args.append((profile, config_file)) or "S")

    def test_explicit_values(self):
        c = AIDPClient.from_config(profile="P", region="r1", instance_id="dl", workspace_key="w")
        assert (c.region, c.aidp_instance_id, c.workspace_key) == ("r1", "dl", "w")
        assert self.signer_args == [("P", "~/.oci/config")]

    def test_environment_fallback(self, monkeypatch):
        monkeypatch.setenv("AIDP_REGION", "r2")
        monkeypatch.setenv("AIDP_INSTANCE_ID", "dl2")
        monkeypatch.setenv("AIDP_WORKSPACE_KEY", "w2")
        c = AIDPClient.from_config()
        assert (c.region, c.aidp_instance_id, c.workspace_key) == ("r2", "dl2", "w2")
        assert self.signer_args == [("DEFAULT", "~/.oci/config")]

    def test_missing_region_or_instance_is_refused_before_signing(self):
        with pytest.raises(RuntimeError, match="AIDP_REGION and AIDP_INSTANCE_ID"):
            AIDPClient.from_config(region="r1")
        assert self.signer_args == []
