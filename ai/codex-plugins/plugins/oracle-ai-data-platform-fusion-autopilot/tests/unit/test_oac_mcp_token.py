"""Unit tests for the OAC MCP connector token bridge (oac.mcp_token).

Pure functions only — no network, no browser. Covers the IDCS→connector format
conversion, the OAC-download normalization, and the .mcp.json wiring contract.
"""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import pytest
from oracle_ai_data_platform_fusion_autopilot.oac import mcp_token as mcp_token_mod
from oracle_ai_data_platform_fusion_autopilot.oac.mcp_token import (
    PORTABLE_CONNECTOR_ARG,
    build_basic_auth_mcp_config,
    build_connector_config,
    bundle_to_connector_payload,
    import_connector_token,
    normalize_downloaded_tokens,
    setup_basic_auth,
    stage_connector,
    wire_mcp_json_file,
    wire_token_into_mcp_config,
    write_basic_auth_mcp_json,
    write_connector_config_file,
    write_connector_token_file,
)
from oracle_ai_data_platform_fusion_autopilot.oac.rest.oauth import TokenBundle


class TestBundleToConnectorPayload:
    def test_camelcase_and_relative_expiry(self) -> None:
        b = TokenBundle(access_token="eyJ.a.b", refresh_token="rt-1", expires_at=1000.0)
        payload = bundle_to_connector_payload(b, now=100.0)
        assert payload == {"accessToken": "eyJ.a.b", "expiresIn": 900, "refreshToken": "rt-1"}

    def test_omits_refresh_when_absent(self) -> None:
        b = TokenBundle(access_token="eyJ.a.b", refresh_token=None, expires_at=500.0)
        payload = bundle_to_connector_payload(b, now=100.0)
        assert "refreshToken" not in payload
        assert payload["accessToken"] == "eyJ.a.b"

    def test_expiry_floored_at_zero(self) -> None:
        b = TokenBundle(access_token="x", refresh_token=None, expires_at=100.0)
        # now past expiry -> expiresIn must not go negative
        assert bundle_to_connector_payload(b, now=999.0)["expiresIn"] == 0


class TestWriteConnectorTokenFile:
    def test_writes_0600_and_roundtrips(self, tmp_path: Path) -> None:
        dst = tmp_path / "nested" / "token.json"
        write_connector_token_file({"accessToken": "eyJ", "expiresIn": 900}, dst)
        assert json.loads(dst.read_text())["accessToken"] == "eyJ"
        if os.name == "posix":
            mode = stat.S_IMODE(dst.stat().st_mode)
            assert mode == stat.S_IRUSR | stat.S_IWUSR  # 0600

    def test_rejects_empty_access_token(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="accessToken"):
            write_connector_token_file({"accessToken": "", "expiresIn": 900}, tmp_path / "t.json")


class TestNormalizeDownloadedTokens:
    def test_camelcase_download_passthrough(self) -> None:
        out = normalize_downloaded_tokens(
            {"accessToken": "eyJ", "refreshToken": "rt", "expiresIn": 900}
        )
        assert out == {"accessToken": "eyJ", "expiresIn": 900, "refreshToken": "rt"}

    def test_accepts_snake_case_idcs_shape(self) -> None:
        out = normalize_downloaded_tokens({"access_token": "eyJ", "refresh_token": "rt"})
        assert out["accessToken"] == "eyJ"
        assert out["refreshToken"] == "rt"
        assert out["expiresIn"] == 900  # default when neither expiresIn nor expires_at present

    def test_missing_access_token_raises(self) -> None:
        with pytest.raises(ValueError, match="accessToken"):
            normalize_downloaded_tokens({"refreshToken": "rt"})


class TestWireTokenIntoMcpConfig:
    def _config(self, args: list[str]) -> dict:
        return {"mcpServers": {"oac-mcp-server": {"command": "node", "args": list(args)}}}

    def test_appends_when_two_args(self) -> None:
        cfg = self._config(["${OAC_MCP_CONNECT_PATH}", "${OAC_URL}"])
        wire_token_into_mcp_config(cfg, token_file="/home/u/.oac-connect/token.json")
        args = cfg["mcpServers"]["oac-mcp-server"]["args"]
        # placeholders preserved verbatim; token appended as 3rd arg
        assert args == ["${OAC_MCP_CONNECT_PATH}", "${OAC_URL}", "/home/u/.oac-connect/token.json"]

    def test_replaces_existing_third_arg_idempotent(self) -> None:
        cfg = self._config(["conn.js", "https://oac", "/old/token.json"])
        wire_token_into_mcp_config(cfg, token_file="/new/token.json")
        wire_token_into_mcp_config(cfg, token_file="/new/token.json")  # idempotent
        assert cfg["mcpServers"]["oac-mcp-server"]["args"] == ["conn.js", "https://oac", "/new/token.json"]

    def test_missing_server_raises_keyerror(self) -> None:
        with pytest.raises(KeyError):
            wire_token_into_mcp_config({"mcpServers": {}}, token_file="/t.json")

    def test_too_few_args_raises_valueerror(self) -> None:
        cfg = self._config(["only-connector.js"])
        with pytest.raises(ValueError, match="at least"):
            wire_token_into_mcp_config(cfg, token_file="/t.json")


class TestFileLevelHelpers:
    def test_wire_mcp_json_file_roundtrip(self, tmp_path: Path) -> None:
        mcp = tmp_path / ".mcp.json"
        mcp.write_text(json.dumps(
            {"mcpServers": {"oac-mcp-server": {"command": "node", "args": ["c.js", "${OAC_URL}"]}}}
        ))
        wire_mcp_json_file(mcp, token_file="/tok.json")
        args = json.loads(mcp.read_text())["mcpServers"]["oac-mcp-server"]["args"]
        assert args[2] == "/tok.json"

    def test_import_connector_token_end_to_end(self, tmp_path: Path) -> None:
        src = tmp_path / "downloaded.json"
        src.write_text(json.dumps({"accessToken": "eyJ.real", "refreshToken": "rt", "expiresIn": 900}))
        dst = tmp_path / "out" / "token.json"
        written, payload = import_connector_token(src, token_file=dst)
        assert written == dst
        assert json.loads(dst.read_text())["accessToken"] == "eyJ.real"
        assert payload["refreshToken"] == "rt"


# ============================================================= basic auth path
class TestBuildConnectorConfig:
    def test_shape_and_default_headless(self) -> None:
        cfg = build_connector_config(oac_url="https://oac.example.com", basic_auth="u:p")
        srv = cfg["mcpServers"][0]
        assert srv == {
            "url": "https://oac.example.com",
            "default": True,
            "basicAuth": "u:p",
            "headless": True,
        }

    def test_headless_can_be_disabled(self) -> None:
        cfg = build_connector_config(oac_url="https://oac", basic_auth="u:p", headless=False)
        assert cfg["mcpServers"][0]["headless"] is False

    def test_normalizes_oac_ui_url_to_origin(self) -> None:
        cfg = build_connector_config(
            oac_url="http://oac.example.com:9888/dv/ui/home.jsp?pageid=home",
            basic_auth="u:p",
        )
        assert cfg["mcpServers"][0]["url"] == "http://oac.example.com:9888"

    def test_rejects_empty_url(self) -> None:
        with pytest.raises(ValueError, match="oac_url"):
            build_connector_config(oac_url="", basic_auth="u:p")

    @pytest.mark.parametrize("bad", ["nopassword", ":onlypass", ""])
    def test_rejects_bad_basic_auth(self, bad: str) -> None:
        with pytest.raises(ValueError, match="username:password"):
            build_connector_config(oac_url="https://oac", basic_auth=bad)


class TestWriteConnectorConfigFile:
    def test_writes_0600(self, tmp_path: Path) -> None:
        dst = tmp_path / "nested" / "oac_mcp_connect_config.json"
        cfg = build_connector_config(oac_url="https://oac", basic_auth="u:p")
        write_connector_config_file(cfg, dst)
        assert json.loads(dst.read_text())["mcpServers"][0]["basicAuth"] == "u:p"
        if os.name == "posix":
            assert stat.S_IMODE(dst.stat().st_mode) == stat.S_IRUSR | stat.S_IWUSR

    def test_rejects_empty_servers(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="mcpServers"):
            write_connector_config_file({"mcpServers": []}, tmp_path / "c.json")


class TestStageConnector:
    def _connector_body(self) -> str:
        return "#!/usr/bin/env node\n" + ("x" * 2048)

    def test_copies_to_dest(self, tmp_path: Path) -> None:
        src = tmp_path / "dl" / "oac-mcp-connect.js"
        src.parent.mkdir()
        src.write_text(self._connector_body())
        dest = tmp_path / "staged" / "oac-mcp-connect.js"
        out = stage_connector(src, dest)
        assert out == dest
        assert dest.read_text() == self._connector_body()

    def test_same_path_is_noop(self, tmp_path: Path) -> None:
        p = tmp_path / "oac-mcp-connect.js"
        p.write_text(self._connector_body())
        assert stage_connector(p, p) == p
        assert p.read_text() == self._connector_body()

    def test_missing_source_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            stage_connector(tmp_path / "nope.js", tmp_path / "d.js")

    def test_rejects_truncated_placeholder(self, tmp_path: Path) -> None:
        p = tmp_path / "oac-mcp-connect.js"
        p.write_text("// conn")
        with pytest.raises(ValueError, match="too small"):
            stage_connector(p, tmp_path / "staged.js")


class TestBasicAuthMcpConfig:
    def test_credential_free_single_arg(self) -> None:
        cfg = build_basic_auth_mcp_config()
        srv = cfg["mcpServers"]["oac-mcp-server"]
        assert srv["command"] == "node"
        assert srv["args"] == [PORTABLE_CONNECTOR_ARG]
        # no URL, no creds leaked anywhere
        assert "basicAuth" not in json.dumps(cfg)

    def test_write_preserves_other_servers(self, tmp_path: Path) -> None:
        mcp = tmp_path / ".mcp.json"
        mcp.write_text(json.dumps({"mcpServers": {"other": {"command": "x", "args": []}}}))
        write_basic_auth_mcp_json(mcp, connector_arg="/abs/conn.js")
        servers = json.loads(mcp.read_text())["mcpServers"]
        assert servers["other"] == {"command": "x", "args": []}  # untouched
        assert servers["oac-mcp-server"]["args"] == ["/abs/conn.js"]

    def test_write_creates_when_absent(self, tmp_path: Path) -> None:
        mcp = tmp_path / "sub" / ".mcp.json"
        mcp.parent.mkdir()
        write_basic_auth_mcp_json(mcp)
        assert json.loads(mcp.read_text())["mcpServers"]["oac-mcp-server"]["args"] == [
            PORTABLE_CONNECTOR_ARG
        ]


class TestSetupBasicAuth:
    def _connector_body(self) -> str:
        return "#!/usr/bin/env node\n" + ("x" * 2048)

    def test_end_to_end_default_location_is_portable(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        src = tmp_path / "oac-mcp-connect.js"
        src.write_text(self._connector_body())
        cfgf = tmp_path / "config.json"
        mcp = tmp_path / ".mcp.json"
        default_dest = tmp_path / ".oac-connect" / "oac-mcp-connect.js"
        monkeypatch.setattr(mcp_token_mod, "DEFAULT_STAGED_CONNECTOR", default_dest)
        summary = setup_basic_auth(
            oac_url="https://oac.example.com/dv/ui/home.jsp?pageid=home",
            user="alice",
            password="secret",
            connector_js=src,
            config_file=cfgf,
            connector_dest=default_dest,  # default → portable ${HOME} arg
            mcp_json=mcp,
        )
        # config file holds creds; .mcp.json does not
        assert json.loads(cfgf.read_text())["mcpServers"][0]["basicAuth"] == "alice:secret"
        args = json.loads(mcp.read_text())["mcpServers"]["oac-mcp-server"]["args"]
        assert args == [PORTABLE_CONNECTOR_ARG]
        assert summary["oac_url"] == "https://oac.example.com"
        assert "secret" not in json.dumps(summary)  # summary carries no password

    def test_custom_dest_uses_absolute_arg(self, tmp_path: Path) -> None:
        src = tmp_path / "oac-mcp-connect.js"
        src.write_text(self._connector_body())
        dest = tmp_path / "staged" / "oac-mcp-connect.js"
        mcp = tmp_path / ".mcp.json"
        summary = setup_basic_auth(
            oac_url="https://oac",
            user="u",
            password="p",
            connector_js=src,
            config_file=tmp_path / "c.json",
            connector_dest=dest,
            mcp_json=mcp,
        )
        assert summary["connector_arg"] == str(dest.resolve())

    def test_requires_credentials(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="user and password"):
            setup_basic_auth(oac_url="https://oac", user="", password="p")

    def test_missing_staged_connector_without_src_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="no connector"):
            setup_basic_auth(
                oac_url="https://oac", user="u", password="p",
                connector_js=None, config_file=tmp_path / "c.json",
                connector_dest=tmp_path / "absent.js",
            )
