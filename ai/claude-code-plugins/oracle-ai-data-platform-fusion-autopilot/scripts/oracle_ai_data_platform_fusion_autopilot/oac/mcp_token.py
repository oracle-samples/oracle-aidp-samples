"""Bridge: mint/import a token the **OAC MCP connector** can consume non-interactively.

Problem this solves
-------------------
The OAC MCP connector (``oac-mcp-connect.js``) forwards every real tool call to
``<oac-url>/api/mcp``, which requires authentication. With no token it falls back
to **interactive browser auth**, which cannot complete inside the Claude Code MCP
client (the client reports ``elicitation=not-supported``) — the connector exits
and the MCP client sees ``-32000: Connection closed``.

The fix is to hand the connector a **token file** (its 2nd positional arg) so it
authenticates with a Bearer token and never opens a browser. The connector expects:

    { "accessToken": "eyJ...", "refreshToken": "...", "expiresIn": 900 }

(camelCase, ``expiresIn`` = seconds-from-now). The bundle's :class:`OacOauthFlow`
already mints user-context tokens via IDCS, but persists them as
``{access_token, refresh_token, expires_at}`` (snake_case, absolute epoch). This
module is the format bridge plus the ``.mcp.json`` wiring.

Two entry points:
  * :func:`mint_connector_token` — run the IDCS Auth-Code+PKCE/device flow (needs a
    confidential-app ``client_id``/``client_secret``) and write the connector token file.
  * :func:`import_connector_token` — take a ``tokens.json`` already downloaded from the
    OAC Profile → Access Tokens UI (which is *already* in connector format) and place
    it as the connector token file. Needs no OAuth client.

Plus :func:`wire_token_into_mcp_config`, which appends the token-file path to the
connector's args in ``.mcp.json`` without disturbing the existing
``${OAC_MCP_CONNECT_PATH}`` / ``${OAC_URL}`` placeholders.

Basic-auth path (recommended for non-IDCS instances)
----------------------------------------------------
Token auth only works on IDCS-protected instances; on a non-IDCS pod the issued
tokens are rejected. There the connector's **basic auth** is the non-interactive
route that works in Claude Code: credentials are supplied up front (the connector
builds ``Authorization: Basic base64(user:pass)``), so it never elicits a browser
login. v1.4 reads them from a connector **config file** the connector auto-discovers
at ``~/.oac-connect/oac_mcp_connect_config.json`` (a ``basicAuth: "user:pass"``
server entry with ``headless: true``). Live-validated on a non-IDCS pod 2026-06-13.

The basic-auth helpers below (:func:`build_connector_config`,
:func:`write_connector_config_file`, :func:`stage_connector`,
:func:`build_basic_auth_mcp_config`, :func:`setup_basic_auth`) write that config
file, stage the connector to a stable path, and wire a **credential-free**
``.mcp.json`` (single connector arg — the URL and creds live only in the 0600
config file, never in the committed repo).
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import time
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit, urlunsplit

from .rest.oauth import OacOauthFlow, TokenBundle, derive_oac_scope, discover_oac_audience

#: The connector's own state/config dir (it auto-discovers files here).
_OAC_CONNECT_DIR = Path(os.environ.get("HOME") or os.path.expanduser("~")) / ".oac-connect"

#: Where the connector looks for its token file by convention (its own log dir).
DEFAULT_CONNECTOR_TOKEN_FILE = _OAC_CONNECT_DIR / "token.json"

#: Connector config file the connector auto-discovers (holds the basicAuth entry).
DEFAULT_CONNECTOR_CONFIG_FILE = _OAC_CONNECT_DIR / "oac_mcp_connect_config.json"

#: Stable location we stage the connector .js to, so ``.mcp.json`` can be portable.
DEFAULT_STAGED_CONNECTOR = _OAC_CONNECT_DIR / "oac-mcp-connect.js"

#: Portable ``.mcp.json`` arg pointing at the staged connector (``${HOME}`` expands
#: in Claude Code's .mcp.json; no username, URL, or creds are committed).
PORTABLE_CONNECTOR_ARG = "${HOME}/.oac-connect/oac-mcp-connect.js"

#: The server name the bundle uses in ``.mcp.json`` for the OAC connector.
DEFAULT_MCP_SERVER_NAME = "oac-mcp-server"


def _normalize_oac_base_url(oac_url: str) -> str:
    """Return the OAC origin expected by the MCP connector."""
    parsed = urlsplit(oac_url.strip())
    if not parsed.scheme or not parsed.netloc:
        return oac_url.strip().rstrip("/")
    return urlunsplit((parsed.scheme, parsed.netloc, "", "", "")).rstrip("/")


def _validate_connector_js(path: Path) -> None:
    """Reject obvious placeholder/truncated connector files."""
    size = path.stat().st_size
    if size < 1024:
        raise ValueError(
            f"connector appears too small to be the real OAC MCP connector: "
            f"{path} ({size} bytes)"
        )


# --------------------------------------------------------------------- format
def bundle_to_connector_payload(
    bundle: TokenBundle, *, now: float | None = None
) -> dict[str, Any]:
    """Convert an IDCS :class:`TokenBundle` into the connector's token-file shape.

    Args:
        bundle: token bundle from :meth:`OacOauthFlow.get_bundle`.
        now: epoch seconds used to derive the relative ``expiresIn`` (defaults to
            ``time.time()``; injectable for deterministic tests).

    Returns:
        ``{"accessToken": ..., "expiresIn": N[, "refreshToken": ...]}``.
        ``refreshToken`` is omitted when the bundle has none (the connector treats
        it as optional and keeps refreshed tokens in memory only).
    """
    now = time.time() if now is None else now
    payload: dict[str, Any] = {
        "accessToken": bundle.access_token,
        "expiresIn": max(0, int(bundle.expires_at - now)),
    }
    if bundle.refresh_token:
        payload["refreshToken"] = bundle.refresh_token
    return payload


def write_connector_token_file(payload: dict[str, Any], path: Path | str) -> Path:
    """Write the connector token payload to ``path`` with owner-only perms (0600)."""
    if "accessToken" not in payload or not payload["accessToken"]:
        raise ValueError("connector token payload missing a non-empty 'accessToken'")
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))
    try:
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    except (OSError, NotImplementedError):
        pass  # Windows: best-effort
    return path


def normalize_downloaded_tokens(data: dict[str, Any]) -> dict[str, Any]:
    """Coerce an OAC-downloaded ``tokens.json`` into the strict connector shape.

    The OAC Profile → Access Tokens download is already camelCase
    (``accessToken``/``refreshToken``/``expiresIn``), but we defensively accept the
    snake_case IDCS shape too, so either file works as a drop-in.

    Raises:
        ValueError: if no access token can be found under any known key.
    """
    access = data.get("accessToken") or data.get("access_token")
    if not access:
        raise ValueError(
            "downloaded token file has neither 'accessToken' nor 'access_token'; "
            "is this an OAC Access-Tokens download?"
        )
    refresh = data.get("refreshToken") or data.get("refresh_token")
    expires_in = data.get("expiresIn")
    if expires_in is None and "expires_at" in data:
        expires_in = max(0, int(float(data["expires_at"]) - time.time()))
    payload: dict[str, Any] = {"accessToken": access, "expiresIn": int(expires_in or 900)}
    if refresh:
        payload["refreshToken"] = refresh
    return payload


# ----------------------------------------------------------------- .mcp.json
def wire_token_into_mcp_config(
    config: dict[str, Any],
    *,
    token_file: str,
    server_name: str = DEFAULT_MCP_SERVER_NAME,
) -> dict[str, Any]:
    """Return ``config`` with ``token_file`` set as the connector's token arg.

    The connector's CLI contract is ``node <connector> <url> [<token-file>]`` — so
    the token file is the **3rd** element of the args array (after the connector path
    and the URL). This preserves elements 0–1 verbatim (including any
    ``${OAC_MCP_CONNECT_PATH}`` / ``${OAC_URL}`` placeholders) and is idempotent:
    re-wiring just replaces the 3rd element.

    Raises:
        KeyError: if ``server_name`` isn't present under ``mcpServers``.
        ValueError: if that server has fewer than 2 args (no URL to append after).
    """
    servers = config.get("mcpServers", {})
    if server_name not in servers:
        raise KeyError(
            f"{server_name!r} not found under mcpServers; "
            f"present: {sorted(servers)}"
        )
    args = servers[server_name].setdefault("args", [])
    if len(args) < 2:
        raise ValueError(
            f"{server_name!r} args has {len(args)} element(s); expected at least "
            f"[connector, url] before appending a token file"
        )
    if len(args) >= 3:
        args[2] = token_file
    else:
        args.append(token_file)
    return config


def wire_mcp_json_file(
    mcp_json_path: Path | str,
    *,
    token_file: str,
    server_name: str = DEFAULT_MCP_SERVER_NAME,
) -> Path:
    """Load ``.mcp.json``, wire the token file into the connector args, write it back."""
    mcp_json_path = Path(mcp_json_path)
    config = json.loads(mcp_json_path.read_text())
    wire_token_into_mcp_config(config, token_file=token_file, server_name=server_name)
    mcp_json_path.write_text(json.dumps(config, indent=2) + "\n")
    return mcp_json_path


# ----------------------------------------------------------------- orchestration
def mint_connector_token(
    *,
    oac_url: str,
    idcs_url: str,
    client_id: str,
    client_secret: str,
    token_file: Path | str = DEFAULT_CONNECTOR_TOKEN_FILE,
    scope: str | None = None,
    flow: str = "auth_code",
    prompt_login: bool = False,
    now_fn: Callable[[], float] = time.time,
    session: Any = None,
) -> tuple[Path, dict[str, Any]]:
    """Mint a user-context token via IDCS and write it in connector format.

    Needs a confidential-app ``client_id``/``client_secret`` (IDCS issues no token
    without a registered client). First call is interactive (browser/device); the
    underlying :class:`OacOauthFlow` persists a refresh token for silent reuse.

    Returns ``(token_file_path, payload)``. ``payload`` never contains a logged
    secret in callers — mask before printing.
    """
    if scope is None:
        try:
            audience = discover_oac_audience(oac_url)
            scope = derive_oac_scope(oac_url, audience=audience)
        except Exception:  # noqa: BLE001 — fall back to URL-derived scope
            scope = derive_oac_scope(oac_url)
    oauth = OacOauthFlow(
        idcs_url=idcs_url,
        client_id=client_id,
        client_secret=client_secret,
        scope=scope,
        flow=flow,  # type: ignore[arg-type]
        prompt_login=prompt_login,
        session=session,
    )
    bundle = oauth.get_bundle()
    payload = bundle_to_connector_payload(bundle, now=now_fn())
    written = write_connector_token_file(payload, token_file)
    return written, payload


def import_connector_token(
    src_path: Path | str,
    *,
    token_file: Path | str = DEFAULT_CONNECTOR_TOKEN_FILE,
) -> tuple[Path, dict[str, Any]]:
    """Place an OAC-downloaded ``tokens.json`` as the connector token file.

    No OAuth client required — uses whatever token the OAC Profile → Access Tokens
    UI issued. Returns ``(token_file_path, payload)``.
    """
    data = json.loads(Path(src_path).read_text())
    payload = normalize_downloaded_tokens(data)
    written = write_connector_token_file(payload, token_file)
    return written, payload


# =================================================================== basic auth
def build_connector_config(
    *,
    oac_url: str,
    basic_auth: str,
    headless: bool = True,
) -> dict[str, Any]:
    """Build the connector config-file dict for non-interactive **basic auth**.

    The connector auto-discovers this file at
    ``~/.oac-connect/oac_mcp_connect_config.json`` and authenticates with
    ``Authorization: Basic base64(user:pass)`` — no browser, no elicitation. We mark
    the entry ``default: true`` so the connector resolves URL + creds from it when
    launched with **no positional args**.

    Args:
        oac_url: OAC base URL (the connector appends ``/api/mcp`` itself).
        basic_auth: ``"username:password"`` (the connector base64-encodes it).
        headless: when True (default) the connector never launches a browser — it
            exits on auth failure instead of falling back to interactive login,
            which is exactly what a terminal MCP client needs.

    Raises:
        ValueError: if ``oac_url`` is empty, or ``basic_auth`` is not ``user:pass``.
    """
    if not oac_url:
        raise ValueError("oac_url must be a non-empty OAC base URL")
    if ":" not in basic_auth or not basic_auth.split(":", 1)[0]:
        raise ValueError('basic_auth must be in "username:password" form')
    return {
        "mcpServers": [
            {
                "url": _normalize_oac_base_url(oac_url),
                "default": True,
                "basicAuth": basic_auth,
                "headless": headless,
            }
        ]
    }


def write_connector_config_file(
    config: dict[str, Any], path: Path | str = DEFAULT_CONNECTOR_CONFIG_FILE
) -> Path:
    """Write the connector config (basic auth) with owner-only perms (0600).

    0600 because the config holds a plaintext password — keep it off any other
    user's read path. Mirrors :func:`write_connector_token_file`.
    """
    servers = config.get("mcpServers")
    if not isinstance(servers, list) or not servers:
        raise ValueError("connector config must have a non-empty 'mcpServers' list")
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config, indent=2))
    try:
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    except (OSError, NotImplementedError):
        pass  # Windows: best-effort
    return path


def stage_connector(
    src_js: Path | str, dest: Path | str = DEFAULT_STAGED_CONNECTOR
) -> Path:
    """Copy the downloaded ``oac-mcp-connect.js`` to a stable location.

    Staging makes ``.mcp.json`` portable: it can reference a fixed
    ``~/.oac-connect/oac-mcp-connect.js`` instead of wherever the user happened to
    extract the zip. The v1.4 connector is self-contained (no sibling ``lib/`` /
    ``node_modules`` needed), so copying the single file is sufficient.

    Raises:
        FileNotFoundError: if ``src_js`` doesn't exist.
        ValueError: if ``src_js`` resolves to the same file as ``dest`` (no-op copy).
    """
    src = Path(src_js)
    if not src.is_file():
        raise FileNotFoundError(f"connector not found: {src}")
    _validate_connector_js(src)
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if src.resolve() == dest.resolve():
        _validate_connector_js(dest)
        return dest  # already staged here
    shutil.copyfile(src, dest)
    return dest


def build_basic_auth_mcp_config(
    *,
    connector_arg: str = PORTABLE_CONNECTOR_ARG,
    server_name: str = DEFAULT_MCP_SERVER_NAME,
) -> dict[str, Any]:
    """Build a **credential-free** ``.mcp.json`` for the config-file basic-auth flow.

    The connector is launched with a single arg — its own path — and reads the URL
    and credentials from the auto-discovered config file. Nothing tenant-specific
    (URL, user, password) lands in ``.mcp.json``, so the committed repo stays clean.

    Args:
        connector_arg: the connector path. Defaults to
            :data:`PORTABLE_CONNECTOR_ARG` (``${HOME}/.oac-connect/oac-mcp-connect.js``),
            which Claude Code expands at launch and which has no username embedded.
        server_name: the ``mcpServers`` key (default ``oac-mcp-server``).
    """
    return {
        "mcpServers": {
            server_name: {"command": "node", "args": [connector_arg]}
        }
    }


def write_basic_auth_mcp_json(
    mcp_json_path: Path | str,
    *,
    connector_arg: str = PORTABLE_CONNECTOR_ARG,
    server_name: str = DEFAULT_MCP_SERVER_NAME,
) -> Path:
    """Merge the credential-free connector entry into ``.mcp.json``.

    Preserves any *other* ``mcpServers`` entries; only ``server_name`` is rewritten
    to the single-arg, config-file-driven form. Creates the file if absent.
    """
    mcp_json_path = Path(mcp_json_path)
    if mcp_json_path.exists():
        config = json.loads(mcp_json_path.read_text())
    else:
        config = {}
    servers = config.setdefault("mcpServers", {})
    servers[server_name] = {"command": "node", "args": [connector_arg]}
    mcp_json_path.write_text(json.dumps(config, indent=2) + "\n")
    return mcp_json_path


def setup_basic_auth(
    *,
    oac_url: str,
    user: str,
    password: str,
    connector_js: Path | str | None = None,
    config_file: Path | str = DEFAULT_CONNECTOR_CONFIG_FILE,
    connector_dest: Path | str = DEFAULT_STAGED_CONNECTOR,
    mcp_json: Path | str | None = None,
    server_name: str = DEFAULT_MCP_SERVER_NAME,
    headless: bool = True,
) -> dict[str, Any]:
    """One-shot: write the basic-auth config file, stage the connector, wire .mcp.json.

    Args:
        oac_url: OAC base URL.
        user, password: basic-auth credentials (combined into ``user:pass``).
        connector_js: path to the downloaded ``oac-mcp-connect.js`` to stage. If
            ``None``, staging is skipped (a connector must already exist at
            ``connector_dest``).
        config_file: where to write the connector config (default auto-discovered loc).
        connector_dest: where to stage the connector.
        mcp_json: ``.mcp.json`` to wire. If ``None``, wiring is skipped.
        server_name: ``mcpServers`` key.
        headless: see :func:`build_connector_config`.

    Returns:
        A summary dict (no secrets): ``{config_file, connector, mcp_json, oac_url, user}``.
    """
    if not user or not password:
        raise ValueError("both user and password are required for basic auth")
    config = build_connector_config(
        oac_url=oac_url, basic_auth=f"{user}:{password}", headless=headless
    )
    written_config = write_connector_config_file(config, config_file)

    staged: Path | None = None
    if connector_js is not None:
        staged = stage_connector(connector_js, connector_dest)
    elif not Path(connector_dest).is_file():
        raise FileNotFoundError(
            f"no connector at {connector_dest}; pass connector_js to stage one"
        )
    else:
        staged = Path(connector_dest)

    # If the connector is staged at the default location, keep .mcp.json portable
    # (${HOME}); otherwise reference the absolute staged path.
    connector_arg = (
        PORTABLE_CONNECTOR_ARG
        if staged.resolve() == Path(DEFAULT_STAGED_CONNECTOR).resolve()
        else str(staged.resolve())
    )

    wired: Path | None = None
    if mcp_json is not None:
        wired = write_basic_auth_mcp_json(
            mcp_json, connector_arg=connector_arg, server_name=server_name
        )

    return {
        "config_file": str(written_config),
        "connector": str(staged),
        "connector_arg": connector_arg,
        "mcp_json": str(wired) if wired else None,
        "oac_url": config["mcpServers"][0]["url"],
        "user": user,
    }


__all__ = [
    "DEFAULT_CONNECTOR_TOKEN_FILE",
    "DEFAULT_CONNECTOR_CONFIG_FILE",
    "DEFAULT_STAGED_CONNECTOR",
    "PORTABLE_CONNECTOR_ARG",
    "DEFAULT_MCP_SERVER_NAME",
    "bundle_to_connector_payload",
    "write_connector_token_file",
    "normalize_downloaded_tokens",
    "wire_token_into_mcp_config",
    "wire_mcp_json_file",
    "mint_connector_token",
    "import_connector_token",
    "build_connector_config",
    "write_connector_config_file",
    "stage_connector",
    "build_basic_auth_mcp_config",
    "write_basic_auth_mcp_json",
    "setup_basic_auth",
]
