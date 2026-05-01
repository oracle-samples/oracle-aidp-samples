"""OAC OAuth 2.0 token acquisition: Authorization Code + PKCE with refresh-token persistence.

OAC's REST API requires **user-context** tokens, NOT client-credentials.
Per [Oracle's Authenticate doc](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/authenticate.html):

> "You must obtain the token with a user context. This means you can't use some
>  grant types, such as the Client Credentials grant type, to access Oracle
>  Analytics Cloud REST APIs."

Verified live 2026-05-01 (TC10h) against ``aidp-fusion-bundle-test-...analytics.ocp.oraclecloud.com``:
``client_credentials`` tokens get HTTP 500 from the catalog backend (Jersey
servlet NPEs trying to resolve the caller's home folder). Auth Code + PKCE
tokens (``sub_type=user``) return HTTP 200.

This module supports two flows:

1. **Authorization Code + PKCE** (default) — opens a browser for one-time consent;
   loopback callback captures the code; refresh-token persists for silent reuse.
2. **Device Authorization Grant** (``flow="device"``) — for headless boxes where
   no browser is available; user opens a separate device on their phone/laptop.

The refresh token persists to ``$HOME/.aidp-fusion-bundle/oac-token.json``
(mode 0600). Subsequent invocations refresh silently — no re-consent.
"""

from __future__ import annotations

import base64
import hashlib
import http.server
import json
import os
import secrets
import socketserver
import stat
import threading
import time
import urllib.parse
import webbrowser
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import requests


@dataclass
class TokenBundle:
    """A user-context Bearer token with refresh metadata."""

    access_token: str
    refresh_token: str | None
    expires_at: float

    def is_valid(self, leeway_seconds: int = 30) -> bool:
        return time.time() + leeway_seconds < self.expires_at

    def to_dict(self) -> dict[str, str | float | None]:
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "expires_at": self.expires_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "TokenBundle":
        return cls(
            access_token=data["access_token"],
            refresh_token=data.get("refresh_token"),
            expires_at=float(data["expires_at"]),
        )


class OacOauthFlow:
    """Acquires user-context OAuth tokens for OAC REST API.

    First invocation triggers an interactive Auth Code + PKCE flow (browser opens,
    user consents, refresh token persists). Subsequent invocations refresh
    silently from the cached refresh token.

    Args:
        idcs_url: IDCS stripe URL (``https://idcs-<stripe>.identity.oraclecloud.com``).
        client_id: Confidential application Client ID (must have ``authorization_code``
            + ``refresh_token`` grants enabled and a loopback redirect registered).
        client_secret: Confidential application Client Secret.
        scope: Full OAuth scope string. For OAC, format is
            ``<OAC_URL>urn:opc:resource:consumer::all`` (concatenated, no separator).
            Use :func:`derive_oac_scope` to build it from the OAC URL.
        token_path: Where to persist the token bundle. Defaults to
            ``~/.aidp-fusion-bundle/oac-token.json``. Created with mode 0600.
        flow: ``"auth_code"`` (default) or ``"device"`` for headless setups.
        callback_port: Loopback port for Auth Code redirect (default 8765).
        timeout: Per-HTTP-request timeout in seconds.
        session: Optional pre-configured ``requests.Session``.

    Usage:
        >>> flow = OacOauthFlow(
        ...     idcs_url="https://idcs-abc.identity.oraclecloud.com",
        ...     client_id="...", client_secret="...",
        ...     scope=derive_oac_scope("https://my-oac.../analytics.ocp.oraclecloud.com"),
        ... )
        >>> token = flow.get_token()  # interactive on first call
        >>> requests.get(url, headers={"Authorization": f"Bearer {token}"})
    """

    def __init__(
        self,
        idcs_url: str,
        client_id: str,
        client_secret: str,
        *,
        scope: str,
        token_path: Path | str | None = None,
        flow: Literal["auth_code", "device"] = "auth_code",
        callback_port: int = 8765,
        timeout: int = 30,
        session: requests.Session | None = None,
    ) -> None:
        if not idcs_url.startswith(("http://", "https://")):
            raise ValueError(f"idcs_url must include scheme: got {idcs_url!r}")
        self._idcs_url = idcs_url.rstrip("/")
        self._token_endpoint = self._idcs_url + "/oauth2/v1/token"
        self._authorize_endpoint = self._idcs_url + "/oauth2/v1/authorize"
        self._device_endpoint = self._idcs_url + "/oauth2/v1/device"
        self._client_id = client_id
        self._client_secret = client_secret
        self._scope = scope
        self._flow = flow
        self._callback_port = callback_port
        self._timeout = timeout
        self._session = session or requests.Session()

        if token_path is None:
            home = Path(os.environ.get("HOME") or os.path.expanduser("~"))
            token_path = home / ".aidp-fusion-bundle" / "oac-token.json"
        self._token_path = Path(token_path)
        self._cached: TokenBundle | None = None

    # ---------------------------------------------------------------- public
    def get_token(self, *, force_refresh: bool = False) -> str:
        """Return a valid Bearer access token, refreshing or re-authorizing as needed."""
        if not force_refresh:
            self._cached = self._cached or self._load_persisted()
            if self._cached is not None and self._cached.is_valid():
                return self._cached.access_token
            if self._cached is not None and self._cached.refresh_token:
                refreshed = self._refresh(self._cached.refresh_token)
                if refreshed is not None:
                    self._cached = refreshed
                    self._persist(self._cached)
                    return self._cached.access_token

        # Fall through to interactive grant
        if self._flow == "device":
            self._cached = self._device_code_flow()
        else:
            self._cached = self._auth_code_flow()
        self._persist(self._cached)
        return self._cached.access_token

    # ------------------------------------------------------------ persistence
    def _persist(self, bundle: TokenBundle) -> None:
        self._token_path.parent.mkdir(parents=True, exist_ok=True)
        self._token_path.write_text(json.dumps(bundle.to_dict(), indent=2))
        try:
            os.chmod(self._token_path, stat.S_IRUSR | stat.S_IWUSR)
        except (OSError, NotImplementedError):
            pass  # Windows: best-effort

    def _load_persisted(self) -> TokenBundle | None:
        if not self._token_path.exists():
            return None
        try:
            return TokenBundle.from_dict(json.loads(self._token_path.read_text()))
        except (json.JSONDecodeError, KeyError, ValueError):
            return None

    # ---------------------------------------------------------------- refresh
    def _refresh(self, refresh_token: str) -> TokenBundle | None:
        response = self._session.post(
            self._token_endpoint,
            data={"grant_type": "refresh_token", "refresh_token": refresh_token,
                  "scope": self._scope},
            auth=(self._client_id, self._client_secret),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=self._timeout,
        )
        if response.status_code != 200:
            return None
        body = response.json()
        return TokenBundle(
            access_token=body["access_token"],
            refresh_token=body.get("refresh_token", refresh_token),
            expires_at=time.time() + int(body.get("expires_in", 3600)),
        )

    # --------------------------------------------------------- auth code flow
    def _auth_code_flow(self) -> TokenBundle:
        verifier = _b64url(secrets.token_bytes(64))
        challenge = _b64url(hashlib.sha256(verifier.encode()).digest())
        state = _b64url(secrets.token_bytes(16))
        redirect_uri = f"http://localhost:{self._callback_port}/callback"

        result: dict[str, str | None] = {"code": None, "state": None, "error": None}

        class CallbackHandler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_args, **_kwargs) -> None:
                pass

            def do_GET(self) -> None:  # noqa: N802 (BaseHTTPRequestHandler API)
                parsed = urllib.parse.urlparse(self.path)
                qs = urllib.parse.parse_qs(parsed.query)
                if parsed.path == "/callback":
                    result["code"] = qs.get("code", [None])[0]
                    result["state"] = qs.get("state", [None])[0]
                    result["error"] = qs.get("error", [None])[0]
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html")
                    self.end_headers()
                    self.wfile.write(
                        b"<h1>OAC consent received.</h1>"
                        b"<p>You can close this tab and return to the terminal.</p>"
                    )
                    threading.Thread(target=self.server.shutdown, daemon=True).start()
                else:
                    self.send_response(404)
                    self.end_headers()

        server = socketserver.TCPServer(("127.0.0.1", self._callback_port), CallbackHandler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            authorize_url = (
                self._authorize_endpoint
                + "?"
                + urllib.parse.urlencode({
                    "response_type": "code",
                    "client_id": self._client_id,
                    "redirect_uri": redirect_uri,
                    "scope": self._scope,
                    "state": state,
                    "code_challenge": challenge,
                    "code_challenge_method": "S256",
                })
            )
            print(f"[oac-oauth] Opening browser for one-time consent at {self._idcs_url}")
            print(f"[oac-oauth] If browser doesn't open, paste this URL manually:\n  {authorize_url}")
            webbrowser.open(authorize_url)

            deadline = time.time() + 300
            while result["code"] is None and result["error"] is None and time.time() < deadline:
                time.sleep(0.5)
        finally:
            server.shutdown()
            server.server_close()

        if result["error"]:
            raise RuntimeError(f"OAC consent failed: {result['error']}")
        if result["state"] != state:
            raise RuntimeError("OAuth state mismatch — possible CSRF; aborting")
        if not result["code"]:
            raise RuntimeError("Timed out waiting for OAuth callback after 300s")

        response = self._session.post(
            self._token_endpoint,
            auth=(self._client_id, self._client_secret),
            data={
                "grant_type": "authorization_code",
                "code": result["code"],
                "redirect_uri": redirect_uri,
                "code_verifier": verifier,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=self._timeout,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"OAuth token exchange failed: HTTP {response.status_code}: {response.text}"
            )
        body = response.json()
        return TokenBundle(
            access_token=body["access_token"],
            refresh_token=body.get("refresh_token"),
            expires_at=time.time() + int(body.get("expires_in", 3600)),
        )

    # ------------------------------------------------------------- device flow
    def _device_code_flow(self) -> TokenBundle:
        response = self._session.post(
            self._device_endpoint,
            auth=(self._client_id, self._client_secret),
            data={"scope": self._scope},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=self._timeout,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Device authorization request failed: HTTP {response.status_code}: {response.text}"
            )
        body = response.json()
        device_code = body["device_code"]
        user_code = body["user_code"]
        verification_uri = body.get("verification_uri") or body.get("verification_url")
        interval = int(body.get("interval", 5))

        print(f"[oac-oauth] On any device, open: {verification_uri}")
        print(f"[oac-oauth] Enter code: {user_code}")
        print(f"[oac-oauth] Waiting for approval (poll every {interval}s)...")

        deadline = time.time() + int(body.get("expires_in", 600))
        while time.time() < deadline:
            time.sleep(interval)
            poll = self._session.post(
                self._token_endpoint,
                auth=(self._client_id, self._client_secret),
                data={
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                    "device_code": device_code,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=self._timeout,
            )
            if poll.status_code == 200:
                pb = poll.json()
                return TokenBundle(
                    access_token=pb["access_token"],
                    refresh_token=pb.get("refresh_token"),
                    expires_at=time.time() + int(pb.get("expires_in", 3600)),
                )
            err = poll.json().get("error") if poll.headers.get("Content-Type", "").startswith("application/json") else None
            if err == "authorization_pending":
                continue
            if err == "slow_down":
                interval += 5
                continue
            raise RuntimeError(f"Device flow failed: HTTP {poll.status_code}: {poll.text}")

        raise RuntimeError("Device flow timed out — user did not approve in time")


def derive_oac_scope(oac_url: str, *, audience: str | None = None) -> str:
    """Build the canonical OAC OAuth scope string.

    OAC's IDCS service-app publishes one scope: ``urn:opc:resource:consumer::all``.
    The scope prefix is the IDCS service-app's **audience**, which is a
    different hostname from the user-facing OAC URL (verified live 2026-05-01:
    ``https://aidp-fusion-bundle-test-...analytics.ocp.oraclecloud.com``
    publishes scopes under ``https://<24-char-id>.analytics.ocp.oraclecloud.com``).

    The full scope-claim format (concatenated, no separator):

        ``<AUDIENCE>urn:opc:resource:consumer::all offline_access``

    Args:
        oac_url: OAC instance URL (with or without trailing slash). Used as
            audience fallback when ``audience`` is not provided. Note: this
            usually does NOT match the published-scope audience, so explicit
            ``audience`` is strongly preferred for production use.
        audience: IDCS service-app audience (the hostname that prefixes the
            scope claim). Auto-discoverable via :func:`discover_oac_audience`
            if not provided.

    Returns:
        Space-delimited scope string ready to send to ``/oauth2/v1/authorize``.
    """
    base = (audience or oac_url).rstrip("/")
    return f"{base}urn:opc:resource:consumer::all offline_access"


def discover_oac_audience(oac_url: str, *, timeout: int = 15) -> str:
    """Probe OAC's login redirect to discover the IDCS service-app audience prefix.

    OAC's ``/ui/`` redirects unauthenticated requests to IDCS with a query
    parameter ``idcs_app_name=<24-char-prefix>_APPID``. That prefix is
    exactly the host-component of the published-scope audience:

        ``https://<prefix>.analytics.ocp.oraclecloud.com``

    Verified live 2026-05-01 against ``aidp-fusion-bundle-test-...``.

    Args:
        oac_url: OAC instance URL.
        timeout: HTTP timeout in seconds.

    Returns:
        Full audience URL ready to feed into :func:`derive_oac_scope`.

    Raises:
        RuntimeError: if the probe doesn't find an ``idcs_app_name`` parameter.
    """
    base = oac_url.rstrip("/")
    response = requests.get(f"{base}/ui/", allow_redirects=False, timeout=timeout)
    location = response.headers.get("Location", "")
    if not location:
        raise RuntimeError(
            f"OAC audience discovery failed: {base}/ui/ did not redirect "
            f"(HTTP {response.status_code}); pass --oauth-audience explicitly."
        )
    parsed = urllib.parse.urlparse(location)
    qs = urllib.parse.parse_qs(parsed.query)
    app_name = qs.get("idcs_app_name", [None])[0]
    if not app_name or "_APPID" not in app_name:
        # Sometimes wrapped through a second redirect; follow once and re-parse.
        response = requests.get(location, allow_redirects=False, timeout=timeout)
        location = response.headers.get("Location", location)
        parsed = urllib.parse.urlparse(location)
        qs = urllib.parse.parse_qs(parsed.query)
        app_name = qs.get("idcs_app_name", [None])[0]
    if not app_name or "_APPID" not in app_name:
        raise RuntimeError(
            f"OAC audience discovery failed: no idcs_app_name parameter in "
            f"redirect chain; pass --oauth-audience explicitly. "
            f"(probed URL: {location[:120]}...)"
        )
    prefix = app_name.split("_APPID", 1)[0]
    return f"https://{prefix}.analytics.ocp.oraclecloud.com"


# Backwards-compatibility shim: prior versions exposed ``IdcsTokenFetcher``.
# Now an alias to ``OacOauthFlow`` so existing callers don't break, but the
# Client Credentials grant is no longer supported (would 500 against catalog).
IdcsTokenFetcher = OacOauthFlow


__all__ = [
    "OacOauthFlow",
    "IdcsTokenFetcher",
    "TokenBundle",
    "derive_oac_scope",
    "discover_oac_audience",
]


# ----------------------------------------------------------------- internals
def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()
