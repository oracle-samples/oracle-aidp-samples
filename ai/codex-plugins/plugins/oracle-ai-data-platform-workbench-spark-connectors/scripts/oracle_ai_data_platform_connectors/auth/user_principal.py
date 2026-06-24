"""HTTP Basic and OAuth helpers for REST-based connectors.

Used by Fusion REST, Fusion BICC trigger, EPM Cloud, Essbase. OAuth path
is reserved for v0.2 (EPM Cloud / Fusion OAuth profiles); Basic is the
v0.1 default for all four products per the auth-strategy research.
"""

from __future__ import annotations

import time
import uuid
from typing import Optional


def http_basic_session(
    username: str,
    password: str,
    base_url: Optional[str] = None,
    timeout: int = 60,
    retries: int = 3,
    backoff_factor: float = 1.5,
    verify_tls: bool = True,
):
    """Return a ``requests.Session`` with HTTP Basic auth + retry/backoff.

    Args:
        username: For EPM Cloud, this must include the identity-domain prefix:
            ``tenancy.user@domain`` (e.g. ``epmloaner622.first.last@oracle.com``).
            For Fusion REST, it's the standard Fusion user name. For Essbase 21c
            on a customer-hosted realm (e.g. ``ess21c.cealinfra.com``), it's the
            Essbase service-admin username.
        password: Plain password.
        base_url: Optional base URL captured on the session for joining
            relative paths via ``session.get(url, ...)`` later.
        timeout: Default per-request timeout (seconds).
        retries: How many times to retry on transient errors (5xx, connection
            drops). 401/403/404 are NOT retried.
        backoff_factor: passed to urllib3 ``Retry`` (delay = backoff * 2^attempt).
        verify_tls: Whether to verify the server's TLS cert. Default ``True``.
            Set ``False`` for Essbase 21c hosts using internal CA chains the
            AIDP cluster doesn't trust (e.g. ``cealinfra.com``). When you set
            this to False, you may also want to call
            ``urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)``
            to silence the noisy warning.

    Returns:
        A configured ``requests.Session``. ``auth`` and ``verify`` are
        pre-bound, so callers just do ``session.get(...)`` / ``session.post(...)``.
    """
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    session = requests.Session()
    session.auth = (username, password)
    session.verify = verify_tls
    session.headers.update({"Content-Type": "application/json"})

    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Stash the timeout + base_url on the session for callers that want them.
    session.request_timeout = timeout  # type: ignore[attr-defined]
    if base_url:
        session.base_url = base_url.rstrip("/")  # type: ignore[attr-defined]

    return session


def oauth_token(
    token_url: str,
    client_id: str,
    private_key_pem: str,
    audience: str = "https://identity.oraclecloud.com/",
    scope: str = "urn:opc:idm:__myscopes__",
    assertion_lifetime_seconds: int = 300,
) -> str:
    """Mint an OAuth2 bearer token via JWT client-credentials.

    Used by EPM Cloud (Option B in v0.2) and Fusion REST OAuth profile.
    Both IDCS and IAM Domains tenants accept the same payload shape, only
    the ``token_url`` host differs:

    - IDCS:        ``https://<idcs-instance>.identity.oraclecloud.com/oauth2/v1/token``
    - IAM Domains: ``https://<iam-domain>.identity.oraclecloud.com/oauth2/v1/token``

    Args:
        token_url: Full token endpoint URL.
        client_id: App registration client ID (also goes in ``iss`` and
            ``sub`` claims for service-to-service flows).
        private_key_pem: RS256 private key for signing the JWT assertion.
            Pre-decoded — pass the PEM string body, not a file path.
        audience: JWT ``aud`` claim. Default works for both IDCS and IAM
            Domains tenants.
        scope: OAuth scope. Default ``urn:opc:idm:__myscopes__`` covers
            most enterprise app scopes.
        assertion_lifetime_seconds: How long the JWT assertion is valid.
            Default 5 min; the resulting access token typically has its own
            longer lifetime (e.g. 60 min).

    Returns:
        The bearer access token string. Caller passes it as
        ``Authorization: Bearer <token>``.
    """
    import jwt
    import requests

    now = int(time.time())
    payload = {
        "iss": client_id,
        "sub": client_id,
        "aud": audience,
        "exp": now + assertion_lifetime_seconds,
        "iat": now,
        "jti": str(uuid.uuid4()),
    }
    assertion = jwt.encode(payload, private_key_pem, algorithm="RS256")

    body = {
        "grant_type": "client_credentials",
        "client_assertion_type": (
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
        ),
        "client_assertion": assertion,
        "scope": scope,
    }
    response = requests.post(token_url, data=body, timeout=10)
    response.raise_for_status()
    data = response.json()
    return data["access_token"]
