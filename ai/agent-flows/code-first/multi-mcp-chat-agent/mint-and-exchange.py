"""
Local verification helper for OAC's JWT User Assertion flow.

Run this on your laptop BEFORE deploying the agent. It mints the two
JWT assertions (client + user), exchanges them at IDCS for an OAC
access_token, and prints both the token and a curl one-liner to test
it against /api/mcp.

If this script succeeds end-to-end, the IDCS-side configuration
(Confidential App, JWT Assertion grant, registered cert, scopes,
service user) is correct and the agent rewrite will Just Work.

If it fails, fix IDCS first — don't bother debugging agent.py.

Usage:
  1. openssl genrsa -out oac-jwt-key.pem 2048
  2. openssl req -x509 -key oac-jwt-key.pem -out oac-cert.pem \\
       -days 365 -subj "/CN=oac-mcp-agent"
  3. Register oac-cert.pem on the Confidential App as a Trusted Client
     cert in your OCI Identity Domain.
  4. Fill in the CONFIG block below.
  5. python3 mint-and-exchange.py
"""

import base64
import json
import time
import uuid
from pathlib import Path

import requests
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_private_key


# ─── CONFIG — fill in these values ───────────────────────────────
# Path to the PEM-encoded private key whose public cert is registered in IDCS.
# Relative paths are resolved next to this script.
PRIVATE_KEY_PATH   = "oac-jwt-key.pem"

# Your OCI Identity Domain host (NOT the user-facing domain console URL).
# Looks like: idcs-<32-hex-chars>.identity.oraclecloud.com
IDCS_HOST          = "idcs-<your-domain-id>.identity.oraclecloud.com"

# Client ID of the Confidential Application that has the cert imported as
# a Trusted Client cert and "JWT Assertion" in Allowed grant types.
CLIENT_ID          = "<your-confidential-app-client-id>"

# OAC user the agent will operate AS. Must be Active and have access to the
# subject areas / datasets you intend to query. All chat users will see data
# filtered as this user — no per-user RLS in this sample.
SERVICE_USER_EMAIL = "oac-agent-svc@your-org.com"

# Full OAC scope from the Token Issuance Policy → Resources table on the
# Confidential App. Uses the IDCS-registered hash hostname (NOT the
# user-facing OAC URL). Format:
#   https://<hash>.analytics.ocp.oraclecloud.comurn:opc:resource:consumer::all
SCOPE              = "https://<hash>.analytics.ocp.oraclecloud.comurn:opc:resource:consumer::all"

# OAC analytics host (the user-facing one — what you see in the OAC console).
# Used only to print the test curl command at the end.
OAC_HOST           = "<your-oac-tenant>.analytics.ocp.oraclecloud.com"

# Cert alias you set when importing the cert in IDCS. Used as the JWT `kid`
# header so IDCS can find the right cert to verify the signature against.
CERT_ALIAS         = "oac-mcp"


# ─── Mint + exchange ─────────────────────────────────────────────


def _mint_jwt(claims: dict, key) -> str:
    # IDCS uses `kid` to identify which registered cert to verify against.
    header = {"alg": "RS256", "typ": "JWT", "kid": CERT_ALIAS}
    seg = lambda d: base64.urlsafe_b64encode(
        json.dumps(d, separators=(",", ":")).encode()
    ).rstrip(b"=")
    body = seg(header) + b"." + seg(claims)
    sig = key.sign(body, padding.PKCS1v15(), hashes.SHA256())
    return (body + b"." + base64.urlsafe_b64encode(sig).rstrip(b"=")).decode()


def main() -> None:
    key_path = Path(PRIVATE_KEY_PATH)
    if not key_path.is_absolute():
        key_path = Path(__file__).parent / key_path
    key = load_pem_private_key(key_path.read_bytes(), password=None)
    token_url = f"https://{IDCS_HOST}/oauth2/v1/token"

    now  = int(time.time())
    base = {
        "iss": CLIENT_ID,
        # OCI Identity Domains expects the generic Oracle audience here,
        # NOT the tenant-specific token URL. Counter-intuitive but correct
        # per Oracle docs.
        "aud": "https://identity.oraclecloud.com/",
        "iat": now,
        "exp": now + 300,
        "jti": str(uuid.uuid4()),
    }
    client_jwt = _mint_jwt({**base, "sub": CLIENT_ID}, key)
    user_jwt   = _mint_jwt({**base, "sub": SERVICE_USER_EMAIL}, key)

    print(f"→ POST {token_url}")
    r = requests.post(
        token_url,
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": user_jwt,
            "client_assertion_type":
                "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": client_jwt,
            "scope": SCOPE,
        },
        timeout=30,
    )
    if not r.ok:
        print(f"✗ {r.status_code}: {r.text}")
        raise SystemExit(1)

    data = r.json()
    access_token = data["access_token"]
    print(f"✓ access_token (TTL={data.get('expires_in', '?')}s)")
    print(f"  first 30: {access_token[:30]}...")

    print()
    print("Test against /api/mcp:")
    print(f"  curl -i -X POST -H 'Authorization: Bearer {access_token[:20]}...' \\")
    print(f"    -H 'Content-Type: application/json' \\")
    print(f"    -H 'Accept: application/json, text/event-stream' \\")
    print(f"    -d '{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{{}}}}' \\")
    print(f"    https://{OAC_HOST}/api/mcp")
    print()
    print("Full token (paste into curl):")
    print(access_token)


if __name__ == "__main__":
    main()
