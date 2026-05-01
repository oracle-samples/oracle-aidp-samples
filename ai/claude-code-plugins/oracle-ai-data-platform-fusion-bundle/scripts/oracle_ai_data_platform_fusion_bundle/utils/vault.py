"""Tiny helper to fetch an OCI Vault secret by OCID and decode its base64 payload.

Used for resolving ``${vault:ocid1.vaultsecret...}`` references in bundle.yaml.
Supports a ``--mock-secret`` mode (env var ``AIDP_FUSION_BUNDLE_MOCK_SECRETS``)
so tests don't need real Vault access.
"""

from __future__ import annotations

import base64
import os
import re
from typing import Any

VAULT_REF_RE = re.compile(r"^\$\{vault:(ocid1\.vaultsecret\.[A-Za-z0-9._-]+)\}$")


def is_vault_ref(value: str) -> bool:
    return bool(VAULT_REF_RE.match(value or ""))


def parse_ocid(value: str) -> str:
    match = VAULT_REF_RE.match(value or "")
    if not match:
        raise ValueError(f"not a ${{vault:...}} reference: {value!r}")
    return match.group(1)


def fetch_secret(secret_ocid: str, *, oci_profile: str = "DEFAULT") -> str:
    """Fetch an OCI Vault secret bundle and return the decoded UTF-8 string.

    The bundle CLI calls this once at startup for each unique vault OCID
    and caches the result in-memory for the lifetime of the process.

    Test-mode override: set ``AIDP_FUSION_BUNDLE_MOCK_SECRETS`` to a
    ``ocid1=value;ocid2=value`` string and this returns the matching value
    without touching OCI.
    """
    mock_env = os.environ.get("AIDP_FUSION_BUNDLE_MOCK_SECRETS")
    if mock_env:
        for entry in mock_env.split(";"):
            if "=" not in entry:
                continue
            key, _, val = entry.partition("=")
            if key.strip() == secret_ocid:
                return val
        raise KeyError(
            f"AIDP_FUSION_BUNDLE_MOCK_SECRETS has no entry for {secret_ocid}"
        )

    # Lazy import so unit tests that exercise mock mode don't need oci installed.
    import oci  # type: ignore[import-not-found]

    config = oci.config.from_file(profile_name=oci_profile)
    client = oci.secrets.SecretsClient(config)
    bundle: Any = client.get_secret_bundle(secret_id=secret_ocid).data
    content = bundle.secret_bundle_content
    if getattr(content, "content_type", None) != "BASE64":
        raise RuntimeError(
            f"vault secret {secret_ocid} has unsupported content_type: "
            f"{getattr(content, 'content_type', None)!r}"
        )
    return base64.b64decode(content.content).decode("utf-8")


def resolve(value: str, *, oci_profile: str = "DEFAULT") -> str:
    """Resolve a string that *may* be a ``${vault:OCID}`` reference.

    Returns the input unchanged if it isn't a vault ref.
    """
    if not is_vault_ref(value):
        return value
    return fetch_secret(parse_ocid(value), oci_profile=oci_profile)


__all__ = ["is_vault_ref", "parse_ocid", "fetch_secret", "resolve"]
