"""Build an OCI config dict from an inline PEM string.

The standard ``oci.config.from_file`` reads a key file from disk. In an AIDP
notebook, secret files often live on /Workspace (FUSE), which intermittently
disconnects with Errno 107. Passing the PEM body inline as a Python string
avoids the FUSE round-trip entirely.
"""

from __future__ import annotations

from typing import Optional


def from_inline_pem(
    user_ocid: str,
    tenancy_ocid: str,
    fingerprint: str,
    private_key_pem: str,
    region: str,
    pass_phrase: Optional[str] = None,
) -> dict:
    """Return an OCI config dict suitable for ``oci.<service>Client(config=...)``.

    Args:
        user_ocid: ``ocid1.user.oc1...`` of the OCI user whose API key this is.
        tenancy_ocid: ``ocid1.tenancy.oc1...``.
        fingerprint: API key fingerprint (e.g. ``aa:bb:cc:...``).
        private_key_pem: PEM-encoded private key body, including the
            ``-----BEGIN PRIVATE KEY-----`` / ``-----END PRIVATE KEY-----``
            markers and the newlines between them.
        region: e.g. ``us-ashburn-1``.
        pass_phrase: Passphrase for the private key, if any.

    Returns:
        A dict that the OCI Python SDK accepts as the ``config`` argument.
        The SDK reads the key from the ``key_content`` field rather than
        ``key_file``, so no on-disk file is created.
    """
    config: dict = {
        "user": user_ocid,
        "tenancy": tenancy_ocid,
        "fingerprint": fingerprint,
        "key_content": private_key_pem,
        "region": region,
    }
    if pass_phrase is not None:
        config["pass_phrase"] = pass_phrase
    return config
