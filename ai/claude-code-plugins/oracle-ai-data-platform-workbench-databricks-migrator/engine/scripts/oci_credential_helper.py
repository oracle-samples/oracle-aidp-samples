"""OCI credential fetcher — cluster-side only. Uses aidputils.secrets.

The migrator's cluster-side code (migrated cells, injected snippets, aidp_compat
overrides) calls get_signer() / get_secret(key) at each point of use. Every
call fetches fresh from the AIDP Credential Store — no caching, no disk write,
no env vars pointing at file paths.

Laptop-side code MUST NOT import this module; laptop-side uses standard OCI SDK
resolution via `~/.oci/config` with the profile named in `cfg.aidp.oci_profile`.

Credentials expected in AIDP Credential Store:
  - migration_tool_api      — 5 keys (user, fingerprint, tenancy, region, key_file_content)
  - migration_tool_secrets  — mlflow-tracking-uri, entity_crypto-arn (2+ arbitrary keys)

Credential names are read from env vars set by the migrator's kernel bootstrap
from cfg.aidp.oci_credentials.name / cfg (both defaulting to the values above),
so customers can rename their credentials in AIDP UI without editing this file.

Scala credential-store counterpart: NOT implemented. aidputils.secrets is
not supported in Scala on the compute image live-tested 2026-10-13. Native
aidputils.widgets is supported in Scala. Scala helpers retain file-based OCI
config until Scala secrets support is available and the integration is validated
(tracked as phase-3).
"""

from __future__ import annotations

import os

# Env var overrides let the migrator's kernel bootstrap pass names from cfg
# without this module needing to load / see the YAML directly. Defaults match
# the names the pilot deployment created in AIDP Credential Store.
_CRED_OCI_NAME_ENV = "AIDP_CRED_OCI_NAME"
_CRED_SECRETS_NAME_ENV = "AIDP_CRED_SECRETS_NAME"
_DEFAULT_CRED_OCI = "migration_tool_api"
_DEFAULT_CRED_SECRETS = "migration_tool_secrets"

# LHS = OCI Signer field name; RHS = the default key name inside migration_tool_api.
# The YAML's `aidp.oci_credentials.keys` block can override the RHS at cluster
# bootstrap time (via env vars in the same style as above).
_OCI_KEY_ENV_PREFIX = "AIDP_CRED_OCI_KEY_"
_DEFAULT_OCI_KEYS = {
    "user": "user",
    "fingerprint": "fingerprint",
    "tenancy": "tenancy",
    "region": "region",
    "private_key_content": "key_file_content",
}


def _cred_oci_name() -> str:
    return os.environ.get(_CRED_OCI_NAME_ENV) or _DEFAULT_CRED_OCI


def _cred_secrets_name() -> str:
    return os.environ.get(_CRED_SECRETS_NAME_ENV) or _DEFAULT_CRED_SECRETS


def _oci_key(field: str) -> str:
    return os.environ.get(_OCI_KEY_ENV_PREFIX + field.upper()) or _DEFAULT_OCI_KEYS[field]


def _get_aidputils_from_kernel():
    """Return the AIDP kernel's `aidputils` global.

    AIDP injects `aidputils` into `__main__`'s namespace on kernel startup.
    It is NOT installable as a Python module — `import aidputils` raises
    ModuleNotFoundError on cluster. It is also NOT in `builtins`. So
    module-scope code (like this file) can't reach it via bare name or
    via `import`; only by looking it up on `sys.modules['__main__']`
    (validated 2026-08-25). Raises ImportError with a clear message if
    aidputils is missing — laptop-side callers should not use this module.
    """
    import sys
    aidputils = getattr(sys.modules.get("__main__"), "aidputils", None)
    if aidputils is None:
        # IPython-semantics %run swaps sys.modules['__main__'] to a fresh
        # module for the script's duration, hiding kernel-injected globals.
        # The kernel's user namespace still holds them (see dbx_export/auth.py
        # for the full rationale). Harmless off-cluster.
        try:
            from IPython import get_ipython  # type: ignore[import-not-found]
            _ip = get_ipython()
            if _ip is not None:
                aidputils = getattr(_ip, "user_ns", {}).get("aidputils")
        except ImportError:
            pass
    if aidputils is None:
        raise ImportError(
            "aidputils global not found on __main__ — oci_credential_helper "
            "is cluster-only. Laptop callers should use `oci.config.from_file` "
            "with cfg.aidp.oci_profile instead.")
    return aidputils


def get_signer():
    """Fresh OCI Signer built from the migration_tool_api credential.

    Cluster-side only. Raises ImportError if aidputils is not available
    on the kernel (laptop-side callers should not import this module —
    use `oci.config.from_file` with the profile from cfg.aidp.oci_profile
    instead).
    """
    import oci.signer

    aidputils = _get_aidputils_from_kernel()
    cred = _cred_oci_name()
    return oci.signer.Signer(
        tenancy=aidputils.secrets.get(cred, key=_oci_key("tenancy")).strip(),
        user=aidputils.secrets.get(cred, key=_oci_key("user")).strip(),
        fingerprint=aidputils.secrets.get(cred, key=_oci_key("fingerprint")).strip(),
        private_key_file_location=None,
        private_key_content=aidputils.secrets.get(cred, key=_oci_key("private_key_content")),
    )


def get_region() -> str:
    """Fetch the OCI region string from the migration_tool_api credential."""
    aidputils = _get_aidputils_from_kernel()
    return aidputils.secrets.get(_cred_oci_name(), key=_oci_key("region")).strip()


def get_secret(key: str) -> str:
    """Fetch a secret value from the migration_tool_secrets credential by key name.

    Used for cluster-runtime-only values (mlflow_tracking_uri, entity_crypto_arn, ...).
    """
    aidputils = _get_aidputils_from_kernel()
    return aidputils.secrets.get(_cred_secrets_name(), key=key)
