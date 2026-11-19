"""OCI signer + region helpers.

Self-contained so the dbx_export package doesn't take a hard dependency on
scripts/aidp_executor.py. Mirrors the auth logic there (API key or security
token), per the project's API-key-only feedback memory.
"""

from __future__ import annotations

import os

import oci

# Runtime-set globals. Populated from customer YAML by every entry-point
# script that imports this module — never hardcoded to a specific customer.
# `DEFAULT_OCI_PROFILE = "DEFAULT"` is the OCI convention (the profile named
# "DEFAULT" in ~/.oci/config), not a pilot deployment-specific value.
DEFAULT_OCI_PROFILE = "DEFAULT"
DEFAULT_LAKE_OCID: str | None = None
DEFAULT_WORKSPACE_ID: str | None = None

# Legacy 3-letter region codes -> canonical OCI region names. MUST stay in
# sync with scripts/aidp_executor.py::REGION_MAP: both maps feed
# region_from_ocid-style helpers whose result selects the AIDP API endpoint
# (https://aidp.<region>.oci.oraclecloud.com) — if they disagree, the
# exporter and the migrator resolve the same lake OCID to different control
# planes. A test asserts they are identical
# (tests/test_cluster_shape_mapping.py).
REGION_MAP = {
    "iad": "us-ashburn-1", "phx": "us-phoenix-1", "fra": "eu-frankfurt-1",
    "lhr": "uk-london-1", "bom": "ap-mumbai-1", "hyd": "ap-hyderabad-1",
    "sin": "ap-singapore-1", "nrt": "ap-tokyo-1", "syd": "ap-sydney-1",
    "gru": "sa-saopaulo-1", "yyz": "ca-toronto-1", "icn": "ap-seoul-1",
}


def _resolve_config_path(explicit: str | None) -> str | None:
    """Pick the OCI config file to load.

    Precedence:
      1. Explicit --oci-config-path argument
      2. $OCI_CONFIG_FILE env var
      3. ~/.oci/config (laptop default)
    Returns None to let oci.config.from_file fall back to its own default.

    When running from an AIDP notebook, callers should set the env var
    OCI_CONFIG_FILE (or pass --oci-config-path) to point at their
    workspace-resident OCI config — paths vary per user / deployment.
    """
    if explicit:
        return os.path.expanduser(explicit)
    env_path = os.environ.get("OCI_CONFIG_FILE")
    if env_path:
        return os.path.expanduser(env_path)
    laptop = os.path.expanduser("~/.oci/config")
    if os.path.isfile(laptop):
        return laptop
    return None


def _get_aidputils_from_kernel():
    """Return the AIDP kernel's `aidputils` global, or None if not on cluster.

    AIDP injects `aidputils` into `__main__`'s namespace on kernel startup.
    It is NOT installable as a Python module — `import aidputils` raises
    ModuleNotFoundError on cluster. It is also NOT in `builtins`. So
    module-scope code can't reach it via bare name or `import`; only by
    looking it up on `sys.modules['__main__']` (validated on clusters
    c5eeec7a + cb027817 2026-08-25). Returns None on laptop where
    `__main__` has no `aidputils` attribute.

    %run caveat: this module's own documented deployment is
    `%run dbx_export/run.py` from a notebook cell. Under IPython %run
    semantics, sys.modules['__main__'] is SWAPPED to a fresh module for the
    script's duration (IPython core/magics/execution.py), which hides the
    kernel-injected global — the __main__ lookup alone would silently take
    the laptop fallback ON CLUSTER (and, with --oci-config-path passed per
    run.py's example, silently read a workspace config file — defeating
    Phase 2). The kernel's user namespace still holds the global, so fall
    back to get_ipython().user_ns. Harmless on laptop: get_ipython() is
    absent or returns None outside IPython, and a laptop IPython session
    has no `aidputils` in user_ns either.
    """
    import sys
    aidputils = getattr(sys.modules.get("__main__"), "aidputils", None)
    if aidputils is None:
        try:
            from IPython import get_ipython  # type: ignore[import-not-found]
            _ip = get_ipython()
            if _ip is not None:
                aidputils = getattr(_ip, "user_ns", {}).get("aidputils")
        except ImportError:
            pass
    return aidputils


def get_oci_signer(profile: str = DEFAULT_OCI_PROFILE, config_path: str | None = None):
    """Build an OCI signer. Supports two contexts:

    1. Cluster (dbx_export runs from a notebook cell — `%run dbx_export/run.py`):
       AIDP kernel has `aidputils` on `__main__`; we fetch OCI signer keys
       from the AIDP Credential Store via aidputils.secrets.get(). NO OCI
       config file read on cluster. Returns (config_dict, signer).
    2. Laptop dev / customer with direct DBX access: standard OCI SDK
       resolution (--oci-config-path arg → $OCI_CONFIG_FILE → ~/.oci/config).
       Supports both API key and security_token_file profiles.

    Cluster path uses the same `AIDP_CRED_OCI_NAME` / `AIDP_CRED_OCI_KEY_*`
    env-var mechanism as aidp_compat (populated by the migrator's kernel
    bootstrap from cfg.aidp.oci_credentials), so per-customer credential
    naming works without editing this module.
    """
    aidputils = _get_aidputils_from_kernel()
    if aidputils is not None:
        # Cluster path — fetch from AIDP Credential Store, no config file.
        if config_path:
            import sys as _sys
            print("[dbx_export.auth] NOTE: --oci-config-path is ignored on "
                  "cluster — OCI credentials come from the AIDP Credential "
                  f"Store ({os.environ.get('AIDP_CRED_OCI_NAME') or 'migration_tool_api'}).",
                  file=_sys.stderr)
        cred = os.environ.get("AIDP_CRED_OCI_NAME") or "migration_tool_api"
        def _k(f):
            return os.environ.get("AIDP_CRED_OCI_KEY_" + f.upper()) or {
                "user": "user", "fingerprint": "fingerprint",
                "tenancy": "tenancy", "region": "region",
                "private_key_content": "key_file_content",
            }[f]
        tenancy     = aidputils.secrets.get(cred, key=_k("tenancy")).strip()
        user        = aidputils.secrets.get(cred, key=_k("user")).strip()
        fingerprint = aidputils.secrets.get(cred, key=_k("fingerprint")).strip()
        region      = aidputils.secrets.get(cred, key=_k("region")).strip()
        key_content = aidputils.secrets.get(cred, key=_k("private_key_content"))
        signer = oci.signer.Signer(
            tenancy=tenancy, user=user, fingerprint=fingerprint,
            private_key_file_location=None, private_key_content=key_content,
        )
        config = {"region": region, "tenancy": tenancy, "user": user,
                  "fingerprint": fingerprint, "key_content": key_content}
        return config, signer

    # Laptop dev / non-AIDP fallback — standard OCI SDK resolution.
    resolved = _resolve_config_path(config_path)
    if resolved:
        config = oci.config.from_file(file_location=resolved, profile_name=profile)
    else:
        config = oci.config.from_file(profile_name=profile)
    if config.get("security_token_file"):
        from oci.auth.signers import SecurityTokenSigner

        token_path = os.path.expanduser(config["security_token_file"])
        with open(token_path) as f:
            token = f.read().strip()
        private_key = oci.signer.load_private_key_from_file(
            os.path.expanduser(config["key_file"]),
            config.get("pass_phrase"),
        )
        signer = SecurityTokenSigner(token=token, private_key=private_key)
    else:
        signer = oci.signer.Signer(
            tenancy=config["tenancy"],
            user=config["user"],
            fingerprint=config["fingerprint"],
            private_key_file_location=config["key_file"],
            private_key_content=config.get("key_content"),
        )
    return config, signer


def region_from_ocid(lake_ocid: str) -> str:
    """OCID format: ocid1.<rt>.<realm>.<region>.<unique>. parts[3] is region.

    Raises ValueError for malformed OCIDs. The region selects the AIDP API
    endpoint, so silently defaulting to some region would send a typo'd
    OCID's requests to the wrong control plane. Fail loud at the source.
    """
    parts = lake_ocid.split(".")
    if len(parts) > 3 and parts[3]:
        region = parts[3]
        return REGION_MAP.get(region, region)
    raise ValueError(
        f"malformed lake OCID {lake_ocid!r}: expected "
        f"'ocid1.<type>.<realm>.<region>.<id>' (cannot derive a region)")


def aidp_base_url(lake_ocid: str) -> str:
    region = region_from_ocid(lake_ocid)
    return f"https://aidp.{region}.oci.oraclecloud.com/20240831/dataLakes/{lake_ocid}"


def aidp_workspace_url(lake_ocid: str, workspace_id: str) -> str:
    return f"{aidp_base_url(lake_ocid)}/workspaces/{workspace_id}"
