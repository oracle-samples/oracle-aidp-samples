"""Single source of truth for OCI auth — every script builds its signer here.

Precedence (maintainer policy, 2026-10-12: the AIDP Credential Store is
canonical; config files are the laptop fallback only):
  1. AIDP Credential Store (cluster-side; via the kernel-injected `aidputils`
     through scripts/oci_credential_helper.py, driven by the YAML's
     `aidp.oci_credentials` block). Off-cluster this raises ImportError —
     that ImportError IS the "am I on a laptop?" signal.
  2. $OCI_CONFIG_FILE env var (explicit file override).
  3. ~/.oci/config (laptop default), incl. security-token profiles.

History: this implementation moved verbatim from aidp_executor.get_oci_signer
(which re-exports it — every existing caller and test binding is unchanged).
Before 2026-10-12, cluster_lifecycle and five standalone utilities each built
their own file-only signer, which forced a materialized key file onto AIDP
clusters and duplicated the auth logic in seven places. Do NOT add new
`oci.config.from_file` call sites elsewhere — delegate to get_oci_signer().

The auth source is printed once per construction ("every silent tool decision
is surfaced"): a config-file signer built in a cluster-like environment (AIDP
env markers present) is a loud WARN, because on a cluster the Credential Store
should have served the request.
"""
import os

import oci

DEFAULT_OCI_PROFILE = "DEFAULT"


def _apply_cred_env_from_config(config_path=None):
    """Populate the AIDP_CRED_* env vars from the discovered YAML's
    `aidp.oci_credentials` block (credential name + key mapping), so the
    cluster-side Credential-Store signer uses the customer's names without the
    operator setting env vars by hand — the YAML is the single source.

    The discoverable YAML is AUTHORITATIVE and is re-resolved on EVERY call:
    when a config with a credential name is discoverable, the FULL AIDP_CRED_*
    set (name + all key fields) is (re)written from it. This deliberately does
    NOT early-return on a pre-existing AIDP_CRED_OCI_NAME, because that made the
    helper (a) sticky to the first config in a process — a later config change
    never took effect — and (b) trust a partially-populated env (name set, keys
    missing) and skip the YAML's key mapping, so the wrong key names were
    requested. Both were review-reproduced (#129 review, o-vazunnavar). A
    pre-existing env is respected ONLY when NO config is discoverable (e.g. a
    bootstrap-set env on a cluster with no YAML) — see the discover_config_path
    branch below. Config discovery honors $AIDP_CONFIG_PATH; an explicit
    ``config_path`` (the caller's --config) takes precedence over it — the
    loader's own CLI-over-env ordering (PR #234 Param review: a utility's
    --config B.yaml must select B's credential mapping, not a discovered A's).

    FAIL-LOUD on an INVALID config: a config file that IS present but is
    malformed / schema-invalid must NOT be swallowed into the default-credential
    fallback — that would surface downstream as a confusing 401 auth failure
    instead of the real "your config is broken" error. So we distinguish
    genuinely-absent (discover_config_path raises → silent return) from
    parse/validation errors (load_config re-raises ConfigError → propagate).
    (a reviewer #127-follow-up review P2.)
    """
    try:
        from aidp_config import load_config, discover_config_path, ConfigError
    except ImportError:
        return  # aidp_config not importable — respect any pre-set env; from_file path handles auth
    try:
        discover_config_path(cli_config_path=config_path)   # raises ConfigError ONLY when no config exists
    except ConfigError:
        # No config discoverable → respect a pre-existing (e.g. bootstrap-set) env
        # and otherwise fall back to defaults. Never clobber what we can't re-derive.
        return
    # A config file EXISTS and is authoritative: re-resolve it and (over)write the
    # full AIDP_CRED_* set below. Parse/schema errors fail loud (ConfigError).
    _oc = getattr(getattr(load_config(cli_config_path=config_path), "aidp", None),
                  "oci_credentials", None)
    if not _oc or not getattr(_oc, "name", None):
        return                   # valid config that simply doesn't set custom names → defaults
    os.environ["AIDP_CRED_OCI_NAME"] = _oc.name
    _keys = getattr(_oc, "keys", None)
    for _field in ("user", "fingerprint", "tenancy", "region", "private_key_content"):
        _k = getattr(_keys, _field, None) if _keys is not None else None
        if _k:
            os.environ["AIDP_CRED_OCI_KEY_" + _field.upper()] = _k


def _surface_auth_source(source: str) -> None:
    """One line per signer build naming the auth source; WARN when a config
    file served a request in a cluster-like environment (AIDP env markers),
    where the Credential Store should have answered."""
    if source == "file" and (os.environ.get("AIDP_ID") or os.environ.get("AIDP_AUTH")):
        print("[oci-auth] WARNING: OCI config FILE used in a cluster-like environment — "
              "the AIDP Credential Store is canonical on clusters; check aidputils / "
              "the YAML's aidp.oci_credentials block")
    elif source == "file":
        print("[oci-auth] signer: OCI config file (laptop fallback)")
    else:
        print("[oci-auth] signer: AIDP Credential Store (cluster)")


def get_oci_signer(profile: str = DEFAULT_OCI_PROFILE, config_path: str = None):
    """Create OCI signer — supports API key and security token auth.

    Returns (config, signer); config is None on the Credential-Store path.
    ``config_path``: the caller's explicit --config YAML; its credential
    mapping wins over any discovered/env YAML on the store path (loader's
    CLI-over-env precedence).

    Detects auth type from the OCI config profile:
      security_token_file present → SecurityTokenSigner (oci session authenticate)
      otherwise                   → API key Signer (fingerprint + key_file)

    Config file lookup precedence (matches dbx_export/auth.py):
      1. $OCI_CONFIG_FILE env var — set this when running on AIDP where
         there is no ~/.oci/config (e.g. /Workspace/<team>/oci/config)
      2. ~/.oci/config (laptop default; oci.config.from_file's own fallback)
    """
    # ── Cluster-side: build the signer from the AIDP Credential Store, driven by
    #    the YAML's `aidp.oci_credentials` block (name + key map). There is no
    #    ~/.oci/config on an AIDP cluster. Off-cluster (no kernel `aidputils`) the
    #    helper raises ImportError and we fall through to the laptop from_file path
    #    below — so laptop runs are byte-for-byte unchanged. Mirrors dbx_export/auth.py.
    try:
        _apply_cred_env_from_config(config_path)          # AIDP_CRED_* ← cfg.aidp.oci_credentials (idempotent)
        from oci_credential_helper import get_signer as _cluster_signer
        signer = _cluster_signer()                        # raises ImportError on a laptop (no aidputils)
        _surface_auth_source("credential-store")
        return None, signer
    except ImportError as _e:
        # ONLY the aidputils-missing signal means "laptop" (PR #234 review F1).
        # Any other ImportError inside the store path is a broken cluster
        # environment: fail loud there instead of silently degrading to a file.
        # (Store LOOKUP failures — missing credential/keys — are not
        # ImportError and always propagate.)
        #
        # ── ACCEPTED EXCEPTION / TEMPORARY — revisit with the Scala aidputils
        #    migration (same milestone as the "FOR SCALA ONLY" blocks in
        #    job_migrate.py) ─────────────────────────────────────────────────
        # Maintainer decision (2026-10-12, PR #234): a cluster-like
        # host where aidputils is CLEANLY ABSENT (non-kernel python process,
        # or a deliberately staged config such as check_data_availability
        # --oci-config-path / '/Workspace/<team>/oci/config') may still fall
        # through to file auth — with the WARN below, never silently. Policy
        # is credential-store-only for Python; this exception exists so the
        # consolidation ships without breaking evidenced deployments. The
        # WARN line is the monitoring signal: when a full migration cycle
        # shows zero '[oci-auth] WARNING' sightings, flip this fall-through
        # to a hard failure in the SAME change that retires the Scala
        # file-based grandfathering (grep: ACCEPTED EXCEPTION, FOR SCALA ONLY).
        # Pinned by test_accepted_exception_cluster_file_fallback_warns —
        # changing this behavior must be deliberate, not incidental.
        if ("aidputils" not in str(_e)
                and (os.environ.get("AIDP_ID") or os.environ.get("AIDP_AUTH"))):
            raise
    cfg_env = os.environ.get("OCI_CONFIG_FILE")
    if cfg_env:
        config = oci.config.from_file(file_location=os.path.expanduser(cfg_env), profile_name=profile)
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
            # Encrypted laptop keys: prepare_validation_cluster passed this before
            # consolidation (PR #234 review F2); the shared helper must be a superset.
            pass_phrase=config.get("pass_phrase"),
        )

    _surface_auth_source("file")
    return config, signer
