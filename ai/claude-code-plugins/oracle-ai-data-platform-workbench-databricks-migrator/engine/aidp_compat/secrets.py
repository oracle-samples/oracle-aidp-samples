"""
AIDP Secrets Utils - Replacement for dbutils.secrets
Uses OCI Vault for secret storage.

Default is Vault-only. Plaintext sources (``AIDP_SECRET_<SCOPE>_<KEY>``
environment variables and the JSON file named by ``AIDP_SECRETS_FILE``) are a
demo/development fallback that must be opted into with
``AIDP_ALLOW_PLAINTEXT_SECRETS=1``; the JSON file must be owner-only (0600)
on POSIX. ``AIDP_SECRET_SCOPES`` (comma list) restricts which scopes may be
read or listed. Values are never printed or returned by the list operations.
"""

import logging
import os
import json
import stat
from typing import Dict, List, Optional

ENV_ALLOW_PLAINTEXT = "AIDP_ALLOW_PLAINTEXT_SECRETS"
ENV_SECRETS_FILE = "AIDP_SECRETS_FILE"
ENV_SECRET_SCOPES = "AIDP_SECRET_SCOPES"
_ENV_SECRET_PREFIX = "AIDP_SECRET_"

_log = logging.getLogger("aidp_compat.secrets")


def _plaintext_allowed() -> bool:
    return os.environ.get(ENV_ALLOW_PLAINTEXT, "").strip() == "1"


def _allowed_scopes() -> Optional[frozenset]:
    """Scope allowlist from AIDP_SECRET_SCOPES, or None when unrestricted."""
    raw = os.environ.get(ENV_SECRET_SCOPES)
    if raw is None or not raw.strip():
        return None
    return frozenset(s.strip().lower() for s in raw.split(",") if s.strip())


def _check_owner_only(path: str) -> None:
    """Refuse a plaintext secrets file that is group- or world-accessible.

    POSIX only: Windows ACLs are not expressed in st_mode, so the check is
    skipped there with a one-line note.
    """
    if os.name != "posix":
        _log.info("plaintext secrets file permission check skipped on %s (no POSIX mode bits)", os.name)
        return
    mode = os.stat(path).st_mode
    if mode & 0o077:
        raise PermissionError(
            f"plaintext secrets file must be owner-only (0600), found "
            f"{stat.filemode(mode)}: {path}"
        )


class AIDPSecretsUtils:
    """Drop-in replacement for dbutils.secrets using OCI Vault."""

    def __init__(self):
        self._cache: Dict[str, Dict[str, str]] = {}
        self._plaintext = _plaintext_allowed()
        self._scopes = _allowed_scopes()
        if self._plaintext:
            # One line, no values: operators must be able to see that the
            # demo fallback is on, and logs must never carry secret material.
            _log.warning("insecure plaintext secrets mode active (%s=1): AIDP_SECRET_* "
                         "environment variables and %s are consulted; not for production",
                         ENV_ALLOW_PLAINTEXT, ENV_SECRETS_FILE)
            self._load_env_secrets()

    # ── Scope policy ──────────────────────────────────────────────────
    def _check_scope(self, scope: str) -> None:
        if self._scopes is not None and scope.lower() not in self._scopes:
            raise PermissionError(
                f"Secret scope not allowlisted: {scope} (set {ENV_SECRET_SCOPES} to allow it)"
            )

    def _scope_allowed(self, scope: str) -> bool:
        return self._scopes is None or scope.lower() in self._scopes

    # ── Plaintext fallbacks (opt-in only) ─────────────────────────────
    def _load_env_secrets(self):
        """Load secrets from environment variables (plaintext mode only).
        Format: AIDP_SECRET_<SCOPE>_<KEY>=value
        """
        for key, value in os.environ.items():
            if key.startswith(_ENV_SECRET_PREFIX):
                parts = key[len(_ENV_SECRET_PREFIX):].split("_", 1)
                if len(parts) == 2:
                    scope, secret_key = parts[0].lower(), parts[1].lower()
                    self._cache.setdefault(scope, {})[secret_key] = value

    def _plaintext_file_secret(self, scope: str, key: str) -> Optional[str]:
        """Read ``scope/key`` from the JSON file (plaintext mode only).

        The file must be named explicitly by AIDP_SECRETS_FILE and be
        owner-only on POSIX. Returns None when disabled, unset or missing.
        """
        if not self._plaintext:
            return None
        secrets_file = os.environ.get(ENV_SECRETS_FILE, "").strip()
        if not secrets_file or not os.path.isfile(secrets_file):
            return None
        _check_owner_only(secrets_file)
        with open(secrets_file, encoding="utf-8") as f:
            secrets = json.load(f)
        scope_entry = secrets.get(scope) if isinstance(secrets, dict) else None
        if isinstance(scope_entry, dict) and key in scope_entry:
            return scope_entry[key]
        return None

    # ── Public API ────────────────────────────────────────────────────
    def get(self, scope: str, key: str) -> str:
        """Get a secret value.

        Order: in-process cache (Vault results, plus AIDP_SECRET_* in
        plaintext mode) -> OCI Vault -> plaintext JSON file (plaintext mode
        only).
        """
        self._check_scope(scope)

        if scope in self._cache and key in self._cache[scope]:
            return self._cache[scope][key]

        # OCI Vault - the only source in the default configuration.
        try:
            return self._get_from_oci_vault(scope, key)
        except Exception:
            pass

        value = self._plaintext_file_secret(scope, key)
        if value is not None:
            return value

        raise KeyError(f"Secret not found: scope={scope}, key={key}")

    def getBytes(self, scope: str, key: str) -> bytes:
        """Get a secret as bytes."""
        return self.get(scope, key).encode('utf-8')

    def list(self, scope: str) -> List[dict]:
        """List secrets in a scope (metadata only, never values)."""
        self._check_scope(scope)
        results = []
        if scope in self._cache:
            for key in self._cache[scope]:
                results.append({"key": key, "lastUpdatedTimestamp": 0})
        return results

    def listScopes(self) -> List[dict]:
        """List available secret scopes (names only, filtered by AIDP_SECRET_SCOPES)."""
        return [{"name": scope} for scope in self._cache.keys() if self._scope_allowed(scope)]

    def _get_from_oci_vault(self, scope: str, key: str) -> str:
        """Retrieve secret from OCI Vault service.

        Uses OCI API key auth via the CLI config file at
        /Workspace/<oci-config-workspace-path> (DEFAULT profile). Override
        via OCI_CONFIG_FILE / OCI_CONFIG_PROFILE env vars.

        NEVER uses oci.auth.signers.get_resource_principals_signer() — resource
        principal has known failure modes on AIDP and is forbidden by project
        policy.
        """
        try:
            import oci

            config_file = os.environ.get(
                "OCI_CONFIG_FILE", "/Workspace/<oci-config-workspace-path>"
            )
            config_profile = os.environ.get("OCI_CONFIG_PROFILE", "DEFAULT")
            config = oci.config.from_file(config_file, config_profile)
            signer = oci.signer.Signer(
                tenancy=config["tenancy"],
                user=config["user"],
                fingerprint=config["fingerprint"],
                private_key_file_location=config["key_file"],
                pass_phrase=oci.config.get_config_value_or_default(config, "pass_phrase"),
            )
            vault_client = oci.vault.VaultsClient(config=config, signer=signer)
            secrets_client = oci.secrets.SecretsClient(config=config, signer=signer)

            vault_id = os.environ.get("AIDP_VAULT_OCID")
            if not vault_id:
                raise ValueError("AIDP_VAULT_OCID not set")

            # Secret name convention: <scope>/<key>
            secret_name = f"{scope}/{key}"
            compartment_id = config.get("tenancy")

            # List secrets to find the right one
            secrets = vault_client.list_secrets(
                compartment_id=compartment_id,
                vault_id=vault_id,
                name=secret_name
            ).data

            if not secrets:
                raise KeyError(f"Secret not found in vault: {secret_name}")

            secret_id = secrets[0].id
            bundle = secrets_client.get_secret_bundle(secret_id=secret_id).data
            import base64
            content = base64.b64decode(bundle.secret_bundle_content.content).decode('utf-8')

            # Cache it
            self._cache.setdefault(scope, {})[key] = content
            return content

        except ImportError:
            raise RuntimeError("OCI SDK not available for vault access")

    def help(self, method: str = None):
        print("dbutils.secrets - AIDP Secret Utils (OCI Vault)")
        print("  get(scope, key) - Get secret value")
        print("  getBytes(scope, key) - Get secret as bytes")
        print("  list(scope) - List secret metadata in scope (names only)")
        print("  listScopes() - List available scopes (names only)")
        print(f"  Plaintext env/file fallback is OFF unless {ENV_ALLOW_PLAINTEXT}=1 (demo only)")
        print(f"  {ENV_SECRET_SCOPES}=<scope,scope> restricts which scopes may be read or listed")
