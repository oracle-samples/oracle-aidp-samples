"""
AIDP Secrets Utils - Replacement for dbutils.secrets
Uses OCI Vault for secret storage.
"""

import os
import json


class AIDPSecretsUtils:
    """Drop-in replacement for dbutils.secrets using OCI Vault."""

    def __init__(self):
        self._cache = {}
        self._load_env_secrets()

    def _load_env_secrets(self):
        """Load secrets from environment variables as fallback.
        Format: AIDP_SECRET_<SCOPE>_<KEY>=value
        """
        for key, value in os.environ.items():
            if key.startswith("AIDP_SECRET_"):
                parts = key[12:].split("_", 1)
                if len(parts) == 2:
                    scope, secret_key = parts[0].lower(), parts[1].lower()
                    self._cache.setdefault(scope, {})[secret_key] = value

    def get(self, scope: str, key: str) -> str:
        """Get a secret value."""
        # Check cache/env first
        if scope in self._cache and key in self._cache[scope]:
            return self._cache[scope][key]

        # Try OCI Vault
        try:
            return self._get_from_oci_vault(scope, key)
        except Exception:
            pass

        # Check for a secrets file
        secrets_file = os.environ.get("AIDP_SECRETS_FILE", "/opt/aidp/config/secrets.json")
        if os.path.exists(secrets_file):
            with open(secrets_file) as f:
                secrets = json.load(f)
                if scope in secrets and key in secrets[scope]:
                    return secrets[scope][key]

        raise KeyError(f"Secret not found: scope={scope}, key={key}")

    def getBytes(self, scope: str, key: str) -> bytes:
        """Get a secret as bytes."""
        return self.get(scope, key).encode('utf-8')

    def list(self, scope: str):
        """List secrets in a scope (metadata only, not values)."""
        results = []
        if scope in self._cache:
            for key in self._cache[scope]:
                results.append({"key": key, "lastUpdatedTimestamp": 0})
        return results

    def listScopes(self):
        """List available secret scopes."""
        return [{"name": scope} for scope in self._cache.keys()]

    def _get_from_oci_vault(self, scope: str, key: str) -> str:
        """Retrieve secret from OCI Vault service.

        Uses OCI API key auth via the CLI config file at
        /Workspace/<oci-config-workspace-path> (DEFAULT profile). Override
        via OCI_CONFIG_FILE / OCI_CONFIG_PROFILE env vars.

        NEVER uses oci.auth.signers.get_resource_principals_signer() â€” resource
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
        print("  list(scope) - List secret metadata in scope")
        print("  listScopes() - List available scopes")
