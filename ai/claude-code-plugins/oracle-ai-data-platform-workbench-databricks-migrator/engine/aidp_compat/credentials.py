"""AIDP Credentials Utils - Replacement for dbutils.credentials"""
import os

class AIDPCredentialsUtils:
    def assumeRole(self, role: str) -> bool:
        print(f"[AIDP] assumeRole not supported. Use OCI API key auth (config file) instead.")
        print(f"[AIDP] Requested role: {role}")
        os.environ["AIDP_ASSUMED_ROLE"] = role
        return True

    def showCurrentRole(self):
        return [os.environ.get("AIDP_ASSUMED_ROLE", "OCI API Key (config file)")]

    def showRoles(self):
        return [os.environ.get("AIDP_ASSUMED_ROLE", "OCI API Key (config file)")]

    def getServiceCredentialsProvider(self, credentialName: str):
        print(f"[AIDP] Using OCI API key auth for credential: {credentialName}")
        return None

    def help(self, method=None):
        print("dbutils.credentials - AIDP Credential Utils")
        print("  Note: AIDP uses OCI API key auth via config file at")
        print("  /Workspace/<oci-config-workspace-path> (DEFAULT profile).")
        print("  Resource/instance principal auth is NOT used on AIDP.")
