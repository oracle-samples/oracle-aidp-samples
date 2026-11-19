"""
AIDP S3 Compat - Route S3 object reads/writes through OCI Object Storage.
Translates S3 bucket names to OCI using the bucket mapping CSV.
"""
import os
import csv
from typing import Optional

# Path to bucket mapping (relative to this file's package location or absolute)
_MAPPING_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "reports", "s3_to_oci_bucket_mapping.csv"
)

_bucket_map: Optional[dict] = None
_oci_client = None  # cached ObjectStorageClient â€” avoids re-reading config + key on every call

# Standard AIDP OCI CLI config location. Override via OCI_CONFIG_FILE / OCI_CONFIG_PROFILE
# env vars for non-default deployments.
_AIDP_OCI_CONFIG_FILE = os.environ.get(
    "OCI_CONFIG_FILE", "/Workspace/<oci-config-workspace-path>"
)
_AIDP_OCI_CONFIG_PROFILE = os.environ.get("OCI_CONFIG_PROFILE", "DEFAULT")


def _load_mapping() -> dict:
    """Load S3->OCI bucket mapping from CSV. Cached after first load."""
    global _bucket_map
    if _bucket_map is not None:
        return _bucket_map
    _bucket_map = {}
    if not os.path.exists(_MAPPING_FILE):
        return _bucket_map
    with open(_MAPPING_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # CSV columns: s3_bucket, oci_bucket, oci_namespace
            s3_bucket = row.get("s3_bucket", "").strip()
            oci_bucket = row.get("oci_bucket", "").strip()
            namespace = row.get("oci_namespace", "").strip()
            if s3_bucket:
                _bucket_map[s3_bucket] = {
                    "oci_bucket": oci_bucket,
                    "namespace": namespace,
                }
    return _bucket_map


def _translate_bucket(s3_bucket: str) -> tuple:
    """Translate an S3 bucket name to (oci_bucket, namespace).

    Returns:
        (oci_bucket_name, namespace) tuple

    Raises:
        KeyError: If bucket not found in mapping
    """
    mapping = _load_mapping()
    if s3_bucket not in mapping:
        known = list(mapping.keys())
        suffix = "..." if len(known) > 10 else ""
        raise KeyError(
            f"S3 bucket '{s3_bucket}' not found in OCI bucket mapping. "
            f"Known buckets: {known[:10]}{suffix}"
        )
    entry = mapping[s3_bucket]
    return entry["oci_bucket"], entry["namespace"]


def _get_oci_client():
    """Return a cached OCI ObjectStorageClient using API key auth.

    Reads OCI CLI config from _AIDP_OCI_CONFIG_FILE (default
    /Workspace/<oci-config-workspace-path>, profile DEFAULT) and constructs
    a Signer from the referenced API key. Override via OCI_CONFIG_FILE /
    OCI_CONFIG_PROFILE env vars.

    NEVER uses oci.auth.signers.get_resource_principals_signer() â€” resource
    principal has known failure modes on AIDP and is forbidden by project
    policy.

    Caching avoids re-reading config + key file on every read/write call.
    The client is constructed on first use only.
    """
    global _oci_client
    if _oci_client is None:
        import oci
        config = oci.config.from_file(_AIDP_OCI_CONFIG_FILE, _AIDP_OCI_CONFIG_PROFILE)
        signer = oci.signer.Signer(
            tenancy=config["tenancy"],
            user=config["user"],
            fingerprint=config["fingerprint"],
            private_key_file_location=config["key_file"],
            pass_phrase=oci.config.get_config_value_or_default(config, "pass_phrase"),
        )
        _oci_client = oci.object_storage.ObjectStorageClient(config=config, signer=signer)
    return _oci_client


def read_s3_object(bucket: str, key: str) -> bytes:
    """Read an S3 object by routing through OCI Object Storage.

    Translates the S3 bucket name to OCI using the bucket mapping,
    then reads via the OCI Object Storage client with API key auth.

    Args:
        bucket: S3 bucket name
        key: S3 object key

    Returns:
        Object contents as bytes
    """
    oci_bucket, namespace = _translate_bucket(bucket)
    client = _get_oci_client()
    response = client.get_object(namespace_name=namespace, bucket_name=oci_bucket, object_name=key)
    return response.data.content


def write_s3_object(bucket: str, key: str, data: bytes) -> None:
    """Write bytes to OCI Object Storage using the S3 bucket mapping.

    Args:
        bucket: S3 bucket name
        key: S3 object key
        data: Bytes to write
    """
    import io
    oci_bucket, namespace = _translate_bucket(bucket)
    client = _get_oci_client()
    client.put_object(
        namespace_name=namespace,
        bucket_name=oci_bucket,
        object_name=key,
        put_object_body=io.BytesIO(data),
    )
