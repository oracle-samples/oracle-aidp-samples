"""
Environment detection and OCI auto-configuration for the AIDP Benchmark Toolkit.

This module hides the difference between running inside AIDP Workbench
(Resource Principal auth) and running outside (~/.oci/config API key auth).

Implements:
- D9: Bucket auto-creation with graceful NotAuthorized fallback
- D10: Namespace + region auto-detection via Resource Principal (no hardcoded values)
- D12 (decision-log): Resource Principal first, ~/.oci/config fallback
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

# Python `oci` SDK is only needed on the LOCAL path (~/.oci/config auth).
# On AIDP, generator.py + benchmark.py use the Java OCI SDK via py4j and never
# touch this module's OCI code. Import oci lazily inside the functions that use
# it so that `from aidp_benchmark import ...` works on AIDP clusters even when
# Python `oci` is not installed.
if TYPE_CHECKING:
    from oci.object_storage import ObjectStorageClient
else:
    ObjectStorageClient = "ObjectStorageClient"  # forward ref for typing

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AIDPEnvironment:
    """Resolved OCI environment — namespace, region, compartment, auth signer."""

    namespace: str
    region: str
    compartment_id: str
    is_resource_principal: bool

    def __repr__(self) -> str:
        rp = "ResourcePrincipal" if self.is_resource_principal else "API key"
        return (f"AIDPEnvironment(namespace={self.namespace!r}, region={self.region!r}, "
                f"compartment_id=...{self.compartment_id[-12:]!r}, auth={rp})")


def _try_resource_principal():
    """Attempt auto-auth in OCI compute (AIDP cluster nodes).

    Tries two signers in order:
    1. Resource Principal — works in OCI Functions and some AIDP contexts
    2. Instance Principal — works on OCI Compute instances (AIDP cluster nodes ARE these)
    """
    import oci
    from oci.object_storage import ObjectStorageClient
    signer = None

    try:
        signer = oci.auth.signers.get_resource_principals_signer()
        logger.debug("Using Resource Principal signer")
    except Exception as e:
        logger.debug("Resource Principal unavailable: %s", e)

    if signer is None:
        try:
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            logger.debug("Using Instance Principal signer")
        except Exception as e:
            logger.debug("Instance Principal unavailable: %s", e)

    if signer is None:
        return None

    try:
        client = ObjectStorageClient(config={}, signer=signer)
        namespace = client.get_namespace().data
        region = (getattr(signer, "region", None)
                  or getattr(signer, "_region", None)
                  or os.environ.get("OCI_RESOURCE_PRINCIPAL_REGION", ""))
        compartment_id = os.environ.get("OCI_COMPARTMENT_ID", "")
        return client, namespace, region, compartment_id
    except Exception as e:
        logger.debug("OCI client setup failed after signer obtained: %s", e)
        return None


def _try_config_file(profile: str = "DEFAULT"):
    """Fallback: API key auth from ~/.oci/config."""
    import oci
    from oci.object_storage import ObjectStorageClient
    config = oci.config.from_file(profile_name=profile)
    client = ObjectStorageClient(config)
    namespace = client.get_namespace().data
    region = config["region"]
    # No native compartment field in config — caller must supply or read from env.
    compartment_id = os.environ.get("OCI_COMPARTMENT_ID", config.get("tenancy", ""))
    return client, namespace, region, compartment_id


def detect_environment(profile: str = "DEFAULT"):
    """Auto-detect OCI environment.

    Tries Resource Principal first (AIDP/OCI compute), falls back to
    ~/.oci/config (local dev). Returns the OCI client and a resolved
    AIDPEnvironment record.
    """
    rp = _try_resource_principal()
    if rp is not None:
        client, ns, region, cid = rp
        return client, AIDPEnvironment(ns, region, cid, is_resource_principal=True)

    client, ns, region, cid = _try_config_file(profile)
    return client, AIDPEnvironment(ns, region, cid, is_resource_principal=False)


def ensure_bucket(client, env: AIDPEnvironment, bucket_name: str) -> str:
    """Ensure an OCI Object Storage bucket exists.

    Returns one of:
      "exists"       — bucket already there
      "created"      — bucket newly created
      "unauthorized" — caller's IAM doesn't permit creation; user must create
                       the bucket manually in OCI Console

    Implements D9 (graceful NotAuthorized fallback).
    """
    import oci
    from oci.object_storage.models import CreateBucketDetails
    try:
        client.get_bucket(env.namespace, bucket_name)
        return "exists"
    except oci.exceptions.ServiceError as e:
        if e.status != 404:
            raise

    if not env.compartment_id:
        logger.warning(
            "Bucket %s missing and no compartment_id resolved — cannot auto-create. "
            "Create the bucket in OCI Console and re-run.", bucket_name,
        )
        return "unauthorized"

    try:
        client.create_bucket(
            env.namespace,
            CreateBucketDetails(name=bucket_name, compartment_id=env.compartment_id),
        )
        logger.info("Created bucket %s in compartment ...%s", bucket_name, env.compartment_id[-12:])
        return "created"
    except oci.exceptions.ServiceError as e:
        if e.code in ("NotAuthorizedOrNotFound", "NotAuthorized", "Forbidden"):
            logger.warning(
                "Cannot create bucket %s — IAM denies (code=%s). "
                "Create the bucket manually in OCI Console under your compartment, "
                "then re-run.", bucket_name, e.code,
            )
            return "unauthorized"
        raise


def bucket_uri(env: AIDPEnvironment, bucket_name: str, path: str = "") -> str:
    """Compose the canonical OCI Hadoop FS URI for a bucket path.

    Example: oci://tpcds-benchmark-sf1@<your-namespace>/customer.parquet
    """
    path = path.lstrip("/")
    return f"oci://{bucket_name}@{env.namespace}/{path}"
