"""
Bucket Shard Router
====================
Spreads write load across N OCI Object Storage buckets to stay under
per-bucket request-rate limits during large-scale parallel migration.

Why
---
OCI Object Storage rate limits are enforced per-bucket. A single bucket
that absorbs 50+ parallel Databricks->AIDP migration jobs sees small-file
PutObject storms that trip the OCI Java SDK CircuitBreaker. Spreading
target buckets via a deterministic hash gives each shard its own request
budget without any application-level coordination.

Usage in a migrated notebook
----------------------------
    from aidp_compat.bucket_shard import BucketRouter

    router = BucketRouter(
        prefix="customer-mig",
        num_shards=16,
        namespace="<WORKSPACE_NAMESPACE>",
    )

    # Deterministic mapping for a logical key (job_id, table name, partition):
    bucket = router.bucket_for("ExampleJob")
    # -> "customer-mig-07"

    target_uri = router.route_uri(
        "oci://customer-mig@<WORKSPACE_NAMESPACE>/feature_store/app_vN/...",
        shard_key="ExampleJob",
    )
    # -> "oci://customer-mig-07@<WORKSPACE_NAMESPACE>/feature_store/app_vN/..."

    df.write.parquet(target_uri)

The router is read-only and stateless; it does NOT create buckets. Bucket
provisioning is an operator step (Terraform / oci-cli loop) -- see
MIGRATION_THROTTLING.md for the bootstrap script.
"""
from __future__ import annotations

import hashlib
import re
from typing import Optional
from urllib.parse import urlsplit, urlunsplit


# Matches "oci://<bucket>@<namespace>/<path>"
_OCI_URI_RE = re.compile(
    r"^oci://(?P<bucket>[^@/]+)@(?P<namespace>[^/]+)(?P<path>/.*)?$"
)


class BucketRouter:
    """Deterministic bucket sharding by hash of a key.

    Args:
        prefix: Logical bucket name prefix. Shard buckets are named
            ``{prefix}-{NN}`` zero-padded to ``shard_width`` digits
            (default 2). e.g. ``customer-mig-00`` ... ``customer-mig-15``.
        num_shards: Number of shard buckets in the pool. 8-16 is a good
            starting range for the a large-scale migration -- size to
            divide your target sustained PUT/sec by the per-bucket budget
            you negotiated with the OCI account team.
        namespace: OCI Object Storage namespace (tenancy-level). All
            shards must share a namespace.
        shard_width: Zero-padding width for the shard index. Default 2.
        algorithm: Hash algorithm; ``md5`` (default) is fast and stable.

    Raises:
        ValueError: invalid arguments.
    """

    def __init__(
        self,
        prefix: str,
        num_shards: int,
        namespace: str,
        shard_width: int = 2,
        algorithm: str = "md5",
    ) -> None:
        if not prefix or "@" in prefix or "/" in prefix:
            raise ValueError(f"invalid prefix: {prefix!r}")
        if num_shards < 1:
            raise ValueError(f"num_shards must be >= 1, got {num_shards}")
        if num_shards > 99 and shard_width < 3:
            raise ValueError(
                f"num_shards={num_shards} requires shard_width >= 3"
            )
        if not namespace:
            raise ValueError("namespace is required")
        if algorithm not in hashlib.algorithms_guaranteed:
            raise ValueError(f"unsupported hash algorithm: {algorithm}")

        self.prefix = prefix
        self.num_shards = num_shards
        self.namespace = namespace
        self.shard_width = shard_width
        self.algorithm = algorithm

    def shard_index(self, key: str) -> int:
        """Stable shard index in [0, num_shards) for a key."""
        if not isinstance(key, str):
            key = str(key)
        digest = hashlib.new(self.algorithm, key.encode("utf-8")).digest()
        # Use the first 8 bytes as an unsigned int -- plenty for mod N
        n = int.from_bytes(digest[:8], byteorder="big", signed=False)
        return n % self.num_shards

    def bucket_for(self, key: str) -> str:
        """Return the shard bucket name for a key."""
        idx = self.shard_index(key)
        return f"{self.prefix}-{idx:0{self.shard_width}d}"

    def all_buckets(self) -> list:
        """Return the full list of shard bucket names (for provisioning)."""
        return [
            f"{self.prefix}-{i:0{self.shard_width}d}"
            for i in range(self.num_shards)
        ]

    def route_uri(self, uri: str, shard_key: Optional[str] = None) -> str:
        """Rewrite an oci:// URI to point at the bucket shard for ``shard_key``.

        If the input URI already names a bucket starting with ``self.prefix``,
        only the bucket is replaced; the namespace and path are preserved.

        Args:
            uri: An ``oci://<bucket>@<namespace>/<path>`` URI.
            shard_key: Key to hash. Defaults to the original bucket+path so
                writes for the same logical location land on the same shard.

        Returns:
            Rewritten URI.

        Raises:
            ValueError: ``uri`` is not a recognised oci:// URI.
        """
        m = _OCI_URI_RE.match(uri)
        if not m:
            raise ValueError(f"not an oci:// URI: {uri!r}")
        original_bucket = m.group("bucket")
        path = m.group("path") or ""

        # Default shard_key: bucket+path so the same logical location is stable
        key = shard_key if shard_key is not None else f"{original_bucket}{path}"
        new_bucket = self.bucket_for(key)
        return f"oci://{new_bucket}@{self.namespace}{path}"


def parse_oci_uri(uri: str) -> dict:
    """Parse an oci:// URI into ``{bucket, namespace, path}``.

    Convenience helper for migration tooling that needs to inspect the
    components without a regex match in three different places.
    """
    m = _OCI_URI_RE.match(uri)
    if not m:
        raise ValueError(f"not an oci:// URI: {uri!r}")
    return {
        "bucket": m.group("bucket"),
        "namespace": m.group("namespace"),
        "path": m.group("path") or "/",
    }


__all__ = ["BucketRouter", "parse_oci_uri"]
