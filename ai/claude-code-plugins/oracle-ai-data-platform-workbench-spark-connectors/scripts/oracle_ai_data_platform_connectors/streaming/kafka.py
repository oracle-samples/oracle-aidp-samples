"""OCI Streaming via Spark structured streaming (Kafka-compat).

Auth = SASL/PLAIN with an OCI auth token. This mirrors the official Oracle
sample at oracle-samples/oracle-aidp-samples
(`data-engineering/ingestion/Streaming/StreamingFromOCIStreamingService.ipynb`).

Critical AIDP gotcha: streaming checkpoints must live under `/Volumes/...`.
`/Workspace/...` (FUSE) and `oci://...` both fail silently.
"""

from __future__ import annotations

from typing import Optional


def bootstrap_for_region(region: str, cell: Optional[int] = None) -> str:
    """Return the OCI Streaming Kafka bootstrap broker for a region.

    Args:
        region: e.g. ``us-ashburn-1``.
        cell: Optional streaming-cell number. If provided, returns the cell-
            prefixed form (``cell-N.streaming.<region>.oci.oraclecloud.com:9092``)
            that the official Oracle AIDP sample uses. If None, returns the
            generic regional form (``streaming.<region>.oci.oraclecloud.com:9092``).
            OCI routes both correctly; pick whichever matches your pool's
            messages-endpoint shown in the OCI Console.

    Returns:
        ``streaming.<region>.oci.oraclecloud.com:9092`` (no cell), or
        ``cell-<N>.streaming.<region>.oci.oraclecloud.com:9092`` (with cell).
    """
    host_prefix = f"cell-{cell}.streaming" if cell is not None else "streaming"
    return f"{host_prefix}.{region}.oci.oraclecloud.com:9092"


def build_kafka_options_sasl_plain(
    bootstrap_servers: str,
    tenancy_name: str,
    username: str,
    stream_pool_ocid: str,
    auth_token: str,
    topic: str,
    *,
    starting_offsets: str = "latest",
    max_partition_fetch_bytes: Optional[int] = 1024 * 1024,
    max_offsets_per_trigger: Optional[int] = None,
) -> dict:
    """Spark Kafka options for SASL/PLAIN with an OCI auth token.

    Mirrors the official Oracle AIDP sample's readStream block. Username
    format follows OCI Streaming Kafka-compat spec for IAM-Domains tenancies:
    ``<tenancy_name>/<username>/<stream_pool_ocid>``. For an IAM-Domains user
    pass ``oracleidentitycloudservice/<email>`` as the ``username`` argument.

    Args:
        bootstrap_servers: Output of ``bootstrap_for_region``.
        tenancy_name: OCI tenancy display name (NOT OCID).
        username: OCI user (typically email, optionally prefixed with
            ``oracleidentitycloudservice/`` for IAM-Domains).
        stream_pool_ocid: ``ocid1.streampool.oc1...``.
        auth_token: 1-hour OCI auth token.
        topic: Kafka topic name (must exist in the stream pool).
        starting_offsets: ``latest`` | ``earliest`` | offsets JSON.
        max_partition_fetch_bytes: Optional Kafka client tuning. Default
            1 MiB matches the official sample.
        max_offsets_per_trigger: Optional cap on rows pulled per micro-batch.
            ``None`` lets Spark decide; the official sample uses ``5`` for
            slow demos.

    Returns:
        Dict ready for ``spark.readStream.format("kafka").options(**dict).load()``.
    """
    sasl_username = f"{tenancy_name}/{username}/{stream_pool_ocid}"
    jaas_config = (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{sasl_username}" '
        f'password="{auth_token}";'
    )
    opts: dict = {
        "kafka.bootstrap.servers": bootstrap_servers,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas_config,
        "subscribe": topic,
        "startingOffsets": starting_offsets,
    }
    if max_partition_fetch_bytes is not None:
        opts["kafka.max.partition.fetch.bytes"] = str(max_partition_fetch_bytes)
    if max_offsets_per_trigger is not None:
        opts["maxOffsetsPerTrigger"] = str(max_offsets_per_trigger)
    return opts


def validate_checkpoint_path(path: str) -> str:
    """Validate that a Spark streaming checkpoint path is AIDP-compatible.

    AIDP-compatible = ``/Volumes/<catalog>/<schema>/<volume>/...``. The
    ``/Workspace/`` mount (FUSE) and ``oci://`` URIs both fail silently for
    streaming checkpoints.

    Returns:
        ``path`` unchanged if valid.

    Raises:
        ValueError: If path is on /Workspace or starts with oci://.
    """
    p = path.strip()
    if p.startswith("/Workspace") or p.startswith("/workspace"):
        raise ValueError(
            "Streaming checkpoints cannot live on /Workspace (FUSE). "
            "Use /Volumes/<catalog>/<schema>/<volume>/_checkpoints/... "
            "instead."
        )
    if p.startswith("oci://"):
        raise ValueError(
            "Streaming checkpoints cannot use oci:// directly. "
            "Use /Volumes/... instead."
        )
    if not p.startswith("/Volumes/"):
        raise ValueError(
            f"Streaming checkpoint should live under /Volumes/...; got {p!r}"
        )
    return p
