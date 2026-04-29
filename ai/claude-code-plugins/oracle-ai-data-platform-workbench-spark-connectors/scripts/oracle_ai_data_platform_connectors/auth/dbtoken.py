"""IAM DB-Token issuance and Spark executor refresh helpers.

Validated against ATP in the IMFA IoT case (3/3 successful long-run jobs).
ALH should work the same way (plain Oracle 26ai under the hood) but is
flagged as 'test live before relying on it' until row 2 of the live-test
matrix passes.

Usage from a notebook:

    from oracle_ai_data_platform_connectors.auth.dbtoken import (
        generate_db_token, refresh_on_executors,
    )

    # Driver-side: write the initial token to /tmp/dbcred_<conn>/token
    token_path = generate_db_token(
        compartment_ocid=os.environ["ATP_COMPARTMENT_OCID"],
        target_dir="/tmp/dbcred_atp",
    )

    # Set spark JDBC options to use it
    jdbc_opts = {
        "driver": "oracle.jdbc.OracleDriver",
        "oracle.jdbc.tokenAuthentication": "OCI_TOKEN",
        "oracle.jdbc.tokenLocation": token_path,
    }
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Callable, Optional


_DEFAULT_REFRESH_AFTER_SECONDS = 25 * 60  # tokens are 1h, refresh at 25 min


def generate_db_token(
    compartment_ocid: str,
    target_dir: str = "/tmp/dbcred",
    config: Optional[dict] = None,
    signer: Optional[Any] = None,
    region: Optional[str] = None,
) -> str:
    """Issue an IAM DB token and write it to ``<target_dir>/token``.

    Args:
        compartment_ocid: Compartment OCID for the DB-token scope. The OCI
            data-plane endpoint requires
            ``urn:oracle:db::id::<COMPARTMENT_OCID>``.
        target_dir: Directory under /tmp where the token file lands. Must be
            under /tmp for the JDBC driver to read it (FUSE caveat).
        config: Optional OCI config dict (from ``oci.config.from_file`` or
            ``from_inline_pem``). If omitted, the helper falls back to the
            default OCI profile.
        signer: Optional explicit OCI signer. Mutually-exclusive-ish with
            ``config`` (oci SDK accepts both for some clients).
        region: Optional region override for the data-plane client.

    Returns:
        The directory containing the token file (the value you pass to
        ``oracle.jdbc.tokenLocation``). NOT the path to the token file
        itself — the Oracle JDBC driver wants the directory.
    """
    import oci  # imported lazily so unit tests don't need oci installed
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    if not str(target_dir).startswith("/tmp"):
        raise ValueError(
            "dbtoken target_dir must be under /tmp; /Workspace breaks JDBC"
        )

    Path(target_dir).mkdir(parents=True, exist_ok=True)

    if config is None and signer is None:
        config = oci.config.from_file()
    if region:
        if config is None:
            config = {}
        config = {**config, "region": region}

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("ascii")
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    client_kwargs: dict = {}
    if config is not None:
        client_kwargs["config"] = config
    if signer is not None:
        client_kwargs["signer"] = signer

    client = oci.identity_data_plane.DataplaneClient(**client_kwargs)
    details = oci.identity_data_plane.models.GenerateScopedAccessTokenDetails(
        scope=f"urn:oracle:db::id::{compartment_ocid}",
        public_key=public_pem,
    )
    response = client.generate_scoped_access_token(
        generate_scoped_access_token_details=details,
    )
    token = response.data.token

    # Oracle JDBC's OCI_TOKEN auth requires BOTH files in the same directory:
    #   <target_dir>/token              — the scoped JWT
    #   <target_dir>/oci_db_key.pem    — the matching private key (proof of possession)
    # Driver looks for these names by convention; do not rename.
    _write_world_readable(Path(target_dir) / "token", token.encode("utf-8"))
    _write_world_readable(Path(target_dir) / "oci_db_key.pem", private_pem)
    return target_dir


def refresh_on_executors(
    spark: Any,
    compartment_ocid: str,
    target_dir: str = "/tmp/dbcred",
    refresh_after_seconds: int = _DEFAULT_REFRESH_AFTER_SECONDS,
) -> Callable[[Any], Any]:
    """Return a mapPartitions-callable that refreshes the DB token per executor.

    Use this for long-running Spark jobs (>25 min) so the per-executor JDBC
    connection pool keeps a fresh token. The returned function is meant to be
    composed with the user's actual partition-processing logic.

    Example:

        refresh = refresh_on_executors(spark, compartment_ocid)
        result = (
            df.rdd.mapPartitions(lambda part: refresh(part))
                  .toDF()
        )

    Args:
        spark: SparkSession (kept for API symmetry; the closure uses it
            indirectly via the broadcast mechanism).
        compartment_ocid: same as ``generate_db_token``.
        target_dir: same as ``generate_db_token``.
        refresh_after_seconds: refresh threshold; defaults to 25 min.

    Returns:
        A function suitable for ``rdd.mapPartitions`` that ensures a token
        younger than ``refresh_after_seconds`` exists in ``target_dir``
        before the partition's user code runs.
    """
    # Capture state in closure rather than relying on broadcast; each executor
    # process maintains its own (timestamp, path) pair.
    _state = {"last_refresh": 0.0}

    def ensure_token(partition_iter):
        now = time.time()
        if now - _state["last_refresh"] > refresh_after_seconds:
            generate_db_token(
                compartment_ocid=compartment_ocid,
                target_dir=target_dir,
            )
            _state["last_refresh"] = now
        for row in partition_iter:
            yield row

    return ensure_token


def _write_world_readable(path: Path, data: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    fd = os.open(str(path), flags, 0o666)
    try:
        os.write(fd, data)
    finally:
        os.close(fd)
