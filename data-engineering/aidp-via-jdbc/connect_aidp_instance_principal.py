#!/usr/bin/env python3
"""
Connect to Oracle AI Data Platform (AIDP) Spark compute over the Simba Spark
JDBC driver, authenticating with the host VM's OCI **Instance Principal**
(no IAM user, no API signing key, no password).

Why this looks the way it does
------------------------------
The Simba Spark JDBC driver exposes three OCI auth modes: API signing key,
browser token, and Resource Principal (RPST). It has *no* dedicated
"instance principal" mode. We therefore bridge the instance principal into the
driver's Resource-Principal path:

  1. Mint the instance principal's federation **security token** + **ephemeral
     private key** from the Instance Metadata Service (IMDS) using the OCI SDK.
  2. Expose them through the ``OCI_RESOURCE_PRINCIPAL_*`` environment variables
     (the v2.2 RPST contract the driver already understands).
  3. Open the JDBC connection with ``OCITokenAuthType=resource_principal``.

The host's instance principal must belong to a dynamic group that is mapped to
an AIDP role (AIDP has its own RBAC, separate from OCI IAM policies).

Prerequisites
-------------
* Java 8+ (tested on OpenJDK 17). ``JAVA_HOME`` set, or libjvm auto-discovered.
* Simba Spark JDBC driver jar (``SparkJDBC<ver>.jar``).
* Python 3.8+ with: ``pip install jaydebeapi JPype1 oci``.
* Runs on an OCI compute instance whose instance principal is mapped to an
  AIDP role, with network reachability to the AIDP JDBC gateway (TCP 443).

Configuration (all overridable by environment variable or CLI flag)
-------------------------------------------------------------------
  AIDP_REGION         OCI region                (default: us-ashburn-1)
  AIDP_JDBC_GATEWAY   AIDP JDBC gateway host
  AIDP_CLUSTER_KEY    Target compute cluster key (the httpPath cliservice id)
  SIMBA_JDBC_JAR      Absolute path to the Simba driver jar
  JVM_PATH            Absolute path to libjvm.{so,dylib} (optional)
  AIDP_QUERY          SQL to run                 (default: SHOW DATABASES)
  AIDP_MAX_ROWS       Max rows to fetch/print    (default: 100)
"""
from __future__ import annotations

import argparse
import glob
import os
import shutil
import sys
import tempfile

import oci
from cryptography.hazmat.primitives import serialization

DRIVER_CLASS = "com.simba.spark.jdbc.Driver"

DEFAULTS = {
    "region":   os.environ.get("AIDP_REGION", "us-ashburn-1"),
    # Leave blank to derive from region: gateway.datalake.<region>.oci.oraclecloud.com
    "gateway":  os.environ.get("AIDP_JDBC_GATEWAY", ""),
    "cluster":  os.environ.get("AIDP_CLUSTER_KEY", ""),
    "jar":      os.environ.get("SIMBA_JDBC_JAR", "./SparkJDBC2.6.18.2065.jar"),
    "jvm":      os.environ.get("JVM_PATH", ""),
    "query":    os.environ.get("AIDP_QUERY", "SHOW DATABASES"),
    "max_rows": int(os.environ.get("AIDP_MAX_ROWS", "100")),
}


def gateway_for(region: str, override: str = "") -> str:
    """The AIDP JDBC gateway host for a region (overridable)."""
    return override or f"gateway.datalake.{region}.oci.oraclecloud.com"


def _write_secret(path: str, text: str) -> None:
    """Write text to a freshly created 0600 file (created restricted, never
    world-readable even briefly)."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as fh:
        fh.write(text)


def mint_instance_principal_rpst(tmpdir: str) -> tuple[str, str]:
    """Mint the instance principal's federation token + ephemeral key.

    Returns paths to (token_file, key_file), both written 0600. The token is a
    bearer credential and the key is the matching session key, so both are
    treated as secrets; the caller deletes them.

    Note: ``federation_client`` / ``session_key_supplier`` are OCI SDK members
    used to surface the live token + key for the driver's RPST contract. Pin the
    ``oci`` version if you depend on this in production.
    """
    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    fed = signer.federation_client
    token = fed.get_security_token()
    private_key = fed.session_key_supplier.get_key_pair()["private"]
    key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()

    token_file = os.path.join(tmpdir, "rpst_token")
    key_file = os.path.join(tmpdir, "rpst_key.pem")
    _write_secret(token_file, token)
    _write_secret(key_file, key_pem)
    return token_file, key_file


def discover_libjvm(explicit: str = "") -> str:
    """Find libjvm.{so,dylib}; fall back to JPype's default search."""
    if explicit:
        return explicit
    patterns = (
        "/usr/lib/jvm/*/lib/server/libjvm.so",
        "/usr/lib/jvm/*/lib/*/server/libjvm.so",
        "/Library/Java/JavaVirtualMachines/*/Contents/Home/lib/server/libjvm.dylib",
    )
    for pattern in patterns:
        hits = sorted(glob.glob(pattern))
        if hits:
            return hits[0]
    import jpype
    return jpype.getDefaultJVMPath()


def run(cfg) -> int:
    if not cfg.cluster:
        sys.exit("error: set AIDP_CLUSTER_KEY (or --cluster-key) to the target "
                 "compute cluster key.")
    if not os.path.isfile(cfg.jar):
        sys.exit(f"error: Simba driver jar not found: {cfg.jar} "
                 "(set SIMBA_JDBC_JAR or --jar).")

    import jpype
    import jaydebeapi

    # The Simba jar (classpath) and the OCI_RESOURCE_PRINCIPAL_* env vars are
    # both fixed at JVM startup, so this script must start the JVM itself. If one
    # is already running we cannot guarantee either — fail clearly rather than
    # silently connecting without the driver/credentials in place.
    if jpype.isJVMStarted():
        raise RuntimeError(
            "A JVM is already running. Run this as a standalone process so the "
            "Simba jar is on the classpath and OCI_RESOURCE_PRINCIPAL_* are set "
            "before JVM initialization."
        )

    gateway = gateway_for(cfg.region, cfg.gateway)
    jdbc_url = (
        f"jdbc:spark://{gateway}/default;SparkServerType=IDL;"
        f"httpPath=cliservice/{cfg.cluster};OCITokenAuthType=resource_principal"
    )

    tmpdir = tempfile.mkdtemp(prefix="aidp_ip_")
    saved_env: dict[str, str | None] = {}
    try:
        token_file, key_file = mint_instance_principal_rpst(tmpdir)

        # Set the RPST env vars (the driver reads them via System.getenv at
        # connect time). Remember prior values so we can restore them later.
        rp_env = {
            "OCI_RESOURCE_PRINCIPAL_VERSION": "2.2",
            "OCI_RESOURCE_PRINCIPAL_RPST": token_file,
            "OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM": key_file,
            "OCI_RESOURCE_PRINCIPAL_REGION": cfg.region,
        }
        for key, value in rp_env.items():
            saved_env[key] = os.environ.get(key)
            os.environ[key] = value

        try:
            jpype.startJVM(discover_libjvm(cfg.jvm),
                           "-Djava.class.path=" + cfg.jar)
            print(f"Connecting to {gateway} (cluster {cfg.cluster}) "
                  f"as instance principal ...")
            conn = jaydebeapi.connect(DRIVER_CLASS, jdbc_url, {"ssl": "1"})
        except Exception:
            sys.stderr.write(
                "\nJVM startup or JDBC connect failed. See the README "
                "'Troubleshooting' section — common causes: JDK/libjvm not found, "
                "the instance principal is not authorized in AIDP (dynamic group "
                "-> role), or no network route to the gateway.\n"
            )
            raise

        try:
            cur = conn.cursor()
            cur.execute(cfg.query)
            # Bound the result so an arbitrary --query can't exhaust memory.
            rows = cur.fetchmany(cfg.max_rows + 1)
            truncated = len(rows) > cfg.max_rows
            rows = rows[:cfg.max_rows]
            suffix = ", truncated" if truncated else ""
            print(f"\n-- {cfg.query} --  ({len(rows)} row(s){suffix})")
            for row in rows:
                print(row)
            if truncated:
                print(f"... output truncated at --max-rows={cfg.max_rows}")
            cur.close()
        finally:
            conn.close()
        return 0
    finally:
        # Restore env vars and remove the short-lived credential material.
        for key, old in saved_env.items():
            if old is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old
        shutil.rmtree(tmpdir, ignore_errors=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--region", default=DEFAULTS["region"])
    p.add_argument("--gateway", default=DEFAULTS["gateway"])
    p.add_argument("--cluster-key", dest="cluster", default=DEFAULTS["cluster"],
                   help="Target compute cluster key (httpPath cliservice id).")
    p.add_argument("--jar", default=DEFAULTS["jar"],
                   help="Path to the Simba Spark JDBC driver jar.")
    p.add_argument("--jvm", default=DEFAULTS["jvm"],
                   help="Path to libjvm.so/.dylib (optional; auto-discovered).")
    p.add_argument("--query", default=DEFAULTS["query"], help="SQL to execute.")
    p.add_argument("--max-rows", dest="max_rows", type=int,
                   default=DEFAULTS["max_rows"],
                   help="Max rows to fetch/print (default 100).")
    return p.parse_args()


if __name__ == "__main__":
    sys.exit(run(parse_args()))
