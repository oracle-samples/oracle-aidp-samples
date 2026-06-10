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
}


def gateway_for(region: str, override: str = "") -> str:
    """The AIDP JDBC gateway host for a region (overridable)."""
    return override or f"gateway.datalake.{region}.oci.oraclecloud.com"


def mint_instance_principal_rpst(tmpdir: str) -> tuple[str, str]:
    """Mint the instance principal's federation token + ephemeral key.

    Returns paths to (token_file, key_file). The key is written PKCS#8 PEM and
    chmod 600. Both files are short-lived; the caller deletes them.
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
    with open(token_file, "w") as fh:
        fh.write(token)
    with open(key_file, "w") as fh:
        fh.write(key_pem)
    os.chmod(key_file, 0o600)
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

    gateway = gateway_for(cfg.region, cfg.gateway)
    jdbc_url = (
        f"jdbc:spark://{gateway}/default;SparkServerType=IDL;"
        f"httpPath=cliservice/{cfg.cluster};OCITokenAuthType=resource_principal"
    )

    tmpdir = tempfile.mkdtemp(prefix="aidp_ip_")
    try:
        token_file, key_file = mint_instance_principal_rpst(tmpdir)

        # The driver reads these via System.getenv at connect time, so they
        # MUST be set before the JVM starts.
        os.environ["OCI_RESOURCE_PRINCIPAL_VERSION"] = "2.2"
        os.environ["OCI_RESOURCE_PRINCIPAL_RPST"] = token_file
        os.environ["OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM"] = key_file
        os.environ["OCI_RESOURCE_PRINCIPAL_REGION"] = cfg.region

        import jpype
        import jaydebeapi

        if not jpype.isJVMStarted():
            jpype.startJVM(discover_libjvm(cfg.jvm),
                           "-Djava.class.path=" + cfg.jar)

        print(f"Connecting to {gateway} (cluster {cfg.cluster}) "
              f"as instance principal ...")
        conn = jaydebeapi.connect(DRIVER_CLASS, jdbc_url, {"ssl": "1"})
        try:
            cur = conn.cursor()
            cur.execute(cfg.query)
            rows = cur.fetchall()
            print(f"\n-- {cfg.query} --  ({len(rows)} row(s))")
            for row in rows:
                print(row)
            cur.close()
        finally:
            conn.close()
        return 0
    finally:
        # Always remove the short-lived credential material.
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
    return p.parse_args()


if __name__ == "__main__":
    sys.exit(run(parse_args()))
