# AIDP via JDBC — Instance Principal auth (via the Resource‑Principal RPST bridge)

This sample connects to an **Oracle AI Data Platform (AIDP)** Spark compute
cluster over the **Simba Spark JDBC** driver and authenticates with the host
VM's **OCI Instance Principal** — no IAM user, no API signing key, no password.

> **Which principal is actually used?** The *authentication identity* is the
> **Instance Principal**. There is no OCI Resource Principal involved as an
> identity (a plain compute VM does not have one). The **Resource‑Principal
> (RPST) mechanism is reused only as the driver's transport** for the Instance
> Principal's token. See [Instance Principal vs Resource Principal](#instance-principal-vs-resource-principal-what-is-actually-used).

It is meant to run on an OCI compute instance whose instance principal is in a
**dynamic group that is mapped to an AIDP role**. The script runs a SQL query
(`SHOW DATABASES` by default) against a live cluster to prove connectivity.

The sample artifact is:

- [connect_aidp_instance_principal.py](./connect_aidp_instance_principal.py)

## Why this sample exists

A common pattern is to query AIDP from an automation host (CI runner, ETL VM,
bastion) **without** distributing user credentials or API keys. OCI Instance
Principal is the idiomatic answer: the VM proves its identity to OCI from the
Instance Metadata Service, and AIDP authorizes it through a dynamic-group →
role mapping.

The catch: the Simba Spark JDBC driver exposes three OCI auth modes —
**API signing key**, **browser token**, and **Resource Principal (RPST)** — but
has **no dedicated "instance principal" mode**. This sample bridges the gap.

## How the authentication works

```
  OCI compute VM (instance principal in a dynamic group mapped to an AIDP role)
        │
        │ 1. oci SDK: InstancePrincipalsSecurityTokenSigner()
        │    └─ federation security token  +  ephemeral private key   (from IMDS 169.254.169.254)
        │
        │ 2. expose them as the Resource-Principal v2.2 contract:
        │       OCI_RESOURCE_PRINCIPAL_VERSION=2.2
        │       OCI_RESOURCE_PRINCIPAL_RPST=<token file>
        │       OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM=<key file>
        │       OCI_RESOURCE_PRINCIPAL_REGION=<region>
        │
        │ 3. JDBC connect with ...;OCITokenAuthType=resource_principal
        ▼
  AIDP JDBC gateway  ──►  Spark compute cluster   (query runs as the instance principal)
```

The driver reads the `OCI_RESOURCE_PRINCIPAL_*` variables at connect time, so
they **must be set before the JVM starts**. The token and key are written to a
short-lived temp directory and deleted when the script exits.

> Note: the `security_token_file` (session-token) profile path is **not** a
> working alternative here — the driver's OCI config-file reader requires a
> `fingerprint` (it only supports the API-signing-key shape), so it rejects a
> session-token profile. The Resource-Principal env-var path above is the one
> that works.

## Instance Principal vs Resource Principal — what is actually used

This sample legitimately involves **both terms**, in two distinct roles, so it
is worth being precise:

| Concern | What is used | Detail |
|---|---|---|
| **Authentication identity** | **Instance Principal** | `oci.auth.signers.InstancePrincipalsSecurityTokenSigner()`. AIDP authorizes the VM through its dynamic‑group → AIDP‑role mapping. The federation token presented is the *instance's*; the resolved principal type is `instance`. |
| **Driver transport contract** | **Resource Principal (RPST) code path** | The Simba driver has no native instance‑principal mode and accepts an externally‑supplied token *only* through its RPST path. We therefore populate `OCI_RESOURCE_PRINCIPAL_*` and set `OCITokenAuthType=resource_principal` — but the token + key written there are the **Instance Principal's**, not a real Resource Principal's. |

In other words: **auth = Instance Principal; carrier = the Resource‑Principal
RPST mechanism.** This is *not* the same as OCI Resource Principal
authentication — `oci.auth.signers.get_resource_principals_signer()` would fail
on a plain compute VM because no RPST tokens are issued to it. We mint the
Instance Principal's token ourselves and hand it to the driver through the RPST
env‑var contract.

## Prerequisites

### 1. IAM / AIDP authorization (one-time, by an admin)

- A **dynamic group** that matches your VM, e.g. by compartment:
  ```
  ANY {instance.compartment.id = '<your-compartment-ocid>'}
  ```
  (or match a specific `instance.id`).
- That dynamic group added as a **member of an AIDP role** (AIDP has its own
  RBAC, separate from OCI IAM policies). When adding a dynamic group to an AIDP
  role, the member `type` is `GROUP` (not `DYNAMIC_GROUP`).

### 2. Host VM runtime

| Requirement | Notes |
|---|---|
| Runs on an OCI compute instance | IMDS (`169.254.169.254`) must be reachable. This will **not** work off-OCI. |
| Network access to the AIDP JDBC gateway | TCP `443` to `gateway.datalake.<region>.oci.oraclecloud.com`. |
| **Java 8+** (tested on OpenJDK 17) | `JAVA_HOME` set, or `libjvm` is auto-discovered under `/usr/lib/jvm`. |
| **Python 3.8+** | The script uses `jaydebeapi`, `JPype1` (needs 3.8+), `oci`, `cryptography`. |
| **Simba Spark JDBC driver** | `SparkJDBC<version>.jar` (see below). |

### 3. The Simba Spark JDBC driver

Download the **Simba Apache Spark JDBC** driver (the OCI-flavored build that
ships the `com.simba.spark.jdbc.Driver` class and supports `SparkServerType=IDL`)
and place the jar on the host. Set its path via `--jar` or `SIMBA_JDBC_JAR`.
The driver is also obtainable from the AIDP / Oracle Intelligent Data Lake
connection documentation.

## Setup

```bash
# 1. (recommended) a virtualenv
python3 -m venv ~/aidp_jdbc_venv
source ~/aidp_jdbc_venv/bin/activate

# 2. Python packages
pip install -r requirements.txt

# 3. point at the driver jar
export SIMBA_JDBC_JAR=/path/to/SparkJDBC2.6.18.2065.jar
```

## Run

```bash
python connect_aidp_instance_principal.py \
  --region us-ashburn-1 \
  --cluster-key <YOUR_CLUSTER_KEY> \
  --query "SHOW DATABASES"
```

The **cluster key** is the AIDP compute cluster's id; it becomes the JDBC
`httpPath=cliservice/<cluster-key>`. Find it in the AIDP console (cluster
details) or via the AIDP API/SDK.

### Configuration

All options are settable by flag or environment variable:

| Flag | Env var | Default |
|---|---|---|
| `--region` | `AIDP_REGION` | `us-ashburn-1` |
| `--gateway` | `AIDP_JDBC_GATEWAY` | derived: `gateway.datalake.<region>.oci.oraclecloud.com` |
| `--cluster-key` | `AIDP_CLUSTER_KEY` | *(required)* |
| `--jar` | `SIMBA_JDBC_JAR` | `./SparkJDBC2.6.18.2065.jar` |
| `--jvm` | `JVM_PATH` | auto-discovered `libjvm` |
| `--query` | `AIDP_QUERY` | `SHOW DATABASES` |

### Expected output

```
Connecting to gateway.datalake.us-ashburn-1.oci.oraclecloud.com (cluster <cluster-key>) as instance principal ...

-- SHOW DATABASES --  (N row(s))
('default',)
('...',)
```

A `SELECT` works the same way:

```bash
python connect_aidp_instance_principal.py \
  --cluster-key <YOUR_CLUSTER_KEY> \
  --query "SELECT * FROM <db>.<table> LIMIT 5"
```

## Security notes

- The federation token and ephemeral private key are written to a `0600`
  temp directory and **removed on exit** (`finally`). Nothing is persisted.
- Instance-principal tokens are **short-lived**. This sample mints a fresh one
  per run, so it is ideal for short queries/jobs. A long-running process would
  need to refresh the token and reconnect.
- No secrets are stored in this repo or in the script.

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `(500654) Error reading the following properties in the OCI Config file: fingerprint` | You tried the OCI **config-file** auth path. Use the Resource-Principal env-var path (this sample's default); the driver's config-file path is API-key only. |
| `401`/`403` / authorization errors at connect | The instance principal is not (yet) authorized in AIDP. Confirm the VM is in the dynamic group and that the dynamic group is a member of an AIDP role. |
| Instance Metadata Service not reachable / hang building the signer | You are not running on an OCI instance (IMDS `169.254.169.254` is unroutable off-OCI). |
| `Could not start JVM` / libjvm not found | Install a JDK and set `JAVA_HOME`, or pass `--jvm /path/to/libjvm.so`. |
| Connection times out | The host cannot reach `gateway.datalake.<region>.oci.oraclecloud.com:443`. Check egress / service gateway / NSGs. |

## References

- OCI SDK Authentication Methods —
  https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdk_authentication_methods.htm
- Calling instance metadata service (IMDS) —
  https://docs.oracle.com/en-us/iaas/Content/Compute/Tasks/gettingmetadata.htm
- JayDeBeApi — https://pypi.org/project/JayDeBeApi/ · JPype — https://jpype.readthedocs.io/
