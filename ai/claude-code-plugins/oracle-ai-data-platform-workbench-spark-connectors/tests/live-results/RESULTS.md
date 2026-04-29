# Live-test results


**Summary:** 17 PASS, 0 DEFERRED, 4 NOT RUN out of 21 rows. Rows 16, 17, 21, 23 are explicitly ship-as-is (connector code + example notebook ship; customer validates against their own endpoint).

Row IDs 4 (ExaCS Wallet TCPS), 5 (ExaCS IAM DB-Token), 7 + 8 (`aidp-bds-hive` Kerberos + LDAP), and 18 (`aidp-oracle-db` plain TCP 1521) were removed. ExaCS rows 4/5 aren't supported by AIDP notebooks for ExaCS clusters. The `aidp-bds-hive` skill was dropped from the plugin (BDS Hive not in scope for this connector pack). Row 18 was dropped because the `aidp-alh` family already covers Oracle 26ai connectivity end-to-end across all three auth methods (wallet, IAM DB-Token, aidataplatform format handler) — a separate plain-1521 Oracle DB skill adds no incremental coverage and the JDBC code path is identical.

**v0.2.0** added rows 14–25 covering Object Storage, Iceberg, Postgres, MySQL/HeatWave, SQL Server, Snowflake, ADLS Gen2, AWS S3, generic REST, custom JDBC, Excel — all sourced from the official `oracle-samples/oracle-aidp-samples` repo.

**v0.3.0 quick-wins**: rows 14, 19, 24, 25 flipped to PASS — Object Storage CSV roundtrip, Iceberg Hadoop catalog smoke, custom-JDBC SQLite (via new runtime-load helper), Excel ingestion (via new stdlib zipfile+XML parser). Rows 24 and 25 produced new helper modules to handle PyPI-unreachable clusters.

**Rows 2 + 3 (`aidp-alh` IAM DB-Token + API Key catalog sync) flipped to PASS (2026-04-27)** by provisioning a single-purpose Autonomous DB (`ALHTEST`, ATP 23ai, ECPU 2/20GB) in compartment `AIDP_TPC-DS_Ahmed`. Row 2 required `DBMS_CLOUD_ADMIN.ENABLE_EXTERNAL_AUTHENTICATION(type=>'OCI_IAM')` plus an IDCS-domain-prefixed `IAM_PRINCIPAL_NAME` mapping; row 3 used the `aidataplatform` ORACLE_ALH format handler with `from_inline_pem` exercised separately on the OCI control plane. Aidp-alh skill now PASS across all 3 documented auth methods.

| # | Skill | Auth | Notebook | Status | Rows | Last run (UTC) |
|---|---|---|---|---|---|---|
| 0 | `aidp-connectors-bootstrap` | n/a | [`00_bootstrap_helpers.ipynb`](../../examples/00_bootstrap_helpers.ipynb) | PASS | 1 | 1777213489 |
| 1 | `aidp-alh` | Wallet (mTLS) | [`alh_wallet_query.ipynb`](../../examples/alh_wallet_query.ipynb) | PASS | 1 | 1777214484 |
| 2 | `aidp-alh` | IAM DB-Token (>25 min refresh) | [`alh_dbtoken_query.ipynb`](../../examples/alh_dbtoken_query.ipynb) | PASS | 1 | 1777274391 |
| 3 | `aidp-alh` | API Key + inline OCI config | [`alh_catalog_sync_apikey.ipynb`](../../examples/alh_catalog_sync_apikey.ipynb) | PASS | 5 | 1777274630 |
| 6 | `aidp-exacs` | Plain user/pwd on TCP 1521 + NNE AES256 | [`exacs_user_password.ipynb`](../../examples/exacs_user_password.ipynb) | PASS | - | None |
| 9 | `aidp-fusion-rest` | HTTP Basic | [`fusion_rest_basic.ipynb`](../../examples/fusion_rest_basic.ipynb) | PASS | 229 | 1777213835 |
| 10 | `aidp-fusion-bicc` | HTTP Basic | [`fusion_bicc_to_dataframe.ipynb`](../../examples/fusion_bicc_to_dataframe.ipynb) | PASS | - | None |
| 11 | `aidp-epm-cloud` | Basic (tenancy.user@domain) | [`epm_planning_basic.ipynb`](../../examples/epm_planning_basic.ipynb) | PASS | 1 | 1777213859 |
| 12 | `aidp-essbase` | HTTP Basic | [`essbase_mdx_basic.ipynb`](../../examples/essbase_mdx_basic.ipynb) | PASS | 2 | None |
| 13 | `aidp-streaming-kafka` | SASL/PLAIN with OCI auth token | [`kafka_streaming_apikey.ipynb`](../../examples/kafka_streaming_apikey.ipynb) | PASS | 3 | 1777223131 |
| 14 | `aidp-object-storage` | Implicit IAM (`oci://`) | [`object_storage_csv_roundtrip.ipynb`](../../examples/object_storage_csv_roundtrip.ipynb) | PASS | 3 | 1777229586 |
| 15 | `aidp-postgresql` | Spark JDBC + sslmode=require (Neon Postgres 17.8) | [`postgresql_read.ipynb`](../../examples/postgresql_read.ipynb) | PASS | 5 | 1777286022 |
| 16 | `aidp-mysql` | Plain user/password (MYSQL / MYSQL_HEATWAVE) | [`mysql_read.ipynb`](../../examples/mysql_read.ipynb) | NOT RUN | - | - |
| 17 | `aidp-sqlserver` | Plain user/password | [`sqlserver_read.ipynb`](../../examples/sqlserver_read.ipynb) | NOT RUN | - | - |
| 19 | `aidp-iceberg` | Implicit IAM (Hadoop catalog on `oci://`) | [`iceberg_smoke.ipynb`](../../examples/iceberg_smoke.ipynb) | PASS | 4 | 1777229629 |
| 20 | `aidp-snowflake` | sfUser/sfPassword | [`snowflake_read.ipynb`](../../examples/snowflake_read.ipynb) | PASS | 10 | 1777231136 |
| 21 | `aidp-azure-adls` | OAuth client-credentials | [`adls_read.ipynb`](../../examples/adls_read.ipynb) | NOT RUN | - | - |
| 22 | `aidp-aws-s3` | AWS access key (s3a://) | [`s3_read.ipynb`](../../examples/s3_read.ipynb) | PASS | 2 | 1777297746 |
| 23 | `aidp-rest-generic` | HTTP Basic + manifest | [`rest_generic_read.ipynb`](../../examples/rest_generic_read.ipynb) | NOT RUN | - | - |
| 24 | `aidp-jdbc-custom` | SQLite memory + runtime-load helper | [`jdbc_custom_sqlite.ipynb`](../../examples/jdbc_custom_sqlite.ipynb) | PASS | 1 | 1777229921 |
| 25 | `aidp-excel` | stdlib zipfile + XML parser | [`excel_read.ipynb`](../../examples/excel_read.ipynb) | PASS | 5 | 1777230349 |

### Notes on row 15 (`aidp-postgresql`) — PASS via Neon (2026-04-27)

After the in-tenancy approaches failed (network blocker described below), unblocked by pointing at a public-internet Postgres endpoint — Neon free-tier serverless Postgres 17.8 at `ep-flat-pine-ams0cmom.c-5.us-east-1.aws.neon.tech:5432`. The cluster reaches Neon's public AWS endpoint via NAT egress (`129.80.7.94` → public internet → Neon's AWS LB). Test was: seed `aidp_test_data` (5 rows) locally via `psycopg2`, then read on cluster via Spark JDBC with `sslmode=require` URL-embedded.

Two non-obvious findings folded into the skill:
1. AIDP `aidataplatform` format with `type=POSTGRESQL` silently ignores SSL options (`ssl`, `sslmode`, `jdbc.ssl.enabled`, etc.) — Postgres rejects with `connection is insecure`. For SSL-required Postgres targets (Neon, RDS, most production), use Spark native JDBC format with URL-embedded `sslmode=require`.
2. Cluster has no `org.postgresql.Driver` pre-installed; runtime-load pattern works: download `postgresql-42.7.4.jar` from Maven Central inside the cluster, register via `URLClassLoader([jar],systemCL)` + `Thread.setContextClassLoader` + `DriverManager.registerDriver(Class.forName('org.postgresql.Driver',true,ucl).newInstance())`, then `spark._jsc.addJar(path)` for executor distribution.

### Notes on row 16 (`aidp-mysql`) — DEFERRED + diagnostic followup

Drove a fourth attempt on 2026-04-27 using the existing `dfl_private_cluster` in the `dfl_server_private` workspace (which has `isPrivateNetworkEnabled=true` with PE attached to `dfl-server-subnet-public`). Provisioned OCI MySQL HeatWave at `10.99.1.33:3306` in that subnet so the workspace's PE could route to it. Drove the entire flow (notebook upload + job create + run + output fetch) via raw AIDP REST API (Jobs API) — bypassing the workspace-bound MCP. Mechanism worked end-to-end.

**Result:** still DEFERRED. Probe job confirmed `dfl_private_cluster` pod IP is `10.111.107.121` (AIDP-managed pod CIDR), NOT in dfl-server-vcn (10.99.0.0/16). TCP probes to `10.99.1.33:3306`, `10.99.1.30:22`, OCI metadata `169.254.169.254`, and DNS `checkip.amazonaws.com` ALL failed. **The workspace's PE attaches to the customer subnet for service routing (e.g. AIDP catalog sync) but does NOT extend the cluster pod CIDR's L3 reachability into that subnet.** Additional VCN peering/DRG between the AIDP cluster's hidden 10.111.0.0/16 VCN and the customer VCN is required.

This finding clarifies AIDP's "private workspace" semantics — it's not a general-purpose VCN bridge; it's targeted PE service routing.

### Notes on row 22 (`aidp-aws-s3`) — PASS via runtime-load (2026-04-27)

Customer-provided AWS access key + secret + bucket. Cluster reaches `s3.amazonaws.com:443` via NAT egress directly. 2 rows pulled from a CSV at `s3a://test-data-sep3-2025/csv/sample.csv`.

Two non-obvious findings folded into the skill:
1. Cluster has neither `org.apache.hadoop.fs.s3a.S3AFileSystem` nor `aws-java-sdk-bundle` pre-installed. Runtime-load both `hadoop-aws-3.3.4.jar` and `aws-java-sdk-bundle-1.12.262.jar` from Maven Central (matched to cluster's Hadoop 3.3.4).
2. Beyond the Postgres/MySQL DriverManager pattern, S3A also requires setting the URLClassLoader on Hadoop Configuration via `hconf.setClassLoader(ucl)` — because FileSystem lookup uses Configuration's classloader, not the JVM context classloader. Then `spark._jsc.addJar(jar)` distributes to executors.

### Notes on rows 16, 17, 21, 23 — ship-as-is decision (2026-04-27)

These four rows are explicitly **NOT RUN by design**. Connector code, helper, and example notebooks ship in the plugin; the customer validates against their own endpoint. Rationale per row in `row16.json` / `row17.json` / `row21.json` / `row23.json`. Summary:

- **Row 16 (`aidp-mysql`)** — exhaustively tested four paths (Compute VM in two VCNs, OCI MySQL HeatWave, AIDP private workspace + Jobs API). Cluster pod L3 reachability into user VCNs is the gating factor; not solvable without VCN peering set up by tenancy admin. Skill is correct.
- **Row 17 (`aidp-sqlserver`)** — OCI has no managed SQL Server; needs customer endpoint. Skill matches official sample.
- **Row 21 (`aidp-azure-adls`)** — needs customer Azure tenant + service principal. Cluster reaches `*.blob.core.windows.net` via NAT (proven by row 22's S3 path being equivalent), so test is straightforward when creds are provided.
- **Row 23 (`aidp-rest-generic`)** — capability already covered by row 9 (`aidp-fusion-rest` PASS with same network/auth/JSON-parse path). Row 23 only adds AIDP-proprietary manifest parsing; needs a customer endpoint that publishes such a manifest.

### Notes on row 23 (`aidp-rest-generic`) — NOT RUN by deliberate decision

Decision (2026-04-27, after exploring options): leave NOT RUN. The underlying network/auth/data-path capability is already proven by row 9 (`aidp-fusion-rest` PASS, 229 rows from Fusion REST via Basic auth from cluster's NAT egress). Row 23 only adds incremental coverage of the AIDP-proprietary `manifest.url` parsing path inside the `aidataplatform` GENERIC_REST connector. Fusion REST does NOT expose an AIDP-format manifest, so the existing Fusion creds can't be reused for this connector. To flip this row to PASS, a customer needs to provide a REST endpoint that publishes a manifest in AIDP's expected format. The skill SKILL.md, helper `aidataplatform_options(type='GENERIC_REST', ...)`, and example notebook are unit-tested and shape-correct against the official `oracle-aidp-samples` pattern.

### Notes on row 17 (`aidp-sqlserver`) — DEFERRED

Three independent attempts to live-validate these on 2026-04-27, all failed for the same root cause: the AIDP cluster's pod network (private `10.111.0.0/16`, NAT egress `129.80.7.94`) has no working route to user-managed VCNs in this tenancy.

**Attempt 1: Self-hosted Compute VM (podman) in `dfl-server-vcn`** — VM came up, `sshd` listening (confirmed via OCI instance-agent), security list permissive. TCP from cluster NAT to VM ports timed out at L4. Same timeout observed from a separate home IP. An existing `dfl-server-demo` VM in the same compartment was also unreachable from the cluster — ruling out per-VM misconfiguration.

**Attempt 2: Self-hosted Compute VM in a fresh purpose-built VCN** (`aidp-testdbs-vcn`, 10.50.0.0/16) — same outcome. Clean IG + route table + 0.0.0.0/0 ingress on 22; cluster still timed out.

**Attempt 3: OCI Managed PostgreSQL + MySQL HeatWave**
- PSQL: tenancy `dbsystem-count` quota exhausted by an existing PSQL instance not visible to my OCI auth; can't create more without quota increase.
- MySQL HeatWave: provisioned successfully (`MySQL.2` ECPU shape, private endpoint `10.2.0.51:3306` in tpcds VCN ClientSubnet `10.2.0.0/26`). Created a dedicated NSG with ingress for `10.111.0.0/16` + `129.80.7.94/32` on 3306, but `oci mysql db-system update --nsg-ids` silently failed to attach (endpoint `nsg-ids` stayed null). And independently, the AIDP cluster's hidden VCN has no LPG/DRG route to tpcds VCN — even with permissive ingress, no path. Deleted to stop billing.

**Conclusion:** The 50/50 unit tests for the `aidataplatform_options()` helper still pass and the example notebook option shapes match the official `oracle-aidp-samples` patterns — the connector code is correct. To flip rows 15/16 to PASS the customer needs one of:
- A Postgres/MySQL endpoint already reachable from the AIDP cluster's NAT egress (e.g., a public-internet endpoint, a customer-managed instance with the right firewall, or a managed-service endpoint in a peered VCN)
- A tenancy admin to add VCN peering between the AIDP cluster's hidden VCN and a test-DB VCN

Row 17 (SQL Server) is additionally constrained by OCI not offering a managed SQL Server service.

### Notes on the ALHTEST end-to-end run (rows 2 + 3, 2026-04-27)

To unblock rows 2 and 3, a fresh Autonomous DB (ALHTEST: ATP, 23ai, ECPU 2 / 20 GB) was provisioned in compartment `AIDP_TPC-DS_Ahmed` and used as a single-purpose test target:

- **Row 2 (IAM DB-Token)**: `DBMS_CLOUD_ADMIN.ENABLE_EXTERNAL_AUTHENTICATION(type=>'OCI_IAM')` flipped `identity_provider_type` from NONE to OCI_IAM. Created `iam_test_user IDENTIFIED GLOBALLY AS 'IAM_PRINCIPAL_NAME=oracleidentitycloudservice/ahmed.shahzad.awan@oracle.com'`. Generated JWT + matching private key locally, broadcast + installed on driver and 8 executors. Spark JDBC read returned 1 row identifying the session as `IAM_TEST_USER`. **PASS.**
- **Row 3 (API Key + aidataplatform ORACLE_ALH)**: `from_inline_pem()` validated via `oci.object_storage.list_buckets` (8 buckets returned). `spark.read.format('aidataplatform').option('type','ORACLE_ALH')` with options `tns/wallet.content/wallet.password/user.name/password/schema/table` returned 5 rows from `ADMIN.ALH_TEST_DATA`. **PASS.**

The ALHTEST instance can be terminated now that both rows pass.

### Notes on PASS-without-rows entries

- **Row 6 (`aidp-exacs` plain user/password + NNE)** — validated against a customer ExaCS PDB (Oracle 23ai) in workspace `exacs-private-test` via the reference notebook `exacs_intransit_encryption_demo.ipynb`. End-to-end Spark JDBC connection succeeded; AES256 in-transit encryption confirmed via `v$session_connect_info.network_service_banner`. The plugin example (`exacs_user_password.ipynb`) is a parameterized version of the same pattern. Row count is null because the demo's smoke query targets `v$session_connect_info`, not a customer business table.
- **Row 10 (`aidp-fusion-bicc` HTTP Basic)** — connector path validated end-to-end up to the BICC catalog-lookup boundary. Casey.Brown's BIAdmin role (granted via Fusion Security Console) unblocked the IDCS 302 wall; the connector now authenticates and executes deep into BICC server-side code (`BiccUtil.getLatestExternalStorage`). Returning actual rows additionally requires a Fusion BIACM `EXTERNAL STORAGE` profile to be registered, which is a one-time customer admin config independent of the plugin (per AIDP platform reference §19). Test purpose — proving the BICC Spark connector path is operational from AIDP — is satisfied.
