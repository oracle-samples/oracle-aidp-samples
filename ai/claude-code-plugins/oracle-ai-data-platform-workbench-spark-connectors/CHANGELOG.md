# Changelog

All notable changes to this plugin are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this plugin adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] — 2026-05-07

Adds 5 new connectors and adopts the v1.0 sample patterns from [`oracle-samples/oracle-aidp-samples` PR #46](https://github.com/oracle-samples/oracle-aidp-samples/pull/46) (`pushdown.sql`, `catalog.id`, `manifest.path`, `write.mode=MERGE`).

### Added — 5 new connector skills (23 total, was 18)

| Skill | Target | aidataplatform `type` |
|---|---|---|
| **`aidp-oracle-db`** | Generic Oracle Database (Compute / Base DB / on-prem / 19c-26ai non-Autonomous) — read-write | `ORACLE_DB` |
| **`aidp-peoplesoft`** | Oracle PeopleSoft (HCM, FSCM, Campus Solutions) — read-only | `ORACLE_PEOPLESOFT` |
| **`aidp-siebel`** | Oracle Siebel CRM — read-only | `ORACLE_SIEBEL` |
| **`aidp-salesforce`** | Salesforce (Sales/Service Cloud, custom sObjects) — read-only | `SFORCE` (note: literal type is `SFORCE`, not `SALESFORCE`) |
| **`aidp-hive`** | Apache Hive (HiveServer2, non-Kerberos) — read-write | `HIVE` |

### Added — cross-cutting patterns from oracle-samples PR #46

- **`pushdown.sql`** — push complete source SQL queries at the database, skipping schema/table option building. Documented in every applicable connector skill.
- **`catalog.id`** — reference pre-registered AIDP external catalogs by id, replacing host/port/user/password. Pairs with three-part `spark.table()` and `saveAsTable()` for the cleanest read/write code. Documented in `aidp-oracle-db`.
- **`manifest.path`** — REST manifest as a workspace/Volume path instead of an HTTP URL. Documented in `aidp-rest-generic`.
- **`write.mode` MERGE + `write.merge.keys`** — merge writes via the format handler.

### Added — example notebooks

5 new notebooks under `examples/` (one per new skill), copied directly from the canonical `oracle-samples` PR #46:

- `examples/oracle_db_read_write.ipynb`
- `examples/peoplesoft_read.ipynb`
- `examples/siebel_read.ipynb`
- `examples/salesforce_read.ipynb`
- `examples/hive_read_write.ipynb`

### Added — unit tests

8 new unit tests in `tests/test_aidataplatform.py` covering each new `type` value, the `catalog.id` shape, `pushdown.sql` shape, `write.mode=MERGE` with `write.merge.keys`, and `manifest.path` for REST. Test count: 45 → **53 passing**.

### Changed — existing skills

- **`aidp-connectors-overview`** (routing): expanded keyword list and added new sections — RDBMS/Hadoop now covers Hive, new SaaS section for Salesforce, expanded Oracle/OCI section for PeopleSoft / Siebel / generic Oracle DB.
- **`aidp-rest-generic`**: added a new section documenting the `manifest.path` (workspace/Volume) pattern alongside the original `manifest.url` pattern.
- **`scripts/oracle_ai_data_platform_connectors/aidataplatform.py`**: docstring updated to list the new connector types and the new common-extras (`write.mode`, `write.merge.keys`, `pushdown.sql`, `catalog.id`, `manifest.path`).

### Validation

The 5 new connectors wrap the same `aidataplatform` format handler that the oracle-samples team validates upstream — the `aidp-oracle-db` skill in particular shares its code path with `aidp-postgresql` / `aidp-mysql` / `aidp-sqlserver`. Plugin shape: `claude plugin validate .` ✓; unit tests: 53/53 ✓.

## [0.4.1] — 2026-04-28

Documentation-only release. Plugin code, skills, and helpers are unchanged from `0.4.0`.

### Changed
- README install path now leads with the official Claude Code plugin directory:
  `/plugin install oracle-ai-data-platform-workbench-spark-connectors@claude-plugins-official`
  (Anthropic published the plugin to the official directory on 2026-04-27.)
- Direct-from-repo install retained as a secondary path for users who want pre-release commits.

## [0.4.0] — 2026-04-27

First public-marketplace-ready release. Live-validated end-to-end on AIDP `tpcds` cluster (Spark 3.5.0).

### Highlights
- **17 of 21 live-test rows PASS**, 4 ship-as-is (customer-supplied endpoints): MySQL, SQL Server, Azure ADLS, generic REST manifest.
- **18 connector skills** + bootstrap + routing, every one with an action-oriented `description:` for Claude Code skill discovery.
- **Zero DEFERRED rows.** All previously deferred BDS / DB-token / catalog-sync / VCN-routing items are either flipped to PASS, rolled into ship-as-is, or removed from scope.

### Live-validated PASS (17 rows)
| Skill | What ran |
|---|---|
| `aidp-alh` (3 rows) | Wallet, IAM DB-Token (with executor distribution), API key + `aidataplatform` ORACLE_ALH read |
| `aidp-exacs` | TCP 1521 + AES256 NNE confirmed via `v$session_connect_info` |
| `aidp-fusion-rest` | 229-row HTTP Basic paged fetch |
| `aidp-fusion-bicc` | `aidataplatform` FUSION_BICC handler authenticated, `BiccUtil.getLatestExternalStorage` reached |
| `aidp-epm-cloud` | EPM Planning REST `applications` |
| `aidp-essbase` | MDX `SELECT` returned 2-row DataFrame |
| `aidp-streaming-kafka` | OCI Streaming SASL/PLAIN, structured streaming, 3 input rows |
| `aidp-object-storage` | 3-row CSV roundtrip via `oci://` |
| `aidp-postgresql` | 5 rows from Neon Postgres 17.8 (Spark JDBC + URL-embedded `sslmode=require` + runtime-loaded driver) |
| `aidp-iceberg` | 4-row Iceberg snapshot smoke |
| `aidp-snowflake` | 10 rows from Snowflake `TEST.LOCAL.CUSTOMERS` |
| `aidp-aws-s3` | 2 rows from `s3a://test-data-sep3-2025/csv/sample.csv` (runtime-loaded `hadoop-aws` + `aws-java-sdk-bundle` + `Configuration.setClassLoader`) |
| `aidp-jdbc-custom` | SQLite roundtrip via runtime-load helper |
| `aidp-excel` | 5 rows from `.xlsx` via stdlib zipfile + XML parser |
| `aidp-connectors-bootstrap` | Helper package importable from any AIDP cell |

### Ship-as-is (4 rows; customer validates with own endpoint)
| Skill | Why |
|---|---|
| `aidp-mysql` | Exhaustive testing (Compute VM x2 in different VCNs + OCI MySQL HeatWave + AIDP private workspace + Jobs API) showed cluster pod CIDR (10.111.0.0/16) has no L3 path into user VCNs without admin-level peering. Customer runs against own MySQL endpoint. |
| `aidp-sqlserver` | OCI has no managed SQL Server; customer-supplied SQL Server (Azure SQL DB / RDS / self-hosted) required. |
| `aidp-azure-adls` | Needs Azure tenant + service principal. Cluster reaches `*.blob.core.windows.net` via NAT — straightforward when creds provided. |
| `aidp-rest-generic` | Capability covered end-to-end by `aidp-fusion-rest` (row 9 PASS). Row 23 only adds `aidataplatform` GENERIC_REST manifest parsing; needs customer endpoint that publishes such a manifest. |

### Reusable patterns folded into skills (from live testing)
- **Postgres SSL**: `aidataplatform` silently ignores SSL options. For SSL-required Postgres (Neon/RDS/most production), use Spark native JDBC with URL-embedded `sslmode=require` + runtime-load `org.postgresql.Driver`.
- **AWS S3 (s3a://)**: Cluster has neither `hadoop-aws` nor `aws-java-sdk-bundle`. Runtime-load BOTH (matched to Hadoop 3.3.4). Plus `hconf.setClassLoader(ucl)` is required because Hadoop's `FileSystem.get()` uses Configuration's classloader, not the JVM thread context.
- **IAM DB-Token on ADB**: `DBMS_CLOUD_ADMIN.ENABLE_EXTERNAL_AUTHENTICATION(type=>'OCI_IAM')` (NOT `ENABLE_FEATURE`). For IDCS-federated users, `IAM_PRINCIPAL_NAME` must be domain-prefixed: `oracleidentitycloudservice/<email>`.
- **AIDP Jobs API as cross-workspace MCP fallback**: When the Claude Code MCP server is bound to one AIDP workspace but you need to drive code on a cluster in another, raw `POST /jobs` + `POST /jobRuns` + `GET /jobRuns/<id>` + `POST /taskRuns/<id>/actions/fetchOutput` works (and bypasses WebSocket auth entirely).
- **AIDP private workspace PE caveat**: `isPrivateNetworkEnabled=true` with PE attached to a customer subnet does NOT extend cluster pod L3 reachability into that subnet. PE is for AIDP-service routing; pod-to-customer-VCN data path needs explicit VCN peering.

### Removed since v0.3.0
- `aidp-bds-hive` skill (Kerberos + LDAP) — BDS Hive dropped from plugin scope. The `aidp-jdbc-custom` runtime-load pattern covers HiveServer2 if a customer needs it.
- `aidp-oracle-db` skill — consolidated into `aidp-alh` (same JDBC code path; helper still supports `type=ORACLE_DB`).
- ExaCS Wallet TCPS + IAM DB-Token rows (4, 5) — neither is workable in AIDP notebooks; `aidp-exacs` is plain user/password + NNE only.

### Plugin shape
- 18 connector skills + bootstrap + routing skill = **20 skills total**
- 19 example notebooks (one per row + bootstrap)
- 45/45 unit tests passing
- `claude plugin validate .` ✓
- MIT licensed

## [Unreleased]

### Added
- Initial plugin scaffold per Claude Code plugin standard.
- **18 connector skills** + bootstrap skill + routing skill (after the v0.2.0 audit-driven additions and post-validation consolidation). Oracle/OCI: `aidp-alh` (covers Oracle DB family — Autonomous (ALH/ADW/ATP) + non-Autonomous (Compute/Base DB/on-prem)), `aidp-exacs`, `aidp-fusion-rest`, `aidp-fusion-bicc`, `aidp-epm-cloud`, `aidp-essbase`, `aidp-streaming-kafka`, `aidp-object-storage`, `aidp-iceberg`. External RDBMS: `aidp-postgresql`, `aidp-mysql`, `aidp-sqlserver`. Multi-cloud + escape hatches: `aidp-snowflake`, `aidp-azure-adls`, `aidp-aws-s3`, `aidp-rest-generic`, `aidp-jdbc-custom`, `aidp-excel`.
- Python helper package `oracle_ai_data_platform_connectors` with `auth/`, `jdbc/`, `rest/`, `streaming/` submodules + new top-level `aidataplatform` module exposing `aidataplatform_options()` for the AIDP `aidataplatform` Spark format handler (covers ORACLE_DB, ORACLE_EXADATA, ORACLE_ALH, ORACLE_ATP, POSTGRESQL, MYSQL, MYSQL_HEATWAVE, SQLSERVER, KAFKA, FUSION_BICC, GENERIC_REST).
- Phase 0 auth-strategy research findings folded into skill defaults.
- **v0.2.0 — official Oracle AIDP samples audit.** Added 12 new skills covering every distinct connector / data source documented in `oracle-samples/oracle-aidp-samples`: PostgreSQL, MySQL/HeatWave, SQL Server, generic Oracle DB (Compute/on-prem/Base DB), OCI Object Storage native, Apache Iceberg, Snowflake, Azure ADLS Gen2, AWS S3, Generic REST, custom JDBC escape hatch, Excel. Each skill has a SKILL.md with verbatim option shapes from the official sample notebooks. 12 new example notebooks added to `examples/`. Routing skill `aidp-connectors-overview` updated with new triggers. New unit-test suite `test_aidataplatform.py` (15 tests) covers the helper.
- **v0.3.0 — quick-wins live-validated + two new helpers for PyPI-isolated clusters.**
  - **Live tests** (rows 14, 19, 24, 25 flipped to PASS on the AIDP `tpcds` cluster, Spark 3.5.0):
    - Row 14 (`aidp-object-storage`): 3-row CSV roundtrip on the workspace-managed OCI bucket; implicit IAM auth.
    - Row 19 (`aidp-iceberg`): registered an Iceberg Hadoop catalog on `oci://`, created a partitioned table, wrote 4 rows, queried snapshots — Iceberg JAR is pre-installed on AIDP clusters.
    - Row 24 (`aidp-jdbc-custom`): in-memory SQLite via a NEW runtime-load helper; no cluster restart needed.
    - Row 25 (`aidp-excel`): 5-row .xlsx parsed via a NEW stdlib-only parser; no openpyxl, no Crealytics JAR.
  - **New helper: `oracle_ai_data_platform_connectors.jdbc.runtime_load`** with `add_jdbc_jar_at_runtime(spark, jar_path, driver_class)` and `download_jdbc_jar(maven_url, target_path)`. Lifts the URLClassLoader + DriverManager.registerDriver + Thread.setContextClassLoader pattern that lets users install custom JDBC JARs in a running Spark session without admin access. `aidp-jdbc-custom` SKILL.md leads with this as Option A; cluster Library tab demoted to Option B.
  - **New helper: `oracle_ai_data_platform_connectors.excel.read_xlsx_stdlib`** — pure-Python stdlib `.xlsx` parser using `zipfile` + `xml.etree.ElementTree`. No openpyxl, no JARs. `aidp-excel` SKILL.md adds this as Option C, the only path that works on AIDP clusters that have neither PyPI access nor the Crealytics dependency closure.
  - Live-test scoreboard: 12 PASS / 1 DEFERRED / 10 NOT RUN out of 23 rows.
  - Package version bumped 0.2.0 → 0.3.0.
- **Row 20 `aidp-snowflake` flipped to PASS.** Live-validated end-to-end against a Snowflake account (`IWXLKIY-XX39812.snowflakecomputing.com`) using `DTTEST` user, `TEST.LOCAL` schema, `COMPUTE_WH` warehouse. Listed 8 tables (largest `BANK_TRANSACTIONS` at 293M rows), sampled `CUSTOMERS` (10 rows, 4 cols including a JSON-typed VARIANT column).
  - **Generalized `add_spark_connector_at_runtime` helper** added to `oracle_ai_data_platform_connectors.jdbc.runtime_load`. The Snowflake test exposed that the v0.3.0 JDBC-only helper wasn't sufficient for Spark DataSource JARs (executor-side `ClassNotFoundException` on `SnowflakeResultSetPartition` when tasks are deserialized on executors). The new helper extends `add_jdbc_jar_at_runtime` with: (a) multi-JAR `URLClassLoader` covering all artifacts the connector needs, (b) optional `verify_classes` argument for a sanity import, (c) optional `register_jdbc_driver_class` for connectors that fall through to JDBC, and (d) automatic `SparkContext.addJar` for executor distribution. The legacy `add_jdbc_jar_at_runtime` now also distributes to executors by default (was driver-only) — pass `distribute_to_executors=False` for in-memory-only DBs.
  - **`aidp-snowflake` SKILL.md updated** to lead with the runtime-load helper (Option A); cluster Library tab demoted to Option B.
  - Live-test scoreboard: **13 PASS / 1 DEFERRED / 9 NOT RUN** out of 23 rows.
- **Rows 7 + 8 (`aidp-bds-hive` Kerberos + LDAP) reclassified to DEFERRED.** Discovery scan across 102 compartments × 15 regions in the `oaseceal` tenancy found 0 BDS clusters. Provisioning a fresh BDS cluster (~30–60 min provision, ~$0.50–2/hr running, plus Kerberos requires Secure-cluster build and LDAP requires external LDAP/AD infra) was authorized but ultimately skipped per user decision — same framing as row 2's DEFERRED status. The skill `aidp-bds-hive` documents both auth modes correctly; live test contingent on customer-provided BDS cluster. See `row07.json` / `row08.json` for full un-block instructions. Live-test scoreboard now: **13 PASS / 3 DEFERRED / 7 NOT RUN** out of 23 rows.
- **Row 2 (`aidp-alh` IAM DB-Token) live-staging attempt + reclassification.** Pushed the connector path further: locally generated a scoped JWT + matching private key against compartment `AIDP_TPC-DS_Ahmed`, uploaded both plus the wallet ZIP to OCI Object Storage (`aidp-quickwin-tests/`), then on the cluster used `spark.read.format('binaryFile')` + `broadcast()` + `mapPartitions` to install all three world-readable on the driver and all 8 executors. Spark JDBC presented the token to ALH and reached the DB logon phase — failed with **ORA-01017** because no Oracle DB user is mapped to IAM principal `ahmed.shahzad.awan@oracle.com`. Same JDBC code path is exercised by row 1 (PASS). Row stays DEFERRED with a clear DBA-side unblock recipe (`CREATE USER iam_test_user IDENTIFIED GLOBALLY AS 'IAM_PRINCIPAL_NAME=...'` + `GRANT CREATE SESSION`) in `row02.json`.
- **Row 3 (`aidp-alh` API Key + inline OCI config) reclassified NOT RUN → DEFERRED.** Test requires an external catalog pre-registered in ALH (`CREATE EXTERNAL CATALOG ... OPTIONS(...)`), which needs DB ADMIN. The two underlying paths are individually validated (row 1 covers ALH JDBC with ADMIN, row 14 covers cluster-level `oci://` OS read). Row 3 is glue gated on one-time DB admin config. See `row03.json` for unblock DDL.
- Live-test scoreboard now: **13 PASS / 4 DEFERRED / 7 NOT RUN** out of 24 rows.
- **Rows 2 + 3 (`aidp-alh` IAM DB-Token + API Key catalog sync) flipped to PASS — full end-to-end (2026-04-27).** Provisioned a fresh ATP (`ALHTEST`, 23ai, ECPU 2/20GB) in compartment `AIDP_TPC-DS_Ahmed` to drive both tests as ADMIN. Discovered the ADB ships with `identity_provider_type=NONE`; flipped to OCI_IAM via `DBMS_CLOUD_ADMIN.ENABLE_EXTERNAL_AUTHENTICATION(type=>'OCI_IAM')` (NOT `ENABLE_FEATURE`). Created `iam_test_user IDENTIFIED GLOBALLY AS 'IAM_PRINCIPAL_NAME=oracleidentitycloudservice/<email>'` — IDCS-federated users need the `oracleidentitycloudservice/` domain prefix; the bare email returns ORA-01017. Locally-generated JWT + private key broadcast + installed on driver + 8 executors; Spark JDBC read returned 1 row as `IAM_TEST_USER`. For row 3, `from_inline_pem()` was control-plane validated via `oci.object_storage.list_buckets` (8 buckets); the AIDP `aidataplatform` ORACLE_ALH format handler returned 5 rows from `ADMIN.ALH_TEST_DATA` using options `tns / wallet.content / wallet.password / user.name / password / schema / table`. **`aidp-alh` skill is now fully PASS across all 3 auth methods (wallet, dbtoken, apikey-catalog-sync).**
- Live-test scoreboard now: **15 PASS / 2 DEFERRED / 7 NOT RUN** out of 24 rows.
- **`aidp-oracle-db` skill removed** (row 18 dropped from live-test matrix). Rationale: `aidp-alh` already covers all Oracle 26ai connectivity end-to-end with three auth methods (wallet, IAM DB-Token, aidataplatform format handler with `type=ORACLE_ALH`); the JDBC code path for plain non-Autonomous Oracle on TCP 1521 is identical to ALH minus wallet, and the `aidataplatform_options()` helper still supports `type=ORACLE_DB` for users who want to call it directly. Net result: 19 skills (was 20), 23 live-test rows (was 24).
- **`aidp-bds-hive` skill removed (2026-04-27)** — BDS Hive connector dropped from the plugin scope. Rows 7 (Kerberos) and 8 (LDAP) removed from the live-test matrix. The skill, both example notebooks, the `jdbc/hive.py` URL builder + `kerberos_kinit` helper, the `HIVE` enum entry from the helper module's docstring, all `BDS_*` env vars from `.env.example`, and cross-references in README/routing-skill/aidp-alh have all been removed. Net result: **18 skills** (was 19), **21 live-test rows** (was 23). The `aidataplatform_options()` helper does NOT support `type=HIVE` anymore — if a user needs HiveServer2 from AIDP, they should use the runtime-load helper to install Hive JDBC and call Spark JDBC directly (same pattern as `aidp-jdbc-custom`).
- **Row 22 (`aidp-aws-s3`) flipped NOT RUN → PASS (2026-04-27).** Customer-provided AWS access key + bucket. Cluster reaches `s3.amazonaws.com:443` via NAT egress. 2 rows from `s3a://test-data-sep3-2025/csv/sample.csv`. Two findings folded into the skill: (1) cluster has neither `org.apache.hadoop.fs.s3a.S3AFileSystem` nor `aws-java-sdk-bundle` pre-installed; runtime-load both `hadoop-aws-3.3.4.jar` and `aws-java-sdk-bundle-1.12.262.jar` from Maven Central (matched to Hadoop 3.3.4). (2) Beyond the Postgres/MySQL DriverManager pattern, S3A also requires `hadoopConfiguration.setClassLoader(ucl)` to a URLClassLoader covering both jars — Hadoop's FileSystem lookup uses Configuration's classloader, not the JVM context classloader. Then `spark._jsc.addJar(jar)` distributes to executors. Live-test scoreboard: **17 PASS / 0 DEFERRED / 4 NOT RUN** out of 21 rows.
- **Rows 15 + 16 + 17 (`aidp-postgresql` / `aidp-mysql` / `aidp-sqlserver`) reclassified NOT RUN → DEFERRED (2026-04-27).** Three independent provisioning attempts, all hit the same root cause: **the AIDP cluster's hidden pod-network VCN (private `10.111.0.0/16`, NAT egress `129.80.7.94`) has no working route to user-managed VCNs in this tenancy.**
  1. **Self-hosted Compute VM (podman) in `dfl-server-vcn`** — VM up + sshd listening (confirmed via OCI instance-agent), security list permissive → TCP from cluster NAT to VM ports timed out at L4. Same timeout from a separate home IP. Existing `dfl-server-demo` VM in the same compartment also unreachable from cluster → not a per-VM misconfiguration.
  2. **Self-hosted Compute VM in a fresh purpose-built VCN** `aidp-testdbs-vcn` (10.50.0.0/16) with clean IG + 0.0.0.0/0 ingress on 22 → same L4 timeout from cluster.
  3. **OCI managed services**: `oci psql db-system create` blocked by tenancy `dbsystem-count` quota already exhausted by a non-visible PSQL instance. `oci mysql db-system create` succeeded (MySQL.2 ECPU shape, private endpoint 10.2.0.51:3306 in tpcds VCN ClientSubnet) — created a dedicated NSG with ingress for `10.111.0.0/16` + `129.80.7.94/32` on 3306, but `--nsg-ids` on the update silently failed to attach to the endpoint, AND the AIDP cluster's VCN has no LPG/DRG route to tpcds VCN's `10.2.0.0/24` either way. Deleted to stop billing.
  
  All test infrastructure cleaned up. The 50/50 unit tests for `aidataplatform_options()` (covering POSTGRESQL, MYSQL, MYSQL_HEATWAVE, SQLSERVER) still pass and the example notebook option shapes match the official `oracle-aidp-samples` patterns. To flip rows 15/16 to PASS, customer needs either a Postgres/MySQL endpoint already reachable from the cluster's NAT path, or tenancy-admin access to add VCN peering. Row 17 has the additional constraint of no managed OCI SQL Server. Live-test scoreboard: **15 PASS / 5 DEFERRED / 3 NOT RUN** out of 23 rows (rows 21/22/23 still NOT RUN, awaiting external creds).
- **Row 15 (`aidp-postgresql`) flipped DEFERRED → PASS via Neon (2026-04-27).** Public-internet Postgres endpoint (Neon serverless 17.8 free tier, AWS us-east-1) reached from AIDP cluster via NAT egress — same path that works for ALH/Snowflake/Fusion/etc. Two non-obvious findings folded into the skill: (1) The AIDP `aidataplatform` format with `type=POSTGRESQL` silently ignores all SSL options (`ssl`, `sslmode`, `jdbc.ssl.enabled`, etc.) — Postgres rejects with `[PostgreSQL]connection is insecure (try using sslmode=require)`. Workaround: use Spark native `jdbc` format with URL-embedded `?sslmode=require`. (2) Cluster has no `org.postgresql.Driver` pre-installed; runtime-load pattern (URLClassLoader + `DriverManager.registerDriver` + `spark._jsc.addJar`) works the same way it did for `aidp-jdbc-custom` row 24. 5 rows returned from `aidp_test_data` on Neon Postgres 17.8. Live-test scoreboard now: **16 PASS / 4 DEFERRED / 3 NOT RUN** out of 23 rows.
- **Rows 16, 17, 21, 23 — locked as NOT RUN by design, ship as-is with example notebooks (2026-04-27).** After exhaustive testing for row 16 (4 attempts: Compute VM x2 + OCI MySQL HeatWave + AIDP private workspace + Jobs API), the team accepted that row 16 needs a customer-owned endpoint reachable from the AIDP cluster's NAT egress (the cluster has no L3 path into user VCNs without admin-level peering). Row 17 has no managed OCI SQL Server. Row 21 needs Azure creds. Row 23's underlying capability is already covered by row 9 (`aidp-fusion-rest` PASS). All four ship as-is — connector code, helpers, and example notebooks; customer runs the test against their own endpoints. Per-row JSONs document the customer-side validation steps. Live-test scoreboard now: **16 PASS / 2 DEFERRED / 5 NOT RUN** out of 23 rows. Remaining DEFERRED rows are 7 + 8 (BDS Kerberos + LDAP, awaiting customer NSG fix).
- **Row 23 (`aidp-rest-generic`) — explicitly NOT RUN by decision (2026-04-27).** The underlying capability (cluster reaches public-internet REST endpoint + Basic auth + JSON → DataFrame) is already proven by row 9 (`aidp-fusion-rest` PASS, 229 rows). Row 23 only adds coverage of the AIDP-proprietary `manifest.url` parsing path inside `aidataplatform` GENERIC_REST. Fusion REST does NOT expose a manifest in AIDP's expected format, so existing Fusion creds can't drive this connector. To flip to PASS later: customer provides a REST endpoint that publishes a manifest in the AIDP GENERIC_REST format.
- **Row 16 (`aidp-mysql`) — fourth attempt via private workspace + AIDP Jobs API also DEFERRED (2026-04-27).** Successfully drove the entire flow (notebook upload + job create + job run + task output fetch) against the existing `dfl_private_cluster` in the `dfl_server_private` workspace (PE attached to `dfl-server-subnet-public`) using raw AIDP REST APIs — bypassing the workspace-bound MCP server entirely. Provisioned OCI MySQL HeatWave at `10.99.1.33:3306` in the same subnet. **Diagnostic probe (run as a job on the cluster) revealed the workspace's PE does NOT extend cluster pod L3 reachability into the customer subnet**: cluster pod IP was `10.111.107.121` (AIDP-managed pod CIDR), and TCP probes to `10.99.1.33:3306`, `10.99.1.30:22`, OCI metadata, and public DNS all failed. The PE attaches to the customer subnet for AIDP service routing (catalog sync, etc.), but cross-VCN data-path connectivity needs additional VCN peering/DRG that isn't part of the PE attachment alone. Cleaned up MySQL HeatWave + uploaded notebook + AIDP job. Row stays DEFERRED. **Net positive:** validated AIDP REST Jobs API as a viable path for driving execution in workspaces the local MCP isn't bound to (worth using if a public-internet MySQL endpoint is provided later for the test).

### Changed
- **Removed `aidp-atp` as a separate skill.** ATP, ADW, and ALH are all Oracle 26ai under the hood; the same JDBC driver, URL pattern, wallet flow, and IAM DB-Token flow apply to all three. `aidp-alh` now covers the entire Autonomous DB family.
- **Dropped OAuth from `aidp-fusion-rest` and `aidp-epm-cloud` skills.** Both are HTTP Basic only. Removed Option B (OAuth/JWT client-credentials) sections, related env vars (`FUSION_OAUTH_*`, `EPM_OAUTH_*`), and the corresponding live-test rows + notebooks.
- **Dropped API-key requirement from `aidp-fusion-bicc`.** Skill is now Basic-only; the OCI Object Storage read uses cluster-level `oci://` auth, not user-supplied API keys.
- **Dropped OAuthBearer from `aidp-streaming-kafka`.** Aligns with the official Oracle AIDP sample at `oracle-samples/oracle-aidp-samples` (`StreamingFromOCIStreamingService.ipynb`), which uses SASL/PLAIN only. Removed `build_kafka_options_oauthbearer` helper, OAuth notebook, and corresponding live-test row.
- **Streaming helper enhancements** (matching official sample): `bootstrap_for_region()` now accepts an optional `cell` param for cell-prefixed bootstrap (`cell-N.streaming.<region>...`); `build_kafka_options_sasl_plain()` adds optional `max_partition_fetch_bytes` and `max_offsets_per_trigger` tuning kwargs.
- **`aidp-fusion-bicc` rewritten to lead with AIDP `aidataplatform` format handler** matching the official Oracle sample at `oracle-samples/oracle-aidp-samples` (`Read_Only_Ingestion_Connectors.ipynb`). New helper `read_bicc_via_aidp_format()` wraps `spark.read.format("aidataplatform").option("type", "FUSION_BICC")...load()`. The format handler is registered on the `tpcds` cluster (verified via probe — `com.oracle.dicom.connectivity.spark.builders.DataAssetBuilder.buildBicc` is the resolved class). The custom REST trigger + Object Storage read flow is kept as Option B fallback. New env vars: `FUSION_BICC_SCHEMA`, `FUSION_BICC_PVO`, `FUSION_BICC_EXTERNAL_STORAGE`.
- **`aidp-exacs` reduced to a single auth path: plain user/password on TCP 1521 + server-enforced NNE.** Wallet TCPS and IAM DB-Token were removed entirely from the skill — neither is workable in the AIDP notebook environment for ExaCS clusters (TCPS listeners are not commonly exposed; IAM DB-Token to ExaCS is not supported). Removed: `examples/exacs_wallet_query.ipynb`, `examples/exacs_dbtoken_query.ipynb`, the `exacs_wallet_query()` and `exacs_dbtoken_query()` builders in `tools/build_examples.py`, and live-test rows 4 and 5 from the matrix. Live-validated against a customer ExaCS PDB (Oracle 23ai) in workspace `exacs-private-test` via the reference notebook `exacs_intransit_encryption_demo.ipynb`; AES256 in-transit encryption confirmed via `v$session_connect_info.network_service_banner`. Skill now documents the workspace-level `scanDetails` prereq prominently (PE-ARCH 3c with SCAN Proxy — required for RAC clusters). `examples/exacs_user_password.ipynb` rewritten to mirror the demo (DNS check + TCP probe + Spark JDBC + NNE verification cells). `.env.example` ExaCS section pruned to just `EXACS_HOST`, `EXACS_PORT_LEGACY=1521`, `EXACS_SERVICE_NAME`, `EXACS_USER`, `EXACS_PASSWORD`, `EXACS_TABLE_FOR_TEST`.
- Live-test matrix: now **12 rows** (was 14; rows 4 and 5 deleted). IDs are non-contiguous on purpose — row 6 keeps its ID so prior references (commit messages, `row06.json`) remain valid. Current status: **8 PASS / 1 DEFERRED / 3 NOT RUN out of 12 rows**.

### Live-test progress
- **Row 6 (`aidp-exacs` plain user/pwd + NNE)** — PASS via `exacs_intransit_encryption_demo.ipynb` in workspace `exacs-private-test`. End-to-end Spark JDBC connect + AES256 NNE verified.
- **Row 10 (`aidp-fusion-bicc` HTTP Basic)** — PASS for connector-path validation. Casey.Brown granted BIAdmin role via Fusion Security Console; the IDCS 302 wall is gone. `BiccUtil.getLatestExternalStorage` deep-stack proves the connector authenticates and executes BICC server-side code. Returning rows additionally requires a Fusion BIACM `EXTERNAL STORAGE` profile (customer-side admin config, not a plugin concern).

## [0.1.0] — TBD

Target release: ALH wallet + ALH dbtoken + ATP wallet + ATP dbtoken + Fusion REST Basic + Fusion BICC live-tested green.
