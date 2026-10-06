# Live-test results


**Summary:** Rows 16, 17, 21, 23 ship as-is (connector code + example notebook ship; customer validates against their own endpoint). Per-row evidence is kept internally; this public copy carries the scoreboard only.

Row IDs 4 (ExaCS Wallet TCPS), 5 (ExaCS IAM DB-Token), 7 + 8 (`aidp-bds-hive` Kerberos + LDAP), and 18 (`aidp-oracle-db` plain TCP 1521) were removed. ExaCS rows 4/5 aren't supported by AIDP notebooks for ExaCS clusters. The `aidp-bds-hive` skill was dropped from the plugin (BDS Hive not in scope for this connector pack). Row 18 was dropped because the `aidp-alh` family already covers Oracle 26ai connectivity end-to-end across all three auth methods (wallet, IAM DB-Token, aidataplatform format handler) — a separate plain-1521 Oracle DB skill adds no incremental coverage and the JDBC code path is identical.

**v0.2.0** added rows 14–25 covering Object Storage, Iceberg, Postgres, MySQL/HeatWave, SQL Server, Snowflake, ADLS Gen2, AWS S3, generic REST, custom JDBC, Excel — all sourced from the official `oracle-samples/oracle-aidp-samples` repo.

**v0.3.0 quick-wins**: rows 14, 19, 24, 25 flipped to PASS — Object Storage CSV roundtrip, Iceberg Hadoop catalog smoke, custom-JDBC SQLite (via new runtime-load helper), Excel ingestion (via new stdlib zipfile+XML parser). Rows 24 and 25 produced new helper modules to handle PyPI-unreachable clusters.

**Rows 2 + 3 (`aidp-alh` IAM DB-Token + API Key catalog sync) flipped to PASS (2026-04-27)** by provisioning a single-purpose Autonomous DB (`ALHTEST`, ATP 23ai, ECPU 2/20GB) in a test compartment. Row 2 required `DBMS_CLOUD_ADMIN.ENABLE_EXTERNAL_AUTHENTICATION(type=>'OCI_IAM')` plus an IDCS-domain-prefixed `IAM_PRINCIPAL_NAME` mapping; row 3 used the `aidataplatform` ORACLE_ALH format handler with `from_inline_pem` exercised separately on the OCI control plane. Aidp-alh skill now PASS across all 3 documented auth methods.

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

Per-row evidence files (`row<N>.json`) are not published: they recorded the test
tenancy's identifiers. The scoreboard above is the public record; the connector code
and example notebooks are what each row exercised.
