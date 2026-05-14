---
description: Help the user pick the right connector skill for their data source from an AIDP notebook. Use as a router when the user mentions multiple sources, isn't sure which connector applies, or asks "how do I connect to X from AIDP". Covers 23 data sources — Oracle Autonomous DB family (ALH/ADW/ATP), generic Oracle DB, ExaCS, PeopleSoft, Siebel, Fusion ERP/BICC, EPM Cloud, Essbase, OCI Streaming, Object Storage, Iceberg, plus PostgreSQL, MySQL/HeatWave, SQL Server, Hive, Snowflake, Azure ADLS, AWS S3, Salesforce, generic REST, custom JDBC, Excel.
allowed-tools: Read
---

# `aidp-connectors-overview` — pick the right connector skill

## When to use
- The user is exploring options ("how do I connect to Oracle from AIDP?", "which connector should I use?").
- The user mentions multiple Oracle sources at once.
- The user describes a source by capability (e.g. "OLAP cube", "structured streaming") rather than naming the product directly.

## When NOT to use
- The user has already named their target product (ALH, ATP, Fusion, etc.) — invoke the matching `aidp-<product>` skill directly.

## How to route

**Before any connector skill works**, the helper package must be uploaded to the user's AIDP workspace. If the user hasn't done this yet (or you see `ModuleNotFoundError: oracle_ai_data_platform_connectors` in any prior cell), invoke [`aidp-connectors-bootstrap`](../aidp-connectors-bootstrap/SKILL.md) first. It uses the AIDP MCP tools to push the package into `/Workspace/Shared/` and runs a sanity-import notebook.

Otherwise, pick the right skill from this table and **invoke that skill**. Don't re-write its content here.

### Oracle / OCI sources

| User says... | Use skill |
|---|---|
| "ALH", "AI Lakehouse", "ADW", "ATP", "Autonomous Database", "26ai" | [`aidp-alh`](../aidp-alh/SKILL.md) — Autonomous DB family (wallet / IAM DB-Token / API key) |
| "Oracle Database", "generic Oracle DB", "on-prem Oracle", "Oracle 19c / 21c", "Base DB", "Oracle on Compute", non-Autonomous Oracle | [`aidp-oracle-db`](../aidp-oracle-db/SKILL.md) — generic Oracle DB via plain user/password on TCP 1521 |
| "ExaCS", "Exadata", "Exadata Cloud", "private-subnet Oracle DB" | [`aidp-exacs`](../aidp-exacs/SKILL.md) |
| "PeopleSoft", "PSFT", "PS HCM", "FSCM", "Campus Solutions" | [`aidp-peoplesoft`](../aidp-peoplesoft/SKILL.md) |
| "Siebel", "Siebel CRM", "S_CONTACT", "S_ORG_EXT" | [`aidp-siebel`](../aidp-siebel/SKILL.md) |
| "Fusion ERP", "Fusion HCM", "Fusion REST", "FA REST", "Cloud ERP API" | [`aidp-fusion-rest`](../aidp-fusion-rest/SKILL.md) |
| "BICC", "BI Cloud Connector", "Fusion bulk extract", >50k rows from Fusion | [`aidp-fusion-bicc`](../aidp-fusion-bicc/SKILL.md) |
| "EPM Cloud", "EPBCS", "Hyperion Planning", "Planning app", "exportdataslice" | [`aidp-epm-cloud`](../aidp-epm-cloud/SKILL.md) |
| "Essbase", "Essbase 21c", "MDX", "OLAP cube", "cube REST" | [`aidp-essbase`](../aidp-essbase/SKILL.md) |
| "OCI Streaming", "Kafka on OCI", "stream pool", "structured streaming Kafka" | [`aidp-streaming-kafka`](../aidp-streaming-kafka/SKILL.md) |
| "OCI Object Storage", "oci://", "external volume", "external table on bucket" | [`aidp-object-storage`](../aidp-object-storage/SKILL.md) |
| "Iceberg", "Apache Iceberg", "time travel", "snapshots", "schema evolution" | [`aidp-iceberg`](../aidp-iceberg/SKILL.md) |

### External RDBMS / Hadoop (non-Oracle)

| User says... | Use skill |
|---|---|
| "PostgreSQL", "Postgres", "psql" | [`aidp-postgresql`](../aidp-postgresql/SKILL.md) |
| "MySQL", "HeatWave", "MDS", "MySQL Database Service" | [`aidp-mysql`](../aidp-mysql/SKILL.md) |
| "SQL Server", "MSSQL", "Azure SQL", "TDS" | [`aidp-sqlserver`](../aidp-sqlserver/SKILL.md) |
| "Hive", "HiveServer2", "HS2", "HCatalog", non-Kerberos Hive | [`aidp-hive`](../aidp-hive/SKILL.md) |

### SaaS

| User says... | Use skill |
|---|---|
| "Salesforce", "SFDC", "Sales Cloud", "Service Cloud", "sObject", "SOQL", "Account / Opportunity / Lead" | [`aidp-salesforce`](../aidp-salesforce/SKILL.md) |

### Multi-cloud + escape hatches

| User says... | Use skill |
|---|---|
| "Snowflake", "sfUrl", "sfWarehouse" | [`aidp-snowflake`](../aidp-snowflake/SKILL.md) |
| "ADLS", "Azure Data Lake", "abfss" | [`aidp-azure-adls`](../aidp-azure-adls/SKILL.md) |
| "S3", "AWS S3", "s3a" | [`aidp-aws-s3`](../aidp-aws-s3/SKILL.md) |
| "Generic REST", "manifest URL", "manifest.path", REST endpoint with manifest schema | [`aidp-rest-generic`](../aidp-rest-generic/SKILL.md) |
| "Custom JDBC", "ClickHouse", "DuckDB", "DB2", "SAP HANA", any DB without a dedicated skill | [`aidp-jdbc-custom`](../aidp-jdbc-custom/SKILL.md) |
| ".xlsx", "Excel", "spreadsheet ingestion" | [`aidp-excel`](../aidp-excel/SKILL.md) |

## What's blocked at the AIDP platform level (so you don't try)
- **Instance Principal** — IMDS (`169.254.169.254`) is unreachable from AIDP notebooks; signer either fails or runs in AIDP's service tenancy, not the customer's.
- **Resource Principal** — AIDP sets `AIDP_AUTH=resource_principal` but does NOT provide `OCI_RESOURCE_PRINCIPAL_RPST` / `OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM`, so `get_resource_principals_signer()` raises.

If the user wants either of those, point them at API Key + inline OCI config (`oracle_ai_data_platform_connectors.auth.from_inline_pem`) instead. The AIDP team is aware; pending Oracle action.

## Cross-cutting AIDP gotchas (every connector inherits these)
1. **Credentials live under `/tmp/`** — never `/Workspace/`. The latter is FUSE-mounted; intermittent disconnects + `os.chmod` no-op.
2. **Files written for the JDBC driver process** must be world-readable up-front via `os.open(..., O_WRONLY|O_CREAT, 0o666)`.
3. **Spark streaming checkpoints** must live under `/Volumes/<catalog>/<schema>/<volume>/...`, never `/Workspace/`, never `oci://`.
4. **Refresh the AIDP session token** before live testing: `oci session authenticate --profile AIDP_SESSION --region us-ashburn-1`.

## References
- Plugin README: [../../README.md](../../README.md)
- Live-test matrix + results: [../../tests/live-results/RESULTS.md](../../tests/live-results/RESULTS.md)
- AIDP notebook auth investigation: `Claude context/AIDP/AIDP Context/AIDP/aidp-notebook-authentication.md`
