# Oracle AI Data Platform Workbench — Spark Connectors

> **Canonical home:** [`oracle-samples/oracle-aidp-samples/ai/claude-code-plugins/oracle-ai-data-platform-workbench-spark-connectors`](https://github.com/oracle-samples/oracle-aidp-samples/tree/main/ai/claude-code-plugins/oracle-ai-data-platform-workbench-spark-connectors).
> This repository is now a personal development mirror. End users should install via Anthropic's community marketplace (see below), which sources from the canonical Oracle-org location.

A Claude Code plugin that ships **23 model-invokable skills** for connecting Oracle AI Data Platform Workbench Spark notebooks to every data source these notebooks commonly need. Each skill produces plain Python (Spark JDBC, Spark structured streaming, Spark `oci://`/`s3a://`/`abfss://`, or REST → Spark DataFrame) that runs in the notebook without any additional runtime.

**v0.5.0** adds 5 new connectors (Oracle Database, PeopleSoft, Siebel, Salesforce, Hive) plus the new pushdown.sql / catalog.id / manifest.path patterns from [oracle-samples/oracle-aidp-samples PR #46](https://github.com/oracle-samples/oracle-aidp-samples/pull/46).

**Live-validated** on the workbench `tpcds` cluster (Spark 3.5.0): **17 PASS / 4 ship-as-is** out of 21 test rows in v0.4.x. The 5 new v0.5.0 connectors ship as-is — they wrap the same `aidataplatform` format handler validated upstream by the oracle-samples team. See [`tests/live-results/RESULTS.md`](tests/live-results/RESULTS.md).

## Install

From Anthropic's community plugin marketplace (recommended):

```
/plugin marketplace add anthropics/claude-plugins-community
/plugin install oracle-ai-data-platform-workbench-spark-connectors
```

Or from this development mirror (gets the latest pre-release commits):

```
/plugin marketplace add ahmedawan-oracle/claude-code-plugins
/plugin install oracle-ai-data-platform-workbench-spark-connectors@aidp-connectors
```

## What's in here

25 skills total (23 connectors + 1 bootstrap + 1 routing).

### Oracle / OCI sources
| Skill | Target | Transport | Recommended auth |
|---|---|---|---|
| `aidp-connectors-overview` | (router) | — | — |
| `aidp-connectors-bootstrap` | one-time setup | — | — |
| `aidp-alh` | Oracle Autonomous DB family (ALH / ADW / ATP) | Spark JDBC | Wallet (mTLS), IAM DB-Token, or API key |
| `aidp-oracle-db` ⭐ NEW | Generic Oracle DB (Compute / Base DB / on-prem / 19c-26ai non-Autonomous) | `aidataplatform` (`type=ORACLE_DB`) | Plain user/password |
| `aidp-exacs` | Exadata Cloud Service | Spark JDBC (TCP 1521 + NNE AES256) | Plain user/password |
| `aidp-peoplesoft` ⭐ NEW | Oracle PeopleSoft | `aidataplatform` (`type=ORACLE_PEOPLESOFT`) | Plain user/password (read-only) |
| `aidp-siebel` ⭐ NEW | Oracle Siebel CRM | `aidataplatform` (`type=ORACLE_SIEBEL`) | Plain user/password (read-only) |
| `aidp-fusion-rest` | Fusion ERP/HCM/SCM | REST → DataFrame | HTTP Basic |
| `aidp-fusion-bicc` | Fusion BICC bulk extracts | `aidataplatform` (`type=FUSION_BICC`) | HTTP Basic |
| `aidp-epm-cloud` | EPM Cloud Planning | REST → DataFrame | HTTP Basic (`tenancy.user@domain`) |
| `aidp-essbase` | Essbase 21c | REST + MDX → DataFrame | HTTP Basic |
| `aidp-streaming-kafka` | OCI Streaming | Spark structured streaming | SASL/PLAIN with OCI auth token |
| `aidp-object-storage` | OCI Object Storage native | Spark `oci://` | Implicit IAM (workspace identity) |
| `aidp-iceberg` | Apache Iceberg on OCI Object Storage | Iceberg Hadoop catalog on `oci://` | Implicit IAM |

### External RDBMS / Hadoop (`aidataplatform` format)
| Skill | Target | Transport | Recommended auth |
|---|---|---|---|
| `aidp-postgresql` | PostgreSQL | Spark JDBC (runtime-loaded driver) for SSL targets, `aidataplatform` `type=POSTGRESQL` for non-SSL | Plain user/password |
| `aidp-mysql` | MySQL / OCI MySQL HeatWave | `aidataplatform` (`type=MYSQL` or `MYSQL_HEATWAVE`) | Plain user/password |
| `aidp-sqlserver` | Microsoft SQL Server / Azure SQL DB | `aidataplatform` (`type=SQLSERVER`) | Plain user/password |
| `aidp-hive` ⭐ NEW | Apache Hive (HiveServer2, non-Kerberos) | `aidataplatform` (`type=HIVE`) | Plain user/password |

### SaaS
| Skill | Target | Transport | Recommended auth |
|---|---|---|---|
| `aidp-salesforce` ⭐ NEW | Salesforce (Sales/Service Cloud, custom sObjects) | `aidataplatform` (`type=SFORCE`) | Username + password+security-token (read-only) |

### Multi-cloud + escape hatches
| Skill | Target | Transport | Recommended auth |
|---|---|---|---|
| `aidp-snowflake` | Snowflake | `format("snowflake")` (Snowflake Spark connector) | sfUser/sfPassword |
| `aidp-azure-adls` | Azure ADLS Gen2 | Spark `abfss://` | OAuth client-credentials (Service Principal) |
| `aidp-aws-s3` | AWS S3 | Spark `s3a://` (runtime-loaded `hadoop-aws` + `aws-java-sdk-bundle`) | AWS access keys |
| `aidp-rest-generic` | Any REST API with a manifest URL or Volume `manifest.path` | `aidataplatform` (`type=GENERIC_REST`) | HTTP Basic |
| `aidp-jdbc-custom` | Any DB with a JDBC driver | Spark `format("jdbc")` (runtime-loaded driver) | Driver-specific |
| `aidp-excel` | `.xlsx` files in Volumes / Object Storage | stdlib `zipfile` + XML parser (no extra deps) | None (file-based) |

### v0.5.0 cross-cutting patterns

The new oracle-samples PR #46 introduced three patterns that are wired into every applicable skill:

| Pattern | What it does |
|---|---|
| `pushdown.sql` | Push a complete source SQL query at the database — replaces schema/table option building. Useful for joins, filters, derived columns, and bulk-table avoidance. |
| `catalog.id` | Reference an existing AIDP external catalog connection by id — drops host/port/user/password from the option list. Pairs with three-part `spark.table()` / `saveAsTable()` for the cleanest read/write code. |
| `manifest.path` | (REST only) Reference a manifest file by workspace/Volume path instead of HTTP URL. Lets manifests be hand-authored and version-pinned alongside the notebook. |

## How to use

### First-time setup (once per workbench workspace)

Tell Claude:

> "Set up the Oracle AI Data Platform Workbench connectors plugin in this workspace."

The `aidp-connectors-bootstrap` skill activates, uses the workbench MCP tools to upload the helper package to `/Workspace/Shared/oracle_ai_data_platform_connectors/`, then runs [`examples/00_bootstrap_helpers.ipynb`](examples/00_bootstrap_helpers.ipynb) which prints `BOOTSTRAP OK` when the package imports cleanly.

(Manual alternative: upload `scripts/oracle_ai_data_platform_connectors/` to `/Workspace/Shared/oracle_ai_data_platform_connectors/scripts/oracle_ai_data_platform_connectors/` via the workbench UI, then run the bootstrap notebook.)

### Day-to-day

In a Claude Code session against your Oracle AI Data Platform Workbench workspace, just describe what you want:

> "I need to load ATP data into Spark in my Oracle AI Data Platform Workbench notebook"

The relevant connector skill activates automatically and walks you through:

1. **Prerequisites** — env vars / OCI Vault secrets, JDBC jar runtime-load if needed.
2. **Auth options** — pick one (wallet, IAM DB-Token, API key + inline PEM, HTTP Basic, OAuth, AWS keys, Service Principal).
3. **Code** — Spark JDBC / REST / streaming snippet ready to paste into a notebook cell.
4. **Gotchas** — known constraints captured from live testing (FUSE write modes, SSL handling, runtime-load classloader, executor distribution, etc.).

### Examples

Per-connector example notebooks are under [`examples/`](examples/) — one per (skill, auth) combo. The full live-test results matrix is in [`tests/live-results/RESULTS.md`](tests/live-results/RESULTS.md).

## Sample run (Oracle EPM Cloud)

```python
import os
from oracle_ai_data_platform_connectors.auth import http_basic_session
from oracle_ai_data_platform_connectors.rest.epm import (
    list_applications, export_data_slice, slice_to_long_dataframe,
)

# EPM_USERNAME MUST be in identity-domain form: tenancy.user@domain
# (e.g. epmloaner622.first.last@oracle.com — the bare email returns 401)
session = http_basic_session(
    username=os.environ["EPM_USERNAME"],
    password=os.environ["EPM_PASSWORD"],
    base_url=os.environ["EPM_BASE_URL"],
)

# Pre-flight: confirm credentials work and the app is reachable
apps = list_applications(session, os.environ["EPM_BASE_URL"])
print("applications:", [a["name"] for a in apps])

# Export a Planning data slice (POV × columns × rows)
slice_response = export_data_slice(
    session=session,
    base_url=os.environ["EPM_BASE_URL"],
    application=os.environ["EPM_APPLICATION"],
    plan_type=os.environ["EPM_PLAN_TYPE"],
    grid_definition={
        "suppressMissingBlocks": True,
        "suppressMissingRows":   True,
        "pov": {
            "dimensions": ["HSP_View", "Year", "Scenario", "Version", "Entity", "Product"],
            "members":    [["BaseData"], ["FY26"], ["Actual"], ["Working"], ["Total Entity"], ["P_TP"]],
        },
        "columns": [{"dimensions": ["Period"],  "members": [["Jan", "Feb", "Mar", "Apr", "May", "Jun"]]}],
        "rows":    [{"dimensions": ["Account"], "members": [["IChildren(PL)"]]}],
    },
)

# Materialize as a long-format Spark DataFrame (one row per cell)
df = slice_to_long_dataframe(spark, slice_response)
df.show(10)
print("cells:", df.count())
```

The `aidp-epm-cloud` skill prints exactly this snippet (with your env vars and grid spec substituted) when you ask Claude to pull Planning data into Spark. POV members must be leaf-level — use `IChildren()` / `ILvl0Descendants()` for parents. Empty cells come back as the literal string `"#Missing"` in the `value` column; cast to numeric and filter as needed.

## Auth methods that Oracle AI Data Platform Workbench notebooks do NOT support today

- **Instance Principal** — the workbench blocks the OCI instance-metadata service; `InstancePrincipalsSecurityTokenSigner()` fails.
- **Resource Principal** — the workbench sets `AIDP_AUTH=resource_principal` but does not provide `OCI_RESOURCE_PRINCIPAL_RPST` or `OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM`.

The skills surface these as a known limitation and route users to **API Key + inline PEM** (`aidp_connectors.auth.oci_config.from_inline_pem`) instead. Background: https://github.com/oracle-samples/oracle-aidp-samples and the workbench team's notebook-auth investigation.

## Plugin development

```bash
# Validate plugin shape
claude plugin validate .

# Run unit tests (no live OCI calls)
python -m pytest tests/ -v

# Live-test a connector against the workbench
oci session authenticate --profile AIDP_SESSION --region us-ashburn-1
# Open examples/<connector>_*.ipynb in your Oracle AI Data Platform Workbench workspace and run
```

Live-validation infrastructure is documented in [`tools/live_test_driver.py`](tools/live_test_driver.py); per-row results are in [`tests/live-results/`](tests/live-results/).

## Versioning + changelog

This plugin follows [SemVer](https://semver.org/). Release notes for every version live in [`CHANGELOG.md`](CHANGELOG.md).

## License

MIT — see [`LICENSE`](LICENSE).
