# Oracle AI Data Platform Workbench — Spark Connectors

A Claude Code plugin that ships **18 model-invokable skills** for connecting Oracle AI Data Platform Workbench Spark notebooks to every data source these notebooks commonly need. Each skill produces plain Python (Spark JDBC, Spark structured streaming, Spark `oci://`/`s3a://`/`abfss://`, or REST → Spark DataFrame) that runs in the notebook without any additional runtime.

**Live-validated** on the workbench `tpcds` cluster (Spark 3.5.0): **17 PASS / 4 ship-as-is** out of 21 test rows. See [`tests/live-results/RESULTS.md`](tests/live-results/RESULTS.md).

## Install

From Anthropic's community plugin marketplace (recommended):

```
/plugin marketplace add anthropics/claude-plugins-community
/plugin install oracle-ai-data-platform-workbench-spark-connectors
```

This is the canonical home. The plugin lives at
[`oracle-samples/oracle-aidp-samples/ai/claude-code-plugins/oracle-ai-data-platform-workbench-spark-connectors`](https://github.com/oracle-samples/oracle-aidp-samples/tree/main/ai/claude-code-plugins/oracle-ai-data-platform-workbench-spark-connectors)
and the community marketplace points at this subdirectory via `git-subdir` source.

## What's in here

20 skills total (18 connectors + 1 bootstrap + 1 routing).

### Oracle / OCI sources
| Skill | Target | Transport | Recommended auth |
|---|---|---|---|
| `aidp-connectors-overview` | (router) | — | — |
| `aidp-connectors-bootstrap` | one-time setup | — | — |
| `aidp-alh` | Oracle DB family (Autonomous: ALH/ADW/ATP + non-Autonomous: Compute/Base DB/on-prem) | Spark JDBC | Wallet (mTLS) for Autonomous, plain user/password for non-Autonomous |
| `aidp-exacs` | Exadata Cloud Service | Spark JDBC (TCP 1521 + NNE AES256) | Plain user/password |
| `aidp-fusion-rest` | Fusion ERP/HCM/SCM | REST → DataFrame | HTTP Basic |
| `aidp-fusion-bicc` | Fusion BICC bulk extracts | `aidataplatform` (`type=FUSION_BICC`) | HTTP Basic |
| `aidp-epm-cloud` | EPM Cloud Planning | REST → DataFrame | HTTP Basic (`tenancy.user@domain`) |
| `aidp-essbase` | Essbase 21c | REST + MDX → DataFrame | HTTP Basic |
| `aidp-streaming-kafka` | OCI Streaming | Spark structured streaming | SASL/PLAIN with OCI auth token |
| `aidp-object-storage` | OCI Object Storage native | Spark `oci://` | Implicit IAM (workspace identity) |
| `aidp-iceberg` | Apache Iceberg on OCI Object Storage | Iceberg Hadoop catalog on `oci://` | Implicit IAM |

### External RDBMS (`aidataplatform` format)
| Skill | Target | Transport | Recommended auth |
|---|---|---|---|
| `aidp-postgresql` | PostgreSQL | Spark JDBC (runtime-loaded driver) for SSL targets, `aidataplatform` `type=POSTGRESQL` for non-SSL | Plain user/password |
| `aidp-mysql` | MySQL / OCI MySQL HeatWave | `aidataplatform` (`type=MYSQL` or `MYSQL_HEATWAVE`) | Plain user/password |
| `aidp-sqlserver` | Microsoft SQL Server / Azure SQL DB | `aidataplatform` (`type=SQLSERVER`) | Plain user/password |

### Multi-cloud + escape hatches
| Skill | Target | Transport | Recommended auth |
|---|---|---|---|
| `aidp-snowflake` | Snowflake | `format("snowflake")` (Snowflake Spark connector) | sfUser/sfPassword |
| `aidp-azure-adls` | Azure ADLS Gen2 | Spark `abfss://` | OAuth client-credentials (Service Principal) |
| `aidp-aws-s3` | AWS S3 | Spark `s3a://` (runtime-loaded `hadoop-aws` + `aws-java-sdk-bundle`) | AWS access keys |
| `aidp-rest-generic` | Any REST API with a manifest | `aidataplatform` (`type=GENERIC_REST`) | HTTP Basic |
| `aidp-jdbc-custom` | Any DB with a JDBC driver | Spark `format("jdbc")` (runtime-loaded driver) | Driver-specific |
| `aidp-excel` | `.xlsx` files in Volumes / Object Storage | stdlib `zipfile` + XML parser (no extra deps) | None (file-based) |

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
