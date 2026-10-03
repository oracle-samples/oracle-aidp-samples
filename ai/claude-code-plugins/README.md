# Claude Code plugins

This directory hosts Claude Code plugins published by the Oracle AI Data Platform team.

Each subdirectory is a self-contained plugin (with its own `.claude-plugin/plugin.json`, skills, helpers, examples, and tests). Plugins are referenced from Anthropic's community Claude Code plugin marketplace ([`anthropics/claude-plugins-community`](https://github.com/anthropics/claude-plugins-community)) via a `git-subdir` source pointing at the plugin's directory in this repo.

## Plugins

| Plugin | What it does |
|---|---|
| [`ask-aidp`](ask-aidp/) | Operates Oracle AI Data Platform Workbench from Claude Code through 43 MCP tools covering 256 documented `aidp-cli` commands, 271 OCI-signed REST operations, native SDK workspace and Git actions, Data Lineage, bundle publishing, Compute configuration management, agents, AI Compute, notebooks, workflows, catalogs, schemas, tables, bundles, run tracking, and log collection. |
| [`oracle-ai-data-platform-workbench-engineer-agent`](oracle-ai-data-platform-workbench-engineer-agent/) | A 37-skill natural-language agent that operates the **entire** AIDP Workbench — catalog discovery, Spark-SQL + full Delta DDL/DML, ingestion, profiling/quality, pipelines, clusters, Spark-UI debugging, governance (roles/credentials/Delta Sharing/MLOps/audit), and AI (Agent Flows + guardrails, Knowledge Base RAG, high-code LangGraph agents). Signature: LLM-in-SQL via `ai_generate()` + cross-source federation in one Spark session. Runs via the official `aidp` CLI / `oci raw-request` (api_key **or** session-token auth). |
| [`oracle-ai-data-platform-workbench-spark-connectors`](oracle-ai-data-platform-workbench-spark-connectors/) | 28 model-invokable skills (26 connectors + bootstrap + routing) connecting Oracle AI Data Platform Workbench Spark notebooks to Oracle (ALH/ADW/ATP, ExaCS, Fusion ERP, BICC, EPM Cloud, Essbase) and external (PostgreSQL, MySQL/HeatWave, SQL Server, Azure SQL, IBM DB2, Snowflake, Azure ADLS Gen2, AWS S3, OCI Streaming, Object Storage, Iceberg, generic REST/JDBC, Excel) data sources. |
| [`oracle-ai-data-platform-workbench-databricks-migrator`](oracle-ai-data-platform-workbench-databricks-migrator/) | 10 skills + 4 commands + 2 agents + 5 references that drive the AIDP Databricks Migration Toolkit end-to-end: notebooks, jobs, schedules, and Unity Catalog / HMS DDL. Pass-1 dependency resolution + Pass-2 cell-by-cell execute/verify/fix on a live AIDP cluster via Claude with tool use. Covers catalog DDL rewriter (18 rules, source-format preserved), `s3://`→`oci://` bucket-map, write-redirect sandbox schema for data safety, pre-migration data-availability scan, `fixup_cell` rewind, and the consecutive-zero-window acceptance contract for batch / streaming convergence. |
| [`oracle-ai-data-platform-fusion-autopilot`](oracle-ai-data-platform-fusion-autopilot/) **(alpha)** | Productized Fusion → AIDP pipeline. Curated BICC extracts for Fusion ERP/HCM/SCM, bronze/silver/gold medallion in Delta, conformed COA/calendar/org/supplier/item dimensions, ready-made AR-aging / AP-aging / GL-balance / PO-backlog / Supplier-spend gold marts, and **MCP-native Oracle Analytics Cloud (OAC) workbook authoring**. A conversational skill family (config → bootstrap → seed/incremental refresh → OAC dataset advisor → workbook authoring) wraps a guarded CLI with fail-closed destructive-seed and drift gates. Productizes Option 1 of the Oracle BICC-into-AIDP blog; additive to and complementary with Oracle FDI/OAC/OTBI/BIP. |
| [`oracle-ai-data-platform-workbench-aws-migrator`](oracle-ai-data-platform-workbench-aws-migrator/) | Migrate an AWS data stack (S3, Glue Data Catalog, Athena saved queries, Glue ETL) to Oracle AI Data Platform via four verbs — inventory, plan, migrate, verify. Deterministic-first translation (Athena→Spark SQL, Glue→PySpark) with explicit review gates and a generated `rclone` S3→OCI transfer job; anything unsafe is flagged, never guessed. |
| [`oracle-ai-data-platform-workbench-snowflake-migrator`](oracle-ai-data-platform-workbench-snowflake-migrator/) | Migrate a Snowflake database to Oracle AI Data Platform through a fixed twelve-step runbook — in-AIDP discovery through the Snowflake connector, a reviewed type-and-name translation plan, target schemas and empty Delta tables created on AIDP compute, and one verified copy job per schema (INSERT-SELECT, row counts, optional decimal sums) for the customer to run. Read-only against Snowflake and dry-run by default against AIDP; teardown releases the compute or removes everything the migration created. |
| [`oracle-ai-data-platform-workbench-fabric-migrator`](oracle-ai-data-platform-workbench-fabric-migrator/) | Migrate a Microsoft Fabric workspace (notebooks, Warehouse T-SQL, Dataflow Gen2 / Power Query M, Data Pipelines, Lakehouse shortcuts, OneLake paths) to Oracle AI Data Platform from its Git export via five verbs — inventory, plan, migrate, verify, publish. Deterministic translation with named rules; anything unsafe is flagged or refused, never guessed. `publish` is a dry run until `--apply` and never overwrites. |
| [`oracle-ai-data-platform-workbench-informatica-migrator`](oracle-ai-data-platform-workbench-informatica-migrator/) | Migrate Informatica ETL (IDMC/IICS cloud mapping JSON, PowerCenter 10.x repository XML) to Oracle AI Data Platform: mappings become PySpark notebooks, workflows become AIDP jobs carrying the session dependency graph, Quartz schedules and a declarable timezone, and worklets are expanded. A deterministic transformation compiler handles the transformation and expression layers, with an optional LLM-assisted path for the long tail; catalog-addressed reads and writes keep credentials and JDBC URLs out of generated notebooks. Pluggable Delta and ADW write strategies, target DDL generation, reconciliation against the source database, and a source-fidelity check that compares output against the raw export rather than the parser's own model. 8.x and 9.x exports are refused by a version gate rather than mis-migrated; anything that cannot convert safely is reported with a named reason and a rebuild path, never guessed. |

## Installing

```
/plugin marketplace add anthropics/claude-plugins-community
/plugin install <plugin-name>
```

## Authoring a new plugin

Follow [Anthropic's plugin reference](https://code.claude.com/docs/en/plugins-reference). Each plugin must have:
- `.claude-plugin/plugin.json` at the plugin root (sibling-to-this-README level + 1)
- A `README.md`, `LICENSE` (MIT preferred for samples), and `CHANGELOG.md`
- Action-oriented `description:` frontmatter on every `SKILL.md` so Claude Code's skill discovery fires correctly
- Optional but encouraged: `examples/`, `tests/` (unit tests for any helpers), and a live-test results matrix

Once the plugin is merged here, request listing in the community marketplace via the [Claude Code plugin directory submission form](https://clau.de/plugin-directory-submission). Reference this directory using `git-subdir` source pattern.
