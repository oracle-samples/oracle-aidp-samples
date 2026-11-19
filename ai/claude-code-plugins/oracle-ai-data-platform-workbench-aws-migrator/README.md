# aws-aidp-migrator

Migration assistant for AWS → Oracle AIDP (AI Data Platform). It inventories an
AWS data stack and translates Amazon Athena SQL and AWS Glue ETL to Apache Spark on AIDP, emitting
reviewable artifacts and flagging anything it cannot convert safely. **Applying
that output to AIDP is manual today** — the tool writes files locally and does
not write to your AIDP workspace.

> **Why:** the official Databricks → AIDP plugin handles the other half of customers.
> Many AIDP migrations originate on the AWS stack — Amazon S3, Glue, Athena, Amazon EMR,
> Amazon SageMaker. `aws-aidp` covers that path.

## What it does

| AWS source | → | AIDP target |
|---|---|---|
| **S3** buckets | → | OCI Object Storage |
| **Glue** Data Catalog (databases, tables, partitions) + ETL jobs | → | AIDP catalog (schemas/tables) + AIDP Jobs |
| **Athena** workgroups + named/saved queries | → | AIDP saved queries (Spark SQL dialect) |
| **EMR** clusters + notebooks + steps | → | AIDP Spark clusters + AIDP notebooks |
| **SageMaker** notebooks + training jobs + models + pipelines | → | AIDP MLOps |

Four verbs, no plug-in zoo:

```
aws-aidp inventory --region us-east-1   # read-only AWS scan → manifest
aws-aidp plan      inv.json             # manifest → mapping plan
aws-aidp migrate   plan.json --demo     # execute; --demo runs offline
aws-aidp verify    migrated/            # classify outcome: PASS / REVIEW / SKIP / FAIL
```

## Quick start (offline demo, no AWS creds needed)

```bash
git clone https://github.com/oracle-samples/oracle-aidp-samples.git
cd oracle-aidp-samples/ai/claude-code-plugins/oracle-ai-data-platform-workbench-aws-migrator
pip install -e .
./demo.sh                               # 5-minute end-to-end demo
```

The demo uses `aws_aidp/fixtures/demo-manifest.json` — a hand-crafted Acme Insurance estate
(5 S3 buckets, 50 Glue tables, 20 Athena queries with realistic dialect gaps,
2 EMR clusters, 3 SageMaker notebooks). No AWS account required.

## Use it as a Claude Code plugin

The repo doubles as a Claude Code plugin (skill + slash commands wrapping the CLI):

**Via Anthropic's community marketplace** (recommended):

```bash
# in Claude Code
/plugin marketplace add anthropics/claude-plugins-community
/plugin install oracle-ai-data-platform-workbench-aws-migrator
```

> Published from this canonical `oracle-samples` location. Anthropic's
> community-marketplace bot picks up new oracle-samples plugins on a weekly
> cadence, so this becomes effective roughly a week after merge.

**From a local clone** (before that, or to run un-merged commits):

```bash
# in Claude Code, from the clone above
/plugin marketplace add ./oracle-aidp-samples/ai/claude-code-plugins/oracle-ai-data-platform-workbench-aws-migrator
/plugin install oracle-ai-data-platform-workbench-aws-migrator@aidp-aws-migrator
```

Then drive it with `/oracle-ai-data-platform-workbench-aws-migrator:inventory`, `:plan`,
`:migrate`, `:verify`, or
just ask in natural language — the `aws-aidp-migrator` skill routes the workflow.
The `aws-aidp` CLI must be pip-installed (`pip install -e .`) so the plugin can call it.

> **Codex / Cursor / any MCP client:** the same four verbs are exposed as an
> **MCP server** (`aws-aidp-mcp`), so any MCP client can drive them. Run
> `pip install -e '.[mcp]'` (Python 3.10+) before first use — without it the
> server exits and the client reports it as failed in `/mcp`, which you will
> see before you see the explanatory message. A Codex-packaged variant of this
> plugin is proposed separately in #109; it is not in this repository's Codex
> marketplace yet.

## Testing

Contributor + intern testing guide: [`TESTING.md`](TESTING.md).

Run the dependency-free offline stress suite:

```bash
PYTHONPATH=. python3 scripts/run_stress_tests.py
PYTHONPATH=. python3 scripts/run_stress_tests.py --release
```

The release mode adds 10,000-asset planning and 1,000-artifact migration tests.
Spark parser/runtime and MCP parity checks run in CI when their optional
dependencies are available.

## Compatibility contract

- The CLI supports Python 3.9–3.14. The optional MCP server supports Python
  3.10+ and is intentionally pinned to `mcp>=1.2,<2` because MCP SDK 2.x removed
  the `mcp.server.fastmcp` interface used by this server.
- Generated workloads target the current
  [AIDP runtime contract](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/manage-compute.html):
  Spark 3.5, Python 3.11, and Java 17. Spark runtime checks can be run with
  `AIDP_SPARK_VERSION=3.5 python -m unittest -v tests.stress.test_spark_runtime`.
- Live job-run calls default to the
  [AIDP REST API `20260430`](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aiwap/op-aidataplatforms-aidataplatformid-workspaces-workspacekey-jobruns-post.html);
  its collection root is
  `https://datalake.<region>.oci.oraclecloud.com/20260430/aiDataPlatforms`.
  `AIDP_API_VERSION` accepts only versions with a known host/path pairing
  (currently `20260430`). For a legacy, sovereign, private, or future route,
  set `AIDP_ENDPOINT` to the complete versioned collection root, such as
  `https://host/20240831/dataLakes`; do not set both variables. URL construction
  is tested offline, but a tenancy should validate it with an authenticated
  `GET <workspace-base>/jobs` before enabling live calls.

## Real AWS (live inventory)

```bash
cp .env.example .env                    # fill AWS_PROFILE, AWS_REGION, OCI_NAMESPACE
pip install -e '.[oci]'                 # add the OCI client (not yet used by any verb)
aws-aidp inventory --region us-east-1 -o inv.json
aws-aidp plan inv.json -o plan.json
aws-aidp migrate plan.json --filter athena --demo -o ./migrated
aws-aidp verify ./migrated --filter athena
```

AWS credentials: standard boto3 chain (`AWS_PROFILE`, `~/.aws/credentials`, IAM role).

## Status

| Verb | What works | Notes |
|---|---|---|
| `inventory` | ✅ all 5 sources | Real AWS via boto3 + fixture mode; Glue job scripts fetched from S3 |
| `plan` | ✅ all 5 source types | 1:1 mapping to AIDP target types |
| `migrate` | ✅ Athena → Spark SQL · ✅ Glue ETL → Spark/PySpark | Writes translated artifacts locally; **nothing is applied to AIDP**. Safe rewrites plus explicit review gates; EMR / SageMaker translators planned |
| `verify` | ✅ classifies migrate outcomes | Per-asset PASS / REVIEW / SKIP / FAIL |

> **What PASS means.** PASS = *translated, and no known issue was detected*. It is
> **not execution-verified**: `verify` does not parse or run the generated artifacts,
> so a construct none of the rules cover is reported clean. Treat PASS as "nothing
> the tool knows about is wrong here", and review artifacts before running them in
> production. REVIEW is the honest signal that something needs a human — a low
> REVIEW count is not by itself evidence of a clean migration.

**Translator coverage today** (Athena → Spark SQL):

| Athena | → | Spark SQL | Type |
|---|---|---|---|
| `date_format(x, '%Y-%m-%d')` | → | `date_format(x, 'yyyy-MM-dd')` | rewrite |
| `CROSS JOIN UNNEST(arr) AS t(x)` | → | `LATERAL VIEW explode(arr) t AS x` | rewrite |
| `array_agg(x)` | → | unchanged | flag: Spark drops NULL elements |
| `APPROX_DISTINCT(x)` | → | `approx_count_distinct(x)` | rewrite |
| `JSON_EXTRACT_SCALAR(j, ...)` | → | `get_json_object(j, ...)` | rewrite |
| `JSON_EXTRACT(j, ...)` | → | — | flag: result-type semantics differ |
| `cardinality(arr)` | → | unchanged | flag: NULL behavior depends on Spark configuration |
| `zip(a, b)` | → | `arrays_zip(a, b)` | rewrite |
| `split(x, regex-sensitive-delimiter)` | → | unchanged | flag: Athena uses a literal delimiter; Spark uses a regex pattern |
| `split(x, delimiter)[n]` | → | unchanged | flag: Athena subscripts are one-based; Spark bracket subscripts are zero-based |
| two-argument `regexp_extract(x, pattern)` | → | unchanged | flag: Athena returns the whole match; Spark defaults to capture group 1 |
| `CAST(x AS VARCHAR[(n)])` | → | unchanged | flag: review and change to a Spark 3.5 string type |
| `strpos(string, substring)` | → | `instr(string, substring)` | rewrite |
| `"identifier"` (Trino double quotes) | → | `` `identifier` `` | rewrite: Spark would read `"..."` as a string literal |
| `contains(array, x)` | → | `array_contains(array, x)` | rewrite: Spark's `contains` is a string function |
| `levenshtein_distance(a, b)` | → | `levenshtein(a, b)` | rewrite |
| regex literal containing `\` (`regexp_replace`, `regexp_extract`, …) | → | unchanged | flag: Spark's default parser consumes backslash escapes before the regex engine |
| `from_unixtime` / `to_unixtime` / `from_iso8601_date` / `day_of_week` | → | unchanged | flag: return type or week-start semantics differ |
| `TRY(...)` / `url_extract_*` / `json_parse` | → | — | flag: no direct Spark equivalent |
| `histogram(...)` | → | — | flag for manual review |
| `UNNEST(...) WITH ORDINALITY` | → | — | flag for manual review |
| ordered/distinct `array_agg(...)` | → | — | flag for manual review |
| Athena `date_diff(unit, ...)` | → | — | flag for manual review |

**Deterministic and honest:** the translator is deterministic (no LLM in the
translation path). It rewrites only where Spark semantics match Athena's;
anything with a semantic gap (NULL behavior, literal-vs-regex `split`, one-based
subscripts, …) or no safe equivalent is **flagged and left in place** for
review — never a silent, possibly-wrong rewrite.

**Translator coverage** (Glue ETL PySpark → Spark, v0.2):

| Glue construct | → | Spark | Type |
|---|---|---|---|
| `from awsglue.* import ...` | → | commented out | rewrite |
| `GlueContext(sc)` / `SparkContext()` / `.spark_session` | → | `SparkSession` / `spark` | rewrite |
| `job = Job(ctx)` / `job.init()` / `job.commit()` | → | removed (no AIDP lifecycle) | rewrite |
| `getResolvedOptions(sys.argv, [...])` | → | required [`oidlUtils.parameters.getParameter(..., None)`](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/parameters1.html) values | rewrite |
| `create_dynamic_frame.from_catalog(db, table)` | → | ``spark.table("`db`.`table`")`` | rewrite with safe per-part quoting |
| `create_dynamic_frame.from_options(s3, format)` | → | `spark.read.format(...).load(...)` | rewrite |
| `write_dynamic_frame.from_options(frame, s3)` | → | `df.write.format(...).save(...)` | rewrite |
| `DynamicFrame.fromDF(df, ...)` / `.toDF()` | → | collapse to `df` | rewrite |
| any `s3://…` literal | → | `oci://bucket@namespace/…` | rewrite |
| `create_dynamic_frame_from_catalog(...)` / `create_dynamic_frame_from_options(...)` / `write_dynamic_frame_from_options(...)` (method form) | → | same as the attribute form | rewrite |
| `create_dynamic_frame_from_rdd` / `write_dynamic_frame_from_catalog` / `write_dynamic_frame_from_jdbc_conf` | → | — | flag: Glue-only API left in place |
| `ApplyMapping` / `ResolveChoice` / `Relationalize` / `Join` / `Filter` / method-style equivalents | → | — | flag for manual review |
| DynamicFrame-only methods (`apply_mapping`, `resolveChoice`, `drop_fields`, …) on helper parameters, helper results, dict/attribute-held frames | → | — | flag: no DataFrame counterpart, so no lineage proof is required |
| `transformation_ctx` on a job that writes | → | — | flag: define AIDP watermark/checkpoint/dedup/replacement policy |
| residual `s3://` / `s3a://`, including f-strings (docstrings excluded) | → | — | flag: unsafe AWS path remains |

DynamicFrame-only transforms (ApplyMapping, ResolveChoice, Relationalize, Join, …)
have no clean 1:1 in Spark — they're **flagged and left in place**, never silently
dropped. A job reaches PASS only when no unhandled transform, bookmark risk, or
residual AWS path remains — including transforms reached through helper
functions, containers, or method-form GlueContext calls.

## Layout

```
aws_aidp/
  cli.py               # argparse, 4 verbs
  _env.py              # tiny .env loader
  aws_client/          # boto3 wrapper (lazy import)
  aidp_client/         # AIDP REST (OCI request signing)
  run/                 # POST /jobRuns + repair loop (for live migrations)
  state/               # resumable on-disk state
  inventory/           # one module per AWS source — read-only scans
  plan/                # manifest → mapping plan
  migrate/             # executes a plan; --demo writes artifacts offline
  verify/              # PASS/REVIEW/SKIP/FAIL classification
  translate/           # source-side dialect translators
  fixtures/            # bundled demo manifest + builder
demo.sh                # end-to-end demo script
```

## Roadmap

- v0.2 — ✅ Glue ETL script translation (DynamicFrame → DataFrame, GlueContext → SparkSession)
- v0.3 — EMR PySpark notebook translation + AIDP cluster auto-sizing
- v0.4 — SageMaker training-job → AIDP MLOps experiment mapping
- v0.5 — live AIDP write-side end-to-end (see Status: demo/offline only today)
