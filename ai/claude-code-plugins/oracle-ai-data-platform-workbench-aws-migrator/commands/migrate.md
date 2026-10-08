---
description: Translate a migration plan to AIDP artifacts (Athena→Spark SQL, Glue→PySpark)
argument-hint: "<plan.json> [--demo] [--filter s3|glue|athena|emr|sagemaker] [-o migrated]"
allowed-tools: Bash(aws-aidp migrate:*), Bash(python3 -m aws_aidp.cli migrate:*), Read
---

Execute a migration plan:

`aws-aidp migrate $ARGUMENTS`

Default to `--demo` (offline artifacts) unless the user explicitly asked for a live
AIDP write — the live write-side is not GA yet. Default output to `-o migrated`.

After it runs, Read `migrated/report.md` and surface: the per-asset diffs, the
`ok / needs_review` counts, and **every `flag` finding** — flagged constructs are
deliberately left in place and need a human. Never rewrite a flagged construct
silently. Then suggest `/oracle-ai-data-platform-workbench-aws-migrator:verify`.
