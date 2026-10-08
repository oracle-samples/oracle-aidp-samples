---
description: Turn an inventory manifest into an AIDP mapping plan
argument-hint: <inv.json> [-o plan.json] [--namespace <oci-namespace>]
allowed-tools: Bash(aws-aidp plan:*), Bash(python3 -m aws_aidp.cli plan:*)
---

Build a migration plan from an inventory manifest:

`aws-aidp plan $ARGUMENTS`

Default the output to `-o plan.json` if not specified. After it runs, report the
by-target-type breakdown (oci_bucket, aidp_schema, aidp_dcat_external_table,
aidp_saved_query, aidp_spark_cluster, aidp_mlops_registered_model) and point the user
to `/oracle-ai-data-platform-workbench-aws-migrator:migrate`.
