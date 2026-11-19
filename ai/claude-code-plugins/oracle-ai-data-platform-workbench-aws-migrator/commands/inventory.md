---
description: Scan an AWS data stack (read-only) and write an AIDP migration manifest
argument-hint: "[--region us-east-1 | --fixture demo] [--sources s3,glue,athena]"
allowed-tools: Bash(aws-aidp inventory:*), Bash(python3 -m aws_aidp.cli inventory:*)
---

Run a read-only AWS inventory and write a manifest. Pass through the user's arguments:

`aws-aidp inventory $ARGUMENTS`

If no output path is given, add `-o inv.json`. If the user has no AWS credentials,
use `--fixture demo` instead of `--region`. After it runs, summarize the per-source
counts (s3 / glue / athena / emr / sagemaker) and tell the user the manifest path so
they can run `/oracle-ai-data-platform-workbench-aws-migrator:plan` next.
