---
description: Classify a migration's output as PASS / REVIEW / SKIP / FAIL
argument-hint: "<migrated/ or report.json> [--filter s3|glue|athena|emr|sagemaker]"
allowed-tools: Bash(aws-aidp verify:*), Bash(python3 -m aws_aidp.cli verify:*)
---

Classify the outcome of a migration:

`aws-aidp verify $ARGUMENTS`

After it runs, report the PASS / REVIEW / SKIP / FAIL counts and the per-asset list.
Explain what each means: PASS = translated with no known issue detected; REVIEW =
translated but has flags a human must check; SKIP = translator not built yet
(EMR/SageMaker are on the roadmap); FAIL = translation error. A non-zero FAIL count is
the only hard failure.

Say plainly that PASS is **not execution-verified** — nothing parses or runs the
artifact, so a construct no rule covers is reported clean. Do not describe a PASS
asset as "runnable" or "ready"; recommend reviewing artifacts before running them.
