---
name: aws-aidp-migrator
description: "Migrate an AWS data stack (S3, Glue, Athena, EMR, SageMaker) to Oracle AI Data Platform (AIDP). Use when the user wants to inventory an AWS data estate, plan a migration to AIDP, translate Athena SQL or Glue ETL/PySpark to Spark on AIDP, or verify a migration's output. Wraps the `aws-aidp` CLI, with the verbs inventory, plan, migrate and verify."
---

# Oracle AIDP migrator for AWS

This skill drives the `aws-aidp` CLI, which migrates an AWS data stack to Oracle
AIDP in four verbs. The translators are **deterministic-first**: each rewrite is a
named rule with a reason, and anything that can't be safely rewritten is **flagged
and left in place**, never silently changed. Preserve that principle — never
hand-edit a flagged construct into a silent rewrite.

## Prerequisite

The CLI must be installed:

```bash
pip install -e .                          # from the plugin directory
pip install -e "${CLAUDE_PLUGIN_ROOT}"    # or, when installed as a plugin
```

There is deliberately no PyPI fallback here: `aws-aidp-migrator` is not a
published package, and an unregistered name in instructions an agent executes
is a name-squatting risk rather than a convenience.

Live AWS mode uses the standard boto3 auth chain (`AWS_PROFILE`, `~/.aws/credentials`,
IAM role). Offline/demo mode needs no AWS account.

## The four verbs (run in order)

```bash
aws-aidp inventory --region us-east-1 -o inv.json   # read-only AWS scan → manifest
aws-aidp plan      inv.json           -o plan.json   # manifest → AIDP mapping plan
aws-aidp migrate   plan.json --demo   -o migrated    # translate; --demo = offline artifacts
aws-aidp verify    migrated                          # classify: PASS / REVIEW / SKIP / FAIL
```

Fixture mode (no AWS): `aws-aidp inventory --fixture demo -o inv.json`.

## How to help the user

1. **Confirm scope first** — which AWS sources (s3, glue, athena, emr, sagemaker) and
   which region. Use `--sources` / `--filter` to narrow when they only want a slice.
2. **Always inventory → plan → migrate → verify in that order.** Each verb consumes the
   previous one's output file.
3. **Default to `--demo`** for `migrate` unless the user explicitly asks for a live AIDP
   write — the live write-side is not GA yet (roadmap v0.5).
4. **Read the reports.** After `migrate`, open `migrated/report.md` and surface the
   diffs and any `flag` findings — those need a human. After `verify`, report the
   PASS/REVIEW/SKIP/FAIL counts and explain SKIP = translator not built yet
   (EMR/SageMaker are stubs).
5. **Translating a single script/query?** You can call the translators directly:
   `python3 -c "from aws_aidp.translate.athena_to_spark_sql import translate; ..."`
   or `aws_aidp.translate.glue_to_spark`.

## Coverage today (be honest about it)

- ✅ Athena SQL → Spark SQL, and Glue ETL → PySpark — translated to reviewable
  artifacts on disk.
- ✅ S3 buckets → a generated `rclone` transfer job. Review it before running;
  the tool never moves data itself.
- 🚧 Glue **Data Catalog** (databases, tables) — inventoried and planned, but no
  DDL is emitted yet, so `verify` reports every catalog asset as SKIP.
- 🚧 EMR, SageMaker translators — stubs (roadmap). `verify` reports them as SKIP.
- 🚧 Live AIDP write-side — nothing is written to AIDP. Applying the artifacts is
  manual today.

See `references/verbs.md` for flags, and the plugin README
(`${CLAUDE_PLUGIN_ROOT}/README.md`) for the full rule tables.
