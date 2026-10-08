# `aws-aidp` verb reference

All verbs also work as `python3 -m aws_aidp.cli <verb>` if the console script isn't
on PATH.

## inventory — read-only AWS scan → manifest
```
aws-aidp inventory [--region R] [--fixture demo] [--sources s3,glue,athena,emr,sagemaker] [-o inv.json]
```
- `--region` — AWS region to scan (default: `$AWS_REGION` or `us-east-1`).
- `--fixture demo` — use the bundled Acme Insurance manifest; no AWS account needed.
- `--sources` — comma-separated subset (default: all five).
- Auth: standard boto3 chain (`AWS_PROFILE`, `~/.aws/credentials`, IAM role).

## plan — manifest → AIDP mapping plan
```
aws-aidp plan <inv.json> [-o plan.json] [--namespace <oci-namespace>]
```
- `--namespace` — OCI namespace for target buckets (default: `$OCI_NAMESPACE`).
- Emits target types: oci_bucket, aidp_schema, aidp_dcat_external_table,
  aidp_saved_query, aidp_spark_cluster, aidp_mlops_registered_model.

## migrate — translate a plan
```
aws-aidp migrate <plan.json> [--demo] [--filter s3|glue|athena|emr|sagemaker] [-o migrated]
```
- `--demo` — offline: write artifacts + `report.md`/`report.json`, no AIDP calls.
  (Use this by default; the live write-side is roadmap v0.5.)
- `--filter` — translate only one slice.
- Writes `migrated/report.md` (human diff view) and `report.json`.

## verify — classify outcomes
```
aws-aidp verify <migrated/ | report.json> [--filter athena]
```
- PASS = translated, no known issue detected · REVIEW = has manual-review flags ·
  SKIP = translator not implemented yet · FAIL = translation error.
- PASS is **not execution-verified**: nothing parses or runs the artifact, so a
  construct no rule covers is reported clean. Review artifacts before running them.
- Exit code is non-zero only when FAIL > 0.
