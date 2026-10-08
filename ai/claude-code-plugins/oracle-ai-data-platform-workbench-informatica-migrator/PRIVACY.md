# Privacy

## What this plugin sends off your machine

- **To Anthropic:** Informatica mapping metadata (transformation names, expressions,
  field names, source/target table and column names) and generated PySpark code, as
  part of LLM-assisted notebook generation. Set by `ANTHROPIC_API_KEY`.
- **To Oracle Cloud Infrastructure:** notebook content, SQL, and DDL, sent to your own
  AIDP DataLake and cluster under your own OCI credentials.

## What it does not send

- Row-level data from your source databases. The reconciler computes counts and
  checksums, and transmits only aggregates.
- Credentials from Informatica connections. These are translated into AIDP
  external-catalog registrations; secret values are read at migration time and are
  not written into generated notebooks or logs.

## What is written locally

- `reports/` — manifests, analysis output, and `JOB_REPORT.md` run records.
- Generated notebooks under your chosen output directory.

Review generated artifacts before sharing them; mapping metadata can itself be
commercially sensitive.
