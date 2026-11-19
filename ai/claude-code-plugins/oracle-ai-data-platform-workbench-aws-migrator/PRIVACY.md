# Privacy

The **Oracle AIDP Migrator for AWS** runs locally and is designed to keep your data
in your own environment.

## What it does with data
- **Inventory** performs *read-only* AWS API calls (Glue, Athena, S3, EMR,
  SageMaker) using your own AWS credentials. It reads resource metadata (names,
  schemas, saved-query SQL, Glue job scripts) to build a local manifest.
- **Plan / migrate / verify** run entirely on your machine and write local
  artifacts (a manifest, a plan, translated SQL/PySpark, and a report). Migration
  translation is deterministic and performs **no network calls**.
- The optional **MCP server** exposes the same four verbs to a local MCP client;
  it spawns the local CLI and returns its output. No data is sent to Oracle,
  Anthropic, OpenAI, or any third party by the migrator itself.

## What it does NOT do
- It does not transmit your source, schemas, or credentials to any external
  service. The generated S3→OCI transfer is an `rclone` **script** you review and
  run yourself; nothing moves until you do.

## Your responsibility
- Live AWS/OCI calls use credentials you supply (standard AWS/OCI auth chains).
- If you separately run an AI-assisted rewrite step against your own AIDP model,
  the flagged snippet is sent to *that model in your own tenancy* — review your
  tenancy's data-handling before enabling it.

Questions or concerns: open a GitHub issue on the repository.
