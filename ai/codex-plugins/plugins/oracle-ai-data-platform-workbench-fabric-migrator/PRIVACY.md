# Privacy

**`inventory`, `plan`, `migrate` and `verify` make no network calls.** They do not
contact Microsoft Fabric, Oracle AIDP, or any telemetry endpoint. There is no
telemetry in this tool at all, on any code path.

**`publish --apply` is the one exception, and the only thing that leaves your
machine.** It uploads the notebooks and creates the AIDP jobs you asked for, by
shelling out to the separately-installed `aidp` CLI. Without `--apply` it is a dry
run: it prints what it would send and makes no network call either. Nothing else in
this tool talks to anything.

**What it reads.**

- The export directory given on the command line, or the bundled demo estate with
  `--fixture demo`.
- The `--tables-csv` file, if you pass one.
- The migrate output directory, for `verify` and `publish`.
- **A `.env` file in the current working directory, on every verb.** It is
  treated as untrusted, and only these keys are taken from it, each only when the
  variable is not already set: `AIDP_WORKSPACE_KEY`, `AIDP_CLUSTER_KEY`,
  `AIDP_INSTANCE_ID`, `OCI_NAMESPACE`, `OCI_CLI_PROFILE`, `OCI_CONFIG_PROFILE`.
  Keys that change where requests or credentials go — `AIDP_ENDPOINT`,
  `OCI_CONFIG_FILE`, `OCI_CLI_AUTH`, `OCI_CLI_REGION`, an `AIDP_SECRET_*` — are
  ignored with a warning naming the key, never its value, and an arbitrary key
  such as `PATH` or `NODE_OPTIONS`, which could run code, is ignored too. On
  Linux and macOS a `.env` that is a symbolic link, is owned by another user,
  or can be written by anyone else is not read at all; group-write is fine
  when the group is the user's alone.
- The matching environment variables — `OCI_NAMESPACE`, `AIDP_WORKSPACE_KEY`,
  `AIDP_CLUSTER_KEY`, `AIDP_INSTANCE_ID`.
- For `publish --apply` only, whatever OCI configuration the `aidp` CLI reads for
  the profile you name.

**What it writes.** Files under the output paths you specify — the manifest, the
plan, the migrate output directory and its reports. Two kinds of transient file
appear beside them and are removed: a `.<name>.<pid>.<uuid>.tmp` scratch file per
atomic write, and a `.fabric-aidp-migration-in-progress` marker written in the
migrate output directory before the first artifact and deleted once `report.json`
is committed. Nothing is written outside those paths.

**What it sends.** Nothing, until you type `--apply`. Your notebook source, SQL,
table names and shortcut targets stay on your machine through `inventory`, `plan`,
`migrate` and `verify`. `publish --apply` sends exactly the migrated artifacts
`report.json` lists as `ok` or `needs_manual_review`, and nothing else — not your
inputs, not the report.

**Credentials.** The four offline verbs require and read none: the tool works from a
Git export, so no Azure credential is involved at any point, and no Oracle
credential is needed to translate. `publish` reads an AIDP workspace, cluster and
instance key from its flags, the environment or a `.env`, and `--apply` needs an OCI
profile the `aidp` CLI can use. No credential is stored in this repository, written
to any output file, or printed.
