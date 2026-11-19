---
name: aidp-fusion-config
description: Generate aidp.config.yaml from human-friendly AIDP names instead of hand-copied OCIDs. Use when the user is setting up the fusion-autopilot and doesn't want to dig workspaceKey / clusterKey / aiDataPlatformId out of console URLs — they give a workspace name + cluster name (+ region) and this resolves the keys via the AIDP REST API and writes the env block. Triggers — "set up aidp.config.yaml", "configure the fusion autopilot connection", "I don't have the OCIDs", "fill in workspace/cluster keys", "what do I put for workspaceKey/clusterKey", "configure AIDP coords by name".
allowed-tools: Read, Bash, Glob, Grep
---

# `aidp-fusion-config` — fill `aidp.config.yaml` from names, not OCIDs

The worst part of fusion-autopilot setup is `aidp.config.yaml`: it wants three opaque identifiers —
`aiDataPlatformId` (a long OCID), `workspaceKey`, and `clusterKey` (UUIDs). Customers should not hunt those
out of console URLs and REST responses by hand. This skill collects **human-friendly names** and resolves the
keys live. This is a **control-plane** skill — no MCP, no notebook session required.

## When to use
- "Set up aidp.config.yaml", "configure the fusion autopilot connection", "what do I put for
  workspaceKey/clusterKey", "I don't have the OCIDs", "fill in the AIDP coords by name".
- The user ran `init`, sees `aidp.config.yaml` full of `*-PLACEHOLDER` values, and asks what goes there.
- A precondition check (e.g. the `aidp-fusion-seed` ladder) reports the config is missing or has placeholders.

## When NOT to use
- Authoring `bundle.yaml`'s `fusion:` connectivity / credentials — customer-supplied policy the CLI can't
  discover (use `init` + hand-edit). This skill only fills the **coordinates** in `aidp.config.yaml`.
- Resolving tenant data variation (column aliases, semantic variants) — that's `bootstrap` / `medallion-author`.

## Engine — the self-contained `aidp-fusion-autopilot init-config` command
The skill shells out to one CLI command; it never re-implements OCI signing or REST calls. The command reuses
the plugin's own `AidpRestClient` discovery primitives (`find_workspace_by_name` / `find_cluster_by_name`,
OCI-signed) to turn names into keys, validates against the `AidpConfig` schema, then writes the env block.

| Op | What you give | What it resolves / writes |
|---|---|---|
| Resolve workspace | `--workspace "<display name>"` | → `workspaceKey` |
| Resolve cluster | `--cluster "<display name>"` | → `clusterKey` + live `state` (warns if not ACTIVE) |
| Anchor | `--aidp-id <DATALAKE_OCID>` | the one root id (copied once from the console URL) |
| Target env | top-level `--env <name>` (default `dev`) | writes `environments.<name>`, preserves siblings |
| Preview | `--dry-run` | prints resolved keys + YAML, writes nothing |

```bash
# 1. Dry-run first — resolve names → keys and show the YAML before writing anything
aidp-fusion-autopilot init-config \
  --aidp-id ocid1.datalake.oc1.iad.<...> \
  --workspace "My Workspace" \
  --cluster   "My Cluster" \
  --region us-ashburn-1 \
  --dry-run

# 2. Write it for real (drop --dry-run). Target a non-dev env with the top-level --env flag:
aidp-fusion-autopilot --env prod init-config \
  --aidp-id ocid1.datalake.oc1.iad.<...> \
  --workspace "My Workspace" --cluster "My Cluster"

# 3. Overwrite an env that already exists:
aidp-fusion-autopilot init-config --aidp-id <...> --workspace "..." --cluster "..." --force
```

All calls sign with the local OCI profile (`--oci-profile`, default `DEFAULT`). For a session-token profile,
have the user run `oci session authenticate` first.

## The one id the user must provide
Two of the three coordinates are name-resolved; the **root anchor** is not:

| Input | How the user gets it | Required? |
|---|---|---|
| **AIDP / DataLake OCID** | Copy once from the AIDP console URL — the `.../aiDataPlatforms/<OCID>/...` segment. | ✅ yes |
| **Workspace name** | The display name in the AIDP console. | ✅ yes |
| **Cluster name** | The Spark cluster's display name. | ✅ yes |
| Region / OCI profile | Defaults `us-ashburn-1` / `DEFAULT`. | default |
| Catalog | Reminder only — lives in `bundle.yaml` under `aidp.catalog`. | optional |

## Patterns
- **Dry-run, then write.** Always run `--dry-run` first so the user confirms the resolved keys and YAML.
- **Name miss → list + pick.** If a workspace/cluster name isn't found, the command prints the **available**
  names. Relay that list and ask the user to choose the exact one, then re-run.
- **Inactive cluster.** If the cluster resolves but isn't `ACTIVE`, the config still writes — surface the
  warning and tell the user to start it before `run` (see `aidp-cluster-ops` / `bootstrap`).
- **Multi-env.** Use `--env <name>` to add `prod` alongside `dev`; sibling environments are preserved.
  `--force` is only needed to overwrite an env that already exists.
- **Hand off.** After writing, point the user at the printed next steps: `aidp-fusion-autopilot validate` →
  `aidp-fusion-autopilot bootstrap`. Remind them the **catalog** goes in `bundle.yaml` (`aidp.catalog`).

## Notes
- **Comments aren't preserved.** Writing re-emits the YAML, so hand-written comments in an existing
  `aidp.config.yaml` are dropped (resolved values are kept). Mention this if the file is heavily commented.
- **Skill family.** Sibling of `aidp-fusion-seed`; both wrap the self-contained CLI rather than importing the
  orchestrator. The seed skill's precondition ladder can invoke this skill when the config is the missing rung.
