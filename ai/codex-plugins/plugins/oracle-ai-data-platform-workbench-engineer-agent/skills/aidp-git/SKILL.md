---
name: aidp-git
description: Operate native Git in an AIDP workspace — branches, checkout, commit/push, pull, merge, rebase, reset, diff, and conflict resolution on workspace git repositories. Use when the user wants to version workspace notebooks/code in Git, manage branches, commit/push changes, or resolve conflicts inside AIDP. Preview GitService API via `oci raw-request`; verify live first.
---

# `aidp-git` — native Git in the workspace (Preview)

Drive AIDP's native Git integration over the REST `GitService` API. Self-contained — no MCP and no
`ai-data-engineer-agent` repo required. All calls are `oci raw-request` against the AIDP control plane
(base URL + auth ladder in `references/oci-raw-request.md`).

> **CLI gap (no invented commands):** the official `aidp` CLI v1.0.0 does **not** expose a full
> `GitService` group — only `aidp workspace create-git-folder` exists. Branch / checkout / commit-push /
> pull / merge / rebase / reset / diff / conflict-resolution all stay on the **REST API (Preview, may be
> unprovisioned)**. Do not assume an `aidp git` command exists — see
> [references/aidp-cli-map.md]($HOME/.aidp/references/aidp-cli-map.md).

> **Preview + verify-first (no-fabrication):** `GitService` is **Preview**. A live read
> (`actions/gitOperationState`) returning `404 NotAuthorizedOrNotFound` in a `20240831` tenancy means
> **GitService is not provisioned** for that env — not that the path is wrong. Confirm the working
> `API_VERSION`/`PATH_PREFIX` with a live read before any mutating op and record it in
> `references/rest-endpoint-map.md`.

## When to use
- "Version my workspace notebooks in Git", "create/checkout a branch", "commit & push", "pull/merge/rebase",
  "what changed (diff)", "resolve conflicts" — inside the AIDP workspace.

## Endpoints (workspace + repo scoped; Preview — default `20240831`, prefix `dataLakes`)
Base: `https://aidp.<region>.oci.oraclecloud.com/20240831/dataLakes/<DATALAKE_OCID>/…`
- `GET|PUT /workspaces/{ws}/gitRepositories/{repo}` · `POST|GET …/gitBranches`
- Actions: `POST …/actions/checkOutBranch|commitPush|merge|pull|rebase|reset|resetState|resolveConflicts`
- Read state: `GET …/actions/gitOperationState|gitDiff|gitDiffDetails`

> Default to `20240831` (the live-verified version for this tenancy). Treat `20260430` as a future GA
> target — only try it after a tenancy upgrade, and probe rather than assume.

## Entry point — register/clone the repo (`CreateGitFolderDetails`)
Before any branch/commit op you need a git folder. This is the **one** git operation with a real CLI
command (CLI README "workspace create-git-folder"):
`aidp workspace create-git-folder <DATALAKE_OCID> <WORKSPACE-KEY> --body <JSON>`. Body
(SDK `create_git_folder_details.py:46-62`):

| Field (wire) | Req | Notes |
|---|---|---|
| `folderPath` | ✅ | absolute path of the git folder to create |
| `gitRepositoryUrl` | ✅ | repo URL to clone |
| `branchName` | ✅ | branch to clone/check out |
| `credentialKey` | ✅ | key of stored git credentials (a `GIT_ACCOUNT` user-setting — see `aidp-user-settings` — or a `credentialStore` entry — see `aidp-credentials`) |
| `gitProviderKey` | – | key of the git provider in the provider table |
| `description` | – | short repo description |

Example — **persist to `.aidp/payloads/create-<name>-git-folder.json` and confirm first:**
```json
{
  "folderPath": "/Workspace/repos/my-repo",
  "gitRepositoryUrl": "https://github.com/org/my-repo.git",
  "branchName": "main",
  "credentialKey": "<CREDENTIAL_KEY>"
}
```
> Field **names** are confirmed (SDK `attribute_map` + CLI README). The CLI command itself targets
> `workspaces/{ws}` and is the documented path here; the rest of `GitService` (branch/commit/…) stays
> verify-first Preview (`…/gitRepositories` GET returned 404 = not provisioned in this env —
> `references/rest-endpoint-map.md`).

## Workflow
1. Base URL + auth ladder (`references/oci-raw-request.md`). **Verify** with `GET …/actions/gitOperationState`.
   A `404 NotAuthorizedOrNotFound` here = GitService Preview not provisioned in this `20240831` env → stop
   and tell the user to enable it; do not fabricate success.
2. Branch: `gitBranches` (list/create) → `actions/checkOutBranch`.
3. Inspect: `actions/gitDiff` / `gitDiffDetails` before committing.
4. Commit/push: `actions/commitPush` (message in body). Integrate: `pull`/`merge`/`rebase`.
5. Conflicts: `actions/resolveConflicts`; recover bad state with `reset`/`resetState` (confirm first).
6. Long ops may be async (202) — poll per the shared conventions in `references/oci-raw-request.md`.

## Guardrails
- `reset`/`resetState`/force-style ops are destructive — show what will be lost and confirm.
- Don't commit secrets/`.env`.
- For mutating ops (commitPush / merge / rebase / reset / resolveConflicts), persist the request body to
  `.aidp/payloads/` and confirm first — see [references/payloads.md]($HOME/.aidp/references/payloads.md).

## References
- [references/aidp-cli-map.md]($HOME/.aidp/references/aidp-cli-map.md) — official `aidp` CLI v1.0.0 has no full GitService group (only `workspace create-git-folder`)
- [references/oci-raw-request.md]($HOME/.aidp/references/oci-raw-request.md) · [references/rest-endpoint-map.md]($HOME/.aidp/references/rest-endpoint-map.md)
- [references/payloads.md]($HOME/.aidp/references/payloads.md) — persist + confirm request bodies for mutating ops
