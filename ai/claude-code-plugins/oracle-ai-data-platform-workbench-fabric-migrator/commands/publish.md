---
description: Upload a finished migration into an AIDP workspace (dry run by default).
---

```bash
fabric-aidp publish ./migrated --prefix <name> --cluster-key <cluster-uuid>
```

**This is the only verb that writes anywhere.** Everything above it —
`inventory`, `plan`, `migrate`, `verify` — is offline and needs no credentials.

Without `--apply` this is a dry run: it prints the notebooks it would upload and
the jobs it would create, sends nothing, and exits 0. **Always run it that way
first and show the user the list.** Only after they have read it:

```bash
fabric-aidp publish ./migrated --prefix <name> --apply \
    --workspace-key <workspace-uuid> --cluster-key <cluster-uuid> \
    --profile <oci-profile> --auth api_key
```

`--apply` needs `aidp-cli` on PATH (`pip install aidp-cli`) and an OCI profile
that can reach the instance. Credentials come from those flags or from
`AIDP_WORKSPACE_KEY` / `AIDP_CLUSTER_KEY` / `AIDP_INSTANCE_ID` in the
environment or a `.env` in the working directory. Never write a credential into
a file in the repository, and never echo one back to the user.

What publish guarantees, and what to tell the user:

- **It never overwrites.** A notebook path or job name that already exists is
  skipped, so re-running is safe. Use a fresh `--prefix` to publish a second
  copy.
- **`--prefix` namespaces paths and job names**, so two people publishing into
  one workspace do not collide. It must be a letter followed by letters, digits
  or underscores.
- **It publishes only rows `report.json` lists as `ok` or `needs_manual_review`**,
  and refuses a migration directory left half-written by an interrupted run.
- **A refusal makes `--apply` exit 1** — a job whose notebook this run did not
  upload, a job with no `--cluster-key`, two pipelines mapping to one job name.
  Report a non-zero exit; do not re-run with different flags to make it green.

Publishing does not run anything. Say so: the notebooks and jobs now exist, and
nobody has yet executed one. Suggest running the simplest job by hand.
