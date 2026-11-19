---
name: aidp-fusion-bootstrap
description: "Guide and run the Oracle AIDP Fusion Autopilot bootstrap phase. Validates bundle/config readiness, checks AIDP/Fusion/IAM prerequisites, runs `aidp-fusion-autopilot bootstrap --check-iam` or `bootstrap --refresh`, surfaces tenant variation choices pinned into the active profile YAML and evidence snapshots, and routes unresolved variation failures (`AIDPF-2010` / `AIDPF-2011`) to `/medallion-author`. Use when the user says 'bootstrap', 'pin tenant variation', 'create the profile', 'run pre-seed checks', 'missing profile', 'bootstrap --refresh', or needs the setup phase before seed/incremental. NOT for seed runs, incremental refresh, OAC dataset/workbook work, or net-new mart authoring."
allowed-tools: Read, Bash, Glob, Grep
---

# aidp-fusion-bootstrap — bootstrap tenant profile and variation

This skill owns the bootstrap phase only: validate the customer project, run the
CLI bootstrap safely, surface what tenant variation was pinned, and route
bootstrap-specific failures. It shells out to `aidp-fusion-autopilot`; it never
edits `profiles/`, `evidence/`, or diagnostics directly.

Bootstrap is the bridge between configuration and first seed. It probes
prerequisites, resolves content-pack variation points against the tenant, and
writes the runtime profile used by seed and incremental runs.

## When to use

- "Run bootstrap", "bootstrap this tenant", "pin tenant variation".
- "Create the profile", "my profile is missing", "profiles/<profile>.yaml is absent".
- "Run pre-seed checks", "check IAM before seed", "verify BICC/AIDP prerequisites".
- "Run bootstrap --refresh" after `AIDPF-2012`, PVO/source drift remediation, or a Fusion release change.
- A seed or incremental precondition reports `missing: ["profile"]`.

## When NOT to use

- First materialization or overwrite of bronze/silver/gold -> `/aidp-fusion-seed`.
- Day-2 refresh -> `/aidp-fusion-incremental`.
- Runtime drift diagnosis (`AIDPF-2072`, `AIDPF-4070/4071`, `AIDPF-4040`) -> `/fusion-drift-doctor`.
- Authoring new analytical content -> `/mart-author`.
- OAC connection, dataset, or workbook work -> OAC dataset/workbook skills.

## What bootstrap does

- Runs pre-onboarding probes: bundle/config shape, BICC auth, BICC catalog reachability, AIDP REST, and optional IAM checks.
- Probes the tenant bronze schema for the active content pack. When a bronze
  table is already landed it reads the live Delta schema; when it is not yet
  landed (a fresh tenant) it resolves the same schema from the BICC PVO source
  via a metadata-only `inferSchema` probe — so bootstrap runs **before** the
  first seed, with no chicken-and-egg. The fingerprint is identical either way
  (bronze audit columns are stripped before fingerprinting).
- Walks pack-declared `columnAliases` and `semanticVariants`.
- Pins resolved values to `profiles/<contentPack.profile>.yaml`.
- Writes `profiles/<contentPack.profile>.schema-snapshot.yaml`.
- Writes evidence under `evidence/<profile>/<timestamp>.yaml`.
- On unresolved required variation points, writes diagnostics under `.aidp/diagnostics/<run_id>/`.

Bootstrap is the only writer to `profiles/` and `evidence/`. Do not hand-edit
resolved profile values to make a run pass.

## Workflow

### 1. Confirm project root and config readiness

Run from the customer project root, where `bundle.yaml` and `aidp.config.yaml`
live. Validate first:

```bash
aidp-fusion-autopilot validate
```

If available, use the shared precondition helper for a structured view. Resolve
the helper relative to this skill directory, or use its absolute path when your
shell is in a customer project:

```bash
python3 ../aidp-fusion-seed/preconditions.py \
  --bundle bundle.yaml \
  --config aidp.config.yaml \
  --env <env>
```

Interpret `missing` for bootstrap this way:

- `bundle`: stop and have the user run `aidp-fusion-autopilot init` or fix `bundle.yaml`.
- `config`: route to `/aidp-fusion-config`; bootstrap cannot invent AIDP coordinates.
- `profile`: this is the bootstrap target; proceed unless another blocker exists.
- `cluster`: start or fix the AIDP cluster before default cluster-dispatched bootstrap.

Also confirm the Fusion password is in the AIDP credential store as
`fusion_bicc_password` with key `password`. Do not ask the user to paste secrets
into chat.

### 2. Choose initial bootstrap vs refresh

Use initial bootstrap when no valid profile exists:

```bash
aidp-fusion-autopilot bootstrap --check-iam
```

Use refresh only when a prior profile exists and you are re-pinning against live
bronze after drift or a Fusion release change:

```bash
aidp-fusion-autopilot bootstrap --refresh --check-iam
```

Do not run `--refresh` as a blind fix for unrelated seed failures. Runtime drift
codes should be diagnosed by `/fusion-drift-doctor` first, then routed here when
the safe fix is bootstrap refresh.

### 3. Preserve operator control

Bootstrap can freeze tenant variation choices. Before running, tell the user:

- which profile will be written,
- whether this is initial pinning or refresh,
- whether IAM checks are enabled,
- whether the run may prompt for a multi-match variation choice.

Prefer the default interactive operator path for real tenants. Do not use
`--non-interactive` for customer onboarding unless the user explicitly asks for
CI/sandbox behavior and accepts first-candidate auto-pick semantics.

For audit identity issues (`AIDPF-1020`), ask for or set a concrete operator:

```bash
aidp-fusion-autopilot bootstrap --check-iam --operator "<operator-id>"
```

### 4. Run bootstrap and classify the result

On success, report the concrete artifacts:

- `profiles/<contentPack.profile>.yaml`
- `profiles/<contentPack.profile>.schema-snapshot.yaml`
- latest `evidence/<profile>/<timestamp>.yaml`
- key resolved variation choices: column aliases and semantic variants
- any IAM/BICC/AIDP warnings that remain
- any yellow `COA advisory` lines (see below)

Then hand off to `/aidp-fusion-seed` for first materialization.

#### COA advisory lines (yellow, never blocking)

Bootstrap ends with a pre-extraction chart-of-accounts advisory: it compares
the charts **visible to the configured Fusion user** (transactional REST LOVs,
laptop-side `FUSION_BICC_USER` / `FUSION_BICC_PASSWORD` env credentials)
against the profile's `chartOfAccounts` mapping. It runs on BOTH paths —
including the no-drift `--refresh` early return, because an unchanged bronze
fingerprint says nothing about a newly active chart. Read it as:

- `chart(s) <ids> are ACTIVE … but have no chartOfAccounts.byChart arm` —
  a real finding: add the arm via `bootstrap --refresh`; an ordinary
  `run --mode incremental` absorbs a new chart (additive fast path).
- `partial — probed N of M` — findings above are confirmed; no all-clear is
  implied for the unprobed rest.
- `COA advisory skipped: …` — the advisory could not run (missing env creds,
  `${aidp:secret:…}` ref, 401/403, time budget). Normal on cluster-dispatch
  and CI; the post-extraction COA gate (AIDPF-2018) remains the
  authoritative, tenant-complete check.

Never treat advisory lines as a bootstrap failure; they carry no exit code
and no AIDPF code.

On failure, classify before recommending action:

| Failure | Meaning | Route |
|---|---|---|
| `AIDPF-2010` | Required `columnAliases` variation point had no candidate on tenant bronze. | `/medallion-author` |
| `AIDPF-2011` | Required `semanticVariants` variation point had no matching detector. | `/medallion-author` |
| `AIDPF-1020` | Operator identity missing. | Re-run with `--operator` or fix `AIDP_OPERATOR` / `USER`. |
| BICC auth/storage | Fusion or BICC external storage prerequisite missing. | Fix Fusion/BICC setup; do not author overlays. |
| AIDP REST/IAM | Workspace/cluster/permission issue. | Fix config/IAM/cluster before rerun. |

For `AIDPF-2010` / `AIDPF-2011`, point the user at the diagnostics directory:

```text
.aidp/diagnostics/<run_id>/AIDPF-2010__<variation-point>.json
.aidp/diagnostics/<run_id>/AIDPF-2011__<variation-point>.json
```

Then invoke or recommend `/medallion-author`. That skill reads the diagnostics
and drafts an overlay; bootstrap remains the only step that commits the final
profile/evidence by rerunning initial bootstrap or `bootstrap --refresh`.

### 5. Refresh-specific rules

Use `bootstrap --refresh` when:

- `/fusion-drift-doctor` classifies drift as mechanically re-pinnable.
- `AIDPF-2012` reports bronze fingerprint drift and the operator confirms the
  bronze change is intended.
- A profile predates schema-snapshot support and needs back-fill.

Refresh must not silently change pinned choices. If refresh wants to change a
pinned value, surface the old value, proposed value, evidence, and ask the user
before continuing. If a new candidate is needed, route to `/medallion-author`.

For power-user refresh after known-good onboarding, `--skip-preonboarding-probes`
is available only with local dispatch. Do not use it as the default demo or
customer path.

## Output contract

Finish with a compact bootstrap ledger:

```text
bootstrap: success
profile: profiles/finance-default.yaml
schema snapshot: profiles/finance-default.schema-snapshot.yaml
evidence: evidence/finance-default/<timestamp>.yaml
variation: 12 column aliases pinned, 2 semantic variants pinned
next: /aidp-fusion-seed
```

On failure:

```text
bootstrap: failed AIDPF-2010
diagnostics: .aidp/diagnostics/<run_id>/AIDPF-2010__invoice_currency_code.json
route: /medallion-author to draft an overlay, then re-run bootstrap
```

## Safety invariants

- The CLI is the contract; never import orchestrator internals or mutate state directly.
- Do not edit `profiles/`, `profiles/*.schema-snapshot.yaml`, or `evidence/` by hand.
- Do not treat pack YAML as proof that the tenant profile exists.
- Do not run seed from this skill; hand off to `/aidp-fusion-seed`.
- Do not hide variation choices. Surface what bootstrap pins.
- Route unresolved variation (`AIDPF-2010` / `AIDPF-2011`) to `/medallion-author`, not `/mart-author`.
- Keep secrets out of chat and logs.
