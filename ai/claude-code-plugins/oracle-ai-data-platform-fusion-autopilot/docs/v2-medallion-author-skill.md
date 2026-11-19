# `medallion-author` skill — operator UX walkthrough

Phase 3b ships an operator-side Tier-2 recovery path for the v2
content-pack architecture: when `aidp-fusion-autopilot bootstrap` fails
to mechanically resolve a variation point (AIDPF-2010 / AIDPF-2011),
operators open Claude Code in the project directory and invoke the
`/medallion-author` skill to draft a content-pack overlay extending
the starter pack's candidate list with the operator's tenant
columns.

This document walks the operator through the experience. The full
contract lives in
`.claude/skills/medallion-author/SKILL.md`.

## When you'll see AIDPF-2010 / AIDPF-2011

Bootstrap walks the candidate list each variation point declares.
When none of the candidates is present on the tenant's bronze
schema, the CLI exits 1 and prints:

```
[red]Diagnostic written: <bundle.yaml.parent>/.aidp/diagnostics/
       20260606T120000Z-abc12345/AIDPF-2010__invoice_currency_code.json[/red]
[red]2 variation point(s) unresolved[/red]
```

The full failure context (observed bronze columns, candidates tried,
detect-clause outcomes) lives in the diagnostic JSON. Don't hand-edit
your profile — open Claude Code and ask the skill to draft an overlay.

## Invocation

```
$ cd <bundle directory>
$ claude
> /medallion-author
```

Optionally pass an explicit `<run_id>` to target a specific bootstrap
run (otherwise the skill picks the latest under
`.aidp/diagnostics/`).

## The 8-step conversation

### 1. Discovery

The skill reads the diagnostic artifacts and reports what it found:

```
Found 2 unresolved variation points in run 20260606T120000Z-abc12345:

  • invoice_currency_code (columnAliases, bronze.ap_invoices)
    Candidates tried:
      - ApInvoicesInvoiceCurrencyCode (column_not_found)
      - ApInvoicesCurrencyCode        (column_not_found)
    Observed bronze schema (28 columns):
      - ApInvoicesXCurrCode (string)
      - ApInvoicesCancelDate (timestamp)
      ... (26 more)

  • vendor_id (columnAliases, bronze.erp_suppliers)
    Candidates tried:
      - VENDORID (column_not_found)
    Observed bronze schema (14 columns):
      - VENDOR_ID (string)
      ...
```

### 2. Refuse gates

If `AIDPF-1020.json` is present (operator-identity gate), the skill
refuses: "fix `--operator` / `AIDP_OPERATOR` / `$USER` before
drafting an overlay".

If the diagnostic schema version is unknown, the skill refuses per
PLAN §9.5.8 forward-compat rule.

### 3. Pack load + affected nodes

The skill loads your pack (overlay chain included) and computes
which silver/gold nodes consume each failing VP. Surfaced to you so
you know what the remediation will touch:

```
invoice_currency_code is referenced by:
  • silver.supplier_spend
  • silver.ap_aging
  • gold.supplier_spend
  • gold.ap_aging
```

### 4. Propose phase

For each failing VP, the skill proposes up to 3 candidates from
your bronze schema, ranked by similarity + the seed knowledge base:

```
Proposed candidate for invoice_currency_code:
  + ApInvoicesXCurrCode  (confidence: high, KB-hint: currency-code-casing-variants)
  
  Rationale: Fusion 25C renamed CurrencyCode → XCurrCode for
  multi-currency tenants. The known-deltas knowledge base confirms
  this pattern matches your observed `ApInvoicesXCurrCode` column.

  Incremental impact: likely-different-semantics; Option D recommended.
  Affected nodes: silver.supplier_spend, silver.ap_aging,
                  gold.supplier_spend, gold.ap_aging.

  Approve this proposal? [y/N/edit]:
```

You can approve (`y`), reject (`n`), or edit the candidate +
rationale (`edit`).

### 5. Operator review

Approve each proposal independently. The skill never commits a
proposal you didn't OK.

### 6. Draft

After approval, the skill writes:

```
overlays/<overlay-name>/
├── pack.yaml              ← validated overlay with extended candidate list
├── resolutions.json       ← (conditional — see below)
├── remediation.md         ← operator runbook for the recommended option
├── remediation.sql        ← (Option B only) surgical backfill MERGE
└── skill-evidence.json    ← skill's own audit trail
```

#### When `resolutions.json` is emitted

- **Skipped** for initial AIDPF-2010 onboarding where the new
  candidate becomes the only matching one on bronze. The next
  bootstrap walks the extended candidate list and AutoResolves
  trivially — no scripted resolutions file needed. Emitting one
  here would be rejected by feature #2's validator.
- **Emitted** for MultiMatch picks (multiple candidates match;
  operator chose one) or refresh-promotion picks (prior pinned
  differs from new chosen). Bootstrap consumes the file via
  `--refresh --resolutions <path>`.

### 7. Hand-off

The skill prints the exact CLI invocation to run. The template
branches on TWO axes — whether resolutions.json was emitted, and
which backend your bundle declares.

#### Example — initial-onboarding + content-pack backend

```
Overlay drafted: overlays/demo-pod-currency-fix/pack.yaml
Remediation:    overlays/demo-pod-currency-fix/remediation.md (Option D)

Next steps:
  1. Review the overlay + remediation.md.
  2. Wire the overlay into the pack chain:
       aidp-fusion-autopilot use-pack overlays/<overlay-name> --profile <tenant>
  3. Re-run bootstrap (NO --resolutions flag needed):
       aidp-fusion-autopilot bootstrap --operator "$USER"
  4. Apply Option D remediation per remediation.md:
       aidp-fusion-autopilot run --mode seed \
         --datasets supplier_spend,ap_aging
  5. Resume scheduled `aidp-fusion-autopilot run --mode incremental`.
```

Phase 9 follow-up deleted the legacy backend (and the
`--execution-backend` flag); only one execution path ships. The runbook
emits the unflagged invocation with pack node IDs as the dataset filter.

### 8. Provenance

Every artifact carries:

| Field | Value | Audit role |
|---|---|---|
| `skillId` | `aidp-fusion-medallion-author` | Bootstrap detects skill-authored overlays via this field |
| `skillVersion` | `0.1.0` | Bumped on every `known-deltas.yaml` change |
| `modelId` | `claude-opus-4-7` (from runtime) | Identifies the LLM that drafted |
| `diagnosticRunId` | `20260606T120000Z-abc12345` | Threads back to feature #2's failure |
| `proposals.<vp>` | `{candidateAdded, confidence, reasoning}` | Per-VP audit |
| `incrementalImpact.<vp>` | `{changeKind, priorPinned, newCandidate, riskLabel, affectedNodes, remediation}` | Per-VP impact analysis |

After your `bootstrap --refresh` commit, feature #2's evidence
snapshot picks up:

- `SnapshotProvenance.skillVersion` ← from overlay's `skillVersion`
- `provenance.approvedBy.mechanism: skill_proposed` ← because
  `skillId` matched and the chosen candidate was a skill proposal
- per-resolution `incremental_impact` ← mirrors overlay's
  `incrementalImpact[vp]`

The SOX audit trail is complete: failure → draft → commit, all
correlated by `diagnosticRunId` and `skillVersion`.

## Remediation menu (round-4 confirmed)

The skill recommends and the operator chooses one of:

| Option | Cost | SOX safety | Default when |
|---|---|---|---|
| **A — No action** | Free | Clean (rename only) | `riskLabel: likely-rename` |
| **B — Surgical backfill MERGE** | Minutes | Operator-reviewed SQL | Advanced opt-in; large tables; column-substitution-only |
| **C — Watermark rewind** | n/a | n/a | **DEFERRED to v0.4** — requires `aidp-fusion-autopilot rewind` verb |
| **D — Targeted re-seed** *(default)* | Hours per affected node | Engine code path | All non-rename cases |
| **E — Full re-seed** | Hours-to-days | Clean baseline | Audit reset (rare) |

## Examples

### Currency-code rename (Fusion 25C)

Tenant has `ApInvoicesXCurrCode` but neither
`ApInvoicesInvoiceCurrencyCode` nor `ApInvoicesCurrencyCode`.
Bootstrap fails `AIDPF-2010__invoice_currency_code.json`. Skill
proposes `ApInvoicesXCurrCode` (KB hint: currency-code-casing-variants),
risk label `likely-different-semantics`, recommends Option D
targeted re-seed of `supplier_spend` + `ap_aging` (silver + gold).

### Non-conventional COA depth

Tenant uses 12 COA segments instead of the conventional 1–6.
Bootstrap fails three `AIDPF-2010__coa_*_segment.json`. Skill
proposes `CodeCombinationSegment{7..12}` (KB hint: coa-deeper-than-6-segments),
operator confirms the role-to-segment mapping is correct. Risk
label is `likely-different-semantics` (different data per segment);
recommends Option D for `dim_account`.

## What the skill explicitly does NOT do

- It does NOT run during `aidp-fusion-autopilot run` (per ADR-0017,
  engine has zero LLM dependency at runtime).
- It does NOT modify `profiles/<tenant>.yaml` or `evidence/<tenant>/`
  — those are bootstrap's domain per PLAN §9.5.7 #6.
- It does NOT author SQL templates or new silver/gold nodes — only
  EXTENDS existing variation-point candidate lists per §9.5.6 #1
  MAY-NOT.
- It does NOT execute the remediation — emits files for the
  operator to run.
- It does NOT support multi-turn iterative refinement in v0.3 —
  one round-trip per `/medallion-author` invocation. Re-invoke for
  revisions.

## Skill version policy

Skill version is loose-coupled per PLAN §9.5.8. Recorded in evidence
snapshots as audit metadata, NOT a plan-hash input. Bumped on:

- New entries to `known-deltas.yaml` → patch.
- New variation-point families → minor.
- Schema-breaking changes to overlay `provenance` → major.

v0.1.0 ships with three KB entries (cancelled-status alternates,
currency-code casing, COA segment depth).
