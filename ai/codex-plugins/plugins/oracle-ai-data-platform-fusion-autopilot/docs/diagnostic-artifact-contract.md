# Diagnostic artifact contract (Phase 3a)

Bootstrap (`aidp-fusion-autopilot bootstrap`) writes structured diagnostic
artifacts under `<bundle.yaml.parent>/.aidp/diagnostics/<run_id>/`
when mechanical resolution cannot proceed. Feature #3
(`v2-phase-3b-medallion-author-skill`) reads these to draft overlays;
other tools (custom recovery scripts, alternate LLMs, Web UIs) can
consume the same contract because Pydantic models + a documented
schema version are the public surface.

## Path naming

Every file in the diagnostics directory uses a discriminator-prefix
so multiple failures in one run never collide:

```
<bundle.yaml.parent>/.aidp/diagnostics/<run_id>/
├── AIDPF-1020.json                       # identity gate (one per run)
├── AIDPF-2010__<vp-name>.json            # one per failing columnAlias
├── AIDPF-2011__<vp-name>.json            # one per failing semanticVariant
└── AIDPF-2012.json                       # Phase 3c — runtime drift (at most one)
```

`<run_id>` is opaque (ISO-timestamp + short uuid). `<vp-name>` is the
variation-point name (e.g. `invoice_currency_code`).

**`AIDPF-2012` is NOT in bootstrap's surface.** Runtime preflight
(feature #4) is the only emitter of 2012 — it signals "evidence stale,
run `bootstrap --refresh`". Bootstrap's job is to *resolve* drift, not
to *detect* it.

## Schema versioning

Every artifact carries a `schemaVersion: 1` field. Forward-compatibility
per PLAN §9.5.8: consumers MUST ignore unknown top-level fields. A
future `schemaVersion: 2` will add fields without breaking v1
consumers; breaking changes bump the literal.

## Pydantic models

Defined in
`scripts/oracle_ai_data_platform_fusion_autopilot/schema/diagnostic_artifact.py`:

* `DiagnosticArtifactBase` — shared header (`schemaVersion`, `runId`,
  `tenant`, `errorCode`, `errorMessage`, `generatedAt`).
* `VariationPointDiagnosticV1` — extends the base with a required
  `variationPoint: VariationPointFailure` payload. `errorCode` is
  constrained to `AIDPF-2010` / `AIDPF-2011`.
* `IdentityDiagnosticV1` — extends the base with a required
  `identityProbe: IdentityProbeFailure` payload. `errorCode` is
  constrained to `AIDPF-1020`. `tenant` MUST be `null` (identity gate
  fires before tenant context is loaded).
* `SchemaDriftDiagnosticV1` — Phase 3c. Extends the base with a
  required `schemaDrift: SchemaDriftFailure` payload. `errorCode` is
  constrained to `AIDPF-2012`. `tenant` is required (drift fires
  AFTER the tenant profile is loaded, so the context is always
  available — unlike `AIDPF-1020`).

## AIDPF-2010 / 2011 — variation-point unresolved

Triggered when a `required: true` variation point has zero matching
candidates on the tenant's bronze schema. Bootstrap collects ALL
failures across the walk loop before exiting; each failure writes one
artifact file named after the variation point.

Example: `AIDPF-2010__invoice_currency_code.json`

```json
{
  "schemaVersion": 1,
  "runId": "20260605T120000Z-abc12345",
  "tenant": "finance-default",
  "errorCode": "AIDPF-2010",
  "errorMessage": "columnAliases.invoice_currency_code has no matching candidate on the tenant's bronze. appliesTo=bronze.ap_invoices",
  "generatedAt": "2026-06-05T12:00:00+00:00",
  "variationPoint": {
    "name": "invoice_currency_code",
    "kind": "columnAliases",
    "appliesTo": "bronze.ap_invoices",
    "candidatesTried": [
      {"candidate": "ApInvoicesInvoiceCurrencyCode", "outcome": "column_not_found", "detail": null},
      {"candidate": "ApInvoicesCurrencyCode", "outcome": "column_not_found", "detail": null}
    ],
    "observedBronzeSchema": [
      {"name": "ApInvoicesId", "type": "bigint", "nullable": true},
      {"name": "ApInvoicesAmount", "type": "decimal(18,2)", "nullable": true}
    ],
    "priorPinned": null
  }
}
```

Field semantics:

| Field | Meaning |
|---|---|
| `variationPoint.name` | Pack-declared variation point id. |
| `variationPoint.kind` | `columnAliases` or `semanticVariants`. |
| `variationPoint.appliesTo` | Bronze table the VP targets (`bronze.<dataset_id>`). |
| `variationPoint.candidatesTried` | Per-candidate walker result, priority order preserved. `outcome` ∈ `column_not_found` (columnAlias) / `detect_clause_failed` (semanticVariant). `detail` carries e.g. `detect.columnExists=ApInvoicesCancelledFlag` for semantic variants. |
| `variationPoint.observedBronzeSchema` | Columns present in the tenant's bronze table at probe time. Skill uses these to suggest a candidate to add to an overlay. |
| `variationPoint.priorPinned` | Value from the prior profile when running `--refresh`; `null` on initial onboarding. |

AIDPF-2011 (semantic variant unresolved) has the same shape — only the
`errorCode` constant + the `outcome` values change.

## AIDPF-1020 — operator identity unresolved

Triggered when the §9.5.9 precedence chain (`--operator` →
`AIDP_OPERATOR` → `USER`) produces no non-empty value. The artifact has
no `<vp-name>` discriminator — one identity gate fires per run.

Example: `AIDPF-1020.json`

```json
{
  "schemaVersion": 1,
  "runId": "20260605T120000Z-abc12345",
  "tenant": null,
  "errorCode": "AIDPF-1020",
  "errorMessage": "AIDPF-1020: operator identity not resolvable from any source (--operator, AIDP_OPERATOR, USER). Set --operator, export AIDP_OPERATOR, or run from a shell where $USER is set.",
  "generatedAt": "2026-06-05T12:00:00+00:00",
  "identityProbe": {
    "probedSources": ["--operator", "AIDP_OPERATOR", "USER"],
    "nonEmptySources": []
  }
}
```

`tenant` is `null` because identity gate fires BEFORE bootstrap loads
the tenant profile. Skill (feature #3) shows the operator the probed
sources and suggests which env var to set.

## AIDPF-2012 — bronze schema fingerprint drift (Phase 3c)

Emitted by the runtime preflight gate inside
`_run_content_pack_backend` (NOT bootstrap) when
`--mode incremental` runs against a bronze schema whose fingerprint
diverges from the value pinned in the tenant profile. The artifact has
no `<vp-name>` discriminator — at most one drift artifact per run.

Example: `AIDPF-2012.json`

```json
{
  "schemaVersion": 1,
  "runId": "cp-20260606120000-abcdef12",
  "tenant": "finance-default",
  "errorCode": "AIDPF-2012",
  "errorMessage": "AIDPF-2012: bronze schema fingerprint diverged from pinned profile; run `aidp-fusion-autopilot bootstrap --refresh` to re-pin.",
  "generatedAt": "2026-06-06T12:00:00+00:00",
  "schemaDrift": {
    "priorFingerprint": "sha256:aaa…",
    "currentFingerprint": "sha256:bbb…",
    "pinnedAt": "2026-06-01T08:00:00+00:00",
    "datasetDeltas": [
      {
        "datasetId": "ap_invoices",
        "addedColumns": [
          {"name": "ApInvoicesNewColumn", "type": "string", "nullable": true}
        ],
        "removedColumns": [
          {"name": "ApInvoicesOldColumn", "type": "string", "nullable": true}
        ],
        "typeChangedColumns": [
          {"name": "ApInvoicesAmount", "priorType": "bigint", "currentType": "string"}
        ]
      }
    ],
    "affectedVariationPoints": [
      {
        "name": "invoice_currency_code",
        "kind": "columnAliases",
        "pinnedCandidate": "ApInvoicesInvoiceCurrencyCode",
        "stillPresent": false
      }
    ]
  }
}
```

| Field | Meaning |
|-------|---------|
| `tenant` | Required (drift is a per-tenant event; profile must be loaded for the gate to fire). |
| `schemaDrift.priorFingerprint` | Value pinned in the tenant profile (last `bootstrap` / `bootstrap --refresh`). |
| `schemaDrift.currentFingerprint` | Live bronze fingerprint computed during preflight. |
| `schemaDrift.pinnedAt` | Timestamp the prior fingerprint was pinned. |
| `schemaDrift.datasetDeltas[]` | **Phase 3d** — per-dataset column-level diff: `addedColumns` / `removedColumns` / `typeChangedColumns`. Populated when the bootstrap-pinned `profiles/<tenant>.schema-snapshot.yaml` is present and self-consistent. Empty (with a one-time WARN log) when the snapshot is absent (pre-3d profile), unparseable, or fingerprint-desynced from the profile — remediation is `aidp-fusion-autopilot bootstrap --refresh` to repin both atomically. Diff key canonicalisation mirrors the fingerprint algorithm: case- and whitespace-only differences are invisible; original casing is preserved on the surfaced entries for operator display. |
| `schemaDrift.affectedVariationPoints[]` | Per-VP deltas — for each VP resolved in the profile, whether its pinned candidate is still present in the live bronze. Empty if every pinned candidate still matches (the fingerprint shifted for an unrelated reason — added/removed/retyped columns outside any VP). |

The `medallion_author.reader.read_run` parses this alongside the
`AIDPF-2010` / `AIDPF-2011` artifacts, surfacing
`result.schema_drift_failure`. A directory containing ONLY a drift
artifact (no 2010/2011) sets `result.has_drift_only`, in which case
the skill **refuses to draft** — drift recovery is
`bootstrap --refresh`, not an overlay.

When emitted via REST dispatch, the cluster-side notebook embeds the
full artifact JSON in the marker payload's `artifact_json` field; the
laptop dispatcher writes it to the laptop-side
`.aidp/diagnostics/<run_id>/AIDPF-2012.json` so the operator's
workflow (skill / `bootstrap --refresh`) finds it at the same path
regardless of execution mode.

## `--resolutions <json-file>` file format

Bootstrap accepts a scripted-resolution file to drive multi-match
choices without a terminal. Feature #3 (skill) writes one of these on
overlay-commit; careful operators can hand-author one for reproducible
CI runs.

Schema:

```json
{
  "schemaVersion": 1,
  "tenant": "finance-default",
  "resolutions": [
    {
      "name": "invoice_currency_code",
      "kind": "columnAliases",
      "chosenCandidate": "ApInvoicesInvoiceCurrencyCode"
    }
  ]
}
```

Pack-aware validation rules (enforced by
`schema.resolutions_input.validate_against_pack`):

1. `tenant` must match `bundle.contentPack.profile`.
2. Every `name` must be declared in the resolved pack's
   `columnAliases` / `semanticVariants`.
3. Every `kind` must match the declared kind for that name.
4. `chosenCandidate` must be a member of the variation point's
   current matched-candidate set (AND in the pack's `candidates`
   list).
5. No duplicate `(name, kind)` pairs.
6. Every variation point with a `MultiMatch` outcome MUST have a
   corresponding entry.
7. No entries for variation points whose walker outcome was
   `AutoResolved` or `NoMatch`.

## Evidence snapshot — successful resolution

A successful bootstrap also writes
`<bundle.yaml.parent>/evidence/<tenant>/<ISO-ts>.yaml` recording the
walker outcomes + approval metadata. Schema lives in
`scripts/oracle_ai_data_platform_fusion_autopilot/schema/evidence_snapshot.py`;
nested shape per PLAN §9.5.7 / §9.5.9:

```yaml
schemaVersion: 1
tenant: finance-default
generatedAt: 2026-06-05T12:00:00+00:00
runId: 20260605T120000Z-abc12345
bronzeSchemaFingerprint: "sha256:..."
provenance:
  approvedBy:
    operator: alice@oracle.com
    timestamp: 2026-06-05T12:00:00+00:00
    mechanism: terminal_prompt    # or auto_resolve / cli_flag / non_interactive
  skillVersion: null              # populated by feature #3 on Tier-2 commit
  evidence:
    snapshots:
      - snapshotId: 20260605T120000Z-abc12345
        capturedAt: 2026-06-05T12:00:00+00:00
        resolutions:
          - name: invoice_currency_code
            kind: columnAliases
            chosenCandidate: ApInvoicesInvoiceCurrencyCode
            candidatesConsidered:
              - candidate: ApInvoicesInvoiceCurrencyCode
                outcome: matched
              - candidate: ApInvoicesCurrencyCode
                outcome: matched
            evidence: {}           # free-form measurement context
```

Old evidence files are PRESERVED on every `--refresh` per the §9.5.7 #2
accumulation rule — bootstrap never deletes prior snapshots.

## Audit-trail mechanism semantics

`provenance.approvedBy.mechanism` records HOW the multi-match choices
were made. Bootstrap records the WEAKEST operator-touched mechanism
applied across the run (audit-floor interpretation: a single
`non_interactive` pick taints the whole profile, because that's the
least-validated approval):

| Mechanism | Meaning |
|---|---|
| `auto_resolve` | Every variation point had exactly one matching candidate; no operator action needed. |
| `cli_flag` | At least one multi-match was resolved via `--resolutions` (feature #3 or careful operator). |
| `terminal_prompt` | At least one multi-match was resolved by operator input at the terminal. |
| `non_interactive` | At least one multi-match was auto-picked under `--non-interactive`. Production runs MUST NOT use this. |
| `skill_proposed` | Reserved for feature #3's Tier-2 overlay-commit path. |

Precedence among operator-touched (weakest wins):
`non_interactive` < `cli_flag` < `terminal_prompt`. `auto_resolve` is
the no-operator baseline; any operator-touched mechanism overrides it.
