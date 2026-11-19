# AIDPF Error Codes

This file is the operator-facing reference for `AIDPF-*` codes emitted by the
Fusion Autopilot plugin. Use it when a CLI command, bootstrap run, seed run,
incremental run, content-pack validation, dashboard validation, or diagnostic
artifact reports an `AIDPF` code.

For conversational recovery, start with `/aidpf-error-triage`; it extracts the
code and diagnostic context, then routes to the right recovery skill or command.

Most structured diagnostics are written under:

```text
.aidp/diagnostics/<run_id>/
```

Some codes are historical, removed, or test-only. They are still listed here so
older reports, ADRs, and tests can be interpreted without guessing.

## Status Values

| Status | Meaning |
|---|---|
| Active | Can be emitted by current runtime, CLI, validation, renderer, or authoring flows. |
| Warn-only | Validation warning; the command can continue unless another error blocks it. |
| Removed | Used by an older phase but no longer emitted in the current flow. |
| Retired | Reserved from a deleted implementation path. |
| Historical | Mentioned in design documents, but not emitted by current code. |
| Test-only | Used only to prove invalid/unknown code handling. |

## Codes

| Code | Area | Status | Meaning | Usual action |
|---|---|---|---|---|
| `AIDPF-1001` | Bundle config | Historical | Planned code for `bundle.yaml` schema version newer than the engine. | Upgrade the plugin/engine or downgrade/migrate the bundle schema. |
| `AIDPF-1010` | Bundle config | Historical | Planned code for an unresolved required environment variable. | Set the env var or replace it with a supported secret reference. |
| `AIDPF-1020` | Bootstrap / diagnostics | Active | Operator identity cannot be resolved. | Set `--operator`, `AIDP_OPERATOR`, or run from a shell where `USER` is set. |
| `AIDPF-1030` | Bundle config | Active | `contentPack.profile` is missing. | Add the profile name under `contentPack.profile` in `bundle.yaml`. |
| `AIDPF-1031` | Bundle config | Active | `bundle.yaml` has no `contentPack` block. | Add a `contentPack` block; Phase 9 uses the content-pack path only. |
| `AIDPF-1032` | Resume | Removed | Resume was not supported under the content-pack backend in older phases. | Use current `--resume`; this code should not appear in current runs. |
| `AIDPF-1033` | Tenant profile | Active | Profile YAML was not found at the resolved path. | Create or point to the correct `profiles/<profile>.yaml`. |
| `AIDPF-1034` | Plan scope | Active | `--datasets` references a node id that is not in the resolved pack. | Fix the dataset/node id or validate the selected content pack. |
| `AIDPF-1036` | Pack validation | Active | Aggregate run-start content-pack validation failure. | Inspect the per-error report for specific `AIDPF-20xx`, `AIDPF-50xx`, `AIDPF-70xx`, or `AIDPF-80xx` codes. |
| `AIDPF-1037` | Pack resolution | Active | Installed content pack name was not found. | Check `contentPack.name` or install/use the intended pack. |
| `AIDPF-1038` | Pack resolution | Active | Resolved pack root exists but has no `pack.yaml`. | Fix the pack path or restore `pack.yaml`. |
| `AIDPF-1039` | Pack staging | Active | SQL path escapes the pack root; traversal was rejected. | Keep SQL paths inside the pack layer. |
| `AIDPF-1040` | Pack staging | Active | Staging provenance root is not present in `chain_roots`. | Treat as an internal staging/overlay consistency issue; validate the overlay chain. |
| `AIDPF-1041` | Pack execution | Historical | Rejected design option for fail-closed bronze precondition. | No current action; current runs use `AIDPF-2071` for bronze readiness. |
| `AIDPF-1042` | Plan scope | Active | `--strict-scope` found a transitive dependency outside the effective roots. | Add the dependency to the selected scope or disable strict scope for exploratory runs. |
| `AIDPF-1043` | Plan scope | Active | `--datasets` includes an id outside the bundle scope. | Add the dataset to the bundle scope or remove it from the CLI filter. |
| `AIDPF-1045` | Plan scope | Active | `--layers` removed every declared root, leaving an empty plan. | Choose layers that include at least one selected root. |
| `AIDPF-1044` | Resume | Active | Resume topology drift — the replayed plan's nodes/edges differ from the run manifest (a `dependsOn`/root edit between runs). | Start a fresh `--mode seed`; a resume cannot span a topology change. |
| `AIDPF-1046` | Resume | Active | Resume mode conflict — an explicit `--mode` conflicts with the manifest / inferred mode, OR the legacy execution history is MIXED (seed+incremental under one run_id). | Drop `--mode` to adopt the run's mode; a mixed legacy history is non-resumable — remediate with a fresh `--mode seed`. |
| `AIDPF-1047` | Resume | Active | Resume scope conflict — an explicit `--datasets`/`--layers`/`--strict-scope` does not match the run manifest's scope. | Drop the filters to resume the original scope, or start a fresh scoped run. |
| `AIDPF-1048` | Resume | Active | Resume identity/profile/exec-policy drift — endpoint/principal/schema/plugin-version, tenant profile, or `allowUnprovableCOA` changed since the manifest was written. | Apply the change via a fresh `--mode seed`, not a resume. **Exception (incremental-coa-chart-onboarding):** a profile change confined to a *proven additive* chart-of-accounts arm (a new `byChart` chart, existing charts byte-identical) is allowed to resume/incremental — only a non-COA change or a *mutating* COA change (existing arm moved/removed) still routes to seed. |
| `AIDPF-1049` | Resume | Active | Resume node-definition drift — a node's SQL/schema/refresh/`requiredColumns`/schemaOverride (its `sem`) or the pack fingerprint changed since the manifest was written, with topology unchanged. | Start a fresh `--mode seed`; a resume cannot mix old and new node definitions. |
| `AIDPF-1050` | Tenant profile | Active | Tenant profile YAML schema validation failed. | Fix the profile shape and field types. |
| `AIDPF-1051` | Tenant profile | Active | Tenant profile `schemaVersion` is unsupported. | Use a supported profile schema version. |
| `AIDPF-2000` | Pack loading | Active | Generic pack load/schema validation failure. | Read the attached message; fix malformed YAML, missing base packs, or schema violations. |
| `AIDPF-2001` | Overlay merge | Active | Orphan overlay override or overlay cycle. Also a malformed `replaceNode`: a bare same-id silver/gold file with no `replaceNode` block; a missing/blank `reason`; `replaceNode` combined with another override key; a non-`<layer>/<id>` or bronze-prefixed key; a missing `<layer>/<id>.yaml` replacement file; a non-shipped id; or a builtin/non-SQL target. | Override only existing base nodes/fields and remove cycles in `extends`. For `replaceNode`, declare a single layer-qualified silver/gold key with a `reason`, ship the matching `<layer>/<id>.yaml` + `.sql`, and target a shipped SQL mart. |
| `AIDPF-2002` | Pack schema | Active | Pack version is not valid SemVer. | Use a SemVer value such as `0.1.0`. |
| `AIDPF-2003` | Pack validation | Active | SQL file declared by a SQL node is missing. | Restore the SQL file or fix the node path. |
| `AIDPF-2004` | Overlay merge | Active | Overlay `extends` version does not match the base pack version contract. | Align the overlay's `extends` version with the base pack. |
| `AIDPF-2005` | Pack validation | Active | A real content-pack node id uses the reserved `__`-prefixed namespace (collides with synthetic state rows like `__run_manifest__` / `__coa_gate__`). | Rename the node so its id does not start with `__`. |
| `AIDPF-2010` | Variation points | Active | Required `columnAliases` variation point is unresolved. | Run bootstrap or add the resolved column alias in the tenant profile. |
| `AIDPF-2011` | Variation points | Active | Required `semanticVariants` variation point is unresolved. | Run bootstrap or add the resolved semantic variant in the tenant profile. |
| `AIDPF-2012` | Bootstrap / drift | Active | Bronze schema fingerprint diverged from the pinned profile. An ABSENT bronze table (never materialised) does not trigger this — it is tolerated with a WARN and baseline-filled from the pinned schema snapshot; only a PRESENT table's schema change (or a dataset added to / removed from the pack) drifts. | Run `bootstrap --refresh` after verifying the live Fusion/AIDP schema change. If the gate fails because a table is absent AND the snapshot is missing/desynced, `bootstrap --refresh` back-fills the snapshot without repinning. |
| `AIDPF-2013` | COA resolution | Active | A `semanticRole` chart-of-accounts mapping could not be resolved (bootstrap), OR — at seed/incremental time — `profile.chartOfAccounts` is MISSING/EMPTY/structurally INVALID while an in-scope node consumes a COA source (the pre-extraction structural COA gate; NOT `allowUnprovableCOA`-eligible). | Configure a complete `profile.chartOfAccounts` (flat `balancing/costCenter/naturalAccountSegment` or a nested `default`, optional numeric `byChart`); run/re-run bootstrap. |
| `AIDPF-2014` | COA resolution | Active | A known COA role is modeled as a bare column-existence alias (no `resolution: semanticRole`) — the existence-auto-match anti-pattern. | Declare the COA role alias with `resolution: semanticRole` instead of a bare column-existence candidate. |
| `AIDPF-2015` | COA resolution | Active | A COA role candidate / `chartOfAccounts` mapping names a column the `gl_coa` bronze `outputSchema` does not guarantee. | Bind the role to a column the `gl_coa` contract guarantees, or extend the `gl_coa` `outputSchema` (bronze type-overlay) to include it. |
| `AIDPF-2016` | COA resolution | Active | Two COA roles map to the same physical column within one chart's mapping. | Give each COA role a distinct `CodeCombinationSegment<N>` column. |
| `AIDPF-2017` | COA resolution | Active | The column bound as `naturalAccountSegment` does not classify into account types (strong, sample-backed contradiction) — likely the wrong segment. | Bind `naturalAccountSegment` to the correct segment; verify against live GL data. |
| `AIDPF-2018` | COA resolution | Active | Multiple active charts of accounts but only a singleton mapping and no operator acceptance / `byChart` — fails closed. | Provide a `byChart` mapping for each active chart, or explicitly accept the singleton mapping. |
| `AIDPF-2019` | COA resolution | Active | A COA role candidate is not a `CodeCombinationSegment<N>` with N in 1..30 (Fusion GL key-flexfield max). | Use a valid `CodeCombinationSegment1`..`CodeCombinationSegment30` column; fix the typo / non-segment name. |
| `AIDPF-2074` | COA gate | Active | The pre-extraction COA correctness PROBE could not EXECUTE (e.g. a constrained Spark session), so COA correctness is UNPROVEN. Blocks by default (correctness first). | Fix the session so probes can run, or set `contentPack.allowUnprovableCOA: true` to proceed with a logged WARN (correctness then rests on the per-node backstop). A real COA VIOLATION still hard-blocks regardless. |
| `AIDPF-2020` | Node strategy | Active | Merge strategy is missing a natural key. | Add `naturalKey` to the node strategy. |
| `AIDPF-2030` | Node schema | Active | Output schema column is missing PII classification. | Add `pii` classification to every output column. |
| `AIDPF-2040` | Pack DAG | Active | Content-pack dependency graph has a cycle. | Break the circular dependency. |
| `AIDPF-2041` | Pack DAG | Active | Content-pack node depends on an undeclared node. | Fix the dependency id or declare the missing node. |
| `AIDPF-2042` | Node preflight | Active | Required source column is missing. | Add/fix the source column, alias mapping, or required column declaration. |
| `AIDPF-2043` | Node preflight | Active | Watermark column is missing from the source schema. | Fix `watermarkColumn` or the source table/PVO. |
| `AIDPF-2044` | Node preflight | Active | Partition column is missing from the source schema. | Fix `partitionColumns` or the source table/PVO. |
| `AIDPF-2045` | Pack validation / column contract | Active | A silver/gold node demands a column missing from — or (for a pass-through column) type-incompatible with — an upstream node's declared `outputSchema`. Design-time, source-independent gate (no live PVO); complements the live AIDPF-4070/4071 gates. | Extend the upstream `outputSchema` to guarantee the column, or fix the consumer's `requiredColumns`. Run profile-aware with `content-pack validate --profile <p>` to also check `$column.*`/`$coa.*` demands. |
| `AIDPF-2046` | Node preflight | Active | A `$column.*` required-column reference cannot be resolved. | Define the referenced column alias in the tenant profile/bootstrap output. |
| `AIDPF-2047` | Cluster bootstrap | Active | Cluster bootstrap pre-dispatch gate failed. | Fix the reason shown: missing config, conflicting flags, or failed AIDP REST probe. |
| `AIDPF-2048` | Cluster bootstrap | Active | Cluster bootstrap dispatch failed before a valid marker was returned. | Open `.aidp/diagnostics/<run_id>/AIDPF-2048.json` and fix the dispatch failure. |
| `AIDPF-2049` | Cluster bootstrap | Active | Cluster bootstrap marker was invalid or missing. | Inspect `AIDPF-2049.json` and the companion `cluster_stdout.log`. |
| `AIDPF-2050` | Node strategy | Active | Merge strategy is missing a watermark. | Add the required watermark configuration. |
| `AIDPF-2051` | Node strategy | Active | Merge strategy has zero primary sources. | Mark one source as primary. |
| `AIDPF-2052` | Node strategy | Active | Merge strategy has multiple primary sources. | Keep exactly one primary source. |
| `AIDPF-2053` | Node strategy | Active | Merge with multiple bronze sources lacks source roles or a primary role. | Add source roles and identify the primary source. |
| `AIDPF-2054` | Node strategy | Active | Replace-partition strategy is missing partition columns. | Add `partitionColumns`. |
| `AIDPF-2055` | Node strategy | Active | Replace-partition strategy has multiple primary sources. | Keep exactly one primary source. |
| `AIDPF-2056` | Node strategy | Active | Append/unique strategy is missing a natural key. | Add the natural key. |
| `AIDPF-2057` | Node strategy | Active | `aggregate_merge` is deferred/not supported in this release. | Use a supported strategy or defer this node. |
| `AIDPF-2058` | Node strategy | Active | Snapshot strategy is missing a unique quality test. | Add the required uniqueness quality test. |
| `AIDPF-2059` | Node strategy | Active | SCD2 strategy is missing tracked columns. | Add tracked columns for SCD2 change detection. |
| `AIDPF-2060` | Node strategy | Retired | Retired `python_legacy` deprecated invariant. | No current action; Phase 9 deleted the legacy implementation type. |
| `AIDPF-2061` | Node strategy | Retired | Retired `python_legacy` callable-spec invariant. | No current action; Phase 9 deleted the legacy implementation type. |
| `AIDPF-2062` | Overlay merge | Active | A same-id bronze file drops a base `requiredColumns` entry. Same-id files are **add-only** for required columns (removal is a gate relaxation). | Restore the dropped column(s); to genuinely remove a required column use a `relaxRequiredColumns` block override (with a `reason`) in `pack.yaml`. |
| `AIDPF-2063` | Overlay merge | Active | A `relaxRequiredColumns` override names a column absent from the base `requiredColumns` for that source (orphan relaxation). | Fix the column name / source id, or drop the relax entry — you can only relax a column the base actually requires. |
| `AIDPF-2064` | Overlay merge | Active | A guarded same-id silver/gold `replaceNode` fork is **stale**: the base mart it forked from changed since the fingerprint was stamped. Two variants — base **logic** (`sqlSha256`: the base `.sql` or a referenced `{{ semantic.* }}` candidate fragment changed) or base **contract** (`contractSha256`: the base `outputSchema`/`pii`, `requiredColumns`, or `quality.tests` changed). | Re-review the base mart against your replacement, then re-stamp with `content-pack refresh-fork <overlay> [--node <layer>/<id>]`. |
| `AIDPF-2065` | Overlay merge | Active | A `replaceNode` replacement changes an **identity** field — `layer`, `target`, the `dependsOn` edge set, the `refresh` contract, or `implementation.type`. That is a re-contract, not a rewrite. | Keep identity equal to base; for an identity change create a new mart id instead. |
| `AIDPF-2071` | Runtime gate | Active | Bronze readiness gate failed for silver/gold execution. | Seed or repair the required bronze tables/columns, then rerun. |
| `AIDPF-2072` | Runtime gate | Active | Live Fusion PVO schema drifted from pack/profile expectations. | Review the diagnostic, refresh bootstrap evidence, or update the pack/profile. |
| `AIDPF-2080` | Pack validation | Warn-only | Bronze extract PVO is not in the curated catalog. | Verify it is an intentional custom PVO; the live drift gate catches real typos. |
| `AIDPF-2081` | Bundle validation | Active | Bundle dataset id does not resolve in any pack layer. | Fix the bundle dataset id or add the node to the pack. |
| `AIDPF-2082` | Pack validation | Active | A `naturalKey` / `partitionColumns` / `trackedColumns` / `watermark.column` name is not a safe unquoted SQL identifier (`^[A-Za-z_][A-Za-z0-9_]*$`). These names interpolate into MERGE / partition / watermark SQL. | Rename the offending column to a plain SQL identifier (no hyphens, dots, spaces, or punctuation). |
| `AIDPF-2083` | Pack validation | Active | A `CalendarProfile` `startDate`/`endDate` is not a valid ISO-8601 (`YYYY-MM-DD`) date. The value interpolates into the `dim_calendar` `sequence(DATE'...')` SQL. | Set the calendar dates to real `YYYY-MM-DD` values. |
| `AIDPF-2084` | Pack validation / declared inputs | Active | A silver/gold SQL node reads an upstream column not declared in its `requiredColumns` — including a `SELECT *` / `<alias>.*` wildcard from a declared upstream (unverifiable, fails closed). The declared-inputs companion to AIDPF-2045 (SQL reads ⊆ requiredColumns). | Declare the column in `requiredColumns[<source>]` (and add it to the upstream `outputSchema` if absent), or project explicit alias-qualified columns instead of `*`. |
| `AIDPF-2085` | Pack validation / declared inputs | Warn-only | A bare (unqualified) identifier in a block with an upstream source matches that upstream's `outputSchema`. Warn (not error) because a bare name may be CTE-derived. | Qualify the column with its table alias so the declared-inputs gate (AIDPF-2084) can verify it. |
| `AIDPF-2092` | Bronze runtime | Active | Bronze cursor exists but target table/state is inconsistent. | Repair the bronze target/state alignment before rerunning incremental extraction. |
| `AIDPF-3010` | Source preflight | Historical | Planned code for BICC PVO schema mismatch. | Run a metadata probe and update the pack/profile to match the live PVO. |
| `AIDPF-3020` | Custom extractors | Historical | Planned code for custom extractor load failure or invalid returned schema. | Check the extractor import path, signature, and required audit columns. |
| `AIDPF-4001` | Tenant drift | Historical | Planned code for tenant fingerprint change. | Confirm the tenant change and refresh bootstrap/profile evidence. |
| `AIDPF-4020` | Runtime preflight | Historical | Planned code for dropped target preflight failure. | Reseed or recreate the missing target. |
| `AIDPF-4021` | State init | Active | State-table location holds files but is not a valid Delta table and is unregistered (orphaned, non-adoptable). The valid-Delta case self-heals silently (adopt-in-place). | Inspect that one object-storage prefix; if it is leftover garbage from an aborted run, delete ONLY that prefix and re-run seed (`fusion_autopilot_state` is disposable run-audit history, not source data). |
| `AIDPF-4030` | Strategy execution | Active | Strategy is not supported by the current content-pack runner. | Change the node strategy or implement support. |
| `AIDPF-4031` | Strategy execution | Active | Target identifier failed the allowlist. | Use a valid three-part target identifier. |
| `AIDPF-4040` | Resume / incremental | Active | Plan-hash drift detected on resume or incremental continuity check. | Confirm the plan change; rerun seed or use the documented repin path only when intentional. **Exception (incremental-coa-chart-onboarding):** a per-node drift whose ENTIRE delta is a proven additive chart-of-accounts change is accepted and re-stamped automatically (a content-checked, fail-closed acceptance — NOT `--repin-plan-hash`), recording `coa_additive_accept_reason` on the node's success row. Any non-COA delta riding along still blocks. |
| `AIDPF-4022` | Run manifest | Active | The durable pre-execution run manifest failed to commit (fresh run), or a resume found the manifest row malformed / unknown-version / missing a required field. | Nothing was extracted on a commit failure — re-run `--mode seed`. A malformed manifest is non-resumable; start a fresh seed. |
| `AIDPF-4050` | Runtime locking | Historical | Planned code for cross-run lock held by another active run. | Wait for the holder to finish, or break the lock only after proving the holder is dead. |
| `AIDPF-4060` | State commit | Active | State-row hard commit failed. | Fix the Delta/state-table write failure before retrying. |
| `AIDPF-4061` | State commit | Active | Output watermark regressed. | Investigate source/order changes; do not advance state until monotonicity is restored. |
| `AIDPF-4070` | Runtime schema | Active | Materialized target schema does not match `node.outputSchema`. | Fix SQL casts/aliases or update the declared output schema. |
| `AIDPF-4071` | Runtime schema | Active | Bronze source column required by the pack is missing before ingest. | Fix the live PVO/source column or update the pack/profile. |
| `AIDPF-5001` | SQL renderer | Active | Identifier substitution failed the allowlist. | Fix catalog/schema/table/column identifiers. |
| `AIDPF-5002` | SQL renderer | Active | Unknown template token or variable. | Use a supported renderer token or declare the variable correctly. |
| `AIDPF-5003` | SQL renderer | Active | Variation point is unresolved or undeclared. | Resolve the column/semantic variation point through bootstrap/profile updates. |
| `AIDPF-5010` | SQL renderer | Active | Post-render SQL safety check rejected the SQL. | Remove rejected fragments such as unsafe comments or multiple statements. |
| `AIDPF-5011` | SQL renderer | Active | `{{ profile.<key> }}` resolved to a disallowed value type. | Use scalar profile values supported by the renderer. |
| `AIDPF-5013` | SQL renderer | Active | `profile.snapshotDate` is present but not an ISO-8601 date. | Use `YYYY-MM-DD` or leave the value absent/empty for `CURRENT_DATE()`. |
| `AIDPF-5014` | Builtin dispatch | Active | Builtin node `implementation.callable` is not in the registry. | Use a registered builtin callable id. |
| `AIDPF-6001` | Quality tests | Historical | Planned code for `reconcile_to` quality test failure. | Review source-vs-target aggregation and fix the reconciliation gap. |
| `AIDPF-6020` | Quality tests | Historical | Planned code for custom quality test load failure or invalid return shape. | Check the quality test import path, signature, and result contract. |
| `AIDPF-7001` | Dashboard validation | Active | Dashboard requires an undeclared/missing table or node. | Fix `requires.tables` / `requires.columns` to match pack gold nodes. |
| `AIDPF-7002` | Dashboard delivery | Historical | Planned code for `.bar` content referencing a column not provided by gold. | Re-author the workbook/snapshot against current gold or extend gold to provide the column. |
| `AIDPF-7003` | Dashboard validation | Active | Dashboard requirement type does not match the referenced pack object. | Fix dashboard metadata so table/column requirements match the pack. |
| `AIDPF-7004` | Dashboard validation | Active | Dashboard pack compatibility check failed. | Align `requires.pack.id`, `minVersion`, or `maxVersion` with the active pack. |
| `AIDPF-7005` | Dashboard validation | Active | `security.allowedColumns` contains columns not listed in `requires.columns`. | Make allowed columns a subset of required columns. |
| `AIDPF-8001` | Dashboard security | Historical | Planned code for high-PII column in dashboard validation queries. | Remove the high-PII column or change the dashboard contract. |
| `AIDPF-8002` | Dashboard security | Active | Dashboard exposes `pii: high` columns in requirements or allowed columns. | Remove high-PII columns or redesign the dashboard security model. |
| `AIDPF-8010` | Quality tests | Active | Quality test failed. | Inspect the failed quality rule and correct data or node logic. |
| `AIDPF-8011` | Quality tests | Active | Quality test is deferred or unsupported. | Implement the quality rule or accept the deferred status intentionally. |
| `AIDPF-9999` | Diagnostics | Test-only | Intentionally invalid/unknown code used by tests. | If seen outside tests, treat it as malformed diagnostic data. |

## Related Non-AIDPF Codes

Dispatch-layer codes such as `DISPATCH_*` are not `AIDPF` codes. They describe
transport or notebook-dispatch failures and may be wrapped by `AIDPF-2048` or
`AIDPF-2049` during cluster bootstrap flows.

## Exit Codes

| Exit code | Related code | Meaning |
|---|---|---|
| `14` | `AIDPF-2012` | Reserved schema-drift exit for active bootstrap/profile fingerprint drift. |
