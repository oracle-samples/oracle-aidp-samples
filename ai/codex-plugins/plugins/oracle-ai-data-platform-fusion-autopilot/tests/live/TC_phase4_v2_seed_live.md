# TC Phase 4 — v2 content-pack seed live evidence

> Captured 2026-06-07 against a real Fusion ERP demo tenant via the
> Phase 4.1 cluster-side bootstrap dispatcher + the parametrized
> v2-seed dispatcher at `tests/live/dispatch_v2_seed.py`. Closes the
> live-evidence row 16 of `docs/v2-phase-4-shipready-report.md` that
> the four D1–D4 defects had been blocking.
>
> Sensitive identifiers (OCIDs, workspace UUIDs, cluster UUIDs, AIDP
> job/run UUIDs, BICC usernames, pod URLs, storage profile names) are
> REDACTED below per the TC29 convention. The raw marker payloads
> stay local (`tests/live/phase4_dispatch_seed_*_1780797067.json` is
> gitignored from this commit).

## Tenant context

| Field | Value |
|---|---|
| Region | `us-ashburn-1` |
| AIDP id | `<REDACTED-AIDP-OCID>` (display name: <aidp-instance>) |
| Workspace | `<REDACTED-WORKSPACE-UUID>` (display name: playground) |
| Cluster name | `fusion_autopilot_dev` |
| Fusion pod | `<REDACTED-POD-URL>` |
| BICC user | `<REDACTED-BICC-USER>` |
| Content pack | `fusion-finance-starter` v0.3 |
| Profile | `finance-default` |

## Bootstrap (Phase 4.1 cluster dispatcher) — fresh resolve

```
.venv/bin/aidp-fusion-autopilot \
  --bundle dev/fusion-finance-starter.live.yaml \
  --config dev/aidp.config.yaml \
  --env dev \
  bootstrap --non-interactive
```

```
                                bootstrap probes
| probe            | status | detail                                           |
|------------------|--------|--------------------------------------------------|
| bundle.yaml      | PASS   | 4 datasets                                       |
| aidp.config.yaml | PASS   | environments: ['dev']                            |
| bicc-auth        | SKIP   | FUSION_BICC_USER / FUSION_BICC_PASSWORD env vars |
|                  |        | not set                                          |
| aidp-rest        | PASS   | workspace <REDACTED-WORKSPACE-UUID> reachable    |
|                  |        | in us-ashburn-1                                  |
```

```
[02:11:24]  wheel cache miss (hash=74306ec9496c1cb9); running `python -m build`
[02:11:30]  notebook_uploaded {'path': '/Workspace/Shared/fusion-autopilot-bootstrap/probe-finance_default-<TS>.ipynb'}
[02:11:34]  job_created   jobKey=<REDACTED-JOB-UUID>
[02:11:35]  jobRun_submitted   jobRunKey=<REDACTED-RUN-UUID>
[02:11:36]  poll status=RUNNING
[02:12:20]  poll status=SUCCESS
bootstrap variation phase complete — profile dev/profiles/finance-default.yaml,
evidence dev/evidence/finance-default/2026-06-07T01-12-32.867492+00-00.yaml.
```

**Cluster wall-time**: 44s (job submit → SUCCESS).
**Bronze fingerprint pinned**: `sha256:ef25f8b89e1f9d7910eeb372d0ba98c8926e37c395ce9f21935eb508e623685f`.

### Auto-resolved variation points

```yaml
resolved:
  column:
    coa_balancing_segment: CodeCombinationSegment1
    coa_cost_center_segment: CodeCombinationSegment2
    coa_natural_account_segment: CodeCombinationSegment3
    invoice_currency_code: ApInvoicesInvoiceCurrencyCode
    supplier_natural_key: SEGMENT1
    vendor_id: VENDORID
  semantic:
    cancelled_status: cancelled_date
```

Six variation points, all auto-resolved (zero multi-match). Validates
Phase 4.1's marker round-trip + the laptop-side resolver's
auto-resolve path end-to-end.

## v2 seed dispatch — 6/6 GREEN

```
.venv/bin/python tests/live/dispatch_v2_seed.py \
  --region us-ashburn-1 \
  --aidp-id <REDACTED-AIDP-OCID> \
  --workspace-key <REDACTED-WORKSPACE-UUID> \
  --cluster-key <REDACTED-CLUSTER-UUID> \
  --cluster-name fusion_autopilot_dev \
  --bundle dev/fusion-finance-starter.live.yaml \
  --profile finance-default \
  --layers silver,gold
```

| Node | Layer | Status | Rows | Duration |
|---|---|---|---|---|
| `dim_account` | silver | ✅ success | 63,464 | 54.2s |
| `dim_calendar` | silver | ✅ success | 4,018 | 27.8s |
| `dim_supplier` | silver | ✅ success | 209 | 45.1s |
| `ap_aging` | gold | ✅ success | 131 | 40.1s |
| `gl_balance` | gold | ✅ success | **10,184,102** | 75.7s |
| `supplier_spend` | gold | ✅ success | 309 | 28.7s |

```
succeeded: 6
failed:    0
skipped:   0
wall:      340.7s (cluster total_duration: 271.7s)
run_id:    cp-20260607014458-<REDACTED-RUN-SUFFIX>
job_key:   <REDACTED-JOB-UUID>
run_key:   <REDACTED-RUN-UUID>
```

## Defect-fix evidence map

This run validates the full D1–D5 chain against a real tenant:

| Defect | Closed in | Live evidence (this run) |
|---|---|---|
| **D1 Layer A** — case-insensitive preflight | commit `d71c3a5` | `dim_supplier` reads live `Segment1` / `VendorId` (PascalCase) against pack-declared `SEGMENT1` / `VENDORID` candidates. 209 rows. |
| **D1 Layer B** — `$column.<key>` grammar + `dim_supplier.sql` templating | commit `d71c3a5` | `silver/dim_supplier.sql`'s 5 hardcoded literals are now `{{ column.supplier_natural_key }}` / `{{ column.vendor_id }}`. Live render produced the same `SEGMENT1` / `VENDORID` output, no AIDPF-2042. |
| **D2** — `dim_account.outputSchema` date vs timestamp | commit `d71c3a5` | `dim_account` materialised with `start_date_active` / `end_date_active` as `date` (matches the declared type). 63,464 rows, no AIDPF-4070. |
| **D3 / Phase 4.1** — cluster-side bootstrap dispatcher | commit `d71c3a5` | Bootstrap above: wheel build → notebook upload → submit → poll → marker parse, all green. Profile + evidence + snapshot written locally. |
| **D4** — canonical AIDP REST endpoint | commit `d71c3a5` | Bootstrap probe table: `aidp-rest PASS workspace <…> reachable in us-ashburn-1`. No SSL hostname mismatch. |
| **D5** — gold-layer decimal-precision widening (NEW, this morning) | this commit | All 7 decimal aggregates across `ap_aging` / `gl_balance` / `supplier_spend` now produce exactly the declared `DECIMAL(28,2)` / `DECIMAL(20,2)`. No AIDPF-4070. |

## D5 — gold-layer decimal-precision widening

**Surfaced**: only after D1+D2 unblocked the cascade. Previously the
3 gold nodes were `cascade-skipped`, so this defect class was masked.

**Root cause**: Spark's decimal arithmetic widens precision automatically:

* `SUM(DECIMAL(28,2))` → `DECIMAL(38,2)` (Spark's max precision cap)
* `DECIMAL(28,2) - DECIMAL(28,2)` → `DECIMAL(29,2)` (precision+1)
* `DECIMAL(28,2) - DECIMAL(28,2) + DECIMAL(28,2) - DECIMAL(28,2)` → `DECIMAL(32,2)`

The pack-declared `outputSchema` widths come from the v1 cents-precision
contract (per `ap_aging.yaml:47-52` "decimal(28,2) widths per
ap_aging.py:463-490"). Dashboards rebind to these declarations.
Widening the YAML would break the consumer contract.

**Fix**: outer `CAST(... AS DECIMAL(28,2))` (or `DECIMAL(20,2)` for
`supplier_spend`) on each affected aggregate. Locks SQL output to
the declared precision while preserving the underlying arithmetic.

| Node | Column | Spark-widened to | Locked back to |
|---|---|---|---|
| `ap_aging` | `open_amount` | `decimal(38,2)` | `decimal(28,2)` |
| `ap_aging` | `invoice_amount_total` | `decimal(38,2)` | `decimal(28,2)` |
| `ap_aging` | `amount_paid_total` | `decimal(38,2)` | `decimal(28,2)` |
| `ap_aging` | `credit_open_amount` | `decimal(38,2)` | `decimal(28,2)` |
| `gl_balance` | `closing_balance` | `decimal(32,2)` | `decimal(28,2)` |
| `supplier_spend` | `total_invoice_amount` | `decimal(31,2)` | `decimal(20,2)` |
| `supplier_spend` | `total_paid` | `decimal(31,2)` | `decimal(20,2)` |

**Files changed**: 3 SQL files in `content_packs/fusion-finance-starter/gold/`.
No YAML changes (consumer contract preserved).

## Operator notes

* Bootstrap took ~30s wheel build + ~44s cluster wall on a fresh
  invalidation; cached wheel cuts ~30s off subsequent runs.
* v2 seed total wall: 5.7 min (cluster total_duration 4.5 min — the
  ~1 min delta is laptop-side: REST round-trip, marker decode,
  result-row writes).
* `gl_balance` row count (10.18M) is the largest live workload v2
  has run end-to-end. No memory / partition issues observed at this
  scale.

## What's still local-only

* `dev/profiles/finance-default.yaml` (real bronze fingerprint) +
  `dev/profiles/finance-default.schema-snapshot.yaml` — `dev/` is
  gitignored.
* The marker JSONs at `tests/live/phase4_dispatch_seed_*_1780797067.json`
  — operator stash; carry raw UUIDs. Redact before any future commit.
