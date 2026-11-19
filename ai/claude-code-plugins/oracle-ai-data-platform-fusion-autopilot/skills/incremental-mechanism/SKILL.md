---
name: incremental-mechanism
description: "Choose and verify the safest low-cost refresh mechanism for an Oracle Fusion AIDP bronze, silver, or gold node. Use when asked whether a node can be incremental, why a node re-extracts or rebuilds in incremental mode, how to reduce refresh cost, or when authoring a new node needs a refresh-strategy decision. Inspects the node grain, sources, watermark, and merge keys; live-probes whether BICC honors the source PVO lineage cursor; then recommends native BICC incremental, a verified LUD delta, a period window, or replace. Does not seed, query business payloads for analysis, or change grain/keys."
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# incremental-mechanism

Choose the cheapest refresh mechanism that is still correct for the node.
Refresh cost matters, but a fast strategy that can silently miss financial
changes is not acceptable.

This skill is for refresh-strategy decisions only. It inspects the existing
content-pack node, verifies source behavior against the live tenant, recommends
a mechanism, and changes pack metadata only after the user confirms. It never
changes a node's business grain, natural key, or output contract.

## When to use

- "Can this node be incremental?"
- "Why does this table full-extract or rebuild every incremental run?"
- "Make this mart cheaper to refresh."
- A new bronze source or mart needs an evidence-backed refresh strategy.
- A cost review identifies a node whose incremental run cost is close to a seed.

## When not to use

- The request is to run a seed or incremental pipeline. Use the run-oriented
  seed/incremental skills or the CLI.
- A drift gate or missing source column failed. Route to the drift/authoring
  workflow first, then return here only if the refresh strategy itself is in
  question.
- The user wants a new mart or dimension designed from scratch. Author the node
  first, then use this skill to verify the refresh mechanism.

## Mechanism ladder

Pick the highest rung supported by both live evidence and source semantics.

| Rung | Mechanism | Use when |
|---|---|---|
| 1 | Native BICC incremental (`incrementalCapable: true`) | The live probe shows BICC honors `fusion.initial.extract-date`, and the source has a reliable lineage timestamp for every meaningful change. Best fit for transaction and master-data PVOs. |
| 2 | LUD datastore filter | Native BICC cursoring is unavailable, but a live-verified last-update-date column is trustworthy as a row-level change cursor. Treat as opt-in until verified on the customer's tenant. |
| 3 | Period-window datastore filter | The source is a high-volume period snapshot where row-level cursor trust is not proven, but a period column can safely limit each incremental run to recent open periods plus periodic full reseeds. |
| 4 | Replace | No reliable change signal exists, or the node is intentionally rebuilt because it is small, date-anchored, or aggregate-shaped in a way that makes partial merge unsafe. |

## Correctness rule

"BICC honors the cursor" only proves that BICC applies the cursor. It does not
prove that every business-significant change advances the cursor.

- For transaction and master-data PVOs, a live cursor-honor result plus a real
  last-update timestamp is usually sufficient for native BICC incremental.
- For snapshot or aggregate cubes, cursor honor is necessary but not sufficient.
  Back-postings, revaluations, translations, and period-close adjustments can
  affect old periods. Prefer a period-window strategy unless row-level cursor
  reliability has been proven on the tenant over real changes.

## Workflow

### 1. Inspect the node

Read the active content-pack YAML and SQL for the node. Capture:

- layer and node id;
- bronze PVOs or upstream tables;
- grain and natural key;
- amount columns and whether currency is in the grain;
- current `refresh.seed` and `refresh.incremental` strategies;
- current watermark column, if declared;
- whether the node is in the shipped pack or a customer overlay.

For silver and gold nodes, trace the decision down to the bronze source PVOs.
The cheapest safe mechanism is usually determined at the extract boundary.

### 2. Classify the source

Classify each relevant bronze source as one of:

- transaction or event stream;
- master data;
- period snapshot or aggregate cube;
- small configuration/reference data;
- date-anchored or replace-only derived node.

This classification is a hypothesis. Do not set `incrementalCapable` from class
alone; verify live behavior.

### 3. Probe BICC cursor behavior

Run the helper against the live source PVO:

```bash
python3 skills/incremental-mechanism/probe_incremental_honor.py \
  --datastore <Full.AM.Hierarchy.ExtractPVO> \
  --label <node-id> \
  --service-url "$AIDP_FUSION_SERVICE_URL" \
  --username "$FUSION_BICC_USER" \
  --external-storage "$AIDP_FUSION_EXTERNAL_STORAGE"
```

The helper performs one full extract count, one recent-watermark count, and one
mid-watermark count. It emits:

- `HONORED`: BICC appears to apply `fusion.initial.extract-date`.
- `IGNORED`: BICC appears to return the full dataset despite the cursor.
- `AMBIGUOUS`: counts do not prove either behavior; adjust watermarks and rerun.

The probe is for metadata and row counts. It does not write lakehouse tables and
does not prove business correctness by itself.

### 4. Choose the mechanism

Use the probe result, source class, table size, and correctness rule:

- `HONORED` plus transaction/master-data semantics -> rung 1.
- `HONORED` plus snapshot/aggregate semantics -> usually rung 3 unless row-level
  cursor reliability is proven for that tenant.
- `IGNORED` plus trustworthy LUD column -> rung 2.
- `IGNORED` plus period snapshot -> rung 3.
- No trustworthy delta/window signal -> rung 4.

Present the recommendation with the evidence: full count, cursor counts, source
class, chosen mechanism, and the reason lower-cost rungs are or are not safe.

### 5. Confirm before changing files

Before editing a pack or catalog file, show the proposed change and get explicit
user confirmation. Then apply the smallest appropriate change:

- Shipped-pack universal behavior: update the maintained source of truth and the
  shipped bronze YAML together. Include focused unit tests and live evidence.
- Customer-specific behavior: author an overlay pack beside the bundle. Do not
  edit the installed shipped pack for tenant-only decisions.
- Period-window behavior: only author an `extractWindow` policy when the runtime
  supports it and the lease/filter lifecycle has been implemented. Otherwise,
  document the recommendation and stop.
- Replace behavior: leave or set replace with a clear reason; do not fake
  incrementality with an unsafe watermark.

Validate any authored pack change:

```bash
aidp-fusion-autopilot content-pack validate <pack-or-overlay>
```

### 6. Require end-to-end evidence

A metadata flip is not done until a seed-to-incremental cycle proves the
runtime path end to end:

- seed or reseed the affected scope;
- run incremental over the same scope;
- confirm row counts, MERGE behavior, downstream correctness, and state rows;
- record the live evidence in the repository's normal test-evidence location.

## Safety invariants

- Verify live behavior; do not guess from PVO naming or class.
- Correctness beats cost.
- Do not change grain, natural key, or output schema as part of refresh tuning.
- Do not edit shipped content for tenant-only decisions; use an overlay.
- Do not bypass drift gates to make a refresh strategy appear to work.
- Treat snapshot/aggregate cursoring as opt-in until proven over real changes.
- Require live end-to-end evidence before calling the optimization complete.
