# fix-supplier-id-types-file — bronze type-overlay (same-id file mechanism)

The **same fix** as `fix-supplier-id-types-block/` (retype the four supplier-ID
columns `decimal(38,30)` → `decimal(18,0)` on `bronze.erp_suppliers`), authored
as a **full-node replacement file** `bronze/erp_suppliers.yaml` instead of a
`pack.yaml` `overrides:` block. Both mechanisms produce an identical merged node.

Use the file form when you prefer authoring the whole node; use the block form
for a small retype.

## Apply

```bash
aidp-fusion-autopilot content-pack validate overlays/fix-supplier-id-types-file
aidp-fusion-autopilot use-pack overlays/fix-supplier-id-types-file --profile <profile> --no-align
aidp-fusion-autopilot validate
aidp-fusion-autopilot run --mode seed --datasets erp_suppliers --layers bronze
# then verify VENDORID non-null + join overlap before rebuilding the marts.
```

## Guards this file is subject to (try them)

The same-id file is **diff-guarded** against the base node — it can only retype
or additively extend, never repoint or narrow. Each of these edits fails closed
(AIDPF-2001) at `content-pack validate` time:

- Rename the file (stem must equal `id: erp_suppliers`).
- Change `implementation.datastore` / `pvo_id`, `target`, `refresh`,
  `refresh.incremental.naturalKey`, or `requiredColumns` (identity → new node id).
- Change `implementation.biccSchema` (any non-`outputSchema`/`quality.tests`
  field — whitelist diff).
- Drop a base `outputSchema` column (incl. an `_`-audit column) — no contract
  narrowing.
- Drop a base `quality.tests` entry — extend only.

A silver/gold same-id file is rejected outright (use `overrides: { sql }` or a new
mart id). Declaring both this file and a `pack.yaml` override for the node errors.
