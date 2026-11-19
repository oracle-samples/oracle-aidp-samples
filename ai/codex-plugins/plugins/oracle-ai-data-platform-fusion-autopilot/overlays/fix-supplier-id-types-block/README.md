# fix-supplier-id-types-block — bronze type-overlay (block mechanism)

Fixes the supplier-ID overflow on `bronze.erp_suppliers`: the starter pack
declares `VENDORID` / `PARTYID` / `PARENTVENDORID` / `PARENTPARTYID` as
`decimal(38,30)` (8 integer digits), so 15-digit Fusion surrogate IDs overflow to
`NULL` on extract and the `supplier_spend` join matches nothing. This overlay
retypes the four IDs to `decimal(18,0)` via an `overrides:` block — declaring
**only** the changed columns; everything else is inherited from the base node.

This is the preferred mechanism for a small retype. (The sibling
`fix-supplier-id-types-file/` shows the equivalent same-id full-file form.)

## Apply

```bash
# 1. Validate the overlay against the base pack.
aidp-fusion-autopilot content-pack validate overlays/fix-supplier-id-types-block

# 2. Wire it into the bundle. --no-align: a narrow bronze fix should not expand
#    the bundle's mart selection.
aidp-fusion-autopilot use-pack overlays/fix-supplier-id-types-block --profile <profile> --no-align
aidp-fusion-autopilot validate

# 3. Re-seed ONLY the bronze node (metadata-only preflight surfaces a real PVO
#    type mismatch in seconds, before any long pull).
aidp-fusion-autopilot run --mode seed --datasets erp_suppliers --layers bronze

# 4. Verify VENDORID is non-null and the supplier join overlaps BEFORE rebuilding
#    dim_supplier / supplier_spend.
```

## Notes

- The retype shifts the node's output-schema hash → re-materialization is
  triggered for `erp_suppliers` and its downstream.
- Identity fields (grain / naturalKey / target / PVO / refresh / requiredColumns)
  are **not** changeable via overlay — a structural change is a new node id.
- Mutually exclusive with a same-id file for the same node (declaring both errors).
