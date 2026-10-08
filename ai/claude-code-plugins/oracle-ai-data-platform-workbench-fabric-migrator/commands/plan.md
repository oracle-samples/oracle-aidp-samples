---
description: Turn an inventory manifest into an AIDP mapping plan.
---

```bash
fabric-aidp plan inv.json -o plan.json --namespace <your-oci-namespace> \
    --catalog <your-aidp-catalog>
```

The plan lists one asset per migratable **object** — not per Fabric item. One
Warehouse becomes a table, view or procedure asset each, and one Dataflow becomes
one asset per query, so the asset count is normally well above the item count.
Each asset carries its AIDP target type and its dependencies, ordered so a producer
precedes its consumers.

`--namespace` and `--catalog` are different things:

- `--namespace` is the OCI object-storage namespace. It appears only in generated
  `oci://` paths, never in a table name. Leave it out and the placeholder
  `<your-oci-namespace>` is written instead; every artifact carrying it is flagged
  `NS01_NAMESPACE_PLACEHOLDER` and graded **REVIEW**, never PASS. Good for review,
  wrong to run.
- `--catalog` is the AIDP catalog table names are built under — the first part of
  `<catalog>.<item>.<table>`. It defaults to `default` and must be a single
  identifier.

`--lakehouses <csv>` is a third, and unrelated to both: a `id,name` map from a
lakehouse GUID to its display name. A Dataflow navigates by `lakehouseId`, a
workspace item id, and a Fabric Git export writes that down in exactly one place —
a notebook bound to the lakehouse. With no such notebook the name cannot be
recovered, and the read or write is flagged for a human as a two-part name that
lands in whatever database the session points at, even though the *table* resolved
cleanly. If the user sees `M10_SOURCE_LAKEHOUSE` or `M12_DESTINATION` flagged with
"is not named anywhere in this export", this flag is the answer: they know their
tenant, the export does not.

Report back the asset count by target type. If the summary names any dangling
dependency or dependency cycle, surface it — a cycle is not an error, but it means
two notebooks reference each other and a human should decide the run order. If the
summary warns about a missing `--namespace`, say so before reporting any count: the
whole run will grade REVIEW for a reason that has nothing to do with the code.
