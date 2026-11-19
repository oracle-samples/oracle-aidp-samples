---
description: Scan a Fabric Git export (read-only) into a migration manifest.
---

Run the read-only inventory over a Fabric workspace export.

```bash
fabric-aidp inventory <export-dir> -o inv.json
fabric-aidp inventory --fixture demo -o inv.json    # no Fabric tenant needed
```

`<export-dir>` is the folder the customer's Fabric workspace is Git-synced to — the
one containing `<name>.Notebook/`, `<name>.Warehouse/` directories. No Azure
credentials are needed or used.

`inventory` writes the manifest and stops. It is one verb of five, not the whole
pipeline: `plan`, `migrate` and `verify` still have to be run, and `--fixture demo`
inventories the bundled estate rather than migrating it.

**Dataflows need Node.** If the dataflow line reads `parser=unavailable` and
`translatable_count=None`, the Power Query parser is not installed: run
`cd fabric_aidp/mparse && npm install` (Node 18+) and re-run. `None` means
*unknown*, not zero — the Dataflows were counted, not dropped — and without the
parser `migrate` will translate none of them.

Report back: the per-source counts, the catalog tier breakdown, and — importantly —
any lakehouse whose shortcut tracking reads `not_tracked`. That means Fabric was not
recording shortcuts for it, so the estate may contain shortcuts this scan cannot see.
Say so rather than reporting zero.
