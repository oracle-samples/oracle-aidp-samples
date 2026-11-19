---
name: oac-dataset-advisor
description: "Turn a natural-language dashboard request into a concrete OAC dataset plan, grounded in the LIVE AIDP gold layer. Inspects the actual materialized Delta tables in the AIDP catalog (evidence — never the content-pack YAMLs), checks what OAC datasets already exist via OAC MCP, then tells the operator exactly which OAC dataset to create (which AIDP gold table(s), which columns, which join key) — or that an existing dataset can be reused, or that the current gold layer CANNOT serve the request and a new mart is needed. Use when the user says 'I want a dashboard with X and Y', 'what dataset should I create in OAC', 'can my gold layer serve this dashboard', 'do I have the data for this'. Does NOT create datasets/marts — it advises."
allowed-tools: Read, Bash, Glob, Grep, mcp__oac-mcp-server__oracle_analytics-search_catalog, mcp__oac-mcp-server__oracle_analytics-find_matching_datasources, mcp__oac-mcp-server__oracle_analytics-describe_data
---

# oac-dataset-advisor — from dashboard intent to a grounded OAC dataset plan

A planning skill. Given "I want a dashboard with supplier spend and GL balance",
it answers three questions, in order:

1. **What gold data actually exists on this tenant?** — by inspecting the
   **live AIDP catalog**, not by reading pack declarations.
2. **Does an OAC dataset for it already exist?** — via OAC MCP.
3. **What should the operator do?** — reuse an existing dataset, create a new
   OAC dataset over specific gold table(s), or (if gold can't serve it) author
   a new mart first.

It **advises only** — it never creates datasets or marts. Dataset creation is
an OAC action over the AIDP connection (the OAC MCP server has no
create-dataset tool); hand CREATE recommendations to `/oac-dataset-setup`.
New marts are the job of the mart-authoring skill.

## Evidence discipline (the load-bearing rule)

**The evidence is the LIVE AIDP catalog** — the Delta tables actually
materialized in `<catalog>.<goldSchema>` on this tenant — captured at advise
time. **Never substitute the content-pack `gold/*.yaml` `outputSchema`
declarations.** Those are *design-time intent*: they describe what the pack
*would* build, not what exists or what the real columns are on this pod. Using
them would let the advisor recommend a dataset over a table that was never
seeded, or with columns that don't match. (Same rule as CLAUDE.md "live
evidence is required for any plugin-portability claim.") Pack metadata such as
PII tags may be layered on as an **advisory overlay**, never as the evidence.

## When to use

- "I want a dashboard showing <A> and <B>" / "build me a CFO view of <…>".
- "What OAC dataset do I need for <dashboard>?" / "which gold tables back this?"
- "Can my current gold layer serve <dashboard>?" / "do I have the data?"
- Before `workbook-authoring`: to decide which dataset the workbook will bind to.

## When NOT to use

- **Authoring the workbook itself** → use `workbook-authoring` (after the
  dataset exists).
- **Creating a new gold mart** (new YAML + SQL) → the mart-authoring skill.
- **Running the pipeline / materializing gold** → `aidp-fusion-autopilot run --mode seed`
  (or the `aidp-fusion-seed` skill).

## Helper

| File | Role | Invoked via |
|---|---|---|
| `../../tests/live/aidp_catalog_probe_live.py` | Produces the live listing: dispatches a `SHOW TABLES`/`DESCRIBE` notebook to an ACTIVE cluster (the only way — no control-plane metastore REST). | `Bash`, writes live.json |
| `catalog_inventory.py` | Structures the **live** AIDP catalog listing (tables→columns) into an inventory + join-key candidates. **Evidence of what EXISTS.** Refuses to read pack YAMLs. | `Bash`, JSON in/out |
| `pack_capability.py` | Reads the content pack's gold/silver YAMLs → the **buildable menu** (what could be materialized if seeded). **NOT evidence of existence.** Used only to route seed-vs-gap when live is empty/insufficient. | `Bash`, JSON out |

**Two questions, two sources — keep them distinct:** the live catalog answers
"what *exists*" (the only thing an OAC dataset can bind to); the content pack
answers "what *could be built*". Never let the pack stand in for live evidence.

---

## Workflow

### 1 — Parse the dashboard intent

Extract: the metrics (measures), the dimensions/grain to slice by, any time
grain, filters, and the business concepts named (e.g. "supplier spend",
"GL balance", "by currency"). Note when the request implies **combining** two
subjects (→ a join/blend is needed).

### 2 — Inspect the LIVE AIDP gold layer (EVIDENCE)

List the gold tables that actually exist and their real columns — do NOT read
the pack YAMLs.

> **The AIDP metastore is Spark-side, not on the control plane.** A control-plane
> `GET .../catalogs` returns **404 `NotAuthorizedOrNotFound`** (verified live
> 2026-06-15) — there is **no** REST metastore browse. The listing MUST come
> from **SQL run on an ACTIVE cluster** (`SHOW TABLES` / `DESCRIBE`).

**Proven method — dispatch a tiny catalog-probe notebook** (no wheel, reuses
the bundle's `AidpRestClient`):

```bash
.venv/bin/python tests/live/aidp_catalog_probe_live.py \
  --aidp-id <OCID> --workspace-key <key> --cluster-key <key> \
  --cluster-name <name> --schema gold --out live.json
```

It runs `SHOW TABLES IN <catalog>.gold` + `DESCRIBE TABLE` for each on the
cluster and writes the live-catalog JSON directly. Coords come from
`aidp.config.yaml` / the gitignored `.env` (`AIDP_DATALAKE_OCID`,
`AIDP_WORKSPACE_KEY`, `AIDP_CLUSTER_ID`, …); the cluster must be ACTIVE
(start it first if STOPPED). Resolve `<catalog>`/`<goldSchema>` from
`bundle.yaml`'s `aidp.catalog` / `aidp.goldSchema`.

*Alternative:* the workbench `aidp-catalog-explore` / `aidp-analyzing-data`
skills run the same `SHOW TABLES`/`DESCRIBE` if that plugin is installed.

Then structure the listing:

```bash
# live.json = {"catalog":"fusion_catalog","schema":"gold",
#              "tables":{"gl_balance":[{"name","type"}...], ...}}
python3 catalog_inventory.py --input live.json
```

`catalog_inventory.py` returns the per-table columns plus
`joinKeyCandidates` (non-audit columns shared across ≥2 tables — the keys a
multi-table dataset can join on, e.g. `currency_code`).

- **Empty gold schema (`tableCount: 0`)** → nothing is materialized. **Do NOT
  dead-end on "seed" yet, and do NOT proceed off pack declarations as if they
  were live.** Go to step 2a to decide whether seeding would even help.

### 2a — If live gold is empty or doesn't cover: consult the buildable menu

When the live catalog has **no** gold tables, or has some but they **lack** a
needed metric/dimension, do not jump to "seed" and do not invent tables.
Consult what the pack **could build** if seeded:

```bash
python3 pack_capability.py            # buildable marts (design-time, NOT live)
```

Compare the pack's `buildableMarts` columns against the dashboard intent:

- **Pack CAN serve it** (a declared mart / column set covers the want) → the
  data just isn't materialized yet. Tell the operator to **run
  `aidp-fusion-autopilot run --mode seed`** (or `/aidp-fusion-seed`) to build it,
  then re-run this advisor. (Name which marts seeding will produce.)
- **Pack CANNOT serve it** (even the declared marts lack the needed
  metric/dimension/grain) → this is a **GAP**. Tell the operator plainly:
  *"There are no live tables in AIDP, so no OAC dataset can be created — and the
  content pack you have will not serve this either; you need column(s)/metric(s)
  `<name the missing ones>`."* Then hand off to the **mart-authoring skill**
  (next in the family): it takes the business logic and authors a new mart
  (or an additive column) — inspecting the **Fusion PVO source schema** (not
  bronze) to see what raw fields exist, and choosing the **lowest-cost,
  additive, non-destructive** change (new `.yaml`+`.sql`, or a new column) that
  never reprocesses already-materialized terabyte-scale bronze/silver.

Keep the distinction explicit in the message: live = what exists; pack = what's
buildable. "Pack covers it" never means the table exists — it means seeding
would create it.

### 3 — Map intent → live tables (coverage analysis)

When live gold tables DO exist, decide from the **live** columns which table(s)
supply each requested metric and dimension. Three outcomes:

- **COVERED — single table:** one live gold table has every needed column →
  recommend a 1-table OAC dataset.
- **COVERED — multi-table:** the metrics/dimensions span 2+ live tables that
  share a `joinKeyCandidate` → recommend one OAC dataset combining those tables
  joined on that key (e.g. `gl_balance` + `supplier_spend` on `currency_code`).
  Name the exact columns to include from each.
- **GAP — not covered:** the live gold layer is missing a needed metric,
  dimension, or grain (no table has it; no usable join key; wrong grain).
  **First run step 2a** — does the pack's buildable menu cover it? If yes →
  "seed" (it's just not materialized). If even the pack can't → it's a true
  gap: say so explicitly, name precisely what's missing, and hand off to the
  mart-authoring skill. Do not force a recommendation onto tables that don't fit.

### 4 — OAC pre-check (don't duplicate an existing dataset)

Before recommending creation, check OAC via MCP:

- `oracle_analytics-search_catalog` `{type: "datasets", search: "*"}` (and
  `find_matching_datasources` for an intent-ranked shortlist) to find datasets
  that already point at the recommended gold table(s).
- `oracle_analytics-describe_data` on a candidate to confirm it carries the
  needed columns.

Then:
- **Suitable dataset exists** → recommend **REUSE** (give its catalog path /
  `xsaExpr`); do not create a duplicate.
- **Partial** → note what it's missing vs. the recommendation.
- **None** → proceed to the create recommendation.

### 5 — Output the recommendation (concise + actionable)

Emit one of:

- **REUSE:** "Use existing OAC dataset **<name>** at `<path>` — it already
  covers <tables/columns>."
- **CREATE:** "In OAC, create a dataset (over the AIDP connection) named
  **<suggested>** including:
  - table **`<a>`**: columns `<…>`
  - table **`<b>`**: columns `<…>` *(when multi-table)*
  - join: `<a>.<key> = <b>.<key>`
  Creation is an OAC UI action over the AIDP connection — the OAC MCP server
  reads/searches the catalog but cannot create datasets. Hand this
  recommendation to `/oac-dataset-setup`; after it verifies the dataset, hand
  to `workbook-authoring` to build the visualization(s)."
- **GAP:** "Your live gold layer can't serve this — missing **<metric/dim/grain>**
  (no live table provides it). Author a new mart (new YAML+SQL) with the
  mart-authoring skill, seed it, then re-run this advisor."

**Governance overlay (advisory):** do not recommend exposing **high-PII**
columns in the dataset/dashboard. PII labels come from the pack's
`outputSchema` (advisory metadata) layered on top of the live evidence — never
as a substitute for it.

---

## Skill family

- **Upstream of** `/oac-dataset-setup` and `workbook-authoring`: this skill
  decides *which dataset*; setup guides and verifies the OAC UI checkpoint; the
  workbook skill binds visualizations to it (and needs the real `XSA(...)`
  subject area from `describe_data`).
- **Hands off to** the mart-authoring skill (forthcoming) on a **true GAP**
  (neither live nor the pack's buildable menu can serve it). That skill takes
  the user's business logic and authors the change, working like a careful data
  engineer:
  - inspects the **Fusion PVO source schema** (not bronze) to see which raw
    fields are actually available to build from;
  - chooses the **lowest-cost, additive** change — a new mart (new `.yaml` +
    `.sql`) or an added column — over anything that reprocesses existing data;
  - **never disturbs already-materialized delta** (bronze/silver may hold
    terabytes; no destructive rebuilds).
  After it ships + seeds, this advisor re-evaluates against the new **live** table.
- **Depends on** live AIDP catalog access — a cluster SQL probe
  (`tests/live/aidp_catalog_probe_live.py`, or workbench
  `aidp-catalog-explore`); there is no control-plane metastore REST. Plus OAC
  MCP read tools and an ACTIVE cluster.

## Safety invariants (do not regress)

- **Live catalog is the only evidence** for what gold exists. Never infer
  available tables/columns from `content_packs/*/gold/*.yaml`.
- **Empty/unreadable live gold → say "seed first"**, never fabricate tables.
- **Never claim a dataset/mart was created** — this skill only advises;
  creation is a user OAC action (dataset) or the mart-authoring skill (mart).
- **Respect PII** — keep high-PII columns out of recommendations.
