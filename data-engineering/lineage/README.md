# Data Lineage in AIDP — API conformance tests + an independent oracle

Two complementary artifacts:

| File | What it does |
|---|---|
| [`test_aidp_lineage_api.py`](./test_aidp_lineage_api.py) | Maintainer harness. Confirms the lineage API is released and reachable, from real responses. 13 passed / 4 xfailed as of 2026-08-16. |
| [`Verify_Data_Lineage.ipynb`](./Verify_Data_Lineage.ipynb) | Derives lineage from Spark's analyzed plan and verifies it against a known DAG. 19/19 checks for the pipeline shapes below. |

> **Read this before you set up a profile.** The API is released. On the one tenancy tested —
> **observed 2026-08-16, a single DataLake in us-ashburn-1, SDK v4.2.1** — no graph came back:
> `fetchLineage` rejected every `anchorNode` we could construct with `400 Invalid anchorNode` (see
> [Known gap](#known-gap-no-graph-came-back-on-the-tenancy-tested)). You can confirm the endpoint
> exists and enforces its contract. Whether you get a graph may differ on your tenancy — Part B
> reports **XPASS** if it does. The notebook does not depend on the API and runs independently.

> **Requires your own tenancy.** `test_aidp_lineage_api.py` signs real requests with your OCI profile
> and needs `AIDP_DATALAKE` exported (see [Running the tests](#running-the-tests)). It is a maintainer
> conformance harness, not a self-contained demo.

## Status: the lineage API is released

AIDP ships lineage under the **`DataLineage`** service (`DataLineageClient`, CLI group
`data-lineage`). It shipped as `SemanticCatalog` in SDK v4.1.0 (2026-08-07) and was renamed in
**v4.1.1 (2026-08-31)**, which the SDK changelog flags as a breaking change -- pin any reference you
write to the SDK version you checked.

| Operation | Call |
|---|---|
| Fetch entity lineage | `POST /20260430/aiDataPlatforms/{aiDataPlatformId}/actions/fetchLineage` |
| Export lineage (CSV) | `POST /20260430/aiDataPlatforms/{aiDataPlatformId}/actions/exportLineage` |

- **Host:** `https://datalake.{region}.oci.oraclecloud.com` (service endpoint prefix `datahub-dp`)
- **API version:** `20260430`
- **Request:** `anchorNode` (required), `direction` (`UPSTREAM|DOWNSTREAM|BOTH`), `level`
  (`ENTITY|COLUMN`), `maxDepth`, `nodeFilters`, `pathFilters`, `shouldIncludeEdges`
- **Response:** `EntityLineage { nodes[], links[] }`, where nodes carry
  `id / qualifiedName / displayName / parentId / type / depth / properties` and links carry
  `fromNodeId / toNodeId / type / providerType / properties`
- **Maturity:** both operations are marked **(Preview)** in the official CLI reference
- **Backing store:** Oracle Data Catalog — table properties carry `com.oracle.dcat.*` keys and
  `request-mode: DATAHUB_EXECUTE`

Column-level lineage (`level: COLUMN`) is part of the released contract, not just table-level.

### Reading a 404

A 404 means different things depending on which host you sent it to, and the response body cannot
tell you which — it is **byte-identical** (111 bytes, same `code`/`message`) for an absent route and
for a resource you are not authorised to see.

| Host | A 404 here means |
|---|---|
| `aidp.{region}` + `/20240831/dataLakes/{ocid}` | the route genuinely does not exist — lineage is not on this generation. It still serves `/catalogs` and `/schemas`. |
| `datalake.{region}` + `/20260430` | **not** a missing route. Almost always a wrong or unauthorised `AIDP_DATALAKE`, which returns the same `NotAuthorizedOrNotFound`. |

`test_A0` is the disambiguator: it lists `/catalogs` on the data-plane host, so if A0 passes, your
profile and OCID are good and a 404 from the lineage route means something else. If A0 fails, fix the
credentials before reading anything into the rest.

The lineage operations are published at `datalake.{region}` + `/20260430`; the SDK CLI reference still
documents `aidp.{region}` as the default endpoint for other operations, so this is a per-service move,
not a wholesale one. `test_A2` and `test_A7` encode both halves of the trap.

### Known gap: no graph came back on the tenancy tested

Observed 2026-08-16, one DataLake in us-ashburn-1, SDK v4.2.1. `fetchLineage` reaches its own
parameter validation and rejected every `anchorNode` we could construct:

```
default.lin_demo.mart_customer_revenue            -> 400 Invalid anchorNode
mart_customer_revenue                             -> 400 Invalid anchorNode
hive.lin_demo.mart_customer_revenue               -> 400 Invalid anchorNode
default.cat/lin_demo.db/mart_customer_revenue     -> 400 Invalid anchorNode
TABLE:default.lin_demo.mart_customer_revenue      -> 400 Invalid anchorNode
<the DataLake OCID>                               -> 400 Invalid anchorNode
```

Omitting the field instead returns `anchorNode must not be null`, so the server distinguishes *missing*
from *unresolvable* — it is doing a real lookup and finding nothing. `POST .../tables/{key}/actions/refresh`
returns `202` but does not change the outcome.

Two candidate explanations, not yet separated:

1. the lineage graph is not populated for this DataLake (no harvest configured, and interactive
   notebook writes may not register a process node — lineage may require Job/Workflow execution); or
2. `anchorNode` expects an internal Data Catalog node id whose format is undocumented — the CLI
   reference lists the field with an **empty description**.

`test_B0` probes the full candidate matrix on every run. It always passes, so pytest **captures** its
output rather than displaying it — with `-v`, `-m existence` or `-rX` you will never see the table.
Read it with:

```bash
pytest test_aidp_lineage_api.py -rP -k B0      # or -s
```

## Running the tests

**Prerequisites.** An OCI config profile with access to your own AI Data Platform instance, and
`AIDP_DATALAKE` exported. There is deliberately no default: without it the suite exits with a message
rather than signing requests against someone else's resource.

That stop is a **collection error (exit 2)** raised at import, not a skip — deliberately. A session
fixture calling `pytest.fail` would be absorbed by Part B's `xfail` markers, turning a missing
variable into the same `xfail` the known gap produces, which is exactly the confusion the note below
warns about.

```bash
pip install -r requirements.txt

export AIDP_DATALAKE=ocid1.aidataplatform.oc1.<region>.<unique-id>   # required
export AIDP_PROFILE=DEFAULT                                         # optional, defaults to DEFAULT
export AIDP_REGION=us-ashburn-1                                     # optional

pytest test_aidp_lineage_api.py -v                 # everything
pytest test_aidp_lineage_api.py -v -m existence    # just the existence checks
pytest test_aidp_lineage_api.py -v -rx             # show why Part B is blocked (xfail reasons)
pytest test_aidp_lineage_api.py -rP -k B0          # read B0's anchor-candidate matrix
pytest test_aidp_lineage_api.py -m "existence and not legacy"   # skip the legacy-host probe
```

| Variable | Required | Default |
|---|---|---|
| `AIDP_DATALAKE` | **yes** | none -- the suite stops if unset |
| `AIDP_PROFILE` | no | `DEFAULT` |
| `AIDP_REGION` | no | `us-ashburn-1` |
| `AIDP_SCHEMA` | no | `default.lin_demo` |
| `AIDP_ANCHOR_TABLE` | no | `<AIDP_SCHEMA>.mart_customer_revenue` |

Without a usable OCI profile the session fixture fails rather than skipping, and Part B reports
`xfail` without a request being sent -- so a credential problem can look like the known gap. Check
that Part A passes before reading anything into Part B.

### Part A — API existence and contract (green)

| Test | Proves |
|---|---|
| `A0_auth_works_on_dataplane_host` | authenticated on the new host, so later 4xx are unambiguous |
| `A1_fetchLineage_route_exists` | route deployed: `400 InvalidParameter`, not `404` |
| `A2_control_bogus_action_is_404` | **control** — a fake sibling action *does* 404, giving A1 meaning |
| `A3_exportLineage_route_exists` | the CSV export operation is deployed too |
| `A4_request_contract_is_enforced_server_side` | missing vs invalid `anchorNode` produce different errors |
| `A5_documented_enums_are_accepted` | `level=ENTITY/COLUMN`, `direction=UPSTREAM/DOWNSTREAM/BOTH` all parse |
| `A6_invalid_enum_is_rejected` | **control** — `direction=SIDEWAYS` → `Invalid LineageDirection: SIDEWAYS` |
| `A7_lineage_absent_from_legacy_api_generation` | documents the wrong-generation trap (also marked `legacy` — it is the only test that needs the old gateway up) |

A5+A6 together are the strongest evidence: a stub that ignored the body and always complained about
`anchorNode` would pass A5 but **fail A6**. The server really parses `LineageDirection`, so a genuine
implementation is behind the route.

### Part B — graph population (xfail; the open gap)

`B1` entity graph · `B2` upstream contains `stg_orders` + `raw_customers` · `B3` column-level edges ·
`B4` CSV export. Marked `xfail(strict=False)` rather than skipped or deleted, so if lineage becomes
populated they flip to **XPASS** and the suite reports that the gap closed. `B2` asserts the same DAG
the notebook derives from Spark — so when it goes green, the platform's graph is confirmed against
independently derived ground truth.

Part B is the only place `AIDP_ANCHOR_TABLE` is used. Part A probes with a deliberately unresolvable
sentinel instead, so Part A stays green on a populated tenancy rather than failing the day Part B
starts passing.

## The notebook: independent ground truth

The API is the platform's *claim* about lineage. The notebook is the oracle you check it against,
deriving lineage from the Catalyst analyzed plan — the one signal that cannot disagree with what
actually ran.

```
raw_orders ──filter status='PAID'──> stg_orders ──┐
                                                 ├─join + GROUP BY──> mart_customer_revenue
raw_customers ───────────────────────────────────┘

unrelated_table                     (decoy — must produce no edges)
```

`mart_customer_revenue` reads **`stg_orders`**, not `raw_orders`; `raw_orders` is only *transitively*
upstream, and the notebook asserts the direct edge only.

Three layers must agree:

| Layer | Source of truth | Proves |
|---|---|---|
| 1. Plan-derived graph | Catalyst analyzed plan | which tables/columns fed each write |
| 2. Delta history | `_delta_log` commit log | the write happened, with row counts |
| 3. File provenance | `inputFiles()` + `DESCRIBE DETAIL` | bytes read live under the claimed table |

Plus **negative controls**, which are what make the result meaningful — an extractor that reported
*every* table would satisfy "no missing edges" while being useless: the decoy appears in no edge; no
direct `mart → raw_orders` edge; no unresolved leaves. Clean run **19/19**, observed 2026-08-16 on
Spark 3.5.0 / Delta 3.2.0-oci-1.0.0.

`DESCRIBE HISTORY` deserves a specific warning: it is commonly mistaken for lineage, but a CTAS commit's
`operationParameters` holds only `partitionBy` / `properties` / `isManaged` — **no source tables**. It
gives temporal provenance, not a graph.

### How the extraction works

- **Table level** — `df._jdf.queryExecution().analyzed().collectLeaves()`, then
  `leaf.catalogTable().get().qualifiedName()` for a clean `catalog.schema.table`.
- **Column level** — the top plan node's `projectList()` (Project) or `aggregateExpressions()`
  (Aggregate); each output expression's `references()` are `AttributeReference`s carrying an `exprId`,
  mapped back to the leaf relations' output attributes.

  Resolution *must* go through `references()`. Aliases and aggregates mint **fresh** `exprId`s —
  `SUM(o.amount) AS revenue` is a new attribute — so output `exprId`s never equal leaf `exprId`s, and
  comparing them directly silently yields no column lineage.

### Scope — what the 19/19 does and does not cover

The 19 checks are proven for **the pipeline shapes in this notebook**: writes whose top plan node is a
`Project` or an `Aggregate` **and where every attribute that node references is emitted directly by a
leaf relation**. Column resolution reads that node's `projectList()` / `aggregateExpressions()` and
maps each reference back through `colmap`, which is built only from `collectLeaves()` — so it is
accurate for aliases, aggregates, `COALESCE`/`CASE` across two sources, `SELECT *` expansion and
self-joins over base tables.

The leaf condition is the one that actually bites, and the top-node shape alone does not predict it. An
attribute minted by an *intermediate* `Project` never appears in `colmap`, so it resolves to nothing
even though the top node is a plain `Project`:

```sql
SELECT * FROM (SELECT cust_id, amount * 2 AS amt FROM raw_orders) s   -- amt <- (nothing)
```

The same happens for `df.withColumn("x", ...).withColumn("y", col("x") + 1)` and for an aggregate over
a subquery. Cell 14 renders those as `(literal)`, which is indistinguishable from a genuine constant.

It is **not** a general-purpose lineage extractor. Outside those shapes it can return an incomplete or
empty column map, in some cases without warning:

| Shape | Behaviour |
|---|---|
| Alias computed in a `FROM`-subquery, or by an earlier `.withColumn()` / `.select()` — i.e. `Project` over `Project`, `Aggregate` over `Project` | that column's lineage is empty, printed as `(literal)` |
| Window function, `Project` over `Aggregate`, `LATERAL VIEW explode`, second branch of a `UNION` | column lineage may be wrong or empty |
| Top node is `Sort` / `GlobalLimit` / `Distinct` / `Filter` (HAVING) / `WithCTE` / `Union` / `Except` | column map empty |
| CTE | adds a spurious `<CTERelationRef>` leaf |
| Scalar / `IN` subquery sources | missing from table-level lineage |

Extending it would mean resolving attributes transitively through `Project`/`Aggregate`/`Window`/`Union`,
including `subqueriesAll()`, and resolving `CTERelationRef` through `cteDefs`. Treat the current version
as a worked demonstration of the technique on a known DAG, not as production tooling.

Capture covers writes routed through the notebook's `write_tracked()` helper, and the graph lives in
kernel memory for the session. Columns used only in `WHERE` / `JOIN ON` (e.g. `raw_orders.status`) are
real dependencies but not top-level outputs, so they do not appear in the column map.

`DataLineage` exposes **no lineage write/ingest operation** — only `fetchLineage` and
`exportLineage` — so a client cannot push this graph into AIDP. Population is the platform's job. For
durable local capture, register a JVM `QueryExecutionListener` on `spark.listenerManager`, or install the
OpenLineage Spark listener
(`spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener`) and point
`spark.openlineage.transport.*` at a collector.

## Clear outputs before committing

`DESCRIBE DETAIL` returns a `location` that embeds your object-storage bucket and namespace, and
`inputFiles()` returns full `oci://` URIs. The notebook prints only the path portion for this reason,
but a committed run can still carry tenancy identifiers in other cells. Strip outputs (`nbstripout`,
or Kernel → Restart & Clear Output) before opening a PR.

## Environment as tested

Spark 3.5.0 · Delta 3.2.0-oci-1.0.0 · `spark.sql.sources.default=delta` · catalog impl `hive` ·
region `us-ashburn-1` · aidp SDK `oracle-samples/aidataplatform-sdk` v4.2.1, lineage operations on
`/20260430`.
