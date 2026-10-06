# Data-movement options — for the customer to choose

`snowmig data-options` writes this catalogue for a run (`DATA_MOVEMENT_OPTIONS.md`); the options are defined in `engine/plan/data_movement.py`. It is surfaced in **every** plan and summary, whether or not a choice has been made.

**The control-plane CLI moves no bytes.** One path is implemented by the data plane: the in-AIDP INSERT-SELECT from Snowflake, run schema by schema by the `snowmig_02_copy_schema` jobs. The options below are architectures for the longer term, outside the plugin's scope: the realistic ways data could move, with the trade-offs and the open questions attached, so the choice is made deliberately.

| Option | Catalog | Moves bytes | Phase |
|---|---|---|---|
| **A1_UNLOAD_OBJECT_STORAGE** — Bulk unload to object storage, land as managed Delta | INTERNAL | yes | historic |
| **A2_FEDERATE_EXTERNAL_CATALOG** — Federate: read Snowflake in place through an EXTERNAL catalog | EXTERNAL | no | historic, ongoing |
| **A3_REDIRECT_INGESTION** — Redirect ingestion at the source (Fivetran and pipelines) | BOTH | yes | ongoing |
| **A4_ICEBERG_INTEROP** — Iceberg interop: share storage instead of copying | EXTERNAL | no | historic, ongoing |
| **A5_HYBRID_WAVES** — Hybrid: federate first, copy selectively, redirect forward | BOTH | yes | historic, ongoing |
| **A6_CUSTOMER_DEFINED** — Customer-defined — something not listed here, or not decided yet | TBD | no | historic, ongoing |

## A1_UNLOAD_OBJECT_STORAGE — Bulk unload to object storage, land as managed Delta

**Path:** Snowflake COPY INTO @stage as Parquet -> cloud object storage -> OCI Object Storage (Interconnect or replication) -> Spark read -> managed Delta in an INTERNAL catalog

**For**

- Parquet carries DECIMAL natively, so NUMBER(p,s) survives if the round trip is verified
- unloading to storage in the SAME cloud region avoids Snowflake egress charges
- lands as managed Delta in an INTERNAL catalog, the writable AIDP catalog type
- resumable and re-runnable per table, so it suits waves

**Against**

- the largest moving part: staging, parallelism, throttling and reconciliation all have to be built
- Snowflake external stages target S3, Azure or GCS, so the path includes a transfer from that storage into OCI Object Storage
- wall-clock at PB scale is measured in days, not hours

**Still unknown**

- which region the source account is actually in -- this decides whether same-region unload and Interconnect apply at all
- whether NUMBER(p,s) survives unload -> Parquet -> Delta with exact precision (to be measured on a representative table)
- transfer throughput actually achievable into OCI Object Storage

Status: `proposal_only`

## A2_FEDERATE_EXTERNAL_CATALOG — Federate: read Snowflake in place through an EXTERNAL catalog

**Path:** AIDP registers Snowflake as an EXTERNAL, read-only catalog over JDBC; queries read through. Copy only the subset that needs to be resident

**For**

- no bulk transfer, so no staging, no egress and no wall-clock risk
- available immediately once credentials exist
- good for a coexistence period, and for validating results against the source before anything is copied
- reduces the migration to only the objects that genuinely must move

**Against**

- EXTERNAL catalogs are read-only in AIDP, so this option reads the source rather than landing data in AIDP
- Snowflake stays in service and continues to consume credits during coexistence
- queries that do not push down to Snowflake read their rows from Snowflake on every run
- the AIDP Snowflake connector is read-only in AIDP 4.0, so dual-write and write-back reconciliation are not part of this option

**Still unknown**

- how much aggregation actually pushes down to Snowflake
- whether the authentication method the customer can grant is supported by the target AIDP version's Snowflake connector

Status: `proposal_only`

## A3_REDIRECT_INGESTION — Redirect ingestion at the source (Fivetran and pipelines)

**Path:** Stop feeding Snowflake for the migrated domains. Point Fivetran at an Oracle destination (ADW/ALH) or via Kafka -> OCI Streaming -> Spark consumer -> Delta. Historic data is handled separately by A1

**For**

- forward-only: new data arrives in AIDP from day one, so the copied history stops growing
- removes the double-write period that a long bulk migration implies
- reuses the existing ingestion tooling rather than replacing it

**Against**

- depends on which Fivetran destinations suit the target: its Oracle destination (ADW/ALH) or the Kafka route into OCI Streaming
- the Oracle destination's release status, volume limits and object-type coverage need confirming
- the Kafka route means owning the sink -- more moving parts to operate
- does not cover historic data on its own

**Still unknown**

- the Fivetran Oracle destination's limits at the customer's ingest rate
- whether OCI Streaming's SASL_SSL is compatible with Fivetran's Kafka destination

Status: `proposal_only`

## A4_ICEBERG_INTEROP — Iceberg interop: share storage instead of copying

**Path:** Snowflake writes Iceberg tables to external object storage; AIDP reads the same files through an Iceberg catalog. No duplication for tables already in, or convertible to, Iceberg format

**For**

- no copy at all for Iceberg-format tables -- one set of files, two engines
- avoids the precision and fidelity risk of an unload round trip entirely
- a genuine coexistence story rather than a cutover

**Against**

- only applies to Iceberg tables; native Snowflake FDN tables must be converted first, which is itself a rewrite of the data
- storage stays wherever Snowflake writes it, so cross-cloud latency and egress may persist
- feature maturity on both sides needs verifying against the actual versions in play

**Still unknown**

- what share of the estate is or could be Iceberg rather than native
- whether the target AIDP version can read Iceberg from the customer's storage account

Status: `proposal_only`

## A5_HYBRID_WAVES — Hybrid: federate first, copy selectively, redirect forward

**Path:** A2 for immediate read access and result validation, A1 for the objects usage data shows actually matter, A3 to stop the history growing. Iceberg (A4) wherever it already applies

**For**

- avoids an all-or-nothing cutover and lets value land before the bulk transfer finishes
- usage data decides what is worth copying, so the expensive path runs on the smallest possible set
- each wave is independently reversible

**Against**

- the most governance: two live systems, a moving boundary, and lineage spanning both
- needs the usage profile to be available, which needs ACCOUNT_USAGE
- needs the most coordination with stakeholders

**Still unknown**

- everything A1, A2 and A3 do not yet know, plus how long a coexistence window the business will accept

Status: `proposal_only`

## A6_CUSTOMER_DEFINED — Customer-defined — something not listed here, or not decided yet

**Path:** Whatever the customer's platform team specifies. This option exists so that 'none of the above' and 'not yet decided' are first-class answers rather than gaps in a form

**For**

- matches the customer's own requirements rather than the options listed here
- lets the decision wait until the people who own it are in the room, without blocking the read-only assessment
- their platform team may already have a pattern, tooling and operational experience that beats anything proposed here

**Against**

- nothing can be planned, sequenced or costed until it is described
- this plugin cannot assess it, so none of the trade-offs or unknowns listed against A1-A5 transfer to it

**Still unknown**

- everything -- by definition. Once described, the unknowns become whatever that design implies, and this plugin has not evaluated them

Status: `proposal_only`

---

## What happens after you choose

`snowmig data-options --choose <id> --rationale "..."` records the option and the reasoning (`record_choice()`); it executes nothing. Settle every open question listed against the chosen option on one representative table before building tooling around it. For A1, the first to settle is whether `NUMBER(p,s)` survives an unload round trip with exact precision.

---

## Capability matrix

| Option | historic_bulk | ongoing_incremental | read_without_copy | cutover |
|---|---|---|---|---|
| `A1_UNLOAD_OBJECT_STORAGE` | ✅ | — | — | ✅ |
| `A2_FEDERATE_EXTERNAL_CATALOG` | — | — | ✅ | — |
| `A3_REDIRECT_INGESTION` | — | ✅ | — | ✅ |
| `A4_ICEBERG_INTEROP` | — | ✅ | ✅ | — |
| `A5_HYBRID_WAVES` | ✅ | ✅ | ✅ | ✅ |
| `A6_CUSTOMER_DEFINED` | ? | ? | ? | ? |

No single option covers every capability except A5, which is a composition of the others. Expect to combine rather than pick.

`A6_CUSTOMER_DEFINED` is `?` across the board on purpose: it is the open slot, and what it covers is unknown until the customer describes it. **The eventual design does not have to be one of the others**, and "not decided yet" is a valid answer that blocks nothing.

---

## What each option would take to build

What a team would need to build or measure for each option. Components, not an estimate; none of this is part of the plugin.

### A1_UNLOAD_OBJECT_STORAGE

- An unload driver: per-table COPY INTO @stage as Parquet, chunked by partition or by a key range, resumable per chunk.
- A transfer step into OCI Object Storage -- rclone, OCI CLI bulk-upload, or storage replication -- with throughput measured, not assumed.
- A landing step: Spark reads the Parquet and writes managed Delta, then registers the table durably rather than relying on CTAS auto-registration.
- A reconciliation harness: exact row counts and exact-decimal column sums compared against the source. Float tolerance is wrong for money.
- Cross-process throttling and 429 handling on the OCI side.

### A2_FEDERATE_EXTERNAL_CATALOG

- Register Snowflake as an EXTERNAL catalog and confirm the auth mode the customer can grant is supported by the target AIDP version.
- Measure pushdown: which predicates and aggregations reach Snowflake, and which pull rows across the wire.
- No transfer code at all -- this is the option that needs the least building and the most measurement.

### A3_REDIRECT_INGESTION

- Decide the Fivetran path: the Oracle destination into ADW/ALH, or the Kafka destination into OCI Streaming.
- If Kafka: a Spark consumer that lands to Delta, plus offset management and exactly-once semantics -- the team owns the sink.
- If Oracle destination: confirm volume and object-type coverage at the customer's real ingest rate.
- A per-domain switchover runbook, since ingestion is redirected domain by domain, not all at once.

### A4_ICEBERG_INTEROP

- Survey what share of the estate is already Iceberg versus native FDN.
- For native tables, a conversion step in Snowflake -- which is itself a full rewrite of the data, so it is not free.
- Register the Iceberg catalog in AIDP and confirm the target version can read from the customer's storage account.
- Decide who owns compaction and snapshot expiry once two engines read the same files.

### A5_HYBRID_WAVES

- Everything A1, A2 and A3 require, plus the wave planner that decides which objects take which path.
- A usage profile to drive that decision, which needs ACCOUNT_USAGE.
- Lineage that spans both systems for the duration of the coexistence window.
- A governance model for the moving boundary: what is authoritative where, and when that changes.

### A6_CUSTOMER_DEFINED

- Capture the customer's design verbatim first: name, description, and who owns it. Do not paraphrase it into one of A1-A5.
- Then assess it on the same axes used here -- does it move bytes, which capabilities does it cover, what does it need to exist -- and add it to this catalogue if it is reusable.
- Until it is described there is nothing to build, and saying so is more useful than proposing a substitute.
