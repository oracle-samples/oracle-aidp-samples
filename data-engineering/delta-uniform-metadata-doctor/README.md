# Delta UniForm metadata doctor

A scheduled health check — and, when allowed, a repair job — for the **Iceberg metadata that
Delta UniForm generates** on Oracle AI Data Platform. One notebook, parameterized by catalog:
run it as a workflow on a schedule and it verifies that every UniForm table in the catalog is
still readable by Iceberg consumers (ADW Iceberg external tables, Spark's Iceberg reader),
reports what isn't, and in `repair` mode fixes it with the least invasive action that works.

## The problem it solves

Everything the doctor watches follows from how the two table formats and the UniForm
conversion are **designed** — none of it is a platform decision:

- **Iceberg records absolute paths.** Per the
  [Iceberg table specification](https://iceberg.apache.org/spec/), every location in the
  metadata is a fully qualified URI (relative paths arrive only with the still-in-development
  format v4). Delta, by contrast, resolves its files relative to the table folder.
- **The UniForm conversion is asynchronous and incremental.** It runs after a data commit, on
  the cluster that made the commit, and each conversion builds on the previous Iceberg state.
- **A managed table's folder is derived from its name** — standard Hive-style managed-table
  semantics, which AIDP's catalog follows. Renaming a managed table therefore also moves its
  folder; that is simply what a rename *is* here.

Two situations follow from that combination and are worth watching:

- **Metadata pointing at a previous location.** When a managed table's folder moves (a rename,
  for example), the Delta side keeps working — relative paths — while the existing Iceberg
  metadata still references the previous folder. Iceberg readers surface that as
  missing-object errors (an ADW external table reports `KUP-13057` / `ORA-20404`, or
  `KUP-13022`, on `version-hint.text` or a `snap-*.avro`). Because generation is incremental,
  the condition persists until the metadata is regenerated at the current location — exactly
  what the repair below does.
- **An incomplete conversion.** When a conversion cannot complete — for example, the cluster
  is stopped right after a write — the Iceberg view stays at the previous version until the
  next commit, which on a low-traffic table may be far away.

Both situations are easiest to handle when they are visible on a schedule; the doctor reports
them, and optionally repairs them.

## How it decides

Each `vN.metadata.json` written by UniForm records the Delta version it was converted from
(property `delta-version`) — that is the completion signal. Structure is checked first, then
freshness:

| State | Test | Repair action (`MODE=repair`) |
|---|---|---|
| `NOT_UNIFORM` | no `delta.universalFormat.enabledFormats=iceberg` | skipped |
| `HEALTHY` | metadata `location` == table location, current manifest list exists, `delta-version` ≥ latest Delta version | none |
| `LAGGING` | behind, oldest unconverted commit younger than `MAX_LAG_MINUTES` | none — normal async behavior |
| `STUCK` | behind for longer than `MAX_LAG_MINUTES` | a commit, to trigger a fresh conversion |
| `BROKEN/MISSING` | UniForm on, no `metadata/` at all (and table older than `MAX_LAG_MINUTES`) | a commit — missing metadata heals on the next conversion |
| `BROKEN/MOVED` | metadata `location` ≠ table location (rename/move) | move `metadata/` aside, then a commit → **full** regeneration at the current location |
| `BROKEN/DAMAGED` | current manifest list missing, or no `delta-version` recorded | same as MOVED |
| `UNREADABLE` | the scan itself failed on this table | protected — reported, never repaired |

The scan reads the Delta version **before** the Iceberg metadata, so a concurrent write can only
make Iceberg look *ahead*, never falsely behind (hence `≥` everywhere). Concurrent writers are
otherwise a non-issue: 40 concurrent commits on one table and 6 tables written simultaneously
all converged with no intervention (measured — see below).

### The repair commit

1. **Marker commit**: insert one typed marker row, delete it — the data ends exactly as it
   was. A *data* commit is what triggers a conversion; a table-properties-only commit does not
   (measured on this platform). The marker column is chosen type-safely (string, else
   int/bigint, else double — never smallint/tinyint/decimal, where the marker value
   overflows), a column whose data already contains the marker value is skipped, `NOT NULL`
   columns get neutral values, and a failed DELETE is retried and then reported loudly with
   the exact cleanup predicate.
2. **Escalation** (`ESCALATE=true` only): a `STUCK` table whose commit did not converge has
   *proven* its conversions fail, so it gets the `BROKEN` treatment.

Why partially damaged metadata gets the full rebuild rather than just a commit: the converter
builds each conversion on the newest Iceberg state it finds (and readers resolve it through
`version-hint.text`), so once that state references files that no longer exist, **every
subsequent incremental conversion fails too** — observed live on a table whose newest
`vN.metadata.json` was gone while the hint still pointed at it. Moving the unusable state
aside is what lets the next conversion start from nothing and succeed; the same logic powers
the `STUCK` escalation, since a table whose marker commit did not converge has proven its
incremental conversions fail.

`metadata/` is never deleted: it is **moved** to `_uniform_doctor_bkp/<table>__metadata_<ts>`
beside the table — outside the table folder, so `VACUUM` never sees it, and restoring is a
single rename. Delete backups yourself once consumers are verified.

## Running it

Interactively: attach to a cluster, set the parameters in cell 1's defaults, run all cells.
`report` mode writes nothing anywhere.

As a scheduled workflow (the intended shape):

1. Upload the notebook to your workspace.
2. Create a job with one notebook task pointing at it; pick any cluster that can read the
   catalog (repairs additionally need write).
3. Set the task **parameters** (`CATALOG` at minimum) and a schedule — e.g. every 30–60 min in
   `report` mode as a monitor, or `repair` mode in a quiet window.
4. Leave `FAIL_ON=broken` (default): the run fails loudly when it finds a `BROKEN` table or a
   repair that does not converge, so the workflow's failure alerting is the notification channel.

### What a run looks like

Both excerpts are from live runs on this platform (object-storage URIs shortened). A
`report` run — read-only; with `FAIL_ON=broken` (the default) this run would end **failed**,
which is the alert:

```
doctor | catalog=finance mode=report max_lag=1min fail_on=none max_repairs=20 escalate=False
scanning 32 table(s) in finance with 16 thread(s)...
scan done in 19.1s | {"HEALTHY": 26, "LAGGING": 0, "STUCK": 1, "BROKEN": 3, "NOT_UNIFORM": 2, "UNREADABLE": 0}
   BROKEN/MOVED     doctor_e2e.t_moved :: metadata points at oci://…/doctor_e2e.db/t_premove
   BROKEN/MISSING   doctor_e2e.t_missing :: UniForm is enabled but no Iceberg metadata exists
   STUCK            doctor_e2e.t_stuck :: converted up to v1, Delta is at v2
   BROKEN/DAMAGED   doctor_e2e.t_damaged :: current manifest list file is missing
report mode - 4 table(s) would be repaired. Set MODE=repair to act.

===== VERDICT ===== catalog=finance mode=report
   {"HEALTHY": 26, "LAGGING": 0, "STUCK": 1, "BROKEN": 3, "NOT_UNIFORM": 2, "UNREADABLE": 0}
```

A `repair` run over four broken tables — the whole job took 59 s, all four repaired inside
one shared wait window:

```
doctor | catalog=finance mode=repair max_lag=30min fail_on=broken max_repairs=20 escalate=False
scanning 4 table(s) in finance with 16 thread(s)...
scan done in 2.8s | {"HEALTHY": 0, "LAGGING": 0, "STUCK": 0, "BROKEN": 4, "NOT_UNIFORM": 0, "UNREADABLE": 0}
   BROKEN/MOVED     doctor_e2e.w0_moved :: metadata points at oci://…/doctor_e2e.db/w0_pre
   BROKEN/MOVED     doctor_e2e.w1_moved :: metadata points at oci://…/doctor_e2e.db/w1_pre
   BROKEN/MOVED     doctor_e2e.w2_moved :: metadata points at oci://…/doctor_e2e.db/w2_pre
   BROKEN/MOVED     doctor_e2e.w3_moved :: metadata points at oci://…/doctor_e2e.db/w3_pre
   doctor_e2e.w0_moved                      FIXED (marker commit)
      backup: oci://…/doctor_e2e.db/_uniform_doctor_bkp/w0_moved__metadata_2026-10-20_13-20-56_utc
   doctor_e2e.w1_moved                      FIXED (marker commit)
      backup: oci://…/doctor_e2e.db/_uniform_doctor_bkp/w1_moved__metadata_2026-10-20_13-20-56_utc
   doctor_e2e.w2_moved                      FIXED (marker commit)
      backup: oci://…/doctor_e2e.db/_uniform_doctor_bkp/w2_moved__metadata_2026-10-20_13-20-56_utc
   doctor_e2e.w3_moved                      FIXED (marker commit)
      backup: oci://…/doctor_e2e.db/_uniform_doctor_bkp/w3_moved__metadata_2026-10-20_13-20-56_utc

===== VERDICT ===== catalog=finance mode=repair
   {"HEALTHY": 4, "LAGGING": 0, "STUCK": 0, "BROKEN": 0, "NOT_UNIFORM": 0, "UNREADABLE": 0}
done.
```

### Parameters

| Parameter | Default | Meaning |
|---|---|---|
| `CATALOG` | — (required) | catalog to scan |
| `MODE` | `report` | `report` = read-only; `repair` = act on STUCK/BROKEN |
| `MAX_LAG_MINUTES` | `30` | maximum tolerated conversion lag; beyond it a table is STUCK |
| `EXCLUDE_SCHEMAS` | empty | comma-separated schemas to skip |
| `MAX_REPAIRS` | `20` | hard cap of repaired tables per run (systemic-event guard) |
| `CONVERSION_WAIT_SECONDS` | `180` | shared wait window per repair **wave** — all repaired tables converge inside one window (measured: 20 concurrent conversions drained within ~3 s of each other), so a run pays it at most twice: once for the marker wave, once for the escalation wave. Regeneration measures seconds up to tens of thousands of files, so a wave silent for 3 minutes has failed |
| `REPAIR_THREADS` | `8` | parallelism of a repair wave's backups and marker commits |
| `ESCALATE` | `false` | allow STUCK → rebuild when its commits don't converge |
| `FAIL_ON` | `broken` | `none` / `broken` / `any` (also fail on STUCK) |
| `SCAN_THREADS` | `16` | scan parallelism |

## Permissions and cost

- `report` mode needs read on the catalog (the scan is `SHOW`/`DESCRIBE` plus listing and
  reading `metadata/` through the cluster's storage identity). `repair` mode needs write on the
  affected tables; the `metadata/` move runs through the Hadoop FS API with the same cluster
  identity — no extra credential, no OCI key in the notebook.
- Scan cost: two catalog calls (`SHOW TBLPROPERTIES`, `DESCRIBE EXTENDED`) plus two flat
  folder listings and one small-file read per table, all through the Hadoop filesystem —
  no Spark job per table. The Delta version and commit time come from listing `_delta_log`
  (a commit file's modification time is what `DESCRIBE HISTORY` reports), and the
  `metadata.json` is read directly instead of through `spark.read.text`; each avoided call
  was a Spark-scheduler round trip per table. What remains per table is the two catalog
  calls, which cannot be safely dropped: the properties are what distinguish a table that
  was never UniForm from one whose metadata is missing, and stop the repair from ever
  writing to a table that is not supposed to be UniForm. Threaded (`SCAN_THREADS`).
  Measured at ~0.45 s per table — budget 7–8 minutes per thousand tables, and scope big
  estates with `EXCLUDE_SCHEMAS` or one job per catalog.
- Repair cost: a tiny commit per table (two in the marker case), plus one full metadata
  regeneration per BROKEN table. Regeneration is metadata-only — it reads the Delta log, never
  the parquet data — so its cost tracks **file count**, not bytes. Measured on this platform:
  5–9 s for small tables, 31 s at 39,048 data files. Repairs run as a wave: backups and marker
  commits threaded (`REPAIR_THREADS`), then one shared wait window for the whole batch — the
  conversions of a batch drain together (measured: 20 tables within ~3 s of each other,
  ~0.15 s apiece once the cluster has CPU to spare), so N repaired tables converge inside a
  single `CONVERSION_WAIT_SECONDS` window instead of paying it per table.
- The conversion a repair triggers runs on the cluster that made the commit, and what delays
  it is **CPU contention, not a conversion queue**: with 20 tables burst-written on a small
  cluster, conversions waited while the writes held the cores (worst commit→converted lag
  ~61 s), then all 20 drained within ~3 s. Run the doctor as its own job (own cluster) and
  its repair conversions never compete with the ETL's work at all.

### Measured behavior (AIDP, Spark 3.5.0 / Delta 3.2.0, Oct 2026)

| Fact | Result |
|---|---|
| Completion signal | each `vN.metadata.json` records the Delta version it was converted from (the `delta-version` property); it caught up ~3 s after a small commit |
| Repair of a renamed table (move `metadata/` aside + one commit) | fresh metadata generated at the current location; the Iceberg read matched the Delta read |
| Concurrent writers | 40 concurrent commits on one table, and 6 tables written simultaneously, all converged with no intervention |
| Marker-row commit | insert + delete leaves the data unchanged; a data commit is required — a table-properties-only commit does not trigger a conversion |
| Full regeneration at scale | 31 s for a 39,048-file liquid-clustered table (the file count of a ~5 TB table at a 128 MB target file size) |
| Conversion throughput under concurrency | 20 tables burst-written at once: all 20 conversions completed within ~3 s of each other (~0.15 s apiece), 15 s after the last commit; the freshness lag was CPU contention from the writes themselves (worst case ~61 s on a saturated 8-core cluster), not a conversion queue |

## Limitations

- **Schema misalignment** (tip snapshot older than `current-schema-id` after `DROP/RENAME
  COLUMN`) is a different condition with a different fix (a data commit on the source) — it is
  the [ADW Iceberg External Table Sync](../adw-iceberg-external-table-sync/README.md)'s
  `MISALIGNED`/`force_snapshot` territory, and the doctor does not act on it.
- Don't run two repair-mode doctors against the same catalog concurrently, nor a repair-mode
  doctor alongside the sync with `flags.fix_renamed_uniform: true` on that catalog: both move
  the same `metadata/` folders.
- Streaming/CDF consumers of a repaired table will see the marker row inserted and deleted
  (the data itself ends unchanged).
- A rename of a managed UniForm table is, in effect, a folder move (the folder is derived
  from the name — Hive-style managed-table semantics), so plan it as one: let the doctor —
  or the sync's `flags.fix_renamed_uniform` — regenerate the metadata afterwards, or recreate
  the table under the new name with `CREATE TABLE <new> … AS SELECT * FROM <old>` (CTAS),
  setting the UniForm `TBLPROPERTIES` explicitly — CTAS does not inherit them — then repoint
  consumers and drop the old table.

## Relation to the sibling samples

- [ADW External Table on Delta UniForm](../adw-ext-table-on-uniform/README.md) — create the
  ADW side for one table.
- [ADW Iceberg External Table Sync](../adw-iceberg-external-table-sync/README.md) — keep a
  whole catalog synced to a fleet of ADWs. The sync also handles the renamed-table case
  natively (`flags.fix_renamed_uniform`); the doctor is the catalog-wide health check that
  covers every state, and a `report`-mode run right before the sync is a cheap way to catch an
  unreadable table before it surfaces in a consumer query.
