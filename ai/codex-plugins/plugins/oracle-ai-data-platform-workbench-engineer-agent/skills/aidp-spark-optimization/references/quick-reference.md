# Quick reference — impact-ranked moves + symptom lookup

Agent decision aid. Pick by symptom (table) or scan the impact-ranked list. Always confirm with `diagnosis.md` before/after.

## First 10 minutes (triage)

1. `spark_get_sql_query` → physical plan: join types (`SortMergeJoin`/`BroadcastHashJoin`), `Exchange` count, `WholeStageCodegen`, scan size.
2. Worst stage by wall time → `spark_get_stage`: shuffle bytes, **spill**, GC, inputBytes, numTasks.
3. `spark_get_task_summary` on that stage → **skew ratio p100/p50**.
4. Map the dominant symptom below. Fix the biggest lever first; re-measure.

## Symptom → technique

| Symptom | Technique | Where | Ref |
|---|---|---|---|
| `SortMergeJoin`, one side small/filterable | Broadcast / semi-join pre-filter | [notebook] | `02-joins.md` |
| Join stage skew p100/p50 > 2x | Semi-join + broadcast; AQE skew join; salting | [notebook] | `02-joins.md`,`07-aqe.md` |
| Spill (disk/mem) > 0, high GC | Raise `memory.fraction`; more partitions; fix skew | [cluster-create]/[notebook] | `04-memory-and-spill.md` |
| Lost/replaced executors (OOM) | Stop SHJ build OOM; reduce broadcast; fix skew | [notebook] | `02-joins.md`,`04-memory-and-spill.md` |
| 100k+ input files; scan dominates | Compaction; `maxPartitionBytes`/`openCostInBytes` | [notebook] | `03-file-layout-io.md` |
| Thousands of tiny tasks for tiny data | Fewer partitions; fix driver-loop fan-out | [notebook] | `01-partitioning.md`,`06-caching-materialization.md` |
| CPU-bound wide aggregation; p50≈p100 | Lower `codegen.maxFields` | [notebook] | `05-codegen.md` |
| Same DF recomputed across jobs | Cache (+repartition layout) | [notebook] | `06-caching-materialization.md` |
| Repeated `collect()`/`head()` in driver loop | Row-list + cache + single action / dict | [notebook] | `06-caching-materialization.md` |
| `MERGE` on huge table times out | JOIN + dynamic partition overwrite | [notebook] | `06-caching-materialization.md` |
| Output = thousands of small files | AQE coalesce (`parallelismFirst=false`, advisory 128mb) | [notebook] | `07-aqe.md`,`03-file-layout-io.md` |
| Cores idle / weak parallelism | More partitions; size cluster | [notebook]/sizing | `01-partitioning.md`,`cluster-sizing.md` |
| `UNION` of the same table | array + explode (single scan) | [notebook] | `02-joins.md` |
| Same key joined across many jobs | bucketing (`bucketBy`) | [notebook] | `02-joins.md` |
| Multi-way join, fresh stats available | CBO + join reorder | [notebook] | `02-joins.md` |
| `groupBy` hot-key skew (AQE won't fix) | salted / two-stage aggregation | [notebook] | `02-joins.md` |
| Delta table: many small files (or OCI 429 on write) | prevent: `optimizeWrite` / AQE coalesce (`REBALANCE`); compact-after: OPTIMIZE / auto-compaction (+VACUUM) | [notebook]/[Delta] | `08-delta-lake.md` |
| Python UDF slow / blocks pushdown | native funcs > `pandas_udf` (Arrow) > Python UDF | [notebook] | `05-codegen.md` |
| Single-task JDBC / Oracle read; slow row-trickle | `partitionColumn`+`numPartitions` for parallelism; **raise `fetchsize`** (Oracle default 10!) | read-option | `09-oracle-database.md`,`01-partitioning.md` |
| Multi-TB Oracle write slow / redo-saturated | ADW/ATP: bulk `COPY_DATA` path; Exadata: bigger `batchsize`+bounded parallelism, DB-side **disable redo-shipping ("ARS")** + NOLOGGING+direct-path | write-option/DB | `09-oracle-database.md` |
| Oracle write fails on array/map/struct column | `to_json` → VARCHAR2; unnest in ADW via `JSON_TABLE`/`TREAT AS JSON` | [notebook] | `09-oracle-database.md` |
| Below-threshold join skew AQE won't split | semi-join+broadcast / salting; or `forceOptimizeSkewedJoin` if a genuine straggler | [notebook] | `07-aqe.md`,`02-joins.md` |

## Impact-ranked moves (high → low typical payoff)

1. **Eliminate join shuffle** — broadcast a small/filtered side; semi-join pre-filter a big dimension to broadcastable size. *Evidence: SMJ→BHJ 71→59.5s, skew 3.0x→1.3x; field 4GB→68MB, stage 21→3.5 min.* `02-joins.md`
2. **Kill the small-file tax** — compact sources / size output ~128MB; tune `openCostInBytes`/`maxPartitionBytes`. *Evidence: 261K files cost ≈ data + 1TB of open cost.* `03-file-layout-io.md`
3. **Stop spill (right-size memory)** — raise `spark.memory.fraction` [cluster-create] when no user structures; or larger RAM tier. *Evidence: 2TB ingest, ~1.4TB spill eliminated; ~15–20% (200GB) to ~3x (2TB).* `04-memory-and-spill.md`,`cluster-sizing.md`
4. **Fix skew** — AQE skew join (default on); semi-join/salting when AQE misses. *A single hot task serializes the stage.* `07-aqe.md`,`02-joins.md`
5. **Remove redundant work** — no `union` of same table; no driver-loop `collect()`; intermediates as views (don't materialize); JOIN over MERGE. *Evidence: union/driver-loop 67% (6h→2h); MERGE 5h+→26 min; materialization 28→22 min.* `06-caching-materialization.md`
6. **zstd for shuffle/spill/output** — `spark.sql.parquet.compression.codec=zstd` [notebook]; `spark.io.compression.codec=zstd` [cluster-create]. *Evidence: 30→13 min output; spill-disk 1051GB→138GB; 2TB zstd 8m22s vs snappy 15m7s.* `03-file-layout-io.md`
7. **Parallelism** — `shuffle.partitions` ≈ 2–3× cores; AQE coalesce for well-sized output files. *Evidence: 47.5→44 min with coalesce tuning.* `01-partitioning.md`,`07-aqe.md`
8. **Constrain wide codegen** — lower `codegen.maxFields` for CPU-bound wide aggregations. *Evidence: 1.42x (26.6→18.8s); field 7.9h→23 min.* `05-codegen.md`
9. **Broadcast lookups, prune/pushdown, cache hot DFs** — column pruning + predicate pushdown are free; cache only multi-use DataFrames. `03-file-layout-io.md`,`06-caching-materialization.md`

## Don't
- Don't add hardware to fix skew or a redundant shuffle — fix the query first (`cluster-sizing.md`).
- Don't `repartition()` without a reason — it's a full shuffle (`01-partitioning.md`).
- Don't set a `[cluster-create]` config from a notebook and assume it applied — verify (`config-matrix.md`).
- Don't leave a `spark.conf.set` set on a shared cluster — revert it (`aidp-notes.md`).
- Don't shrink AQE advisory/skew thresholds so far you create a small-file problem (`07-aqe.md`).
