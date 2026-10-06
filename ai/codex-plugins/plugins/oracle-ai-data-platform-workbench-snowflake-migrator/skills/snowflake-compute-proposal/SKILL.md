---
name: snowflake-compute-proposal
description: Map Snowflake warehouses onto AIDP Spark compute clusters and produce a sizing and cost proposal. Inventories every warehouse with its size, multi-cluster scaling policy and auto-suspend, pulls observed credit consumption from ACCOUNT_USAGE where permitted, and proposes worker counts and autoscale ceilings per warehouse. Use when the user asks about compute sizing, warehouse equivalence, cluster shapes, credits, or what the migration will cost.
---

> **Paths.** `<plugin-root>` is this plugin's directory, two levels above
> this `SKILL.md`. Write its absolute path wherever `<plugin-root>` appears.

# Compute proposal — warehouses to clusters

```bash
"<plugin-root>/bin/snowmig" compute \
  [--credit-price 3.0]
```

Every Snowflake coordinate comes from the migration config (`snowmig-config.yaml`, discovered automatically and printed as `config: <path>`). Pass `--account/--user/--auth/...` only to override a field for one run.

Produces `warehouses.json`, `compute.json` and `COMPUTE_PROPOSAL.md`.

## The mapping

Snowflake warehouse sizing is a clean doubling series — X-Small is 1 node and 1
credit/hour, and every step doubles both. A Snowflake node is roughly 8 vCPU /
16 GB, so the proposal converts declared size into vCPU-equivalent and then into
Spark worker count, with the autoscale ceiling covering `max_cluster_count`.

## Three things to say, not skip

1. **Shape families need confirming.** No exact OCI shape SKU is asserted,
   because availability is tenancy- and region-specific. Every proposal carries
   `shape_confirmation_required`. Tell the user to confirm before provisioning.
2. **No cost without a credit price.** Snowflake credit prices vary by edition
   and region, so one is never assumed. Without `--credit-price` the report shows
   credits only. Ask the user for their rate rather than picking a number.
3. **State the credits basis.** `observed` means real
   `ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY` data. `declared_size_only` means
   that grant was unavailable and the sizing rests on declared warehouse size
   alone — which says nothing about how hard the warehouse is actually worked.

Also report `max_concurrent_clusters`: the worst-case simultaneous cluster count
the target has to absorb. It is usually much larger than the warehouse count and
is the number that sizes the environment, not the sum of warehouse sizes.
