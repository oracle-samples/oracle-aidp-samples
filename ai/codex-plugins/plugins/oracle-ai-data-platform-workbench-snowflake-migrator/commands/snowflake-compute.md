---
description: Map Snowflake warehouses to AIDP Spark clusters and produce a sizing and cost proposal.
---

# `/snowflake-compute`

Thin wrapper over [`snowflake-compute-proposal`](../skills/snowflake-compute-proposal/SKILL.md).

1. Run the compute stage. Ask for the customer's credit price; without it, report credits only.
2. Present `COMPUTE_PROPOSAL.md`, leading with `max_concurrent_clusters`.
3. Say plainly that shape families need confirmation against the target tenancy.
4. State whether credits were observed or inferred from declared size.
