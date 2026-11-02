# Advanced DNS validation scope

V01–V03 demonstrate explicit Advanced DNS scenarios: S3, customer Network Source and cross-region Object Storage. V04 combines the existing private-endpoint baseline and preservation across unrelated DNS edits. V05–V07 examine endpoint coverage, configuration and Spark continuity. A successful baseline alone does not establish compatibility after changes.

| Case | Additional behavior | Customer value | Current notebook coverage |
| --- | --- | --- | --- |
| V04 | Preserve existing private database connectivity across unrelated DNS edits | Keep established integrations working. | Before/add/edit/delete/restored database identity and configuration checks; default proxy and legacy adoption claims require additional evidence. |
| V05 | Distinguish default resolution, service-zone allowances and explicit mappings | Avoid unnecessary entries and identify the actual DNS mechanism. | Selected endpoint configuration and authenticated access; attributing a default mechanism needs the additional evidence below. |
| V06 | Self-service DNS CRUD, validation and cancellation | Maintain scoped configuration and reject invalid input. | Manual UI operations with supplied save/readback and validation observations. |
| V07 | Apply supported Advanced DNS edits without restarting existing Spark workloads | Change connectivity while workloads continue running. | DNS convergence, functional phases, application identity and independent lifecycle evidence; deployment prerequisites apply. |

These are test objectives, not statements that every deployment implements them. No notebook performs workspace edits or automatically certifies the complete requirement. Keep external evidence private and record the deployed capability/version with your run. The customer examples deliberately avoid internal implementation details and rollout controls.

## V04 — Private database baseline and preservation

**Why run it:** Establish the pre-existing private ADB connection, then repeat the same read-only database identity query after unrelated Advanced DNS changes. The former V08 preservation check is now part of this notebook. The database's private descriptor, route and authentication stay unchanged throughout.

**Five phases:** Run `before` without adding a database DNS entry. Add unrelated owned hostname A and run `after_add`; replace A with B and run `after_edit`; delete B and run `after_delete`; restore the complete original DNS/SCAN settings and run `restored`. Capture an authenticated query and complete configuration snapshot in every phase. Keep earlier `RESULTS` and compare the same expected database identity. The evaluator rejects a baseline-only run, unchanged DNS snapshots, missing phases and a different database identity.

**Scope:** Private ADB names unresolved by the service resolver use the default private DNS proxy and customer DNS context. Verify existing overrides and the actual private path before attributing the baseline to that mechanism. This test adds no explicit entry for the private database and does not demonstrate a new private-database capability. Follow supported edit/lifecycle prerequisites; V07 separately tests changes without a Spark restart. V04 does not establish live DNS propagation to AI compute.

Use a fresh authenticated connection where practical so cached addresses or long-lived connections do not conceal a regression. The checker evaluates supplied observations and requires human review of private endpoint, DNS path and transport evidence. It does not establish the historical origin of an entry or perform platform rollout/migration. A previously working database is not necessarily support-configured.

**Optional manual acceptance for legacy adoption:**

1. Identify a confirmed historical support-configured endpoint and privately reference its original change record. Capture a fresh baseline with the original authentication and restrictions. If an override supplies connectivity, keep the default-proxy attribution separate.
2. Have the administrator identify the supported adoption transition. Record deployment state, existing configuration and any prerequisite lifecycle action. Without an observed transition, label this a **connection regression** run only.
3. Capture configuration and repeat the original operation after adoption, before adding new entries. Confirm that no replacement allowlisting request or manual re-entry was needed.
4. Complete V04's unrelated add/edit/delete/restored sequence, preserving original DNS/SCAN entries, target identity, routes and authentication.
5. Review provenance, transition, retained settings and operations together. Missing provenance/transition evidence leaves legacy adoption **INCOMPLETE**, regardless of the general regression result.

## V05 — Endpoint coverage and default resolution

**Why run it:** Verify selected service endpoints and distinguish endpoints that work through platform defaults from endpoints that need customer configuration. This complements the individual connectivity scenarios by examining coverage across actual resources.

**Current scope:** The notebook evaluates supplied configuration and authenticated-operation records for selected endpoints. Its checker does not determine whether an entry was automatically included. Configuration acceptance, a successful DNS lookup, or an authenticated read alone cannot prove automatic inclusion or removal of a support dependency.

**Additional manual acceptance for default resolution:**

1. Select an actual eligible endpoint using the documentation and capabilities of your deployed service. Record its region, endpoint type and supported operation. Do not treat a list of service families as an eligibility guarantee.
2. Capture the complete original customer DNS/SCAN configuration and establish whether a historical support override exists. Use a clean, approved test workspace if that history cannot be established; do not remove an override on a shared workspace to manufacture a baseline.
3. Before adding any entry, run runtime DNS and an authenticated read/query. Preserve evidence that the target is eligible and that no manual entry or historical override supplied its connectivity. Automatic platform behavior need not appear as an editable UI row.
4. Separately test any endpoint requiring explicit configuration. Label that result **manual configuration**, rather than automatic inclusion. Preserve the original policies and routes.
5. Review eligibility, configuration history, DNS, operation and path evidence together. If history or eligibility is unavailable, report automatic inclusion as **INCOMPLETE**, even if endpoint coverage succeeds. Identify whether a regional service-zone allowance, explicit mapping or default private DNS proxy supplied the observed behavior.

Classify regional service-zone allowances, explicit FQDN mappings and default private DNS proxy resolution separately. A private ADB name using the default proxy does not need an explicit entry solely because it is private. Cross-region public endpoints, S3 and customer source-context requirements can require explicit configuration. Success without a user entry does not by itself establish which mechanism supplied connectivity. Testing one resource does not establish coverage of all resources in its DNS zone. Keep the extra observations in a private companion record; the existing checker remains a selected-endpoint coverage check.

## V06 — Self-service configuration lifecycle

**Why run it:** Demonstrate that users can maintain Advanced DNS entries, reopen saved configuration, reject malformed input and cancel an unsaved change. A successful data read does not validate those management operations.

**Prerequisite:** A/B must be resolvable from the service environment and suitable for explicit-entry provisioning; customer-only private resolution does not qualify. Keep an unresolved name only as a deliberate negative test.

**Current scope:** The notebook checks supplied add/edit/delete/cancel snapshots and input-validation observations. Exercise the supported workspace creation and editing flows separately where available. Keep unrelated DNS and SCAN entries intact. The current procedure tests Advanced DNS entries; preserving a SCAN snapshot is not a functional SCAN proxy test.

For each saved operation, capture both its operation result and reopened settings. For cancellation and rejected input, capture the unchanged baseline. Record duplicate/normalization and partial-provisioning behavior as observed; do not assume undocumented atomic rollback. For runtime effects, pair this case with V01–V04 or V07. Internal reconciliation, mapping ownership/cleanup, rollout flags and telemetry require separate engineering validation.

## V07 — Apply changes without a Spark restart

**Why run it:** Establish that supported Advanced DNS additions, updates and deletions reach an already-running workload without restarting or replacing its Spark application.

**Prerequisite:** Use service-resolvable public hostnames exercising explicit Advanced DNS mappings, rather than private-only hostnames handled through the default proxy.

**Current scope:** Run before/add/edit/delete/restored phases against real DNS changes. Retain DNS/mapping evidence, authenticated workload results, Spark application identity and independent lifecycle records throughout. Continued `SELECT 1` alone does not prove that the new destination became usable; an unchanged application display name alone does not exclude a restart.

Verify deployment support before starting. General workspace network-edit documentation says most network changes require stopped compute; this test applies only to the Advanced DNS operation your deployment supports live. If the UI requires a stop, record **BLOCKED** and do not bypass it. Older deployments/clusters can have an adoption prerequisite: confirm any one-time restart with the administrator, record it separately, then begin a fresh measured run. A restart during the measured change sequence fails the no-restart objective. Subsequent adoption does not retroactively turn a failed run into a pass.

## Public product reference

The [AIDP workspace guide](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/workspaces.html) describes private networking, Advanced DNS and workspace-edit prerequisites. Verify the behavior available in your deployment before executing a case. See [DNS resolution paths](dns-resolution-paths.md) and [network and IAM examples](network-and-iam-examples.md) for preparation templates.

Copyright (c) 2026 Oracle and/or its affiliates. Licensed under the [Universal Permissive License](../../../LICENSE.txt), Version 1.0.
