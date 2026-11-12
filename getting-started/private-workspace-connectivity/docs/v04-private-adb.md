# V04: Private ADB access and preservation during DNS edits

Establish an authenticated connection to an existing ADB private endpoint. Keep that connection configuration fixed while adding, replacing and deleting unrelated DNS entries, then verify restoration. Private database connectivity is the baseline; preserving it across those edits is the enhancement check.

## Prerequisites

Use an existing ADB private endpoint and an approved private-endpoint catalog or wallet connection. Copy `HOST`, `PORT` and `SERVICE_NAME` from **Database connection > Private endpoint**. A public endpoint protected by an ACL is a different connection path, even if the database also has private access.

Record the actual private hostname, database VCN/subnet/NSGs, expected database instance identity from the owner, workspace/compute, and full DNS/SCAN baseline. A TNS alias selects a descriptor; SERVICE_NAME selects the database service. Neither is the listener hostname to resolve.

Prepare two owned, service-resolvable test FQDNs A and B unrelated to the database. They must be safe to add/remove without affecting another workload. Verify existing private-hostname overrides with the administrator before attributing the baseline to the default customer DNS path.

## IAM and database setup

The SQL connection uses the existing database user through the catalog or wallet. OCI management IAM and IAM Network Sources do not authorize this password-authenticated SQL session. A dynamic group is not required solely for this query.

The database owner can grant an approved existing test user permission to log in:

```sql
GRANT CREATE SESSION TO <TEST_DB_USER>;
```

The test reads `DUAL` to check the database identity. It requires no application-table grants or database-management IAM rights. Preserve the configured TLS requirements and server-certificate verification. Supply a wallet password/database password through the approved private runtime mechanism, never in notebook source.

An operator inspecting the ADB metadata can use an appropriately scoped management policy:

```text
Allow group <IAM_DOMAIN>/<DB_INSPECTORS> to read autonomous-databases in compartment <DB_COMPARTMENT>
```

Changing an existing ADB's network mode is outside this test. Have its owner prepare private access after reviewing current consumers. Keep any existing restricted public access unchanged. Confirm the selected catalog/wallet uses the private descriptor before setting `private_endpoint_confirmed=True`.

## Network setup

| Component | Required configuration |
| --- | --- |
| Private path | Workspace private connection -> customer VCN -> ADB private endpoint. Use local routing for the same VCN; for another VCN, verify approved peering/DRG and return routes. |
| Workspace egress | Stateful TCP to the database descriptor's actual listener port and destination. |
| Database NSG/security | Stateful ingress on that listener port from the actual customer-side private-connection source. For example: source `<CUSTOMER_SOURCE_CIDR>`, TCP destination `<DB_PORT>`. |
| DNS | Customer DNS must resolve the private hostname. For private-only names unresolved on the service side, verify the default private-endpoint DNS proxy path. |
| Workspace entries | Add no database hostname merely because it is private. Do not replace this connection with the public descriptor or add a SCAN proxy as a shortcut. |

NAT/internet egress is not required for the illustrated private database path. Preserve existing platform network prerequisites and any needed DNS forwarding. A mapped DNS address alone does not prove the selected private path.

## Logging setup

Capture flow logs on the private connection's customer-side VNIC/subnet and, where available, the ADB private-endpoint VNIC/subnet. Capture source/destination TCP port from the private descriptor, including accepted and rejected flows, with 100% sampling during validation. Verify the endpoint-to-VNIC association before interpreting a record.

Record a fresh SQL connection's UTC interval and query result in each phase. The database owner can correlate available connection/audit evidence. Object Storage Read Access Events are not evidence for this SQL connection. A client-IP field or TCP handshake alone does not prove successful authentication or mTLS.

The support trace has two purposes: establish the effective private DNS/PE mapping, and inspect each unrelated DNS update's completion. Correlate both with the same compute and test interval. Flow logs omit link-local VCN DNS traffic; configuration/update logs do not themselves establish packet traversal. Disable test-only logs after validation.

### Enable collection before the test

1. Have an administrator create/reuse an approved regional log group. Example logging-operator policies -- replace the domain, group and compartments:

   ```text
   Allow group <IAM_DOMAIN>/<LOG_OPERATORS> to manage log-groups in compartment <LOG_COMPARTMENT>
   Allow group <IAM_DOMAIN>/<LOG_READERS> to read log-content in compartment <LOG_COMPARTMENT>
   Allow group <IAM_DOMAIN>/<LOG_OPERATORS> to {SUBNET_READ, SUBNET_UPDATE} in compartment <NETWORK_COMPARTMENT>
   ```

2. In **Networking > Network Command Center > Flow logs**, select the customer private-connection subnet/VNIC and log group. Use a dedicated flow-log capture filter with TCP source OR destination port the private descriptor port, INCLUDE both accepted/rejected traffic, and 100% sampling. The network administrator supplies the filter; do not edit a shared filter. Confirm the log is enabled and ACTIVE.
3. The database owner identifies the private-endpoint subnet/VNIC and enables equivalent flow collection where supported. Arrange available database connection/audit evidence before the query; this notebook does not change the database auditing policy.
4. In **Logging > Search**, select the correct region, log and UTC interval. Match VNIC, source/destination addresses and ports, protocol, action and capture status. Empty results can reflect delay, capture loss or the wrong VNIC; ACCEPT alone does not establish application authorization. Never copy credential fields into an evidence export.

If setup returns `NotAuthorizedOrNotFound`, verify operator permissions in both resource and log compartments, plus source endorsement/destination admission for cross-tenancy administration. Do not expand the test reader's permissions to solve a logging-operator error. Review retention and logging costs with the resource owner; disable only test-created collection after validation.

See [Logging permissions](https://docs.oracle.com/en-us/iaas/Content/Logging/Task/managinglogs.htm) and [flow-log setup](https://docs.oracle.com/en-us/iaas/Content/Network/Tasks/vcn-flow-logs-enable.htm).

## Customer steps

1. Verify the private descriptor, database user and private route. Save the complete original DNS/SCAN settings and record any pre-existing overrides.
2. Attach compute and run **Initialize V04**. Fill **Your configuration** with the approved catalog or wallet details and independently obtained `expected_instance`. Set `private_endpoint_confirmed=True` only after descriptor review.
3. Set `RUN_LIVE=True`, `PHASE="before"`. Run the driver diagnostic and a fresh authenticated identity query. Fix a failed baseline before changing workspace DNS.
4. Add unrelated hostname A. Save, inspect the work request and reopen final settings. Set `PHASE="after_add"`; repeat the unchanged database query and save the full DNS/SCAN snapshot.
5. Replace A with B; repeat as `after_edit`. Remove B; repeat as `after_delete`. Follow any required compute lifecycle action. Keep the database hostname, descriptor, credentials, routes and security rules fixed.
6. Restore and verify the complete original settings. Run `restored`. Fill the five-phase evidence template using actual observations, enable `RUN_EVIDENCE_CHECKS`, and evaluate. Export results and evidence before ending the session.

These five phases test preservation across supported edits. They do not establish automatic propagation to every running cluster or AI compute. A required restart is recorded as part of the procedure; start a new session only after saving earlier results.

## Configuration

Use the private descriptor's exact `hostname` and `port`. Prefer `catalog_id` for an approved private connection. For the wallet alternative, supply `wallet_zip`, `tns` and `user`, with `ADB_PASSWORD` supplied privately at runtime. Set `expected_instance` to the owner's independently verified identity, not the ADB display name. The diagnostic probes CONFIG's hostname; it cannot inspect a descriptor hidden inside a managed catalog.

## Expected results

All five authenticated identity reads should report `READ_PASSED`, returning the expected instance. The evidence evaluator also checks actual add/edit/delete transitions, complete snapshots and restoration. Set `EVIDENCE["evidence_ref"]` to your private run record, copy the complete before/restored settings as dictionaries containing `workspace_label`, `dns_zones` and `scan`, and set `expected_instance` to the reviewed database identity. In each phase record the functional result, query evidence reference, returned/verified instance and full configuration snapshot. Save work-request references alongside those snapshots. The evaluator checks your recorded observations; it does not fetch them from Workbench. A baseline alone does not complete the preservation test.

`source_path: NOT_ASSESSED` means the private route still needs evidence review. `mtls: NOT_ASSESSED` means client-certificate presentation has not been established; it does not mean the authenticated SQL connection failed. Claim mTLS only with effective transport evidence, not from a password, wallet filename or port number.

## Troubleshooting

| Symptom | Next check |
| --- | --- |
| Private endpoint not confirmed | Review catalog/wallet HOST and PORT against the selected private connection profile. |
| DNS failure or unexpected answer | Private DNS zone/forwarding, effective CoreDNS configuration and exact PE mapping for this compute. |
| TCP timeout | Actual PE VNIC/source, route/return route, database NSG and matching flow ACCEPT/REJECT. |
| ORA-12529 | Check whether the client used a public descriptor and encountered its ACL. Do not loosen the shared ACL to pass a private-endpoint test. |
| Login/certificate error | Database account, TNS alias, current wallet, trust and server-name verification after confirming network reachability. |
| Query succeeds but identity differs | Confirm the selected catalog/service; resolve the target identity before accepting the result. |
| Failure only after unrelated DNS edit | Compare before/after snapshots, completed work request and the actual compute's loaded DNS configuration; restore the baseline. |

Send support the workspace/compute, private FQDN/port, endpoint and VNIC IDs, UTC intervals, update request IDs and sanitized first error. Keep wallet contents and passwords out of reports.

## Restore and export

Restore the full original DNS/SCAN configuration, repeat the final query and export all phases. Disable temporary logs and preserve the reused database, catalog, routes, ACLs and NSGs. Stop compute only if it was started solely for this test.

## References

- [ADB private endpoint configuration](https://docs.oracle.com/en-us/iaas/autonomous-database-serverless/doc/private-endpoints-autonomous.html)
- [ADB connections](https://docs.oracle.com/en-us/iaas/autonomous-database-serverless/doc/connect-introduction.html)
- [VCN flow log fields and limitations](https://docs.oracle.com/en-us/iaas/Content/Logging/Reference/details_for_vcn_flow_logs.htm)
- [VCN flow log setup](https://docs.oracle.com/en-us/iaas/Content/Network/Tasks/vcn-flow-logs-enable.htm)
- [AIDP workspace prerequisites](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/workspaces.html)

Copyright (c) 2026 Oracle and/or its affiliates. Licensed under the [Universal Permissive License](../../../LICENSE.txt), Version 1.0.
