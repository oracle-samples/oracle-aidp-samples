# V03: Cross-region OCI Object Storage

Read an Object Storage fixture in region B from a workspace in region A. Repeat an independent region A read in every phase to detect effects on local access.

## Prerequisites

Choose two private fixtures: one remote and one local. For each, record bucket, namespace, object key, actual region, regional endpoint, verified size up to 64 KiB and expected CSV rows. Regions A and B must differ; the objects must be distinct. Have the bucket owners verify their regions independently of the notebook configuration.

Confirm the native `oci://` connector's effective reader with the AIDP administrator and request evidence. The operator's local OCI profile is not automatically the runtime reader. Record the selected workspace/compute and complete DNS/SCAN baseline.

## IAM setup

Grant the actual runtime reader access to both buckets before testing DNS. If that caller is the AIDP instance resource principal, create or reuse a dynamic group with:

```text
ALL {resource.id = '<AIDP_INSTANCE_OCID>'}
```

Find the instance OCID in **AI Data Platform > instance Actions > View Details**. Use the instance identifier, not a workspace UUID or tenancy ID. The rule selects an identity; it does not change the connector's signer. If the request uses another principal or delegation, the IAM administrator must resolve that identity first.

Example grants for the verified reader:

```text
Allow dynamic-group <IAM_DOMAIN>/<READER_GROUP> to read objects in compartment <REMOTE_BUCKET_COMPARTMENT> where target.bucket.name='<REMOTE_BUCKET>'
Allow dynamic-group <IAM_DOMAIN>/<READER_GROUP> to read objects in compartment <LOCAL_BUCKET_COMPARTMENT> where target.bucket.name='<LOCAL_BUCKET>'
```

If the connector performs bucket metadata operations, grant `read buckets` for those same named buckets. IAM grants apply to their compartment resources; a different bucket region does not by itself require a new policy verb. Cross-tenancy buckets require corresponding source endorsement and destination admission.

This is a regional access test. It does not require an IAM Network Source. Preserve existing source conditions and verify they allow the chosen egress context; remote public-endpoint access through NAT cannot be assumed to carry the local service-gateway VCN context. Keep grants unchanged throughout the DNS comparison.

## Network setup

| Path | Required configuration |
| --- | --- |
| Remote bucket, region B | Customer subnet route through NAT or approved outbound connectivity to the region B endpoint; stateful TCP 443 and return traffic. |
| Local bucket, region A | Preserve the existing regional service-gateway route and access. |
| DNS change | Add only the missing, actual region B Object Storage FQDN. Preserve the local entry and every unrelated DNS/SCAN setting. |
| Connector | Apply the region B endpoint to its bucket/namespace. Keep the local control's region A endpoint independent. |

The region A service gateway only reaches supported services in region A. It is not the route to region B. This public-endpoint design does not require inter-region VCN peering. A customer private peering design needs its own verified routes.

## Logging setup

Enable TCP 443 flow logging on the customer-side private-connection VNIC/subnet in region A, using 100% sampling for the test interval. Identify the VNIC and NAT/egress address from actual resource metadata.

Enable **Read Access Events** on the remote bucket **in region B** and the control bucket **in region A**, using an authorized log group in each region. Search each destination's logs in its own region. Confirm logging is active before the reads; it is not retrospective.

Match each GET by object, UTC interval, principal, client IP and `opcRequestId`. Review the remote event's region and effective connector endpoint; a configured string alone does not establish which regional service handled the read. Correlate the remote flow with customer egress and keep local-control evidence separate. Do not infer an IAM Network Source decision from a successful read. Disable test-only logs after evidence collection.

### Enable collection before the test

1. Have an administrator create/reuse an approved regional log group. Example logging-operator policies -- replace the domain, group and compartments:

   ```text
   Allow group <IAM_DOMAIN>/<LOG_OPERATORS> to manage log-groups in compartment <LOG_COMPARTMENT>
   Allow group <IAM_DOMAIN>/<LOG_READERS> to read log-content in compartment <LOG_COMPARTMENT>
   Allow group <IAM_DOMAIN>/<LOG_OPERATORS> to {SUBNET_READ, SUBNET_UPDATE} in compartment <NETWORK_COMPARTMENT>
   ```

2. In **Networking > Network Command Center > Flow logs**, select the customer private-connection subnet/VNIC and log group. Use a dedicated flow-log capture filter with TCP source OR destination port 443, INCLUDE both accepted/rejected traffic, and 100% sampling. The network administrator supplies the filter; do not edit a shared filter. Confirm the log is enabled and ACTIVE.
3. In each bucket's own region, open **Bucket > Logs** and enable **Read Access Events** in that region's log group. A logging operator also needs resource permissions, for example `Allow group <IAM_DOMAIN>/<LOG_OPERATORS> to use buckets in compartment <BUCKET_COMPARTMENT> where target.bucket.name='<TEST_BUCKET>'`. Repeat for the other bucket. This is the operator grant, separate from the runtime reader's `read objects` grant.
4. In **Logging > Search**, select the correct region, log and UTC interval. Match VNIC, source/destination addresses and ports, protocol, action and capture status. Empty results can reflect delay, capture loss or the wrong VNIC; ACCEPT alone does not establish application authorization. Never copy credential fields into an evidence export.

If setup returns `NotAuthorizedOrNotFound`, verify operator permissions in both resource and log compartments, plus source endorsement/destination admission for cross-tenancy administration. Do not expand the test reader's permissions to solve a logging-operator error. Review retention and logging costs with the resource owner; disable only test-created collection after validation.

See [Logging permissions](https://docs.oracle.com/en-us/iaas/Content/Logging/Task/managinglogs.htm) and [flow-log setup](https://docs.oracle.com/en-us/iaas/Content/Network/Tasks/vcn-flow-logs-enable.htm).

## Customer steps

1. Complete both bucket permissions, routes and logging setup. Save complete workspace DNS/SCAN settings.
2. Attach compute and run **Initialize V03**. Fill **Your configuration** for region B and **In-region control configuration** for region A. Run each configuration cell once.
3. Set `RUN_LIVE=True`, `PHASE="before"`. Run the remote diagnostic/read and the local control. Save both results even if the remote read fails.
4. Add only the missing region B endpoint FQDN in the workspace. Save and inspect the operation outcome, then reopen to verify final settings. Preserve the region A route and DNS entries. Follow any required compute lifecycle action.
5. Refresh the client/session when caches can retain earlier endpoints. Set `PHASE="after_add"` and repeat both reads using the same identity. Record both regional request events and network evidence.
6. Remove only the remote entry introduced by this test. Set `PHASE="restored"`, repeat both reads and export. The local control must remain successful throughout; the remote restored result should match its original behavior.

## Configuration

`CONFIG` describes the remote fixture: `uri="oci://<remote-bucket>@<namespace>/<key>"`, remote `endpoint`, exact `hostname`, `region=B`, and `workspace_region=A`. `CONTROL_CONFIG` describes a separate local object with `region=A`, `workspace_region=A`, and its own endpoint. Set port 443, verified fixture sizes and expected rows for both. Re-running the main configuration cell resets the in-memory result list; use **Choose phase** between attempts.

## Expected results

After the DNS update, both exact fixture reads should report `READ_PASSED`. Every phase includes a remote result and a separately labeled local control. `source_path: NOT_ASSESSED` identifies unreviewed route evidence, not a failed object read. Overall acceptance also requires matching region B request evidence, the intended customer egress path, and successful local reads in all phases.

## Troubleshooting

| Symptom | Next check |
| --- | --- |
| Remote fails, local succeeds | Region B endpoint, NAT/egress, remote-bucket policy and region B logs. |
| Both fail | Shared compute/identity, common subnet security and connector initialization. |
| 403/404 | Runtime principal, bucket namespace/name/key and policy scope; Object Storage can mask an authorization failure as not found. |
| Wrong endpoint despite CONFIG | Existing filesystem-client cache and effective bucket-specific endpoint. Recreate the client/session under the supported procedure. |
| Local fails only after the edit | Compare the complete saved DNS/SCAN settings and local route; stop further edits and restore the baseline. |
| No remote read event in Ashburn | Search the remote bucket's actual region, such as Phoenix, and allow for delivery delay. |

Provide support with both regional request IDs and UTC intervals, workspace/compute identifiers, exact region B FQDN, and the workspace update request ID. The DNS workflow belongs to the workspace region; the remote GET belongs to the bucket region.

## Restore and export

Restore only the DNS changes introduced by this test. Preserve shared bucket contents, grants, local routes and existing logs. Disable temporary collection after validation. Export the remote and local results together, plus reviewed evidence references.

## References

- [Regional service gateway scope](https://docs.oracle.com/en-us/iaas/Content/Network/Tasks/servicegateway.htm)
- [Object Storage policy reference](https://docs.oracle.com/en-us/iaas/Content/Identity/Reference/objectstoragepolicyreference.htm)
- [AIDP instance IAM configuration](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/iam-policies-oracle-ai-data-platform.html)
- [Object Storage read log fields](https://docs.oracle.com/en-us/iaas/Content/Logging/Reference/details_for_object_storage.htm)
- [VCN flow log setup](https://docs.oracle.com/en-us/iaas/Content/Network/Tasks/vcn-flow-logs-enable.htm)
- [OCI HDFS connector](https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/hdfsconnector.htm)

Copyright (c) 2026 Oracle and/or its affiliates. Licensed under the [Universal Permissive License](../../../LICENSE.txt), Version 1.0.
