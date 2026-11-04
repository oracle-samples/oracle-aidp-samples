# V02: Read an OCI bucket only from the customer VCN

Demonstrate that AIDP can read a private Object Storage object only when the request uses the approved customer VCN. The bucket, workspace and service gateway are in the same OCI region and tenancy.

## How the IAM restriction works

| IAM element | What it checks | This example |
| --- | --- | --- |
| Resource principal | **Who signed the request?** | The AIDP instance resource identity. |
| Dynamic group | **Is that resource a member?** | `aidp_v02_reader` contains the selected AIDP instance. |
| Network Source | **Where did the request originate?** | `aidp_v02_vcn` contains the customer VCN and allowed subnet range. |
| Policy | **What may this caller do, from that source?** | Read the named bucket only when the Network Source matches. |

The dynamic group and Network Source are independent IAM objects. The policy connects them. Neither object grants access by itself. Putting an AIDP resource in the dynamic group does not put its requests in the allowed network.

For this grant, **caller is a group member AND bucket matches AND source matches** must all be true. IAM evaluates the source condition when Object Storage authorizes the request. It uses the request's VCN/source-address context through the service gateway; the notebook does not send a Network Source name as a credential.

A dynamic group is the identity mechanism chosen for this walkthrough. A policy that directly matches a resource principal can also authorize it without a dynamic group; do not add that alternative grant alongside this test policy.

## Before you begin

Use an existing private-network workspace and a small private test bucket. Have the AIDP administrator confirm that the native `oci://` reader uses the AIDP instance resource principal. A dynamic group does not change the reader's authentication mode. If the reader uses a user credential, stop and resolve that setup before using this resource-principal walkthrough.

Record:

| Value | Obtain from |
| --- | --- |
| `<AIDP_RESOURCE_ID>` | AIDP instance details: its resource OCID, not the workspace or cluster UUID. |
| `<IAM_DOMAIN>` | Identity domain where the dynamic group will be created. |
| `<BUCKET_COMPARTMENT_ID>`, `<BUCKET_NAME>`, namespace | Object Storage bucket details. |
| `<CUSTOMER_VCN_ID>`, `<SOURCE_SUBNET_CIDR>` | The VCN and customer-side source subnet used by the workspace private connection. |
| Object key, size and expected rows | An existing CSV fixture of at most 64 KiB, uploaded by the bucket owner. |

## IAM setup

An existing IAM administrator completes these three steps before the tester runs the notebook.

**Permission to perform setup:** creating a Network Source requires `NETWORK_SOURCE_CREATE` in the target tenancy. Workspace access, VCN management and the bucket-read policy below do not supply this permission. If a separate setup operator will create it, an authorized tenancy administrator can add this policy in the **root compartment**, for an existing group containing that operator:

```text
Allow group <IAM_DOMAIN>/<SETUP_OPERATORS> to manage network-sources in tenancy
```

This is a **human operator group**, not the `aidp_v02_reader` dynamic group used by the notebook. It permits Network Source management; it does not delegate dynamic-group or policy creation. The IAM administrator still performs those steps. For a tenancy without identity domains, omit `<IAM_DOMAIN>/`. Allow permission changes to propagate before retrying.

This `Allow` example assumes the operator's group belongs to the target tenancy. An operator from another tenancy needs corresponding **Endorse** and **Admit** grants for Network Source creation. Access to the target VCN or permission to read all resources does not establish permission to create a Network Source. Have the IAM owners verify that cross-tenancy setup before proceeding.

### 1. Create the dynamic group: who may read

In **Identity & Security > Domains > your domain > Dynamic groups**, create `aidp_v02_reader`. In a tenancy without identity domains, open **Identity & Security > Dynamic Groups**. Enter this matching rule, replacing the placeholder:

```text
ALL {resource.id = '<AIDP_RESOURCE_ID>'}
```

Use `resource.id` for the AIDP resource, not a Compute instance matching rule. This selects one resource; it does not include every resource in the compartment.

### 2. Create the Network Source: where requests may originate

Sign in to the tenancy that owns the customer VCN. In that tenancy's **home region**, open **Identity & Security > Network Sources > Create Network Source**. Network Sources belong to the **tenancy/root compartment**, not the bucket or workspace compartment. For API/CLI creation, `compartmentId` / `--compartment-id` must be the **tenancy OCID** (`<TENANCY_OCID>`).

| Field | Value |
| --- | --- |
| Name | `aidp_v02_vcn` |
| Network type | **Virtual Cloud Network** |
| VCN OCID | `<CUSTOMER_VCN_ID>` |
| IP address/CIDR | `<SOURCE_SUBNET_CIDR>`, such as `10.20.1.0/24` in an illustrative network |

Add no public IP range for this test. The VCN ID and source range form a pair: the same IP range in a different VCN does not match. Do not enter the Object Storage destination IP.

**If creation reports “Authorization failed or requested resource not found”:** verify the signed-in operator's group has the setup grant above, the target is the correct tenancy root, and the VCN OCID belongs to that tenancy. Check the home region and any defined-tag permissions as well; omit optional defined tags when testing creation. Capture the failed operation, HTTP status/error code and `opc-request-id` if it still fails. Being able to list Network Sources does not prove permission to create one. A dynamic-group bucket-read grant cannot resolve a setup-operator authorization failure.

### 3. Create the policy: permission plus source restriction

Open **Identity & Security > Policies** in the bucket compartment or an authorized ancestor. Create a policy using the manual editor:

```text
Allow dynamic-group <IAM_DOMAIN>/aidp_v02_reader to read objects in compartment id <BUCKET_COMPARTMENT_ID> where all {target.bucket.name='<BUCKET_NAME>', request.networkSource.name='aidp_v02_vcn'}
Allow dynamic-group <IAM_DOMAIN>/aidp_v02_reader to read buckets in compartment id <BUCKET_COMPARTMENT_ID> where all {target.bucket.name='<BUCKET_NAME>', request.networkSource.name='aidp_v02_vcn'}
```

For a tenancy without identity domains, use `dynamic-group aidp_v02_reader`. The first statement permits object reads/listing; the second permits bucket metadata checks. No write grant is needed.

Review all effective grants for this resource and bucket, including inherited policies and other dynamic-group memberships. An additional unconditional allow can bypass this restriction. Choose an isolated test bucket if existing grants cannot be safely scoped. Allow IAM and dynamic-group membership changes to propagate, then keep these definitions unchanged throughout the test.

## Network setup

| Component | Required configuration |
| --- | --- |
| Workspace | Existing private connection to the customer VCN/subnet recorded above; platform private-network prerequisites already satisfied. |
| Service gateway | Enabled Object Storage service, or All Services label including Object Storage, in this same region. |
| Source subnet route | Destination type **Service**, matching regional service label, target the service gateway. |
| Egress controls | Stateful TCP 443 to the regional service label through the applicable subnet/NSG/firewall rules. |

Expected application path: **AIDP -> workspace private connection -> customer subnet -> service gateway -> Object Storage**. The service-gateway path supplies the customer VCN source context needed by this policy.

## Customer steps

1. **Record the baseline.** Save the dynamic-group rule, Network Source, effective policies, route and full workspace DNS/SCAN settings. If the Object Storage hostname is already configured, preserve it and label the run as validation of an existing setup.
2. **Configure the read.** Attach the approved test compute. Run **Test helpers** and **Your configuration** once. Set the fixture URI, endpoint and expected rows. Both region fields must match the workspace region.
3. **Run before.** Set `RUN_LIVE=True` and `PHASE="before"` in **Choose phase**, then run the read cell. Retain the result.
4. **Apply the hostname setting.** Edit the workspace, open **Advanced options**, and add the exact Object Storage hostname used by the reader, for example `objectstorage.us-ashburn-1.oraclecloud.com`. Preserve all other entries. Follow any required compute-stop action, save, and reopen the settings to confirm. Keep IAM and routes unchanged.
5. **Run after.** Set `PHASE="after_add"` and repeat the same read with the same identity and object. Expect the exact fixture rows. Use a fresh connection as needed after DNS caches expire. If a required restart resets the session, export the earlier result before restarting.
6. **Restore.** Remove only the hostname introduced by this run and verify the original DNS/SCAN settings. Run `PHASE="restored"`, then export results. Stop compute only if you started it for this test.

Use **Choose phase** between reads; do not rerun the configuration cell and clear earlier results.

## Review the result

| Caller and path | Expected authorization, assuming no other grant allows access |
| --- | --- |
| Selected AIDP resource, allowed VCN/subnet through the service gateway | Read allowed. |
| Selected AIDP resource, outside the allowed Network Source | Read denied. |
| Resource outside the dynamic group, allowed VCN/subnet | This policy grants no read access. |

For each attempted read, correlate the object and UTC time with the bucket's **Read Access Events** when available. Record the requestor `principalId`, request ID and result; confirm that the requestor matches the dynamic-group rule. Review the source/path evidence together with the unchanged policy. Log delivery is best effort; missing records do not prove denial.

A timeout or DNS error is not evidence of an IAM denial. If the same authorized customer path already worked before the edit, report successful existing-configuration validation, not a deny-to-allow transition. A CSV read alone does not prove that the Network Source restricted access.

Retain the evidence privately. Remove temporary IAM objects only after the agreed test is complete and after confirming they are not used elsewhere.

## References

- [Dynamic-group matching rules](https://docs.oracle.com/en-us/iaas/Content/Identity/dynamicgroups/Writing_Matching_Rules_to_Define_Dynamic_Groups.htm)
- [Network Sources](https://docs.oracle.com/en-us/iaas/Content/Identity/Tasks/managingnetworksources.htm)
- [Network Source setup permissions](https://docs.oracle.com/en-us/iaas/Content/Identity/policyreference/iampolicyreference.htm)
- [Cross-tenancy operator policies](https://docs.oracle.com/en-us/iaas/Content/Identity/policieshow/iam-cross-domain.htm)
- [Service-gateway source restrictions](https://docs.oracle.com/en-us/iaas/Content/Network/Tasks/servicegateway.htm)
- [Object Storage permissions](https://docs.oracle.com/en-us/iaas/Content/Identity/Reference/objectstoragepolicyreference.htm)
- [Object Storage request log fields](https://docs.oracle.com/en-us/iaas/Content/Logging/Reference/details_for_object_storage.htm)

Copyright (c) 2026 Oracle and/or its affiliates. Licensed under the [Universal Permissive License](../../../LICENSE.txt), Version 1.0.
