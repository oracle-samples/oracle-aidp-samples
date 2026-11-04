# V02: Access a bucket and Autonomous Database from the customer VCN

Read a small CSV from a private Object Storage bucket and query an existing Autonomous Database through the same approved customer VCN. The workspace, bucket, database public endpoint and service gateway are in one OCI region. Create a dedicated test bucket in your chosen compartment; reuse the database.

## How the source restrictions work

| Target | Who authenticates? | What restricts the connection source? | Successful operation |
| --- | --- | --- | --- |
| Object Storage | AIDP resource principal, selected by a dynamic group | IAM policy condition `request.networkSource.name='aidp_v02_vcn'` | Read the exact CSV rows. |
| Autonomous Database public endpoint | Existing database user through an approved catalog or wallet connection | The database's **VCN access control list**, plus database login privileges | Return the expected database service and session details from `DUAL`. |

A Network Source is an IAM definition of allowed request origins; it does not connect to a service or grant access by itself. For the bucket, the policy combines **identity + named bucket + allowed source**. The service-gateway path supplies the customer VCN/source context.

The database check uses a password-authenticated SQL connection. Its VCN ACL is configured separately: the IAM Network Source name is **not** an ADB SQL access rule. Putting `request.networkSource.name` on an `autonomous-databases` management policy does not restrict this SQL session. This notebook tests two source controls for the same customer network and reports their results separately.

## Before you begin

Have the AIDP administrator confirm the native `oci://` reader's effective identity. This walkthrough selects the AIDP instance resource principal; a dynamic-group rule cannot change a reader that actually uses user credentials.

Record the following before starting compute:

| Value | Obtain from |
| --- | --- |
| `<TEST_COMPARTMENT_ID>` | The compartment for the new bucket and existing database. |
| `<AIDP_RESOURCE_ID>`, `<IAM_DOMAIN>` | AIDP instance OCID and the dynamic group's identity domain. |
| `<CUSTOMER_VCN_ID>`, `<SOURCE_SUBNET_CIDR>` | Workspace private-connection VCN and actual customer-side source subnet. |
| `<BUCKET_NAME>`, namespace, object key | New private bucket and the supplied `fixtures/sample.csv`; it contains two rows and is 24 bytes. |
| ADB public hostname, port and service name | **Database connection > Public endpoint** for the existing ADB. Copy the selected descriptor's service name, not the display name. |
| Catalog key, or wallet ZIP/TNS/user | An approved connection to that **public endpoint**. Verify the effective descriptor before setting `public_endpoint_confirmed=True`. |

Keep secrets out of notebook cells and outputs. Prefer an existing catalog. If using a wallet, supply the password through a private runtime `ADB_PASSWORD` environment variable. Do not download credentials into the test bucket.

## IAM and database setup

Complete these steps before the reads. The notebook does not create policies or change the database.

### 1. Verify the operator's setup permissions

An IAM administrator creates the dynamic group, Network Source and bucket policy. A bucket owner creates the private bucket and uploads `sample.csv`. A database owner verifies the ACL and connection.

Creating a Network Source requires `NETWORK_SOURCE_CREATE` at the tenancy root. For a setup group in the same tenancy, an administrator can delegate management with:

```text
Allow group <IAM_DOMAIN>/<SETUP_OPERATORS> to manage network-sources in tenancy
```

For an operator from another tenancy, verify corresponding **Endorse** and **Admit** grants. A local `Allow any-user` statement does not replace cross-tenancy authorization. Being able to list Network Sources does not prove permission to create them. If creation fails, check the caller, tenancy root, home region and failed request ID.

### 2. Select the bucket reader with a dynamic group

Create `aidp_v02_reader` in **Identity & Security > Domains > your domain > Dynamic groups**, using:

```text
ALL {resource.id = '<AIDP_RESOURCE_ID>'}
```

Use the AIDP **instance OCID**, not a workspace/compute UUID or an `instance.id` rule for a Compute VM. In a tenancy without identity domains, use **Identity & Security > Dynamic Groups**.

### 3. Define the Network Source

In the tenancy's **home region**, open **Identity & Security > Network Sources**. Create or verify:

| Field | Value |
| --- | --- |
| Name | `aidp_v02_vcn` |
| Placement | Tenancy root; API `compartmentId` must be `<TENANCY_OCID>`. |
| Network type | Virtual Cloud Network |
| VCN OCID | `<CUSTOMER_VCN_ID>` |
| IP address/CIDR | `<SOURCE_SUBNET_CIDR>`, for example `10.20.1.0/24` |
| Public sources | Empty |

The VCN and range are a pair. Enter the **source subnet**, not an Object Storage destination address.

### 4. Grant reads from that source to the new bucket

In the test compartment or an authorized ancestor, create:

```text
Allow dynamic-group <IAM_DOMAIN>/aidp_v02_reader to read objects in compartment id <TEST_COMPARTMENT_ID> where all {target.bucket.name='<BUCKET_NAME>', request.networkSource.name='aidp_v02_vcn'}
Allow dynamic-group <IAM_DOMAIN>/aidp_v02_reader to read buckets in compartment id <TEST_COMPARTMENT_ID> where all {target.bucket.name='<BUCKET_NAME>', request.networkSource.name='aidp_v02_vcn'}
```

For a tenancy without identity domains, omit `<IAM_DOMAIN>/`. This grants object reads/listing and bucket metadata checks, not writes.

**Check overlapping grants before claiming enforcement.** A new bucket still inherits compartment/tenancy policies. A broad object grant through another dynamic group can authorize reads outside the Network Source. The IAM owner must exclude the test bucket from such grants or provide an otherwise isolated reader. Preserve shared access to other buckets. Until this is resolved, label the restriction test **BLOCKED**, even if the CSV read succeeds. Allow IAM and dynamic-group changes to propagate before testing.

### 5. Restrict the ADB public endpoint to the customer VCN

Open the existing ADB's **Network > Access control list > Edit**. Verify an approved **Virtual cloud network** entry for `<CUSTOMER_VCN_ID>`. Optionally specify `<SOURCE_SUBNET_CIDR>` to restrict clients further. Review existing consumers before tightening an ACL; preserve approved entries and any existing private endpoint.

Use an existing database account with `CREATE SESSION`; the query reads `DUAL` and changes no data. No `manage autonomous-databases` or `use database-connections` grant is needed for this password-authenticated SQL session. Database-management IAM permissions are required only for an administrator changing the database configuration. IAM token authentication would require a separate database integration and is not used here.

If the database has both public and private access, select **Public endpoint** explicitly. A successful query through the private descriptor does not validate this public-endpoint VCN ACL.

## Network setup

| Component | Required configuration |
| --- | --- |
| Workspace | Existing private connection to the recorded customer VCN/subnet. |
| Service gateway | **All services in the regional Oracle Services Network**, covering Object Storage and the ADB public endpoint. An Object Storage-only gateway label is insufficient for both checks. |
| Source subnet route | Destination type **Service**, matching the regional All Services label, target the service gateway. |
| Egress | Stateful TCP 443 for Object Storage and the ADB descriptor's TCP port, commonly 1521 for TLS or 1522 for mTLS. Retain applicable workspace prerequisites and return traffic. |
| Workspace hostnames | Exact Object Storage hostname and ADB **public** hostname used by the clients. Retain the original hostnames for TLS identity verification. |

Application paths:

- AIDP -> workspace private connection -> customer subnet -> service gateway -> Object Storage -> IAM Network Source check.
- AIDP -> workspace private connection -> customer subnet -> service gateway -> ADB public endpoint -> VCN ACL and database login.

## Customer steps

1. **Prepare and record.** Complete IAM, bucket, ADB and network setup. Record all existing DNS/SCAN entries. If a target hostname is already present, preserve it and label that target as an existing-configuration validation.
2. **Initialize and configure.** Attach the approved compute. Run the code cell under **Initialize V02** and wait for **V02 initialized**. Fill `CONFIG` for the CSV and `ADB_CONFIG` for the database. Verify the catalog/wallet public descriptor; set its exact hostname, port and service, then set `public_endpoint_confirmed=True`. Run **Your configuration**.
3. **Run the baseline.** In **Choose phase**, set `RUN_LIVE=True` and `PHASE="before"` and run that cell. Run **Read the bucket object** and **Query the database**. Save both results; their outcomes may differ.
4. **Apply the missing hostname entries.** Edit the workspace's **Advanced options** and add only the two exact client hostnames that are missing. Preserve all existing entries. Follow any required compute-stop action, save, and reopen to verify. Keep IAM, ACLs, identities and routes unchanged.
5. **Repeat both operations.** Set `PHASE="after_add"` and rerun both read cells. Expect the exact CSV rows and the expected database service. Refresh connections after DNS caching as needed. Export earlier results before any required session restart.
6. **Restore and compare.** Remove only entries introduced by this test. Set `PHASE="restored"`, repeat both reads, and export. Leave pre-existing entries intact. Stop compute if you started it solely for this test.

Use **Choose phase** between attempts; rerunning the configuration cell clears results. Keep the test bucket until evidence is reviewed, then have its owner remove the fixture and bucket. Preserve the reused database and catalog.

**After a session restart or compute reattachment:** rerun **Initialize V02**, **Your configuration**, and **Choose phase** before either read cell. Restore your intended phase and live flag. Earlier in-memory results are lost after a session restart, so export them beforehand.

**If a read reports that V02 is not initialized:** run the code cell under **Initialize V02**, wait for its confirmation, and retry the read. Initialization alone preserves any configuration and results still in the current session. This is a notebook execution-order issue; the read has not reached Object Storage or ADB.

## Review the result

| Target | From the approved customer source | From a source outside its configured rule |
| --- | --- | --- |
| Bucket, same authorized reader and no bypass grant | Exact fixture rows | Object read denied by IAM |
| ADB public endpoint, same valid database login | Expected service and session details | Connection rejected by the ADB ACL |

A successful CSV read or SQL query alone does not prove source enforcement. For Object Storage, correlate the UTC time/object with **Read Access Events**, checking request ID and `principalId`; review effective IAM grants and service-gateway evidence. For ADB, retain the public descriptor, ACL snapshot, service-gateway evidence, returned service/user/client address, and a controlled outside-source comparison where available. A client address alone does not prove the VCN route. Missing best-effort logs, DNS errors and timeouts do not establish an IAM denial.

The notebook reports functional results separately and leaves `source_path` for evidence review. The SQL result does not claim that IAM Network Sources control database login, or that a client certificate was presented. If an authorized path already works at baseline, report existing-configuration validation rather than a deny-to-allow transition.

## References

- [Network Sources](https://docs.oracle.com/en-us/iaas/Content/Identity/Tasks/managingnetworksources.htm)
- [Network Source setup permissions](https://docs.oracle.com/en-us/iaas/Content/Identity/policyreference/iampolicyreference.htm)
- [Cross-tenancy operator policies](https://docs.oracle.com/en-us/iaas/Content/Identity/policieshow/iam-cross-domain.htm)
- [Dynamic-group matching rules](https://docs.oracle.com/en-us/iaas/Content/Identity/dynamicgroups/Writing_Matching_Rules_to_Define_Dynamic_Groups.htm)
- [Object Storage permissions](https://docs.oracle.com/en-us/iaas/Content/Identity/Reference/objectstoragepolicyreference.htm)
- [Object Storage request log fields](https://docs.oracle.com/en-us/iaas/Content/Logging/Reference/details_for_object_storage.htm)
- [ADB VCN access-control rules](https://docs.oracle.com/en-us/iaas/autonomous-database-serverless/doc/access-control-rules-autonomous.html)
- [Service gateway](https://docs.oracle.com/en-us/iaas/Content/Network/Tasks/servicegateway.htm)

Copyright (c) 2026 Oracle and/or its affiliates. Licensed under the [Universal Permissive License](../../../LICENSE.txt), Version 1.0.
