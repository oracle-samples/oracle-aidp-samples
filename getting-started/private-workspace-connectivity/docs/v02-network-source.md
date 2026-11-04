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

Have the AIDP administrator confirm the native `oci://` reader's effective identity. The policy example below assumes the effective Object Storage caller is the AIDP instance resource principal. Verify that assumption with the AIDP administrator and service request evidence before using that matching rule. A runtime signer can be a service compute principal or part of a delegated request chain; its class name alone does not identify the final Object Storage caller. A dynamic-group rule does not change the reader’s identity.

Record the following before starting compute:

| Value | Obtain from |
| --- | --- |
| `<TEST_COMPARTMENT_ID>` | The compartment for the new bucket and existing database. |
| `<AIDP_INSTANCE_OCID>` | **AI Data Platform > instance Actions > View Details > OCID**; see step 2 below. |
| `<IAM_DOMAIN>` | Identity domain containing the dynamic group. |
| `<CUSTOMER_VCN_ID>`, `<SOURCE_SUBNET_CIDR>` | Workspace private-connection VCN and actual customer-side source subnet. |
| `<BUCKET_NAME>`, namespace, object key | New private bucket and the supplied `fixtures/sample.csv`; it contains two rows and is 24 bytes. |
| ADB public hostname, port and service name | **Database connection > Public endpoint** for the existing ADB. Copy the selected descriptor's service name, not the display name. |
| Catalog key, or wallet ZIP/TNS/user | An approved connection to that **public endpoint**. Verify the effective descriptor before setting `public_endpoint_confirmed=True`. |

Keep secrets out of notebook cells and outputs. Prefer an existing catalog. If using a wallet, supply the password through a private runtime `ADB_PASSWORD` environment variable. Do not download credentials into the test bucket.

## Which ADB name belongs in the configuration?

**Use the exact `HOST` from the selected database connection descriptor.** FQDN means the full DNS hostname; “ADB DNS” alone does not distinguish a listener hostname from a DNS zone or a database service name.

| Connection value | Example | Where it belongs |
| --- | --- | --- |
| Public listener `HOST` | `adb.<region>.oraclecloud.com` | `ADB_CONFIG["hostname"]`; this is the exact name whose public-endpoint route V02 checks. |
| Private listener `HOST` | `<private-label>.adb.<region>.oraclecloud.com` | A private-endpoint connection. It selects a different path and does not test the public VCN ACL. |
| TNS alias | `<database>_public_high` in a refreshed dual-access wallet | Selects a descriptor from `tnsnames.ora`; it is not a DNS hostname. Existing catalogs may retain an older alias: inspect its descriptor. |
| `SERVICE_NAME` | `<service-id>_<database>_high.adb.oraclecloud.com` | `ADB_CONFIG["expected_service"]`; it selects the database service after connecting to HOST/PORT. Do not resolve it as the listener hostname. |
| DNS zone / wildcard | `*.adb.<region>.oraclecloud.com` | Resolver-policy scope, not the single HOST value for this test. Do not broaden the workspace configuration to this wildcard. |

In **Autonomous Database > Database connection**, select **Public endpoint** and the desired connection profile. Copy its `HOST`, `PORT` and `SERVICE_NAME` separately. Verify the catalog or selected wallet alias uses that descriptor, then set `public_endpoint_confirmed=True`. The diagnostic cannot inspect the descriptor stored inside a managed catalog or verify that checkbox independently. Do not paste passwords, wallet contents or a complete credential-bearing JDBC URL into cells.

For the workspace DNS entry, use the plain exact HOST, without `https://`, a port, a path or a wildcard. An existing service-zone allowance does not by itself make public traffic originate in the customer VCN. Check the **completed** workspace configuration and runtime result. An entry that is omitted after an update needs AIDP administrator review; adding it as a SCAN proxy entry is not an equivalent fix.

## IAM and database setup

Complete these steps before the reads. The optional permission-test cells temporarily change an existing bucket grant or database ACL, then restore it; they do not create resources.

### 1. Verify the operator's setup permissions

An IAM administrator creates the dynamic group, Network Source and bucket policy. A bucket owner creates the private bucket and uploads `sample.csv`. A database owner verifies the ACL and connection.

Creating a Network Source requires `NETWORK_SOURCE_CREATE` at the tenancy root. For a setup group in the same tenancy, an administrator can delegate management with:

```text
Allow group <IAM_DOMAIN>/<SETUP_OPERATORS> to manage network-sources in tenancy
```

For an operator from another tenancy, verify corresponding **Endorse** and **Admit** grants. A local `Allow any-user` statement does not replace cross-tenancy authorization. Being able to list Network Sources does not prove permission to create them. If creation fails, check the caller, tenancy root, home region and failed request ID.

### 2. Select the bucket reader with a dynamic group

**AIDP has an OCI resource identifier at the instance level.** `resource.id` is the IAM matching-rule variable for that OCID; it is not a field to look for on a workspace or compute cluster.

Find the value before creating the group:

1. In the OCI Console, select the tenancy and region containing the AIDP instance. Open **Analytics & AI > AI Data Platform**.
2. Locate the instance that hosts your workspace. Open its **Actions** menu and select **View Details**.
3. Copy the complete **OCID** into `<AIDP_INSTANCE_OCID>`. Its prefix is `ocid1.aidataplatform.`; older instances can use `ocid1.datalake.`. Keep the identifier exactly as displayed. If the instance is not listed, check the region/compartment filter and ask the AIDP administrator for its instance details.

These steps are illustrated in [Oracle's AIDP IAM documentation](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/iam-policies-oracle-ai-data-platform.html). The [Get AI Data Platform API/CLI](https://docs.oracle.com/en-us/iaas/tools/oci-cli/latest/oci_cli_docs/cmdref/ai-data-platform/ai-data-platform/get.html) also identifies the instance by its OCID.

Then create `aidp_v02_reader` in **Identity & Security > Domains > your domain > Dynamic groups**. Enter this rule manually and replace the placeholder with the copied OCID:

```text
ALL {resource.id = '<AIDP_INSTANCE_OCID>'}
```

This selects the **AIDP instance resource principal**, not an individual workspace. Workspace/compute UUIDs and the URL's `TENANT_ID` cannot be substituted for this OCID. Keep `resource.id`; `instance.id` is used for OCI Compute VMs. In a tenancy without identity domains, use **Identity & Security > Dynamic Groups**.

**Confirm the reader before testing enforcement.** Finding the instance OCID does not prove that the native `oci://` read is authorized as that principal. Have the AIDP/IAM administrator verify the effective Object Storage caller and any delegation with a correlated read event. If it differs, this rule does not select that caller: resolve the identity mapping before claiming the Network Source test passed. Do not substitute a service-owned compute OCID merely because it appears in a runtime token. See [Verify the customer VCN path](#verify-the-customer-vcn-path).

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

**Check overlapping grants before claiming enforcement.** A new bucket still inherits compartment/tenancy policies. A broad object grant through another dynamic group can authorize reads outside the Network Source. The IAM owner must exclude the test bucket from such grants or provide an otherwise isolated reader. Preserve shared access to other buckets. Until this is resolved, a successful CSV read means **access succeeded; source not yet verified**. The permission test below detects an overlapping read grant when the read continues to succeed during its deny phase. Allow IAM and dynamic-group changes to propagate before testing.

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

Verify the effective connection after applying the hostname entries. An accepted entry alone does not establish that the deployed AIDP configuration routes a regional public ADB endpoint through the customer VCN. If a valid public connection still returns `ORA-12529`, retain the ACL and resolve the source-path prerequisite before the switching test. Do not substitute the private endpoint for this public-ACL test.

Application paths:

- AIDP -> workspace private connection -> customer subnet -> service gateway -> Object Storage -> IAM Network Source check.
- AIDP -> workspace private connection -> customer subnet -> service gateway -> ADB public endpoint -> VCN ACL and database login.

## Customer steps

1. **Prepare and record.** Complete IAM, bucket, ADB and network setup. Record all existing DNS/SCAN entries. If a target hostname is already present, preserve it and label that target as an existing-configuration validation.
2. **Initialize and configure.** Attach the approved compute. Run the code cell under **Initialize V02** and wait for **V02 initialized**. Fill `CONFIG` for the CSV and `ADB_CONFIG` for the database. Verify the catalog/wallet public descriptor; set its exact hostname, port and service, then set `public_endpoint_confirmed=True`. Run **Your configuration**.
3. **Run the baseline.** In **Choose phase**, set `RUN_LIVE=True` and `PHASE="before"` and run that cell. Run **Read the bucket object** and **Query the database**. Save both results; their outcomes may differ.
4. **Apply the missing hostname entries.** Edit the workspace's **Advanced options** and add only the two exact client hostnames that are missing. Preserve all existing entries. Follow any required compute-stop action, save, and reopen to verify **the completed configuration**, not just the initial update response. If the ADB hostname is omitted, stop that target’s DNS comparison and have the administrator establish a supported customer-VCN public-endpoint path. Keep IAM, ACLs, identities and routes unchanged.
5. **Repeat both operations.** Set `PHASE="after_add"` and rerun both read cells. Expect the exact CSV rows and the expected database service. Refresh connections after DNS caching as needed. Export earlier results before any required session restart.
6. **Restore and compare.** Remove only entries introduced by this test. Set `PHASE="restored"`, repeat both reads, and export. Leave pre-existing entries intact. Stop compute if you started it solely for this test.

Use **Choose phase** between attempts; rerunning the configuration cell clears results. Keep the test bucket until evidence is reviewed, then have its owner remove the fixture and bucket. Preserve the reused database and catalog.

**After a session restart or compute reattachment:** rerun **Initialize V02**, **Your configuration**, and **Choose phase** before either read cell. Restore your intended phase and live flag. Earlier in-memory results are lost after a session restart, so export them beforehand.

**If a read reports that V02 is not initialized:** run the code cell under **Initialize V02**, wait for its confirmation, and retry the read. Initialization alone preserves any configuration and results still in the current session. This is a notebook execution-order issue; the read has not reached Object Storage or ADB.

## Troubleshoot the ADB connection

Run **Troubleshoot ADB** after setting the exact HOST and PORT and `RUN_LIVE=True`. It changes no DNS, IAM or ACL settings and needs no OCI administrator credentials.

1. **Check DNS and TCP first.** Leave `RUN_ADB_DIAGNOSTIC_QUERY=False`. The cell compares Python and Spark JVM DNS answers and attempts TCP handshakes to up to four resolved addresses with a three-second connection timeout each. JDBC uses the JVM; different answers can also result from DNS rotation or caching.
2. **Optionally compare saved settings.** Set `ADB_WORKSPACE_SNAPSHOT_FILE` to the private JSON produced by `collect_v02_route_snapshot.py`. The report states whether the exact hostname appears and shows the capture time. This is a snapshot, not a live read or proof of the running resolver configuration. Refresh it after a workspace update.
3. **Test SQL with the existing connection.** Verify the public catalog/wallet descriptor, set `public_endpoint_confirmed=True`, then set `RUN_ADB_DIAGNOSTIC_QUERY=True`. The cell runs the bounded identity query and reports Oracle error codes without raw credential-bearing exception text. A password is never requested or printed by this diagnostic.
4. **Follow the reported next action.** Export the diagnostic with the UTC time, compute identity, hostname/port and sanitized configuration for the administrator investigating the failure. Keep the existing source restriction while diagnosing it.

| Observation | What it tells you | Customer action |
| --- | --- | --- |
| Host missing, URL, alias or SERVICE_NAME used as HOST | Connection fields need correction. | Copy HOST and PORT from the selected public descriptor. |
| DNS lookup fails | That runtime could not resolve the configured name. | Check spelling and have the administrator inspect effective workspace DNS forwarding. |
| Python/JVM answers differ | The runtimes observed different address sets. | Compare repeated lookups and caching; this alone does not prove stale DNS or require a restart. |
| DNS resolves but TCP fails | The listener was not reached at the tested address/port. | Check the descriptor port, egress rules, Service Gateway route and workspace private connection. |
| TCP succeeds; `ORA-12529` / `ORA-12506` | A connection filter rejected the SQL request. | Verify the actual descriptor, ADB VCN ACL and effective customer-VCN path with the AIDP administrator. The code alone does not identify the source VCN. |
| `ORA-01017`, `ORA-28000`, `ORA-28001` | Login/account problem. | Have the database owner check the credential and account state. |
| `ORA-29024`, `ORA-28759`, `ORA-28860` | Certificate, wallet or TLS problem. | Check the selected descriptor and approved wallet; retain certificate validation. |
| `ORA-12514` / `ORA-12505` | Listener/service selection problem. | Check the TNS alias and SERVICE_NAME against current connection information. |
| SQL returns the expected service | The selected SQL connection works. | Run the separate source-control comparison before claiming customer-VCN enforcement. |

DNS answers identify destinations. A public address or a mapped `255.x.x.x` address alone does not identify the connection source. A TCP handshake does not prove TLS, mTLS, SQL login or the customer VCN route. The diagnostic is scoped to the driver; executor and AI compute paths require their own observations.

## Test the source controls: allow, deny, restore

Keep the target, reader, workspace DNS and routes unchanged during these tests. First make each normal read succeed. Then run one permission test at a time.

| Phase | Bucket | ADB public endpoint | Expected result |
| --- | --- | --- | --- |
| Allow | Existing `read objects` grant with the Network Source condition | Original customer-VCN ACL | Read succeeds. |
| Deny | Temporarily replace **only** `read objects` with `inspect objects` in that exact grant | Temporarily restrict the same VCN to a CIDR that does not overlap the workspace source subnet | Object GET is denied / new database connection is rejected by its ACL. |
| Restore | Restore the original statement | Restore the original ACL exactly | Read succeeds again; ADB returns a different session ID. |

**What this proves.** The bucket cycle uses a fresh native OCI filesystem stream on the driver for each attempt, then checks the Spark fixture. This avoids mistaking a cached Spark result for a new object GET. A completed cycle establishes that the driver GET depends on the grant conditioned on the named customer VCN/source subnet; it does not independently verify executor request sources. `inspect objects` retains metadata access but removes `OBJECT_READ`; the Network Source itself stays unchanged. The ADB cycle establishes that its public VCN ACL admits the source subnet: removing that subnet prevents a new connection and restoring it permits a fresh session. These are service authorization checks, not packet traces. A timeout, DNS error or unchanged successful read does not count as the expected denial.

### Operator permissions and credentials

The administrator switching controls is separate from the AIDP principal reading the object and the database user running SQL. A dynamic group for the reader does not give notebook Python an administrator signer.

Use an approved, private OCI operator profile on an administrator-controlled runtime with the OCI Python SDK installed. Set `ADMIN_CONFIG_FILE` and `ADMIN_PROFILE` to that existing profile. **Do not upload a personal OCI config, token or private key to a shared Workbench folder.** If Workbench has no approved private operator credential mechanism, run the controller on the administrator host and execute the displayed read probes in Workbench between changes. Keep a copy of the recovery journal on that host. A short-lived operator session must have at least 20 minutes remaining before either cycle starts.

An IAM administrator can grant the operator the following privileges, scoped to the existing test policy and ADB. The `where` condition identifies the **existing** policy; it does not authorize policy creation.

```text
Allow group <IAM_DOMAIN>/<TEST_OPERATORS> to read network-sources in tenancy
Allow group <IAM_DOMAIN>/<TEST_OPERATORS> to manage policies in compartment id <POLICY_COMPARTMENT_ID> where all {target.policy.id='<BUCKET_POLICY_OCID>', any {request.permission='POLICY_READ', request.permission='POLICY_UPDATE'}}
Allow group <IAM_DOMAIN>/<TEST_OPERATORS> to inspect autonomous-databases in compartment id <DB_COMPARTMENT_ID> where target.id='<ADB_OCID>'
Allow group <IAM_DOMAIN>/<TEST_OPERATORS> to use autonomous-databases in compartment id <DB_COMPARTMENT_ID> where all {target.id='<ADB_OCID>', request.permission='AUTONOMOUS_DATABASE_UPDATE', request.operation.actiontype='whitelistedIps'}
```

For a policy located at the tenancy root, replace `in compartment id <POLICY_COMPARTMENT_ID>` with `in tenancy`. For a cross-tenancy operator, the IAM administrator must configure matching Endorse/Admit authorization. These operator privileges manage controls; they do not authorize the notebook's SQL login.

### Run the bucket permission test

1. Complete `BUCKET_CONTROL`: exact policy OCID, full original `read objects` statement, bucket name, Network Source OCID/name, VCN OCID and source subnet CIDR. The source must contain only that VCN/subnet and no public alternatives.
2. Set `RUN_BUCKET_PERMISSION_TEST=True`. Run **Bucket: allow, deny, restore**. The cell first reads the fixture; if that fails it changes nothing.
3. The cell changes the selected grant to `inspect objects`, retries the same bounded read while IAM propagates (up to `max_wait_seconds`, 120 seconds by default), then restores `read objects` in `finally`. It preserves the statement count and unrelated policy statements.
4. Expect `ALLOWED → DENIED → ALLOWED`, `SOURCE_CONTEXT_VERIFIED` and `Restoration: RESTORED`. If the negative read still succeeds, the result is `INCONCLUSIVE`: review overlapping grants and propagation rather than declaring source verification.

### Run the ADB ACL test

1. Verify the catalog/wallet uses the public descriptor. Complete `ADB_CONTROL` with database/VCN OCIDs, the workspace source CIDR and a different, nonoverlapping CIDR in that VCN. For example, source `10.20.1.0/24`, negative-test CIDR `10.20.2.0/24`.
2. Confirm a maintenance window for clients of the shared public endpoint. Set both `RUN_ADB_ACL_TEST=True` and `ADB_INTERRUPTION_APPROVED=True`.
3. Run **ADB: allow, deny, restore**. It requires a successful baseline query and an original public ACL containing only the selected VCN (optionally limited to the source CIDR).
4. During the negative phase the API value is `["<CUSTOMER_VCN_ID>;10.20.2.0/24"]`. The semicolon separates the VCN from its CIDR. **An empty ACL is never used.** Only `whitelisted_ips` changes; private endpoint settings, NSGs, database accounts and credentials stay intact.
5. Expect an explicit ACL rejection (`ORA-12529` or `ORA-12506`), followed by a successful query after restoration with a new database session ID. A timeout or reused session makes the result `INCONCLUSIVE`.

### Restoration and recovery

Both controllers write the original settings to a private JSON journal **before** the first change, use ETags, and restore in `finally`. They verify the restored setting by reading it back. A terminated kernel or expired operator session can prevent that code from running. Retain the journal outside the notebook session and keep an administrator session available throughout the test.

If the result is `RESTORE_REQUIRED`, renew the operator session if necessary and run **Restore from journal** for the affected target. Do not start another cycle or overwrite that journal until restoration is verified. If another administrator changed the same grant/ACL, compare the journal and current settings and resolve that change with the owner; the helper will not overwrite an unexpected edit.

## Verify the customer VCN path

**A DNS answer is a destination, not the request's source.** AIDP can return translated, service-side destination addresses such as `255.x.x.x` for its managed private connection. You should not expect those answers to be in your customer subnet. The customer subnet is used on the customer side of that connection:

```text
Notebook -> AIDP mapped destination -> private connection
         -> customer VCN/subnet -> customer service gateway -> Object Storage
```

The allow/deny/restore cells above provide the primary source-control test. The optional diagnostic below provides configuration and request-log evidence when available. Run **Read the bucket object**, then **Verify the customer VCN path**. The new cell makes no additional network requests. It shows four separate observations: driver DNS answers, the configured customer path, the bucket read, and whether the customer source is confirmed. It never treats `dns: PASS` as route confirmation.

**1. Collect the configuration.** From an administrator's terminal with the OCI Python SDK and `requests` installed, run the sample's read-only collector. Use an existing OCI CLI profile; keep credentials outside the notebook.

```bash
python collect_v02_route_snapshot.py \
  --profile <OCI_PROFILE> --region <WORKSPACE_REGION> \
  --instance-id <AIDP_INSTANCE_ID> --workspace-id <WORKSPACE_ID> \
  --network-source-id <NETWORK_SOURCE_ID> --output v02-route-snapshot.json
```

Upload the JSON into your private Workbench folder and set `ROUTE_SNAPSHOT_FILE` to its `/Workspace/...` path. The collector reads the workspace DNS/subnet association, subnet route table, regional service gateway and Network Source. It changes nothing. `CONFIGURED_IN_SNAPSHOT` confirms those recorded settings, not a packet's path. Collect again after changing the workspace or network.

**2. Correlate a real request.** In OCI Logging, find the test bucket's **Read Access Events** for the read's UTC interval and exact object. If read logging is not enabled, the owner must enable it for this bucket and repeat the read; earlier requests cannot be reconstructed. Copy only these fields into `OBJECT_STORAGE_EVENT`, retaining their original values:

```python
OBJECT_STORAGE_EVENT = {
    "bucketName": "", "namespace": "", "objectName": "",
    "requestAction": "GET", "statusCode": 200, "apiType": "native",
    "isPar": False, "opcRequestId": "", "principalId": "",
    "clientIpAddress": "", "startTime": ""
}
```

Use `namespaceName` if that is the field in your event. Do not copy the log's `credentials` field. A matching successful object GET identifies the actual reader and the source address seen by Object Storage. An address alone does not identify its VCN.

**3. Verify the source restriction.** Have the IAM administrator review all applicable grants and dynamic-group memberships for that logged principal. Only after confirming this principal can read this object solely under the named customer-VCN Network Source, complete `SOURCE_REVIEW`, set `no_bypass_grants=True`, and reference the review record. The Network Source must contain only that VCN, with no public-source alternatives. Keep the policy and route unchanged during the read. The snapshot and review timestamps must be within 15 minutes of the read for this diagnostic.

`CONFIRMED_BY_REVIEWED_SOURCE_CONTROL` requires all three records plus a successful matching read. This establishes the bucket request's customer-VCN source through the service gateway using IAM source enforcement. It is not a packet trace and does not confirm the separate ADB connection. Missing or mismatched evidence leaves `NOT_CONFIRMED` and explains the next action.

The collector needs existing AIDP workspace-view access plus network and Network Source read access. Log reviewers also need access to the selected log compartment. Examples for the administrator to scope to the review team:

```text
Allow group <IAM_DOMAIN>/<REVIEWERS> to read virtual-network-family in compartment id <NETWORK_COMPARTMENT_ID>
Allow group <IAM_DOMAIN>/<REVIEWERS> to read network-sources in tenancy
Allow group <IAM_DOMAIN>/<REVIEWERS> to read log-groups in compartment id <LOG_COMPARTMENT_ID>
Allow group <IAM_DOMAIN>/<REVIEWERS> to read log-content in compartment id <LOG_COMPARTMENT_ID>
```

## Review the result

| Target | From the approved customer source | From a source outside its configured rule |
| --- | --- | --- |
| Bucket, same authorized reader and no bypass grant | Exact fixture rows | Object read denied by IAM |
| ADB public endpoint, same valid database login | Expected service and session details | Connection rejected by the ADB ACL |

A successful CSV read or SQL query alone does not prove source enforcement. Use the allow/deny/restore cycle above. As optional corroborating evidence for Object Storage, correlate the UTC time/object with **Read Access Events**, checking request ID and `principalId`; review effective IAM grants and service-gateway evidence. For ADB, retain the public descriptor, ACL snapshot, service-gateway evidence, returned service/user/client address, and a controlled outside-source comparison where available. A client address alone does not prove the VCN route. Missing best-effort logs, DNS errors and timeouts do not establish an IAM denial.

A normal read reports `ACCESS_SUCCEEDED_SOURCE_NOT_VERIFIED`; that is an incomplete source check, not a blocked read. The permission cycle reports `SOURCE_CONTEXT_VERIFIED` only when denial, recovery and restoration pass. Otherwise it reports `INCONCLUSIVE`, `NEEDS_SETUP` or `NOT_RUN`, with the next action. The SQL result does not claim that IAM Network Sources control database login, or that a client certificate was presented. If an authorized path already works at baseline, report existing-configuration validation rather than a deny-to-allow transition.

## References

- [Network Sources](https://docs.oracle.com/en-us/iaas/Content/Identity/Tasks/managingnetworksources.htm)
- [Network Source setup permissions](https://docs.oracle.com/en-us/iaas/Content/Identity/policyreference/iampolicyreference.htm)
- [Cross-tenancy operator policies](https://docs.oracle.com/en-us/iaas/Content/Identity/policieshow/iam-cross-domain.htm)
- [Dynamic-group matching rules](https://docs.oracle.com/en-us/iaas/Content/Identity/dynamicgroups/Writing_Matching_Rules_to_Define_Dynamic_Groups.htm)
- [Object Storage permissions](https://docs.oracle.com/en-us/iaas/Content/Identity/Reference/objectstoragepolicyreference.htm)
- [Object Storage request log fields](https://docs.oracle.com/en-us/iaas/Content/Logging/Reference/details_for_object_storage.htm)
- [ADB administration IAM permissions and ACL-only action](https://docs.oracle.com/en/cloud/paas/autonomous-database/serverless/adbsb/autonomous-database-iam-policies.html)
- [ADB public/private descriptors and TNS aliases](https://docs.oracle.com/en-us/iaas/autonomous-database-serverless/doc/private-endpoints-autonomous.html)
- [ORA-12529 filtering rejection](https://docs.oracle.com/en/error-help/db/ora-12529/)
- [ADB VCN access-control rules](https://docs.oracle.com/en-us/iaas/autonomous-database-serverless/doc/access-control-rules-autonomous.html)
- [Service gateway](https://docs.oracle.com/en-us/iaas/Content/Network/Tasks/servicegateway.htm)

Copyright (c) 2026 Oracle and/or its affiliates. Licensed under the [Universal Permissive License](../../../LICENSE.txt), Version 1.0.
