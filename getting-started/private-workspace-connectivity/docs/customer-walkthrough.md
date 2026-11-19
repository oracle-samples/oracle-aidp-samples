# Customer walkthrough: V01 through V04

Start with one use case. Each notebook contains its own network diagram, setup examples, executable checks and cleanup steps. Opening a notebook does not change your network or start compute.

| Your requirement | Open | DNS choice | Application traffic |
| --- | --- | --- | --- |
| Read a private Amazon S3 object | V01_Amazon_S3.ipynb | Explicit entry for the actual public S3 hostname | Workspace private connection -> customer subnet -> NAT -> S3 HTTPS |
| Restrict bucket and ADB public access to the customer VCN | V02_Customer_Network_Source.ipynb | Explicit entry for the in-region service hostname | Workspace private connection -> customer subnet -> regional service gateway -> OCI service |
| Read Object Storage in another OCI region | V03_Cross_Region_Object_Storage.ipynb | Explicit entry for the remote regional hostname | Workspace private connection -> customer subnet -> NAT -> remote Object Storage HTTPS |
| Use a database private endpoint | V04_Service_Private_Endpoint.ipynb | Default private DNS proxy for the private-only database name | Workspace private connection -> customer private route -> database listener |

AIDP manages CoreDNS. You choose the private network and, when needed, destination FQDNs in workspace settings. You do not install a CoreDNS server. DNS chooses the destination address; routes carry application traffic, security rules allow connections, and service credentials/IAM authorize the operation.

| Use case | Authorization first | Routing evidence to collect |
| --- | --- | --- |
| V01 | AWS reader and private S3 bucket permissions | Customer VNIC flow logs plus S3 access logs or CloudTrail object data events |
| V02 | Runtime reader, bucket Network Source policy, and separate ADB public ACL/database login | Source-control tests, customer flows and Object Storage read events |
| V03 | Verified runtime reader granted both remote and local bucket access | Remote-region and local-region Object Storage read events plus customer flows |
| V04 | Existing database user and reviewed private connection descriptor | Private-endpoint flows, SQL identity results and five DNS-edit snapshots |

Each notebook includes its own setup and troubleshooting. Configure logging before execution; the notebooks do not enable logs. Normal read cells do not change IAM; V02 includes separately enabled administrator control tests. Oracle Support can use workspace/update request IDs and UTC intervals to trace the DNS workflow.

## Common Workbench actions

### Prepare once

1. **Network administrator:** identify an existing customer VCN/subnet with the required route and security rules. Verify the AIDP private-network prerequisites and the destination controls in the notebook's setup sections.
2. **Service owner:** provide a small test object or an actual database private endpoint, plus the existing runtime authorization. A local OCI CLI profile is not the Spark runtime's service identity.
3. **Tester:** open **Workspaces**, select the test workspace, and inspect its private network configuration. Record the VCN, subnet, NSGs, complete Advanced DNS list and SCAN settings. Retain a screenshot or configuration export as the baseline.
4. Open the selected notebook from **Shared**. Use its compute selector to attach the intended test compute. Start it only when the target, permissions and network are ready. Record whether it was already running.
5. Read the numbered steps below. Run **Initialize V01/V02/V03/V04** for your selected notebook once, fill **Your configuration**, then run that configuration cell once. Keep `RUN_LIVE=False` while preparing. Use the separate **Choose phase** cell for subsequent runs; do not rerun configuration and clear your earlier results.

### Add an explicit destination for V01, V02 or V03

1. In the workspace's edit form, expand **Advanced options** and enable the DNS-name control if necessary. Enter the exact hostname from the client's endpoint, for example `objectstorage.<region>.oraclecloud.com`. Enter no `https://`, path, port, IP address or wildcard.
2. Preserve all existing DNS and SCAN entries. If the exact entry already exists, record that baseline; do not delete an in-use entry to manufacture a failing test.
3. Save the workspace change. Follow any compute-stop requirement shown by the deployment. Inspect the operation result, then reopen the form and confirm the saved entry. A saved row alone is not proof that destination mapping succeeded.
4. Return to the same notebook. If a required restart cleared the session, save your previous results privately and initialize a new session. Otherwise retain `RESULTS`. Set the next phase and rerun the object-read or database-query cell with a fresh connection/client as needed after DNS caches expire.

Explicit destinations must resolve from the AIDP service environment during mapping creation. Private-only names that need the customer resolver follow the default private DNS proxy path. Do not add a private database hostname as a troubleshooting shortcut.

### Understand the output

| Output | Customer decision |
| --- | --- |
| `functional: PASS` | The exact fixture rows or expected database identity were returned. |
| `functional: SETUP_REQUIRED` | Complete the named prerequisite, then repeat this phase. |
| `functional: FAIL` | Inspect the error privately; verify endpoint, route, permission and credential separately. |
| `source_path: NOT_ASSESSED` in V01/V03/V04 | Supply and review independent path evidence before claiming the intended route. This can coexist with a successful read. |
| `mtls: NOT_ASSESSED` in V04 | The query does not prove client-certificate use. Review transport evidence only if making an mTLS claim. |

Keep the time, phase and operation result. A DNS answer or open TCP port is not an authenticated test. V01/V03/V04 report `READ_PASSED` for successful reads and `NOT_ASSESSED` for unreviewed path evidence. V02 uses its own source-control results. Review configuration and routing evidence before accepting the complete scenario.

## V01 - Amazon S3

Read the [complete case setup and troubleshooting](v01-s3.md).

1. Complete AWS authorization, connector, network and logging prerequisites. Save the complete workspace DNS/SCAN baseline.
2. Attach the intended compute. Run **Initialize V01**, fill **Your configuration**, then run that configuration cell once. Leave credentials out of the dictionaries.
3. Set `RUN_LIVE=True` and `PHASE="before"` under **Choose phase**. Run the driver diagnostic and the S3 read. Record the real result; an already successful baseline is valid.
4. Add only the missing S3 client hostname entries. Save, inspect the update result, reopen the workspace and verify the final configuration. Follow any required compute lifecycle action; do not assume changes propagate to every running compute.
5. Use a fresh client/session as needed for caching. Set `PHASE="after_add"`; repeat the same object read with unchanged AWS identity and endpoint. Correlate its UTC interval with both sides' logs.
6. Restore only entries introduced by this test. Set `PHASE="restored"`, repeat and export. Preserve earlier results before any session restart, then initialize and configure the new session.

## V02 - Bucket and ADB access from the customer VCN

Read a new private bucket under a Network Source policy and query an existing ADB public endpoint under its VCN ACL. Follow the [standalone V02 setup](v02-network-source.md) before execution.

1. **Prepare and record.** Complete IAM, bucket, ADB and network setup. Record all existing DNS/SCAN entries. If a target hostname is already present, preserve it and label that target as an existing-configuration validation.
2. **Configure both targets.** Run the initialization cell and **Your configuration** once. Fill `CONFIG` for the CSV and `ADB_CONFIG` for the database. Verify the catalog/wallet public descriptor; set its exact hostname, port and service, then set `public_endpoint_confirmed=True`.
3. **Run the baseline.** Attach the approved compute. In **Choose phase**, set `RUN_LIVE=True` and `PHASE="before"`. Run **Read the bucket object** and **Query the database**. Save both results; their outcomes may differ.
4. **Apply the missing hostname entries.** Edit the workspace's **Advanced options** and add only the two exact client hostnames that are missing. Preserve all existing entries. Follow any required compute-stop action, save, and reopen to verify. Keep IAM, ACLs, identities and routes unchanged.
5. **Repeat both operations.** Set `PHASE="after_add"` and rerun both read cells. Expect the exact CSV rows and the expected database service. Refresh connections after DNS caching as needed. Export earlier results before any required session restart.
6. **Restore and compare.** Remove only entries introduced by this test. Set `PHASE="restored"`, repeat both reads, and export. Leave pre-existing entries intact. Stop compute if you started it solely for this test.

Use **Choose phase** between attempts; rerunning the configuration cell clears results. Keep the test bucket until evidence is reviewed, then have its owner remove the fixture and bucket. Preserve the reused database and catalog.

## V03 - Cross-region Object Storage

Read the [complete case setup and troubleshooting](v03-cross-region.md).

1. Complete both bucket permissions, routes and logging setup. Save complete workspace DNS/SCAN settings.
2. Attach compute and run **Initialize V03**. Fill **Your configuration** for region B and **In-region control configuration** for region A. Run each configuration cell once.
3. Set `RUN_LIVE=True`, `PHASE="before"`. Run the remote diagnostic/read and the local control. Save both results even if the remote read fails.
4. Add only the missing region B endpoint FQDN in the workspace. Save and inspect the operation outcome, then reopen to verify final settings. Preserve the region A route and DNS entries. Follow any required compute lifecycle action.
5. Refresh the client/session when caches can retain earlier endpoints. Set `PHASE="after_add"` and repeat both reads using the same identity. Record both regional request events and network evidence.
6. Remove only the remote entry introduced by this test. Set `PHASE="restored"`, repeat both reads and export. The local control must remain successful throughout; the remote restored result should match its original behavior.

## V04 - Private database baseline and preservation

Read the [complete case setup and troubleshooting](v04-private-adb.md).

1. Verify the private descriptor, database user and private route. Save the complete original DNS/SCAN settings and record any pre-existing overrides.
2. Attach compute and run **Initialize V04**. Fill **Your configuration** with the approved catalog or wallet details and independently obtained `expected_instance`. Set `private_endpoint_confirmed=True` only after descriptor review.
3. Set `RUN_LIVE=True`, `PHASE="before"`. Run the driver diagnostic and a fresh authenticated identity query. Fix a failed baseline before changing workspace DNS.
4. Add unrelated hostname A. Save, inspect the work request and reopen final settings. Set `PHASE="after_add"`; repeat the unchanged database query and save the full DNS/SCAN snapshot.
5. Replace A with B; repeat as `after_edit`. Remove B; repeat as `after_delete`. Follow any required compute lifecycle action. Keep the database hostname, descriptor, credentials, routes and security rules fixed.
6. Restore and verify the complete original settings. Run `restored`. Fill the five-phase evidence template using actual observations, enable `RUN_EVIDENCE_CHECKS`, and evaluate. Export results and evidence before ending the session.

These five phases test preservation across supported edits. They do not establish automatic propagation to every running cluster or AI compute. A required restart is recorded as part of the procedure; start a new session only after saving earlier results.

## License

Copyright (c) 2026 Oracle and/or its affiliates. Licensed under the [Universal Permissive License](../../../LICENSE.txt), Version 1.0.
