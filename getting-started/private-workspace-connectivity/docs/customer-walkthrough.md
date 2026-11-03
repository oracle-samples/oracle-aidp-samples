# Customer walkthrough: V01 through V04

Start with one use case. Each notebook contains its own network diagram, setup examples, executable checks and cleanup steps. Opening a notebook does not change your network or start compute.

| Your requirement | Open | DNS choice | Application traffic |
| --- | --- | --- | --- |
| Read a private Amazon S3 object | V01_Amazon_S3.ipynb | Explicit entry for the actual public S3 hostname | Workspace private connection -> customer subnet -> NAT -> S3 HTTPS |
| Keep an OCI IAM Network Source restriction | V02_Customer_Network_Source.ipynb | Explicit entry for the in-region service hostname | Workspace private connection -> customer subnet -> regional service gateway -> OCI service |
| Read Object Storage in another OCI region | V03_Cross_Region_Object_Storage.ipynb | Explicit entry for the remote regional hostname | Workspace private connection -> customer subnet -> NAT -> remote Object Storage HTTPS |
| Use a database private endpoint | V04_Service_Private_Endpoint.ipynb | Default private DNS proxy for the private-only database name | Workspace private connection -> customer private route -> database listener |

AIDP manages CoreDNS. You choose the private network and, when needed, destination FQDNs in workspace settings. You do not install a CoreDNS server. DNS chooses the destination address; routes carry application traffic, security rules allow connections, and service credentials/IAM authorize the operation.

## Common Workbench actions

### Prepare once

1. **Network administrator:** identify an existing customer VCN/subnet with the required route and security rules. Verify the AIDP private-network prerequisites and the destination controls in the notebook's appendix.
2. **Service owner:** provide a small test object or an actual database private endpoint, plus the existing runtime authorization. A local OCI CLI profile is not the Spark runtime's service identity.
3. **Tester:** open **Workspaces**, select the test workspace, and inspect its private network configuration. Record the VCN, subnet, NSGs, complete Advanced DNS list and SCAN settings. Retain a screenshot or configuration export as the baseline.
4. Open the selected notebook from **Shared**. Use its compute selector to attach the intended test compute. Start it only when the target, permissions and network are ready. Record whether it was already running.
5. Read the numbered steps below. Run **Test helpers** once, fill **Your configuration**, then run that configuration cell once. Keep `RUN_LIVE=False` while preparing. Use the separate **Choose phase** cell for subsequent runs; do not rerun configuration and clear your earlier results.

### Add an explicit destination for V01, V02 or V03

1. In the workspace's edit form, expand **Advanced options** and enable the DNS-name control if necessary. Enter the exact hostname from the client's endpoint, for example `objectstorage.<region>.oraclecloud.com`. Enter no `https://`, path, port, IP address or wildcard.
2. Preserve all existing DNS and SCAN entries. If the exact entry already exists, record that baseline; do not delete an in-use entry to manufacture a failing test.
3. Save the workspace change. Follow any compute-stop requirement shown by the deployment. Inspect the operation result, then reopen the form and confirm the saved entry. A saved row alone is not proof that destination mapping succeeded.
4. Return to the same notebook. If a required restart cleared the session, save your previous results privately and initialize a new session. Otherwise retain `RESULTS`. Set the next phase and rerun **Run a workload phase** with a fresh connection/client as needed after DNS caches expire.

Explicit destinations must resolve from the AIDP service environment during mapping creation. Private-only names that need the customer resolver follow the default private DNS proxy path. Do not add a private database hostname as a troubleshooting shortcut.

### Understand the output

| Output | Customer decision |
| --- | --- |
| `functional: PASS` | The exact fixture rows or expected database identity were returned. |
| `functional: BLOCKED` | Complete the named prerequisite, then repeat this phase. |
| `functional: FAIL` | Inspect the error privately; verify endpoint, route, permission and credential separately. |
| `source_path: BLOCKED` | Supply and review independent path evidence before claiming the intended route. This can coexist with a successful read. |
| `mtls: BLOCKED` in V04 | The query does not prove client-certificate use. Review transport evidence only if making an mTLS claim. |

Keep the time, phase and operation result. A DNS answer or open TCP port is not an authenticated test. A successful functional check can still leave the overall case `INCOMPLETE` pending path/configuration evidence.

## V01 - Amazon S3

**Customer outcome:** read a private S3 object through the customer network's outbound path.

1. **Prepare S3.** Ask the bucket owner for an existing private bucket, its AWS region and a test object such as `dns-test/sample.csv`. Upload the supplied two-row fixture using your normal AWS tooling if an approved test object is needed. Record its byte size. Keep Block Public Access enabled. The notebook reads only; it does not create a bucket or AWS VPC.
2. **Prepare the OCI path.** In OCI Console, open the workspace subnet's route table. Verify `0.0.0.0/0 -> NAT gateway`, a usable NAT gateway, and permitted HTTPS egress to S3 through the applicable NSGs/security lists/firewall. Use the IAM examples in the appendix for the AWS runtime identity.
3. **Prepare the reader.** Have the compute administrator configure a supported S3A credential provider and matching Hadoop/AWS libraries before testing. Keep keys/tokens out of notebook source. Record the actual regional endpoint and any bucket hostname the client contacts.
4. **Run before the change.** Fill the configuration table below. Run helpers and configuration once; set `RUN_LIVE=True` and `PHASE="before"` in **Choose phase**. Run the workload cell. Save the result, including a real failure if one occurs. A pre-existing successful route is a valid baseline.
5. **Apply DNS and repeat.** Add only the required actual S3 FQDNs using the common Workbench actions. Set `PHASE="after_add"`; rerun the same fixture read with the same identity. Expect `functional: PASS` and the exact two rows. Review NAT/network evidence separately.
6. **Restore.** Remove only entries introduced by this run, preserve the original list and SCAN settings, set `PHASE="restored"`, and repeat. The restored result should match the original behavior; it may fail if the baseline required the added entry. Export the evidence. Stop compute only if you started it for this test.

| CONFIG field | Value to supply |
| --- | --- |
| `workspace_label`, `compute_label` | Names shown in Workbench; these labels do not select compute. |
| `uri` | `s3a://<your-bucket>/dns-test/sample.csv` |
| `endpoint` | Regional S3 endpoint used by the client, such as `s3.us-east-1.amazonaws.com`. |
| `hostname` | Actual name to probe; include a bucket-specific name if that is the client's destination. Configure every required client hostname in Workbench. |
| `region` | Bucket's AWS region, such as `us-east-1`. |
| `workspace_region` | Your OCI workspace region. |
| `fixture_size_bytes` | Verified object size; the supplied sample is 24 bytes. |
| `expected_rows` | Keep the supplied alpha/beta rows only when using the supplied sample. |

**Completion:** exact object contents plus saved DNS change and reviewed customer NAT path. An S3 permission error is not evidence that DNS failed.

## V02 - Customer Network Source

**Customer outcome:** read an Object Storage bucket in the workspace's region while IAM permits that read only from the selected customer VCN. **Set up IAM first; then change only DNS during the comparison.**

1. **IAM administrator: prepare authorization.** Complete the V02 IAM setup: identify the Spark reader's principal, create the VCN Network Source, and apply the bucket-scoped read policy with `request.networkSource.name`. Confirm no broader grant bypasses the restriction. The notebook includes the policy statements before its executable cells; in the repository, use [V02 IAM setup](network-and-iam-examples.md#v02--customer-network-source-and-service-gateway).
2. **Network administrator: prepare routing.** Verify the workspace's customer subnet has a route to Object Storage through the **same-region service gateway**, plus TCP 443 egress. Retain the complete original workspace DNS/SCAN configuration and existing route/security settings.
3. **Tester: prepare the read.** Fill CONFIG with the private test bucket, known CSV object and actual regional endpoint. Set both region fields to the workspace region. Run helpers and configuration once. Start the approved test compute only after IAM and network setup are complete.
4. **Run `before`.** In **Choose phase**, set `RUN_LIVE=True` and `PHASE="before"`, then run **Run a workload phase**. Keep the result and UTC time. If the exact Advanced DNS entry already exists, preserve it and record an existing-configuration baseline.
5. **Run `after_add`.** Add the exact regional Object Storage hostname to workspace Advanced DNS, following the supported save/lifecycle steps. Reopen the saved settings. Keep the same policy, Network Source, identity, object and route; set `PHASE="after_add"` and rerun. Expect the two fixture rows. Review the matching service request and effective policy before declaring the source restriction demonstrated.
6. **Run `restored`.** Remove only the entry introduced by this test, verify the original DNS/SCAN configuration, and rerun with `PHASE="restored"`. Export the observations before stopping compute that you started. Reuse existing IAM/network resources; remove temporary test grants only after evidence collection and according to the agreed cleanup plan.

| Phase | Expected observation |
| --- | --- |
| `before` | A request arriving outside the allowed VCN is denied by the effective source-restricted policy. A DNS failure, timeout or wrong credential alone does not prove IAM denial. An already-working customer path can succeed. |
| `after_add` | Exact fixture rows returned; same runtime principal, bucket and effective source restriction; request and route records support the customer VCN/service-gateway path. |
| `restored` | Original DNS/SCAN configuration and baseline behavior restored. |

If all phases succeed because the route was already configured, report **existing-configuration validation**, not a demonstrated deny-to-allow transition. If IAM setup or source evidence is missing, the Network Source demonstration remains incomplete even when the CSV read succeeds.

| CONFIG field | Value to supply |
| --- | --- |
| `uri` | `oci://<bucket>@<namespace>/dns-test/sample.csv` |
| `endpoint` | Actual in-region endpoint URL, such as `https://objectstorage.us-ashburn-1.oraclecloud.com`. |
| `hostname` | The endpoint hostname without the `https://` prefix. |
| `region`, `workspace_region` | The same verified OCI region. |
| `fixture_size_bytes`, `expected_rows` | Verified size and known rows of that exact object. |
| `workspace_label`, `compute_label` | The selected workspace and test compute names. |

**Completion:** exact object contents, unchanged effective Network Source restriction, saved DNS evidence and reviewed source/path evidence. V03 covers the remote bucket; V04 covers ADB. Their authorization differences and policy examples are explained in [Object Storage and ADB authorization](network-and-iam-examples.md#object-storage-and-adb-authorization).

## V03 - Cross-region Object Storage

**Customer outcome:** read an Object Storage fixture in region B from a workspace in region A, while keeping an independent region A read working.

1. **Select two objects.** Obtain a small private fixture in region B and a separate control object in region A. Record each object's bucket, namespace, key, size, endpoint and expected rows. For example, choose Ashburn as A and Phoenix as B; the sample does not require this pair.
2. **Prepare the remote route.** Verify the workspace subnet's NAT/approved internet-egress path to region B on TCP 443. Keep the region A service-gateway route for the local control. A local service gateway does not reach a different region. This public-endpoint example does not require a DRG.
3. **Verify authorization.** Confirm the actual connector signer can read both buckets. Use the remote-bucket and local-control policy examples in the appendix. If the remote bucket has a source restriction, verify it matches the chosen NAT/egress context.
4. **Run both baselines.** Fill `CONFIG` with the remote fixture and `CONTROL_CONFIG` with the separate in-region fixture. Run helpers/configuration once. Set `RUN_LIVE=True`, `PHASE="before"`; run **Run a workload phase** and **Run the in-region control**. Save both results.
5. **Apply remote DNS and repeat both.** Add the actual region B endpoint FQDN using the common actions. Set `PHASE="after_add"`; rerun both cells with the same signer. Expect exact rows from region B and an unchanged successful region A control. The reader applies a bucket-specific endpoint option; review effective client/target evidence to confirm use of region B. Recreate a cached client/session when needed and retain prior results privately.
6. **Restore and compare.** Remove only the remote entry added by this run. Set `PHASE="restored"`; repeat both cells and export all results. The local control should remain successful. The remote result should match its original baseline.

| Configuration | Value to supply |
| --- | --- |
| `CONFIG["uri"]` | `oci://<region-b-bucket>@<namespace>/dns-test/sample.csv` |
| `CONFIG["endpoint"]`, `CONFIG["hostname"]` | Region B's actual URL and hostname, for example `https://objectstorage.us-phoenix-1.oraclecloud.com` and `objectstorage.us-phoenix-1.oraclecloud.com`. |
| `CONFIG["region"]`, `CONFIG["workspace_region"]` | Different regions B and A respectively. |
| `CONTROL_CONFIG` | Separate region A URI, endpoint, hostname, size and expected rows. Set its `region` and `workspace_region` to A. |
| Both dictionaries | The same workspace/compute labels and each object's verified size and expected rows. |

**Completion:** correct remote contents, successful independent local control and evidence of the effective remote endpoint/path. The endpoint text in CONFIG alone is not proof of regional routing.

## V04 - Private database baseline and preservation

**Customer outcome:** use an existing database private endpoint, then confirm that unrelated Advanced DNS edits preserve that connection.

1. **Verify the endpoint type.** In OCI Console, open the ADB's network configuration and Database Connection details. Confirm an actual private endpoint, private hostname, subnet and listener port. A public regional endpoint with an ACL is not this case. Obtain the expected database instance identity independently from the database owner.
   If converting an existing public database, use **More actions > Update network access > Private endpoint access only**, select the approved VCN/subnet and database NSG, then save after reviewing its current consumers. A private endpoint with retained restricted public access can also be tested, provided this notebook uses the private descriptor. Obtain refreshed connection information and a new wallet where required; do not reuse the old public descriptor for V04.
2. **Verify private routing and DNS.** Confirm local private routing or the existing peering/DRG and return routes to the database subnet. Allow the actual listener port in the workspace egress and database ingress controls. Verify customer DNS can resolve the private hostname. AIDP's default private-endpoint DNS proxy handles private-only names unresolved by the service resolver. Add no database-specific Advanced DNS entry for this baseline.
3. **Prepare the existing connection.** Prefer an approved external catalog and its `catalog_id`. Otherwise supply an existing wallet ZIP, TNS alias and database user, and arrange a private runtime `ADB_PASSWORD` environment variable. Preserve server identity and TLS requirements. The query needs login permission and reads `DUAL`; it does not need database-management IAM rights.
4. **Run the baseline.** Fill CONFIG, run helpers/configuration once, then set `RUN_LIVE=True`, `PHASE="before"`. Run the identity query. Expect the database owner's `expected_instance` and `functional: PASS`. If this fails, resolve the private route/DNS/authentication first.
5. **Exercise unrelated edits.** Select two owned, service-resolvable test hostnames A and B unrelated to the database. Add A and run `after_add`; replace A with B and run `after_edit`; remove B and run `after_delete`. Follow the common Workbench save/readback steps each time. Repeat the same database query, keep the database connection settings fixed, and retain all results. V04 permits required lifecycle actions; it is not a no-restart test.
6. **Restore and review.** Verify the complete original DNS/SCAN settings are restored. Run `restored`. Fill the evidence template with actual snapshots and references for all five phases, then enable `RUN_EVIDENCE_CHECKS` and evaluate. Expect the same database identity in every phase. Export results and review private-endpoint/default-proxy evidence separately.

| CONFIG field | Value to supply |
| --- | --- |
| `hostname`, `port` | Actual private database hostname and listener port from its connection descriptor. |
| `catalog_id` | Existing AIDP external catalog key; use the catalog connected to this private endpoint. |
| `wallet_zip`, `tns`, `user` | Only for the wallet alternative; leave unused fields empty when using a catalog. |
| `expected_instance` | Database instance identity supplied by the owner, not guessed from the ADB display name. |
| `workspace_label`, `compute_label` | The selected workspace and compute. |

**Completion:** all five identity queries pass, actual unrelated edits are evidenced, and original DNS/SCAN configuration is restored. A wallet-configured query does not independently prove mTLS. Record only the transport evidence you have.

## License

Copyright (c) 2026 Oracle and/or its affiliates. Licensed under the [Universal Permissive License](../../../LICENSE.txt), Version 1.0.
