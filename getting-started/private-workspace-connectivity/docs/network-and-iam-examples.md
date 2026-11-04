# Network and IAM preparation examples

These are preparation templates for an administrator, not provisioning commands executed by the notebooks. Replace every angle-bracket placeholder privately. Labels, address ranges, resource names and policy subjects must come from your environment. Keep the existing route and authorization baseline fixed during comparisons. Reuse approved resources; apply only the examples needed for the chosen topology.

## Shared prerequisites

### Separate operator, service and runtime permissions

The workspace operator needs access to the existing AIDP instance and the appropriate Workbench role/permissions to edit that workspace. Example OCI policies for an identity-domain group are:

```text
Allow group <IDENTITY_DOMAIN>/<WORKSPACE_OPERATORS> to use ai-data-platforms in compartment <AIDP_COMPARTMENT>
Allow group <IDENTITY_DOMAIN>/<WORKSPACE_OPERATORS> to inspect vcns in compartment <VCN_COMPARTMENT>
Allow group <IDENTITY_DOMAIN>/<WORKSPACE_OPERATORS> to inspect subnets in compartment <SUBNET_COMPARTMENT>
Allow group <IDENTITY_DOMAIN>/<WORKSPACE_OPERATORS> to inspect network-security-groups in compartment <NSG_COMPARTMENT>
```

These statements allow instance access and network selection; they do not grant Workbench workspace-edit privileges, create infrastructure or authorize reads from data sources. Use the existing approved Workbench role. Policies belong in the relevant compartment or an ancestor with authority over that scope.

The platform also needs its private-network service permissions. The following adapts the documented AIDP network statements to explicit resource compartments and one instance:

```text
Allow any-user to manage vnics in compartment <VNIC_COMPARTMENT> where all {request.principal.type='<AIDP_PRINCIPAL_TYPE>', request.principal.id='<AIDP_INSTANCE_ID>'}
Allow any-user to use subnets in compartment <SUBNET_COMPARTMENT> where all {request.principal.type='<AIDP_PRINCIPAL_TYPE>', request.principal.id='<AIDP_INSTANCE_ID>'}
Allow any-user to use network-security-groups in compartment <NSG_COMPARTMENT> where all {request.principal.type='<AIDP_PRINCIPAL_TYPE>', request.principal.id='<AIDP_INSTANCE_ID>'}
```

Select the actual instance principal type (`aidataplatform` or the applicable legacy type), instance ID and placement compartments with your administrator. Retain the other existing service setup policies; this is not a full tenancy bootstrap. The runtime reading data can use a different identity from the operator or AIDP service: establish the connector's effective signer before choosing its policy subject.

Sources: [AIDP IAM policies](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/iam-policies-oracle-ai-data-platform.html), [AIDP workspaces](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/workspaces.html), [IAM policy scope](https://docs.oracle.com/en-us/iaas/Content/Security/Reference/iam_security_topic-IAM_Security_Policies.htm).

### Platform network baseline

The current workspace guide requires ingress from the customer VCN address range to all destination ports for private-network data access. An example baseline rule is **ingress; source CIDR `<CUSTOMER_VCN_CIDR>`; all protocols/ports; stateful**, in the security control selected for that workspace topology. Have the administrator reconcile this platform prerequisite with the approved design. The target-specific examples below are additional destination controls, not a replacement for platform rules. Do not use an internet-wide ingress rule.

Retain working DNS resolver access. With custom resolvers, permit UDP and TCP 53 to the approved resolver and configure forwarding/resolution in the customer network. Explicit Advanced DNS provisioning requires service-side resolvability; a successful customer-only lookup does not establish that prerequisite. Private ADB names unresolved by the service resolver use the default private DNS proxy/customer resolver path. Do not add them solely because they are private. Advanced DNS does not create a customer DNS zone, resolver endpoint, route, NAT gateway, service gateway or database listener. Use stateful connection rules in these examples; stateless designs need explicit return rules. Security lists and NSGs are additive, so check all applicable rules.

Source: [OCI security rules](https://docs.oracle.com/en-us/iaas/Content/Network/Concepts/securityrules.htm).

## V01 — Amazon S3 through customer egress

### Read-only AWS identity policy

For a general-purpose private bucket in the same AWS account as the runtime role, attach this example to that role. `<TEST_BUCKET>` is the bucket name and `dns-test/` is an example fixture prefix; use the same prefix in the notebook URI.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadFixturePrefix",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::<TEST_BUCKET>/dns-test/*"
    },
    {
      "Sid": "ListFixturePrefix",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::<TEST_BUCKET>",
      "Condition": {"StringLike": {"s3:prefix": ["dns-test", "dns-test/*"]}}
    },
    {
      "Sid": "LocateFixtureBucket",
      "Effect": "Allow",
      "Action": "s3:GetBucketLocation",
      "Resource": "arn:aws:s3:::<TEST_BUCKET>"
    }
  ]
}
```

This permits reads/listing for the fixture, not upload or deletion. A connector making a bucket-wide `HeadBucket`/list request may be denied by the prefix condition; configure the known regional endpoint and inspect actual API requirements rather than silently broadening access. Cross-account access additionally needs the bucket owner's resource policy permitting the specific runtime role; explicit denies, SCPs and permission boundaries still apply. Keep Block Public Access enabled and the bucket private.

For a fixture encrypted with a customer-managed AWS KMS key, the role also needs an identity policy such as the following, and authorization in the key policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "kms:Decrypt",
    "Resource": "<KMS_KEY_ARN>"
  }]
}
```

### Route and security example

| Control | Example |
| --- | --- |
| Customer subnet route | Destination `0.0.0.0/0`, target existing NAT gateway `<NAT_GATEWAY_ID>`. This is a routing entry, not an unrestricted firewall permission. |
| Customer egress | Stateful TCP 443 to the S3 destination ranges allowed by your firewall; source ports unrestricted. OCI NSG CIDR rules cannot match an FQDN; maintain ranges or use your approved hostname-aware egress firewall. |
| AWS networking | Public regional S3 endpoint over HTTPS; no AWS VPC, internet-facing compute or S3 public bucket is required. |
| Advanced DNS input | Exact regional/bucket hostnames actually called by the client, including necessary redirects; use FQDNs without paths or credentials. |

Sources: [AWS S3 identity policy examples](https://docs.aws.amazon.com/AmazonS3/latest/userguide/example-policies-s3.html), [S3 operation permissions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-with-s3-policy-actions.html), [SSE-KMS permissions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html), [OCI NAT gateway](https://docs.oracle.com/en-us/iaas/Content/Network/Tasks/NATgateway.htm).

## V02 — Customer Network Source and service gateway

### How the IAM restriction works

| IAM element | What it checks | This example |
| --- | --- | --- |
| Resource principal | **Who signed the request?** | The AIDP instance resource identity. |
| Dynamic group | **Is that resource a member?** | `aidp_v02_reader` contains the selected AIDP instance. |
| Network Source | **Where did the request originate?** | `aidp_v02_vcn` contains the customer VCN and allowed subnet range. |
| Policy | **What may this caller do, from that source?** | Read the named bucket only when the Network Source matches. |

The dynamic group and Network Source are independent IAM objects. The policy connects them. Neither object grants access by itself. Putting an AIDP resource in the dynamic group does not put its requests in the allowed network.

For this grant, **caller is a group member AND bucket matches AND source matches** must all be true. IAM evaluates the source condition when Object Storage authorizes the request. It uses the request's VCN/source-address context through the service gateway; the notebook does not send a Network Source name as a credential.

A dynamic group is the identity mechanism chosen for this walkthrough. A policy that directly matches a resource principal can also authorize it without a dynamic group; do not add that alternative grant alongside this test policy.


### Before you begin

Use an existing private-network workspace and a small private test bucket. Have the AIDP administrator confirm that the native `oci://` reader uses the AIDP instance resource principal. A dynamic group does not change the reader's authentication mode. If the reader uses a user credential, stop and resolve that setup before using this resource-principal walkthrough.

Record:

| Value | Obtain from |
| --- | --- |
| `<AIDP_RESOURCE_ID>` | AIDP instance details: its resource OCID, not the workspace or cluster UUID. |
| `<IAM_DOMAIN>` | Identity domain where the dynamic group will be created. |
| `<BUCKET_COMPARTMENT_ID>`, `<BUCKET_NAME>`, namespace | Object Storage bucket details. |
| `<CUSTOMER_VCN_ID>`, `<SOURCE_SUBNET_CIDR>` | The VCN and customer-side source subnet used by the workspace private connection. |
| Object key, size and expected rows | An existing CSV fixture of at most 64 KiB, uploaded by the bucket owner. |


### IAM setup

An existing IAM administrator completes these three steps before the tester runs the notebook.

**Permission to perform setup:** creating a Network Source requires `NETWORK_SOURCE_CREATE` in the target tenancy. Workspace access, VCN management and the bucket-read policy below do not supply this permission. If a separate setup operator will create it, an authorized tenancy administrator can add this policy in the **root compartment**, for an existing group containing that operator:

```text
Allow group <IAM_DOMAIN>/<SETUP_OPERATORS> to manage network-sources in tenancy
```

This is a **human operator group**, not the `aidp_v02_reader` dynamic group used by the notebook. It permits Network Source management; it does not delegate dynamic-group or policy creation. The IAM administrator still performs those steps. For a tenancy without identity domains, omit `<IAM_DOMAIN>/`. Allow permission changes to propagate before retrying.

This `Allow` example assumes the operator's group belongs to the target tenancy. An operator from another tenancy needs corresponding **Endorse** and **Admit** grants for Network Source creation. Access to the target VCN or permission to read all resources does not establish permission to create a Network Source. Have the IAM owners verify that cross-tenancy setup before proceeding.

#### 1. Create the dynamic group: who may read

In **Identity & Security > Domains > your domain > Dynamic groups**, create `aidp_v02_reader`. In a tenancy without identity domains, open **Identity & Security > Dynamic Groups**. Enter this matching rule, replacing the placeholder:

```text
ALL {resource.id = '<AIDP_RESOURCE_ID>'}
```

Use `resource.id` for the AIDP resource, not a Compute instance matching rule. This selects one resource; it does not include every resource in the compartment.

#### 2. Create the Network Source: where requests may originate

Sign in to the tenancy that owns the customer VCN. In that tenancy's **home region**, open **Identity & Security > Network Sources > Create Network Source**. Network Sources belong to the **tenancy/root compartment**, not the bucket or workspace compartment. For API/CLI creation, `compartmentId` / `--compartment-id` must be the **tenancy OCID** (`<TENANCY_OCID>`).

| Field | Value |
| --- | --- |
| Name | `aidp_v02_vcn` |
| Network type | **Virtual Cloud Network** |
| VCN OCID | `<CUSTOMER_VCN_ID>` |
| IP address/CIDR | `<SOURCE_SUBNET_CIDR>`, such as `10.20.1.0/24` in an illustrative network |

Add no public IP range for this test. The VCN ID and source range form a pair: the same IP range in a different VCN does not match. Do not enter the Object Storage destination IP.

**If creation reports “Authorization failed or requested resource not found”:** verify the signed-in operator's group has the setup grant above, the target is the correct tenancy root, and the VCN OCID belongs to that tenancy. Check the home region and any defined-tag permissions as well; omit optional defined tags when testing creation. Capture the failed operation, HTTP status/error code and `opc-request-id` if it still fails. Being able to list Network Sources does not prove permission to create one. A dynamic-group bucket-read grant cannot resolve a setup-operator authorization failure.

#### 3. Create the policy: permission plus source restriction

Open **Identity & Security > Policies** in the bucket compartment or an authorized ancestor. Create a policy using the manual editor:

```text
Allow dynamic-group <IAM_DOMAIN>/aidp_v02_reader to read objects in compartment id <BUCKET_COMPARTMENT_ID> where all {target.bucket.name='<BUCKET_NAME>', request.networkSource.name='aidp_v02_vcn'}
Allow dynamic-group <IAM_DOMAIN>/aidp_v02_reader to read buckets in compartment id <BUCKET_COMPARTMENT_ID> where all {target.bucket.name='<BUCKET_NAME>', request.networkSource.name='aidp_v02_vcn'}
```

For a tenancy without identity domains, use `dynamic-group aidp_v02_reader`. The first statement permits object reads/listing; the second permits bucket metadata checks. No write grant is needed.

Review all effective grants for this resource and bucket, including inherited policies and other dynamic-group memberships. An additional unconditional allow can bypass this restriction. Choose an isolated test bucket if existing grants cannot be safely scoped. Allow IAM and dynamic-group membership changes to propagate, then keep these definitions unchanged throughout the test.


### Network setup

| Component | Required configuration |
| --- | --- |
| Workspace | Existing private connection to the customer VCN/subnet recorded above; platform private-network prerequisites already satisfied. |
| Service gateway | Enabled Object Storage service, or All Services label including Object Storage, in this same region. |
| Source subnet route | Destination type **Service**, matching regional service label, target the service gateway. |
| Egress controls | Stateful TCP 443 to the regional service label through the applicable subnet/NSG/firewall rules. |

Expected application path: **AIDP -> workspace private connection -> customer subnet -> service gateway -> Object Storage**. The service-gateway path supplies the customer VCN source context needed by this policy.

## Object Storage and ADB authorization

Choose the authorization for the target before selecting the corresponding notebook. The examples below assume the caller's identity has already been established in the V02 setup.

| Target | Required authorization | Source restriction |
| --- | --- | --- |
| Same-region Object Storage, V02 | Bucket-scoped read policies from step 3 for the runtime principal. | VCN Network Source plus regional service-gateway path. |
| Another-region Object Storage, V03 | A grant covering the **remote bucket and its compartment**; the local bucket grant does not cover a different bucket. | The illustrated NAT path presents the NAT public IP. A VCN-only Network Source does not match that path. |
| ADB with database username/password | Database login and SQL privileges; no OCI IAM statement grants this SQL session. | ADB public-endpoint ACL, or private-endpoint routes and NSG/security-list rules. |

**Remote-bucket example:** for a user-principal reader in the same tenancy, the base read grants are:

```text
Allow group <IDENTITY_DOMAIN>/<FIXTURE_READERS> to read objects in compartment <REMOTE_DATA_COMPARTMENT> where target.bucket.name='<REMOTE_TEST_BUCKET>'
Allow group <IDENTITY_DOMAIN>/<FIXTURE_READERS> to read buckets in compartment <REMOTE_DATA_COMPARTMENT> where target.bucket.name='<REMOTE_TEST_BUCKET>'
```

Use the same verified principal variant as in step 3. For the resource-principal setup in V02, use `dynamic-group <IAM_DOMAIN>/aidp_v02_reader` as the subject in both lines. IAM resource grants apply across subscribed regions unless their conditions restrict the region; they do not create cross-region routes. These examples assume one tenancy. Cross-tenancy access needs a separate endorsement/admission setup.

If the remote test must also restrict source, create a **separate** Network Source, `aidp_v03_nat`, with **Public Network** equal to the customer's NAT public IP `/32`. Replace each remote policy's `where target.bucket.name=...` with `where all {target.bucket.name='<REMOTE_TEST_BUCKET>', request.networkSource.name='aidp_v03_nat'}`. Retain the same dynamic-group subject for the AIDP caller. Do not keep the unconditional remote grant alongside its restricted replacement. V03's local control bucket also needs its own read grant.

**ADB password connection used by V04:** a DBA provisions the login securely and grants the following to an existing dedicated test user:

```sql
GRANT CREATE SESSION TO <TEST_DB_USER>;
```

V04 queries `SYS_CONTEXT('USERENV','INSTANCE_NAME') FROM dual`; no application-table grant is needed. A separate application-table test would require `GRANT SELECT ON <OWNER>.<TABLE> TO <TEST_DB_USER>`. The AIDP external catalog or wallet supplies connection information; SQL privileges and ACL/NSG rules still apply. Adding `request.networkSource.name` to an OCI database-management policy does not enforce the source of a password-authenticated SQL connection.

**Optional ADB IAM-token authentication:** this is a separate client/authentication setup, not enabled by this V02 notebook or V04's password-based reader. After a DBA enables IAM integration and maps the IAM group to a database user with SQL privileges, a narrowly scoped token-use policy can be:

```text
Allow group <IDENTITY_DOMAIN>/<DB_TOKEN_USERS> to use database-connections in compartment <DB_COMPARTMENT> where target.database.id='<ADB_ID>'
```

Use a client that supports IAM database tokens. Network ACL/NSG controls still apply to the database session. IAM-token authorization does not replace them. Separately, an operator downloading a wallet needs OCI wallet permissions, such as `read autonomous-databases` in the approved database compartment; that control-plane permission does not grant SQL access.

Sources: [Network Source setup](https://docs.oracle.com/en-us/iaas/Content/Identity/Tasks/managingnetworksources.htm), [Object Storage permissions](https://docs.oracle.com/en-us/iaas/Content/Identity/Reference/objectstoragepolicyreference.htm), [AIDP principal policies](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/iam-policies-oracle-ai-data-platform.html), [service-gateway source restrictions](https://docs.oracle.com/en-us/iaas/Content/Network/Tasks/servicegateway.htm), [Object Storage log fields](https://docs.oracle.com/en-us/iaas/Content/Logging/Reference/details_for_object_storage.htm), [ADB network controls](https://docs.oracle.com/en-us/iaas/autonomous-database-serverless/doc/security-acl-vcn-private-endpoint.html), [IAM database-token authorization](https://docs.oracle.com/en/database/oracle/oracle-database/19/dbseg/configuring-iam-oracle-dbaas.html), [ADB control-plane permissions](https://docs.oracle.com/en/cloud/paas/autonomous-database/serverless/adbsb/autonomous-database-iam-policies.html).

## V03 — Cross-region Object Storage

### Runtime read policy

For an existing dynamic group matching the connector's signer, grant access to the remote fixture bucket:

```text
Allow dynamic-group <RUNTIME_DYNAMIC_GROUP> to read objects in compartment <REMOTE_DATA_COMPARTMENT> where target.bucket.name='<REMOTE_TEST_BUCKET>'
Allow dynamic-group <RUNTIME_DYNAMIC_GROUP> to read buckets in compartment <REMOTE_DATA_COMPARTMENT> where target.bucket.name='<REMOTE_TEST_BUCKET>'
```

If the signer is a user, use `group <IDENTITY_DOMAIN>/<FIXTURE_READERS>` as the subject instead. Grant equivalent access to the separate in-region control fixture if it is not already authorized. These examples assume one tenancy; cross-tenancy access needs its own endorsement/admission arrangement. IAM permission does not configure a regional endpoint or establish a route.

### Route and security example

| Control | Example |
| --- | --- |
| Region A customer subnet | Default route `0.0.0.0/0` to existing `<NAT_GATEWAY_ID>` or approved egress appliance. |
| Remote destination | Region B public Object Storage endpoint, stateful TCP 443 to its approved ranges through the firewall/NSG. |
| Regional service route | Region A service gateway remains the Region A route; it cannot supply the Region B path. |
| Client and DNS | Explicit Region B bucket endpoint and its exact Advanced DNS FQDN where required; use a fresh client to avoid a cached Region A endpoint. |

If the remote bucket requires a Network Source, its restriction must match the actual approved egress context. The V02 customer-VCN/service-gateway condition is not automatically applicable to this NAT path. Do not remove a restriction just to pass the test.

Sources: [Object Storage policy examples](https://docs.oracle.com/en/solutions/oci-object-storage-modernization/configure-tenancy.html), [NAT gateway](https://docs.oracle.com/en-us/iaas/Content/Network/Tasks/NATgateway.htm), [HDFS endpoint configuration](https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/hdfsconnector.htm).

## V04 — Database service private endpoint

### Authorization example

The notebook's database identity query uses the existing database login/catalog credentials. OCI `manage autonomous-databases` is a control-plane permission, not permission to query database data. Reuse the existing approved login. If an administrator has already created a dedicated test database user, the connection privilege example is:

```sql
GRANT CREATE SESSION TO <TEST_DB_USER>;
```

The sample query reads `SYS_CONTEXT('USERENV','INSTANCE_NAME') FROM dual`; it does not need grants on application tables or catalog-wide dictionary access. An additional application-table test would need its own object grant. Existing AIDP catalog access must also be granted through Workbench. Wallet access, secret retrieval and IAM database authentication, if selected, have separate prerequisites; this SQL example does not configure them. Preserve TLS/mTLS and server identity checks.

### Route, resolver and NSG examples

| Control | Example |
| --- | --- |
| Same customer VCN | Existing local private routing to the service private endpoint; no NAT or internet gateway for that path. |
| Other VCN | Route `<DATABASE_SUBNET_CIDR>` to the approved LPG/DRG attachment in the source route table, reciprocal route `<WORKSPACE_SUBNET_CIDR>` in the target, and required peering/DRG route distribution. Use non-overlapping CIDRs. |
| Workspace egress | Stateful TCP to destination `<DATABASE_PRIVATE_IP>/32`, destination port `<DB_LISTENER_PORT>`, source ports unrestricted. |
| Database NSG ingress | Stateful TCP from `<WORKSPACE_SUBNET_CIDR>` or verified source NSG, destination port `<DB_LISTENER_PORT>`, source ports unrestricted. Use the actual customer-side source range observed for this topology. |
| Listener | Copy the actual private descriptor's port; 1522 is an mTLS example, not proof of client-certificate use. |
| DNS | Verify default private-endpoint DNS proxy resolution into customer DNS, forwarding/listening when needed, and the actual private runtime target. Add no test-specific Advanced DNS entry for this baseline. Capture existing override provenance. |

The destination NSG rule supplements the platform network baseline. Cross-VCN NSG references have scope limits: use the approved CIDR alternative where NSG-to-NSG referencing is unavailable. Default private DNS resolution does not create peering. Do not submit an unresolvable private-only hostname to the explicit mapping mechanism to repair a resolver/route problem.

Sources: [ADB user connection privileges](https://docs.oracle.com/en/cloud/paas/autonomous-database/serverless/adbsb/manage-users-create.html), [ADB private endpoint configuration](https://docs.oracle.com/en/cloud/paas/autonomous-database/serverless/adbsb/private-endpoints-autonomous.html), [ADB connection modes](https://docs.oracle.com/en-us/iaas/autonomous-database-serverless/doc/connect-introduction.html).

### Preserve the database during unrelated DNS edits

Reuse the same database login/catalog grants and exact route, NSG rules and private descriptor across all five V04 phases. No new database-management IAM grant is needed for the read-only regression. Prepare unrelated owned A/B hostnames suitable for explicit DNS provisioning, following the V06 pattern below; their routes and permissions remain separate from the database baseline. Respect supported workspace-edit lifecycle prerequisites. V04 checks connectivity after each edit; V07 separately checks the absence of compute restarts.

For an optional historical-support/adoption variant, obtain the original configuration and transition evidence through the existing administrator/evidence-access process. The notebook does not grant access to support records, migrate configuration, or prove legacy adoption from a successful query alone. Preserve original authentication and source restrictions, and test a fresh connection where practical in addition to the existing client.

## V05 — Service endpoint coverage

Reuse each selected service's existing runtime identity and approved topology. There is no universal IAM policy, listener port or gateway for all six possible service families. For ADB, use the V04 database login and private-route pattern where applicable; for Object Storage, use the V02/V03 bucket-scoped read pattern with the correct endpoint type and source requirement.

For an OCI Streaming REST consume test, this is an example user-group policy scoped to the selected stream:

```text
Allow group <IDENTITY_DOMAIN>/<STREAM_READERS> to use stream-pull in compartment <STREAM_COMPARTMENT> where target.stream.id='<TEST_STREAM_ID>'
```

This is a REST consume example, not a complete Kafka client setup. Use the actual message endpoint, required HTTPS route, and the client's supported authentication. Kafka consumer-group operations can require additional service permissions. PostgreSQL, OpenSearch, Kafka and interactive compute endpoints each need their own service/data-plane credentials and actual listener rules; adding their hostname does not grant query or consume access.

For every endpoint, record the operation, effective identity, required policy/role, route and listener rule. Missing service-specific permissions/fixtures mean **BLOCKED** coverage. For default behavior, capture the baseline and existing overrides before adding any hostname. Distinguish regional service-zone permission, explicit mapping and private DNS proxy fallback; do not call a private-proxy result automatic entry insertion.

Source: [OCI policy templates for Streaming](https://docs.oracle.com/en-us/iaas/Content/Identity/policiescommon/commonpolicies.htm).

## V06 — DNS configuration operations

Use the shared workspace operator permissions and existing approved workspace-edit role. Reuse owned disposable A/B public endpoints that resolve from the service environment and their existing route, listener rule and authenticated reader. For example, if A/B are in-region Object Storage endpoints, retain the V02 service route and bucket read policy; for public endpoints, retain the V01/V03 approved egress pattern.

An optional unresolved U is an owned test name with no usable resolution, not a request to create a public zone or remove a production record. No extra IAM grant to create DNS zones, change resolver settings or administer gateways is required just to edit Advanced DNS entries. Full baseline snapshots include unrelated DNS/SCAN settings; do not broaden their policies during input tests.

## V07 — Workload continuity

Reuse the V02 or V03 route, fixture and runtime read policy for the workload actually selected. Use service-resolvable public A/B hostnames for the explicit mapping changes. Keep permissions and network controls unchanged while measuring Advanced DNS add/edit/delete. The test adds no new service-access privilege.

Use your existing read-only access to independent lifecycle evidence. If OCI Logging holds the relevant lifecycle records, an example grant for the evidence reviewer is:

```text
Allow group <IDENTITY_DOMAIN>/<EVIDENCE_REVIEWERS> to read log-content in compartment <TEST_LOG_COMPARTMENT>
```

This reads available logs; it does not enable logging or guarantee Spark restart events are recorded. The administrator must select an authoritative lifecycle source for the deployment. Capture evidence privately. Any one-time adoption restart is a separate preparation event; do not include it in a passing no-restart sequence.

Source: [OCI Logging permissions](https://docs.oracle.com/en-us/iaas/Content/Identity/Reference/loggingpolicyreference.htm).

## Preparation ownership and restoration

Network administrators perform any route, resolver, peering, NSG or gateway construction through the organization's existing privileged workflow. Testers need only the workspace edit, fixture access and evidence read permissions for their assigned case. Capture policy and network baselines before the run, keep them fixed during DNS comparisons, and remove only test-specific grants/resources introduced under the approved plan. No policies or infrastructure are applied by opening or running these distributed notebooks.

Copyright (c) 2026 Oracle and/or its affiliates. Licensed under the [Universal Permissive License](../../../LICENSE.txt), Version 1.0.
