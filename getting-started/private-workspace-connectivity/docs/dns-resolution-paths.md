# Distinguish DNS resolution paths

## Resolution paths

Advanced DNS lets you specify hostnames whose traffic needs to use your customer VCN. Private hostnames can already use customer DNS through the workspace's private connection.

| Your destination | How it is handled | What you need to do |
| --- | --- | --- |
| A hostname resolvable from the AIDP service network, whose traffic must use your customer VCN—for example, an S3 or public OCI service endpoint | An explicit Advanced DNS entry configures handling through the customer-network connection. | Add the actual hostname used by your application. Verify the required customer routes, security rules, and service permissions. |
| A private hostname, such as a private ADB hostname, that the AIDP service resolver cannot resolve | DNS resolution uses the private-endpoint DNS proxy and customer DNS context by default. | Verify customer DNS, private connectivity, and destination access rules. An Advanced DNS entry is not required solely because the hostname is private. |

A private endpoint does not by itself determine whether an entry is needed. Use the actual connection hostname and its resolution behavior. Validate the connection with an authenticated application operation.

Regional service-zone allowances are a separate policy consideration; they are not another resolution path or proof of an automatically inserted workspace entry. DNS configuration does not create routes or grant service permissions. Keep using the hostname for TLS identity checks, even when AIDP returns a mapped address.

**Before testing:** record the complete DNS/SCAN list, endpoint type, existing overrides and effective client configuration. A successful query alone does not identify the resolution mechanism. Do not delete a shared override to create a clean baseline. Use an approved clean test workspace or mark the default-path claim **INCOMPLETE** when provenance cannot be established.

For V04, first test the private endpoint without adding a database Advanced DNS entry. Keep that database configuration fixed while adding, replacing and deleting unrelated service-resolvable test hostnames, repeating the database query in each phase and after restoration. For V01-V03, explicit-entry tests require service-resolvable hostnames; private-only names are unsuitable for demonstrating that provisioning mechanism.

The [public workspace guide](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/workspaces.html) lists broad private-network scenarios; endpoint type and deployed resolver behavior determine whether an explicit entry is necessary. The [ADB private endpoint guide](https://docs.oracle.com/en/cloud/paas/autonomous-database/serverless/adbsb/private-endpoints-autonomous.html) describes the destination's private access setup. Confirm your deployment's behavior with the administrator and retain independent evidence for the resolution path you claim.

## License

Copyright (c) 2026 Oracle and/or its affiliates. Licensed under the [Universal Permissive License](../../../LICENSE.txt), Version 1.0.
