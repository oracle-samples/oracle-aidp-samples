# What the four use cases validate

Use one notebook for one customer connectivity requirement. Each includes its own prerequisites, authorization, network setup, executable checks, expected results and troubleshooting. Supply resources from your own environment in a private working copy.

| Case | Customer requirement | Acceptance |
| --- | --- | --- |
| [V01: Amazon S3](v01-s3.md) | Read a private S3 object through customer egress. | Exact object contents after the scoped DNS change, with the AWS reader unchanged; correlate customer flows, NAT/egress configuration and the S3 request. Restore the original settings and repeat. |
| [V02: Customer source restrictions](v02-network-source.md) | Restrict bucket reads and ADB public access to the customer VCN. | Bucket Network Source and principal-policy setup, plus the separate ADB public VCN ACL/database login. Establish working baselines, run approved allow/deny/restore controls, and review correlated source evidence. |
| [V03: Cross-region Object Storage](v03-cross-region.md) | Read a remote bucket while retaining local bucket access. | Exact reads from the remote fixture and a distinct local control in every phase. Review each region's request logs and the customer egress path. Restore only the test DNS entry. |
| [V04: Private ADB](v04-private-adb.md) | Keep an existing private database connection working across unrelated DNS edits. | The reviewed private descriptor returns the same expected database identity in before/add/edit/delete/restored phases, with actual configuration changes and restoration recorded. |

## V04 baseline and DNS-change coverage

First establish the existing private connection without adding an explicit entry merely because the database hostname is private. Keep the database descriptor, credentials, routes and security rules fixed. Add an unrelated owned, service-resolvable hostname A; replace A with B; remove B; restore the complete original DNS/SCAN settings. Repeat the identity query after each step.

The evidence evaluator checks all five phases, successful queries, recorded configuration changes, restoration and the expected database identity. A baseline-only run or five unchanged snapshots does not complete the case. Review the private endpoint and default DNS-proxy path separately using actual endpoint/network evidence.

## Interpret results

A DNS answer shows resolution. A TCP handshake shows listener reachability. An authenticated read/query establishes service access. Customer routing and source restrictions require their applicable network, request or control-test evidence. Driver probes do not prove every executor's path.

Follow the workspace's supported lifecycle prerequisites. These four cases do not certify automatic propagation to every running cluster or AI compute. A wallet-configured query or listener port alone does not establish mTLS client-certificate use. Preserve the deployed TLS requirements and review transport evidence before making that claim.

Keep run records private. The [workspace guide](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/workspaces.html), [resolution-path explanation](dns-resolution-paths.md) and [network/IAM templates](network-and-iam-examples.md) support preparation.

Copyright (c) 2026 Oracle and/or its affiliates. Licensed under the [Universal Permissive License](../../../LICENSE.txt), Version 1.0.
