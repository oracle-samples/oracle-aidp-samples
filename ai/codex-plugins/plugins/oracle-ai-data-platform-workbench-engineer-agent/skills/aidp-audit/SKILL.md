---
name: aidp-audit
description: Manage and search AIDP audit logs — enable/disable auditing, set retention, and query audit-log entries for a DataLake. Use when the user asks about audit logging, who did what, compliance/retention of AIDP activity, or wants to search audit events. Self-contained — official `aidp audit` CLI preferred, `oci raw-request` fallback.
---

# `aidp-audit` — audit logs (enable, retention, search)

Manage AIDP Workbench audit logging and search audit entries. Self-contained: no MCP / `ai-data-engineer-agent`
required. Engine precedence per `references/aidp-cli-map.md` — prefer the official `aidp` CLI, else `oci raw-request`.

## When to use
- "Turn on / configure audit logging", "set audit retention", "who did X / search the audit log", compliance questions.

## Engine (CLI-preferred)
- **Manage:** `aidp audit manage-logs <instance-id> --body '{"action":"ENABLE","retentionPeriod":<days>}' --auth api_key --profile DEFAULT`
  (`action` e.g. `ENABLE`/`DISABLE`; `retentionPeriod` in the body).
- **Search:** `aidp audit search-logs <instance-id> …` (filter by time/principal/resource per `aidp help audit`).
- **Fallback (`oci raw-request`):** `POST …/20240831/dataLakes/<OCID>/actions/manageAuditLogs` (manage) and
  the audit search endpoint; verify the exact path/body live and record in `references/rest-endpoint-map.md`
  before asserting (no-fabrication).

## Workflow
1. Read current state / search first; show the user what you found.
2. For `manage-logs` (enable/disable/retention) — **confirm** before changing, since it affects compliance posture.
3. Present results; for search, summarize matches (don't dump raw if large).

## Guardrails
- Changing audit config is a governance action — confirm. Never disable auditing without explicit instruction.

## References
- [references/aidp-cli-map.md]($HOME/.aidp/references/aidp-cli-map.md) · [references/oci-raw-request.md]($HOME/.aidp/references/oci-raw-request.md) · [references/rest-endpoint-map.md]($HOME/.aidp/references/rest-endpoint-map.md)
