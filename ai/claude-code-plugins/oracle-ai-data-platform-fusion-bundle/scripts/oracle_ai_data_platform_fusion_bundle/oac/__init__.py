"""OAC dashboard layer.

Bundle-side Python that uses **OAC REST API** (NOT the OAC MCP) to:
- POST /catalog/connections to register the AIDP JDBC data source
- POST /snapshots + /system/actions/restoreSnapshot to deliver workbook content
  via a ``.bar`` snapshot the customer uploads to their own OCI Object Storage
- Poll /workRequests/{id} until the restore completes

All endpoints are documented in Oracle's openapi.json (verified 2026-05-01,
TC10h-2). Per-workbook ``.dva`` imports were intentionally removed in that
refactor — the imports endpoint isn't in the public spec.

OAC MCP (Preview) is downstream, end-user-only — exposes Discover/Describe/Execute
read-only Logical SQL tools to AI clients (Claude/Cline/Copilot). The bundle does
not call OAC MCP; it just prints the config snippet end users paste into their
``claude_desktop_config.json``.
"""
