"""OAC dashboard layer.

Bundle-side Python that uses **OAC REST API** (NOT the OAC MCP) to:
- POST /catalog/connections to register the AIDP JDBC data source
- POST /snapshots + /system/actions/restoreSnapshot to deliver workbook content
  via a ``.bar`` snapshot the customer uploads to their own OCI Object Storage
- Poll /workRequests/{id} until the restore completes

All endpoints are documented in Oracle's openapi.json. Per-workbook ``.dva``
imports are intentionally not used because the imports endpoint isn't in the
public spec.

OAC MCP (Preview) exposes Discover/Describe/Execute Logical SQL tools to
MCP-compatible clients. The bundle prints configuration that operators can
place in the active Codex MCP configuration or register with ``codex mcp add``.
"""
