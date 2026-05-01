"""OAC dashboard layer.

Bundle-side Python that uses **OAC REST API** (NOT the OAC MCP) to:
- register the AIDP JDBC data source in a customer's OAC instance
- import ``.dva`` workbook exports from ``oac/workbooks/`` (top-level repo dir)
- validate each imported workbook renders against ``fusion_catalog.gold.*``

OAC MCP (Preview) is downstream, end-user-only — exposes Discover/Describe/Execute
read-only Logical SQL tools to AI clients (Claude/Cline/Copilot). The bundle does
not call OAC MCP; it just prints the config snippet end users paste into their
``claude_desktop_config.json``.
"""
