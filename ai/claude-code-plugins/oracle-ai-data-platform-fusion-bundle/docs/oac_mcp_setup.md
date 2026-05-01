# OAC MCP setup — chat with your Fusion data from Claude Desktop / Cline / Copilot

After the bundle's `dashboard install --target oac` lands the AIDP JDBC connection in your OAC instance, end users can connect their AI client to OAC's MCP server and ask natural-language questions against the gold marts (e.g. `gold.supplier_spend`).

This doc walks an end user through the **one-time MCP setup** on their machine. It takes ~5 minutes per user.

---

## Prerequisites

| | |
|---|---|
| OAC version | November 2025 release or later (the OAC MCP Server is in preview from that release) |
| OAC entitlement | Your user must be able to log into the OAC home page and have at least *Discover* access on the imported Fusion workbook(s) |
| AI client | One of: Claude Desktop, Claude Code, Cline (VS Code extension), GitHub Copilot |
| Local | Node.js 18+ on your laptop |

---

## Step 1 — Download the OAC MCP connector

1. Open OAC: e.g. `https://<your-oac-host>/ui/`
2. Click your initials badge in the top right → **Profile**
3. Click the **MCP Connect** tab
4. Click **Download** to get `oac-mcp-connect.zip`
5. Extract it locally — note the path to `oac-mcp-connect.js`. Recommended location:

   | OS | Path |
   |---|---|
   | macOS / Linux | `~/oac-mcp-connect/oac-mcp-connect.js` |
   | Windows | `C:\Users\<you>\oac-mcp-connect\oac-mcp-connect.js` |

---

## Step 2 — Copy the JSON config from OAC

On the same MCP Connect tab, click **Copy JSON** to copy the per-user MCP server configuration. It looks roughly like:

```json
{
  "mcpServers": {
    "oac-mcp-server": {
      "command": "node",
      "args": ["path/to/oac-mcp-connect.js"],
      "env": {
        "OAC_INSTANCE_URL": "https://<your-oac-host>",
        ...
      }
    }
  }
}
```

> Replace `path/to/oac-mcp-connect.js` with the actual path you noted in Step 1.

---

## Step 3 — Paste into your AI client's config

### Claude Desktop
1. Open `claude_desktop_config.json`:
   - **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
   - **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
2. Merge the `oac-mcp-server` entry into the existing `mcpServers` object (if no `mcpServers` key, paste the whole snippet).
3. Save and **restart Claude Desktop**.

### Claude Code (`claude` CLI)
1. Open `~/.claude/settings.json` (or `.claude/settings.local.json` for project-scoped).
2. Merge under `mcpServers`.
3. Restart Claude Code.

### Cline (VS Code extension)
1. Open `cline_mcp_settings.json` from VS Code Command Palette → "Cline: Open MCP Settings".
2. Merge under `mcpServers`.
3. Reload window.

### GitHub Copilot
1. Open Copilot Chat MCP config (Copilot → Settings → MCP).
2. Add the `oac-mcp-server` entry.
3. Reload.

---

## Step 4 — Verify the connection

Restart your AI client. In a fresh chat session, ask something like:

> *"List the OAC MCP tools available."*

Expect to see:

| Tool | Purpose |
|---|---|
| `oracle_analytics-discoverData` | List datasets and subject areas |
| `oracle_analytics-describeData` | Get column/measure metadata for a dataset |
| `oracle_analytics-executeLogicalSQL` | Run a Logical SQL query |

If those three appear, MCP is connected.

---

## Step 5 — First grounded question against the bundle's gold mart

Once `aidp-fusion-bundle dashboard install --target oac` has imported the workbooks (or the user has manually authored a workbook against `aidp_fusion_jdbc.fusion_catalog.gold.supplier_spend`), ask:

> *"Which vendors had over $100M in invoice spend? Show vendor_id, total_invoice_amount, and invoice_count, ordered by spend descending."*

The AI client should:
1. Call `oracle_analytics-discoverData` to find the supplier-spend dataset
2. Call `oracle_analytics-describeData` to get the columns
3. Construct a Logical SQL query and call `oracle_analytics-executeLogicalSQL`
4. Return the answer with citations

Live-tested 2026-04-30 in [TC9](../tests/live/TC9_genai_results.md): the agent identified the top vendor `300000047507499` at $892.7M with concentration math (26.18%) and anomaly detection (`vendor_id=-10016`, stale invoice `2018-12-21`).

---

## Critical capability boundaries

OAC MCP (Preview, Nov 2025) is intentionally narrow:

| Can | Cannot |
|---|---|
| List/describe datasets, subject areas, columns, measures | Create or modify workbooks |
| Run governed Logical SQL queries (read-only) | Register data sources (use OAC REST API for that — see `oac/rest/`) |
| Auth runs as the **end user** — governance preserved | Run arbitrary SQL DDL (no schema changes, no inserts) |

For write operations (registering new connections, importing workbooks), the bundle's `dashboard install --target oac` uses **OAC REST API**, not MCP. See [oac_rest_api_setup.md](oac_rest_api_setup.md) (separate doc).

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `oac-mcp-server` not in client's tool list | Path to `oac-mcp-connect.js` wrong, or client not restarted | Verify the path; restart the AI client |
| 401 on first query | Your OAC session expired | Open OAC web UI, log in, retry |
| `oracle_analytics-executeLogicalSQL` returns empty | Workbook/dataset not visible to your user, or query referenced a column that doesn't exist | Check OAC permissions; describe the dataset first to confirm column names |
| MCP server crashes on start | Node.js < 18 | Upgrade Node.js |
| `OAC_INSTANCE_URL` mismatch | Copied JSON pointed at a different OAC instance | Re-fetch JSON from the right OAC's MCP Connect tab |

---

## What this enables in the Fusion bundle pitch

This closes the bundle's pdf1-aligned end-to-end story:

```
Fusion BICC PVO  →  AIDP bronze  →  AIDP silver  →  AIDP gold
                                                       │
                                            JDBC ◀────┘
                                              │
                                              ▼
                                          OAC workbook
                                              │
                                       MCP ◀──┘
                                          │
                                          ▼
                            Claude / Cline / Copilot
                            "What's our AR aging?"
                            "Which vendors had >$100M Q1 spend?"
```

End-user value: **non-technical analysts can ask natural-language questions against the bundle's curated gold marts**, with answers computed by OAC over real Fusion data — no SQL knowledge required, governance preserved by OAC's row-level security.

---

## References

- [OAC MCP Server (Preview) — overview](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acsdv/access-oracle-analytics-cloud-mcp-server-preview.html)
- [OAC MCP — Tools available](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acsdv/tools-available-oracle-analytics-cloud-mcp-server-preview.html)
- [OAC MCP — Add to AI client](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acsdv/add-oracle-analytics-cloud-mcp-server-your-ai-client-preview.html)
- [Bundle TC9 results](../tests/live/TC9_genai_results.md) — proof that Spark SQL `ai_generate()` works against the same gold mart (alternative path; Spark-native vs OAC-mediated)
