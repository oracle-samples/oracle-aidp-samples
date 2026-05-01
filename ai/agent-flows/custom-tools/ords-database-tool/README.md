# ORDS Database Tool

Three tools for working with an Oracle Autonomous Database (ADB) through the ORDS (Oracle REST Data Services) REST API. No Oracle Client, JDBC driver, or Oracle `oracledb` package needed — this sample uses only `urllib.request` from the standard library.

## Tools

| Class | Purpose |
|---|---|
| `OrdsSqlTool` | Executes an arbitrary SQL statement (SELECT/INSERT/UPDATE/DELETE/DDL), returning up to `limit` rows formatted as an aligned text table. |
| `OrdsSchemaExplorer` | Lists tables, lists views, or describes a single table's columns. Delegates to `OrdsSqlTool` internally. |
| `OrdsListSchemas` | Lists all database users/schemas visible to the connected account. |

## What the sample demonstrates

- **Credentialed REST calls** with HTTP Basic auth (base64-encoded `username:password`).
- **Multi-tool packages** where one tool delegates to another's `_execute_tool` classmethod.
- **Defensive input handling** — `int(runtime_params.get("limit", ...))` wrapped in a `try/except` because template substitution can deliver numbers as strings.
- **Rich output formatting** — `utils/text_utils.format_table` renders query results as an aligned, human-readable table using `jsonColumnName` from the ORDS metadata as the row key and `columnName` as the display header.
- **Error surfacing** — `HTTPError` bodies are returned as `{"error": ...}` so the framework marks `isError: true` (see *"Return errors and set isError"* in the [user guide](../USER_GUIDE.md)).

## Files

```
ords-database-tool/
├── tool_implementation.py      # OrdsSqlTool, OrdsSchemaExplorer, OrdsListSchemas
├── tool_config.json            # UI configuration reference — EDIT the placeholders
├── requirements.txt            # stdlib only (urllib.request)
└── utils/
    ├── __init__.py
    ├── text_utils.py           # truncate_output, format_table
    └── model_utils.py          # format_result helper
```

## Prerequisites

- An Oracle Autonomous Database with **ORDS** enabled.
- The target schema (e.g. `ADMIN`) must have `ords.enable_schema` set and `ords.enable_object` permissions for the tables you intend to query.
- The ORDS endpoint should be reachable from your AIDP compute environment (check network policy and any egress restrictions).

## Walkthrough

1. **Fill in the ORDS endpoint and credentials** in `tool_config.json`:
   - `ords_base_url` — your ADB's ORDS URL, for example `https://abcd1234.adb.us-phoenix-1.oraclecloudapps.com/ords/admin`.
   - `username` — the schema user (often `ADMIN` for quick experiments; production flows should use a least-privilege user).
   - `password` — leave as the `{{db_password}}` template variable and supply the actual value through the agent flow's parameter mechanism, or embed a value for quick testing. **Never commit real credentials.**
2. **Package the ZIP** with the build utility:
   ```bash
   python ../scripts/build_custom_tool.py .
   ```
   Writes `custom_tool_ords_database_tool.zip` alongside the sample folder.
3. **Upload** the ZIP to your workspace volume.
4. **Configure each tool** in the agent flow UI using the matching entry in `tool_config.json`.
5. **Test** each tool:
   - SQL: `{"sql": "SELECT 1 FROM dual", "limit": 1}`.
   - Schema Explorer: `{"operation": "list_tables"}` or `{"operation": "describe_table", "table_name": "EMPLOYEES"}`.
   - List Schemas: `{}` (no parameters).

## Security notes

- **Do not commit real credentials.** The `password` field in `tool_config.json` uses the `{{db_password}}` template variable; supply the actual value via the agent flow's parameter UI, or from a secrets store.
- `OrdsSchemaExplorer.describe_table` sanitizes the `table_name` input (uppercases it, strips `'` and `;`) before interpolating it into SQL. That's adequate for schema-introspection queries against `user_tab_columns`, but **don't copy the pattern for tools that accept user-supplied SQL fragments** — prefer ORDS bind variables for anything more substantial.
- `OrdsSqlTool` runs whatever SQL the LLM emits. Point it at a read-only user for untrusted flows, or gate it behind human-in-the-loop approval.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `HTTP 401: Unauthorized` | Wrong username/password, or the schema isn't REST-enabled. |
| `HTTP 404: Not Found` | `ords_base_url` doesn't point at the right schema. Confirm the URL path ends with `/ords/<schema>`. |
| `-- ERROR (ORA-00942): table or view does not exist` | The connected user doesn't have SELECT on that object. Grant the needed privilege or use a different schema user. |
| Timeouts | Increase `timeout` in `conf`, or reduce the query's complexity. |

## See also

- [USER_GUIDE.md](../USER_GUIDE.md) — full custom tools contract.
