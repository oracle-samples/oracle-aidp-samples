import json
import urllib.request
import urllib.error
import base64
import ssl

from aidputils.agents.tools.custom_tools.base import CustomToolBase
from .utils.text_utils import truncate_output, format_table


@CustomToolBase.register
class OrdsSqlTool(CustomToolBase):
    """Execute SQL statements against Oracle ADB via ORDS REST API."""

    @classmethod
    def _execute_tool(cls, conf, runtime_params, **context_vars):
        sql = runtime_params.get("sql", "").strip()
        if not sql:
            return {"error": "No SQL statement provided"}

        # AIDP framework nests tool config under "conf" key
        tool_conf = conf.get("conf", conf)

        try:
            limit = int(runtime_params.get("limit", tool_conf.get("max_rows", 100)))
        except (ValueError, TypeError):
            limit = 100
        ords_url = tool_conf.get("ords_base_url", "").rstrip("/")
        username = tool_conf.get("username", "")
        password = tool_conf.get("password", "")
        timeout = tool_conf.get("timeout", 60)

        endpoint = f"{ords_url}/_/sql"

        try:
            # Build request
            payload = json.dumps({
                "statementText": sql,
                "limit": limit
            }).encode("utf-8")

            req = urllib.request.Request(
                endpoint,
                data=payload,
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Authorization": "Basic " + base64.b64encode(
                        f"{username}:{password}".encode()
                    ).decode()
                }
            )

            ctx = ssl.create_default_context()

            with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            return cls._parse_ords_response(data)

        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            return {"error": f"HTTP {e.code}: {body[:500]}"}
        except Exception as e:
            return {"error": str(e)}

    @classmethod
    def _parse_ords_response(cls, data):
        """Parse ORDS SQL response into readable output."""
        items = data.get("items", [])
        results = []

        for item in items:
            stmt_type = item.get("statementType", "")
            result_set = item.get("resultSet", {})
            response_text = item.get("response", "")

            # Check for errors first
            error_msg = item.get("errorMessage", "")
            if error_msg:
                error_code = item.get("errorCode", "")
                results.append(f"-- ERROR (ORA-{error_code}): {error_msg}")
                continue

            if result_set and result_set.get("items"):
                columns = result_set.get("metadata", [])
                # ORDS uses jsonColumnName (lowercase) as keys in result items
                col_names = [c.get("jsonColumnName", c.get("columnName", f"col{i}")) for i, c in enumerate(columns)]
                display_names = [c.get("columnName", c.get("jsonColumnName", f"COL{i}")) for i, c in enumerate(columns)]
                rows = result_set.get("items", [])
                table = format_table(col_names, rows, display_names)
                row_count = len(rows)
                has_more = result_set.get("hasMore", False)
                summary = f"-- {stmt_type}: {row_count} row(s)"
                if has_more:
                    summary += " (more rows available, increase limit)"
                results.append(f"{summary}\n{table}")
            elif response_text:
                results.append(f"-- {stmt_type}: {response_text}")
            else:
                results.append(f"-- {stmt_type}: OK")

        if not results:
            error_msg = data.get("errorMessage", "")
            if error_msg:
                return {"error": error_msg}
            return {"output": "Statement executed successfully (no output)"}

        return {"output": "\n\n".join(results)}


@CustomToolBase.register
class OrdsSchemaExplorer(CustomToolBase):
    """Explore Oracle database schema via ORDS."""

    OPERATIONS = {
        "list_tables": "SELECT table_name FROM user_tables ORDER BY table_name",
        "list_views": "SELECT view_name FROM user_views ORDER BY view_name",
    }

    @classmethod
    def _execute_tool(cls, conf, runtime_params, **context_vars):
        operation = runtime_params.get("operation", "").strip()
        table_name = runtime_params.get("table_name", "").strip()

        if operation == "describe_table":
            if not table_name:
                return {"error": "table_name is required for describe_table"}
            # Sanitize table name to prevent injection
            safe_name = table_name.upper().replace("'", "").replace(";", "")
            sql = (
                f"SELECT column_name, data_type, data_length, nullable "
                f"FROM user_tab_columns WHERE table_name = '{safe_name}' "
                f"ORDER BY column_id"
            )
        elif operation in cls.OPERATIONS:
            sql = cls.OPERATIONS[operation]
        else:
            return {"error": f"Unknown operation: {operation}. Use: list_tables, list_views, describe_table"}

        # Delegate to OrdsSqlTool
        return OrdsSqlTool._execute_tool(conf, {"sql": sql})


@CustomToolBase.register
class OrdsListSchemas(CustomToolBase):
    """List all database schemas (users) in the Oracle database."""

    @classmethod
    def _execute_tool(cls, conf, runtime_params, **context_vars):
        sql = (
            "SELECT username AS schema_name "
            "FROM all_users "
            "ORDER BY username"
        )
        return OrdsSqlTool._execute_tool(conf, {"sql": sql})
