"""Live-test driver: upload each notebook to AIDP, run, capture result.

This is meant to be executed *by Claude Code via the AIDP MCP tools*, not run
directly by Python. It documents the canonical flow and provides a Python
helper that the human-driven notebook (or a future MCP-aware script) can call.

Per-row flow:
    1. Read examples/<row>.ipynb from disk.
    2. mcp__aidp__nb_save_file → push to AIDP workspace
       under Shared/connectors-tests/<row>.ipynb.
    3. mcp__aidp__nb_create_session against cluster `tpcds`.
    4. mcp__aidp__nb_execute_code → run each cell sequentially.
    5. Capture stdout, look for AIDP_LIVE_TEST_RESULT_BEGIN/END markers,
       parse the JSON between them.
    6. Write tests/live-results/<row>.json with parsed payload + duration.
    7. Append/update a row in tests/live-results/RESULTS.md.

This file is the single source of truth for which notebooks correspond to
which live-test rows. Update LIVE_TEST_ROWS when adding new connectors.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


REPO = Path(__file__).resolve().parent.parent
EXAMPLES = REPO / "examples"
LIVE_RESULTS = REPO / "tests" / "live-results"
LIVE_RESULTS.mkdir(parents=True, exist_ok=True)


@dataclass
class LiveTestRow:
    row_number: int
    skill: str
    auth_method: str
    notebook: str
    pass_criteria: str
    in_v0_1_release_gate: bool


LIVE_TEST_ROWS = [
    LiveTestRow(0,  "aidp-connectors-bootstrap", "n/a",                          "00_bootstrap_helpers.ipynb",     "BOOTSTRAP OK printed; package importable",                         True),
    # ALH covers the entire Autonomous DB family (ALH = ADW = ATP = Oracle 26ai under the hood).
    LiveTestRow(1,  "aidp-alh",            "Wallet (mTLS)",                     "alh_wallet_query.ipynb",         "Spark DataFrame with non-zero rows from a known ALH/ADW/ATP table", True),
    LiveTestRow(2,  "aidp-alh",            "IAM DB-Token (>25 min refresh)",     "alh_dbtoken_query.ipynb",        "Same query via DB-token, on-executor refresh, zero auth failures", True),
    LiveTestRow(3,  "aidp-alh",            "API Key + inline OCI config",        "alh_catalog_sync_apikey.ipynb",  "External-catalog metadata refresh succeeds; downstream Spark read", False),
    # Rows 4 and 5 (ExaCS Wallet TCPS / IAM DB-Token) were removed: neither is supported by AIDP notebooks for ExaCS.
    LiveTestRow(6,  "aidp-exacs",          "Plain user/pwd on TCP 1521 + NNE AES256", "exacs_user_password.ipynb",  "Spark JDBC connect; AES256 NNE confirmed via v$session_connect_info", False),
    # Rows 7 and 8 (aidp-bds-hive Kerberos / LDAP) were removed: skill dropped from the plugin (BDS Hive not in scope).
    LiveTestRow(9,  "aidp-fusion-rest",    "HTTP Basic",                         "fusion_rest_basic.ipynb",        "<=499-row paged fetch lands as Spark DataFrame",                   True),
    LiveTestRow(10, "aidp-fusion-bicc",    "HTTP Basic",                         "fusion_bicc_to_dataframe.ipynb", "Extract kicks off -> CSV in OS bucket -> Spark reads it",          True),
    LiveTestRow(11, "aidp-epm-cloud",      "Basic (tenancy.user@domain)",        "epm_planning_basic.ipynb",       "Planning REST applications=200; MDX export -> DataFrame",          False),
    LiveTestRow(12, "aidp-essbase",        "HTTP Basic",                         "essbase_mdx_basic.ipynb",        "MDX SELECT returns DataFrame with expected dim count",             False),
    LiveTestRow(13, "aidp-streaming-kafka", "SASL/PLAIN with OCI auth token",    "kafka_streaming_apikey.ipynb",   "query.lastProgress shows non-zero numInputRows; messages parsed from topic", True),
    # v0.2.0 — official-AIDP-samples-driven additions
    LiveTestRow(14, "aidp-object-storage", "Implicit IAM (oci://)",              "object_storage_csv_roundtrip.ipynb", "Round-trip CSV write+read against an OCI bucket; no keys",   False),
    LiveTestRow(15, "aidp-postgresql",     "Plain user/password",                "postgresql_read.ipynb",          "Spark DataFrame from a known PostgreSQL table",                    False),
    LiveTestRow(16, "aidp-mysql",          "Plain user/password (MYSQL or MYSQL_HEATWAVE)", "mysql_read.ipynb",      "Spark DataFrame from a known MySQL or HeatWave table",             False),
    LiveTestRow(17, "aidp-sqlserver",      "Plain user/password",                "sqlserver_read.ipynb",           "Spark DataFrame from a known SQL Server table",                    False),
    LiveTestRow(19, "aidp-iceberg",        "Implicit IAM (oci:// Hadoop catalog)", "iceberg_smoke.ipynb",          "Iceberg table created + rows written + read; snapshots visible",   False),
    LiveTestRow(20, "aidp-snowflake",      "sfUser/sfPassword",                  "snowflake_read.ipynb",           "Spark DataFrame from a Snowflake table via Snowflake connector",   False),
    LiveTestRow(21, "aidp-azure-adls",     "OAuth client-credentials",           "adls_read.ipynb",                "Spark CSV read from an ADLS Gen2 container",                       False),
    LiveTestRow(22, "aidp-aws-s3",         "AWS access key",                     "s3_read.ipynb",                  "Spark JSON read from an S3 bucket via s3a://",                     False),
    LiveTestRow(23, "aidp-rest-generic",   "HTTP Basic + manifest",              "rest_generic_read.ipynb",        "Spark DataFrame from a REST API with manifest schema",             False),
    LiveTestRow(24, "aidp-jdbc-custom",    "Driver-specific (SQLite memory)",    "jdbc_custom_sqlite.ipynb",       "Spark JDBC plumbing smoke test against in-memory SQLite",          False),
    LiveTestRow(25, "aidp-excel",          "None (file-based)",                  "excel_read.ipynb",               "Spark DataFrame from a .xlsx via pandas->CSV fallback",            False),
]


_SUMMARY_RE = re.compile(
    r"AIDP_LIVE_TEST_RESULT_BEGIN\s*(?P<json>\{.*?\})\s*AIDP_LIVE_TEST_RESULT_END",
    re.DOTALL,
)


def parse_summary(stdout: str) -> Optional[dict]:
    """Extract the JSON summary that the final cell of every notebook prints.

    Notebooks emit:
        AIDP_LIVE_TEST_RESULT_BEGIN
        { "connector": ..., "auth": ..., "rows": ..., "schema": [...], "timestamp_utc": ... }
        AIDP_LIVE_TEST_RESULT_END

    Returns the parsed dict, or None if markers weren't found.
    """
    match = _SUMMARY_RE.search(stdout)
    if not match:
        return None
    try:
        return json.loads(match.group("json"))
    except json.JSONDecodeError:
        return None


def regenerate_results_md() -> str:
    """Build tests/live-results/RESULTS.md from the per-row JSON files.

    Returns the markdown body (also writes it to disk).
    """
    # Compute summary
    counts = {"PASS": 0, "FAIL": 0, "DEFERRED": 0, "BLOCKED": 0, "NOT RUN": 0}
    statuses = {}
    for row in LIVE_TEST_ROWS:
        result_path = LIVE_RESULTS / f"row{row.row_number:02d}.json"
        if result_path.exists():
            data = json.loads(result_path.read_text())
            explicit = data.get("result")
            row_count = data.get("rows")
            if explicit == "PASS" or (explicit is None and row_count and row_count > 0):
                statuses[row.row_number] = "PASS"
            elif explicit in ("FAIL", "DEFERRED", "BLOCKED"):
                statuses[row.row_number] = explicit
            else:
                statuses[row.row_number] = "UNKNOWN"
        else:
            statuses[row.row_number] = "NOT RUN"
        counts[statuses[row.row_number]] = counts.get(statuses[row.row_number], 0) + 1

    summary = ", ".join(f"{n} {k}" for k, n in counts.items() if n > 0)

    lines = [
        "# Live-test results\n",
        "",
        f"**Summary:** {summary} out of {len(LIVE_TEST_ROWS)} rows.",
        "",
        "| # | Skill | Auth | Notebook | Status | Rows | Last run (UTC) |",
        "|---|---|---|---|---|---|---|",
    ]
    for row in LIVE_TEST_ROWS:
        result_path = LIVE_RESULTS / f"row{row.row_number:02d}.json"
        status = statuses.get(row.row_number, "NOT RUN")
        if result_path.exists():
            data = json.loads(result_path.read_text())
            row_count = data.get("rows")
            rows = row_count if row_count is not None else "-"
            ts = data.get("timestamp_utc", "-")
        else:
            rows = "-"
            ts = "-"
        lines.append(
            f"| {row.row_number} | `{row.skill}` | {row.auth_method} | "
            f"[`{row.notebook}`](../../examples/{row.notebook}) | {status} | {rows} | {ts} |"
        )

    body = "\n".join(lines) + "\n"
    (LIVE_RESULTS / "RESULTS.md").write_text(body, encoding="utf-8")
    return body


if __name__ == "__main__":
    body = regenerate_results_md()
    print(body)
