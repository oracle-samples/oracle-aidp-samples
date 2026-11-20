"""Tests for nightshift.fingerprint. Local only; no Spark, no network."""
from __future__ import annotations

import re

import pytest

from nightshift.config import MODEL_LADDER
from nightshift.fingerprint import (
    AI_TEXT_MAX_CHARS,
    LABELS,
    RULES,
    ai_generate_sql,
    fingerprint,
    fingerprint_key,
    normalise,
    parse_ai_label,
)

README_LABELS = {
    "TIMEOUT", "OUT_OF_MEMORY", "PERMISSION_DENIED", "RATE_LIMIT", "NOT_FOUND", "CONNECTION_REFUSED",
    "SCHEMA_MISMATCH", "SKEW", "SMALL_FILES", "UPSTREAM_MISSING", "BAD_VALUE", "SYNTAX_ERROR",
    "POLICY_BLOCKED", "UNKNOWN",
    "EXTERNAL_SYSTEM_ERROR",
}

# (error snippet, expected label) -- realistic Spark / Delta / Python / JDBC / OCI SDK text
CASES = [
    # OUT_OF_MEMORY
    ("java.lang.OutOfMemoryError: Java heap space", "OUT_OF_MEMORY"),
    ("Container killed by YARN for exceeding memory limits. 12.4 GB of 12 GB physical memory used. "
     "Consider boosting spark.yarn.executor.memoryOverhead", "OUT_OF_MEMORY"),
    ("ExecutorLostFailure (executor 3 exited unrelated to the running tasks) Reason: Container marked as failed",
     "OUT_OF_MEMORY"),
    ("java.lang.OutOfMemoryError: GC overhead limit exceeded", "OUT_OF_MEMORY"),
    ("MemoryError: Unable to allocate 7.45 GiB for an array with shape (1000000000,)", "OUT_OF_MEMORY"),
    # TIMEOUT
    ("org.apache.spark.SparkException: Job aborted due to stage failure: Task 12 in stage 4.0 failed 4 times, "
     "most recent failure: task timeout", "TIMEOUT"),
    ("java.util.concurrent.TimeoutException: Futures timed out after [300 seconds]", "TIMEOUT"),
    ("java.net.SocketTimeoutException: Read timed out", "TIMEOUT"),
    ("requests.exceptions.ReadTimeout: HTTPSConnectionPool(host='example.oraclecloud.com', port=443)",
     "TIMEOUT"),
    # UPSTREAM_MISSING
    ("org.apache.spark.sql.AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `bronze`.`orders_raw` "
     "cannot be found.", "UPSTREAM_MISSING"),
    ("AnalysisException: Path does not exist: oci://landing@frxyz/sales/2026-11-01/", "UPSTREAM_MISSING"),
    ("[DELTA_PATH_DOES_NOT_EXIST] oci://lake@frxyz/bronze/orders doesn't exist", "UPSTREAM_MISSING"),
    ("java.io.FileNotFoundException: /mnt/staging/part-00001.parquet (No such file or directory)", "UPSTREAM_MISSING"),
    # SCHEMA_MISMATCH
    ("AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `cust_id` "
     "cannot be resolved. Did you mean one of the following? [`customer_id`]", "SCHEMA_MISMATCH"),
    ("cannot resolve 'order_total' given input columns: [order_id, amount]", "SCHEMA_MISMATCH"),
    ("[DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields 'amount' and 'amount'. Failed to merge incompatible "
     "data types DecimalType(18,2) and StringType", "SCHEMA_MISMATCH"),
    ("A schema mismatch detected when writing to the Delta table (Table ID: 3f9a2b1c-7d8e-4f10-9a2b-1c3d4e5f6a7b)",
     "SCHEMA_MISMATCH"),
    # PERMISSION_DENIED
    ("com.oracle.bmc.model.BmcException: (403, NotAuthorizedOrNotFound, false) Authorization failed or requested "
     "resource not found.", "PERMISSION_DENIED"),
    ("HTTP 403 Forbidden", "PERMISSION_DENIED"),
    ("PERMISSION_DENIED: User does not have SELECT on table gold.customer_360", "PERMISSION_DENIED"),
    # RATE_LIMIT
    ("BmcException: (429, TooManyRequests, false) Too many requests for the user", "RATE_LIMIT"),
    ("Request was throttled. Retry after 30 seconds", "RATE_LIMIT"),
    # SYNTAX_ERROR
    ("org.apache.spark.sql.catalyst.parser.ParseException: [PARSE_SYNTAX_ERROR] Syntax error at or near 'FORM'",
     "SYNTAX_ERROR"),
    ("ParseException: mismatched input 'SELCT' expecting {'(', 'SELECT', 'WITH'}", "SYNTAX_ERROR"),
    # BAD_VALUE
    ("pyspark.errors.exceptions.captured.AnalysisException: [CAST_INVALID_INPUT] The value 'N/A' of the type "
     "STRING cannot be cast to INT", "BAD_VALUE"),
    ('java.lang.NumberFormatException: For input string: "12,50"', "BAD_VALUE"),
    ("java.time.format.DateTimeParseException: Text '31/02/2026' could not be parsed: Invalid date", "BAD_VALUE"),
    # CONNECTION_REFUSED
    ("java.net.ConnectException: Connection refused (Connection refused)", "CONNECTION_REFUSED"),
    ("psycopg2.OperationalError: could not connect to server: Connection refused", "CONNECTION_REFUSED"),
    ("java.sql.SQLRecoverableException: IO Error: The Network Adapter could not establish the connection "
     "(CONNECTION_ID=...)", "CONNECTION_REFUSED"),
    # POLICY_BLOCKED
    ("Policy denied: CONFIRMATION_REQUIRED for action job_update_schedule", "POLICY_BLOCKED"),
    ("Action blocked by policy rule gold.no_write", "POLICY_BLOCKED"),
    # SKEW
    ("Stage 7: 199 of 200 tasks finished in 40 s; one task still running after 38 min with 14.2 GiB spill to disk",
     "SKEW"),
    ("Detected data skew in join on key region_id", "SKEW"),
    # SMALL_FILES
    ("Too many small files: 48,211 files under 1 MB in gold.events", "SMALL_FILES"),
    # NOT_FOUND
    ("BmcException: (404, NotFound, false) The job run was not found", "NOT_FOUND"),
    ("ModuleNotFoundError: No module named 'sqlglot'", "NOT_FOUND"),
    # UNKNOWN
    ("Everything went fine, exit code 0", "UNKNOWN"),
    ("", "UNKNOWN"),
]


def test_labels_match_readme():
    assert set(LABELS) == README_LABELS
    assert len(LABELS) == 15
    assert all(label in LABELS for label, _, _ in RULES)
    assert all(0.0 < conf <= 1.0 for _, _, conf in RULES)


def test_enough_cases():
    assert len(CASES) >= 25


@pytest.mark.parametrize("text,expected", CASES, ids=[c[1] + ":" + c[0][:30] for c in CASES])
def test_fingerprint_label(text, expected):
    label, confidence = fingerprint(text)
    assert label == expected
    assert 0.0 <= confidence <= 1.0
    if expected == "UNKNOWN":
        assert confidence == 0.0
    else:
        assert confidence > 0.0


def test_priority_policy_beats_iam_and_oci_403_beats_not_found():
    assert fingerprint("Policy denied (rule gold.no_write): 403 would also appear here")[0] == "POLICY_BLOCKED"
    assert fingerprint("(403, NotAuthorizedOrNotFound)")[0] == "PERMISSION_DENIED"
    assert fingerprint("[TABLE_OR_VIEW_NOT_FOUND] not found")[0] == "UPSTREAM_MISSING"


def test_fingerprint_is_deterministic():
    text = CASES[0][0]
    assert all(fingerprint(text) == fingerprint(text) for _ in range(5))


def test_normalise_strips_volatile_tokens():
    a = ("2026-11-01T03:12:44.123Z ERROR executor 7 app application_1761966000000_0042 path "
         "oci://b@frxyz/x/2026-11-01/part-00001.parquet id 3f9a2b1c-1111-2222-3333-444455556666 "
         "0xdeadbeef42 took 1,234.5 ms at /opt/spark/work/app-20261101/0/stderr")
    b = ("2026-11-02T04:00:01.999Z ERROR executor 12 app application_1761970000000_0007 path "
         "oci://b@frxyz/y/2026-11-02/part-00119.parquet id 00000000-aaaa-bbbb-cccc-dddddddddddd "
         "0xcafebabe99 took 9.1 ms at /opt/spark/work/app-20261102/3/stderr")
    na, nb = normalise(a), normalise(b)
    assert na == nb
    for placeholder in ("<ts>", "<n>", "<path>", "<uuid>", "<hex>"):
        assert placeholder in na
    assert not re.search(r"\d", na.replace("<n>", "").replace("<ts>", ""))  # no raw digits survive
    assert normalise(r"C:\Users\someone\job.py line 42") == "<path> line <n>"
    assert normalise("  a   b \n c ") == "a b c"
    assert normalise("") == ""


def test_normalise_keeps_identifiers():
    n = normalise("cannot resolve 'order_total' given input columns: [order_id, amount]")
    assert "order_total" in n and "order_id" in n


def test_key_stability_and_shape():
    label, _ = fingerprint(CASES[0][0])
    k1 = fingerprint_key(label, normalise(CASES[0][0]))
    k2 = fingerprint_key(label, normalise(CASES[0][0] + "   "))
    assert k1 == k2
    assert re.fullmatch(r"[0-9a-f]{16}", k1)
    assert fingerprint_key("TIMEOUT", "x") != fingerprint_key("OUT_OF_MEMORY", "x")
    assert fingerprint_key("TIMEOUT", "x") != fingerprint_key("TIMEOUT", "y")


def test_same_failure_different_run_same_key():
    t1 = "2026-11-01 02:00:03 AnalysisException: Path does not exist: oci://landing@frxyz/sales/2026-11-01/run-0042"
    t2 = "2026-11-02 02:00:09 AnalysisException: Path does not exist: oci://landing@frxyz/sales/2026-11-02/run-0043"
    l1, l2 = fingerprint(t1)[0], fingerprint(t2)[0]
    assert l1 == l2 == "UPSTREAM_MISSING"
    assert fingerprint_key(l1, normalise(t1)) == fingerprint_key(l2, normalise(t2))


def test_ai_generate_sql_default_model_and_shape():
    sql = ai_generate_sql(None, "boom")
    assert sql.startswith(f"SELECT ai_generate('{MODEL_LADDER['low_compute']}', '")
    assert sql.endswith("') AS label")
    assert sql.count("ai_generate(") == 1
    for label in LABELS:
        assert label in sql
    assert "openai" not in sql.lower()


def test_ai_generate_sql_escapes_quotes_and_backslashes():
    text = "it's 'quoted' and C:\\tmp\\x\nnext line"
    sql = ai_generate_sql("xai.grok-4.7", text)
    assert "it\\'s \\'quoted\\'" in sql
    assert "C:\\\\tmp\\\\x" in sql
    assert "\n" not in sql
    # the literal is balanced: every quote inside the prompt is backslash-escaped
    inner = sql[len("SELECT ai_generate('xai.grok-4.7', '"):-len("') AS label")]
    assert "'" not in inner.replace("\\'", "")


def test_ai_generate_sql_truncates():
    marker = "Z" * 5000
    sql = ai_generate_sql(None, marker)
    assert "Z" * AI_TEXT_MAX_CHARS in sql
    assert "Z" * (AI_TEXT_MAX_CHARS + 1) not in sql


def test_ai_generate_sql_refuses_openai_and_missing_model():
    with pytest.raises(ValueError, match="is refused"):
        ai_generate_sql("openai.gpt-4o", "boom")
    with pytest.raises(ValueError):
        ai_generate_sql("", "boom")  # empty model name
    # None means "use the default"; critical/paused tiers have no model and callers must check the ladder first
    assert MODEL_LADDER["critical"] is None and MODEL_LADDER["paused"] is None


@pytest.mark.parametrize("raw,expected", [
    ("OUT_OF_MEMORY", "OUT_OF_MEMORY"),
    ("  out of memory.\n", "OUT_OF_MEMORY"),
    ("`SCHEMA_MISMATCH`", "SCHEMA_MISMATCH"),
    ("Label: schema-mismatch", "SCHEMA_MISMATCH"),
    ("I think this is a TIMEOUT rather than NOT_FOUND", "TIMEOUT"),
    ("UPSTREAM_MISSING (the table is gone)", "UPSTREAM_MISSING"),
    ("no idea", "UNKNOWN"),
    ("", "UNKNOWN"),
    (None, "UNKNOWN"),
])
def test_parse_ai_label(raw, expected):
    assert parse_ai_label(raw) == expected
    assert parse_ai_label(raw) in LABELS


def test_external_system_error_beats_timeout_and_keys_collapse():
    from nightshift.fingerprint import cause_signature, incident_fingerprint
    a = ("severity_tick: java.lang.RuntimeException: [Command 1111-aaaa in Context 2222] failed: "
         "ConnectorException: ORA-06550: line 1, column 7:\n"
         "Caused by: java.sql.SQLException: ORA-06550: line 1, column 7:\n"
         "\tat oracle.jdbc.driver.OracleStatement.doExecuteWithTimeout(OracleStatement.java:1633)")
    b = a.replace("1111-aaaa", "9999-ffff").replace("2222", "7777").replace("1633", "1701")
    la, ca, ka = incident_fingerprint(a)
    lb, cb, kb = incident_fingerprint(b)
    assert la == lb == "EXTERNAL_SYSTEM_ERROR"
    assert ka == kb
    assert "ora-" in cause_signature(a)


def test_timeout_does_not_match_inside_identifiers():
    from nightshift.fingerprint import fingerprint
    assert fingerprint("at oracle.jdbc.driver.OracleStatement.doExecuteWithTimeout(OracleStatement.java:1633)")[0] != "TIMEOUT"
    assert fingerprint("java.util.concurrent.TimeoutException: Futures timed out after [300 seconds]")[0] == "TIMEOUT"
