"""REF-257: probe access failures are failure detail, not "missing" data.

Incident (Example_Alerts_Summary, DBX job 100000000000003): Phase 1 probed two
paths under oci://oci-example-dp-rds@exmplnspace2/, the HDFS connector threw
``IOException: Unable to determine if path is a directory`` whose *truncated*
record collapsed to ``exists: False`` → counted paths_missing → a data-copy
request that cannot fix the real cause. Live diagnosis at the cluster showed
the cause line the 150-char truncation discarded::

    Caused by: BmcException: ... ListObjects ... (404, BucketNotFound, false)
    Either the bucket ... does not exist ... or you are not authorized ...

(The bucket exists — the sibling path ``example_db2/metrics`` probed True — OCI
masks authorization failures as 404.) These tests pin the fixed contract:
exceptions classify into explicit probe-failure statuses with the cause
preserved, counted apart from "missing", aggregated per bucket@namespace,
and carried into accurate stakeholder messages.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import check_data_availability_for_workflow as cda  # noqa: E402
from build_proactive_data_migration_plan import build_plan  # noqa: E402


# The exact live cause chain captured on cluster cccccc16 (2026-10-27).
BMC_404_TEXT = (
    "An error occurred while calling o350.exists.\n"
    ": java.io.IOException: Unable to determine if path is a directory\n"
    "\tat com.oracle.bmc.hdfs.store.BmcDataStore.getDirectoryStatus(BmcDataStore.java:1457)\n"
    "Caused by: com.oracle.bmc.model.BmcException: Error returned by ListObjects "
    "operation in ObjectStorage service.(404, BucketNotFound, false) Either the "
    "bucket named 'oci-example-dp-rds' does not exist in the namespace "
    "'exmplnspace2' or you are not authorized to access it\n"
)
BMC_403_TEXT = (
    "An error occurred while calling o1.exists.\n"
    "Caused by: com.oracle.bmc.model.BmcException: (403, AccessDenied, false) "
    "Access denied on bucket x\n"
)


class _Raise:
    def __init__(self, text):
        self._text = text

    def __call__(self, *a, **k):
        raise RuntimeError(self._text)

    def __getattr__(self, name):
        return self


class _FakeJvmPath:
    def __init__(self, text=None, exists=None):
        self.text, self.exists_value = text, exists


def _run_probe_code(behaviour):
    """Execute the REAL generated kernel probe code with a scripted fs.

    behaviour: {path: exception-text | bool} — a string raises it from
    fs.exists; a bool is returned from fs.exists (False = clean missing).
    """
    specs = [{"path": p, "probe_path": p} for p in behaviour]
    code = cda._oci_path_probe_code(json.dumps(specs))

    class _Fs:
        @staticmethod
        def exists(p):
            action = behaviour[p.target]
            if isinstance(action, str):
                raise RuntimeError(action)
            return action

        @staticmethod
        def listStatus(p):
            return []

    class _PathCls:
        def __init__(self, target):
            self.target = target

    class _Jvm:
        class java:  # noqa: N801 — mirrors jvm attribute shape
            class net:
                URI = staticmethod(lambda t: t)

        class org:  # noqa: N801
            class apache:
                class hadoop:
                    class fs:
                        Path = _PathCls
                        FileSystem = type("FS", (), {"get": staticmethod(lambda u, c: _Fs)})

    class _Sc:
        class _jsc:
            @staticmethod
            def hadoopConfiguration():
                return {}

    spark = type("S", (), {"_jvm": _Jvm, "sparkContext": _Sc})
    printed = []
    exec(code, {"spark": spark, "print": lambda *a: printed.append(" ".join(map(str, a)))})
    return json.loads(printed[-1])


def test_bucketnotfound_exception_classifies_access_error_with_cause():
    out = _run_probe_code({"oci://b@ns/x": BMC_404_TEXT})
    rec = out["oci://b@ns/x"]
    assert rec["exists"] == "access_error"
    # the decisive Caused-by line survives, not a blind 150-char prefix
    assert "BucketNotFound" in rec["error"]
    assert "not authorized" in rec["error"]


def test_403_classifies_access_denied():
    out = _run_probe_code({"oci://b@ns/x": BMC_403_TEXT})
    assert out["oci://b@ns/x"]["exists"] == "access_denied"


def test_other_exception_classifies_probe_error_never_missing():
    out = _run_probe_code({"oci://b@ns/x": "java.net.SocketTimeoutException: read timed out"})
    rec = out["oci://b@ns/x"]
    assert rec["exists"] == "probe_error"
    assert rec["exists"] is not False


def test_clean_fs_exists_false_is_still_missing():
    out = _run_probe_code({"oci://b@ns/x": False})
    assert out["oci://b@ns/x"]["exists"] is False


def test_true_path_unaffected():
    out = _run_probe_code({"oci://b@ns/x": True})
    assert out["oci://b@ns/x"]["exists"] is True


def _report_with_entries(entries):
    report = {
        "job_name": "t", "summary": {"shared_access_failures": 0},
        "notebooks": {"/nb": {"read_paths": entries}},
    }
    cda._aggregate_shared_access_failures(report)
    return report


def test_shared_authority_failures_aggregate_to_one_finding():
    entries = [
        {"source": "s3://examplecorp-dp-unit-rds/rds-and-kafka/example_db2/mars_evaluations",
         "oci_path": "oci://oci-example-dp-rds@exmplnspace2/rds-and-kafka/example_db2/mars_evaluations",
         "probe_status": "ACCESS_ERROR", "error": "Caused by: BmcException ... BucketNotFound ..."},
        {"source": "s3://examplecorp-dp-unit-rds/rds-and-kafka/example_db2/mars_evaluations_anomalous_results",
         "oci_path": "oci://oci-example-dp-rds@exmplnspace2/rds-and-kafka/example_db2/mars_evaluations_anomalous_results",
         "probe_status": "ACCESS_ERROR", "error": "Caused by: BmcException ... BucketNotFound ..."},
        {"source": "s3://other/ok", "oci_path": "oci://other@ns/ok", "exists": True},
    ]
    report = _report_with_entries(entries)
    findings = report["shared_access_failures"]
    assert len(findings) == 1
    finding = findings[0]
    assert finding["authority"] == "oci-example-dp-rds@exmplnspace2"
    assert finding["failure_classes"] == ["ACCESS_ERROR"]
    assert len(finding["affected_paths"]) == 2
    assert "BucketNotFound" in finding["cause"]
    assert report["summary"]["shared_access_failures"] == 1


def test_no_failures_means_no_finding_section():
    report = _report_with_entries([{"source": "s3://b/x", "oci_path": "oci://b@ns/x", "exists": True}])
    assert "shared_access_failures" not in report


def test_plan_row_carries_access_failure_message():
    report = {
        "notebooks": {"/nb": {
            "read_paths": [{
                "source": "s3://examplecorp-dp-unit-rds/rds-and-kafka/example_db2/mars_evaluations",
                "oci_path": "oci://oci-example-dp-rds@exmplnspace2/rds-and-kafka/example_db2/mars_evaluations",
                "exists": "access_error", "probe_status": "ACCESS_ERROR",
                "object_type": "DIRECTORY",
                "error": "Caused by: BmcException ... BucketNotFound ...",
            }],
        }},
    }
    plan = build_plan(report, "100000000000003")
    rows = [r for r in plan["requirements"] if "mars_evaluations" in r["object_name"]]
    assert rows, plan["requirements"]
    row = rows[0]
    assert row["access_failure"] == "ACCESS_ERROR"
    assert "not authorized" in row["stakeholder_message"]
    assert "read+list" in row["stakeholder_message"]
    # the misleading plain "Prepare ... before migration." phrasing is gone
    assert not row["stakeholder_message"].startswith("Prepare ")


def test_plan_row_without_failure_keeps_legacy_message():
    report = {
        "notebooks": {"/nb": {
            "read_paths": [{
                "source": "s3://b/data", "oci_path": "oci://b@ns/data",
                "exists": False, "object_type": "DIRECTORY",
            }],
        }},
    }
    plan = build_plan(report, "1")
    row = [r for r in plan["requirements"] if r["object_name"] == "oci://b@ns/data"][0]
    assert row["access_failure"] == ""
    assert row["stakeholder_message"].startswith("Prepare ")


# ── Report-level coverage (review round 2026-10-27) ──────────────────────────

def _summary_with(**overrides):
    base = {"tables_found": 0, "tables_missing": 0, "tables_unverified": 0,
            "paths_found": 0, "paths_missing": 0, "paths_unverified": 0,
            "scopes_unverified": 0, "paths_unmapped": 0,
            "paths_access_errors": 0, "unresolved": 0,
            "mlflow_artifacts_found": 0, "mlflow_artifacts_missing": 0,
            "mlflow_artifact_probe_errors": 0,
            "libs_installed": 0, "libs_missing": 0, "libs_unverified": 0}
    base.update(overrides)
    return base


def test_access_errors_never_render_ready_to_migrate(capsys):
    # Pre-fix regression shape: a sole access-failing read left every
    # missing/unverified counter at 0 and the verdict said ready-to-migrate.
    report = {
        "job_name": "t", "generated_at": "now",
        "summary": _summary_with(paths_access_errors=1),
        "notebooks": {"/nb": {"read_paths": [{
            "source": "s3://examplecorp-dp-unit-rds/rds-and-kafka/example_db2/mars_evaluations",
            "type": "s3",
            "oci_path": "oci://oci-example-dp-rds@exmplnspace2/rds-and-kafka/example_db2/mars_evaluations",
            "exists": "access_error", "probe_status": "ACCESS_ERROR",
            "error": "Caused by: BmcException ... BucketNotFound ...",
        }], "read_tables": [], "write_tables": [], "write_paths": [],
            "unresolved": [], "unresolved_scopes": [],
            "unresolved_mlflow_artifacts": [], "libraries": [],
            "excluded_reads": [], "mlflow_artifacts": []}},
    }
    cda.print_report(report)
    out = capsys.readouterr().out
    assert "ready to migrate" not in out
    assert "manual review required" in out
    # rendering names the state instead of lying MISSING
    assert "ACCESS ERROR (bucket not found or not authorized)" in out
    assert "MISSING" not in out.split("RESULT:")[0].replace(
        "mlflow", "")  # no per-path MISSING label for this entry


def test_external_connector_read_keeps_access_classification():
    notebook = {"read_paths": []}
    summary = _summary_with()
    read = {"source_connector": "jdbc-ish", "source_object": "s3://b/x",
            "target_object": "oci://oci-example-dp-rds@exmplnspace2/x",
            "object_type": "path"}
    oci_results = {"oci://oci-example-dp-rds@exmplnspace2/x": {
        "exists": "access_error",
        "error": "Caused by: BmcException ... BucketNotFound ..."}}
    cda._append_external_read_report(notebook, summary, read, {}, oci_results, {})
    entry = notebook["read_paths"][0]
    assert entry["probe_status"] == "ACCESS_ERROR"
    assert "BucketNotFound" in entry["error"]
    assert summary["paths_access_errors"] == 1
    assert summary["paths_unverified"] == 0


def test_timeout_renders_timeout_not_missing():
    # Local-path probes can return "timeout"; the centralized renderer must
    # keep the legacy TIMEOUT label rather than falling through to MISSING.
    assert cda._render_path_status("timeout") == "TIMEOUT"
    assert cda._render_path_status(False) == "MISSING"
    assert cda._render_path_status("unverified") == "UNVERIFIED"


def _run_probe_code_with_list(behaviour, list_behaviour):
    """Like _run_probe_code but lets tests script listStatus too."""
    specs = [{"path": p, "probe_path": p} for p in behaviour]
    code = cda._oci_path_probe_code(json.dumps(specs))

    class _Fs:
        @staticmethod
        def exists(p):
            action = behaviour[p.target]
            if isinstance(action, str):
                raise RuntimeError(action)
            return action

        @staticmethod
        def listStatus(p):
            action = list_behaviour.get(p.target)
            if isinstance(action, str):
                raise RuntimeError(action)
            return action or []

    class _PathCls:
        def __init__(self, target):
            self.target = target

    class _Jvm:
        class java:  # noqa: N801
            class net:
                URI = staticmethod(lambda t: t)

        class org:  # noqa: N801
            class apache:
                class hadoop:
                    class fs:
                        Path = _PathCls
                        FileSystem = type("FS", (), {"get": staticmethod(lambda u, c: _Fs)})

    class _Sc:
        class _jsc:
            @staticmethod
            def hadoopConfiguration():
                return {}

    spark = type("S", (), {"_jvm": _Jvm, "sparkContext": _Sc})
    printed = []
    exec(code, {"spark": spark, "print": lambda *a: printed.append(" ".join(map(str, a)))})
    return json.loads(printed[-1])


def test_liststatus_failure_is_never_found():
    # Reproduced review finding: exists=True + listStatus raising ListObjects
    # BucketNotFound recorded {"exists": true, "items": "?"} — a false FOUND
    # that could render a ready verdict. Head-allowed/List-denied is exactly
    # the Mars IAM shape; Spark reads fail at runtime.
    out = _run_probe_code_with_list(
        {"oci://b@ns/x": True},
        {"oci://b@ns/x": "Caused by: com.oracle.bmc.model.BmcException: "
                          "(404, BucketNotFound, false) ListObjects denied"})
    rec = out["oci://b@ns/x"]
    assert rec["exists"] == "access_error"
    assert "BucketNotFound" in rec["error"]


def test_liststatus_transient_failure_is_probe_error():
    out = _run_probe_code_with_list(
        {"oci://b@ns/x": True},
        {"oci://b@ns/x": "java.net.SocketTimeoutException: read timed out"})
    assert out["oci://b@ns/x"]["exists"] == "probe_error"


def test_liststatus_success_still_counts_items():
    out = _run_probe_code_with_list({"oci://b@ns/x": True},
                                    {"oci://b@ns/x": ["a", "b"]})
    rec = out["oci://b@ns/x"]
    assert rec["exists"] is True and rec["items"] == 2


def test_probe_to_verdict_liststatus_denial_requires_review(capsys):
    # Probe -> report-shape -> verdict: the listStatus denial must reach the
    # manual-review verdict, never ready-to-migrate.
    out = _run_probe_code_with_list(
        {"oci://b@ns/x": True},
        {"oci://b@ns/x": "Caused by: BmcException: (404, BucketNotFound, false)"})
    status = out["oci://b@ns/x"]["exists"]
    report = {
        "job_name": "t", "generated_at": "now",
        "summary": _summary_with(paths_access_errors=1),
        "notebooks": {"/nb": {"read_paths": [{
            "source": "s3://b/x", "type": "s3", "oci_path": "oci://b@ns/x",
            "exists": status,
            "probe_status": cda._PROBE_FAILURE_STATUSES[status],
            "error": out["oci://b@ns/x"]["error"],
        }], "read_tables": [], "write_tables": [], "write_paths": [],
            "unresolved": [], "unresolved_scopes": [],
            "unresolved_mlflow_artifacts": [], "libraries": [],
            "excluded_reads": [], "mlflow_artifacts": []}},
    }
    cda.print_report(report)
    text = capsys.readouterr().out
    assert "ready to migrate" not in text
    assert "manual review required" in text


NESTED_CAUSE_TEXT = (
    "java.io.IOException: Unable to determine if path is a directory\n"
    "Caused by: java.io.IOException: getDirectoryStatus failed\n"
    "Caused by: com.oracle.bmc.model.BmcException: "
    "(404, BucketNotFound, false) ListObjects: bucket missing or not authorized"
)


def test_nested_cause_chain_keeps_decisive_bmc_line_exists_branch():
    # Review 2026-10-28: an intermediate IOException 'Caused by' preceding
    # the BmcException dropped the decisive OCI diagnosis from the record.
    rec = _run_probe_code({"oci://b@ns/x": NESTED_CAUSE_TEXT})["oci://b@ns/x"]
    assert rec["exists"] == "access_error"
    assert "BmcException" in rec["error"]
    assert "BucketNotFound" in rec["error"]
    assert "getDirectoryStatus" not in rec["error"]


def test_nested_cause_chain_keeps_decisive_bmc_line_liststatus_branch():
    rec = _run_probe_code_with_list(
        {"oci://b@ns/x": True}, {"oci://b@ns/x": NESTED_CAUSE_TEXT})["oci://b@ns/x"]
    assert rec["exists"] == "access_error"
    assert "BucketNotFound" in rec["error"]
    assert "getDirectoryStatus" not in rec["error"]


def test_plain_caused_by_without_bmc_still_preserved():
    text = ("java.io.IOException: outer\n"
            "Caused by: java.net.SocketTimeoutException: read timed out")
    rec = _run_probe_code({"oci://b@ns/x": text})["oci://b@ns/x"]
    assert rec["exists"] == "probe_error"
    assert "SocketTimeoutException" in rec["error"]


def test_probe_to_generate_report_to_verdict_end_to_end(capsys):
    # Review 2026-10-28: no manually supplied counters — the real generated
    # probe code produces the results, the real generate_report() builds the
    # report and counters, print_report() renders the verdict.
    import data_check_extract as dce
    nb = {"metadata": {"language_info": {"name": "python"}},
          "cells": [{"cell_type": "code", "source": [
              'df = spark.read.parquet("oci://b@ns/x")\n']}]}
    refs = {"/nb": dce.extract_from_notebook(nb)}
    oci_results = _run_probe_code_with_list(
        {"oci://b@ns/x": True}, {"oci://b@ns/x": NESTED_CAUSE_TEXT})
    report = cda.generate_report(refs, {}, oci_results, {}, {}, "job-t")
    assert report["summary"]["paths_access_errors"] == 1
    assert report["summary"]["paths_missing"] == 0
    finding = report["shared_access_failures"][0]
    assert finding["authority"] == "b@ns"
    assert "BucketNotFound" in finding["cause"]
    cda.print_report(report)
    out = capsys.readouterr().out
    assert "ready to migrate" not in out
    assert "manual review required" in out
