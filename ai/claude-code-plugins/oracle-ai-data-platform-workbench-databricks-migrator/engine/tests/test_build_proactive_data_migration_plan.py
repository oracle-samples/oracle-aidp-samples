import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from build_proactive_data_migration_plan import build_plan


def test_empty_target_plan_keeps_available_inputs_and_excludes_outputs():
    report = {"notebooks": {
        "/Workspace/a.ipynb": {
            "read_tables": [
                {"table": "src.monthly", "exists": True,
                 "filters": ["month_start_date = '2026-08-01'"],
                 "partition_column": "month_start_date", "partition_value": "2026-08-01"},
                {"table": "tmp_view", "exists": False},
                {"table": "derived.output", "exists": True},
            ],
            "read_paths": [{"source": "s3://bucket/full/", "oci_path": "oci://target@ns/full/", "exists": True}],
            "write_tables": ["derived.output"],
            "write_paths": [],
            "unresolved": [],
        }
    }}
    plan = build_plan(report, "123")
    assert plan["summary"]["requirements"] == 2
    monthly = next(row for row in plan["requirements"] if row["object_name"] == "src.monthly")
    assert monthly["request_type"] == "PARTITION_BACKFILL"
    assert monthly["partition_value"] == "2026-08-01"
    assert any(row["object_name"] == "derived.output" for row in plan["exclusions"])
    assert any(row["object_name"] == "tmp_view" for row in plan["exclusions"])


def test_date_range_and_unmapped_path_are_preserved():
    report = {"notebooks": {"/Workspace/a.ipynb": {
        "read_tables": [{"table": "src.history", "exists": True,
                         "filters": ["created_at >= '2026-08-01'", "created_at <= '2026-08-10'"]}],
        "read_paths": [{"source": "/FileStore/input.csv", "exists": "unmapped", "source_format": "csv"}],
        "write_tables": [], "write_paths": [], "unresolved": [],
    }}}
    plan = build_plan(report, "123")
    history = next(row for row in plan["requirements"] if row["object_name"] == "src.history")
    assert history["request_type"] == "DATE_RANGE_BACKFILL"
    assert history["start_date"] == "2026-08-01"
    assert history["end_date"] == "2026-08-10"
    unmapped = next(row for row in plan["requirements"] if row["source_path"] == "/FileStore/input.csv")
    assert unmapped["status"] == "MAPPING_REQUIRED"


def test_distinct_scopes_for_same_table_create_union_requirements():
    report = {"notebooks": {"/Workspace/a.ipynb": {
        "read_tables": [
            {"table": "src.monthly", "exists": True,
             "filters": ["month_start_date = '2026-07-01'"],
             "partition_column": "month_start_date",
             "partition_value": "2026-07-01"},
            {"table": "src.monthly", "exists": True,
             "filters": ["month_start_date = '2026-08-01'"],
             "partition_column": "month_start_date",
             "partition_value": "2026-08-01"},
        ],
        "read_paths": [], "write_tables": [], "write_paths": [],
        "unresolved": [],
    }}}
    plan = build_plan(report, "123")
    monthly = [
        row for row in plan["requirements"]
        if row["object_name"] == "src.monthly"
    ]
    assert {row["partition_value"] for row in monthly} == {
        "2026-07-01", "2026-08-01",
    }


def test_filename_date_range_and_pyspark_in_filter_are_structured():
    report = {"notebooks": {"/Workspace/a.ipynb": {
        "read_tables": [],
        "read_paths": [
            {
                "source": "/FileStore/input_2026-08-01_2026_08_10.csv",
                "exists": "unmapped",
                "source_format": "csv",
                "filters": ["col('meta_address_id').isNotNull()"],
            },
            {
                "source": "s3://bucket/geo",
                "oci_path": "oci://target@ns/geo",
                "source_format": "org.apache.hudi",
                "filters": ["classification IN (['VKYC_LOCATION', 'UNDERWRITING'])"],
            },
        ],
        "write_tables": [], "write_paths": [], "unresolved": [],
    }}}

    plan = build_plan(report, "123")
    dated = next(row for row in plan["requirements"] if row["source_path"].startswith("/FileStore"))
    assert dated["request_type"] == "DATE_RANGE_BACKFILL"
    assert dated["start_date"] == "2026-08-01"
    assert dated["end_date"] == "2026-08-10"
    assert dated["partition_column"] is None

    geo = next(row for row in plan["requirements"] if row["source_path"] == "s3://bucket/geo")
    assert geo["request_type"] == "PARTIAL"
    assert geo["filters"] == ["classification IN ('VKYC_LOCATION', 'UNDERWRITING')"]


def test_append_history_and_same_cell_read_modify_write_are_requirements():
    report = {"notebooks": {
        "/Workspace/task1.ipynb": {
            "read_tables": [], "read_paths": [], "unresolved": [],
            "write_tables": ["collections.append_history"], "write_paths": [],
            "write_references": [{
                "object_type": "TABLE", "object_name": "collections.append_history",
                "usage": "write_create", "write_mode": "append", "cell_number": 3,
            }],
        },
        "/Workspace/task2.ipynb": {
            "read_tables": [
                {"table": "collections.append_history", "cell_number": 5,
                 "filters": ["created_timestamp = latest_timestamp"]},
                {"table": "collections.rewrite_history", "cell_number": 10,
                 "filters": ["run_date != '2026-08-10'"]},
                {"table": "collections.generated", "cell_number": 20},
            ],
            "read_paths": [], "unresolved": [], "write_paths": [],
            "write_tables": ["collections.rewrite_history", "collections.generated"],
            "write_references": [
                {"object_type": "TABLE", "object_name": "collections.rewrite_history",
                 "usage": "write_create", "write_mode": "overwrite", "cell_number": 10},
                {"object_type": "TABLE", "object_name": "collections.rewrite_history",
                 "usage": "write_create", "write_mode": "append", "cell_number": 12},
                {"object_type": "TABLE", "object_name": "collections.generated",
                 "usage": "write_create", "write_mode": "overwrite", "cell_number": 2},
            ],
        },
    }}
    plan = build_plan(report, "123")
    by_name = {row["object_name"]: row for row in plan["requirements"]}
    append_history = by_name["collections.append_history"]
    assert append_history["request_type"] == "FULL"
    assert append_history["stateful_reason"] == "STATEFUL_APPEND_HISTORY"
    rewrite = by_name["collections.rewrite_history"]
    assert rewrite["request_type"] == "PARTIAL"
    assert rewrite["filters"] == ["run_date != '2026-08-10'"]
    assert rewrite["stateful_reason"] == "STATEFUL_READ_BEFORE_WRITE"
    assert "collections.generated" not in by_name


def test_unresolved_scopes_are_preserved_in_migration_contract():
    report = {"notebooks": {"/Workspace/a.ipynb": {
        "read_tables": [], "read_paths": [], "write_tables": [],
        "write_paths": [], "unresolved": ["spark.read.table(dynamic_name)"],
        "unresolved_scopes": [{
            "object_type": "table", "argument_expression": "dynamic_name",
            "filters": ["run_date = '2026-08-25'"],
        }],
    }}}
    plan = build_plan(report, "123")
    assert plan["summary"]["unresolved_references"] == 2
    assert {row["category"] for row in plan["unresolved"]} == {
        "DYNAMIC_REFERENCE", "UNRESOLVED_SCOPE",
    }
