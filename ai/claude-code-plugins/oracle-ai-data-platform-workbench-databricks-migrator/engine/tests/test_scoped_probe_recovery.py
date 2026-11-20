import ast
import asyncio
import json
import re
from pathlib import Path


def _load_scoped_function():
    source_path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "check_data_availability_for_workflow.py"
    )
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    node = next(
        item for item in tree.body
        if isinstance(item, ast.AsyncFunctionDef)
        and item.name == "check_scoped_references"
    )
    calls = []

    async def execute_with_recovery(
        session, code, operation, timeout, attempts=2, probe_stage=None
    ):
        match = re.search(r"^specs = (.+)$", code, re.MULTILINE)
        batch = json.loads(match.group(1))
        calls.append((session, batch, operation, timeout, attempts))
        return {item["key"]: {"scope_exists": True} for item in batch}

    namespace = {
        "json": json,
        "_safe_scope_predicate": lambda filters: "id = 1" if filters else None,
        "_scope_key": lambda notebook, kind, name, filters: name,
        "map_to_oci_path": lambda path: {"oci_path": path},
        "_execute_json_probe_with_recovery": execute_with_recovery,
    }
    exec(
        compile(ast.Module(body=[node], type_ignores=[]), str(source_path), "exec"),
        namespace,
    )
    return namespace["check_scoped_references"], calls


def test_scoped_references_reuse_shared_recovery_in_small_batches():
    check_scoped_references, calls = _load_scoped_function()
    refs = {
        f"notebook-{index}": {
            "tables": [{"table": f"schema.table_{index}", "filters": ["id = 1"]}],
            "paths": [],
        }
        for index in range(6)
    }
    table_results = {
        f"schema.table_{index}": {
            "exists": True,
            "resolved": f"schema.table_{index}",
        }
        for index in range(6)
    }
    shared_stage = object()
    control_session = object()
    asyncio.run(check_scoped_references(
        control_session, refs, table_results, {}, probe_stage=shared_stage
    ))
    assert [len(call[1]) for call in calls] == [5, 1]
    assert all(call[3] == 300 for call in calls)
    assert all(call[0] is control_session for call in calls)
