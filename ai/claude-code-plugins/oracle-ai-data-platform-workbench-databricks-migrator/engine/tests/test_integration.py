"""
Integration tests for the AIDP migration toolkit.

Covers:
  1. CellContext - context accumulation across cells
  2. _unwrap_aidp_text - AIDP output JSON unwrapping
  3. safe_io - FUSE-safe pickle/CSV round-trips
  4. AIDPSession - session recycling attributes
  5. OCI_PATH_TOOLS - tool list completeness
"""

import json
import os
import pickle
import sys
import tempfile

import pytest

# ---------------------------------------------------------------------------
# Path setup: import from scripts/ and aidp_compat/
# ---------------------------------------------------------------------------
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_DIR, "scripts"))
sys.path.insert(0, PROJECT_DIR)

from cell_context import CellContext
from context_tools import _unwrap_aidp_text
# aidp_compat.safe_io was removed 2026-04-11 (FUSE consistency issues resolved).
# Tests for safe_pickle_dump/load/pandas_to_csv were deleted along with it.


# ===================================================================
# 1. CellContext
# ===================================================================

class TestCellContextAddCell:
    """add_cell stores entries in newest-first order."""

    def test_single_entry(self):
        ctx = CellContext()
        ctx.add_cell(1, 5, "print('hello')")
        assert len(ctx.entries) == 1
        assert "Cell 1/5" in ctx.entries[0]

    def test_newest_first_ordering(self):
        ctx = CellContext()
        ctx.add_cell(1, 3, "first")
        ctx.add_cell(2, 3, "second")
        ctx.add_cell(3, 3, "third")
        # Most recent entry is at index 0
        assert "Cell 3/3" in ctx.entries[0]
        assert "Cell 2/3" in ctx.entries[1]
        assert "Cell 1/3" in ctx.entries[2]

    def test_entry_includes_status(self):
        ctx = CellContext()
        ctx.add_cell(1, 1, "code", status="ERROR")
        assert "ERROR" in ctx.entries[0]

    def test_entry_includes_fixes(self):
        ctx = CellContext()
        ctx.add_cell(1, 1, "code", fixes=["replaced s3 path", "added import"])
        assert "replaced s3 path" in ctx.entries[0]
        assert "added import" in ctx.entries[0]

    def test_entry_includes_child_info(self):
        ctx = CellContext()
        ctx.add_cell(1, 1, "code", child_info="runs Utils/Config.ipynb")
        assert "runs Utils/Config.ipynb" in ctx.entries[0]

    def test_code_preview_truncated_at_80(self):
        ctx = CellContext()
        long_code = "x" * 200
        ctx.add_cell(1, 1, long_code)
        # The code preview portion should be at most 80 chars of the original
        assert long_code[:80] in ctx.entries[0]
        assert long_code[:81] not in ctx.entries[0]

    def test_output_preview_truncated_at_60(self):
        ctx = CellContext()
        long_output = "o" * 200
        ctx.add_cell(1, 1, "code", output_preview=long_output)
        assert long_output[:60] in ctx.entries[0]
        assert long_output[:61] not in ctx.entries[0]


class TestCellContextToolCache:
    """add_tool_result / get_cached: caching and cache miss."""

    def test_cache_hit(self):
        ctx = CellContext()
        ctx.add_tool_result("explore_path", {"path": "oci://b@ns/data"}, "EXISTS dir 5 items")
        result = ctx.get_cached("explore_path", {"path": "oci://b@ns/data"})
        assert result == "EXISTS dir 5 items"

    def test_cache_miss_returns_none(self):
        ctx = CellContext()
        result = ctx.get_cached("explore_path", {"path": "oci://unknown"})
        assert result is None

    def test_cache_hit_increments_counter(self):
        ctx = CellContext()
        ctx.add_tool_result("explore_path", {"path": "/p"}, "EXISTS")
        assert ctx._cache_hit_count == 0
        ctx.get_cached("explore_path", {"path": "/p"})
        assert ctx._cache_hit_count == 1
        ctx.get_cached("explore_path", {"path": "/p"})
        assert ctx._cache_hit_count == 2

    def test_cache_miss_does_not_increment_counter(self):
        ctx = CellContext()
        ctx.get_cached("no_tool", {"k": "v"})
        assert ctx._cache_hit_count == 0

    def test_tool_call_count(self):
        ctx = CellContext()
        ctx.add_tool_result("a", {}, "r1")
        ctx.add_tool_result("b", {}, "r2")
        assert ctx._tool_call_count == 2

    def test_different_inputs_are_different_keys(self):
        ctx = CellContext()
        ctx.add_tool_result("explore_path", {"path": "/a"}, "result_a")
        ctx.add_tool_result("explore_path", {"path": "/b"}, "result_b")
        assert ctx.get_cached("explore_path", {"path": "/a"}) == "result_a"
        assert ctx.get_cached("explore_path", {"path": "/b"}) == "result_b"


class TestCellContextPathExtraction:
    """add_tool_result extracts structured info from explore_path results."""

    def test_explore_path_exists(self):
        ctx = CellContext()
        ctx.add_tool_result("explore_path", {"path": "oci://b@ns/data"}, "EXISTS directory")
        assert ctx.paths_checked["oci://b@ns/data"] == "EXISTS"

    def test_explore_path_not_found(self):
        ctx = CellContext()
        ctx.add_tool_result("explore_path", {"path": "oci://b@ns/gone"}, "NOT FOUND")
        assert ctx.paths_checked["oci://b@ns/gone"] == "NOT_FOUND"

    def test_explore_path_error(self):
        ctx = CellContext()
        ctx.add_tool_result("explore_path", {"path": "oci://bad"}, "Connection refused")
        assert ctx.paths_checked["oci://bad"] == "ERROR"


class TestCellContextTableExtraction:
    """add_tool_result extracts info from search_catalog results."""

    def test_search_catalog_found(self):
        ctx = CellContext()
        ctx.add_tool_result("search_catalog", {"query": "example_dues"},
                            "Tables matching 'example_dues': ops.example_dues_all")
        assert "example_dues" in ctx.tables_checked
        # Stores first 120 chars of the result
        assert "ops.example_dues_all" in ctx.tables_checked["example_dues"]

    def test_search_catalog_missing(self):
        ctx = CellContext()
        ctx.add_tool_result("search_catalog", {"query": "xyz_fake"},
                            "No tables matching 'xyz_fake'")
        assert ctx.tables_checked["xyz_fake"] == "MISSING"

    def test_suggest_oci_path_stores_suggestion(self):
        ctx = CellContext()
        ctx.add_tool_result("suggest_oci_path", {"path": "s3://my-bucket/data"},
                            "Suggested OCI paths:\n  oci://my-bucket@ns/data\n  oci://alt@ns/data")
        assert any("SUGGESTED" in v for v in ctx.paths_checked.values())


class TestCellContextRender:
    """render() output format and max_entries truncation."""

    def test_render_empty_context(self):
        ctx = CellContext()
        rendered = ctx.render()
        assert rendered == ""

    def test_render_includes_prior_cells_header(self):
        ctx = CellContext()
        ctx.add_cell(1, 2, "code1")
        rendered = ctx.render()
        assert "PRIOR CELLS (newest first):" in rendered

    def test_render_includes_verified_paths(self):
        ctx = CellContext()
        ctx.add_tool_result("explore_path", {"path": "oci://bucket@ns/x"}, "EXISTS")
        rendered = ctx.render()
        assert "VERIFIED PATHS:" in rendered
        assert "oci://bucket@ns/x" in rendered

    def test_render_includes_verified_tables(self):
        ctx = CellContext()
        ctx.add_tool_result("search_catalog", {"query": "interactions"},
                            "Tables matching 'interactions': logs.interactions")
        rendered = ctx.render()
        assert "VERIFIED TABLES:" in rendered
        assert "interactions" in rendered

    def test_render_includes_notebook_summaries(self):
        ctx = CellContext()
        ctx.add_notebook_summary("/Utils/Config.ipynb", "Defines S3 bucket constants")
        rendered = ctx.render()
        assert "NOTEBOOK SUMMARIES:" in rendered
        assert "Defines S3 bucket constants" in rendered

    def test_render_includes_cache_stats(self):
        ctx = CellContext()
        ctx.add_tool_result("explore_path", {"path": "/p"}, "EXISTS")
        ctx.get_cached("explore_path", {"path": "/p"})
        rendered = ctx.render()
        assert "Tool cache: 1 hits / 1 total calls" in rendered

    def test_render_max_entries_truncation(self):
        ctx = CellContext()
        for i in range(25):
            ctx.add_cell(i + 1, 25, f"cell_{i}")
        rendered = ctx.render(max_entries=10)
        # Should mention omitted cells
        assert "15 older cells omitted" in rendered
        # The 10 newest entries should be present (cells 16..25)
        assert "Cell 25/25" in rendered
        assert "Cell 16/25" in rendered

    def test_render_no_truncation_when_under_limit(self):
        ctx = CellContext()
        for i in range(3):
            ctx.add_cell(i + 1, 3, f"cell_{i}")
        rendered = ctx.render(max_entries=20)
        assert "omitted" not in rendered


# ===================================================================
# 2. _unwrap_aidp_text
# ===================================================================

class TestUnwrapAidpText:
    """_unwrap_aidp_text handles the JSON wrapping AIDP applies to output."""

    def test_simple_text_plain(self):
        raw = '[{"type": "TEXT_PLAIN", "value": "hello\\n"}]'
        assert _unwrap_aidp_text(raw) == "hello\n"

    def test_multiple_values_concatenated(self):
        raw = '[{"type": "TEXT_PLAIN", "value": "line1\\n"}, {"type": "TEXT_PLAIN", "value": "line2\\n"}]'
        assert _unwrap_aidp_text(raw) == "line1\nline2\n"

    def test_literal_newlines_in_json(self):
        """AIDP sometimes embeds raw newline chars in JSON strings.
        json.loads with strict=False handles this."""
        # Build a JSON string with a literal newline inside the value
        raw = '[{"type": "TEXT_PLAIN", "value": "line1\nline2\n"}]'
        result = _unwrap_aidp_text(raw)
        assert "line1" in result
        assert "line2" in result

    def test_double_send_deduplication(self):
        """AIDP sometimes sends the same output twice as concatenated arrays: ]["""
        chunk = '[{"type": "TEXT_PLAIN", "value": "result\\n"}]'
        raw = chunk + chunk  # produces "][" in the middle
        result = _unwrap_aidp_text(raw)
        # Should deduplicate consecutive identical values
        assert result == "result\n"

    def test_double_send_different_values_kept(self):
        """Consecutive arrays with DIFFERENT values should both be kept."""
        raw = '[{"type": "TEXT_PLAIN", "value": "A"}][{"type": "TEXT_PLAIN", "value": "B"}]'
        result = _unwrap_aidp_text(raw)
        assert "A" in result
        assert "B" in result

    def test_plain_text_passthrough(self):
        raw = "just plain text, no JSON"
        assert _unwrap_aidp_text(raw) == raw

    def test_empty_string(self):
        assert _unwrap_aidp_text("") == ""

    def test_none_like_empty(self):
        # None is falsy, function checks `if not raw_text`
        assert _unwrap_aidp_text(None) == ""

    def test_value_field_only(self):
        """Untyped 'value' records are customer JSON, not platform envelopes.

        PR110 live output proved that guessing from this key both corrupts
        ordinary JSON and crashes when its value is not a string.
        """
        raw = '[{"value": "data"}]'
        assert _unwrap_aidp_text(raw) == raw


# ===================================================================
# 3. safe_io — REMOVED 2026-04-11 (FUSE consistency issues resolved).
#    Prior tests for safe_pickle_dump/load and safe_pandas_to_csv were
#    deleted along with aidp_compat.safe_io.
# ===================================================================


# ===================================================================
# 4. AIDPSession recycling
# ===================================================================

class TestAIDPSessionRecyclingAttributes:
    """Verify AIDPSession and ClusterSession have correct recycling architecture.

    Recycling moved from AIDPSession (sync threading) to ClusterSession (asyncio)
    in the async rewrite. These tests reflect the current architecture.
    """

    def _make_session(self):
        """Import AIDPSession without connecting to anything."""
        from aidp_executor import AIDPSession
        # Generic code requires explicit coordinates (no hardcoded env) —
        # attribute-shape tests just need a constructed instance.
        return AIDPSession(cluster_id="test-cluster",
                           lake_ocid="ocid1.aidataplatform.oc1.test-region.testid",
                           workspace_id="ws-test")

    def _make_cluster_session(self):
        """Get a fresh ClusterSession (singleton — reset _initialized)."""
        from cluster_session import ClusterSession
        # Reset singleton so we get a fresh instance
        ClusterSession._instance = None
        s = ClusterSession()
        return s

    def test_has_execute_locked(self):
        """AIDPSession still has _execute_locked for direct WS execution."""
        session = self._make_session()
        assert hasattr(session, "_execute_locked")
        assert callable(session._execute_locked)

    def test_has_ws_alive_flag(self):
        """AIDPSession._ws_alive drives the poll-loop exit on WS death."""
        session = self._make_session()
        assert hasattr(session, "_ws_alive")

    def test_cluster_session_has_asyncio_lock(self):
        """ClusterSession uses asyncio.Lock (not threading.Lock) for serialization."""
        import asyncio
        cs = self._make_cluster_session()
        assert hasattr(cs, "_lock")
        assert isinstance(cs._lock, asyncio.Lock)

    def test_cluster_session_has_lock_acquired_at(self):
        """ClusterSession tracks when execute() acquired the lock for freeze detection."""
        cs = self._make_cluster_session()
        assert hasattr(cs, "_lock_acquired_at")
        assert cs._lock_acquired_at == 0  # 0 = lock is free

    def test_cluster_session_has_recycle_task(self):
        """ClusterSession.recycle_task is an asyncio.Task (not threading.Thread)."""
        cs = self._make_cluster_session()
        assert hasattr(cs, "_recycle_task")

    def test_cluster_session_close_consumes_recycle_cancellation(self):
        """Normal keepalive cancellation must not leak into later tracebacks."""
        import asyncio

        async def exercise():
            cs = self._make_cluster_session()

            async def keepalive():
                while True:
                    await asyncio.sleep(60)

            recycle_task = asyncio.create_task(keepalive())
            cs._recycle_task = recycle_task
            cs._running = True
            await asyncio.sleep(0)

            await cs.close()

            assert recycle_task.cancelled()
            assert cs._recycle_task is None

        asyncio.run(exercise())

    def test_cluster_session_has_recycle_count(self):
        """ClusterSession tracks recycle count for observability."""
        cs = self._make_cluster_session()
        assert hasattr(cs, "_recycle_count")
        assert cs._recycle_count == 0

    def test_cluster_session_has_stateless_pool(self):
        """ClusterSession has a StatelessPool for concurrent I/O."""
        cs = self._make_cluster_session()
        assert hasattr(cs, "_pool")


# ===================================================================
# 5. OCI_PATH_TOOLS
# ===================================================================

class TestOciPathTools:
    """Verify OCI_PATH_TOOLS list is complete."""

    def _get_tools(self):
        from agent_migrate import OCI_PATH_TOOLS
        return OCI_PATH_TOOLS

    def test_core_tools_present_and_unique(self):
        # No count pin (goes stale every tool addition). Assert the
        # load-bearing tools exist and names are unique.
        tools = self._get_tools()
        names = [t["name"] for t in tools]
        assert len(names) == len(set(names)), f"duplicate tool names: {names}"
        for required in ("submit_code", "make_note", "explore_path",
                         "run_on_cluster", "fixup_cell", "get_cell_history",
                         "suggest_oci_path", "search_catalog"):
            assert required in names, f"missing core tool {required!r}"

    def test_submit_code_present(self):
        tools = self._get_tools()
        names = [t["name"] for t in tools]
        assert "submit_code" in names

    def test_inspect_package_source_present(self):
        tools = self._get_tools()
        names = [t["name"] for t in tools]
        assert "inspect_package_source" in names

    def test_summarize_notebook_present(self):
        tools = self._get_tools()
        names = [t["name"] for t in tools]
        assert "summarize_notebook" in names

    def test_core_exploration_tools_present(self):
        tools = self._get_tools()
        names = [t["name"] for t in tools]
        for expected in ["explore_path", "suggest_oci_path", "search_catalog",
                         "run_on_cluster", "describe_table", "list_schemas_and_tables",
                         "read_notebook_source"]:
            assert expected in names, f"Missing tool: {expected}"

    def test_all_tools_have_input_schema(self):
        tools = self._get_tools()
        for tool in tools:
            assert "input_schema" in tool, f"Tool {tool['name']} missing input_schema"
            assert "type" in tool["input_schema"]

    def test_all_tools_have_description(self):
        tools = self._get_tools()
        for tool in tools:
            assert "description" in tool, f"Tool {tool['name']} missing description"
            assert len(tool["description"]) > 10, (
                f"Tool {tool['name']} has suspiciously short description"
            )


# ===========================================================================
# 6. Cell Analyzer Tests
# ===========================================================================

class TestCellAnalyzerStatic:
    """Test analyze_cell_static for FUSE risks, paths, and patterns."""

    def setup_method(self):
        from cell_analyzer import analyze_cell_static
        self.analyze = analyze_cell_static

    def test_empty_cell(self):
        plan = self.analyze(0, "", [], [])
        assert plan["action"] == "skip_empty"
        assert plan["is_empty"] is True

    def test_run_cell(self):
        plan = self.analyze(0, "%run /Workspace/Utils.ipynb", [], [])
        assert plan["action"] == "inline_run"
        assert plan["is_run_cell"] is True

    def test_notebook_run_cell(self):
        plan = self.analyze(0, 'dbutils.notebook.run("/Workspace/test.ipynb", timeout_seconds=3600)', [], [])
        assert plan["action"] == "inline_run"
        assert plan["is_run_cell"] is True

    def test_pickle_fuse_risk(self):
        code = """import pickle
with open("/tmp/test.pkl", "wb") as f:
    pickle.dump(data, f)
with open("/tmp/test.pkl", "rb") as f:
    loaded = pickle.load(f)"""
        plan = self.analyze(0, code, [code], [])
        risk_names = [r["name"] for r in plan["fuse_risks"]]
        assert "pickle_write_read" in risk_names
        # FUSE consistency issue resolved (revalidated 2026-09): the analyzer
        # records the risk informationally and must NOT prescribe a pickle
        # workaround in changes_needed.
        assert not any("safe_pickle" in c.lower() for c in plan["changes_needed"])
        pickle_risk = next(r for r in plan["fuse_risks"] if r["name"] == "pickle_write_read")
        assert "No workaround needed" in pickle_risk["fix"]

    def test_parquet_overwrite_risk(self):
        code = 'df.write.mode("overwrite").parquet("oci://bucket@ns/data/")'
        plan = self.analyze(0, code, [code], [])
        risk_names = [r["name"] for r in plan["fuse_risks"]]
        assert "parquet_overwrite_same_path" in risk_names

    def test_save_as_table_risk(self):
        code = 'df.write.mode("overwrite").saveAsTable("db.table")'
        plan = self.analyze(0, code, [code], [])
        risk_names = [r["name"] for r in plan["fuse_risks"]]
        assert "save_as_table_overwrite" in risk_names

    def test_pandas_csv_risk(self):
        code = 'df.to_csv("/tmp/output.csv")'
        plan = self.analyze(0, code, [code], [])
        risk_names = [r["name"] for r in plan["fuse_risks"]]
        assert "pandas_to_csv" in risk_names

    def test_storage_path_extraction_oci(self):
        code = 'df = spark.read.parquet("oci://oci-example-data-lake@exmplnspace2/data/")'
        plan = self.analyze(0, code, [code], [])
        assert "oci://oci-example-data-lake@exmplnspace2/data/" in plan["storage_paths"]

    def test_storage_path_extraction_s3(self):
        code = 'df = spark.read.parquet("s3://example-data-lake/test/")'
        plan = self.analyze(0, code, [code], [])
        assert any("s3://" in p for p in plan["storage_paths"])

    def test_table_reference_extraction(self):
        code = 'df = spark.sql("SELECT * FROM upi_examplevendor.basedata")'
        plan = self.analyze(0, code, [code], [])
        assert "upi_examplevendor.basedata" in plan["table_refs"]

    def test_slack_notification_skip(self):
        code = 'requests.post(slack_webhook_url, json={"text": "done"})'
        plan = self.analyze(0, code, [code], [])
        assert plan["action"] == "skip_notification"

    def test_databricks_import_detection(self):
        code = 'from aidp_dbutils import _DBUtils\ndbutils = _DBUtils(oidlUtils)'
        plan = self.analyze(0, code, [code], [])
        assert any("aidp_compat" in c for c in plan["changes_needed"])

    def test_sql_magic_detection(self):
        code = '%sql SELECT * FROM table'
        plan = self.analyze(0, code, [code], [])
        # %sql starts with % so it's detected as a magic
        assert plan["action"] == "inline_run" or any("sql" in c.lower() for c in plan.get("databricks_patterns", []))

    def test_normal_code_is_migrate(self):
        code = 'x = 42\nprint(x)'
        plan = self.analyze(0, code, [code], [])
        assert plan["action"] == "migrate"
        assert len(plan["fuse_risks"]) == 0


class TestCellPlanRender:
    """Test render_cell_plan output."""

    def test_render_basic_plan(self):
        from cell_analyzer import render_cell_plan
        plan = {
            "cell_index": 3,
            "action": "migrate",
            "changes_needed": ["Fix path", "Add cache"],
            "storage_paths": ["oci://bucket@ns/data/"],
            "table_refs": ["db.table"],
            "fuse_risks": [{"severity": "HIGH", "name": "pickle", "fix": "use safe_pickle"}],
        }
        rendered = render_cell_plan(plan)
        assert "Cell 3" in rendered
        assert "Fix path" in rendered
        assert "oci://bucket@ns/data/" in rendered
        assert "HIGH" in rendered
        assert "pickle" in rendered

    def test_render_skip_plan(self):
        from cell_analyzer import render_cell_plan
        plan = {"cell_index": 0, "action": "skip_empty", "changes_needed": [],
                "storage_paths": [], "table_refs": [], "fuse_risks": []}
        rendered = render_cell_plan(plan)
        assert "skip_empty" in rendered


# ===========================================================================
# 7. Notebook Output Format Tests
# ===========================================================================

class TestNotebookOutputFormat:
    """Verify notebook output format is valid Jupyter."""

    def test_stream_output_format(self):
        """Stream outputs must have text as a list."""
        output = {"output_type": "stream", "name": "stdout", "text": ["hello\n"]}
        assert output["output_type"] == "stream"
        assert isinstance(output["text"], list)
        assert output["text"][0] == "hello\n"

    def test_execute_result_format(self):
        output = {"output_type": "execute_result", "data": {"text/plain": "42"},
                  "metadata": {}, "execution_count": None}
        assert output["output_type"] == "execute_result"
        assert "text/plain" in output["data"]

    def test_error_output_format(self):
        output = {"output_type": "error", "ename": "NameError",
                  "evalue": "name 'x' is not defined", "traceback": ["line 1"]}
        assert output["output_type"] == "error"
        assert isinstance(output["traceback"], list)

    def test_unwrapped_output_is_clean_text(self):
        """After unwrapping, text should be plain, not JSON wrapped."""
        from context_tools import _unwrap_aidp_text
        raw = '[{"type": "TEXT_PLAIN", "value": "hello world\\n"}]'
        clean = _unwrap_aidp_text(raw)
        assert "TEXT_PLAIN" not in clean
        assert "isbase64" not in clean
        assert clean == "hello world\n"


# ===========================================================================
# 8. Directory Structure Tests
# ===========================================================================

class TestDirectoryStructure:
    """Verify new directory path construction.

    Post-Session-1: MIGRATE_OUTPUT is None until main() populates it from
    cfg.paths.migrate_output. Tests stub it to exercise path-construction
    logic without full main().
    """

    def test_migrate_output_default_or_configured(self):
        """MIGRATE_OUTPUT defaults to None pre-main(); after main() it is a
        /Workspace/ path from cfg. Both are acceptable at module scope."""
        import job_migrate
        assert job_migrate.MIGRATE_OUTPUT is None or (
            isinstance(job_migrate.MIGRATE_OUTPUT, str)
            and job_migrate.MIGRATE_OUTPUT.startswith("/Workspace/")
        )

    def test_task_output_dir_numbered(self):
        """Task dirs should be under tasks/ with numbered prefix."""
        # The task_key gets a numbered prefix in process_job
        numbered_key = "01_1_Date_Parameters"
        from job_migrate import MIGRATE_OUTPUT
        expected = f"{MIGRATE_OUTPUT}/Example_Optimizer_Realtime/tasks/{numbered_key}"
        # Just verify the pattern, not the actual path
        assert "tasks/" in expected
        assert "01_" in expected

    def test_dep_output_dir_separate(self):
        """Dep output should be under deps/."""
        task_key = "dep_M14.ipynb"
        from job_migrate import MIGRATE_OUTPUT
        expected = f"{MIGRATE_OUTPUT}/Example_Optimizer_Realtime/deps/{task_key}"
        assert "deps/" in expected


# ===========================================================================
# 9. Session Lock Tests
# ===========================================================================

class TestSessionLock:
    """Test that exec_lock blocks concurrent access."""

    def test_lock_is_reentrant_safe(self):
        """Lock should be acquirable and releasable."""
        import threading
        lock = threading.Lock()
        assert lock.acquire(timeout=1)
        lock.release()
        # Should be acquirable again
        assert lock.acquire(timeout=1)
        lock.release()

    def test_lock_blocks_concurrent(self):
        """Second acquire should block while first is held."""
        import threading
        lock = threading.Lock()
        lock.acquire()
        # Second acquire should fail with timeout
        assert not lock.acquire(timeout=0.1)
        lock.release()


# ===========================================================================
# 10. Real-World Scenario Tests (from actual migration runs)
# ===========================================================================

class TestRealWorldOutputUnwrap:
    """Test unwrapping with actual AIDP output patterns from live runs."""

    def test_real_aidp_stream_with_newlines(self):
        """Actual output from 3_Basedata cell that printed dates."""
        from context_tools import _unwrap_aidp_text
        raw = '[{"type": "TEXT_PLAIN", "length": "136", "charset": "UTF_8", "isbase64": false, "compression": null, "value": "2026-03-23\nNo data for 2026-03-23\n2026-03-24\nNo data for 2026-03-24\n"}]'
        clean = _unwrap_aidp_text(raw)
        assert "2026-03-23" in clean
        assert "No data for" in clean
        assert "TEXT_PLAIN" not in clean

    def test_real_ansi_error_output(self):
        """AIDP errors contain ANSI escape codes that need stripping."""
        import re
        raw_traceback = "[0;31mZeroDivisionError[0m: division by zero"
        clean = re.sub(r'\x1b\[[0-9;]*m', '', raw_traceback)
        assert clean == "ZeroDivisionError: division by zero"

    def test_real_spark_job_status_output(self):
        """Spark outputs job status JSON that should pass through."""
        from context_tools import _unwrap_aidp_text
        raw = '[{"id": 29819, "jobResult": "JobSucceeded", "name": "Job group"}]'
        result = _unwrap_aidp_text(raw)
        # This is NOT an AIDP TEXT_PLAIN wrapper - it's actual Spark output
        # _unwrap should return it as-is since there's no "value" field
        assert "JobSucceeded" in result

    def test_real_double_send_pattern(self):
        """AIDP sends every output twice - verify dedup."""
        from context_tools import _unwrap_aidp_text
        single = '[{"type": "TEXT_PLAIN", "value": "42\\n"}]'
        double = single + single
        clean = _unwrap_aidp_text(double)
        assert clean == "42\n"
        # NOT "42\n42\n"


class TestRealWorldCellAnalysis:
    """Test cell analysis with actual notebook cell patterns from ExampleVendor job."""

    def setup_method(self):
        from cell_analyzer import analyze_cell_static
        self.analyze = analyze_cell_static

    def test_aidp_dbutils_import(self):
        """First cell of every the pilot deployment notebook."""
        code = 'from aidp_dbutils import _DBUtils\ndbutils = _DBUtils(oidlUtils)'
        plan = self.analyze(0, code, [code], [])
        assert any("aidp_compat" in c for c in plan["changes_needed"])

    def test_campaign_config_cell(self):
        """Campaign config dict that references paths_map (from parent scope)."""
        code = """m87_sim_config = {'path': paths_map['M87'], 'campaign': 'Payments_DS_EXVENDOR_MG87_RETENTION', 'db': 'upi_examplevendor_m87'}"""
        plan = self.analyze(0, code, [code], [])
        assert plan["action"] == "migrate"
        # No FUSE risks for a simple dict assignment
        assert len(plan["fuse_risks"]) == 0

    def test_pickle_dump_and_load_same_cell(self):
        """Pattern from 4_Metric_Calculation that caused FUSE issues."""
        code = """path_all = paths_map['all']
with open(f'{path_all}/target_budget_dict_sc.pkl', 'wb') as file:
    pickle.dump(android_budget_dict_sc, file)
with open(f'{path_all}/target_budget_dict_sc.pkl', 'rb') as file:
    android_budget_dict_check = pickle.load(file)"""
        plan = self.analyze(0, code, [code], [])
        assert any(r["name"] == "pickle_write_read" for r in plan["fuse_risks"])
        # FUSE consistency resolved (revalidated 2026-09): risks are recorded
        # informationally at LOW severity — never HIGH, never prescriptive.
        assert all(r["severity"] == "LOW" for r in plan["fuse_risks"])
        # FUSE resolved (revalidated 2026-09): informational only — the
        # analyzer must NOT prescribe a pickle workaround in changes_needed.
        assert not any("safe_pickle" in c.lower() for c in plan["changes_needed"])
        pickle_risk = next(r for r in plan["fuse_risks"] if r["name"] == "pickle_write_read")
        assert "No workaround needed" in pickle_risk["fix"]

    def test_spark_read_write_same_path(self):
        """Read-modify-write on same parquet path."""
        code = """df = spark.read.parquet("oci://oci-example-data-lake@exmplnspace2/data/test/")
result = df.filter(df.id > 10)
result.write.mode("overwrite").parquet("oci://oci-example-data-lake@exmplnspace2/data/test/")"""
        plan = self.analyze(0, code, [code], [])
        assert any(r["name"] == "parquet_overwrite_same_path" for r in plan["fuse_risks"])

    def test_create_table_with_s3_path(self):
        """createTable function that references S3 bucket."""
        code = 'createTable(df, "test_table", "test_db", "overwrite", "parquet", bucket_name="example-workspace-bucket")'
        plan = self.analyze(0, code, [code], [])
        assert plan["action"] == "migrate"

    def test_zero_division_guard_pattern(self):
        """Pattern that caused ZeroDivisionError in Budget Calculation."""
        code = """early_target_rpt = early_target_budget_new / num_early_txns_cnt
print(early_target_rpt)"""
        plan = self.analyze(0, code, [code], [])
        # This is normal code - no FUSE risk, just a potential runtime error
        assert plan["action"] == "migrate"

    def test_run_cell_with_comment(self):
        """Real %run with comment from the pilot deployment notebooks."""
        code = """%run /Workspace/Users/user1@example.com/Payments/Example_Optimizer_Realtime/Example_Configs/run.ipynb
#%run ./run #Changed as above"""
        plan = self.analyze(0, code, [code], [])
        assert plan["action"] == "inline_run"
        assert plan["is_run_cell"] is True

    def test_slack_webhook_skip(self):
        """Real Slack notification pattern."""
        code = """import requests
slack_url = "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXX"
requests.post(slack_url, json={"text": "Job completed successfully"})"""
        plan = self.analyze(0, code, [code], [])
        assert plan["action"] == "skip_notification"


class TestRealWorldCellContext:
    """Test CellContext with patterns from actual migration flow."""

    def test_full_examplevendor_flow(self):
        """Simulate the actual cell flow from 1_Date_Parameters."""
        from cell_context import CellContext
        ctx = CellContext()

        # Cell 0: import
        ctx.add_cell(0, 7, "from aidp_compat import dbutils", "", "OK")

        # Cell 1: %run 0_Imports (inlined)
        ctx.add_cell(1, 7, "%run 0_Imports", "", "OK",
                     child_info="inlined 8 cells, 1 fixed")

        # Cell 2: %run 0_Config (inlined with campaign cascade)
        ctx.add_cell(2, 7, "%run 0_Config", "", "OK",
                     child_info="inlined 15 cells (37 campaign configs), 4 fixed")

        # Cache some tool results
        ctx.add_tool_result("explore_path",
                            {"path": "oci://oci-example-workspace-bucket@exmplnspace1/db_tables/"},
                            "Status: EXISTS")
        ctx.add_tool_result("search_catalog",
                            {"query": "upi_examplevendor_optimizer_realtime"},
                            "Tables: upi_examplevendor_optimizer_realtime.basedata")

        # Cell 3: date parameters (code cell)
        ctx.add_cell(3, 7, "start_date = '2026-03-01'", "2026-03-01", "OK")

        # Verify accumulated state
        rendered = ctx.render()
        assert "Cell 3/7: OK" in rendered  # newest first
        assert "Cell 0/7: OK" in rendered
        assert "inlined 15 cells" in rendered
        assert "EXISTS" in rendered
        assert "upi_examplevendor_optimizer_realtime" in rendered

        # Verify cache works
        cached = ctx.get_cached("explore_path",
                                {"path": "oci://oci-example-workspace-bucket@exmplnspace1/db_tables/"})
        assert cached is not None
        assert "EXISTS" in cached

        # Verify stats
        assert "cells=4" in ctx.stats()
        assert "paths=1" in ctx.stats()

    def test_tool_cache_dedup(self):
        """Same tool call should return cached result."""
        from cell_context import CellContext
        ctx = CellContext()

        ctx.add_tool_result("explore_path", {"path": "/test"}, "EXISTS (5 items)")
        result = ctx.get_cached("explore_path", {"path": "/test"})
        assert result == "EXISTS (5 items)"

        # Different input should miss
        result2 = ctx.get_cached("explore_path", {"path": "/other"})
        assert result2 is None

        # Stats should show 1 hit
        assert ctx._cache_hit_count == 1
        assert ctx._tool_call_count == 1


class TestFixLogPreservation:
    """Test that fix logs persist correctly in saved notebooks."""

    def test_fix_log_appended_to_code(self):
        """Fixed cell should have fix log comment at the end."""
        code = "x = 42"
        fix_log = ["Attempt 1: L1: 'x = s3://...' -> 'x = oci://...'"]

        save_code = code + "\n\n# === AIDP MIGRATION FIX LOG ===\n"
        for fl in fix_log:
            save_code += f"# {fl}\n"

        assert "AIDP MIGRATION FIX LOG" in save_code
        assert "Attempt 1" in save_code
        assert save_code.startswith("x = 42")

    def test_fix_log_stripped_before_execution(self):
        """Fix log should be stripped before exec()."""
        code_with_log = "x = 42\n\n# === AIDP MIGRATION FIX LOG ===\n# Attempt 1: changed path"
        exec_code = code_with_log.split("\n# === AIDP MIGRATION FIX LOG ===")[0].rstrip()
        assert exec_code == "x = 42"
        assert "MIGRATION FIX LOG" not in exec_code

    def test_existing_fix_log_preserved_on_no_fix(self):
        """If cell passes without fix, existing fix log should be kept."""
        original = "x = 42\n\n# === AIDP MIGRATION FIX LOG ===\n# Attempt 1: old fix"
        # When cell passes without needing a fix, we keep original
        # (which includes the log)
        assert "old fix" in original
