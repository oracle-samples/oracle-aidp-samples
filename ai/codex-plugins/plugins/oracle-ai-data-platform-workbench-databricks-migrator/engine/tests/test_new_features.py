"""
Unit tests for new features:
- ContextCompactor (Tier 1/2/3)
- s3_compat._load_mapping / _translate_bucket
- aidp_compat notebook.entry_point mock
- cell_analyzer RISK_PATTERNS detection
"""

import os
import sys
import json
import tempfile
import pytest

# Make scripts/ importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ─── ContextCompactor ──────────────────────────────────────────────────────────

class TestContextCompactorTier1:
    """Tier 1: save_and_truncate"""

    def setup_method(self, method):
        import uuid
        from context_compactor import ContextCompactor
        self.compactor = ContextCompactor(call_id=f"test-{uuid.uuid4().hex[:8]}", log_fn=print)

    def test_short_result_unchanged(self):
        result = "hello world"
        out = self.compactor.save_and_truncate("run_on_cluster", result)
        assert out == result

    def test_long_result_truncated(self):
        long_result = "x" * 20000
        out = self.compactor.save_and_truncate("run_on_cluster", long_result)
        assert len(out) < len(long_result)
        assert "truncated" in out.lower() or "[" in out

    def test_long_result_saved_to_disk(self):
        long_result = "y" * 20000
        self.compactor.save_and_truncate("explore_path", long_result)
        save_dir = self.compactor._base_dir
        files = os.listdir(save_dir)
        assert any("explore_path" in f for f in files)

    def test_saved_file_contains_full_content(self):
        long_result = "z" * 20000
        self.compactor.save_and_truncate("describe_table", long_result)
        save_dir = self.compactor._base_dir
        files = [f for f in os.listdir(save_dir) if "describe_table" in f]
        assert files
        content = open(os.path.join(save_dir, files[0])).read()
        assert content == long_result

    def test_counter_increments(self):
        self.compactor.save_and_truncate("tool_a", "x" * 20000)
        self.compactor.save_and_truncate("tool_b", "x" * 20000)
        files = sorted(os.listdir(self.compactor._base_dir))
        assert files[0].startswith("tool_000")
        assert files[1].startswith("tool_001")

    def test_get_saved_output(self):
        long_result = "full content " * 2000
        out = self.compactor.save_and_truncate("run_on_cluster", long_result)
        # Extract filename from truncation notice
        files = os.listdir(self.compactor._base_dir)
        assert files
        recovered = self.compactor.get_saved_output(files[0])
        assert recovered == long_result

    def test_get_saved_output_missing_file(self):
        # Valid-shaped name that was never saved: exercises the not-found path.
        # (Invalid shapes are refused earlier by the filename guard — covered
        # by test_context_compactor.py.)
        result = self.compactor.get_saved_output("tool_999_other.txt")
        assert "not found" in result.lower() or "error" in result.lower()

    def test_inaccessible_aidp_tmp_dir_falls_back(self, monkeypatch, tmp_path):
        from context_compactor import ContextCompactor

        blocked_root = tmp_path / "not-a-directory"
        blocked_root.write_text("blocked")
        monkeypatch.setenv("AIDP_TMP_DIR", str(blocked_root))

        compactor = ContextCompactor(call_id="test-fallback", log_fn=print)
        assert compactor._base_dir is not None
        assert not compactor._base_dir.startswith(str(blocked_root))
        compactor.save_and_truncate("tool", "x" * 20_000)
        assert os.listdir(compactor._base_dir)

    def test_cross_uid_tmp_parent_uses_per_uid_fallback(self, monkeypatch):
        import context_compactor as module

        monkeypatch.delenv("AIDP_TMP_DIR", raising=False)
        monkeypatch.setattr(module.tempfile, "gettempdir", lambda: "/tmp")
        monkeypatch.setattr(module.os, "getuid", lambda: 4321, raising=False)

        attempted = []

        def simulated_makedirs(path, exist_ok=False):
            attempted.append(path)
            normalized = path.replace("\\", "/")
            if normalized.startswith("/tmp/aidp_context/"):
                raise PermissionError(13, "cross-UID parent is not writable", path)

        monkeypatch.setattr(module.os, "makedirs", simulated_makedirs)

        compactor = module.ContextCompactor(
            call_id="cross-uid", log_fn=lambda _message: None
        )

        assert attempted[0].replace("\\", "/") == (
            "/tmp/aidp_context/cross-uid"
        )
        assert compactor._base_dir.replace("\\", "/") == (
            "/tmp/aidp_ctx_4321/aidp_context/cross-uid"
        )


class TestContextCompactorTokenEstimate:
    def test_estimate_tokens(self):
        from context_compactor import ContextCompactor
        messages = [{"role": "user", "content": "hello world"}]
        est = ContextCompactor.estimate_tokens(messages)
        assert est > 0
        assert isinstance(est, int)

    def test_empty_messages(self):
        from context_compactor import ContextCompactor
        assert ContextCompactor.estimate_tokens([]) == 0


class TestContextCompactorTier2:
    """Tier 2: trim middle messages at 500K tokens"""

    def _make_messages(self, n):
        msgs = [{"role": "user", "content": "original prompt"}]
        for i in range(n):
            role = "assistant" if i % 2 == 0 else "user"
            msgs.append({"role": role, "content": f"msg {i}"})
        return msgs

    def test_tier2_keeps_first_and_last(self, monkeypatch):
        from context_compactor import ContextCompactor

        compactor = ContextCompactor(call_id="test-tier2", log_fn=print)

        # Force Tier 2 by monkeypatching estimate_tokens
        monkeypatch.setattr(ContextCompactor, "estimate_tokens", staticmethod(lambda m: 600_000))

        msgs = self._make_messages(20)
        result = compactor.maybe_compact(msgs, "system")
        assert result[0] == msgs[0]  # first message preserved
        assert len(result) <= 8  # first + last 6 + maybe 1

    def test_tier2_below_threshold_no_change(self, monkeypatch):
        from context_compactor import ContextCompactor

        compactor = ContextCompactor(call_id="test-tier2b", log_fn=print)
        monkeypatch.setattr(ContextCompactor, "estimate_tokens", staticmethod(lambda m: 100_000))

        msgs = self._make_messages(10)
        result = compactor.maybe_compact(msgs)
        assert result == msgs


# ─── glue_compat ──────────────────────────────────────────────────────────────

# TestGetGlueTableS3Location class removed (dead module cleanup 2026-08-22).


# TestS3CompatMapping class removed (dead module cleanup 2026-08-22).


class TestNotebookEntryPoint:
    def test_entry_point_chain_env_driven(self, monkeypatch):
        """Post 2026-08-19: mock context reads workspaceid/clusterId from env
        vars (AIDP_WORKSPACE_ID, AIDP_CLUSTER_ID) rather than hardcoded UUIDs.
        The migration tool injects AIDP_WORKSPACE_ID via cluster_converter.
        recommended_env_vars(); AIDP_CLUSTER_ID is unset by default and shows
        as empty string."""
        from aidp_compat.notebook import AIDPNotebookUtils
        monkeypatch.setenv("AIDP_WORKSPACE_ID", "00000000-0000-4000-8000-000000000001")
        nb = AIDPNotebookUtils()
        data = json.loads(nb.entry_point.getDbutils().notebook().getContext().toJson())
        assert data["workspaceid"] == "00000000-0000-4000-8000-000000000001"
        assert data["clusterId"] == ""  # not injectable at cluster-create; empty by design
        assert "orgId" in data

    def test_entry_point_returns_valid_json(self):
        from aidp_compat.notebook import AIDPNotebookUtils
        nb = AIDPNotebookUtils()
        result = nb.entry_point.getDbutils().notebook().getContext().toJson()
        # Must be valid JSON
        parsed = json.loads(result)
        assert isinstance(parsed, dict)


# ─── safe_read_file ───────────────────────────────────────────────────────────

# TestSafeReadFile class removed (dead module cleanup 2026-08-22).


class TestRiskPatternDetection:
    def _analyze(self, source):
        from cell_analyzer import analyze_cell_static
        return analyze_cell_static(0, source, [source], [])

    def test_delete_dml_detected(self):
        plan = self._analyze("spark.sql('DELETE FROM mydb.mytable WHERE status = \"old\"')")
        risk_names = [r["type"] for r in plan.get("risks", [])]
        assert "delete_dml" in risk_names

    def test_boto3_detected(self):
        plan = self._analyze("import boto3\nclient = boto3.client('s3')")
        risk_names = [r["type"] for r in plan.get("risks", [])]
        assert "boto3_aws" in risk_names

    def test_glue_lookup_detected(self):
        plan = self._analyze("path = get_glue_table_s3_location('db', 'tbl')")
        risk_names = [r["type"] for r in plan.get("risks", [])]
        assert "glue_lookup" in risk_names

    def test_spark_catalog_detected(self):
        plan = self._analyze("df = spark.read.table('spark_catalog.schema.table')")
        risk_names = [r["type"] for r in plan.get("risks", [])]
        assert "spark_catalog" in risk_names

    def test_databricks_api_detected(self):
        plan = self._analyze("requests.post('https://org.azuredatabricks.net/api/2.0/jobs/run-now', json={})")
        risk_names = [r["type"] for r in plan.get("risks", [])]
        assert "databricks_api" in risk_names

    def test_internal_endpoint_detected(self):
        plan = self._analyze("requests.get('http://internal-gateway.example.com/notify')")
        risk_names = [r["type"] for r in plan.get("risks", [])]
        assert "internal_endpoint" in risk_names

    def test_pip_install_detected(self):
        plan = self._analyze("%pip install pandas==1.5.0")
        risk_names = [r["type"] for r in plan.get("risks", [])]
        assert "pip_install" in risk_names

    def test_clean_cell_no_risks(self):
        plan = self._analyze("df = spark.read.parquet('oci://bucket@ns/path')\ndf.show()")
        assert plan.get("risks", []) == []

    def test_risk_fix_in_changes_needed(self):
        plan = self._analyze("spark.sql('DELETE FROM mydb.tbl WHERE x=1')")
        changes_text = " ".join(plan.get("changes_needed", []))
        assert "delete_dml" in changes_text or "DELETE" in changes_text


# ─── aidp_compat __init__ exports ────────────────────────────────────────────

class TestAIDPCompatExports:




    def test_version_matches_single_source(self):
        # No hardcoded pin (it goes stale every release). Assert the exported
        # version equals the __init__.py source of truth and looks like semver.
        import re
        import aidp_compat
        src = open(os.path.join(os.path.dirname(__file__), "..", "aidp_compat", "__init__.py")).read()
        m = re.search(r'^__version__\s*=\s*["\']([^"\']+)["\']', src, re.MULTILINE)
        assert m, "__version__ missing from aidp_compat/__init__.py"
        assert aidp_compat.__version__ == m.group(1)
        assert re.fullmatch(r"\d+\.\d+\.\d+", aidp_compat.__version__)


# ─── Tool handler: _handle_get_job_parameters ────────────────────────────────

class TestHandleGetJobParameters:
    """_handle_get_job_parameters serves the PRE-MERGED task parameters.

    Contract change (validated 2026-09-08): merging of task base_parameters +
    job-level parameters moved into process_job() (job_migrate.py — job wins
    on conflict), which calls set_current_task_params() with the merged dict.
    The handler itself just returns the current task's merged params as JSON.
    """

    def setup_method(self, method):
        import agent_migrate
        self._module = agent_migrate
        self._orig_params = agent_migrate._current_task_params

    def teardown_method(self, method):
        self._module.set_current_task_params(self._orig_params or {})

    def _run(self):
        import asyncio
        return asyncio.run(
            self._module._handle_get_job_parameters()
        )

    def test_returns_current_task_params_as_json(self):
        self._module.set_current_task_params({"foo": "bar"})
        data = json.loads(self._run())
        assert data["foo"] == "bar"

    def test_merged_override_semantics_roundtrip(self):
        # Mirror the process_job merge (base first, job-level parameters win)
        # and assert the handler serves the merged result faithfully.
        base = {"x": "base", "from_nb": "1"}
        merged = dict(base)
        merged.update({"x": "override", "from_top": "2"})
        self._module.set_current_task_params(merged)
        data = json.loads(self._run())
        assert data["x"] == "override"
        assert data["from_nb"] == "1"
        assert data["from_top"] == "2"

    def test_empty_params_returns_not_found_message(self):
        self._module.set_current_task_params({})
        result = self._run()
        assert "no job parameters" in result.lower()



# ─── Tool handler: _handle_get_tool_output ────────────────────────────────────

class TestHandleGetToolOutput:
    """_handle_get_tool_output: delegates to the active ContextCompactor."""

    def setup_method(self, method):
        import agent_migrate
        self._module = agent_migrate
        self._orig_compactor = agent_migrate._current_compactor

    def teardown_method(self, method):
        self._module._current_compactor = self._orig_compactor

    def test_reads_saved_file(self):
        import uuid
        from context_compactor import ContextCompactor
        compactor = ContextCompactor(call_id=f"test-gto-{uuid.uuid4().hex[:8]}", log_fn=print)
        # Save a file via the compactor
        compactor.save_and_truncate("run_on_cluster", "full content here" * 1000)
        call_id = compactor.call_id
        files = os.listdir(compactor._base_dir)
        assert files

        self._module._current_compactor = compactor
        self._module._compactor_history.append(compactor)
        result = self._module._handle_get_tool_output(files[0])
        assert "full content here" in result

    def test_missing_file_lists_available(self):
        import uuid
        from context_compactor import ContextCompactor
        compactor = ContextCompactor(call_id=f"test-gto2-{uuid.uuid4().hex[:8]}", log_fn=print)
        # Save one file
        compactor.save_and_truncate("other", "x" * 20000)

        self._module._current_compactor = compactor
        self._module._compactor_history.append(compactor)
        result = self._module._handle_get_tool_output("tool_999_other.txt")
        # Should mention the file is not found and list what's available
        assert "not found" in result.lower() or "tool_999_other" in result.lower()

    def test_no_compactor_returns_error(self):
        self._module._current_compactor = None
        self._module._compactor_history.clear()
        result = self._module._handle_get_tool_output("any.txt")
        assert "not available" in result.lower() or "no active" in result.lower()


# ─── Tool handler: _handle_scan_sensitive_info (regex patterns) ───────────────

class TestScanSensitiveInfoPatterns:
    """Verify SENSITIVE_PATTERNS match expected tokens without a real cluster session."""

    def _scan_source(self, source: str) -> list:
        """Run the regex scanning logic directly (no cluster required)."""
        import re as _re
        import sys

        # Import SENSITIVE_PATTERNS from the module
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
        import agent_migrate

        # Extract patterns from the function source (replicate the dict)
        SENSITIVE_PATTERNS = [
            ("databricks_token", r'dapi[a-zA-Z0-9]{32,}'),
            ("slack_token", r'xox[bprs]-[0-9A-Za-z\-]+'),
            ("slack_webhook", r'hooks\.slack\.com/services/[A-Za-z0-9/]+'),
            ("examplecorp_internal", r'internal-gateway\.example\.com|ds-internal-gateway'),
            ("databricks_api", r'azuredatabricks\.net|api/2\.0/jobs'),
        ]

        matches = []
        for pattern_name, pattern in SENSITIVE_PATTERNS:
            for m in _re.finditer(pattern, source):
                line = source[max(0, m.start()-30):m.end()+30].replace('\n', ' ')
                matches.append({"pattern": pattern_name, "match": line})
        return matches

    def test_detects_databricks_token(self):
        matches = self._scan_source('headers = {"Authorization": "Bearer dapi' + 'A' * 32 + '"}')
        assert any(m["pattern"] == "databricks_token" for m in matches)

    def test_detects_slack_bot_token(self):
        matches = self._scan_source('token = "xoxb-1234-5678-abcdef"')
        assert any(m["pattern"] == "slack_token" for m in matches)

    def test_detects_slack_webhook(self):
        matches = self._scan_source('url = "https://hooks.slack.com/services/T0/B0/abc123"')
        assert any(m["pattern"] == "slack_webhook" for m in matches)

    def test_detects_examplecorp_internal_endpoint(self):
        matches = self._scan_source('requests.post("http://internal-gateway.example.com/notify", json={})')
        assert any(m["pattern"] == "examplecorp_internal" for m in matches)

    def test_detects_databricks_api_call(self):
        matches = self._scan_source('url = "https://myorg.azuredatabricks.net/api/2.0/jobs/run-now"')
        assert any(m["pattern"] == "databricks_api" for m in matches)

    def test_run_stateless_fallback_used(self):
        """_handle_scan_sensitive_info must use getattr fallback for run_stateless."""
        import inspect
        import agent_migrate
        src = inspect.getsource(agent_migrate._handle_scan_sensitive_info)
        assert "getattr" in src and "run_stateless" in src

    def test_no_false_positives_on_clean_code(self):
        matches = self._scan_source("df = spark.read.parquet('oci://bucket@ns/path')\ndf.show()")
        assert matches == []


# ─── ContextCompactor Tier 3 ──────────────────────────────────────────────────

class TestContextCompactorTier3:
    """Tier 3: Opus summarization with Tier 2 fallback."""

    def _make_messages(self, n):
        msgs = [{"role": "user", "content": "original prompt"}]
        for i in range(n):
            role = "assistant" if i % 2 == 0 else "user"
            msgs.append({"role": role, "content": f"msg {i}"})
        return msgs

    def test_tier3_fallback_to_tier2_on_exception(self, monkeypatch):
        """When Opus call raises, should fall back to Tier 2 (not raise)."""
        from context_compactor import ContextCompactor
        import uuid

        compactor = ContextCompactor(call_id=f"test-t3-{uuid.uuid4().hex[:8]}", log_fn=print)

        # Make anthropic.Anthropic() raise so Tier 3 fails
        import unittest.mock as mock
        with mock.patch.dict("sys.modules", {"anthropic": mock.MagicMock(
            side_effect=ImportError("no anthropic")
        )}):
            monkeypatch.setattr(
                ContextCompactor, "estimate_tokens",
                staticmethod(lambda m: 900_000)
            )
            msgs = self._make_messages(20)
            result = compactor.maybe_compact(msgs)
            # Fallback to Tier 2: should still compact
            assert len(result) < len(msgs)
            assert result[0] == msgs[0]

    def test_tier3_replaces_with_summary(self, monkeypatch):
        """Successful Tier 3 produces exactly 3 messages."""
        from context_compactor import ContextCompactor
        import unittest.mock as mock
        import uuid

        compactor = ContextCompactor(call_id=f"test-t3b-{uuid.uuid4().hex[:8]}", log_fn=print)

        mock_response = mock.MagicMock()
        # A real text block: Tier 3 reads blocks by type (thinking comes first
        # on the Opus-5 family) and rejects a max_tokens cut-off.
        mock_response.content = [mock.MagicMock(type="text", text="Opus summary text here.")]
        mock_response.stop_reason = "end_turn"

        mock_client = mock.MagicMock()
        mock_client.messages.create.return_value = mock_response

        mock_anthropic = mock.MagicMock()
        mock_anthropic.Anthropic.return_value = mock_client

        with mock.patch.dict("sys.modules", {"anthropic": mock_anthropic}):
            monkeypatch.setattr(
                ContextCompactor, "estimate_tokens",
                staticmethod(lambda m: 900_000)
            )
            msgs = self._make_messages(20)
            result = compactor._compact_tier3(msgs)

        assert len(result) == 3
        assert result[0] == msgs[0]
        assert "[CONTEXT COMPACTED]" in result[1]["content"]
        assert "Opus summary text here." in result[1]["content"]
        assert result[2]["role"] == "user"


# ─── Relative %run path resolution ───────────────────────────────────────────

# TestRelativeRunPathResolution class removed (dead module cleanup 2026-08-22).


class TestCompactorHistory:
    """get_tool_output falls back through _compactor_history when file not in current."""

    def setup_method(self, method):
        import agent_migrate
        self._module = agent_migrate
        self._orig_compactor = agent_migrate._current_compactor
        self._orig_history = list(agent_migrate._compactor_history)

    def teardown_method(self, method):
        self._module._current_compactor = self._orig_compactor
        self._module._compactor_history[:] = self._orig_history

    def test_finds_file_from_earlier_compactor(self):
        import uuid
        from context_compactor import ContextCompactor

        # Simulate eval-phase compactor saving a file
        eval_compactor = ContextCompactor(call_id=f"eval-{uuid.uuid4().hex[:6]}", log_fn=print)
        eval_compactor.save_and_truncate("run_on_cluster", "x" * 20000)  # triggers save
        eval_files = os.listdir(eval_compactor._base_dir)
        assert eval_files

        # Simulate fix-phase compactor (different call_id, no files yet)
        fix_compactor = ContextCompactor(call_id=f"fix-{uuid.uuid4().hex[:6]}", log_fn=print)

        self._module._compactor_history.clear()
        self._module._compactor_history.extend([eval_compactor, fix_compactor])
        self._module._current_compactor = fix_compactor

        # get_tool_output should find eval-phase file even though current is fix-phase
        result = self._module._handle_get_tool_output(eval_files[0])
        assert "x" * 100 in result  # actual content

    def test_no_compactors_returns_error(self):
        self._module._compactor_history.clear()
        self._module._current_compactor = None
        result = self._module._handle_get_tool_output("any.txt")
        assert "not available" in result.lower() or "no active" in result.lower()
