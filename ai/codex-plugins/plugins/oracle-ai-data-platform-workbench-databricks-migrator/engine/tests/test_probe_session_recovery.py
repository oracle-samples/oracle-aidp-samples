import asyncio
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import check_data_availability_for_workflow as data_checker

from check_data_availability_for_workflow import (
    ProbeAttemptsExhausted,
    _ProbeSessionRecovery,
    _execute_json_probe_with_recovery,
    _unverified_local_results,
    _unverified_mlflow_results,
    check_mlflow_artifacts,
    check_oci_paths,
    check_scoped_references,
    check_tables_describe,
    unwrap_outputs,
)


def _output(payload):
    return {"outputs": [{"type": "stream", "text": json.dumps(payload)}]}


class ControlSession:
    lake_ocid = "lake"
    workspace_id = "workspace"
    cluster_id = "cluster"
    oci_profile = "DEFAULT"
    _session_name = "data_check_example_job"
    _session_dir = "Workspace/aidp_tool_session"

    def __init__(self, outcomes=None):
        self.outcomes = list(outcomes or [])
        self.calls = []
        self.closed = False

    async def execute(self, code, timeout):
        self.calls.append((code, timeout))
        outcome = self.outcomes.pop(0) if self.outcomes else {"ok": True}
        if isinstance(outcome, Exception):
            raise outcome
        if outcome == "invalid-json":
            return {"outputs": [{"type": "stream", "text": "garbage"}]}
        return _output(outcome)

    async def close(self):
        self.closed = True


def _run_main_scenario(
    monkeypatch, tmp_path, *, refs, session_class, job_name,
):
    """Drive the real main() with a recording AIDPSession test double."""
    manifest_path = tmp_path / f"{job_name}_manifest.json"
    report_path = tmp_path / f"{job_name}_report.json"
    notebook = next(iter(refs))
    manifest_path.write_text(json.dumps({"jobs": [{
        "job_name": job_name,
        "job_id": "source-job",
        "default_cluster": "control-cluster",
        "tasks": [{
            "task_key": "task",
            "notebook_path": notebook,
            "cluster": "control-cluster",
        }],
    }]}))

    async def extract(*_args, **_kwargs):
        return refs

    async def classify(_session, _paths, classified_refs):
        return classified_refs

    class AidpCfg:
        region = "ap-mumbai-1"
        lake_ocid = "lake"
        workspace_id = "workspace"
        oci_profile = "DEFAULT"

    class Config:
        aidp = AidpCfg()
        bucket_mapping = type("BucketCfg", (), {
            "path": str(tmp_path / "mapping.json")
        })()

    monkeypatch.setattr("aidp_config.load_config", lambda **_kwargs: Config())
    monkeypatch.setattr(data_checker, "AIDPSession", session_class)
    monkeypatch.setattr(data_checker, "load_bucket_ns_mapping", lambda _p: None)
    monkeypatch.setattr(data_checker, "extract_data_refs", extract)
    monkeypatch.setattr(data_checker, "classify_refs_on_cluster", classify)
    monkeypatch.setattr(
        data_checker, "attach_data_migration_requirements", lambda *_args: None
    )
    monkeypatch.setattr(sys, "argv", [
        "check_data_availability_for_workflow.py",
        "--manifest", str(manifest_path),
        "--cluster", "control-cluster",
        "--output", str(report_path),
    ])

    asyncio.run(data_checker.main())
    return json.loads(report_path.read_text())


def test_stage_reuses_warm_control_session_without_creating_or_closing_it():
    control = ControlSession([
        {"code": "one", "timeout": 180},
        {"code": "two", "timeout": 180},
    ])

    class UnexpectedProbe:
        def __init__(self, **_kwargs):
            raise AssertionError("healthy path must not create a fresh session")

    async def run():
        stage = _ProbeSessionRecovery(control, "availability", UnexpectedProbe)
        first = await stage.execute_json("one", "first", 180)
        second = await stage.execute_json("two", "second", 180)
        await stage.close()
        return first, second

    first, second = asyncio.run(run())
    assert first["code"] == "one" and second["code"] == "two"
    assert control.closed is False
    assert len(control.calls) == 2


@pytest.mark.parametrize("failure_point", ["execute", "json"])
def test_stage_replaces_failed_session_then_recovers(failure_point):
    created = []
    first_outcome = (
        RuntimeError("execute failed")
        if failure_point == "execute"
        else "invalid-json"
    )
    control = ControlSession([first_outcome])

    class Probe:
        def __init__(self, **kwargs):
            self.number = len(created)
            self.name = kwargs["session_name"]
            self.closed = False
            self.calls = []
            created.append(self)

        async def connect(self):
            pass

        async def execute(self, code, timeout):
            self.calls.append((code, timeout))
            if "SHOW DATABASES" in code:
                return _output({"ready": True})
            return _output({"ok": True})

        async def close(self):
            self.closed = True

    async def run():
        stage = _ProbeSessionRecovery(control, "availability", Probe)
        result = await stage.execute_json("code", "operation", 120)
        await stage.close()
        return result

    assert asyncio.run(run()) == {"ok": True}
    assert control.closed is True
    assert len(created) == 1
    assert created[0].closed is True
    assert created[0].name.endswith("_availability_1")
    warmup_code, warmup_timeout = created[0].calls[0]
    assert "spark.range(1).count()" in warmup_code
    assert 'spark.sql("SHOW DATABASES").collect()' in warmup_code
    assert warmup_timeout == 180


def test_double_failure_raises_and_closes_every_probe():
    created = []
    control = ControlSession([RuntimeError("control poisoned")])

    class Probe:
        def __init__(self, **kwargs):
            self.closed = False
            created.append(self)

        async def connect(self):
            pass

        async def execute(self, code, timeout):
            if "SHOW DATABASES" in code:
                return _output({"ready": True})
            raise RuntimeError("still poisoned")

        async def close(self):
            self.closed = True

    async def run():
        stage = _ProbeSessionRecovery(control, "availability", Probe)
        await stage.execute_json("code", "table batch", 120)

    with pytest.raises(
        ProbeAttemptsExhausted, match="table batch failed after 2"
    ) as error:
        asyncio.run(run())
    assert control.closed is True
    assert len(created) == 1
    assert all(probe.closed for probe in created)
    assert "attempt 1 RuntimeError: control poisoned" in str(error.value)
    assert "attempt 2 RuntimeError: still poisoned" in str(error.value)


def test_replacement_execute_failures_open_circuit_at_creation_budget():
    created = []
    control = ControlSession([RuntimeError("control poisoned")])

    class Probe:
        def __init__(self, **_kwargs):
            self.closed = False
            created.append(self)

        async def connect(self):
            pass

        async def execute(self, code, timeout):
            if "SHOW DATABASES" in code:
                return _output({"ready": True})
            raise RuntimeError("replacement execute failed")

        async def close(self):
            self.closed = True

    async def run():
        stage = _ProbeSessionRecovery(control, "availability", Probe)
        errors = []
        for operation in (
            "first batch", "second batch", "third batch", "fourth batch"
        ):
            try:
                await stage.execute_json("code", operation, 120)
            except RuntimeError as exc:
                errors.append(str(exc))
        return stage, errors

    stage, errors = asyncio.run(run())
    assert stage.unavailable is True
    assert len(created) == 4
    assert all(probe.closed for probe in created)
    assert "failed after 2 warm-session attempts" in errors[0]
    assert "failed after 2 warm-session attempts" in errors[1]
    assert "failed after 2 warm-session attempts" in errors[2]
    assert "replacement-session budget exhausted (4/4)" in errors[2]
    assert "unavailable" in errors[3]


def test_current_session_returns_none_when_replacement_budget_is_exhausted():
    async def run():
        stage = _ProbeSessionRecovery(ControlSession(), "availability")
        stage.probe = None
        stage._replacement_creations = stage._replacement_budget
        current = await stage.current_session()
        return stage, current

    stage, current = asyncio.run(run())

    assert current is None
    assert stage.unavailable is True
    assert stage.unavailable_reason == "replacement-session budget exhausted (4/4)"


def test_execute_stops_immediately_when_replacement_creation_opens_circuit(
    capsys,
):
    control = ControlSession([RuntimeError("control probe failed")])
    created = []

    class Probe:
        def __init__(self, **_kwargs):
            self.closed = False
            created.append(self)

        async def connect(self):
            raise RuntimeError("session-create HTTP 500")

        async def close(self):
            self.closed = True

    async def run():
        stage = _ProbeSessionRecovery(control, "availability", Probe)
        with pytest.raises(RuntimeError) as error:
            await stage.execute_json("code", "OCI batch", 180)
        return stage, str(error.value)

    stage, error = asyncio.run(run())

    assert stage.unavailable is True
    assert len(created) == 1
    assert created[0].closed is True
    assert "failed after 2 warm-session attempts" in error
    assert "attempt 2 RuntimeError: session-create HTTP 500" in error
    assert "attempt 3" not in error
    assert "Replacing probe session and retrying once" in capsys.readouterr().out


def test_execute_does_not_retry_when_budget_opens_circuit_on_first_attempt(
    capsys,
):
    async def run():
        stage = _ProbeSessionRecovery(ControlSession(), "availability")
        stage.probe = None
        stage._replacement_creations = stage._replacement_budget
        with pytest.raises(RuntimeError) as error:
            await stage.execute_json("code", "OCI batch", 180)
        return stage, str(error.value)

    stage, error = asyncio.run(run())

    assert stage.unavailable is True
    assert "failed after 1 warm-session attempts" in error
    assert "attempt 1 RuntimeError: replacement-session budget exhausted (4/4)" in error
    assert "attempt 2" not in error
    assert "Replacing probe session and retrying once" not in capsys.readouterr().out


def test_replacement_warmup_failure_is_fail_loud_and_not_reused():
    created = []
    control = ControlSession([RuntimeError("control poisoned")])

    class Probe:
        def __init__(self, **_kwargs):
            self.closed = False
            created.append(self)

        async def connect(self):
            pass

        async def execute(self, code, timeout):
            raise RuntimeError("cold metastore did not become ready")

        async def close(self):
            self.closed = True

    async def run():
        stage = _ProbeSessionRecovery(control, "availability", Probe)
        await stage.execute_json("code", "table batch", 120)

    with pytest.raises(RuntimeError, match="failed after 2 warm-session attempts"):
        asyncio.run(run())
    assert len(created) == 1
    assert created[0].closed is True


def test_replacement_creation_failure_opens_stage_circuit_breaker():
    created = []
    changes = []
    control = ControlSession([RuntimeError("control poisoned")])

    class Probe:
        def __init__(self, **_kwargs):
            self.closed = False
            created.append(self)

        async def connect(self):
            raise RuntimeError("session-create HTTP 500")

        async def close(self):
            self.closed = True

    async def run():
        stage = _ProbeSessionRecovery(
            control, "availability", Probe,
            on_session_change=changes.append,
        )
        errors = []
        for operation in ("first table", "second table", "third batch"):
            try:
                await stage.execute_json("code", operation, 120)
            except RuntimeError as exc:
                errors.append(str(exc))
        return stage, errors, await stage.current_session()

    stage, errors, current = asyncio.run(run())
    assert stage.unavailable is True
    assert current is None
    assert len(created) == 1
    assert created[0].closed is True
    assert len(errors) == 3
    assert "session-create HTTP 500" in errors[0]
    assert all("unavailable" in error for error in errors[1:])
    assert changes[0] is None
    assert changes[1] is created[0]
    assert changes[-1] is None


def test_replacement_factory_failure_also_opens_circuit_breaker():
    factory_calls = []
    control = ControlSession([RuntimeError("control poisoned")])

    class BrokenFactory:
        def __init__(e, **_kwargs):
            factory_calls.append(True)
            raise RuntimeError("replacement factory failed")

    async def run():
        stage = _ProbeSessionRecovery(control, "availability", BrokenFactory)
        for operation in ("first", "second"):
            with pytest.raises(RuntimeError):
                await stage.execute_json("code", operation, 120)
        return stage

    stage = asyncio.run(run())
    assert stage.unavailable is True
    assert len(factory_calls) == 1


def test_unverified_fallbacks_preserve_reportable_status():
    reason = "replacement session unavailable"
    local = _unverified_local_results(["/Workspace/a"], reason)
    mlflow = _unverified_mlflow_results(
        [{"artifact_uri": "runs:/run/model"}], reason
    )
    assert local["/Workspace/a"] == {
        "exists": "unverified",
        "probe_status": "UNVERIFIED",
        "error": reason,
    }
    assert mlflow["runs:/run/model"]["exists"] == "unverified"
    assert mlflow["runs:/run/model"]["probe_error"] is True


def test_error_outputs_include_structured_exception_details():
    output = unwrap_outputs([
        {"type": "error", "ename": "RuntimeError", "evalue": "fs.exists hung"}
    ])
    assert output == "RuntimeError: fs.exists hung"


def test_scope_unverified_is_visible_in_console(capsys):
    notebook = "/Workspace/job.ipynb"
    filters = ["id = 1"]
    refs = {notebook: {
        "tables": [{
            "table": "schema.table", "usage": "read", "filters": filters,
        }],
        "paths": [],
        "python_imports": [],
        "scala_imports": [],
        "unresolved": [],
    }}
    key = data_checker._scope_key(
        notebook, "table", "schema.table", filters
    )
    report = data_checker.generate_report(
        refs,
        {"schema.table": {"exists": True, "columns": 3}},
        {}, {}, {"by_notebook": {}}, "job",
        scope_results={key: {
            "base_exists": True,
            "scope_exists": None,
            "probe_status": "UNVERIFIED",
            "error": "scope probe unavailable",
        }},
    )

    data_checker.print_report(report)

    output = capsys.readouterr().out
    assert "UNVERIFIED       schema.table" in output
    assert "FILTER/PARTITION SCOPE UNVERIFIED" in output


def test_per_scope_error_is_unverified_instead_of_false_green(capsys):
    notebook = "/Workspace/job.ipynb"
    filters = ["id = 1"]
    refs = {notebook: {
        "tables": [{
            "table": "schema.table", "usage": "read", "filters": filters,
        }],
        "paths": [],
        "python_imports": [],
        "scala_imports": [],
        "unresolved": [],
    }}
    key = data_checker._scope_key(
        notebook, "table", "schema.table", filters
    )
    report = data_checker.generate_report(
        refs,
        {"schema.table": {"exists": True, "columns": 3}},
        {}, {}, {"by_notebook": {}}, "job",
        scope_results={key: {
            "base_exists": True,
            "scope_exists": None,
            "error": "scope evaluation failed",
        }},
    )

    data_checker.print_report(report)

    entry = report["notebooks"][notebook]["read_tables"][0]
    assert entry["exists"] == "unverified"
    assert entry["scope_probe_status"] == "UNVERIFIED"
    assert report["summary"]["tables_found"] == 0
    assert report["summary"]["tables_unverified"] == 1
    assert report["summary"]["scopes_unverified"] == 1
    output = capsys.readouterr().out
    assert "ALL DEPENDENCIES AVAILABLE" not in output
    assert "UNVERIFIED       schema.table" in output


def test_per_path_scope_error_is_unverified_instead_of_false_green(capsys):
    notebook = "/Workspace/job.ipynb"
    path = "oci://bucket@namespace/path"
    filters = ["id = 1"]
    refs = {notebook: {
        "tables": [],
        "paths": [{
            "path": path,
            "usage": "read",
            "filters": filters,
            "source_format": "parquet",
        }],
        "python_imports": [],
        "scala_imports": [],
        "unresolved": [],
    }}
    key = data_checker._scope_key(notebook, "path", path, filters)
    report = data_checker.generate_report(
        refs,
        {}, {path: {"exists": True, "items": 1}}, {},
        {"by_notebook": {}}, "job",
        scope_results={key: {
            "base_exists": True,
            "scope_exists": None,
            "error": "scope evaluation failed",
        }},
    )

    data_checker.print_report(report)

    entry = report["notebooks"][notebook]["read_paths"][0]
    assert entry["exists"] == "unverified"
    assert entry["scope_probe_status"] == "UNVERIFIED"
    assert report["summary"]["paths_found"] == 0
    assert report["summary"]["paths_unverified"] == 1
    assert report["summary"]["scopes_unverified"] == 1
    output = capsys.readouterr().out
    assert "ALL DEPENDENCIES AVAILABLE" not in output
    assert "UNVERIFIED" in output
    assert path in output


def test_main_writes_unverified_report_when_replacement_cannot_connect(
    monkeypatch, tmp_path,
):
    manifest_path = tmp_path / "manifest.json"
    report_path = tmp_path / "report.json"
    notebook = "/Workspace/job.ipynb"
    manifest_path.write_text(json.dumps({"jobs": [{
        "job_name": "circuit_breaker_job",
        "job_id": "source-job",
        "default_cluster": "control-cluster",
        "tasks": [{
            "task_key": "task",
            "notebook_path": notebook,
            "cluster": "control-cluster",
        }],
    }]}))

    sessions = []

    class Session(ControlSession):
        def __init__(self, **kwargs):
            super().__init__()
            self.number = len(sessions)
            self.lake_ocid = kwargs["lake_ocid"]
            self.workspace_id = kwargs["workspace_id"]
            self.cluster_id = kwargs["cluster_id"]
            self.oci_profile = kwargs["oci_profile"]
            self._session_name = kwargs["session_name"]
            self._session_dir = "Workspace/aidp_tool_session"
            sessions.append(self)

        async def connect(self):
            if self.number:
                raise RuntimeError("session-create HTTP 500")

        async def execute(self, code, timeout):
            self.calls.append((code, timeout))
            raise RuntimeError("control probe poisoned")

    async def extract(*_args, **_kwargs):
        return {notebook: {
            "tables": [{"table": "schema.table", "usage": "read"}],
            "paths": [
                {"path": "oci://bucket@namespace/path", "usage": "read"},
                {"path": "/Workspace/input", "usage": "read"},
            ],
            "mlflow_artifacts": [{"artifact_uri": "runs:/run/model"}],
            "python_imports": [],
            "scala_imports": [],
            "unresolved": [],
        }}

    async def classify(_session, _paths, refs):
        return refs

    class AidpCfg:
        region = "ap-mumbai-1"
        lake_ocid = "lake"
        workspace_id = "workspace"
        oci_profile = "DEFAULT"

    class BucketCfg:
        path = str(tmp_path / "mapping.json")

    class Config:
        aidp = AidpCfg()
        bucket_mapping = BucketCfg()

    monkeypatch.setattr("aidp_config.load_config", lambda **_kwargs: Config())
    monkeypatch.setattr(data_checker, "AIDPSession", Session)
    monkeypatch.setattr(data_checker, "load_bucket_ns_mapping", lambda _p: None)
    monkeypatch.setattr(data_checker, "extract_data_refs", extract)
    monkeypatch.setattr(data_checker, "classify_refs_on_cluster", classify)
    monkeypatch.setattr(
        data_checker, "attach_data_migration_requirements", lambda *_args: None
    )
    monkeypatch.setattr(sys, "argv", [
        "check_data_availability_for_workflow.py",
        "--manifest", str(manifest_path),
        "--cluster", "control-cluster",
        "--output", str(report_path),
    ])

    asyncio.run(data_checker.main())

    report = json.loads(report_path.read_text())
    assert len(sessions) == 2
    assert all(session.closed for session in sessions)
    assert report["summary"]["tables_unverified"] == 1
    assert report["summary"]["paths_unverified"] == 3
    entries = report["notebooks"][notebook]["read_paths"]
    assert all(entry.get("exists") == "unverified" for entry in entries)


def test_main_writes_report_when_boundary_replacement_cannot_connect(
    monkeypatch, tmp_path,
):
    manifest_path = tmp_path / "manifest.json"
    report_path = tmp_path / "report.json"
    notebook = "/Workspace/job.ipynb"
    manifest_path.write_text(json.dumps({"jobs": [{
        "job_name": "boundary_circuit_job",
        "job_id": "source-job",
        "default_cluster": "control-cluster",
        "tasks": [{
            "task_key": "task",
            "notebook_path": notebook,
            "cluster": "control-cluster",
        }],
    }]}))

    sessions = []

    class Session(ControlSession):
        def __init__(self, **kwargs):
            super().__init__()
            self.number = len(sessions)
            self.lake_ocid = kwargs["lake_ocid"]
            self.workspace_id = kwargs["workspace_id"]
            self.cluster_id = kwargs["cluster_id"]
            self.oci_profile = kwargs["oci_profile"]
            self._session_name = kwargs["session_name"]
            self._session_dir = "Workspace/aidp_tool_session"
            sessions.append(self)

        async def connect(self):
            if self.number == 2:
                raise RuntimeError("boundary session-create HTTP 500")

        async def execute(self, code, timeout):
            self.calls.append((code, timeout))
            if self.number == 1 and "SHOW DATABASES" in code:
                return _output({"ready": True})
            raise RuntimeError(
                "control probe poisoned" if self.number == 0
                else "replacement probe poisoned"
            )

    async def extract(*_args, **_kwargs):
        return {notebook: {
            "tables": [],
            "paths": [
                {"path": "oci://bucket@namespace/path", "usage": "read"},
                {"path": "/Workspace/input", "usage": "read"},
            ],
            "mlflow_artifacts": [{"artifact_uri": "runs:/run/model"}],
            "python_imports": [],
            "scala_imports": [],
            "unresolved": [],
        }}

    async def classify(_session, _paths, refs):
        return refs

    class AidpCfg:
        region = "ap-mumbai-1"
        lake_ocid = "lake"
        workspace_id = "workspace"
        oci_profile = "DEFAULT"

    class BucketCfg:
        path = str(tmp_path / "mapping.json")

    class Config:
        aidp = AidpCfg()
        bucket_mapping = BucketCfg()

    monkeypatch.setattr("aidp_config.load_config", lambda **_kwargs: Config())
    monkeypatch.setattr(data_checker, "AIDPSession", Session)
    monkeypatch.setattr(data_checker, "load_bucket_ns_mapping", lambda _p: None)
    monkeypatch.setattr(data_checker, "extract_data_refs", extract)
    monkeypatch.setattr(data_checker, "classify_refs_on_cluster", classify)
    monkeypatch.setattr(
        data_checker, "attach_data_migration_requirements", lambda *_args: None
    )
    monkeypatch.setattr(sys, "argv", [
        "check_data_availability_for_workflow.py",
        "--manifest", str(manifest_path),
        "--cluster", "control-cluster",
        "--output", str(report_path),
    ])

    asyncio.run(data_checker.main())

    report = json.loads(report_path.read_text())
    assert len(sessions) == 3
    assert all(session.closed for session in sessions)
    assert report["summary"]["paths_unverified"] == 3
    assert "boundary session-create HTTP 500" in json.dumps(report)


def test_main_recovers_local_path_failure_and_still_checks_mlflow(
    monkeypatch, tmp_path,
):
    manifest_path = tmp_path / "manifest.json"
    report_path = tmp_path / "report.json"
    notebook = "/Workspace/job.ipynb"
    manifest_path.write_text(json.dumps({"jobs": [{
        "job_name": "local_probe_recovery_job",
        "job_id": "source-job",
        "default_cluster": "control-cluster",
        "tasks": [{
            "task_key": "task",
            "notebook_path": notebook,
            "cluster": "control-cluster",
        }],
    }]}))

    sessions = []

    class Session(ControlSession):
        def __init__(self, **kwargs):
            super().__init__()
            self.number = len(sessions)
            self.lake_ocid = kwargs["lake_ocid"]
            self.workspace_id = kwargs["workspace_id"]
            self.cluster_id = kwargs["cluster_id"]
            self.oci_profile = kwargs["oci_profile"]
            self._session_name = kwargs["session_name"]
            self._session_dir = "Workspace/aidp_tool_session"
            sessions.append(self)

        async def connect(self):
            pass

        async def execute(self, code, timeout):
            self.calls.append((code, timeout))
            if "SHOW DATABASES" in code:
                return _output({"ready": True})
            if "subprocess.run" in code:
                raise RuntimeError("ls probe: kernel died")
            if "MlflowClient" in code:
                return _output({
                    "runs:/run/model": {"exists": True, "items": 1}
                })
            raise AssertionError(f"unexpected probe code: {code[:80]}")

    async def extract(*_args, **_kwargs):
        return {notebook: {
            "tables": [],
            "paths": [{"path": "/Workspace/input", "usage": "read"}],
            "mlflow_artifacts": [{
                "artifact_uri": "runs:/run/model",
                "run_id": "run",
                "artifact_path": "model",
            }],
            "python_imports": [],
            "scala_imports": [],
            "unresolved": [],
        }}

    async def classify(_session, _paths, refs):
        return refs

    class AidpCfg:
        region = "ap-mumbai-1"
        lake_ocid = "lake"
        workspace_id = "workspace"
        oci_profile = "DEFAULT"

    class Config:
        aidp = AidpCfg()
        bucket_mapping = type("BucketCfg", (), {
            "path": str(tmp_path / "mapping.json")
        })()

    monkeypatch.setattr("aidp_config.load_config", lambda **_kwargs: Config())
    monkeypatch.setattr(data_checker, "AIDPSession", Session)
    monkeypatch.setattr(data_checker, "load_bucket_ns_mapping", lambda _p: None)
    monkeypatch.setattr(data_checker, "extract_data_refs", extract)
    monkeypatch.setattr(data_checker, "classify_refs_on_cluster", classify)
    monkeypatch.setattr(
        data_checker, "attach_data_migration_requirements", lambda *_args: None
    )
    monkeypatch.setattr(sys, "argv", [
        "check_data_availability_for_workflow.py",
        "--manifest", str(manifest_path),
        "--cluster", "control-cluster",
        "--output", str(report_path),
    ])

    asyncio.run(data_checker.main())

    report = json.loads(report_path.read_text())
    assert len(sessions) == 3
    assert all(session.closed for session in sessions)
    assert report["summary"]["paths_unverified"] == 1
    assert report["summary"]["mlflow_artifacts_found"] == 1
    assert "ls probe: kernel died" in json.dumps(report)


def test_main_does_not_create_boundary_session_without_downstream_work(
    monkeypatch, tmp_path,
):
    manifest_path = tmp_path / "manifest.json"
    report_path = tmp_path / "report.json"
    notebook = "/Workspace/job.ipynb"
    manifest_path.write_text(json.dumps({"jobs": [{
        "job_name": "no_downstream_probe_job",
        "job_id": "source-job",
        "default_cluster": "control-cluster",
        "tasks": [{
            "task_key": "task",
            "notebook_path": notebook,
            "cluster": "control-cluster",
        }],
    }]}))
    sessions = []

    class Session(ControlSession):
        def __init__(self, **kwargs):
            super().__init__()
            self.lake_ocid = kwargs["lake_ocid"]
            self.workspace_id = kwargs["workspace_id"]
            self.cluster_id = kwargs["cluster_id"]
            self.oci_profile = kwargs["oci_profile"]
            self._session_name = kwargs["session_name"]
            self._session_dir = "Workspace/aidp_tool_session"
            sessions.append(self)

        async def connect(self):
            pass

        async def execute(self, code, timeout):
            raise AssertionError("no availability probe should execute")

    async def extract(*_args, **_kwargs):
        return {notebook: {
            "tables": [], "paths": [], "mlflow_artifacts": [],
            "python_imports": [], "scala_imports": [], "unresolved": [],
        }}

    async def classify(_session, _paths, refs):
        return refs

    class AidpCfg:
        region = "ap-mumbai-1"
        lake_ocid = "lake"
        workspace_id = "workspace"
        oci_profile = "DEFAULT"

    class Config:
        aidp = AidpCfg()
        bucket_mapping = type("BucketCfg", (), {
            "path": str(tmp_path / "mapping.json")
        })()

    monkeypatch.setattr("aidp_config.load_config", lambda **_kwargs: Config())
    monkeypatch.setattr(data_checker, "AIDPSession", Session)
    monkeypatch.setattr(data_checker, "load_bucket_ns_mapping", lambda _p: None)
    monkeypatch.setattr(data_checker, "extract_data_refs", extract)
    monkeypatch.setattr(data_checker, "classify_refs_on_cluster", classify)
    monkeypatch.setattr(
        data_checker, "attach_data_migration_requirements", lambda *_args: None
    )
    monkeypatch.setattr(sys, "argv", [
        "check_data_availability_for_workflow.py",
        "--manifest", str(manifest_path),
        "--cluster", "control-cluster",
        "--output", str(report_path),
    ])

    asyncio.run(data_checker.main())

    assert len(sessions) == 1
    assert sessions[0].closed is True
    assert report_path.exists()


def test_main_logs_post_scope_boundary_failure_for_control_libraries(
    monkeypatch, tmp_path, capsys,
):
    manifest_path = tmp_path / "manifest.json"
    report_path = tmp_path / "report.json"
    notebook = "/Workspace/job.ipynb"
    manifest_path.write_text(json.dumps({"jobs": [{
        "job_name": "post_scope_boundary_job",
        "job_id": "source-job",
        "default_cluster": "control-cluster",
        "tasks": [{
            "task_key": "task",
            "notebook_path": notebook,
            "cluster": "control-cluster",
        }],
    }]}))
    sessions = []

    class Session(ControlSession):
        def __init__(self, **kwargs):
            super().__init__()
            self.number = len(sessions)
            self.lake_ocid = kwargs["lake_ocid"]
            self.workspace_id = kwargs["workspace_id"]
            self.cluster_id = kwargs["cluster_id"]
            self.oci_profile = kwargs["oci_profile"]
            self._session_name = kwargs["session_name"]
            self._session_dir = "Workspace/aidp_tool_session"
            sessions.append(self)

        async def connect(self):
            if self.number == 2:
                raise RuntimeError("post-scope session-create HTTP 500")

        async def execute(self, code, timeout):
            self.calls.append((code, timeout))
            if "SHOW DATABASES" in code:
                return _output({"ready": True})
            if "DESCRIBE TABLE" in code:
                return _output({
                    "schema.table": {
                        "exists": True,
                        "resolved": "schema.table",
                        "columns": 1,
                    }
                })
            if "frame.where" in code:
                raise RuntimeError("scope probe failed")
            raise AssertionError(f"unexpected probe code: {code[:80]}")

    async def extract(*_args, **_kwargs):
        return {notebook: {
            "tables": [{
                "table": "schema.table",
                "usage": "read",
                "filters": ["id = 1"],
            }],
            "paths": [],
            "mlflow_artifacts": [],
            "python_imports": ["requests"],
            "scala_imports": [],
            "unresolved": [],
        }}

    async def classify(_session, _paths, refs):
        return refs

    class AidpCfg:
        region = "ap-mumbai-1"
        lake_ocid = "lake"
        workspace_id = "workspace"
        oci_profile = "DEFAULT"

    class Config:
        aidp = AidpCfg()
        bucket_mapping = type("BucketCfg", (), {
            "path": str(tmp_path / "mapping.json")
        })()

    monkeypatch.setattr("aidp_config.load_config", lambda **_kwargs: Config())
    monkeypatch.setattr(data_checker, "AIDPSession", Session)
    monkeypatch.setattr(data_checker, "load_bucket_ns_mapping", lambda _p: None)
    monkeypatch.setattr(data_checker, "extract_data_refs", extract)
    monkeypatch.setattr(data_checker, "classify_refs_on_cluster", classify)
    monkeypatch.setattr(
        data_checker, "attach_data_migration_requirements", lambda *_args: None
    )
    monkeypatch.setattr(sys, "argv", [
        "check_data_availability_for_workflow.py",
        "--manifest", str(manifest_path),
        "--cluster", "control-cluster",
        "--output", str(report_path),
    ])

    asyncio.run(data_checker.main())

    output = capsys.readouterr().out
    report = json.loads(report_path.read_text())
    assert len(sessions) == 3
    assert "job-cluster dependency probe could not run" in output
    assert output.count("WARNING:") == 1
    assert "post-scope session-create HTTP 500" in output
    assert "post-scope session-create HTTP 500" in json.dumps(report)


def test_main_isolates_hung_oci_endpoint_and_preserves_later_checks(
    monkeypatch, tmp_path,
):
    notebook = "/Workspace/job.ipynb"
    hung_paths = [
        f"oci://aaa-hung@namespace/path-{index:02d}" for index in range(44)
    ]
    healthy_path = "oci://zzz-healthy@namespace/input"
    refs = {notebook: {
        "tables": [{
            "table": "schema.table", "usage": "read", "filters": ["id = 1"],
        }],
        "paths": [
            *({"path": path, "usage": "read"} for path in hung_paths),
            {"path": healthy_path, "usage": "read"},
            {"path": "/Workspace/input", "usage": "read"},
        ],
        "mlflow_artifacts": [{
            "artifact_uri": "runs:/run/model",
            "run_id": "run",
            "artifact_path": "model",
        }],
        "python_imports": ["requests"],
        "scala_imports": [],
        "unresolved": [],
    }}
    sessions = []

    class Session(ControlSession):
        def __init__(self, **kwargs):
            super().__init__()
            self.number = len(sessions)
            self.lake_ocid = kwargs["lake_ocid"]
            self.workspace_id = kwargs["workspace_id"]
            self.cluster_id = kwargs["cluster_id"]
            self.oci_profile = kwargs["oci_profile"]
            self._session_name = kwargs["session_name"]
            self._session_dir = "Workspace/aidp_tool_session"
            sessions.append(self)

        async def connect(self):
            pass

        async def execute(self, code, timeout):
            self.calls.append((code, timeout))
            if "SHOW DATABASES" in code:
                return _output({"ready": True})
            if "DESCRIBE TABLE" in code:
                return _output({
                    "schema.table": {
                        "exists": True,
                        "resolved": "schema.table",
                        "columns": 1,
                    }
                })
            if "path_specs = " in code:
                specs = json.loads(
                    code.split("path_specs = ", 1)[1].splitlines()[0]
                )
                if "aaa-hung@namespace" in specs[0]["probe_path"]:
                    raise RuntimeError("fs.exists timed out on hung endpoint")
                return _output({
                    spec["path"]: {"exists": True, "items": 1}
                    for spec in specs
                })
            if "subprocess.run" in code:
                return _output({"/Workspace/input": {"exists": True, "items": 1}})
            if "MlflowClient" in code:
                return _output({
                    "runs:/run/model": {"exists": True, "items": 1}
                })
            if "frame.where" in code:
                specs = json.loads(code.split("specs = ", 1)[1].splitlines()[0])
                return _output({
                    spec["key"]: {
                        "base_exists": True,
                        "scope_exists": True,
                        "predicate": spec["predicate"],
                    }
                    for spec in specs
                })
            if "import json, importlib, subprocess" in code:
                return _output({"python": {}, "scala": {}})
            raise AssertionError(f"unexpected probe code: {code[:100]}")

    report = _run_main_scenario(
        monkeypatch,
        tmp_path,
        refs=refs,
        session_class=Session,
        job_name="hung_endpoint_isolation_job",
    )

    assert len(sessions) == 3
    assert all(session.closed for session in sessions)
    assert report["summary"]["paths_unverified"] == 44
    assert report["summary"]["paths_found"] == 3
    assert report["summary"]["mlflow_artifacts_found"] == 1
    assert report["summary"]["libs_unverified"] == 0
    assert report["summary"]["scopes_unverified"] == 0
    assert "skipped after batch 1 double failure" in json.dumps(report)


def test_main_reports_runwide_budget_exhaustion_after_three_hung_sources(
    monkeypatch, tmp_path,
):
    notebook = "/Workspace/job.ipynb"
    refs = {notebook: {
        "tables": [],
        "paths": [
            {"path": f"oci://{bucket}@namespace/input", "usage": "read"}
            for bucket in ("aaa-hung", "bbb-hung", "ccc-hung")
        ] + [{"path": "/Workspace/input", "usage": "read"}],
        "mlflow_artifacts": [{
            "artifact_uri": "runs:/run/model",
            "run_id": "run",
            "artifact_path": "model",
        }],
        "python_imports": ["requests"],
        "scala_imports": [],
        "unresolved": [],
    }}
    sessions = []

    class Session(ControlSession):
        def __init__(self, **kwargs):
            super().__init__()
            self.number = len(sessions)
            self.lake_ocid = kwargs["lake_ocid"]
            self.workspace_id = kwargs["workspace_id"]
            self.cluster_id = kwargs["cluster_id"]
            self.oci_profile = kwargs["oci_profile"]
            self._session_name = kwargs["session_name"]
            self._session_dir = "Workspace/aidp_tool_session"
            sessions.append(self)

        async def connect(self):
            pass

        async def execute(self, code, timeout):
            self.calls.append((code, timeout))
            if "SHOW DATABASES" in code:
                return _output({"ready": True})
            if "path_specs = " in code:
                raise RuntimeError("deterministic endpoint timeout")
            raise AssertionError(
                "downstream probes must not execute after the circuit opens"
            )

    report = _run_main_scenario(
        monkeypatch,
        tmp_path,
        refs=refs,
        session_class=Session,
        job_name="runwide_budget_exhaustion_job",
    )

    serialized = json.dumps(report)
    assert len(sessions) == 5
    assert all(session.closed for session in sessions)
    assert report["summary"]["paths_unverified"] == 5
    assert report["summary"]["mlflow_artifact_probe_errors"] == 1
    assert report["summary"]["libs_unverified"] == 1
    assert "replacement-session budget exhausted (4/4)" in serialized
    assert "Probe stage unavailable after replacement-session failure" in serialized


def test_stage_names_are_stable_and_bounded_to_two_slots():
    stage = _ProbeSessionRecovery(ControlSession(), "availability")
    names = []
    for generation in range(6):
        stage._generation = generation
        names.append(stage._session_name())

    assert len(set(names)) == 2
    assert names[0].endswith("_availability_0")
    assert names[1].endswith("_availability_1")


def test_simple_executor_fallback_uses_existing_protocol():
    class SimpleExecutor:
        async def execute(self, code, timeout):
            return _output({"ok": True})

    result = asyncio.run(_execute_json_probe_with_recovery(
        SimpleExecutor(), "code", "simple", 60
    ))
    assert result == {"ok": True}


def test_oci_double_failure_skips_later_batch_from_same_endpoint():
    created = []
    control = ControlSession([RuntimeError("control fs.exists timeout")])

    class Probe:
        def __init__(self, **_kwargs):
            self.number = len(created)
            self.closed = False
            created.append(self)

        async def connect(self):
            pass

        async def execute(self, code, timeout):
            if "SHOW DATABASES" in code:
                return _output({"ready": True})
            if self.number == 0:
                raise RuntimeError("replacement fs.exists timeout")
            specs = json.loads(code.split("path_specs = ", 1)[1].splitlines()[0])
            return _output({spec["path"]: {"exists": True} for spec in specs})

        async def close(self):
            self.closed = True

    paths = [f"oci://bucket@namespace/path-{index}" for index in range(6)]
    shared_stage = _ProbeSessionRecovery(control, "availability", Probe)
    results = asyncio.run(check_oci_paths(
        control, paths, probe_stage=shared_stage
    ))

    assert len(results) == 6
    assert all(results[path]["exists"] == "unverified" for path in paths[:5])
    assert results[paths[5]]["exists"] == "unverified"
    assert "skipped after batch 1 double failure" in results[paths[5]]["error"]
    assert shared_stage.unavailable is False
    assert len(created) == 1
    assert created[0].closed is True
    assert "control fs.exists timeout" in results[paths[0]]["error"]
    assert "replacement fs.exists timeout" in results[paths[0]]["error"]
    assert "timeout=180s" in results[paths[0]]["operation"]


def test_oci_dead_stage_does_not_guess_endpoint_double_failure():
    stage = _ProbeSessionRecovery(ControlSession(), "availability")
    stage.probe = None
    stage._dead_error = RuntimeError("replacement-session budget exhausted (4/4)")
    paths = [f"oci://healthy-bucket@namespace/path-{index}" for index in range(6)]

    results = asyncio.run(check_oci_paths(
        stage.control, paths, probe_stage=stage
    ))

    assert all(item["exists"] == "unverified" for item in results.values())
    assert all(
        "not executed because the probe stage is unavailable" in item["error"]
        for item in results.values()
    )
    assert all("double failure" not in item["error"] for item in results.values())


def test_oci_incomplete_batch_does_not_skip_later_batch():
    class IncompleteThenHealthy(ControlSession):
        async def execute(self, code, timeout):
            self.calls.append((code, timeout))
            specs = json.loads(code.split("path_specs = ", 1)[1].splitlines()[0])
            if len(self.calls) == 1:
                return _output({
                    spec["path"]: {"exists": True}
                    for spec in specs[:-1]
                })
            return _output({spec["path"]: {"exists": True} for spec in specs})

    control = IncompleteThenHealthy()
    paths = [f"oci://bucket@namespace/path-{index}" for index in range(6)]
    stage = _ProbeSessionRecovery(control, "availability")

    results = asyncio.run(check_oci_paths(
        control, paths, probe_stage=stage
    ))

    assert all(results[path]["exists"] == "unverified" for path in paths[:5])
    assert "incomplete results" in results[paths[0]]["error"]
    assert "double failure" not in results[paths[0]]["error"]
    assert results[paths[5]]["exists"] is True
    assert stage.unavailable is False


def test_hung_oci_endpoint_is_skipped_and_next_endpoint_is_probed():
    created = []
    control = ControlSession([RuntimeError("control bucket timeout")])

    class Probe:
        def __init__(self, **_kwargs):
            self.number = len(created)
            self.closed = False
            created.append(self)

        async def connect(self):
            pass

        async def execute(self, code, timeout):
            if "SHOW DATABASES" in code:
                return _output({"ready": True})
            if self.number == 0:
                raise RuntimeError(f"bucket timeout on replacement {self.number}")
            specs = json.loads(code.split("path_specs = ", 1)[1].splitlines()[0])
            return _output({spec["path"]: {"exists": True} for spec in specs})

        async def close(self):
            self.closed = True

    # Eleven paths from one endpoint form three batches.  Once its first batch
    # fails twice, the remaining two batches are skipped without consuming the
    # global replacement budget; a different endpoint is still probed.
    hung_paths = [
        f"oci://hung-bucket@namespace/path-{index:02d}"
        for index in range(11)
    ]
    healthy_path = "oci://healthy-bucket@namespace/input"
    paths = hung_paths + [healthy_path]
    shared_stage = _ProbeSessionRecovery(control, "availability", Probe)
    results = asyncio.run(check_oci_paths(
        control, paths, probe_stage=shared_stage
    ))

    assert all(results[path]["exists"] == "unverified" for path in hung_paths)
    assert "skipped after batch 1 double failure" in results[hung_paths[-1]]["error"]
    assert results[healthy_path]["exists"] is True
    assert shared_stage.unavailable is False
    assert len(created) == 2
    assert created[0].closed is True
    assert created[1].closed is False


def test_table_double_failure_is_unverified_and_next_table_continues():
    created = []
    control = ControlSession([RuntimeError("control catalog timeout")])

    class Probe:
        def __init__(self, **_kwargs):
            self.number = len(created)
            self.closed = False
            created.append(self)

        async def connect(self):
            pass

        async def execute(self, code, timeout):
            if "SHOW DATABASES" in code:
                return _output({"ready": True})
            if self.number == 0:
                raise RuntimeError("replacement catalog timeout")
            table = json.loads(code.split("tables = ", 1)[1].splitlines()[0])[0]
            return _output({table: {"exists": True}})

        async def close(self):
            self.closed = True

    shared_stage = _ProbeSessionRecovery(control, "availability", Probe)
    results = asyncio.run(check_tables_describe(
        control, ["schema.first", "schema.second"],
        probe_stage=shared_stage,
    ))

    assert results["schema.first"]["exists"] == "unverified"
    assert results["schema.second"]["exists"] is True
    assert shared_stage.unavailable is False
    assert len(created) == 2


def test_mlflow_double_failure_skips_remaining_artifacts_on_same_tracker():
    created = []
    control = ControlSession([RuntimeError("control MLflow timeout")])

    class Probe:
        def __init__(self, **_kwargs):
            self.number = len(created)
            self.closed = False
            created.append(self)

        async def connect(self):
            pass

        async def execute(self, code, timeout):
            if "SHOW DATABASES" in code:
                return _output({"ready": True})
            if self.number == 0:
                raise RuntimeError("replacement MLflow timeout")
            specs = json.loads(code.split("specs = ", 1)[1].splitlines()[0])
            uri = specs[0]["artifact_uri"]
            return _output({uri: {"exists": True, "items": 1}})

        async def close(self):
            self.closed = True

    artifacts = [
        {
            "artifact_uri": "runs:/run-1/slow",
            "run_id": "run-1",
            "artifact_path": "slow",
            "object_type": "DIRECTORY",
        },
        {
            "artifact_uri": "runs:/run-1/healthy",
            "run_id": "run-1",
            "artifact_path": "healthy",
            "object_type": "DIRECTORY",
        },
    ]
    shared_stage = _ProbeSessionRecovery(control, "availability", Probe)
    results = asyncio.run(check_mlflow_artifacts(
        control, artifacts, probe_stage=shared_stage
    ))

    assert results["runs:/run-1/slow"]["exists"] == "unverified"
    assert results["runs:/run-1/slow"]["probe_status"] == "UNVERIFIED"
    assert "control MLflow timeout" in results["runs:/run-1/slow"]["error"]
    assert "replacement MLflow timeout" in results["runs:/run-1/slow"]["error"]
    assert results["runs:/run-1/healthy"]["exists"] == "unverified"
    assert "skipped after artifact 1 double failure" in (
        results["runs:/run-1/healthy"]["error"]
    )
    assert shared_stage.unavailable is False
    assert len(created) == 1
    assert created[0].closed is True


def test_mlflow_missing_result_does_not_skip_later_artifact():
    artifacts = [
        {"artifact_uri": "runs:/run-1/missing", "run_id": "run-1"},
        {"artifact_uri": "runs:/run-1/healthy", "run_id": "run-1"},
    ]

    class MissingThenHealthy(ControlSession):
        async def execute(self, code, timeout):
            self.calls.append((code, timeout))
            specs = json.loads(code.split("specs = ", 1)[1].splitlines()[0])
            uri = specs[0]["artifact_uri"]
            if len(self.calls) == 1:
                return _output({})
            return _output({uri: {"exists": True, "items": 1}})

    control = MissingThenHealthy()
    stage = _ProbeSessionRecovery(control, "availability")
    results = asyncio.run(check_mlflow_artifacts(
        control, artifacts, probe_stage=stage
    ))

    assert results["runs:/run-1/missing"]["exists"] == "unverified"
    assert "returned no result" in results["runs:/run-1/missing"]["error"]
    assert "double failure" not in results["runs:/run-1/missing"]["error"]
    assert results["runs:/run-1/healthy"]["exists"] is True
    assert stage.unavailable is False


def test_mlflow_healthy_artifacts_reuse_warm_control_session():
    artifacts = [
        {"artifact_uri": "runs:/run-1/one", "run_id": "run-1"},
        {"artifact_uri": "runs:/run-1/two", "run_id": "run-1"},
    ]

    class MlflowControl(ControlSession):
        async def execute(self, code, timeout):
            self.calls.append((code, timeout))
            specs = json.loads(code.split("specs = ", 1)[1].splitlines()[0])
            uri = specs[0]["artifact_uri"]
            return _output({uri: {"exists": True, "items": 1}})

    control = MlflowControl()

    class UnexpectedProbe:
        def __init__(self, **_kwargs):
            raise AssertionError("healthy MLflow probes must reuse control")

    shared_stage = _ProbeSessionRecovery(
        control, "availability", UnexpectedProbe
    )
    results = asyncio.run(check_mlflow_artifacts(
        control, artifacts, probe_stage=shared_stage
    ))

    assert set(results) == {"runs:/run-1/one", "runs:/run-1/two"}
    assert all(item["exists"] is True for item in results.values())
    assert len(control.calls) == 2
    assert control.closed is False


def test_scoped_double_failure_marks_each_scope_unverified(monkeypatch):
    async def fail(*_args, **_kwargs):
        raise RuntimeError("scope probe timed out")

    monkeypatch.setattr(
        "check_data_availability_for_workflow._execute_json_probe_with_recovery",
        fail,
    )
    refs = {"notebook": {"tables": [
        {"table": "schema.table", "filters": ["id = 1"]},
    ], "paths": []}}
    shared_stage = object()
    results = asyncio.run(check_scoped_references(
        ControlSession(), refs, {"schema.table": {"exists": True}}, {},
        probe_stage=shared_stage,
    ))
    record = next(iter(results.values()))
    assert record["scope_exists"] is None
    assert record["probe_status"] == "UNVERIFIED"
    assert "scope probe timed out" in record["error"]
