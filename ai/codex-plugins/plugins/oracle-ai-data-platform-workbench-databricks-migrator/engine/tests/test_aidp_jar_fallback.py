import pytest
from types import SimpleNamespace

from dbx_export.__main__ import (
    DEFAULT_AIDP_JAR_DIR,
    configured_jar_fallback_dir,
    phase2_upload_files,
    resolve_failed_jar_fallbacks,
)
from dbx_export.jar_discovery import (
    JarDiscoveryResult,
    JarResolution,
    reconcile_failed_declared_jars,
)
from dbx_export.aidp_client import AidpApiError, AidpClient, AidpConfig
from dbx_export import aidp_client
from dbx_export.dependency_resolver import FileNode, Graph
from dbx_export.report import Report


class _Aidp:
    def __init__(self, existing=(), contents=None):
        self.existing = set(existing)
        self.contents = dict(contents or {})
        self.probes = []

    def object_exists(self, path):
        self.probes.append(path)
        return path in self.existing

    def download_file(self, path):
        return self.contents.get(path, b"shared-jar-bytes")


class _UploadFailsAidp(_Aidp):
    def upload_file(self, path, content, overwrite=False):
        raise AidpApiError("uploadFileMeta", 500, "test upload failure")


def _graph(uri):
    return Graph(files={
        uri: FileNode(
            dbx_source=uri,
            kind="jar",
            fetch_error="workspace_export_failed",
            error_detail="HTTP 429",
        )
    })


def _report():
    return Report(
        run_id="test-run",
        dbx_root_job_id="123",
        export_output="/Workspace/ai_tool_exported",
        prefix="ai_tool",
    )


def test_configured_jar_fallback_dir_reads_yaml_value(monkeypatch):
    import aidp_config

    def fake_load_config(cli_config_path=None):
        assert cli_config_path == "/Workspace/customer.yaml"
        return SimpleNamespace(
            paths=SimpleNamespace(jar_fallback_dir="/Workspace/team/shared-jars")
        )

    monkeypatch.setattr(aidp_config, "load_config", fake_load_config)

    assert configured_jar_fallback_dir("/Workspace/customer.yaml") == (
        "/Workspace/team/shared-jars"
    )


def test_failed_dbx_jar_rejects_unverifiable_shared_repository_candidate():
    uri = "/Workspace/Databricks Jars/model-runtime.jar"
    shared = f"{DEFAULT_AIDP_JAR_DIR}/model-runtime.jar"
    graph = _graph(uri)
    aidp = _Aidp(existing={shared})

    resolve_failed_jar_fallbacks(aidp, graph, DEFAULT_AIDP_JAR_DIR)

    assert graph.files[uri].aidp_fallback_path is None
    assert graph.files[uri].aidp_fallback_sha256
    assert "rejected shared candidate" in graph.files[uri].aidp_fallback_provenance
    mapping, failed = phase2_upload_files(
        aidp, graph, "/Workspace/ai_tool_exported", DEFAULT_AIDP_JAR_DIR,
        False, _report()
    )
    assert mapping == {}
    assert failed is True


def test_failed_dbx_jar_records_sanitized_shared_candidate_without_reusing_it():
    uri = "/Workspace/Databricks Jars/model runtime (1).jar"
    shared = f"{DEFAULT_AIDP_JAR_DIR}/model_runtime_1_.jar"
    graph = _graph(uri)
    aidp = _Aidp(existing={shared})

    resolve_failed_jar_fallbacks(aidp, graph, DEFAULT_AIDP_JAR_DIR)

    assert graph.files[uri].aidp_fallback_path is None
    assert shared in graph.files[uri].aidp_fallback_provenance


def test_failed_dbx_jar_soft_fails_when_shared_repository_has_no_match():
    uri = "/Workspace/Databricks Jars/missing.jar"
    graph = _graph(uri)
    aidp = _Aidp()

    resolve_failed_jar_fallbacks(aidp, graph, DEFAULT_AIDP_JAR_DIR)

    mapping, failed = phase2_upload_files(
        aidp, graph, "/Workspace/ai_tool_exported", DEFAULT_AIDP_JAR_DIR,
        False, _report()
    )
    assert mapping == {}
    assert failed is True


def test_failed_declared_jar_is_replaced_when_context_references_are_covered():
    uri = "dbfs:/FileStore/jars/deleted-runtime.jar"
    fallback = f"{DEFAULT_AIDP_JAR_DIR}/replacement-runtime.jar"
    context = "job:123/job_cluster:main"
    graph = Graph(files={
        uri: FileNode(
            dbx_source=uri,
            kind="jar",
            fetch_error="not_found",
            referenced_by=[context],
        )
    })
    discovery = JarDiscoveryResult(
        fallback_path=DEFAULT_AIDP_JAR_DIR,
        resolutions=[JarResolution(
            reference="com.example.Runtime",
            reference_type="class",
            notebook="/Users/example/task",
            cell_index=2,
            cluster_context=context,
            status="MATCHED",
            selected_jar=fallback,
        )],
    )

    reconcile_failed_declared_jars(graph, discovery)

    assert graph.files[uri].aidp_replacement_paths == [fallback]
    report = _report()
    mapping, failed = phase2_upload_files(
        _Aidp(), graph, "/Workspace/ai_tool_exported",
        DEFAULT_AIDP_JAR_DIR, False, report,
    )
    assert mapping == {}
    assert failed is False
    assert report.files[-1].status == "SKIPPED_REPLACED"
    assert report.summary()["files_failed"] == 0


def test_failed_declared_jar_remains_failed_when_context_has_no_jvm_reference():
    uri = "dbfs:/FileStore/jars/deleted-runtime.jar"
    graph = Graph(files={
        uri: FileNode(
            dbx_source=uri,
            kind="jar",
            fetch_error="not_found",
            referenced_by=["job:123/job_cluster:main"],
        )
    })

    reconcile_failed_declared_jars(
        graph,
        JarDiscoveryResult(fallback_path=DEFAULT_AIDP_JAR_DIR),
    )

    assert graph.files[uri].aidp_replacement_paths == []


def test_failed_dbx_jar_raises_in_strict_mode_when_no_fallback():
    uri = "/Workspace/Databricks Jars/missing.jar"
    graph = _graph(uri)
    aidp = _Aidp()

    resolve_failed_jar_fallbacks(aidp, graph, DEFAULT_AIDP_JAR_DIR)

    with pytest.raises(RuntimeError, match="no verified shared AIDP JAR"):
        phase2_upload_files(
            aidp, graph, "/Workspace/ai_tool_exported", DEFAULT_AIDP_JAR_DIR,
            False, _report(), strict_jars=True,
        )


def test_jar_upload_error_uses_existing_shared_fallback(capsys):
    uri = "/Workspace/Databricks Jars/helperlib-1.6.1.jar"
    fallback = f"{DEFAULT_AIDP_JAR_DIR}/helperlib-1.6.1.jar"
    graph = Graph(files={
        uri: FileNode(dbx_source=uri, kind="jar", content=b"jar-bytes")
    })
    report = _report()

    mapping, failed = phase2_upload_files(
        _UploadFailsAidp(existing={fallback}, contents={fallback: b"jar-bytes"}),
        graph,
        "/Workspace/ai_tool_exported",
        DEFAULT_AIDP_JAR_DIR,
        True,
        report,
    )

    assert mapping == {uri: fallback}
    assert failed is False
    assert report.files[-1].status == "SKIPPED_EXISTS"
    assert report.files[-1].aidp_path == fallback
    assert report.summary()["files_failed"] == 0
    assert report.summary()["warnings"] == 1
    assert "JAR upload errored; fallback used" in capsys.readouterr().out


def test_jar_upload_error_rejects_same_name_with_different_checksum():
    uri = "/Workspace/Databricks Jars/helperlib-1.6.1.jar"
    fallback = f"{DEFAULT_AIDP_JAR_DIR}/helperlib-1.6.1.jar"
    graph = Graph(files={
        uri: FileNode(dbx_source=uri, kind="jar", content=b"dbx-jar-bytes")
    })

    with pytest.raises(RuntimeError, match="required jar upload failed"):
        phase2_upload_files(
            _UploadFailsAidp(
                existing={fallback}, contents={fallback: b"different-jar-bytes"}
            ),
            graph,
            "/Workspace/ai_tool_exported",
            DEFAULT_AIDP_JAR_DIR,
            True,
            _report(),
            strict_jars=True,
        )


def test_workspace_download_stream_enforces_byte_limit(monkeypatch):
    metadata = SimpleNamespace(
        status_code=200,
        text="",
        json=lambda: {"parUrl": "https://object.example/file"},
    )

    class StreamResponse:
        status_code = 200
        text = ""
        headers = {}

        @staticmethod
        def iter_content(chunk_size):
            assert chunk_size == 1024 * 1024
            yield b"abc"
            yield b"def"

    monkeypatch.setattr(aidp_client.requests, "post", lambda *a, **k: metadata)
    monkeypatch.setattr(
        aidp_client.requests,
        "get",
        lambda *a, **k: StreamResponse(),
    )
    client = AidpClient(
        object(),
        AidpConfig(
            "ocid1.aidataplatform.oc1.us-phoenix-1.example",
            "workspace",
            "/Workspace/export",
        ),
    )

    with pytest.raises(AidpApiError, match="exceeds 5 byte limit"):
        client.download_file("/Workspace/jars/large.jar", max_bytes=5)
