import io
import hashlib
import json
import zipfile
from types import SimpleNamespace

import pytest

from dbx_export.__main__ import (
    _attach_context_jars,
    _context_jar_install_items,
    _install_context_jars_on_cluster,
    build_arg_parser,
    phase3_create_clusters_and_workflows,
    phase2_upload_files,
)
from dbx_export.aidp_client import AidpApiError
from dbx_export.dbx_client import ExportedFile
from dbx_export.dependency_resolver import (
    DependencyResolver,
    FileNode,
    Graph,
    JobNode,
    NotebookNode,
)
from dbx_export.jar_discovery import (
    discover_notebook_jars,
    scan_source_for_jvm_references,
)
from dbx_export.report import Report
import dbx_export.jar_discovery as jar_discovery


def _jar_bytes(*class_names):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        for class_name in class_names:
            archive.writestr(class_name.replace(".", "/") + ".class", b"class")
    return output.getvalue()


class _FakeAidp:
    def __init__(self, jars):
        self.jars = jars

    def list_workspace_objects(self, path):
        return [
            {"path": jar_path, "type": "FILE"}
            for jar_path in sorted(self.jars)
            if jar_path.startswith(path.rstrip("/") + "/")
        ]

    def download_file(self, path):
        return self.jars[path]


def _graph(source, language="scala"):
    job_id = "123"
    context = f"job:{job_id}/job_cluster:scoring"
    graph = Graph(
        jobs={
            job_id: JobNode(
                dbx_job_id=job_id,
                depth=0,
                job_def={
                    "settings": {
                        "tasks": [{
                            "task_key": "score",
                            "job_cluster_key": "scoring",
                            "notebook_task": {"notebook_path": "/Users/score"},
                        }],
                        "job_clusters": [{
                            "job_cluster_key": "scoring",
                            "new_cluster": {},
                        }],
                    }
                },
            )
        },
        notebooks={
            "/Users/score": NotebookNode(
                dbx_path="/Users/score",
                role="task",
                referenced_by=[f"job:{job_id}/task:score"],
                content=b"{}",
                nb_dict={
                    "cells": [{"cell_type": "code", "source": source}],
                    "metadata": {"language_info": {"name": language}},
                },
            )
        },
    )
    return graph, context


def test_scanner_finds_customer_jvm_references_and_excludes_platform_classes():
    refs = scan_source_for_jvm_references(
        """
        import com.example.datascience.helperlib._
        import com.example.examplepkg.exampledecryptor.ExampleEncryptor
        import org.apache.spark.sql.SparkSession
        import org.apache.commons.io.IOUtils
        import org.json._
        import spark.implicits._
        val klass = Class.forName("com.example.crypto.SecretCodec")
        value = spark._jvm.com.example.bridge.EntryPoint
        """,
        language="scala",
    )

    assert ("com.example.datascience.helperlib", "package") in refs
    assert ("com.example.examplepkg.exampledecryptor.ExampleEncryptor", "class") in refs
    assert ("com.example.crypto.SecretCodec", "class") in refs
    assert ("com.example.bridge.EntryPoint", "class") in refs
    assert ("org.apache.commons.io.IOUtils", "class") in refs
    assert ("org.json", "package") in refs
    assert not any(ref.startswith("org.apache.spark") for ref, _ in refs)
    assert not any(ref.startswith("spark.") for ref, _ in refs)


def test_scanner_does_not_treat_python_imports_as_jar_dependencies():
    source = """
        import matplotlib.pyplot as plt
        import pyspark.sql.functions as F
        from pyspark.sql.functions import col
        """
    refs = scan_source_for_jvm_references(source, language="python")

    assert refs == set()
    # Older workspace exports may omit notebook language metadata. Their
    # ordinary Python namespaces must remain non-JAR references too.
    assert scan_source_for_jvm_references(source) == set()


def test_unknown_language_does_not_infer_jvm_dependency_from_import_alone():
    source = """
        import org.json
        import com.example.Client
        import com.example.client
        import io.customer.module
        """

    assert scan_source_for_jvm_references(source) == set()


def test_python_cell_still_discovers_explicit_jvm_bridge_reference():
    refs = scan_source_for_jvm_references(
        "value = spark._jvm.com.example.bridge.EntryPoint",
        language="python",
    )

    assert refs == {("com.example.bridge.EntryPoint", "class")}


def test_python_java_import_is_explicit_jvm_evidence():
    refs = scan_source_for_jvm_references(
        'java_import(spark._jvm, "org.apache.commons.io.IOUtils")',
        language="python",
    )

    assert refs == {("org.apache.commons.io.IOUtils", "class")}


def test_scala_cell_magic_overrides_python_notebook_language():
    refs = scan_source_for_jvm_references(
        "%scala\nimport com.example.feature.Entry",
        language="python",
    )

    assert refs == {("com.example.feature.Entry", "class")}


def test_discovery_uses_notebook_language_for_python_imports():
    fallback = "/Workspace/migration-dependencies/jars"
    graph, _ = _graph(
        """
        import matplotlib.pyplot as plt
        import pyspark.sql.functions as F
        """,
        language="python",
    )

    result = discover_notebook_jars(_FakeAidp({}), graph, fallback)

    assert result.resolutions == []
    assert result.blockers == []


def test_discovery_prefers_fixed_jar_registers_matches_and_reports_missing(tmp_path):
    fallback = "/Workspace/migration-dependencies/jars"
    old_helperlib = f"{fallback}/helperlib_1.6.1_assembly.jar"
    fixed_helperlib = f"{fallback}/helperlib-assembly-1.6.1-fixed.jar"
    decryptor = f"{fallback}/example-decryptor-1.0-SNAPSHOT-all.jar"
    aidp = _FakeAidp({
        old_helperlib: _jar_bytes("com.example.datascience.helperlib.package$"),
        fixed_helperlib: _jar_bytes("com.example.datascience.helperlib.package$"),
        decryptor: _jar_bytes(
            "com.example.examplepkg.exampledecryptor.ExampleEncryptor"
        ),
    })
    graph, context = _graph(
        """
        import com.example.datascience.helperlib._
        import com.example.examplepkg.exampledecryptor.ExampleEncryptor
        import com.example.missing.NotInFallback
        """
    )

    result = discover_notebook_jars(aidp, graph, fallback)

    by_ref = {item.reference: item for item in result.resolutions}
    assert by_ref["com.example.datascience.helperlib"].status == "MATCHED"
    assert by_ref["com.example.datascience.helperlib"].selected_jar == fixed_helperlib
    assert by_ref[
        "com.example.examplepkg.exampledecryptor.ExampleEncryptor"
    ].selected_jar == decryptor
    assert by_ref["com.example.missing.NotInFallback"].status == "MISSING"
    assert len(result.blockers) == 1

    generated = [
        node for uri, node in graph.files.items()
        if uri.startswith("/dbx_export_generated/jar_fallback/")
    ]
    assert len(generated) == 2
    assert all(context in node.referenced_by for node in generated)
    assert {node.aidp_fallback_path for node in generated} == {
        fixed_helperlib,
        decryptor,
    }

    report_path = result.write(str(tmp_path), "123")
    report = json.loads(tmp_path.joinpath("dbx_job_123_jar_discovery.json").read_text())
    assert report_path.endswith("dbx_job_123_jar_discovery.json")
    assert report["summary"]["missing"] == 1
    assert report["summary"]["matched_jars"] == 2


def test_discovery_reports_ambiguous_when_no_reviewed_fixed_candidate_exists():
    fallback = "/Workspace/migration-dependencies/jars"
    aidp = _FakeAidp({
        f"{fallback}/feature-1.0.jar": _jar_bytes("com.example.feature.Entry"),
        f"{fallback}/feature-2.0.jar": _jar_bytes("com.example.feature.Entry"),
    })
    graph, _ = _graph("import com.example.feature.Entry")

    result = discover_notebook_jars(aidp, graph, fallback)

    assert len(result.resolutions) == 1
    assert result.resolutions[0].status == "AMBIGUOUS"
    assert len(result.resolutions[0].candidates) == 2


def test_org_json_ambiguity_is_reported_without_arbitrary_selection():
    fallback = "/Workspace/migration-dependencies/jars"
    aidp = _FakeAidp({
        f"{fallback}/fat-a.jar": _jar_bytes("org.json.JSONObject"),
        f"{fallback}/fat-b.jar": _jar_bytes("org.json.JSONObject"),
    })
    graph, _ = _graph("import org.json._")

    result = discover_notebook_jars(aidp, graph, fallback)

    assert result.allow_unresolved is True
    assert len(result.resolutions) == 1
    assert result.resolutions[0].status == "AMBIGUOUS"
    assert result.resolutions[0].selected_jar is None
    assert not any(
        uri.startswith("/dbx_export_generated/jar_fallback/")
        for uri in graph.files
    )


def test_fallback_jar_index_respects_count_cap(monkeypatch):
    fallback = "/Workspace/migration-dependencies/jars"
    aidp = _FakeAidp({
        f"{fallback}/a.jar": _jar_bytes("com.example.a.Entry"),
        f"{fallback}/b.jar": _jar_bytes("com.example.b.Entry"),
    })
    graph, _ = _graph("import com.example.b.Entry")
    monkeypatch.setattr(jar_discovery, "MAX_FALLBACK_JARS", 1)

    result = discover_notebook_jars(aidp, graph, fallback)

    assert result.resolutions[0].status == "MISSING"
    assert any("JAR count" in error for error in result.index_errors)


def test_fallback_jar_index_rejects_oversized_jar(monkeypatch):
    fallback = "/Workspace/migration-dependencies/jars"
    aidp = _FakeAidp({f"{fallback}/large.jar": _jar_bytes("com.example.Entry")})
    graph, _ = _graph("import com.example.Entry")
    monkeypatch.setattr(jar_discovery, "MAX_JAR_BYTES", 8)

    result = discover_notebook_jars(aidp, graph, fallback)

    assert result.resolutions[0].status == "MISSING"
    assert any("byte per-file" in error for error in result.index_errors)


def _mounted_repository(monkeypatch, tmp_path, jars):
    """Expose test bytes only at the exact paths returned by the workspace API."""
    local_files = {}
    for index, (path, content) in enumerate(jars.items()):
        local = tmp_path / f"artifact-{index}.jar"
        local.write_bytes(content)
        local_files[path] = local

    def mounted_open(path, mode):
        return open(local_files[path] if path in local_files else path, mode)

    monkeypatch.setattr(jar_discovery, "_get_aidputils_from_kernel", lambda: object())
    monkeypatch.setattr(jar_discovery, "open", mounted_open, raising=False)


@pytest.mark.parametrize("failure", ["http413", "http500", "size_limit"])
def test_mounted_jar_fallback_resolves_registers_and_attaches(monkeypatch, tmp_path, failure):
    repository = "/Workspace/team/libs"
    path = f"{repository}/provider.jar"
    content = _jar_bytes(
        "com.amazonaws.regions.Regions",
        "com.amazonaws.services.s3.AmazonS3ClientBuilder",
        "com.amazonaws.services.s3.AmazonS3URI",
    )
    _mounted_repository(monkeypatch, tmp_path, {path: content})
    aidp = _FakeAidp({path: content})
    if failure == "size_limit":
        monkeypatch.setattr(jar_discovery, "MAX_JAR_BYTES", 8)
    else:
        def download_file(path, max_bytes=None):
            raise AidpApiError("par_get", 413 if failure == "http413" else 500, "download failed")
        aidp.download_file = download_file
    graph, context = _graph(
        "import com.amazonaws.regions.Regions\n"
        "import com.amazonaws.services.s3.{AmazonS3ClientBuilder, AmazonS3URI}"
    )

    result = discover_notebook_jars(aidp, graph, repository)

    assert {r.status for r in result.resolutions} == {"MATCHED"}
    assert result.index_errors == []
    assert result.blockers == []
    assert result.to_dict()["index_fallbacks"]
    assert "using mounted repository JAR" in result.index_fallbacks[0]
    assert len(result.index_fallbacks) == 1
    uri, node = next(iter(graph.files.items()))
    assert node.aidp_fallback_sha256 == hashlib.sha256(content).hexdigest()
    report = Report(run_id="test", dbx_root_job_id="123", export_output="/Workspace/exported", prefix="test")
    uri_to_aidp, failed = phase2_upload_files(
        aidp, graph, export_output="/Workspace/exported",
        lib_repository_dir=repository, overwrite=False, report=report,
    )
    assert not failed
    assert uri_to_aidp[uri] == path
    assert _context_jar_install_items(graph, context, uri_to_aidp) == [
        {"operation": "INSTALL", "type": "WORKSPACE_FILE", "path": path},
    ]


def test_successful_download_does_not_read_mounted_copy(monkeypatch, tmp_path):
    repository = "/Workspace/team/libs"
    path = f"{repository}/provider.jar"
    _mounted_repository(monkeypatch, tmp_path, {path: b"invalid local JAR"})
    graph, _ = _graph("import com.example.Entry")

    result = discover_notebook_jars(
        _FakeAidp({path: _jar_bytes("com.example.Entry")}), graph, repository,
    )

    assert result.resolutions[0].status == "MATCHED"
    assert result.index_fallbacks == []
    assert result.index_errors == []


@pytest.mark.parametrize("local_state", ["missing", "corrupt", "permission_denied", "outside_kernel", "outside_repository"])
def test_unusable_mounted_fallback_remains_reported(monkeypatch, tmp_path, local_state):
    repository = "/Workspace/team/libs"
    path = f"{repository}/provider.jar"
    content = _jar_bytes("com.example.Entry")
    _mounted_repository(monkeypatch, tmp_path, {path: content})
    if local_state == "missing":
        def local_open(path, mode):
            raise FileNotFoundError("mounted JAR missing")
        monkeypatch.setattr(jar_discovery, "open", local_open)
    elif local_state == "corrupt":
        _mounted_repository(monkeypatch, tmp_path, {path: b"not a ZIP"})
    elif local_state == "permission_denied":
        def local_open(path, mode):
            raise PermissionError("mounted JAR unreadable")
        monkeypatch.setattr(jar_discovery, "open", local_open)
    elif local_state == "outside_kernel":
        monkeypatch.setattr(jar_discovery, "_get_aidputils_from_kernel", lambda: None)
    elif local_state == "outside_repository":
        path = "/Workspace/team/private/provider.jar"
    aidp = _FakeAidp({path: content})
    aidp.list_workspace_objects = lambda root: [{"path": path}]
    def download_file(path, max_bytes=None):
        raise AidpApiError("par_get", 413, "too large")
    aidp.download_file = download_file
    graph, _ = _graph("import com.example.Entry")

    result = discover_notebook_jars(aidp, graph, repository)

    assert result.resolutions[0].status == "MISSING"
    assert result.index_errors
    assert graph.files == {}


def test_mounted_fallback_preserves_declared_jar_ownership(monkeypatch, tmp_path):
    repository = "/Workspace/team/libs"
    path = f"{repository}/provider.jar"
    content = _jar_bytes("com.example.Entry")
    _mounted_repository(monkeypatch, tmp_path, {path: content})
    monkeypatch.setattr(jar_discovery, "MAX_JAR_BYTES", 8)
    graph, context = _graph("import com.example.Entry")
    uri = "dbfs:/jars/provider.jar"
    graph.files[uri] = FileNode(
        dbx_source=uri, kind="jar", referenced_by=[context],
        aidp_repo_path=path, aidp_repo_provenance="aidp-first: exact basename",
    )

    result = discover_notebook_jars(_FakeAidp({path: content}), graph, repository)

    assert result.resolutions[0].status == "PRESENT"
    assert result.index_errors == []
    assert set(graph.files) == {uri}


def test_mounted_fallback_preserves_cumulative_budget(monkeypatch, tmp_path):
    repository = "/Workspace/team/libs"
    content = _jar_bytes("com.example.Entry")
    jars = {f"{repository}/a.jar": content, f"{repository}/b.jar": content}
    _mounted_repository(monkeypatch, tmp_path, jars)
    monkeypatch.setattr(jar_discovery, "MAX_JAR_BYTES", 8)
    monkeypatch.setattr(jar_discovery, "MAX_TOTAL_JAR_BYTES", len(content))
    graph, _ = _graph("import com.example.Entry")

    result = discover_notebook_jars(_FakeAidp(jars), graph, repository)

    assert result.resolutions[0].selected_jar == f"{repository}/a.jar"
    assert any("cumulative cap" in error for error in result.index_errors)


def test_mounted_jar_larger_than_total_budget_remains_unresolved(monkeypatch, tmp_path):
    repository = "/Workspace/team/libs"
    path = f"{repository}/large.jar"
    content = _jar_bytes("com.example.Entry")
    _mounted_repository(monkeypatch, tmp_path, {path: content})
    monkeypatch.setattr(jar_discovery, "MAX_JAR_BYTES", 8)
    monkeypatch.setattr(jar_discovery, "MAX_TOTAL_JAR_BYTES", len(content) - 1)
    graph, _ = _graph("import com.example.Entry")

    result = discover_notebook_jars(_FakeAidp({path: content}), graph, repository)

    assert result.resolutions[0].status == "MISSING"
    assert any("byte index budget" in error for error in result.index_errors)


def test_mounted_jar_candidates_keep_ambiguity(monkeypatch, tmp_path):
    repository = "/Workspace/team/libs"
    content = _jar_bytes("com.example.Entry")
    jars = {f"{repository}/a.jar": content, f"{repository}/b.jar": content}
    _mounted_repository(monkeypatch, tmp_path, jars)
    monkeypatch.setattr(jar_discovery, "MAX_JAR_BYTES", 8)
    graph, _ = _graph("import com.example.Entry")

    result = discover_notebook_jars(_FakeAidp(jars), graph, repository)

    assert result.resolutions[0].status == "AMBIGUOUS"
    assert result.resolutions[0].candidates == sorted(jars)
    assert result.index_errors == []
    assert graph.files == {}


def test_mounted_jar_disappearing_before_registration_remains_unresolved(monkeypatch, tmp_path):
    repository = "/Workspace/team/libs"
    path = f"{repository}/provider.jar"
    content = _jar_bytes("com.example.Entry")
    _mounted_repository(monkeypatch, tmp_path, {path: content})
    mounted_open = jar_discovery.open
    opened = []
    def disappearing_open(path, mode):
        if opened:
            raise FileNotFoundError("mounted JAR disappeared")
        opened.append(path)
        return mounted_open(path, mode)
    monkeypatch.setattr(jar_discovery, "open", disappearing_open)
    monkeypatch.setattr(jar_discovery, "MAX_JAR_BYTES", 8)
    graph, _ = _graph("import com.example.Entry")

    result = discover_notebook_jars(_FakeAidp({path: content}), graph, repository)

    assert result.resolutions[0].status == "MISSING"
    assert result.resolutions[0].candidates == [path]
    assert any("mounted JAR disappeared" in error for error in result.index_errors)
    assert graph.files == {}


def test_discovery_scans_transitive_dependency_and_reuses_source_declared_jar():
    fallback = "/Workspace/migration-dependencies/jars"
    source_uri = "dbfs:/FileStore/jars/customer.jar"
    graph, context = _graph("%run ./utils")
    graph.notebooks["/Users/utils"] = NotebookNode(
        dbx_path="/Users/utils",
        role="run_dep",
        referenced_by=["/Users/score"],
        content=b"{}",
        nb_dict={
            "cells": [{
                "cell_type": "code",
                "source": "import com.example.customer.SourceOnlyClass",
            }],
            "metadata": {"language_info": {"name": "scala"}},
        },
    )
    graph.files[source_uri] = FileNode(
        dbx_source=source_uri,
        kind="jar",
        referenced_by=["job:123/task:score"],
        content=_jar_bytes("com.example.customer.SourceOnlyClass"),
    )

    result = discover_notebook_jars(_FakeAidp({}), graph, fallback)

    assert len(result.resolutions) == 1
    resolution = result.resolutions[0]
    assert resolution.notebook == "/Users/utils"
    assert resolution.status == "PRESENT"
    assert resolution.selected_jar == source_uri
    assert context in graph.files[source_uri].referenced_by


def test_aidp_first_source_jar_wins_over_ambiguous_class_candidates():
    fallback = "/Workspace/migration-dependencies/libs"
    declared_uri = (
        "dbfs:/FileStore/jars/Production Jar/"
        "Example Decrypt UDF 1.0.2.jar"
    )
    declared_path = f"{fallback}/Spark_Decrypt_UDF_1.0.2_Final.jar"
    newer_path = f"{fallback}/Spark_Decrypt_UDF_1.0.6_Final.jar"
    class_name = "com.example.dp.UdfRegistrar"
    graph, context = _graph(f"import {class_name}")
    graph.files[declared_uri] = FileNode(
        dbx_source=declared_uri,
        kind="jar",
        referenced_by=["job:123/task:score"],
        aidp_repo_path=declared_path,
        aidp_repo_provenance=(
            "AIDP-first: exact basename "
            "Spark_Decrypt_UDF_1.0.2_Final.jar present in "
            "/Workspace/migration-dependencies/libs"
        ),
    )
    aidp = _FakeAidp({
        declared_path: _jar_bytes(class_name),
        newer_path: _jar_bytes(class_name),
    })

    result = discover_notebook_jars(aidp, graph, fallback)

    assert len(result.resolutions) == 1
    resolution = result.resolutions[0]
    assert resolution.status == "PRESENT"
    assert resolution.selected_jar == declared_path
    assert resolution.candidates == [declared_path]
    assert context in graph.files[declared_uri].referenced_by
    assert _context_jar_install_items(
        graph,
        context,
        {declared_uri: declared_path},
    ) == [{
        "operation": "INSTALL",
        "type": "WORKSPACE_FILE",
        "path": declared_path,
    }]
    assert not any(
        uri.startswith("/dbx_export_generated/jar_fallback/")
        for uri in graph.files
    )


def test_non_exact_aidp_first_substitution_does_not_override_ambiguity():
    fallback = "/Workspace/migration-dependencies/libs"
    declared_uri = "dbfs:/FileStore/jars/Spark_Decrypt_UDF_1.0.2_Final.jar"
    substituted_path = f"{fallback}/Spark_Decrypt_UDF_1.0.6_Final.jar"
    other_path = f"{fallback}/Spark_Decrypt_UDF_1.0.7_Final.jar"
    class_name = "com.example.dp.UdfRegistrar"
    graph, context = _graph(f"import {class_name}")
    graph.files[declared_uri] = FileNode(
        dbx_source=declared_uri,
        kind="jar",
        referenced_by=["job:123/task:score"],
        aidp_repo_path=substituted_path,
        aidp_repo_provenance=(
            "AIDP-first: Spark_Decrypt_UDF requested==1.0.2 not staged; "
            "using highest available 1.0.6"
        ),
    )
    aidp = _FakeAidp({
        substituted_path: _jar_bytes(class_name),
        other_path: _jar_bytes(class_name),
    })

    result = discover_notebook_jars(aidp, graph, fallback)

    assert len(result.resolutions) == 1
    resolution = result.resolutions[0]
    assert resolution.status == "AMBIGUOUS"
    assert resolution.selected_jar is None
    assert resolution.candidates == [substituted_path, other_path]
    assert context not in graph.files[declared_uri].referenced_by


def test_non_exact_aidp_first_substitution_is_matched_when_unambiguous():
    fallback = "/Workspace/migration-dependencies/libs"
    declared_uri = "dbfs:/FileStore/jars/udf-1.0.2.jar"
    substituted_path = f"{fallback}/udf-1.0.6.jar"
    class_name = "com.example.VersionSubstitutedProvider"
    graph, _ = _graph(f"import {class_name}")
    graph.files[declared_uri] = FileNode(
        dbx_source=declared_uri,
        kind="jar",
        referenced_by=["job:123/task:score"],
        aidp_repo_path=substituted_path,
        aidp_repo_provenance=(
            "AIDP-first: udf requested==1.0.2 not staged; "
            "using highest available 1.0.6"
        ),
    )

    result = discover_notebook_jars(
        _FakeAidp({substituted_path: _jar_bytes(class_name)}),
        graph,
        fallback,
    )

    resolution = result.resolutions[0]
    assert resolution.status == "MATCHED"
    assert resolution.selected_jar == substituted_path


def test_source_jar_declared_on_sibling_cluster_remains_present():
    fallback = "/Workspace/migration-dependencies/libs"
    source_uri = "dbfs:/FileStore/jars/provider.jar"
    class_name = "com.example.SharedProvider"
    job_id = "123"
    context_a = f"job:{job_id}/job_cluster:A"
    context_b = f"job:{job_id}/job_cluster:B"
    graph = Graph(
        jobs={
            job_id: JobNode(
                dbx_job_id=job_id,
                depth=0,
                job_def={"settings": {
                    "tasks": [
                        {
                            "task_key": "producer",
                            "job_cluster_key": "A",
                            "notebook_task": {"notebook_path": "/Users/producer"},
                        },
                        {
                            "task_key": "consumer",
                            "job_cluster_key": "B",
                            "notebook_task": {"notebook_path": "/Users/consumer"},
                        },
                    ],
                    "job_clusters": [
                        {"job_cluster_key": "A", "new_cluster": {}},
                        {"job_cluster_key": "B", "new_cluster": {}},
                    ],
                }},
            )
        },
        notebooks={
            "/Users/producer": NotebookNode(
                dbx_path="/Users/producer",
                role="task",
                referenced_by=[f"job:{job_id}/task:producer"],
                content=b"{}",
                nb_dict={"cells": [], "metadata": {"language_info": {"name": "scala"}}},
            ),
            "/Users/consumer": NotebookNode(
                dbx_path="/Users/consumer",
                role="task",
                referenced_by=[f"job:{job_id}/task:consumer"],
                content=b"{}",
                nb_dict={
                    "cells": [{"cell_type": "code", "source": f"import {class_name}"}],
                    "metadata": {"language_info": {"name": "scala"}},
                },
            ),
        },
        files={
            source_uri: FileNode(
                dbx_source=source_uri,
                kind="jar",
                referenced_by=[f"job:{job_id}/task:producer", context_a],
                content=_jar_bytes(class_name),
            )
        },
    )

    result = discover_notebook_jars(_FakeAidp({}), graph, fallback)

    resolution = result.resolutions[0]
    assert resolution.cluster_context == context_b
    assert resolution.status == "PRESENT"
    assert resolution.selected_jar == source_uri
    assert resolution.detail == "indexed source JAR from another cluster"
    assert context_b in graph.files[source_uri].referenced_by
    assert result.blockers == []


def test_init_script_source_jar_inherits_script_cluster_context():
    fallback = "/Workspace/migration-dependencies/libs"
    jar_uri = "dbfs:/jars/lib.jar"
    script_uri = "dbfs:/init/copy.sh"
    class_name = "com.example.InitProvider"
    graph, context = _graph(f"import {class_name}")
    graph.files[script_uri] = FileNode(
        dbx_source=script_uri,
        kind="init_script",
        referenced_by=[context],
        content=b"cp /dbfs/jars/lib.jar /databricks/jars/lib.jar",
    )
    graph.files[jar_uri] = FileNode(
        dbx_source=jar_uri,
        kind="jar",
        referenced_by=[f"init_script:{script_uri}"],
        content=_jar_bytes(class_name),
    )

    result = discover_notebook_jars(_FakeAidp({}), graph, fallback)

    resolution = result.resolutions[0]
    assert resolution.status == "PRESENT"
    assert resolution.selected_jar == jar_uri
    assert context in graph.files[jar_uri].referenced_by
    assert result.blockers == []


def test_init_script_source_jar_inherits_context_without_notebook_jvm_reference():
    """Init-script JARs must install even when notebook scanning finds no JVM class."""
    fallback = "/Workspace/migration-dependencies/libs"
    jar_uri = "dbfs:/jars/configured-provider.jar"
    script_uri = "dbfs:/init/copy.sh"
    graph, context = _graph("val value = 1")
    graph.files[script_uri] = FileNode(
        dbx_source=script_uri,
        kind="init_script",
        referenced_by=[context],
        content=b"cp /dbfs/jars/configured-provider.jar /databricks/jars/provider.jar",
    )
    graph.files[jar_uri] = FileNode(
        dbx_source=jar_uri,
        kind="jar",
        referenced_by=[f"init_script:{script_uri}"],
        content=_jar_bytes("com.example.Provider"),
    )

    result = discover_notebook_jars(_FakeAidp({}), graph, fallback)
    conversion = SimpleNamespace(library_refs=[])

    assert result.resolutions == []
    assert context in graph.files[jar_uri].referenced_by
    assert _attach_context_jars(conversion, graph, context) == [jar_uri]
    assert conversion.library_refs == [{"jar": jar_uri}]


def test_no_jvm_init_script_jars_remain_scoped_to_their_execution_clusters():
    """A child init-script JAR must not be attached to an unrelated root cluster."""
    root_context = "job:root/job_cluster:root_cluster"
    child_context = "job:child/job_cluster:child_cluster"
    root_script = "dbfs:/init/root.sh"
    child_script = "dbfs:/init/child.sh"
    root_jar = "dbfs:/jars/root.jar"
    child_jar = "dbfs:/jars/child.jar"
    graph = Graph(
        jobs={
            "root": JobNode(
                dbx_job_id="root", depth=0,
                job_def={"settings": {"tasks": [], "job_clusters": []}},
            ),
            "child": JobNode(
                dbx_job_id="child", depth=1,
                job_def={"settings": {"tasks": [], "job_clusters": []}},
            ),
        },
        files={
            root_script: FileNode(
                dbx_source=root_script, kind="init_script",
                referenced_by=[root_context], content=b"# root",
            ),
            child_script: FileNode(
                dbx_source=child_script, kind="init_script",
                referenced_by=[child_context], content=b"# child",
            ),
            root_jar: FileNode(
                dbx_source=root_jar, kind="jar",
                referenced_by=[f"init_script:{root_script}"],
                content=_jar_bytes("com.example.RootProvider"),
            ),
            child_jar: FileNode(
                dbx_source=child_jar, kind="jar",
                referenced_by=[f"init_script:{child_script}"],
                content=_jar_bytes("com.example.ChildProvider"),
            ),
        },
    )

    # No notebook JVM references: ownership propagation must still run.
    result = discover_notebook_jars(_FakeAidp({}), graph, "/Workspace/libs")

    assert result.resolutions == []
    assert root_context in graph.files[root_jar].referenced_by
    assert child_context in graph.files[child_jar].referenced_by
    assert child_context not in graph.files[root_jar].referenced_by
    assert root_context not in graph.files[child_jar].referenced_by

    root_conversion = SimpleNamespace(library_refs=[])
    child_conversion = SimpleNamespace(library_refs=[])
    assert _attach_context_jars(root_conversion, graph, root_context) == [root_jar]
    assert _attach_context_jars(child_conversion, graph, child_context) == [child_jar]


def test_phase3_fresh_cluster_attaches_init_script_wheel_only_to_owner(monkeypatch):
    """The real fresh-cluster branch must scope init-script wheels by context."""
    import dbx_export.__main__ as exporter

    root_context = "job:root/job_cluster:root_cluster"
    child_context = "job:child/job_cluster:child_cluster"
    child_script = "dbfs:/init/child.sh"
    child_wheel = "dbfs:/libs/helper.whl"

    def job(job_id, name, cluster_key):
        return JobNode(
            dbx_job_id=job_id,
            depth=0,
            job_def={"settings": {
                "name": name,
                "job_clusters": [{
                    "job_cluster_key": cluster_key,
                    "new_cluster": {},
                }],
                "tasks": [],
            }},
        )

    graph = Graph(
        jobs={
            "root": job("root", "root_job", "root_cluster"),
            "child": job("child", "child_job", "child_cluster"),
        },
        files={
            child_script: FileNode(
                dbx_source=child_script,
                kind="init_script",
                referenced_by=[child_context],
                content=b"# child init script",
            ),
            child_wheel: FileNode(
                dbx_source=child_wheel,
                kind="whl",
                referenced_by=[f"init_script:{child_script}"],
                content=b"wheel",
            ),
        },
    )

    # This is the production discovery call that supplies the ownership
    # context consumed by Phase 3. There are deliberately no JVM references.
    discover_notebook_jars(_FakeAidp({}), graph, "/Workspace/libs")
    assert child_context in graph.files[child_wheel].referenced_by
    assert root_context not in graph.files[child_wheel].referenced_by

    class FakeAidp:
        def __init__(self):
            self.installs = []

        def find_cluster_by_name(self, _name):
            return None

        def find_workflow_by_name(self, _name):
            return {"key": "already-exists"}

        def install_cluster_libraries(self, cluster_key, items):
            self.installs.append((cluster_key, items))

        def list_clusters(self):
            return []

    aidp = FakeAidp()

    def fake_conversion(**_kwargs):
        return SimpleNamespace(
            aidp_payload={},
            init_script_refs=[],
            library_refs=[],
            shape_mapping_log_lines=[],
            driver_shape_res=None,
            worker_shape_res=None,
            translated_conf=[],
            dropped_env=[],
            dropped_conf=[],
        )

    monkeypatch.setattr(exporter, "convert_dbx_cluster", fake_conversion)
    monkeypatch.setattr(
        exporter,
        "_create_cluster_with_intel_fallback",
        lambda *_args, **_kwargs: {"key": "created-cluster"},
    )
    monkeypatch.setattr(exporter, "_attach_only_consolidated_requirements", lambda *_a: None)
    monkeypatch.setattr(exporter, "_attach_context_jars", lambda *_a: [])
    monkeypatch.setattr(
        exporter,
        "build_library_install_items",
        lambda refs, mapping: [
            {"operation": "INSTALL", "type": "WORKSPACE_FILE", "path": mapping[ref["whl"]]}
            for ref in refs if ref.get("whl")
        ],
    )

    report = Report(
        run_id="wheel-owner",
        dbx_root_job_id="root",
        export_output="/Workspace/export",
        prefix="SP_AI",
    )
    phase3_create_clusters_and_workflows(
        aidp=aidp,
        dbx=SimpleNamespace(),
        graph=graph,
        nb_target_paths={},
        uri_to_aidp={child_wheel: "/Workspace/libs/helper.whl"},
        export_output="/Workspace/export",
        prefix="SP_AI",
        overwrite_workflow=False,
        cluster_override_key=None,
        report=report,
    )

    wheel_installs = [
        (cluster, items)
        for cluster, items in aidp.installs
        if any(item.get("path") == "/Workspace/libs/helper.whl" for item in items)
    ]
    assert wheel_installs == [("created-cluster", [{
        "operation": "INSTALL",
        "type": "WORKSPACE_FILE",
        "path": "/Workspace/libs/helper.whl",
    }])]


def test_exact_staged_jar_is_indexed_outside_fallback_directory():
    fallback = "/Workspace/migration-dependencies/libs"
    source_uri = "dbfs:/FileStore/jars/udf-1.0.2.jar"
    staged_path = "/Workspace/migration-dependencies/jars/udf-1.0.2.jar"
    newer_path = f"{fallback}/udf-1.0.6.jar"
    class_name = "com.example.ExactProvider"
    graph, _ = _graph(f"import {class_name}")
    graph.files[source_uri] = FileNode(
        dbx_source=source_uri,
        kind="jar",
        referenced_by=["job:123/task:score"],
        aidp_repo_path=staged_path,
        aidp_repo_provenance=(
            "AIDP-first: exact basename udf-1.0.2.jar present in "
            "/Workspace/migration-dependencies/jars"
        ),
    )
    aidp = _FakeAidp({
        staged_path: _jar_bytes(class_name),
        newer_path: _jar_bytes(class_name),
    })

    result = discover_notebook_jars(aidp, graph, fallback)

    resolution = result.resolutions[0]
    assert resolution.status == "PRESENT"
    assert resolution.selected_jar == staged_path
    assert resolution.candidates == [staged_path]


def test_exact_staged_jar_wins_over_codeclared_fetched_assembly():
    fallback = "/Workspace/migration-dependencies/libs"
    exact_uri = "dbfs:/jars/udf-1.0.2.jar"
    exact_path = f"{fallback}/udf-1.0.2.jar"
    assembly_uri = "dbfs:/jars/customer-all.jar"
    class_name = "com.example.CoDeclaredProvider"
    graph, context = _graph(f"import {class_name}")
    graph.files[exact_uri] = FileNode(
        dbx_source=exact_uri,
        kind="jar",
        referenced_by=["job:123/task:score", context],
        aidp_repo_path=exact_path,
        aidp_repo_provenance=(
            "AIDP-first: exact basename udf-1.0.2.jar present in "
            "/Workspace/migration-dependencies/libs"
        ),
    )
    graph.files[assembly_uri] = FileNode(
        dbx_source=assembly_uri,
        kind="jar",
        referenced_by=["job:123/task:score", context],
        content=_jar_bytes(class_name),
    )

    result = discover_notebook_jars(
        _FakeAidp({exact_path: _jar_bytes(class_name)}), graph, fallback
    )

    resolution = result.resolutions[0]
    assert resolution.status == "PRESENT"
    assert resolution.selected_jar == exact_path
    assert resolution.detail == "preferred the exact AIDP-first declared artifact"


def test_task_level_jar_is_owned_by_its_job_cluster_context():
    job_id = "123"
    jar_uri = "dbfs:/FileStore/jars/customer.jar"

    class FakeDbx:
        @staticmethod
        def parse_github_owner_repo(_url):
            return None, None

        def get_job(self, requested_job_id):
            assert requested_job_id == job_id
            return {
                "job_id": int(job_id),
                "settings": {
                    "tasks": [{
                        "task_key": "score",
                        "job_cluster_key": "scoring",
                        "notebook_task": {"notebook_path": "/Users/score"},
                        "libraries": [{"jar": jar_uri}, {"jar": jar_uri}],
                    }],
                    "job_clusters": [{
                        "job_cluster_key": "scoring",
                        "new_cluster": {},
                    }],
                },
            }

        def export_notebook(self, path):
            assert path == "/Users/score"
            return ExportedFile(
                content=b"{}",
                is_notebook=True,
                extension=".ipynb",
            )

        def list_workspace_folder(self, _path):
            return []

    graph = DependencyResolver(FakeDbx()).resolve(job_id)

    assert graph.files[jar_uri].referenced_by == [
        "job:123/task:score",
        "job:123/job_cluster:scoring",
    ]


def test_root_job_nested_cluster_artifacts_have_execution_ownership():
    job_id = "123"
    task_jar = "dbfs:/jars/task-new-cluster.jar"
    task_init = "dbfs:/init/task-new-cluster.sh"
    existing_jar = "dbfs:/jars/existing-cluster.jar"
    job_jar = "dbfs:/jars/job-cluster.jar"
    job_init = "dbfs:/init/job-cluster.sh"

    class FakeDbx:
        @staticmethod
        def parse_github_owner_repo(_url):
            return None, None

        def get_job(self, requested_job_id):
            assert requested_job_id == job_id
            return {
                "job_id": int(job_id),
                "settings": {
                    "tasks": [{
                        "task_key": "task_cluster_task",
                        "notebook_task": {"notebook_path": "/Users/task"},
                        "new_cluster": {
                            "libraries": [{"jar": task_jar}],
                            "init_scripts": [{
                                "workspace": {"destination": task_init},
                            }],
                        },
                    }, {
                        "task_key": "existing_cluster_task",
                        "existing_cluster_id": "cluster-123",
                        "libraries": [{"jar": existing_jar}],
                    }],
                    "job_clusters": [{
                        "job_cluster_key": "shared",
                        "new_cluster": {
                            "libraries": [{"jar": job_jar}],
                            "init_scripts": [{
                                "workspace": {"destination": job_init},
                            }],
                        },
                    }],
                },
            }

        def export_notebook(self, path):
            assert path == "/Users/task"
            return ExportedFile(content=b"{}", is_notebook=True, extension=".ipynb")

        def list_workspace_folder(self, _path):
            return []

    graph = DependencyResolver(FakeDbx()).resolve(job_id)

    task_refs = [
        "job:123/task:task_cluster_task",
        "job:123/task_cluster:task_cluster_task",
    ]
    assert graph.files[task_jar].referenced_by == task_refs
    assert graph.files[task_init].referenced_by == task_refs
    assert graph.files[existing_jar].referenced_by == [
        "job:123/task:existing_cluster_task",
        "existing_cluster:cluster-123",
    ]
    assert graph.files[job_jar].referenced_by == [
        "job:123/job_cluster:shared",
    ]
    assert graph.files[job_init].referenced_by == [
        "job:123/job_cluster:shared",
    ]


def test_notebook_discovered_child_job_artifacts_get_cluster_ownership():
    child_id = "456"
    jar_uri = "dbfs:/FileStore/jars/child.jar"
    task_cluster_jar = "dbfs:/FileStore/jars/child-task-cluster.jar"
    existing_cluster_jar = "dbfs:/FileStore/jars/child-existing-cluster.jar"
    job_cluster_jar = "dbfs:/FileStore/jars/child-job-cluster.jar"

    class FakeDbx:
        @staticmethod
        def parse_github_owner_repo(_url):
            return None, None

        def get_job(self, requested_job_id):
            assert requested_job_id == child_id
            return {
                "job_id": int(child_id),
                "settings": {
                    "tasks": [{
                        "task_key": "child_score",
                        "job_cluster_key": "child_cluster",
                        "notebook_task": {"notebook_path": "/Users/child_score"},
                        "libraries": [{"jar": jar_uri}],
                    }, {
                        "task_key": "child_task_cluster",
                        "new_cluster": {
                            "libraries": [{"jar": task_cluster_jar}],
                        },
                    }, {
                        "task_key": "child_existing_cluster",
                        "existing_cluster_id": "cluster-456",
                        "libraries": [{"jar": existing_cluster_jar}],
                    }],
                    "job_clusters": [{
                        "job_cluster_key": "child_cluster",
                        "new_cluster": {
                            "libraries": [{"jar": job_cluster_jar}],
                        },
                    }],
                },
            }

        def export_notebook(self, path):
            assert path == "/Users/child_score"
            return ExportedFile(content=b"{}", is_notebook=True, extension=".ipynb")

        def list_workspace_folder(self, _path):
            return []

    resolver = DependencyResolver(FakeDbx())
    graph = Graph()
    resolver._enqueue_child_job(graph, child_id, "/Users/parent")

    assert graph.files[jar_uri].referenced_by == [
        "job:456/task:child_score",
        "job:456/job_cluster:child_cluster",
    ]
    assert graph.files[task_cluster_jar].referenced_by == [
        "job:456/task:child_task_cluster",
        "job:456/task_cluster:child_task_cluster",
    ]
    assert graph.files[existing_cluster_jar].referenced_by == [
        "job:456/task:child_existing_cluster",
        "existing_cluster:cluster-456",
    ]
    assert graph.files[job_cluster_jar].referenced_by == [
        "job:456/job_cluster:child_cluster",
    ]


def test_resolved_context_jars_flow_to_cluster_install_items():
    context = "job:123/job_cluster:scoring"
    uri = "/dbx_export_generated/jar_fallback/abc_helperlib.jar"
    aidp_path = "/Workspace/migration-dependencies/jars/helperlib-fixed.jar"
    graph = Graph(files={
        uri: FileNode(
            dbx_source=uri,
            kind="jar",
            referenced_by=[context],
            fetch_error="not_declared_in_databricks",
            aidp_fallback_path=aidp_path,
        )
    })
    conversion = SimpleNamespace(library_refs=[])

    attached = _attach_context_jars(conversion, graph, context)
    items = _context_jar_install_items(graph, context, {uri: aidp_path})

    assert attached == [uri]
    assert conversion.library_refs == [{"jar": uri}]
    assert items == [{
        "operation": "INSTALL",
        "type": "WORKSPACE_FILE",
        "path": aidp_path,
    }]


def test_reused_cluster_jar_attach_soft_fails_and_reports_warning():
    class FailingAidp:
        def install_cluster_libraries(self, cluster_key, items):
            raise AidpApiError("install_cluster_libraries", 500, "failed")

    context = "job:123/job_cluster:scoring"
    uri = "/dbx_export_generated/jar_fallback/abc.jar"
    graph = Graph(files={
        uri: FileNode(
            dbx_source=uri,
            kind="jar",
            referenced_by=[context],
            aidp_fallback_path="/Workspace/migration-dependencies/jars/abc.jar",
        )
    })
    messages = []
    report = SimpleNamespace(errors=[])

    installed = _install_context_jars_on_cluster(
        FailingAidp(), "cluster", graph, context,
        {uri: "/Workspace/migration-dependencies/jars/abc.jar"},
        tprint=messages.append, report=report,
    )

    assert installed == 0
    assert any("[WARN]" in message for message in messages)
    assert report.errors[0].startswith("WARN jar-library-install:")


def test_unresolved_jars_warn_by_default_and_strict_mode_is_opt_in():
    parser = build_arg_parser()

    assert parser.parse_args([]).allow_unresolved_jars is True
    assert parser.parse_args(["--allow-unresolved-jars"]).allow_unresolved_jars is True
    assert parser.parse_args(["--strict-unresolved-jars"]).allow_unresolved_jars is False


def test_jar_discovery_report_records_warning_enforcement():
    fallback = "/Workspace/migration-dependencies/jars"
    graph, _ = _graph("import com.example.missing.NotInFallback")

    result = discover_notebook_jars(_FakeAidp({}), graph, fallback)
    result.allow_unresolved = True
    report = result.to_dict()

    assert report["allow_unresolved"] is True
    assert report["enforcement"] == "warn"
    assert report["summary"]["missing"] == 1
