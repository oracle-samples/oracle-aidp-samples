"""REF-264 regression for the retired automatic dbutils.fs.ls -> OciFs shortcut.

Drive real parent/inline-child loops and call_fix with scripted Scala/model I/O.
This validates orchestration, saved code and outputs, NOT live Volume access or
model quality. The old tests' bounded retries, wrapped failures, fresh outputs,
reconnect/repeated-cell and inert-source lessons are exercised at these boundaries.
"""
import asyncio
import contextvars
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import agent_migrate as am
import context_tools
import job_migrate as jm
from test_child_exit_conversion import _ExitKernel
from test_conditional_exit_below_execute import _patch_common, _saved_sources
from test_eval_verdict_gate import _notebook

ERROR = ("Command ID 1234-abcd failed with java.lang.RuntimeException:\n"
         "<console>:22: error: not found: value dbutils")
VOLUME = "/Volumes/catalog/schema/artifacts/output"
OCI = "oci://fixture-bucket@fixture-namespace/output"
PATHS = ["dbfs:/FileStore/output", "/dbfs/FileStore/output", VOLUME, OCI]
CONFIG = "/Workspace/fixture/oci_config"


def _source(path, *, mixed=False):
    additional = f'\n  val other = dbutils.fs.ls("{OCI}")' if mixed else ""
    return f'''%scala
val outputPath = "{path}"
// Preserve dbutils.fs.ls(outputPath) in comments and strings.
val helpText = "dbutils.fs.ls(outputPath)"
val domain = "https://example.cloud.databricks.com"
try {{
  val files = dbutils.fs.ls(outputPath){additional}
  files.foreach(f => println(domain + "/files/output/" + f.name))
}} catch {{ case _: Exception => println("not yet written") }}
'''


def _repair(source, path, *, mixed=False):
    """Syntactically valid native Scala fixture; filesystem responses are mocked."""
    target = path if path.startswith("oci://") else VOLUME
    prefix = (jm._oci_fs_ls_helper_source(CONFIG)
              if path.startswith("oci://") or mixed else "%scala\n")
    original = "\n".join("// " + line for line in source.splitlines()[1:])
    listing = ('OciFs.ls(outputPath).foreach(f => println(f.path))'
               if path.startswith("oci://") else
               '''val files = java.nio.file.Files.list(java.nio.file.Paths.get(outputPath))
try { files.forEach(p => println(p.toString)) } finally { files.close() }''')
    if mixed:
        listing += f'\nOciFs.ls("{OCI}").foreach(f => println(f.path))'
    return prefix + "\n" + original + f'''
// Oracle tool modification: use the verified filesystem and print its paths.
val outputPath = "{target}"
{listing}
println("UNRESOLVED: clickable downloads require an approved AIDP integration")
'''


def _active(code):
    return "\n".join(line for line in code.splitlines()
                     if not line.lstrip().startswith("//"))


def _output(text):
    return {"status": "ok", "outputs": [
        {"type": "stream", "name": "stdout", "text": text}]}


class ListingKernel(_ExitKernel):
    def __init__(self, repairs=(), failures=()):
        super().__init__(fire=False)
        self.repairs = {code.strip() for code in repairs}
        self.failures = list(failures)
        self.listings = []
        self.bootstrap_calls = 0

    async def execute(self, code, **kwargs):
        if code.lstrip().startswith("%scala"):
            active = _active(code)
            if "val outputPath" in active:
                self.executed.append(code)
                self.listings.append(code)
                if "val files = dbutils.fs.ls(" in active:
                    return _output(ERROR)
                if code.strip() in self.repairs:
                    target = OCI if "OciFs.ls(outputPath)" in active else VOLUME
                    return _output(self.failures.pop(0) if self.failures else
                                   target + "/part-00000.parquet\n"
                                   "UNRESOLVED: clickable downloads")
                if "val files = OciFs.ls(" in active:
                    # The incident's unsupported API error is caught by source.
                    # This response MUST NOT short-circuit model repair.
                    return _output("not yet written")
            if "object OciFs" in code:
                self.executed.append(code)
                self.bootstrap_calls += 1
                return _output("OCIFS_BOOTSTRAP_OK")
        return await super().execute(code, **kwargs)


def _setup(monkeypatch, tmp_path, sources, *, child, native=False):
    notebook = _notebook(*(s.removeprefix("%scala\n") if native else s for s in sources))
    if native:
        notebook["metadata"]["language_info"] = {"name": "scala"}
    _patch_common(monkeypatch, tmp_path, notebook)
    # Exercise the real namespace guard against an authoritative fixture map,
    # independent of config-loading tests that previously emptied its cache.
    monkeypatch.setattr(context_tools, "_BUCKET_NS_INDEX",
                        {"fixture-bucket": "fixture-namespace"})
    monkeypatch.setattr(jm, "call_fix", am.call_fix)
    monkeypatch.setattr(jm, "OCI_CONFIG_PATH", CONFIG)
    monkeypatch.setattr(jm, "_cell_history", [])
    monkeypatch.setattr(jm, "_current_cell_notes", [])
    monkeypatch.setattr(jm, "_migration_cache", {})
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    monkeypatch.setattr(jm, "_manifest_dependency_paths",
                        contextvars.ContextVar("listing_fixture_manifest", default=None))
    monkeypatch.setattr(am, "_cell_history", [])
    if not child:
        return None
    path = tmp_path / "child.ipynb"
    path.write_text(json.dumps(notebook), encoding="utf-8")
    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path))
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    monkeypatch.setattr(jm, "_notebook_content_cache", {str(path): json.dumps(notebook)})
    async def summarize(*args, **kwargs):
        return "fixture listing"
    monkeypatch.setattr(jm, "_summarize_cell_code", summarize)
    return path


def _run(kernel, child_path):
    if child_path is not None:
        return asyncio.run(jm._inline_child_notebook(
            str(child_path), kernel, SimpleNamespace(log=lambda *a: None),
            "listing_fixture", "", "", "", {}))
    return asyncio.run(jm.process_notebook(
        "/Workspace/test_cond/source.ipynb", kernel,
        "listing_fixture", "task", {}, run_all=True))


def _saved(kernel, child_path):
    if child_path is None:
        return _saved_sources(kernel)
    nb = json.loads(child_path.read_text(encoding="utf-8"))
    return "\n".join("".join(c["source"]) for c in nb["cells"]), nb


@pytest.mark.parametrize("child", [False, True], ids=["parent", "inline-child"])
@pytest.mark.parametrize("path,mixed,native", [
    (path, False, False) for path in PATHS
] + [(PATHS[0], True, False), (PATHS[0], False, True)],
    ids=["dbfs-uri", "dbfs-mount", "volume", "oci", "mixed", "native-scala"])
def test_listing_failure_reaches_model_and_saves_its_verified_repair(
        monkeypatch, tmp_path, child, path, mixed, native):
    source = _source(path, mixed=mixed)
    repaired = _repair(source, path, mixed=mixed)
    child_path = _setup(monkeypatch, tmp_path, [source], child=child, native=native)
    seen = []
    async def model(system, user_content, **kwargs):
        seen.append(user_content)
        assert "not found: value dbutils" in user_content
        assert source.strip() in user_content
        assert "starting with %scala" in user_content
        assert am.PATH_PREFIX_MIGRATION_GUIDANCE in system
        assert "OciFs.ls" in system and "UNRESOLVED" in system
        return repaired, {}
    monkeypatch.setattr(am, "call_opus_with_tools", model)
    kernel = ListingKernel([repaired])
    result = _run(kernel, child_path)
    assert len(seen) == 1, result
    assert result["status"] in ("PASS", "ok"), result
    assert result.get("failed", 0) == 0 and not result.get("failed_cells"), result
    assert result.get("fixed", result.get("cells_fixed")) == 1, result
    assert [code.strip() for code in kernel.listings] == [source.strip(), repaired.strip()]
    assert kernel.bootstrap_calls == 0
    saved, notebook = _saved(kernel, child_path)
    assert repaired.strip() in saved
    assert "UNRESOLVED: clickable downloads" in saved
    assert "dbutils.fs.ls(outputPath)" in saved  # original preserved
    assert "https://example.cloud.databricks.com" in saved
    assert "https://example.cloud.databricks.com" not in _active(saved)
    if child:
        outputs = json.dumps(notebook["cells"][-1].get("outputs", []))
        target = OCI if path.startswith("oci://") else VOLUME
        assert target + "/part-00000.parquet" in outputs
        assert "not found: value dbutils" not in outputs


@pytest.mark.parametrize("child", [False, True], ids=["parent", "inline-child"])
@pytest.mark.parametrize("failure", [
    "Command ID abc-123-def failed with java.lang.RuntimeException: SDK unavailable",
    "Failing command: listing\njava.io.FileNotFoundException: output absent",
])
def test_failed_repair_is_retried_and_latest_outputs_are_saved(
        monkeypatch, tmp_path, child, failure):
    source = _source(PATHS[0])
    repaired = _repair(source, PATHS[0])
    child_path = _setup(monkeypatch, tmp_path, [source], child=child)
    seen = []
    async def model(system, user_content, **kwargs):
        seen.append(user_content)
        return repaired, {}
    monkeypatch.setattr(am, "call_opus_with_tools", model)
    kernel = ListingKernel([repaired], [failure])
    result = _run(kernel, child_path)
    assert len(seen) == 2 and failure in seen[1], result
    assert result.get("fixed", result.get("cells_fixed")) == 1, result
    assert len(kernel.listings) == 3
    assert repaired.strip() in _saved(kernel, child_path)[0]


@pytest.mark.parametrize("child", [False, True], ids=["parent", "inline-child"])
@pytest.mark.parametrize("repair_fails", [False, True], ids=["unchanged", "missing-path"])
def test_exhausted_repairs_are_reported_not_marked_fixed(
        monkeypatch, tmp_path, child, repair_fails):
    source = _source(PATHS[0])
    repaired = _repair(source, PATHS[0])
    child_path = _setup(monkeypatch, tmp_path, [source], child=child)
    seen = []
    async def model(system, user_content, **kwargs):
        seen.append(user_content)
        return (repaired if repair_fails else source), {}
    monkeypatch.setattr(am, "call_opus_with_tools", model)
    failure = "Command ID abc-123 failed with java.io.FileNotFoundException: output absent"
    kernel = ListingKernel([repaired], [failure] * jm.MAX_FIX_ATTEMPTS)
    result = _run(kernel, child_path)
    assert len(seen) == jm.MAX_FIX_ATTEMPTS - 1 == 4, result
    assert result.get("failed", 0) or result.get("failed_cells"), result
    assert result.get("fixed", result.get("cells_fixed")) == 0, result
    assert kernel.bootstrap_calls == 0
    if repair_fails:
        assert failure in seen[-1]
    else:
        assert source.strip() in _saved(kernel, child_path)[0]


@pytest.mark.parametrize("child", [False, True], ids=["parent", "inline-child"])
def test_each_failed_cell_is_repaired_without_stale_bootstrap_state(monkeypatch, tmp_path, child):
    sources = [_source(path) for path in PATHS[:2]]
    repairs = [_repair(source, path) for source, path in zip(sources, PATHS)]
    child_path = _setup(monkeypatch, tmp_path, sources, child=child)
    seen = []
    async def model(system, user_content, **kwargs):
        i = len(seen)
        seen.append(user_content)
        assert sources[i].strip() in user_content
        return repairs[i], {}
    monkeypatch.setattr(am, "call_opus_with_tools", model)
    kernel = ListingKernel(repairs)
    result = _run(kernel, child_path)
    assert len(seen) == 2 and len(kernel.listings) == 4, result
    assert result.get("fixed", result.get("cells_fixed")) == 2, result
    saved, _ = _saved(kernel, child_path)
    assert all(repaired.strip() in saved for repaired in repairs)


@pytest.mark.parametrize("child", [False, True], ids=["parent", "inline-child"])
@pytest.mark.parametrize("source", [
    '%scala\n// dbutils.fs.ls("dbfs:/FileStore/output")\nprintln("ready")',
    '%scala\nval helpText = "dbutils.fs.ls(path)"\nprintln(helpText)',
    f'%scala\nval files = OciFs.ls("{OCI}")',
])
def test_working_code_is_not_rewritten_or_repaired(monkeypatch, tmp_path, child, source):
    child_path = _setup(monkeypatch, tmp_path, [source], child=child)
    async def unexpected(*args, **kwargs):
        pytest.fail("Working code must not request model repair")
    monkeypatch.setattr(am, "call_opus_with_tools", unexpected)
    kernel = ListingKernel()
    result = _run(kernel, child_path)
    assert result.get("fixed", result.get("cells_fixed")) == 0, result
    assert result.get("failed", 0) == 0 and not result.get("failed_cells"), result
    assert source.strip() in _saved(kernel, child_path)[0]


def test_listing_and_download_guidance_reaches_both_prompt_entrypoints():
    guidance = am.PATH_PREFIX_MIGRATION_GUIDANCE
    assert guidance in am.FIX_PROMPT and guidance in jm.CELL_MIGRATE_PROMPT
    assert "OciFs.ls" in guidance and "UNRESOLVED" in guidance
