"""Shared dependencies belong to each job's output, including warm processes."""
import asyncio
import json
from pathlib import Path

import pytest

from test_inline_child_preparation import flow, FilesystemKernel, jm, _notebook
from test_manifest_task_scan_flag import _setup


@pytest.mark.parametrize("manifest_mode", [False, True])
@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("skip_migrated", [False, True])
def test_shared_helpers_are_prepared_per_job_and_reused_within_job(
        monkeypatch, tmp_path, flow, manifest_mode, nested, skip_migrated):
    source, _ = flow
    helper = source / "helper.ipynb"
    helper.write_text(json.dumps(_notebook(["VALUE = 42"])))
    outer = source / "outer.ipynb"
    outer.write_text(json.dumps(_notebook([f'%run "{helper}"'])))
    entry = outer if nested else helper
    parent = source / "parent.ipynb"
    parent.write_text(json.dumps(_notebook([f'%run "{entry}"'])))
    original_bytes = {p: p.read_bytes() for p in (helper, outer, parent)}
    _setup(monkeypatch, tmp_path)
    monkeypatch.setattr(jm, "CODE_ONLY", False)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    monkeypatch.setattr(jm, "SKIP_MIGRATED", skip_migrated)
    monkeypatch.setattr(jm, "download_notebook",
                        lambda path: Path("/" + path.lstrip("/")).read_bytes())

    async def noop(*args, **kwargs):
        pass

    monkeypatch.setattr(jm, "_ensure_redirect_schema", noop)
    converted = []
    actual_convert = jm.process_notebook

    async def convert(path, session, job_name, task_key, *args, **kwargs):
        if task_key.startswith("dep_"):
            converted.append((job_name, Path(path).name))
        return await actual_convert(path, session, job_name, task_key, *args, **kwargs)

    monkeypatch.setattr(jm, "process_notebook", convert)
    kernel = FilesystemKernel(fire=False)
    kernel.cluster_id = "test-cluster"
    leaf_edge = {"path": str(helper), "original_path": str(helper),
                 "exists": True, "nested_deps": []}
    edge = ({"path": str(outer), "original_path": str(outer), "exists": True,
             "nested_deps": [leaf_edge]} if nested else leaf_edge)

    def job(name):
        return {"job_name": name, "root": str(source), "tasks": [{
            "task_key": "main", "notebook_path": str(parent), "depends_on": [],
            "run_deps": [edge]}], "execution_layers": [["main"]],
            "default_cluster": kernel.cluster_id}

    async def run():
        for index, name in enumerate(("first", "first", "second")):
            result = await jm.process_job(job(name), kernel,
                                          skip_task_dependency_scan=manifest_mode)
            assert result["task_results"]["main"]["status"] == "PASS", result
            root = jm.migration_job_output_path(jm.MIGRATE_OUTPUT, name) + "/notebooks/"
            for original in ([helper, outer] if nested else [helper]):
                saved = jm._migration_cache[jm.normalize_nb_path(str(original))]
                assert saved.startswith(root), (name, saved)
                assert Path(saved).is_file()
            # A repeated caller in the same job uses its prepared cache entry.
            assert len(converted) == (1 + int(nested)) * (1 if index < 2 else 2)

    asyncio.run(run())
    assert all(path.read_bytes() == content for path, content in original_bytes.items())
    for name in ("first", "second"):
        saved = Path(jm.migration_job_output_path(jm.MIGRATE_OUTPUT, name)) / "notebooks" / jm.normalize_nb_path(str(helper))
        assert "VALUE = 42" in saved.read_text()


def test_failed_second_job_preparation_cannot_reuse_foreign_artifact(
        monkeypatch, tmp_path, flow):
    source, _ = flow
    helper = source / "helper.ipynb"
    helper.write_text(json.dumps(_notebook(["VALUE = 42"])))
    parent = source / "parent.ipynb"
    parent.write_text(json.dumps(_notebook([f'%run "{helper}"'])))
    _setup(monkeypatch, tmp_path)
    monkeypatch.setattr(jm, "CODE_ONLY", False)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    monkeypatch.setattr(jm, "download_notebook",
                        lambda path: Path("/" + path.lstrip("/")).read_bytes())

    async def noop(*args, **kwargs):
        pass

    monkeypatch.setattr(jm, "_ensure_redirect_schema", noop)
    kernel = FilesystemKernel(fire=False)
    kernel.cluster_id = "test-cluster"

    def job(name):
        return {"job_name": name, "root": str(source), "tasks": [{
            "task_key": "main", "notebook_path": str(parent), "depends_on": [],
            "run_deps": [{"path": str(helper), "exists": True, "nested_deps": []}]}],
            "execution_layers": [["main"]], "default_cluster": kernel.cluster_id}

    async def run():
        first = await jm.process_job(job("first"), kernel, skip_task_dependency_scan=True)
        assert first["task_results"]["main"]["status"] == "PASS"
        old_copy = Path(jm._migration_cache[jm.normalize_nb_path(str(helper))])
        old_bytes = old_copy.read_bytes()
        before = len(kernel.executed)
        helper.write_text(json.dumps(_notebook(["def invalid(:"])))
        second = await jm.process_job(job("second"), kernel, skip_task_dependency_scan=True)
        assert second["task_results"]["main"]["status"] == "FAIL", second
        assert jm.normalize_nb_path(str(helper)) not in jm._migration_cache
        assert "VALUE = 42" not in kernel.executed[before:]
        assert old_copy.read_bytes() == old_bytes

    asyncio.run(run())
