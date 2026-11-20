"""Local module mirrors use the same export-base mapping as saved notebooks."""

import asyncio
import contextlib
import io
import json
import os
import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import cluster_lifecycle as lifecycle
import job_migrate as migrator


@pytest.mark.parametrize(
    "source,export_base,expected",
    [
        (
            "/Workspace/project/exported/notebooks/Users/customer",
            "project/exported/notebooks",
            "/Workspace/project/migrated/job/notebooks/Users/customer",
        ),
        (
            "/Workspace/project/exported/notebooks/Users/customer/src",
            "/Workspace/project/exported/notebooks/",
            "/Workspace/project/migrated/job/notebooks/Users/customer/src",
        ),
        (
            "/Workspace/project/exported/notebooks",
            "/Workspace/project/exported/notebooks",
            "/Workspace/project/migrated/job/notebooks",
        ),
        (
            "/Workspace/project/exported/notebooks_other/Users/customer",
            "project/exported/notebooks",
            "/Workspace/project/migrated/job/notebooks/project/exported/notebooks_other/Users/customer",
        ),
        (
            "/Workspace/project/exported/notebooks/Users/customer",
            "",
            "/Workspace/project/migrated/job/notebooks/project/exported/notebooks/Users/customer",
        ),
    ],
)
def test_local_module_mirror_destination_and_import_roots(
    monkeypatch, source, export_base, expected
):
    class Session:
        async def execute(self, code, **_kwargs):
            return {
                "status": "ok",
                "outputs": [{"type": "stream", "text":
                             "__AIDP_MIG_MISSING_BEGIN__emgen__AIDP_MIG_MISSING_END__"}],
            }

    copied = []

    async def discover(_session, _notebooks, _imports, **_kwargs):
        return {"emgen"}, {source, f"{source}/emgen/src"}, [f"{source}/emgen/src/emgen"]

    async def mirror(_session, src, dst, **_kwargs):
        copied.append((src, dst))
        return True

    monkeypatch.setattr(lifecycle, "_discover_local_modules", discover)
    monkeypatch.setattr(lifecycle, "_pick_mirror_root", lambda _paths: source)
    monkeypatch.setattr(lifecycle, "_mirror_source_tree", mirror)

    result = asyncio.run(lifecycle.ensure_requirements_installed(
        "unused-cluster", Session(), ["emgen"],
        "/Workspace/project/migrated/job",
        notebook_paths=[f"{source}/emgen/notebook.ipynb"],
        migrated_base="/Workspace/project/migrated/job/notebooks",
        export_base=export_base,
    ))

    assert copied == [(source, expected)]
    assert result["src_roots"] == [expected, f"{expected}/emgen/src"]


@pytest.mark.parametrize("wrap", [
    lambda payload: "[]" + payload + "\n",
    lambda payload: json.dumps([{"type": "TEXT_PLAIN", "value": payload + "\n"}]),
])
def test_mirror_accepts_aidp_output_envelopes(wrap):
    class Session:
        async def execute(self, _code, **_kwargs):
            payload = json.dumps({"ok": True, "files_copied": 2})
            return {"status": "ok", "outputs": [
                {"type": "stream", "text": wrap(payload)},
            ]}

    assert asyncio.run(lifecycle._mirror_source_tree(
        Session(), "/Workspace/exported/src", "/Workspace/migrated/src"
    ))


@pytest.mark.parametrize("payload,expected", [
    ('{"ok": false, "error": "permission denied"}', "permission denied"),
    ('UserWarning: extra output\n{"ok": true}', "result unreadable"),
])
def test_mirror_failure_reports_error_instead_of_using_source(payload, expected):
    class Session:
        async def execute(self, _code, **_kwargs):
            return {"status": "ok", "outputs": [{"type": "stream", "text": payload}]}

    with pytest.raises(lifecycle.MirrorCopyError, match=expected):
        asyncio.run(lifecycle._mirror_source_tree(
            Session(), "/Workspace/exported/src", "/Workspace/migrated/src",
        ))


def test_failed_mirror_hard_fails_before_using_customer_source(monkeypatch):
    class Session:
        async def execute(self, _code, **_kwargs):
            return {"status": "ok", "outputs": [{
                "type": "stream",
                "text": "__AIDP_MIG_MISSING_BEGIN__emgen__AIDP_MIG_MISSING_END__",
            }]}

    async def discover(_session, _notebooks, _imports, **_kwargs):
        return {"emgen"}, {"/Workspace/exported/src"}, ["/Workspace/exported/src/emgen"]

    async def mirror(_session, _src, _dst, **_kwargs):
        return False

    monkeypatch.setattr(lifecycle, "_discover_local_modules", discover)
    monkeypatch.setattr(lifecycle, "_pick_mirror_root", lambda _paths: "/Workspace/exported/src")
    monkeypatch.setattr(lifecycle, "_mirror_source_tree", mirror)
    with pytest.raises(lifecycle.MirrorCopyError, match="Local-module mirror copy failed"):
        asyncio.run(lifecycle.ensure_requirements_installed(
            "unused-cluster", Session(), ["emgen"], "/Workspace/migrated",
            notebook_paths=["/Workspace/exported/notebook.ipynb"],
            migrated_base="/Workspace/migrated/notebooks", export_base="exported",
        ))


def test_legacy_caller_without_migrated_base_keeps_isolated_destination(monkeypatch):
    class Session:
        async def execute(self, _code, **_kwargs):
            return {"status": "ok", "outputs": [{
                "type": "stream",
                "text": "__AIDP_MIG_MISSING_BEGIN__emgen__AIDP_MIG_MISSING_END__",
            }]}

    async def discover(_session, _notebooks, _imports, **_kwargs):
        return {"emgen"}, {"/Workspace/exported/src"}, ["/Workspace/exported/src/emgen"]

    copied = []

    async def mirror(_session, src, dst, **_kwargs):
        copied.append((src, dst))
        return True

    monkeypatch.setattr(lifecycle, "_discover_local_modules", discover)
    monkeypatch.setattr(lifecycle, "_pick_mirror_root", lambda _paths: "/Workspace/exported/src")
    monkeypatch.setattr(lifecycle, "_mirror_source_tree", mirror)
    result = asyncio.run(lifecycle.ensure_requirements_installed(
        "unused-cluster", Session(), ["emgen"], "/Workspace/job",
        notebook_paths=["/Workspace/exported/notebook.ipynb"],
        export_base="exported",
    ))

    assert copied == [("/Workspace/exported/src", "/Workspace/job/local_modules/src")]
    assert result["src_roots"] == ["/Workspace/job/local_modules/src"]


@pytest.mark.skipif(os.name == "nt", reason="AIDP mirror paths use POSIX workspace roots")
def test_repaired_mirrored_sibling_survives_saveback_and_fresh_run(
    monkeypatch, tmp_path, capsys
):
    source = tmp_path / "exported" / "notebooks"
    source.mkdir(parents=True)
    (source / "utils.py").write_bytes(b"original\n")
    (source / "pkg").mkdir()
    (source / "pkg" / "sub.py").write_bytes(b"original nested\n")
    migrated_base = tmp_path / "migrated" / "notebooks"

    class Session:
        def __init__(self):
            self.commands = []

        async def execute(self, code, **_kwargs):
            self.commands.append(code)
            if "__AIDP_MIG_MISSING_BEGIN__" in code:
                return {"status": "ok", "outputs": [{"type": "stream", "text":
                        "__AIDP_MIG_MISSING_BEGIN__utils__AIDP_MIG_MISSING_END__"}]}
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                exec(code, {})
            return {"status": "ok", "outputs": [{"type": "stream", "text": output.getvalue()}]}

    async def discover(_session, _notebooks, _imports, **_kwargs):
        return {"utils"}, {str(source)}, [str(source / "utils.py")]

    monkeypatch.setattr(lifecycle, "_discover_local_modules", discover)
    monkeypatch.setattr(lifecycle, "_pick_mirror_root", lambda _paths: str(source))
    session = Session()
    async def prepare(notebook_name, *, refresh=False):
        return await lifecycle.ensure_requirements_installed(
            "unused-cluster", session, ["utils"], str(tmp_path / "job"),
            notebook_paths=[str(source / notebook_name)],
            migrated_base=str(migrated_base), export_base="",
            refresh_mirror=refresh,
        )

    first = asyncio.run(prepare("first.ipynb"))
    destination = Path(first["mirror_root_dst"])
    assert (destination / "utils.py").read_bytes() == b"original\n"
    assert (destination / "pkg" / "sub.py").read_bytes() == b"original nested\n"

    # This is the model's repaired copy. Save-back must not reintroduce the
    # exported original after execution has already accepted the repair.
    (destination / "utils.py").write_bytes(b"repaired\n")
    (destination / "pkg" / "sub.py").write_bytes(b"repaired nested\n")
    logs = []
    asyncio.run(migrator._copy_sibling_python_files(
        session, str(source), str(destination), logs.append,
    ))
    assert (destination / "utils.py").read_bytes() == b"repaired\n"
    assert (destination / "pkg" / "sub.py").read_bytes() == b"repaired nested\n"

    (source / "new.py").write_bytes(b"new module\n")
    before_second = len(session.commands)
    second = asyncio.run(prepare("second.ipynb"))
    assert second["mirror_root_dst"] == str(destination)
    assert len(session.commands) == before_second + 2  # import check and gap-filling copy
    assert "(1 .py copied, 2 existing skipped)" in capsys.readouterr().out
    asyncio.run(migrator._copy_sibling_python_files(
        session, str(source), str(destination), logs.append,
    ))
    assert (destination / "utils.py").read_bytes() == b"repaired\n"
    assert (destination / "pkg" / "sub.py").read_bytes() == b"repaired nested\n"
    assert (destination / "new.py").read_bytes() == b"new module\n"

    # A new process has no ownership record. Its mirror copy must still leave
    # existing repairs intact unless the operator explicitly requests refresh.
    asyncio.run(prepare("rerun.ipynb"))
    assert "(0 .py copied, 3 existing skipped)" in capsys.readouterr().out
    assert (destination / "utils.py").read_bytes() == b"repaired\n"
    assert (destination / "pkg" / "sub.py").read_bytes() == b"repaired nested\n"

    asyncio.run(prepare("refresh.ipynb", refresh=True))
    assert (destination / "utils.py").read_bytes() == b"original\n"
    assert (destination / "pkg" / "sub.py").read_bytes() == b"original nested\n"


@pytest.mark.skipif(os.name == "nt", reason="AIDP mirror paths use POSIX workspace roots")
def test_interrupted_mirror_copy_leaves_no_final_file_and_retries(monkeypatch, tmp_path):
    source = tmp_path / "exported"
    destination = tmp_path / "migrated"
    source.mkdir()
    (source / "utils.py").write_bytes(b"complete source\n")

    class Session:
        async def execute(self, code, **_kwargs):
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                exec(code, {})
            return {"outputs": [{"type": "stream", "text": output.getvalue()}]}

    original_copy2 = shutil.copy2

    def interrupted_copy(_source, target, *_args, **_kwargs):
        Path(target).write_bytes(b"partial")
        raise OSError("interrupted during copy")

    monkeypatch.setattr(shutil, "copy2", interrupted_copy)
    with pytest.raises(lifecycle.MirrorCopyError, match="interrupted during copy"):
        asyncio.run(lifecycle._mirror_source_tree(Session(), str(source), str(destination)))
    assert not (destination / "utils.py").exists()
    assert not list(destination.glob(".utils.py.*.aidp-copy-tmp"))

    monkeypatch.setattr(shutil, "copy2", original_copy2)
    assert asyncio.run(lifecycle._mirror_source_tree(Session(), str(source), str(destination)))
    assert (destination / "utils.py").read_bytes() == b"complete source\n"


@pytest.mark.skipif(os.name == "nt", reason="AIDP mirror paths use POSIX workspace roots")
def test_interrupted_refresh_preserves_existing_mirror(monkeypatch, tmp_path):
    source = tmp_path / "exported"
    destination = tmp_path / "migrated"
    source.mkdir()
    destination.mkdir()
    (source / "utils.py").write_bytes(b"original\n")
    (destination / "utils.py").write_bytes(b"repaired\n")

    class Session:
        async def execute(self, code, **_kwargs):
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                exec(code, {})
            return {"outputs": [{"type": "stream", "text": output.getvalue()}]}

    def interrupted_copy(_source, target, *_args, **_kwargs):
        Path(target).write_bytes(b"partial")
        raise OSError("interrupted during refresh")

    monkeypatch.setattr(shutil, "copy2", interrupted_copy)
    with pytest.raises(lifecycle.MirrorCopyError, match="interrupted during refresh"):
        asyncio.run(lifecycle._mirror_source_tree(
            Session(), str(source), str(destination), refresh_mirror=True,
        ))
    assert (destination / "utils.py").read_bytes() == b"repaired\n"
    assert not list(destination.glob(".utils.py.*.aidp-copy-tmp"))


@pytest.mark.skipif(os.name == "nt", reason="AIDP mirror paths use POSIX workspace roots")
def test_interrupted_sibling_copy_leaves_no_final_file_and_retries(monkeypatch, tmp_path):
    source = tmp_path / "exported"
    destination = tmp_path / "migrated"
    source.mkdir()
    (source / "utils.py").write_bytes(b"complete source\n")
    (source / "__init__.py").write_bytes(b"")

    class Session:
        async def execute(self, code, **_kwargs):
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                exec(code, {})
            return {"outputs": [{"type": "stream", "text": output.getvalue()}]}

    original_copy2 = shutil.copy2

    def interrupted_copy(_source, target, *_args, **_kwargs):
        Path(target).write_bytes(b"partial")
        raise OSError("interrupted during sibling copy")

    monkeypatch.setattr(shutil, "copy2", interrupted_copy)
    logs = []
    asyncio.run(migrator._copy_sibling_python_files(
        Session(), str(source), str(destination), logs.append,
    ))
    assert not (destination / "utils.py").exists()
    assert not (destination / "__init__.py").exists()
    assert not list(destination.glob(".*.aidp-copy-tmp"))

    monkeypatch.setattr(shutil, "copy2", original_copy2)
    asyncio.run(migrator._copy_sibling_python_files(
        Session(), str(source), str(destination), logs.append,
    ))
    assert (destination / "utils.py").read_bytes() == b"complete source\n"
    assert (destination / "__init__.py").read_bytes() == b""


@pytest.mark.skipif(os.name == "nt", reason="AIDP mirror paths use POSIX workspace roots")
def test_fresh_discovery_excludes_migrated_copy_before_selecting_source_root(
    monkeypatch, tmp_path,
):
    source = tmp_path / "exported" / "notebooks"
    source.mkdir(parents=True)
    (source / "utils.py").write_bytes(b"original\n")
    (source / "pkg").mkdir()
    (source / "pkg" / "__init__.py").write_bytes(b"")
    (source / "pkg" / "sub.py").write_bytes(b"original nested\n")
    migrated_root = tmp_path / "migrated"
    migrated_base = migrated_root / "current-job" / "notebooks"
    sibling = migrated_root / "sibling-job" / "notebooks"
    sibling.mkdir(parents=True)
    (sibling / "utils.py").write_bytes(b"unrelated job's mirror\n")
    (sibling / "pkg").mkdir()
    (sibling / "pkg" / "__init__.py").write_bytes(b"")

    class Session:
        async def execute(self, code, **_kwargs):
            if "__AIDP_MIG_MISSING_BEGIN__" in code:
                output = "__AIDP_MIG_MISSING_BEGIN__utils,pkg__AIDP_MIG_MISSING_END__"
            else:
                captured = io.StringIO()
                with contextlib.redirect_stdout(captured):
                    exec(code, {})
                output = captured.getvalue()
            return {"status": "ok", "outputs": [{"type": "stream", "text": output}]}

    real_discover = lifecycle._discover_local_modules

    async def bounded_discover(session, notebooks, candidates, **kwargs):
        return await real_discover(
            session, notebooks, candidates, max_ancestor_depth=3, **kwargs,
        )

    monkeypatch.setattr(lifecycle, "_discover_local_modules", bounded_discover)
    session = Session()

    def prepare():
        return asyncio.run(lifecycle.ensure_requirements_installed(
            "unused-cluster", session, ["utils", "pkg"], str(tmp_path / "job"),
            notebook_paths=[str(source / "validation.ipynb")],
            migrated_base=str(migrated_base),
            export_base=str(tmp_path / "exported"),
            migrate_output_root=str(migrated_root),
        ))

    first = prepare()
    assert first["mirror_root_orig"] == str(source)
    destination = Path(first["mirror_root_dst"])
    assert destination.is_relative_to(migrated_base)
    (destination / "utils.py").write_bytes(b"repaired\n")
    (destination / "pkg" / "sub.py").write_bytes(b"repaired nested\n")
    asyncio.run(migrator._copy_sibling_python_files(
        session, str(source), str(destination), lambda _message: None,
    ))

    _, _, matched_paths = asyncio.run(real_discover(
        session, [str(source / "validation.ipynb")], {"utils", "pkg"},
        max_ancestor_depth=3, excluded_roots=(str(migrated_root),),
    ))
    assert matched_paths
    assert all(Path(path.rstrip("/")).is_relative_to(source) for path in matched_paths)

    second = prepare()
    assert second["mirror_root_orig"] == str(source)
    assert second["mirror_root_dst"] == str(destination)
    assert (destination / "utils.py").read_bytes() == b"repaired\n"
    assert (destination / "pkg" / "sub.py").read_bytes() == b"repaired nested\n"
    assert not (destination / "migrated").exists()


@pytest.mark.skipif(os.name == "nt", reason="AIDP mirror paths use POSIX workspace roots")
def test_mirror_walk_skips_other_jobs_when_output_root_is_inside_source(monkeypatch, tmp_path):
    source = tmp_path / "project"
    source.mkdir()
    (source / "common.py").write_bytes(b"exported original\n")
    migrated_root = source / "migrated"
    other_job = migrated_root / "other-job" / "notebooks"
    other_job.mkdir(parents=True)
    (other_job / "common.py").write_bytes(b"other job repair\n")
    migrated_base = migrated_root / "current-job" / "notebooks"

    class Session:
        async def execute(self, code, **_kwargs):
            if "__AIDP_MIG_MISSING_BEGIN__" in code:
                output = "__AIDP_MIG_MISSING_BEGIN__common__AIDP_MIG_MISSING_END__"
            else:
                captured = io.StringIO()
                with contextlib.redirect_stdout(captured):
                    exec(code, {})
                output = captured.getvalue()
            return {"status": "ok", "outputs": [{"type": "stream", "text": output}]}

    real_discover = lifecycle._discover_local_modules

    async def bounded_discover(session, notebooks, candidates, **kwargs):
        return await real_discover(
            session, notebooks, candidates, max_ancestor_depth=1, **kwargs,
        )

    monkeypatch.setattr(lifecycle, "_discover_local_modules", bounded_discover)
    result = asyncio.run(lifecycle.ensure_requirements_installed(
        "unused-cluster", Session(), ["common"], str(migrated_root / "current-job"),
        notebook_paths=[str(source / "validation.ipynb")],
        migrated_base=str(migrated_base), export_base=str(source),
        migrate_output_root=str(migrated_root),
    ))

    assert result["mirror_root_orig"] == str(source)
    destination = Path(result["mirror_root_dst"])
    assert destination.is_relative_to(migrated_base)
    assert (destination / "common.py").read_bytes() == b"exported original\n"
    assert (other_job / "common.py").read_bytes() == b"other job repair\n"
    assert not (destination / "migrated").exists()


@pytest.mark.skipif(os.name == "nt", reason="AIDP mirror paths use POSIX workspace roots")
def test_mirror_walk_skips_its_existing_destination_without_output_root(tmp_path):
    source = tmp_path / "project"
    destination = source / "migrated" / "current-job"
    destination.mkdir(parents=True)
    (source / "common.py").write_bytes(b"original\n")
    (destination / "repaired.py").write_bytes(b"repaired\n")

    class Session:
        async def execute(self, code, **_kwargs):
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                exec(code, {})
            return {"status": "ok", "outputs": [{"type": "stream", "text": output.getvalue()}]}

    assert asyncio.run(lifecycle._mirror_source_tree(
        Session(), str(source), str(destination),
    ))
    assert (destination / "common.py").read_bytes() == b"original\n"
    assert (destination / "repaired.py").read_bytes() == b"repaired\n"
    assert not (destination / "migrated" / "current-job").exists()


def test_uncovered_notebook_directory_still_gets_sibling_copy(tmp_path):
    source = tmp_path / "exported" / "other"
    destination = tmp_path / "migrated" / "other"
    (source / "nested").mkdir(parents=True)
    (source / "helpers.py").write_bytes(b"original helper\n")
    (source / "nested" / "ignored.py").write_bytes(b"nested\n")

    class Session:
        async def execute(self, code, **_kwargs):
            exec(code, {})
            return {"status": "ok", "outputs": []}

    logs = []
    asyncio.run(migrator._copy_sibling_python_files(
        Session(), str(source), str(destination), logs.append,
    ))
    assert (destination / "helpers.py").read_bytes() == b"original helper\n"
    assert not (destination / "nested" / "ignored.py").exists()
    assert not logs

    (destination / "helpers.py").write_bytes(b"repaired helper\n")
    asyncio.run(migrator._copy_sibling_python_files(
        Session(), str(source), str(destination), logs.append,
    ))
    assert (destination / "helpers.py").read_bytes() == b"repaired helper\n"
    assert not logs


@pytest.mark.skipif(os.name == "nt", reason="AIDP mirror paths use POSIX workspace roots")
def test_parent_mirror_after_child_fills_gaps_without_replacing_repair(monkeypatch, tmp_path):
    source = tmp_path / "exported" / "notebooks" / "User"
    child = source / "proj"
    child.mkdir(parents=True)
    (child / "utils.py").write_bytes(b"original child\n")
    (source / "common.py").write_bytes(b"parent module\n")
    migrated_base = tmp_path / "migrated" / "notebooks"
    current_root = child

    class Session:
        async def execute(self, code, **_kwargs):
            if "__AIDP_MIG_MISSING_BEGIN__" in code:
                return {"status": "ok", "outputs": [{"type": "stream", "text":
                        "__AIDP_MIG_MISSING_BEGIN__utils__AIDP_MIG_MISSING_END__"}]}
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                exec(code, {})
            return {"status": "ok", "outputs": [{"type": "stream", "text": output.getvalue()}]}

    async def discover(_session, _notebooks, _imports, **_kwargs):
        return {"utils"}, {str(current_root)}, [str(current_root / "utils.py")]

    monkeypatch.setattr(lifecycle, "_discover_local_modules", discover)
    monkeypatch.setattr(lifecycle, "_pick_mirror_root", lambda _paths: str(current_root))

    def prepare():
        return asyncio.run(lifecycle.ensure_requirements_installed(
            "unused-cluster", Session(), ["utils"], str(tmp_path / "job"),
            notebook_paths=[str(current_root / "notebook.ipynb")],
            migrated_base=str(migrated_base), export_base="",
        ))

    child_result = prepare()
    child_destination = Path(child_result["mirror_root_dst"])
    (child_destination / "utils.py").write_bytes(b"repaired child\n")

    current_root = source
    parent_result = prepare()
    parent_destination = Path(parent_result["mirror_root_dst"])
    assert child_destination == parent_destination / "proj"
    assert (child_destination / "utils.py").read_bytes() == b"repaired child\n"
    assert (parent_destination / "common.py").read_bytes() == b"parent module\n"


def test_mirror_failure_reaches_process_notebook(monkeypatch, tmp_path):
    import cell_analyzer
    from test_pip_magic_gate import _Kernel, _patch_common

    notebook = {"cells": [{"cell_type": "code", "source": ["import utils\n"],
                           "metadata": {}, "outputs": [], "execution_count": None}],
                "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    _patch_common(monkeypatch, tmp_path, notebook, [])

    async def analyze(cells, *_args, **_kwargs):
        return [{"cell_index": 0, "action": "execute", "description": "",
                 "dependencies": ["utils"], "risks": [], "changes_needed": [],
                 "storage_paths": [], "table_refs": [], "fuse_risks": []}]

    async def ensure(*_args, **kwargs):
        assert kwargs["migrate_output_root"] == migrator.MIGRATE_OUTPUT
        raise lifecycle.MirrorCopyError("Local-module mirror copy failed: source → destination")

    monkeypatch.setattr(cell_analyzer, "analyze_notebook_cells", analyze)
    monkeypatch.setattr(lifecycle, "ensure_requirements_installed", ensure)
    with pytest.raises(lifecycle.MirrorCopyError, match="mirror copy failed"):
        asyncio.run(migrator.process_notebook(
            "/Workspace/exported/notebook.ipynb", _Kernel(), "test_pip", "task", {},
        ))


@pytest.mark.parametrize("code_only_dependency", [False, True])
def test_process_notebook_saveback_uses_copy_if_absent(
    monkeypatch, tmp_path, code_only_dependency
):
    """Main tasks and dependency save-back both protect existing siblings."""
    import cell_analyzer
    from test_pip_magic_gate import _Kernel, _patch_common

    source = "/Workspace/exported/notebooks/User"
    destination = "/Workspace/test_pip/notebooks/User"
    notebook = {"cells": [{"cell_type": "code", "source": ["x = 1\n"],
                           "metadata": {}, "outputs": [], "execution_count": None}],
                "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    _patch_common(monkeypatch, tmp_path, notebook, [])
    monkeypatch.setattr(migrator, "EXPORT_BASE", "exported/notebooks")

    async def analyze(cells, *_args, **_kwargs):
        return [{"cell_index": 0, "action": "execute", "description": "",
                 "dependencies": ["utils"], "risks": [], "changes_needed": [],
                 "storage_paths": [], "table_refs": [], "fuse_risks": []}]

    async def ensure(_cluster_id, _session, _imports, _output, **kwargs):
        return {"installed": [], "src_roots": [destination],
                "mirror_root_orig": source, "mirror_root_dst": destination}

    monkeypatch.setattr(cell_analyzer, "analyze_notebook_cells", analyze)
    monkeypatch.setattr(lifecycle, "ensure_requirements_installed", ensure)
    kernel = _Kernel()
    result = asyncio.run(migrator.process_notebook(
        f"{source}/first.ipynb", kernel, "test_pip", "task", {},
        run_all=not code_only_dependency,
    ))

    assert result["status"] == (
        "CONVERTED_NOT_VALIDATED" if code_only_dependency else "PASS"
    ), result
    assert any("_copy_python_file_staged(_sp_sib, _dp_sib)" in code
               for code in kernel.executed)
