"""Whole-line comments must not create runtime notebook dependencies.

Regression for run aaaaaaaa-0000-4000-8000-000000000200: a Scala
// notebook.run call triggered original-notebook fallback and CHILD_PERSISTENCE.
Exercise the real parent/child loops; only the kernel and external I/O are fake.
"""
import pytest

import test_child_exit_conversion as harness


@pytest.mark.parametrize("entry", ["child", "parent"])
@pytest.mark.parametrize("api", ["dbutils", "oidlUtils"])
@pytest.mark.parametrize("language,marker", [("scala", "//"), ("python", "#"),
                                             ("sql", "--")])
def test_commented_call_never_inlines_child(monkeypatch, tmp_path, entry, api,
                                           language, marker):
    source = (f"%{language}\n"
              f"  {marker} {api}.notebook.run(\"/Workspace/dead\", 3600,\n"
              f"  {marker} Map(\"arg1\" -> \"fixture\"))")
    result, saved, kernel, _, logs = harness._run(
        monkeypatch, tmp_path,
        {"child": [source], "dead": ["DEAD_CHILD_SENTINEL = 1"]},
        parent=[source] if entry == "parent" else None)
    assert result["status"] == ("PASS" if entry == "parent" else "ok"), result
    assert not any("DEAD_CHILD_SENTINEL" in code for code in kernel.executed)
    assert not any("inlining nested" in line for line in logs)
    assert saved["child"] == [source]


@pytest.mark.parametrize("entry", ["child", "parent"])
@pytest.mark.parametrize("api", ["dbutils", "oidlUtils"])
@pytest.mark.parametrize("language,marker", [("scala", "//"), ("python", "#")])
def test_commented_decoy_does_not_hide_active_call(monkeypatch, tmp_path, entry,
                                                 api, language, marker):
    source = (f"%{language}\n"
              f"{marker} {api}.notebook.run(\"/Workspace/dead\", 3600)\n"
              f"{api}.notebook.run(\"/Workspace/live\", 3600)")
    result, _, kernel, _, _ = harness._run(
        monkeypatch, tmp_path,
        {"child": [source], "dead": ["DEAD_CHILD_SENTINEL = 1"],
         "live": ["LIVE_CHILD_SENTINEL = 1"]},
        parent=[source] if entry == "parent" else None)
    assert result["status"] == ("PASS" if entry == "parent" else "ok"), result
    assert not any("DEAD_CHILD_SENTINEL" in code for code in kernel.executed)
    assert any("LIVE_CHILD_SENTINEL" in code for code in kernel.executed)


@pytest.mark.parametrize("entry", ["child", "parent"])
def test_native_scala_comment_without_magic(monkeypatch, tmp_path, entry):
    original = harness._notebook

    def scala_notebook(sources):
        nb = original(sources)
        nb["metadata"] = {"language_info": {"name": "scala"}}
        return nb

    monkeypatch.setattr(harness, "_notebook", scala_notebook)
    source = '// oidlUtils.notebook.run("/Workspace/dead", 3600)'
    result, _, kernel, _, _ = harness._run(
        monkeypatch, tmp_path,
        {"child": [source], "dead": ["val DEAD_CHILD_SENTINEL = 1"]},
        parent=[source] if entry == "parent" else None)
    assert result["status"] == ("PASS" if entry == "parent" else "ok"), result
    assert not any("DEAD_CHILD_SENTINEL" in code for code in kernel.executed)
