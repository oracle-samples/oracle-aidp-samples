"""Exit-tail diagnostics must explain incompleteness without changing verdicts."""
import json

import pytest

from test_pip_magic_gate import _drive_reporting_job, jm
from test_child_exit_conversion import CONDITIONAL, UNCONDITIONAL, _run


DEPENDENCIES = [
    ("-r requirements.txt", "-r requirements.txt"),
    ("custom_pkg @ https://example.com/custom.whl", "direct reference"),
    ("git+https://example.com/repo.git", "not installable by name"),
    ("--group dev", "dependency group not resolved"),
    ("custom_pkg --hash sha256:abc", "unhashed installation not attempted"),
]
EXIT = "if NO_DATA:\n    aidputils.notebook.exit('done')"


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("args,reason", DEPENDENCIES)
def test_parent_exit_tail_dependency_reports_keep_existing_gates(
        monkeypatch, tmp_path, strict, args, reason):
    result, reports, installs, kernel = _drive_reporting_job(
        monkeypatch, tmp_path, [EXIT, f"%pip install {args}\nTAIL_SENTINEL = 42"],
        fire_exit=True, strict=strict)
    task = result["task_results"]["task"]
    assert task["status"] == ("FAIL" if strict else "PARTIAL")
    assert task["pip_unresolved"] == 1
    assert task["ok"] == 1
    assert task["failed"] == int(strict)
    assert installs == []
    for name in (["JOB_REPORT.md"] if strict else
                 ["JOB_REPORT.md", "migration_report.md", "test_report.md"]):
        report = reports[name]
        assert reason in report
        assert "dependency resolution remains incomplete in an unexecuted branch" in report
        assert "no installation was attempted for this cell" in report
        assert "the task is PASS, not a failure" not in report
    assert not any("TAIL_SENTINEL = 42" in code for code in kernel.executed
                   if "base64.b64decode" not in code)
    artifacts = [json.loads(text) for path, text in kernel.files.items() if path.endswith(".ipynb")]
    assert bool(artifacts) is not strict
    if artifacts:
        tail = "".join(artifacts[0]["cells"][-1]["source"])
        assert "installation not attempted" in tail
        assert "dependency resolution incomplete" in tail
        assert "provided by cluster libraries" not in tail
        assert "install FAILED" not in tail
        assert not jm._find_pip_installs(tail)[0]


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("invocation", ["%run ./dep", 'dbutils.notebook.run("./dep", 60)',
                                        '%scala\ndbutils.notebook.run("./dep", 60)'])
@pytest.mark.parametrize("args,reason", DEPENDENCIES)
def test_child_exit_tail_dependency_details_reach_job_reports(
        monkeypatch, tmp_path, strict, invocation, args, reason):
    result, reports, installs, _ = _drive_reporting_job(
        monkeypatch, tmp_path, [invocation], fire_exit=True, strict=strict,
        child_source=[EXIT, f"%pip install {args}\nCHILD_TAIL_SENTINEL = 42"])
    task = result["task_results"]["task"]
    # A sole unresolved %run cell leaves zero successful parent cells, so
    # the existing gate is FAIL even outside strict mode. Do not soften it.
    assert task["status"] == ("FAIL" if strict or invocation.startswith("%run")
                              else "PARTIAL"), task
    assert task["pip_unresolved"] == 1
    assert installs == []
    for name in (["JOB_REPORT.md"] if strict else
                 ["JOB_REPORT.md", "migration_report.md", "test_report.md"]):
        assert reason in reports[name]
        assert "dependency resolution remains incomplete in an unexecuted branch" in reports[name]
        assert "no installation was attempted for this cell" in reports[name]


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("exit_source", [CONDITIONAL, UNCONDITIONAL])
@pytest.mark.parametrize("nested", [False, True])
def test_child_saved_annotation_and_nested_propagation(
        monkeypatch, tmp_path, strict, exit_source, nested):
    notebooks = {"child": [exit_source, "%pip install -r requirements.txt\nLEAF_TAIL = 42"]}
    if nested:
        notebooks["leaf"] = notebooks["child"]
        notebooks["child"] = ["%run ./leaf", "ENCLOSING_TAIL = 42"]
    result, saved, kernel, installs, _ = _run(
        monkeypatch, tmp_path, notebooks, strict=strict,
        parent=["%run /Workspace/child", "PARENT_TAIL = 42"])
    assert result["status"] == "FAIL", result  # No successful parent cell before %run.
    assert result["pip_unresolved"] == 1
    assert installs == []
    details = " ".join(c.get("note", "") for c in result["cell_results"])
    assert "dependency resolution remains incomplete in an unexecuted branch" in details
    assert "no installation was attempted for this cell" in details
    if nested:
        assert "leaf.ipynb" in details
    tail = saved["leaf" if nested else "child"][-1]
    assert "dependency resolution incomplete" in tail
    assert "installation not attempted" in tail
    assert "provided by cluster libraries" not in tail
    assert "install FAILED" not in tail
    assert "LEAF_TAIL = 42" in tail
    assert not jm._find_pip_installs(tail)[0]
    assert not any("_TAIL = 42" in c for c in kernel.executed if "base64.b64decode" not in c)


@pytest.mark.parametrize("strict", [False, True])
def test_clean_exit_tail_is_still_coverage_only(monkeypatch, tmp_path, strict):
    result, reports, installs, _ = _drive_reporting_job(
        monkeypatch, tmp_path, [EXIT, "%pip install supported_pkg\nTAIL_SENTINEL = 42"],
        fire_exit=True, strict=strict)
    task = result["task_results"]["task"]
    assert task["status"] == "PASS_CONDITIONAL_EXIT_COVERAGE"
    assert task["pip_unresolved"] == 0
    assert installs == []
    assert "The task verdict remains **PASS_CONDITIONAL_EXIT_COVERAGE**" in reports["test_report.md"]
    assert "not evidence that the full notebook was runtime-validated" in reports["test_report.md"]
    assert "resolution remains incomplete" not in " ".join(reports.values())


@pytest.mark.parametrize("strict", [False, True])
def test_exit_not_taken_does_not_claim_unexecuted_dependency(monkeypatch, tmp_path, strict):
    result, reports, installs, _ = _drive_reporting_job(
        monkeypatch, tmp_path, [EXIT, "%pip install -r requirements.txt"],
        fire_exit=False, strict=strict)
    assert result["task_results"]["task"]["status"] == ("FAIL" if strict else "PARTIAL")
    assert installs == []
    text = " ".join(reports.values())
    assert "-r requirements.txt" in text
    assert "in an unexecuted branch" not in text
    assert "no installation was attempted for this cell" not in text


@pytest.mark.parametrize("child", [False, True])
@pytest.mark.parametrize("strict", [False, True])
def test_exit_tail_conversion_problem_remains_a_failure(monkeypatch, tmp_path, child, strict):
    sources = [EXIT, "%pip install supported_pkg\n%time x = 1"]
    result, reports, installs, _ = _drive_reporting_job(
        monkeypatch, tmp_path, ["%run ./dep"] if child else sources,
        child_source=sources if child else None, fire_exit=True, strict=strict)
    assert result["task_results"]["task"]["status"] == ("FAIL" if strict or child else "PARTIAL")
    assert installs == []
    assert "conversion remains incomplete in an unexecuted branch" in reports["JOB_REPORT.md"]
    assert "dependency resolution remains incomplete" not in reports["JOB_REPORT.md"]


def test_failed_install_before_exit_is_not_disguised_as_coverage(monkeypatch, tmp_path):
    result, reports, installs, kernel = _drive_reporting_job(
        monkeypatch, tmp_path, ["%pip install broken_pkg", EXIT, "TAIL_SENTINEL = 42"],
        fire_exit=True, install_result=False)
    assert result["task_results"]["task"]["status"] == "PARTIAL"
    assert installs == ["broken_pkg"]
    assert "The task verdict remains **PARTIAL**" in reports["test_report.md"]
    assert "the task is PASS, not a failure" not in reports["test_report.md"]
    assert "no installation was attempted for this cell" not in " ".join(reports.values())
    artifacts = [json.loads(t) for p, t in kernel.files.items() if p.endswith(".ipynb")]
    assert "install FAILED" in "".join(artifacts[0]["cells"][0]["source"])
