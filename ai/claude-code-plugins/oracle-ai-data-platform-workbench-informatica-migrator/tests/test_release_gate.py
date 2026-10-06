"""Release gate: every product go/no-go criterion in one run.

A single place that checks the criteria that must hold before a release
and prints a legible pass/fail table -- not just a pile of asserts.

Runnable two ways:
    pytest tests/test_release_gate.py
    python3 tests/test_release_gate.py     # standalone, prints the table

Design note: this file deliberately does NOT bend a criterion to make
itself pass. If a check below is false, it stays false and the table (and
the pytest assertion) says so -- a failing criterion here is the most
useful thing this file can produce, not a bug in the file.

The "tests passing" criterion re-runs the rest of the suite via
``--ignore`` (never itself) rather than the naive "shell out to `pytest
tests/`" -- that would recurse into this very file. This keeps the
regression-safety measure ("did the suite stay green") cleanly separate
from this file's own criteria, which are allowed to fail without that
being mistaken for a suite regression.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ENGINE = ROOT / "engine"

# So this also runs as `python3 tests/test_release_gate.py` with no
# PYTHONPATH set up by the caller (pytest gets this for free from
# pyproject.toml's `[tool.pytest.ini_options] pythonpath = ["engine"]`).
for _p in (str(ENGINE), str(ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

TEST_FLOOR = 1500
# One notebook per mapping in tests/fixtures/corpus. Pinned rather than
# counted so that a fixture disappearing shows up here as a failure instead
# of quietly shrinking what the demo proves.
DEMO_EXPECTED_NOTEBOOKS = 11


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""


def _run(cmd: list[str], cwd: Path = ROOT, env: dict | None = None, timeout: int = 600):
    return subprocess.run(
        cmd, cwd=str(cwd), env=env, capture_output=True, text=True, timeout=timeout
    )


def _nb_code_text(notebook_path: str) -> str:
    nb = json.loads(Path(notebook_path).read_text(encoding="utf-8"))
    return "\n".join(
        "".join(c["source"]) if isinstance(c["source"], list) else c["source"]
        for c in nb["cells"] if c["cell_type"] == "code"
    )


# ---------------------------------------------------------------------------
# Individual criteria
# ---------------------------------------------------------------------------

def check_test_floor() -> CheckResult:
    """The suite (everything except this file) must still be >= TEST_FLOOR
    passing, 0 failed -- a pure regression check, kept separate from this
    file's own criteria below (which are allowed to fail; a suite
    regression is not). Raise the floor when tests are added, so silently
    deleting tests cannot pass this check."""
    proc = _run([
        sys.executable, "-m", "pytest", "tests/", "-q",
        "--ignore=tests/test_release_gate.py",
        # Excluded so the floor is the same number on every machine.
        # test_expression_execution.py needs pyspark, a test-only extra;
        # it skips as a whole module when absent, which would drop the
        # count by a dozen and fail this check for a missing optional
        # dependency rather than for anything being wrong. Those tests
        # still run in the suite -- they are just not what this floor is
        # counting.
        "--ignore=tests/test_expression_execution.py",
        # Same reason, and it runs every golden case's notebook on Spark.
        "--ignore=tests/test_golden_execution.py",
    ])
    out = proc.stdout + proc.stderr
    passed_m = re.search(r"(\d+) passed", out)
    failed_m = re.search(r"(\d+) failed", out)
    skipped_m = re.search(r"(\d+) skipped", out)
    n_passed = int(passed_m.group(1)) if passed_m else 0
    n_failed = int(failed_m.group(1)) if failed_m else 0
    n_skipped = int(skipped_m.group(1)) if skipped_m else 0
    # Skipped counts toward the floor. The floor exists to catch tests
    # silently disappearing, and a test skipped for a declared reason has
    # not disappeared. The executable expression tests skip when pyspark is
    # absent -- it is a test-only dependency, so a machine without it would
    # otherwise fail this check for having fewer tests rather than for
    # anything being wrong.
    ok = proc.returncode == 0 and (n_passed + n_skipped) >= TEST_FLOOR and n_failed == 0
    detail = (
        f"{n_passed} passed, {n_skipped} skipped, {n_failed} failed "
        f"(floor {TEST_FLOOR}, this file excluded)"
    )
    if not ok and not passed_m:
        detail += f" -- could not parse pytest output, rc={proc.returncode}"
    return CheckResult(f"Tests passing+skipped >= {TEST_FLOOR}, none failing", ok, detail)


def check_demo_sh() -> CheckResult:
    proc = _run(["bash", "demo.sh"])
    out = proc.stdout + proc.stderr
    m = re.search(r"notebooks=(\d+)\s+error=(\d+)", out)
    if not m:
        return CheckResult(
            f"demo.sh ends error=0 with {DEMO_EXPECTED_NOTEBOOKS} notebooks",
            False, f"could not parse demo.sh output (rc={proc.returncode}): {out[-300:]!r}",
        )
    notebooks, error = int(m.group(1)), int(m.group(2))
    ok = notebooks == DEMO_EXPECTED_NOTEBOOKS and error == 0
    return CheckResult(
        f"demo.sh ends error=0 with {DEMO_EXPECTED_NOTEBOOKS} notebooks",
        ok, f"notebooks={notebooks} error={error}",
    )


def check_plugin_manifest() -> CheckResult:
    p = ROOT / ".claude-plugin" / "plugin.json"
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception as exc:
        return CheckResult("plugin.json valid, name/license/version", False, f"invalid JSON: {exc}")
    expected_name = "oracle-ai-data-platform-workbench-informatica-migrator"
    ok = (
        data.get("name") == expected_name
        and data.get("license") == "MIT"
        and data.get("version") == "0.1.0"
    )
    detail = f"name={data.get('name')!r} version={data.get('version')!r} license={data.get('license')!r}"
    return CheckResult("plugin.json valid, name/license/version", ok, detail)


def _pyproject_version() -> str | None:
    try:
        import tomllib
    except ImportError:  # pragma: no cover - py<3.11 fallback
        import tomli as tomllib  # type: ignore
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return data.get("project", {}).get("version")


def _setup_py_version() -> str | None:
    text = (ROOT / "setup.py").read_text(encoding="utf-8")
    m = re.search(r'version\s*=\s*["\']([^"\']+)["\']', text)
    return m.group(1) if m else None


def _init_py_version() -> str | None:
    text = (ENGINE / "infa2aidp" / "__init__.py").read_text(encoding="utf-8")
    m = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', text)
    return m.group(1) if m else None


def check_version_agreement() -> CheckResult:
    pyproject_v = _pyproject_version()
    setup_v = _setup_py_version()
    init_v = _init_py_version()

    env = dict(os.environ)
    env["PYTHONPATH"] = str(ENGINE) + os.pathsep + env.get("PYTHONPATH", "")
    proc = _run([sys.executable, "-m", "infa2aidp.cli", "version"], env=env)
    cli_m = re.search(r"infa2aidp (\S+)", proc.stdout)
    cli_v = cli_m.group(1) if cli_m else None

    versions = {
        "pyproject.toml": pyproject_v,
        "setup.py": setup_v,
        "__init__.py": init_v,
        "infa2aidp version": cli_v,
    }
    ok = pyproject_v is not None and len(set(versions.values())) == 1
    detail = ", ".join(f"{k}={v}" for k, v in versions.items())
    return CheckResult("All 4 version locations agree at 0.1.0", ok, detail)


def check_engine_contents() -> CheckResult:
    # engine/ holds exactly two packages: infa2aidp (the workstation-side
    # library) and infa_compat (the cluster-side runtime, installed onto the
    # AIDP cluster separately -- see engine/setup.py). Anything else
    # appearing here is scope creep, so this set is updated deliberately
    # rather than loosened.
    #
    # Build and tooling droppings are ignored: they are gitignored, so a
    # developer who has merely run the test suite or `compileall` must not
    # see a false NO-GO.
    IGNORED = {"__pycache__", ".DS_Store", ".pytest_cache", ".mypy_cache"}
    expected = {"infa2aidp", "infa_compat", "requirements.txt", "setup.py"}
    actual = {
        p.name for p in ENGINE.iterdir()
        if p.name not in IGNORED and not p.name.endswith((".egg-info", ".pyc"))
    }
    ok = actual == expected
    return CheckResult(
        "engine/ contains only infa2aidp/, infa_compat/, requirements.txt, setup.py",
        ok, f"engine/ = {sorted(actual)}",
    )


def check_cli_size() -> CheckResult:
    n = len((ENGINE / "infa2aidp" / "cli.py").read_text(encoding="utf-8").splitlines())
    ok = n < 500
    return CheckResult("cli.py under 500 lines", ok, f"{n} lines")


def _claude_model_default_from_source() -> str | None:
    """Read the fallback literal straight out of config.py's source rather
    than importing config and reading config.CLAUDE_MODEL live -- the
    repo's own .env sets CLAUDE_MODEL explicitly, which would silently
    test the local dev environment's override instead of the code's
    actual default.
    """
    text = (ENGINE / "infa2aidp" / "config.py").read_text(encoding="utf-8")
    m = re.search(
        r'CLAUDE_MODEL\s*=\s*_get\(\s*["\']CLAUDE_MODEL["\']\s*,\s*["\']([^"\']+)["\']\s*\)',
        text,
    )
    return m.group(1) if m else None


def _dotenv_claude_model() -> str | None:
    env_path = ROOT / ".env"
    if not env_path.is_file():
        return None
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("CLAUDE_MODEL="):
            return line.split("=", 1)[1].strip()
    return None


def check_claude_model_default() -> CheckResult:
    default = _claude_model_default_from_source()
    ok = default == "claude-opus-5"
    detail = f"config.py source default = {default!r}"
    override = _dotenv_claude_model()
    if override and override != default:
        detail += (
            f" -- note: this repo's local .env currently overrides it to "
            f"{override!r} for live runs; does not change the code default"
        )
    return CheckResult("CLAUDE_MODEL defaults to claude-opus-5", ok, detail)


def check_both_formats_produce_real_notebook() -> CheckResult:
    """One PowerCenter XML fixture and one IICS JSON fixture, both run
    through the actual public run_migration(use_llm=False) entrypoint
    (the rule-based / no-API-key / recommended-default path), must each
    yield a notebook with a real spark.table() read, a real write, and
    zero unresolved names (infa2aidp.generators.code_validation).
    """
    from infa2aidp.migrator import run_migration
    from infa2aidp.generators.code_validation import unresolved_names

    details = []
    ok = True

    # tests/fixtures/corpus/orders_transform.xml carries real SOURCE/TARGET/INSTANCE
    # metadata (see tests/fixtures/powercenter/README.md) -- unlike most of
    # the committed corpus fixtures, which intentionally omit that metadata
    # and so only exercise transformation bodies (a placeholder read, no
    # write; see Part A's fidelity findings). This is the one fixture
    # guaranteed to produce a genuine read+write on the rule-based path.
    xml_fixture = ROOT / "tests" / "fixtures" / "corpus" / "orders_transform.xml"
    with tempfile.TemporaryDirectory() as d:
        result = run_migration(
            [str(xml_fixture)], d, use_llm=False,
            score_confidence=False, skip_lineage=True, skip_optimize=True,
        )
        code = _nb_code_text(result.outcomes[0].notebook_path)
        has_read = "spark.table(" in code
        has_write = ("saveAsTable(" in code) or (".merge(" in code)
        bad = unresolved_names(code)
        xml_ok = has_read and has_write and not bad
        ok = ok and xml_ok
        details.append(
            f"XML({xml_fixture.name}): read={has_read} write={has_write} unresolved={bad or 'none'}"
        )

    # Minimal IICS JSON SOURCE -> TARGET mapping -- same proven shape as
    # tests/test_iics_end_to_end.py's own fixture.
    iics_mapping = {
        "name": "m_iics_customers",
        "transformations": [
            {"name": "SRC_CUST", "type": "SOURCE",
             "fields": [{"name": "CUST_ID", "datatype": "integer"},
                        {"name": "CUST_NAME", "datatype": "string"}]},
            {"name": "TGT_CUST", "type": "TARGET",
             "fields": [{"name": "CUST_ID", "datatype": "integer"},
                        {"name": "CUST_NAME", "datatype": "string"}]},
        ],
        "connections": [
            {"from": {"transformation": "SRC_CUST", "field": "CUST_ID"},
             "to": {"transformation": "TGT_CUST", "field": "CUST_ID"}},
        ],
    }
    with tempfile.TemporaryDirectory() as d:
        json_path = Path(d) / "m_iics_customers.json"
        json_path.write_text(json.dumps(iics_mapping), encoding="utf-8")
        out_dir = Path(d) / "out"
        result = run_migration(
            [str(json_path)], str(out_dir), use_llm=False,
            score_confidence=False, skip_lineage=True, skip_optimize=True,
        )
        code = _nb_code_text(result.outcomes[0].notebook_path)
        has_read = "spark.table(" in code
        has_write = "saveAsTable(" in code
        bad = unresolved_names(code)
        json_ok = has_read and has_write and not bad
        ok = ok and json_ok
        details.append(
            f"JSON(m_iics_customers): read={has_read} write={has_write} unresolved={bad or 'none'}"
        )

    return CheckResult(
        "Both source formats -> real notebook (read+write, 0 unresolved names)",
        ok, "; ".join(details),
    )


def check_fidelity_on_rule_based_path() -> tuple[CheckResult, str]:
    """Part A: source_fidelity must be populated on the rule-based path,
    not only the LLM path. Proven here by running the full demo corpus
    (12 fixtures, 1 version-gate-refused) through the real
    run_migration(use_llm=False) entrypoint and checking the aggregate
    fidelity_summary it returns is non-empty.

    Returns (CheckResult for the table, the full per-fixture report text
    for the narrative behind it).
    """
    from infa2aidp.migrator import run_migration

    corpus = ROOT / "tests" / "fixtures" / "corpus"
    inputs = sorted(str(p) for p in corpus.iterdir() if p.suffix.lower() in (".xml", ".json"))
    with tempfile.TemporaryDirectory() as d:
        result = run_migration(
            inputs, d, use_llm=False, score_confidence=False,
            skip_lineage=True, skip_optimize=True,
        )
        summary = result.fidelity_summary
        report_text = ""
        if summary.get("report_path"):
            report_text = Path(summary["report_path"]).read_text(encoding="utf-8")

        ok = bool(summary) and summary.get("checked", 0) > 0
        if ok:
            detail = (
                f"{summary['checked']} mapping(s) checked, {summary['with_gaps']} with gaps "
                f"(rule-based path, {len(inputs)} corpus fixtures, "
                f"{result.notebooks} notebook(s) produced)"
            )
        else:
            detail = "fidelity_summary is EMPTY on the rule-based path"
        return CheckResult("fidelity_summary populated on the rule-based path (Part A)", ok, detail), report_text


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_all_checks() -> tuple[list[CheckResult], str]:
    results = [
        check_test_floor(),
        check_demo_sh(),
        check_plugin_manifest(),
        check_version_agreement(),
        check_engine_contents(),
        check_cli_size(),
        check_claude_model_default(),
        check_both_formats_produce_real_notebook(),
    ]
    fidelity_result, fidelity_report_text = check_fidelity_on_rule_based_path()
    results.append(fidelity_result)
    return results, fidelity_report_text


def render_table(results: list[CheckResult]) -> str:
    name_w = max(len(r.name) for r in results)
    lines = [
        "Release Gate",
        "=" * 12,
        "",
        f"{'STATUS':6}  {'CRITERION':{name_w}}  DETAIL",
        f"{'-'*6}  {'-'*name_w}  {'-'*40}",
    ]
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        lines.append(f"{status:6}  {r.name:{name_w}}  {r.detail}")
    n_pass = sum(r.passed for r in results)
    lines.append("")
    lines.append(f"{n_pass}/{len(results)} criteria passed")
    lines.append("GO" if n_pass == len(results) else "NO-GO")
    return "\n".join(lines)


def test_release_gate(capsys):
    results, _fidelity_report = run_all_checks()
    table = render_table(results)
    with capsys.disabled():
        print("\n" + table)
    failed = [r for r in results if not r.passed]
    assert not failed, (
        "Release gate: NO-GO. Failing criteria:\n"
        + "\n".join(f"- {r.name}: {r.detail}" for r in failed)
    )


def main() -> int:
    results, _fidelity_report = run_all_checks()
    print(render_table(results))
    return 0 if all(r.passed for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
