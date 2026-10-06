"""Each in-AIDP stage saves its runbook-step values under report/output.

The notebooks run on the cluster, where this process's token report cannot
reach, so each writes its own step result -- S06 discovery, S10 structure,
S11 copy per schema, S11 reconcile -- into the workspace's report/output
folder at the end of its run, next to the accumulated report the CLI
publishes after every stage. `--output-dir ''` switches it off.
"""
import importlib.util
import json
import re
import pathlib
import sys

import pytest

SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "dataplane"
sys.path.insert(0, str(SCRIPTS))


def _load(name):
    sys.path.insert(0, str(SCRIPTS))
    spec = importlib.util.spec_from_file_location(f"so_{name}",
                                                  SCRIPTS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_the_helper_writes_a_named_step_file(tmp_path):
    from snowmig_source import write_step_output
    path = write_step_output(str(tmp_path / "report" / "output"),
                             "S06_discover.json", {"step": "S06", "tables": 3})
    body = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
    assert body["step"] == "S06" and body["tables"] == 3 and body["written_at"]


def test_an_empty_output_dir_switches_it_off(tmp_path):
    from snowmig_source import write_step_output
    assert write_step_output("", "S06_discover.json", {}) is None


def test_a_write_failure_never_raises(tmp_path):
    from snowmig_source import write_step_output
    blocker = tmp_path / "file"
    blocker.write_text("x")
    assert write_step_output(str(blocker / "sub"), "S.json", {}) is None


@pytest.mark.parametrize("script,name", [
    ("00_discover_snowflake", "S06_discover.json"),
    ("01_create_structure", "S10_structure.json"),
    ("02_copy_schema", "S11_copy_"),
    ("03_reconcile", "S11_reconcile.json")])
def test_every_stage_writes_its_step_and_takes_output_dir(script, name):
    src = (SCRIPTS / f"{script}.py").read_text(encoding="utf-8")
    assert "write_step_output(" in src and name in src
    mod = _load(script)
    assert mod.DEFAULT_OUTPUT_DIR == "/Workspace/report/output"
    assert '"--output-dir"' in src


def test_the_structure_stage_writes_s10_with_its_counts(tmp_path, monkeypatch):
    import types
    mod = _load("01_create_structure")

    class _DF:
        def __init__(self, rows=None):
            self._rows = rows or []

        def collect(self):
            return self._rows

    class Spark:
        """S10 DESCRIBEs before creating and again after, so the fake has
        to remember: always answering "no columns" reads as a table that
        exists and is empty, which is drift, not a create."""

        def __init__(self):
            self.made = {}

        def sql(self, s):
            s = " ".join(s.split())
            if s.upper().startswith("DESCRIBE "):
                fqn = s.split(None, 1)[1].strip()
                if fqn not in self.made:
                    raise RuntimeError(f"Table or view not found: {fqn}")
                return _DF([{"col_name": n, "data_type": ty}
                            for n, ty in self.made[fqn]])
            m = re.match(r"(?is)CREATE TABLE IF NOT EXISTS (\S+) \((.*?)\) "
                         r"USING DELTA", s)
            if m:
                depth, part, parts = 0, "", []
                for ch in m.group(2):
                    if ch == "(":
                        depth += 1
                    elif ch == ")":
                        depth -= 1
                    if ch == "," and depth == 0:
                        parts.append(part); part = ""
                    else:
                        part += ch
                parts.append(part)
                cols = []
                for raw in parts:
                    bits = raw.strip().split(None, 1)
                    if bits:
                        cols.append((bits[0].strip("`"),
                                     bits[1].strip() if len(bits) > 1 else ""))
                self.made[m.group(1)] = cols
            return _DF()
    fake = types.ModuleType("pyspark.sql")
    fake.SparkSession = types.SimpleNamespace(
        builder=types.SimpleNamespace(getOrCreate=lambda: Spark()))
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake)
    reports = tmp_path / "reports"
    reports.mkdir()
    (tmp_path / "plan").mkdir()
    (reports / "discovery_manifest.json").write_text(json.dumps({"schemas": [
        {"name": "S", "tables": [{"name": "T", "columns": []}], "views": []}]}))
    (tmp_path / "plan" / "ddl_plan.json").write_text(json.dumps({"statements": [
        {"source_identifier": "D.S.T", "object_type": "TABLE",
         "target_fqn": "cat.d_s.t",
         "expected_columns": [{"name": "A", "type": "INT"}]}]}))
    out = tmp_path / "report" / "output"
    assert mod.main(["--target-catalog", "cat", "--reports-dir", str(reports),
                     "--output-dir", str(out)]) == 0
    s10 = json.loads((out / "S10_structure.json").read_text(encoding="utf-8"))
    assert s10["step"] == "S10" and s10["schemas"]["S"] == {"created": 1}
    assert s10["failures"] == 0


def test_provision_passes_the_output_dir_into_the_notebooks():
    import inspect
    from target import provisioning
    src = inspect.getsource(provisioning.provision)
    assert '"output-dir"' in src


JOB_SCRIPT = {"snowmig_00_discover": "00_discover_snowflake",
              "snowmig_01_structure": "01_create_structure",
              "snowmig_02_copy_schema": "02_copy_schema",
              "snowmig_03_reconcile": "03_reconcile"}


def test_every_workflow_phase_writes_the_step_its_runbook_names():
    """STAGES says which runbook step a workflow implements; the notebook's
    SXX file must say the same, or report/output contradicts the board."""
    import re
    from report.stages import STAGES
    for spec in (s for s in STAGES if s.get("job")):
        src = (SCRIPTS / f'{JOB_SCRIPT[spec["job"]]}.py').read_text(encoding="utf-8")
        written = set(re.findall(r'f?"(S\d\d)_', src))
        expected = {f"S{int(n):02d}" for n in re.findall(r"S(\d+)",
                                                          spec["runbook"])}
        assert written == expected, (spec["stage"], written, expected)


def test_the_committed_notebooks_carry_the_step_output():
    root = SCRIPTS.parents[1] / "data-migration-scripts"
    for script in JOB_SCRIPT.values():
        nb = (root / f"{script}.ipynb").read_text(encoding="utf-8")
        assert "def write_step_output" in nb and "--output-dir" in nb, script
