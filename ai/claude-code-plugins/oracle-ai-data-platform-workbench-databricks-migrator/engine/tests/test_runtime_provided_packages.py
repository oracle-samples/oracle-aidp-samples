"""runtime_provided_packages: platform-integrated packages are unpinned.

Incident (dbx_export, 2026-09-09): the same library appeared with two exact
pins across requirement sources. Unlisted packages with conflicting exact pins
are now consolidated highest-wins (a version clash must never block cluster
creation; the collapse is reported). For packages the AIDP runtime
ships with platform integration (mlflow), the right resolution is neither pin:
installing ANY version over the runtime copy shadows the integration (the
manual-pip numpy _ARRAY_API class). Policy: listed packages collapse to the
bare name — pip judges the runtime version satisfied and leaves it alone —
with every dropped pin reported in the consolidated file's header.

Scope guard: the list is narrow and explicit. Unlisted packages (e.g. xgboost —
model files require the exact training version) keep their pins verbatim.

SUPERSEDING DECISION (2026-09-16, PR #109): unlisted conflicting exact pins are
now consolidated HIGHEST-WINS and the collapse is recorded in the export report
(`requirements-conflict: <cluster>: …`), rather than failing the export. This
explicitly reverses the earlier 2026-09-09 (#88) decision that "unlisted
conflicting pins fail loudly" — that fail-loud gate blocked every multi-task
`per_task` job when a registry/stdout read duplicated (see PR #109 Issue). Only
genuinely unsatisfiable range combinations, conflicting pins across overlapping
markers, and conflicting `===` pins still fail loudly (nothing to pick).
pandas/numpy stay OUT of the list: they are deliberately constrained by
config/default_requirements.txt + target_runtime_requirements.txt.
"""
import os
import sys
import textwrap

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dbx_export.requirements import (
    CompatibilityPolicy,
    RequirementConsolidationError,
    RequirementSource,
    consolidate_requirement_sources,
    load_compatibility_policy,
)


def _policy(tmp_path, body):
    p = tmp_path / "policy.yaml"
    p.write_text(textwrap.dedent(body), encoding="utf-8")
    return load_compatibility_policy(p)


MLFLOW_POLICY = """
    schema_version: 1
    runtime_provided_packages:
      - mlflow
"""


def test_conflicting_mlflow_pins_collapse_to_bare_name(tmp_path):
    """The incident shape: two exact pins of the same runtime-provided
    package across sources → consolidates cleanly to the bare name."""
    policy = _policy(tmp_path, MLFLOW_POLICY)
    out = consolidate_requirement_sources(
        [RequirementSource("cluster spec", "mlflow==2.9.2\n"),
         RequirementSource("notebook %pip", "mlflow==3.1.0\n")],
        compatibility_policy=policy,
    )
    lines = [l for l in out.splitlines() if l and not l.startswith("#")]
    assert lines == ["mlflow"]
    # every dropped pin is visible in the header (no silent decisions)
    assert "runtime-provided:mlflow" in out
    assert "mlflow==2.9.2" in out and "mlflow==3.1.0" in out


def test_single_mlflow_pin_is_also_unpinned(tmp_path):
    policy = _policy(tmp_path, MLFLOW_POLICY)
    out = consolidate_requirement_sources(
        [RequirementSource("cluster spec", "mlflow==2.9.2\n")],
        compatibility_policy=policy,
    )
    lines = [l for l in out.splitlines() if l and not l.startswith("#")]
    assert lines == ["mlflow"]


def test_mlflow_never_injected_when_absent(tmp_path):
    policy = _policy(tmp_path, MLFLOW_POLICY)
    out = consolidate_requirement_sources(
        [RequirementSource("cluster spec", "requests==2.33.1\n")],
        compatibility_policy=policy,
    )
    lines = [l for l in out.splitlines() if l and not l.startswith("#")]
    assert lines == ["requests==2.33.1"]  # mlflow not injected


def test_unlisted_package_keeps_exact_pin_verbatim(tmp_path):
    """xgboost-class guarantee: the model file needs the training version."""
    policy = _policy(tmp_path, MLFLOW_POLICY)
    out = consolidate_requirement_sources(
        [RequirementSource("cluster spec", "xgboost==1.7.6\n")],
        compatibility_policy=policy,
    )
    lines = [l for l in out.splitlines() if l and not l.startswith("#")]
    assert lines == ["xgboost==1.7.6"]


def test_unlisted_conflicting_pins_resolve_to_highest(tmp_path):
    # A package NOT covered by the compatibility policy, pinned to conflicting
    # exact versions across sources, is consolidated highest-wins (never blocks
    # cluster creation) and the collapse is reported.
    policy = _policy(tmp_path, MLFLOW_POLICY)
    events = []
    content = consolidate_requirement_sources(
        [RequirementSource("cluster spec", "xgboost==1.7.6\n"),
         RequirementSource("notebook %pip", "xgboost==2.0.3\n")],
        compatibility_policy=policy,
        on_conflict=events.append,
    )
    assert "xgboost==2.0.3" in content
    assert events and events[0]["name"] == "xgboost"
    assert events[0]["chosen"] == "2.0.3"


def test_policy_rejects_runtime_provided_and_protected_overlap(tmp_path):
    with pytest.raises(RequirementConsolidationError, match="both runtime-provided"):
        _policy(tmp_path, """
            schema_version: 1
            protected_packages:
              mlflow: "==3.0.0"
            runtime_provided_packages:
              - mlflow
        """)


def test_fix7c_stage_unpins_runtime_provided_and_reports_truthfully():
    """Live export 2026-09-10: Fix 7c logged 'kept highest (mlflow==2.21.0)'
    for a pin the consolidation stage then dropped — the two stages must tell
    one story. Stage 1 now emits the bare name and no 'kept highest' WARN for
    runtime-provided packages."""
    from dbx_export.dependency_resolver import Graph, NotebookNode
    from dbx_export.__main__ import discover_notebook_pip_deps
    from dbx_export.report import Report

    g = Graph()

    def nb(path, pip_line):
        return NotebookNode(dbx_path=path, role="root", is_notebook=True, nb_dict={
            "cells": [{"cell_type": "code",
                       "source": f"%pip install {pip_line}\n"}]})

    g.notebooks["/nb1"] = nb("/nb1", "mlflow==2.14.2")
    g.notebooks["/nb2"] = nb("/nb2", "mlflow==2.21.0 xgboost==1.7.6")

    report = Report(run_id="test", dbx_root_job_id="9999", export_output="/tmp/exp", prefix="test")
    uri = discover_notebook_pip_deps(g, "/tmp/exp", "9999", report=report)
    content = g.files[uri].content.decode()
    lines = [l for l in content.splitlines() if l and not l.startswith("#")]
    assert lines == ["mlflow", "xgboost==1.7.6"]
    # no misleading kept-highest conflict recorded for the policy-resolved pkg
    assert not any("mlflow" in e for e in report.errors)
    assert report.pip_findings == []
    assert "kept highest" not in content


def test_protected_replacement_is_reported_in_header(tmp_path):
    """PR #88 review MAJOR 3: a protected-package rewrite must be visible in
    the header, exactly like runtime-provided collapses — silently rewriting
    a workload pin (possibly violating its own bound) is the defect class
    this policy machinery exists to prevent. NOTE the contract: protected
    replacement REPLACES rather than conflicts — a pin the consolidation
    would otherwise reject loudly now passes WITH a report line."""
    policy = _policy(tmp_path, """
        schema_version: 1
        protected_packages:
          numpy: "==1.26.4"
    """)
    out = consolidate_requirement_sources(
        [RequirementSource("notebook %pip", "numpy==2.0.1\n")],
        compatibility_policy=policy,
    )
    lines = [l for l in out.splitlines() if l and not l.startswith("#")]
    assert lines == ["numpy==1.26.4"]
    assert "protected:numpy===1.26.4" in out or "protected:numpy==1.26.4" in out
    assert "numpy==2.0.1" in out  # the dropped pin is named


def test_protected_untouched_pin_reports_nothing(tmp_path):
    policy = _policy(tmp_path, """
        schema_version: 1
        protected_packages:
          numpy: "==1.26.4"
    """)
    out = consolidate_requirement_sources(
        [RequirementSource("baseline", "numpy==1.26.4\n")],
        compatibility_policy=policy,
    )
    # replacement of an identical pin is a no-op — no noisy report line
    assert "(replaced:" not in out


def test_fix7c_protected_conflict_names_every_dropped_pin(capsys):
    """Adversarial-verify finding 2: with TWO conflicting protected pins,
    stage 1 must name BOTH dropped pins (the consolidation header does)."""
    from dbx_export.dependency_resolver import Graph, NotebookNode
    from dbx_export.__main__ import discover_notebook_pip_deps

    g = Graph()
    for i, pin in enumerate(("numpy==2.0.1", "numpy==2.1.0")):
        g.notebooks[f"/nb{i}"] = NotebookNode(
            dbx_path=f"/nb{i}", role="root", is_notebook=True, nb_dict={
                "cells": [{"cell_type": "code",
                           "source": f"%pip install {pin}\n"}]})
    uri = discover_notebook_pip_deps(g, "/tmp/exp", "8888")
    out = capsys.readouterr().out
    assert "numpy==2.0.1" in out and "numpy==2.1.0" in out
    lines = [l for l in g.files[uri].content.decode().splitlines()
             if l and not l.startswith("#")]
    assert lines == ["numpy==1.26.4"]


def test_fix7c_protected_preserves_extras():
    """Adversarial-verify finding 6: pkg[extras]==X keeps extras when the
    protected pin replaces the version (consolidation keeps them too)."""
    from dbx_export.dependency_resolver import Graph, NotebookNode
    from dbx_export.__main__ import discover_notebook_pip_deps

    g = Graph()
    g.notebooks["/nb"] = NotebookNode(
        dbx_path="/nb", role="root", is_notebook=True, nb_dict={
            "cells": [{"cell_type": "code",
                       "source": "%pip install pandas[performance]==2.1.0\n"}]})
    uri = discover_notebook_pip_deps(g, "/tmp/exp", "8889")
    lines = [l for l in g.files[uri].content.decode().splitlines()
             if l and not l.startswith("#")]
    assert lines == ["pandas[performance]==2.2.3"]


def test_fix7c_stage_replaces_protected_pin_with_log():
    """Both stages must tell one story for protected packages too — the
    pip_deps file may not carry numpy==2.0.1 while the consolidated file
    carries ==1.26.4 (PR #88 review, stage-disagreement)."""
    from dbx_export.dependency_resolver import Graph, NotebookNode
    from dbx_export.__main__ import discover_notebook_pip_deps

    g = Graph()
    g.notebooks["/nb"] = NotebookNode(
        dbx_path="/nb", role="root", is_notebook=True, nb_dict={
            "cells": [{"cell_type": "code",
                       "source": "%pip install numpy==2.0.1 xgboost==1.7.6\n"}]})
    uri = discover_notebook_pip_deps(g, "/tmp/exp", "7777")
    lines = [l for l in g.files[uri].content.decode().splitlines()
             if l and not l.startswith("#")]
    assert "numpy==1.26.4" in lines      # protected pin applied at stage 1
    assert "numpy==2.0.1" not in lines
    assert "xgboost==1.7.6" in lines     # unlisted pin untouched


def test_fix7c_runtime_provided_preserves_extras():
    """PR #88 review P2 (param, 2026-09-10): unpinning must not discard
    requested optional dependencies — mlflow[databricks]==2.14.2 →
    mlflow[databricks], never bare mlflow."""
    from dbx_export.dependency_resolver import Graph, NotebookNode
    from dbx_export.__main__ import discover_notebook_pip_deps

    g = Graph()
    g.notebooks["/nb"] = NotebookNode(
        dbx_path="/nb", role="root", is_notebook=True, nb_dict={
            "cells": [{"cell_type": "code",
                       "source": "%pip install mlflow[databricks]==2.14.2\n"}]})
    uri = discover_notebook_pip_deps(g, "/tmp/exp", "9990")
    lines = [l for l in g.files[uri].content.decode().splitlines()
             if l and not l.startswith("#")]
    assert lines == ["mlflow[databricks]"]


def test_fix7c_uses_the_selected_policy_file(tmp_path):
    """PR #88 review P2 (param): a YAML-selected policy file must reach BOTH
    stages — stage 1 previously loaded the repo default, so an operator
    policy with no protected packages still got numpy rewritten (the
    historic env-vs-file config-routing class)."""
    from dbx_export.dependency_resolver import Graph, NotebookNode
    from dbx_export.__main__ import discover_notebook_pip_deps

    empty_policy = tmp_path / "policy.yaml"
    empty_policy.write_text("schema_version: 1\n", encoding="utf-8")

    g = Graph()
    g.notebooks["/nb"] = NotebookNode(
        dbx_path="/nb", role="root", is_notebook=True, nb_dict={
            "cells": [{"cell_type": "code",
                       "source": "%pip install numpy==2.0.1 mlflow==2.9.2\n"}]})
    uri = discover_notebook_pip_deps(
        g, "/tmp/exp", "9991", compatibility_policy_file=str(empty_policy))
    lines = [l for l in g.files[uri].content.decode().splitlines()
             if l and not l.startswith("#")]
    # operator's empty policy: NOTHING rewritten at stage 1
    assert sorted(lines) == ["mlflow==2.9.2", "numpy==2.0.1"]

    # default (no file passed): repo policy applies as before
    g2 = Graph()
    g2.notebooks["/nb"] = NotebookNode(
        dbx_path="/nb", role="root", is_notebook=True, nb_dict={
            "cells": [{"cell_type": "code",
                       "source": "%pip install numpy==2.0.1 mlflow==2.9.2\n"}]})
    uri2 = discover_notebook_pip_deps(g2, "/tmp/exp", "9992")
    lines2 = [l for l in g2.files[uri2].content.decode().splitlines()
              if l and not l.startswith("#")]
    assert sorted(lines2) == ["mlflow", "numpy==1.26.4"]


def _discover_lines(pip_line, job="9995", policy_file=None):
    from dbx_export.dependency_resolver import Graph, NotebookNode
    from dbx_export.__main__ import discover_notebook_pip_deps
    g = Graph()
    g.notebooks["/nb"] = NotebookNode(
        dbx_path="/nb", role="root", is_notebook=True, nb_dict={
            "cells": [{"cell_type": "code",
                       "source": f"%pip install \"{pip_line}\"\n"}]})
    uri = discover_notebook_pip_deps(
        g, "/tmp/exp", job, compatibility_policy_file=policy_file)
    return [l for l in g.files[uri].content.decode().splitlines()
            if l and not l.startswith("#")]


def test_fix7c_preserves_environment_markers_both_branches():
    """PR #88 review round 4 (param): version substitution/unpinning must
    keep the requirement CONDITIONAL — a marker'd source line must never
    become an unconditional install."""
    # runtime-provided branch
    lines = _discover_lines("mlflow[databricks]==2.14.2; python_version < '3.10'",
                            job="9996")
    assert len(lines) == 1
    assert lines[0].startswith("mlflow[databricks]")
    assert "==" not in lines[0]
    assert 'python_version < "3.10"' in lines[0]
    # protected branch
    lines = _discover_lines("numpy==2.0.1; python_version < '3.10'", job="9997")
    assert len(lines) == 1
    assert lines[0].startswith("numpy==1.26.4")
    assert 'python_version < "3.10"' in lines[0]


def test_repo_policy_file_loads_and_lists_mlflow():
    """The committed policy file itself parses and carries the decision."""
    policy = load_compatibility_policy()
    assert "mlflow" in policy.runtime_provided
    assert "pandas" not in policy.runtime_provided
    assert "numpy" not in policy.runtime_provided
