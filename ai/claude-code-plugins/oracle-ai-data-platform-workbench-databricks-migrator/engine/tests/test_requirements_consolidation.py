from types import SimpleNamespace

import pytest

from dbx_export.__main__ import (
    _attach_only_consolidated_requirements,
    _resolve_pip_spec_conflicts,
    consolidate_cluster_requirements,
)
from dbx_export.dependency_resolver import FileNode, Graph, JobNode, NotebookNode
from dbx_export.requirements import (
    RequirementConsolidationError,
    RequirementSource,
    consolidate_requirement_sources,
    load_compatibility_policy,
)
from dbx_export.job_converter import convert_dbx_job


def _executable_lines(content):
    """Requirement lines only — header/provenance comments excluded (the
    replaced-pin provenance note deliberately quotes old pins; the artifact
    contract is that no stale pin survives as an EXECUTABLE entry)."""
    return [l.strip() for l in content.splitlines()
            if l.strip() and not l.strip().startswith("#")]


def _notebook(source, referenced_by):
    return NotebookNode(
        dbx_path="unused",
        role="task",
        referenced_by=referenced_by,
        nb_dict={"cells": [{"cell_type": "code", "source": source}]},
    )


def test_consolidates_requirements_per_cluster_with_run_dependencies():
    graph = Graph()
    graph.jobs["42"] = JobNode(
        dbx_job_id="42",
        depth=0,
        job_def={
            "settings": {
                "job_clusters": [
                    {"job_cluster_key": "cpu", "new_cluster": {}},
                    {"job_cluster_key": "gpu", "new_cluster": {}},
                ],
                "tasks": [
                    {
                        "task_key": "prepare",
                        "job_cluster_key": "cpu",
                        "notebook_task": {"notebook_path": "/prepare"},
                        "libraries": [{"pypi": {"package": "joblib==1.4.2"}}],
                    },
                    {
                        "task_key": "train",
                        "job_cluster_key": "gpu",
                        "notebook_task": {"notebook_path": "/train"},
                    },
                ],
            }
        },
    )
    graph.notebooks["/prepare"] = _notebook("%pip install requests==2.32.3", ["job:42/task:prepare"])
    graph.notebooks["/shared"] = _notebook("%pip install tqdm", ["/prepare"])
    graph.notebooks["/train"] = _notebook("%pip install lightgbm", ["job:42/task:train"])
    graph.files["/cpu.txt"] = FileNode(
        dbx_source="/cpu.txt",
        kind="requirements",
        referenced_by=["job:42/task:prepare"],
        content=b"scipy==1.12.0\n",
    )
    graph.files["/gpu.txt"] = FileNode(
        dbx_source="/gpu.txt",
        kind="requirements",
        referenced_by=["job:42/job_cluster:gpu"],
        content=b"torch==2.4.0\n",
    )
    graph.files["/cpu-init.sh"] = FileNode(
        dbx_source="/cpu-init.sh",
        kind="init_script",
        referenced_by=["job:42/task:prepare"],
        content=(
            b"#!/bin/bash\n"
            b"pip install tenacity==8.5.0\n"
            b"pip3 install urllib3==2.2.2\n"
            b"/databricks/python/bin/pip install certifi==2024.7.4\n"
        ),
    )

    generated = consolidate_cluster_requirements(graph, "42")
    cpu = graph.files[generated["job:42/job_cluster:cpu"]].content.decode()
    gpu = graph.files[generated["job:42/job_cluster:gpu"]].content.decode()

    assert "requests==2.32.3" in cpu
    assert "scipy==1.12.0" in cpu
    assert "tqdm" in cpu
    assert "joblib==1.4.2" in cpu
    assert "tenacity==8.5.0" in cpu
    assert "urllib3==2.2.2" in cpu
    assert "certifi==2024.7.4" in cpu
    assert "lightgbm" not in cpu
    assert "torch==2.4.0" not in cpu
    assert "lightgbm" in gpu
    assert "torch==2.4.0" in gpu
    assert "scipy==1.12.0" not in gpu

    converted = SimpleNamespace(
        library_refs=[
            {"requirements": "/cpu.txt"},
            {"pypi": {"package": "joblib==1.4.2"}},
            {"jar": "/x.jar"},
        ]
    )
    selected = _attach_only_consolidated_requirements(
        converted, graph, "job:42/job_cluster:cpu"
    )
    assert converted.library_refs == [
        {"jar": "/x.jar"},
        {"requirements": selected},
    ]


def test_requirement_tab_comment_does_not_create_false_pin_conflict():
    content = consolidate_requirement_sources([
        RequirementSource("one", "numpy==1.26.4\t# runtime pin\n"),
        RequirementSource("two", "numpy==1.26.4 # same pin\n"),
    ])

    assert "numpy==1.26.4" in content
    assert "runtime pin" not in content


def test_duplicate_bare_requirement_is_absorbed_by_pin():
    content = consolidate_requirement_sources([
        RequirementSource("one", "pydantic\n"),
        RequirementSource("two", "pydantic<2\n"),
    ])
    assert "pydantic<2" in content
    assert "\npydantic\n" not in content


def test_conflicting_exact_pins_resolve_to_highest():
    # Highest-wins across sources: a version clash must never block cluster
    # creation. The highest pin is kept and the collapse is reported.
    events = []
    content = consolidate_requirement_sources(
        [
            RequirementSource("one", "numpy==1.24.3\n"),
            RequirementSource("two", "numpy==1.26.4\n"),
        ],
        on_conflict=events.append,
    )
    body = [ln for ln in content.splitlines() if ln and not ln.startswith("#")]
    assert "numpy==1.26.4" in body
    # The dropped pin must NOT be in the installed body, but SHOULD be named in
    # the header audit comment (so the collapse is traceable).
    assert not any("numpy==1.24.3" in ln for ln in body)
    assert events and events[0]["name"] == "numpy"
    assert events[0]["chosen"] == "1.26.4"
    assert sorted(events[0]["versions"]) == ["1.24.3", "1.26.4"]
    assert events[0]["kind"] == "highest-wins"
    assert any("1.24.3" in d for d in events[0]["dropped"])
    assert any(ln.startswith("#") and "numpy" in ln and "1.24.3" in ln
               for ln in content.splitlines())


def test_exact_pin_wins_over_incompatible_range_across_sources():
    # An exact pin is authoritative; an incompatible range from another source
    # is dropped (with the collapse reported), not raised.
    events = []
    content = consolidate_requirement_sources(
        [
            RequirementSource("task notebook A", "numpy==1.26.4\n"),
            RequirementSource("init script B", "numpy<1.26\n"),
        ],
        on_conflict=events.append,
    )
    body = [ln for ln in content.splitlines() if ln and not ln.startswith("#")]
    assert "numpy==1.26.4" in body
    assert not any("<1.26" in ln for ln in body)   # not installed…
    assert events and events[0]["chosen"] == "1.26.4"
    # …but a single pin overriding a range is labelled pin-override (not
    # "highest") and the dropped range is named.
    assert events[0]["kind"] == "pin-override"
    assert any("<1.26" in d for d in events[0]["dropped"])
    assert any(ln.startswith("#") and "overrides" in ln and "<1.26" in ln
               for ln in content.splitlines())


@pytest.mark.parametrize("pin,other,dropped_fragment", [
    ("foo==1.0", "foo>=2.0", ">=2.0"),
    ("foo==1.5", "foo!=1.5", "!=1.5"),
    ("foo==1.3.0", "foo==1.2.*", "1.2.*"),
    ("foo==2.0", "foo~=1.4", "~=1.4"),
])
def test_pin_override_labels_and_names_dropped_constraint(pin, other, dropped_fragment):
    # A single pin that a co-asserted range/exclusion/wildcard forbids resolves
    # to the pin (never blocks the run) but is labelled pin-override — NOT
    # "highest" — and the dropped constraint is named in header + payload.
    events = []
    content = consolidate_requirement_sources(
        [RequirementSource("A", pin + "\n"), RequirementSource("B", other + "\n")],
        on_conflict=events.append,
    )
    body = [ln for ln in content.splitlines() if ln and not ln.startswith("#")]
    assert pin in body                       # the pin is what installs
    assert events and events[0]["kind"] == "pin-override"
    assert any(dropped_fragment in d for d in events[0]["dropped"])
    assert any(ln.startswith("#") and "overrides" in ln and dropped_fragment in ln
               for ln in content.splitlines())
    assert not any("highest" in ln for ln in content.splitlines())


def test_prerelease_pin_wins_over_stable_and_is_reported():
    events = []
    content = consolidate_requirement_sources(
        [RequirementSource("A", "foo==2.0rc1\n"), RequirementSource("B", "foo==1.9\n")],
        on_conflict=events.append,
    )
    body = [ln for ln in content.splitlines() if ln and not ln.startswith("#")]
    assert "foo==2.0rc1" in body
    assert events and events[0]["chosen"] == "2.0rc1"


def test_consolidation_is_deterministic_across_source_order():
    srcs = [
        RequirementSource("code", "shap==0.39.0\nfoo==1.5\nfoo>=1.0\n"),
        RequirementSource("init", "shap==0.46.0\nbar==2.0\n"),
        RequirementSource("pypi", "shap==0.44.0\nbar==2.0\n"),
    ]
    a = consolidate_requirement_sources(list(srcs))
    b = consolidate_requirement_sources(list(reversed(srcs)))
    # Body (installed lines) is identical regardless of source order.
    body = lambda c: sorted(ln for ln in c.splitlines() if ln and not ln.startswith("#"))
    assert body(a) == body(b)
    # And a repeat of the SAME input is byte-identical (stable header + order).
    assert consolidate_requirement_sources(list(srcs)) == a


@pytest.mark.parametrize("specs,expect", [
    (["foo==1.2.*", "foo==1.3.0"], "foo==1.3.0"),   # concrete beats wildcard
    (["foo==1.0", "foo==bad*", "foo==2.0"], "foo==2.0"),  # unparseable never wins
])
def test_stage1_pip_resolver_no_crash_on_wildcard_or_unparseable(specs, expect):
    # Regression: _pip_version_key returned a Version for parseable and a tuple
    # for unparseable versions, so max() over a mix raised
    # `TypeError: '>' not supported between 'Version' and 'tuple'`.
    resolved, conflicts, _dedups = _resolve_pip_spec_conflicts(specs)
    assert expect in resolved
    # Audit text preserves the ORIGINAL version spelling (the wildcard `1.2.*`
    # must not be truncated to `1.2.` in the warning/header).
    if any("*" in s for s in specs):
        vers = [v for c in conflicts for v in c["versions"]]
        assert any(v.endswith("*") for v in vers), vers


def test_version_spelling_is_not_a_conflict():
    events = []
    content = consolidate_requirement_sources(
        [RequirementSource("A", "foo==1.0\n"), RequirementSource("B", "foo==1.0.0\n")],
        on_conflict=events.append,
    )
    assert events == []          # 1.0 and 1.0.0 are the same release
    body = [ln for ln in content.splitlines() if ln and not ln.startswith("#")]
    assert len(body) == 1 and body[0].startswith("foo==1.0")


def test_per_cluster_conflict_is_recorded_in_the_export_report():
    # Notebook %pip xgboost==1.7.6 vs task PyPI library xgboost==2.0.3 on the
    # same cluster: the file resolves to 2.0.3, and the export report must carry
    # a per-cluster requirements-conflict WARN naming the cluster + the drop —
    # otherwise an operator triaging a run-time import failure sees nothing.
    from dbx_export.report import Report
    graph = Graph()
    graph.jobs["77"] = JobNode(
        dbx_job_id="77", depth=0,
        job_def={"settings": {
            "job_clusters": [{"job_cluster_key": "c1", "new_cluster": {}}],
            "tasks": [{
                "task_key": "t1", "job_cluster_key": "c1",
                "notebook_task": {"notebook_path": "/t1"},
                "libraries": [{"pypi": {"package": "xgboost==2.0.3"}}],
            }],
        }},
    )
    graph.notebooks["/t1"] = _notebook("%pip install xgboost==1.7.6", ["job:77/task:t1"])
    report = Report(run_id="r", dbx_root_job_id="77", export_output="/x", prefix="p")
    generated = consolidate_cluster_requirements(graph, "77", report=report)
    c1 = graph.files[generated["job:77/job_cluster:c1"]].content.decode()
    assert "xgboost==2.0.3" in c1
    assert "xgboost==1.7.6" not in [l for l in c1.splitlines() if not l.startswith("#")]
    conflict_lines = [e for e in report.errors if "requirements-conflict" in e]
    assert conflict_lines, f"no per-cluster conflict recorded: {report.errors}"
    assert any("c1" in e and "xgboost" in e for e in conflict_lines)
    # WARN-prefixed => counted as a warning, not a hard error, by Report.summary
    assert all(e.startswith("WARN ") for e in conflict_lines)


def test_bare_pin_overlaps_every_marker_group_and_conflicts():
    # A marker-less pin applies in every environment, so it overlaps the marked
    # group; conflicting versions there is a genuine unsatisfiable conflict and
    # must fail loudly (documented caveat), unlike two unmarked pins.
    with pytest.raises(RequirementConsolidationError):
        consolidate_requirement_sources([
            RequirementSource("A", "foo==1.0\n"),
            RequirementSource("B", 'foo==2.0; python_version<"3.12"\n'),
        ])


def test_mutually_exclusive_platform_markers_do_not_conflict_hostindependent():
    # platform_system Linux vs Windows never co-apply, so both lines survive with
    # no conflict — and this must hold regardless of the exporting host (the
    # overlap grid uses self-consistent OS triples, not the laptop's values).
    content = consolidate_requirement_sources([
        RequirementSource("A", 'foo==1.0; platform_system=="Linux"\n'),
        RequirementSource("B", 'foo==2.0; platform_system=="Windows"\n'),
    ])
    body = [ln for ln in content.splitlines() if ln and not ln.startswith("#")]
    assert any('foo==1.0' in ln and "Linux" in ln for ln in body)
    assert any('foo==2.0' in ln and "Windows" in ln for ln in body)


def test_redundant_bounds_collapse_to_exact_pin():
    # foo==1.5,>=1.0 -> foo==1.5 (redundant lower bound dropped; no conflict
    # reported because 1.5 satisfies >=1.0).
    events = []
    content = consolidate_requirement_sources(
        [
            RequirementSource("code", "foo==1.5\n"),
            RequirementSource("init", "foo>=1.0\n"),
        ],
        on_conflict=events.append,
    )
    body = [ln for ln in content.splitlines() if ln and not ln.startswith("#")]
    assert "foo==1.5" in body
    assert not any(">=1.0" in ln for ln in body)
    assert events == []  # compatible: clean simplification, not a conflict


def test_conflicting_exact_pins_with_extras_union_and_highest():
    # foo[bar]==1.0 + foo[baz]==2.0 -> foo[bar,baz]==2.0 (extras unioned,
    # highest version kept, collapse reported).
    events = []
    content = consolidate_requirement_sources(
        [
            RequirementSource("code", "foo[bar]==1.0\n"),
            RequirementSource("init", "foo[baz]==2.0\n"),
        ],
        on_conflict=events.append,
    )
    body = [ln for ln in content.splitlines() if ln and not ln.startswith("#")]
    assert "foo[bar,baz]==2.0" in body
    assert events and events[0]["chosen"] == "2.0"


def test_marker_differentiated_pins_stay_separate():
    # Mutually-exclusive markers are NOT a conflict — both branches survive.
    events = []
    content = consolidate_requirement_sources(
        [
            RequirementSource("a", 'foo==1.0; python_version<"3.9"\n'),
            RequirementSource("b", 'foo==2.0; python_version>="3.9"\n'),
        ],
        on_conflict=events.append,
    )
    body = [ln for ln in content.splitlines() if ln and not ln.startswith("#")]
    assert any('foo==1.0; python_version < "3.9"' in ln for ln in body)
    assert any('foo==2.0; python_version >= "3.9"' in ln for ln in body)
    assert events == []  # separate markers, no collapse


@pytest.mark.parametrize(
    "left,right",
    [
        ("pandas>=2.2", "pandas<2"),
        ("pyarrow==12.*", "pyarrow<11"),
    ],
)
def test_disjoint_ranges_fail_with_source_attribution(left, right):
    # Pure-range combinations that no version can satisfy still fail loudly —
    # there is no version to pick.
    with pytest.raises(RequirementConsolidationError) as exc_info:
        consolidate_requirement_sources([
            RequirementSource("task notebook A", left + "\n"),
            RequirementSource("init script B", right + "\n"),
        ])

    message = str(exc_info.value)
    assert "task notebook A" in message
    assert "init script B" in message


def test_overlapping_marker_exact_conflict_fails_loud():
    # Two DIFFERENT marker groups that both apply on py3.9 cannot be collapsed
    # to one version; a conflicting exact pin across them is unsatisfiable there
    # -> fail at export (with attribution) rather than let pip fail at cluster
    # creation.
    with pytest.raises(RequirementConsolidationError, match="incompatible constraints"):
        consolidate_requirement_sources([
            RequirementSource("A", 'foo==1.0; python_version>="3.8"\n'),
            RequirementSource("B", 'foo==2.0; python_version<"3.10"\n'),
        ])


def test_overlapping_marker_exact_vs_range_conflict_fails_loud():
    # ==2.0 and <1.3 both apply on py3.9 -> unsatisfiable -> fail at export, not
    # at pip cluster provisioning. (Exact-vs-range sibling of the exact-vs-exact
    # overlapping-marker case.)
    with pytest.raises(RequirementConsolidationError):
        consolidate_requirement_sources([
            RequirementSource("A", 'foo==2.0; python_version>="3.8"\n'),
            RequirementSource("B", 'foo<1.3; python_version<"3.10"\n'),
        ])


def test_overlapping_markers_no_false_positive_after_highest_wins():
    # marker A pins {1.0, 2.0} -> resolves to 2.0; overlapping marker B pins 2.0.
    # After per-group highest-wins both are 2.0 -> compatible -> must NOT raise.
    content = consolidate_requirement_sources([
        RequirementSource("a1", 'foo==1.0; python_version>="3.8"\n'),
        RequirementSource("a2", 'foo==2.0; python_version>="3.8"\n'),
        RequirementSource("b", 'foo==2.0; python_version<"3.10"\n'),
    ])
    body = [ln for ln in content.splitlines() if ln and not ln.startswith("#")]
    assert all("foo==2.0" in ln for ln in body)
    assert not any("foo==1.0" in ln for ln in body)


@pytest.mark.parametrize("left,right", [
    ("foo===abc", "foo===xyz"),
    ("foo===abc", "foo==1.0"),
])
def test_conflicting_arbitrary_equality_pins_fail_loud(left, right):
    with pytest.raises(RequirementConsolidationError, match="arbitrary-equality"):
        consolidate_requirement_sources([
            RequirementSource("A", left + "\n"),
            RequirementSource("B", right + "\n"),
        ])


def test_arbitrary_equality_pin_is_preserved_verbatim():
    # === is a distinct operator whose RHS need not be a PEP 440 version; it must
    # never be parsed as a Version or rewritten to ==.
    content = consolidate_requirement_sources([
        RequirementSource("a", "foo===abc\n"),   # would crash Version("abc")
        RequirementSource("b", "bar===1.0\n"),   # must stay ===, not ==
    ])
    body = [ln for ln in content.splitlines() if ln and not ln.startswith("#")]
    assert "foo===abc" in body
    assert "bar===1.0" in body
    assert "bar==1.0" not in body


def test_local_wheel_hash_and_comment_lines_are_preserved():
    content = consolidate_requirement_sources([
        RequirementSource(
            "cluster requirements",
            "# retained comment\n"
            "/Workspace/wheels/customer_pkg.whl\n"
            "--hash=sha256:abc123\n",
        )
    ])

    assert "/Workspace/wheels/customer_pkg.whl" in content
    assert "--hash=sha256:abc123" in content
    assert content.index("/Workspace/wheels/customer_pkg.whl") < content.index("--hash")


def test_hash_option_on_requirement_line_is_preserved_verbatim():
    line = "sample_pkg==1.0 --hash=sha256:abc --hash=sha256:def"
    with pytest.warns(UserWarning, match="preserving pip-specific requirement"):
        content = consolidate_requirement_sources([RequirementSource("hashed input", line)])
    assert line in content.splitlines()


def test_target_baseline_applies_tensorflow_aidp_compatibility_policy():
    graph = Graph()
    graph.jobs["908"] = JobNode(
        dbx_job_id="908",
        depth=0,
        job_def={
            "settings": {
                "job_clusters": [
                    {"job_cluster_key": "scoring", "new_cluster": {}}
                ],
                "tasks": [{
                    "task_key": "score",
                    "job_cluster_key": "scoring",
                    "notebook_task": {"notebook_path": "/score"},
                }],
            }
        },
    )
    graph.notebooks["/score"] = _notebook(
        "%pip install tensorflow==2.13.0 typing_extensions==4.5.0",
        ["job:908/task:score"],
    )

    generated = consolidate_cluster_requirements(graph, "908")
    content = graph.files[
        generated["job:908/job_cluster:scoring"]
    ].content.decode()

    assert "# Sources: target runtime baseline, notebook %pip: /score" in content
    assert "anthropic" not in content
    assert "websocket-client" not in content
    # numpy/pandas are protected_packages since 2026-09-09 (operator decision:
    # the installed profile versions must not change) — the exact pins now
    # supersede the target-runtime ranges even under the tensorflow rule.
    # TF 2.14.1 requires numpy>=1.23.5,<2, which ==1.26.4 satisfies.
    assert "numpy==1.26.4" in content
    assert "pandas==2.2.3" in content
    assert "numpy<2,>=1.23.5" not in content
    assert "tensorflow==2.14.1" in content
    assert "typing-extensions==4.16.0" in content
    assert not any(l.startswith("tensorflow==2.13.0") for l in _executable_lines(content))
    assert "typing-extensions==4.5.0" not in content
    assert (
        "# Applied compatibility rules: "
        "tensorflow-2.13-typing-extensions-aidp-runtime"
    ) in content


def test_target_baseline_rewrites_tensorflow_2_12_to_keras2_ceiling():
    """TF 2.12 caps numpy<1.24 and collides with the protected numpy==1.26.4
    (ResolutionImpossible, observed 2026-09 at cluster readiness). The policy
    must rewrite it to TF 2.15 + keras 2.15 — the Keras-2 ceiling that accepts
    numpy 1.26.4 — pinning keras in lockstep and NEVER crossing into TF 2.16/
    Keras 3.
    """
    graph = Graph()
    graph.jobs["700"] = JobNode(
        dbx_job_id="700",
        depth=0,
        job_def={
            "settings": {
                "job_clusters": [
                    {"job_cluster_key": "ml", "new_cluster": {}}
                ],
                "tasks": [{
                    "task_key": "train",
                    "job_cluster_key": "ml",
                    "notebook_task": {"notebook_path": "/train"},
                }],
            }
        },
    )
    graph.notebooks["/train"] = _notebook(
        "%pip install tensorflow==2.12.0 keras==2.12.0",
        ["job:700/task:train"],
    )

    generated = consolidate_cluster_requirements(graph, "700")
    content = graph.files[
        generated["job:700/job_cluster:ml"]
    ].content.decode()

    # numpy stays protected; TF+keras move to the Keras-2 ceiling in lockstep.
    assert "numpy==1.26.4" in content
    assert "tensorflow==2.15.0" in content
    assert "keras==2.15.0" in content
    assert not any(l.startswith("tensorflow==2.12.0") for l in _executable_lines(content))
    assert not any(l.startswith("keras==2.12.0") for l in _executable_lines(content))
    # Never Keras 3 (would install then break the exported Keras-2 code).
    assert "tensorflow==2.16" not in content
    assert "keras==3" not in content
    assert (
        "tensorflow-2.12-numpy-1.26-aidp-runtime"
    ) in content


def test_policy_rewrite_preserves_extras_and_environment_marker():
    policy = load_compatibility_policy()
    content = consolidate_requirement_sources(
        [RequirementSource(
            "workflow",
            "tensorflow[and-cuda]==2.13.0; sys_platform == 'linux'\n",
        )],
        compatibility_policy=policy,
    )
    assert (
        'tensorflow[and-cuda]==2.14.1; sys_platform == "linux"'
        in content
    )


def test_injected_keras_inherits_marked_tensorflow_marker():
    """False-on-target regression: a marker-gated TF pin that is FALSE on some
    AIDP clusters must NOT cause an unconditional keras install.

    AIDP is multi-arch (ARM/AMD/Intel/GPU), so `platform_machine == 'x86_64'` is
    false on ARM. The TF rewrite keeps the marker (so TF is inactive there), and
    the injected keras must inherit the SAME marker — otherwise keras installs on
    clusters where TF never does (the P1 the reviewer flagged).
    """
    policy = load_compatibility_policy()
    content = consolidate_requirement_sources(
        [RequirementSource(
            "workflow",
            "tensorflow==2.12.0; platform_machine == 'x86_64'\n",  # no keras in source
        )],
        compatibility_policy=policy,
    )
    # TF rewritten to the Keras-2 ceiling, marker preserved.
    assert 'tensorflow==2.15.0; platform_machine == "x86_64"' in content
    # keras injected UNDER THE SAME MARKER — never unconditional.
    assert 'keras==2.15.0; platform_machine == "x86_64"' in content
    assert not any(
        line.strip() == "keras==2.15.0" for line in content.splitlines()
    ), "keras must not be injected unconditionally when TF is marker-gated"


def test_injected_keras_unconditional_when_tensorflow_unmarked():
    """The common case is unchanged: an unmarked TF pin injects keras
    unconditionally (marker propagation is a no-op when there's no marker)."""
    policy = load_compatibility_policy()
    content = consolidate_requirement_sources(
        [RequirementSource("workflow", "tensorflow==2.12.0\n")],  # no marker, no keras
        compatibility_policy=policy,
    )
    assert any(
        line.strip() == "keras==2.15.0" for line in content.splitlines()
    ), "unmarked TF should inject an unconditional keras (no behaviour change)"
    assert "keras==2.15.0;" not in content  # not marker-gated


def test_missing_default_compatibility_policy_warns_and_uses_source_requirements(
    monkeypatch, capsys,
):
    graph = Graph()
    graph.jobs["908"] = JobNode(
        dbx_job_id="908", depth=0,
        job_def={"settings": {
            "job_clusters": [{"job_cluster_key": "score", "new_cluster": {}}],
            "tasks": [{
                "task_key": "score", "job_cluster_key": "score",
                "notebook_task": {"notebook_path": "/score"},
            }],
        }},
    )
    graph.notebooks["/score"] = _notebook(
        "%pip install tensorflow==2.13.0", ["job:908/task:score"]
    )

    monkeypatch.delenv("AIDP_COMPATIBILITY_POLICY_FILE", raising=False)
    monkeypatch.setattr(
        "dbx_export.__main__.load_compatibility_policy",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RequirementConsolidationError("default policy missing")
        ),
    )
    generated = consolidate_cluster_requirements(graph, "908")
    content = graph.files[
        generated["job:908/job_cluster:score"]
    ].content.decode()

    assert "tensorflow==2.13.0" in content
    assert "compatibility policy unavailable" in capsys.readouterr().out


def test_missing_explicit_compatibility_policy_fails_closed(tmp_path):
    with pytest.raises(
        RequirementConsolidationError, match="unable to load compatibility policy"
    ):
        consolidate_cluster_requirements(
            Graph(), "908",
            compatibility_policy_file=str(tmp_path / "missing.yaml"),
        )


def test_missing_environment_compatibility_policy_fails_closed(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv(
        "AIDP_COMPATIBILITY_POLICY_FILE", str(tmp_path / "missing.yaml")
    )
    with pytest.raises(
        RequirementConsolidationError, match="unable to load compatibility policy"
    ):
        consolidate_cluster_requirements(Graph(), "908")


def test_custom_compatibility_policy_is_configurable(tmp_path):
    policy_path = tmp_path / "compatibility.yaml"
    policy_path.write_text(
        """\
schema_version: 1
protected_packages:
  typing_extensions: "==4.16.0"
rules:
  - id: future-tensorflow-rule
    when:
      tensorflow: "==2.15.*"
    requirements:
      tensorflow: "==2.16.1"
""",
        encoding="utf-8",
    )

    policy = load_compatibility_policy(policy_path)
    content = consolidate_requirement_sources(
        [RequirementSource(
            "workflow",
            "tensorflow==2.15.0\ntyping-extensions==4.5.0\n",
        )],
        compatibility_policy=policy,
    )

    assert "tensorflow==2.16.1" in content
    assert "typing-extensions==4.16.0" in content
    assert "future-tensorflow-rule" in content


def test_policy_does_not_guess_for_unpinned_condition(tmp_path):
    policy_path = tmp_path / "compatibility.yaml"
    policy_path.write_text(
        """\
schema_version: 1
protected_packages: {}
rules:
  - id: exact-only
    when:
      tensorflow: "==2.13.*"
    requirements:
      tensorflow: "==2.14.1"
""",
        encoding="utf-8",
    )

    content = consolidate_requirement_sources(
        [RequirementSource("workflow", "tensorflow>=2.13,<3\n")],
        compatibility_policy=load_compatibility_policy(policy_path),
    )

    assert "tensorflow<3,>=2.13" in content
    assert "typing-extensions" not in content
    assert "# Applied compatibility rules: none" in content


def test_protected_package_is_not_injected_when_absent_from_sources(tmp_path):
    policy_path = tmp_path / "compatibility.yaml"
    policy_path.write_text(
        "schema_version: 1\n"
        "protected_packages:\n"
        "  typing_extensions: '==4.16.0'\n"
        "rules: []\n",
        encoding="utf-8",
    )

    content = consolidate_requirement_sources(
        [RequirementSource("workflow", "pandas==2.2.3\n")],
        compatibility_policy=load_compatibility_policy(policy_path),
    )

    assert "pandas==2.2.3" in content
    assert "typing-extensions" not in content


def test_task_level_new_cluster_mapping_is_used_by_workflow_task():
    payload = convert_dbx_job(
        {
            "job_id": "1",
            "settings": {
                "name": "task-cluster-job",
                "tasks": [{
                    "task_key": "score",
                    "new_cluster": {"num_workers": 1},
                    "notebook_task": {"notebook_path": "/score"},
                }],
            },
        },
        "/Workspace/export",
        "ai_tool",
        {},
        dbx_task_key_to_aidp_key={"score": "aidp-task-cluster"},
    )

    assert payload["tasks"][0]["cluster"] == {
        "clusterKey": "aidp-task-cluster"
    }
    assert payload["jobClusters"] == [{"clusterKey": "aidp-task-cluster"}]


FLASH_ATTN_WHEEL_URL = (
    "https://github.com/Dao-AILab/flash-attention/releases/download/"
    "v2.7.4.post1/flash_attn-2.7.4.post1%2Bcu12torch2.5cxx11abiFALSE"
    "-cp311-cp311-linux_x86_64.whl"
)


def _flash_attn_graph(pip_line):
    graph = Graph()
    graph.jobs["1790"] = JobNode(
        dbx_job_id="1790",
        depth=0,
        job_def={
            "settings": {
                "job_clusters": [
                    {"job_cluster_key": "gpu", "new_cluster": {}}
                ],
                "tasks": [{
                    "task_key": "infer",
                    "job_cluster_key": "gpu",
                    "notebook_task": {"notebook_path": "/infer"},
                }],
            }
        },
    )
    graph.notebooks["/infer"] = _notebook(pip_line, ["job:1790/task:infer"])
    return graph


def test_flash_attn_policy_maps_unpinned_source_to_prebuilt_wheel():
    """Incident 2026-10-05 (STANDALONE_GENERIC_E2E 1000000000061_84953597):
    unpinned `!pip install flash-attn` + torch fails cluster-library install —
    the PyPI mirror has sdists only, whose build imports torch (never present
    in the isolated installer env). The policy must map the pair to the
    newest torch line with a published wheel + the direct wheel URL (the only
    installable artifact; verified INSTALLED through the real installer)."""
    graph = _flash_attn_graph("!pip install flash-attn\n!pip install torch")

    generated = consolidate_cluster_requirements(graph, "1790")
    content = graph.files[
        generated["job:1790/job_cluster:gpu"]
    ].content.decode()

    assert f"flash-attn @ {FLASH_ATTN_WHEEL_URL}" in content
    assert "torch==2.5.1" in content
    # the index-resolved forms must be gone: no bare/sdist flash-attn line and
    # no unpinned torch line survive the rule
    lines = [l.strip() for l in content.splitlines() if not l.startswith("#")]
    assert "flash-attn" not in lines
    assert "torch" not in lines
    assert "flash-attn-prebuilt-wheel-torch-2.5-cu12-cp311-glibc214" in content


def test_flash_attn_policy_maps_pinned_source_too():
    """A source-pinned flash-attn (any version) is equally uninstallable from
    the index — the rule replaces every discovered version."""
    graph = _flash_attn_graph("%pip install flash-attn==2.7.3 torch==2.4.0")

    generated = consolidate_cluster_requirements(graph, "1790")
    content = graph.files[
        generated["job:1790/job_cluster:gpu"]
    ].content.decode()

    assert f"flash-attn @ {FLASH_ATTN_WHEEL_URL}" in content
    assert "torch==2.5.1" in content
    assert not any(l.startswith(("flash-attn==2.7.3", "torch==2.4.0"))
               for l in _executable_lines(content))


def test_policy_rule_requirement_rejects_url_outside_rules():
    """Direct URLs stay rule-only: protected_packages with a URL must fail
    the loader (the allowance is scoped, not global)."""
    import pytest
    from dbx_export.requirements import (
        RequirementConsolidationError, _policy_requirement,
    )
    with pytest.raises(RequirementConsolidationError):
        _policy_requirement(
            "flash-attn", f"@ {FLASH_ATTN_WHEEL_URL}", "protected_packages"
        )
    # and the rule path accepts it
    req = _policy_requirement(
        "flash-attn", f"@ {FLASH_ATTN_WHEEL_URL}",
        "rules[0].requirements", allow_url=True,
    )
    assert req.url and req.name == "flash-attn"


def test_flash_attn_rule_injects_torch_when_sources_omit_it():
    """The incident notebooks pip ONLY flash-attn (torch arrives transitively,
    which pip would resolve UNPINNED — breaking the wheel's torch-2.5 ABI
    match). The rule must inject torch==2.5.1 alongside the wheel even when no
    source mentions torch — the same absent-package injection the TF-2.12 rule
    uses for keras (lockstep pin, trigger-marker inheritance)."""
    graph = _flash_attn_graph("!pip install flash-attn")

    generated = consolidate_cluster_requirements(graph, "1790")
    content = graph.files[
        generated["job:1790/job_cluster:gpu"]
    ].content.decode()

    assert f"flash-attn @ {FLASH_ATTN_WHEEL_URL}" in content
    assert "torch==2.5.1" in content


def test_flash_attn_policy_replaces_newer_pin_and_surfaces_substitution():
    """Review hardening (PR #202): a source pinning a NEWER flash-attn than
    the rule's wheel (and a newer torch) is still rewritten — every
    index-resolved flash-attn install fails regardless of version (sdist-only
    on PyPI/mirror), and the newer wheel line itself was live-REJECTED
    (v2.8.3 requires GLIBC_2.32 the runtime lacks). The substitution must be
    SURFACED, not silent: the applied rule id appears in the generated
    header (PR #88 header-reporting contract), so an operator can see the
    workload's own pins were overridden and why."""
    graph = _flash_attn_graph("%pip install flash-attn==2.8.3 torch==2.8.0")

    generated = consolidate_cluster_requirements(graph, "1790")
    content = graph.files[
        generated["job:1790/job_cluster:gpu"]
    ].content.decode()

    assert f"flash-attn @ {FLASH_ATTN_WHEEL_URL}" in content
    assert "torch==2.5.1" in content
    assert not any(l.startswith(("flash-attn==2.8.3", "torch==2.8.0"))
               for l in _executable_lines(content))
    # surfaced, never silent: the rule id is in the header of the artifact
    assert "flash-attn-prebuilt-wheel-torch-2.5-cu12-cp311-glibc214" in content


def test_flash_attn_marker_gated_source_consolidates_with_marker():
    """Round-2 review F1 (executed repro): a marker-gated flash-attn source
    (`flash-attn; sys_platform == 'linux'` — a pip-documented form the
    exporter's own pip discovery feeds in) crashed the export with an uncaught
    packaging.InvalidRequirement: the URL render joined `url;marker` without
    whitespace and packaging's URL token swallowed the semicolon. The wheel
    line must render with the SPACED separator and keep the marker, and the
    injected torch pin must inherit it (trigger-marker inheritance)."""
    graph = _flash_attn_graph('%pip install "flash-attn; sys_platform == \'linux\'"')

    generated = consolidate_cluster_requirements(graph, "1790")
    content = graph.files[
        generated["job:1790/job_cluster:gpu"]
    ].content.decode()

    assert f'flash-attn @ {FLASH_ATTN_WHEEL_URL} ; sys_platform == "linux"' in content
    assert 'torch==2.5.1; sys_platform == "linux"' in content


def test_policy_render_parse_failure_is_structured_not_traceback():
    """Any future rule render packaging rejects must surface as
    RequirementConsolidationError (the exit-15 contract), never an uncaught
    InvalidRequirement."""
    import pytest
    from dbx_export.requirements import (
        RequirementConsolidationError, _reparse_policy_render,
    )
    with pytest.raises(RequirementConsolidationError):
        _reparse_policy_render("flash-attn @ not a url ;; nor a marker")


def test_rule_header_surfaces_replaced_pins():
    """Round-2 review A3: the header must say WHAT the rule overrode, not just
    that it fired — parity with the protected-packages path."""
    graph = _flash_attn_graph("%pip install flash-attn==2.8.3 torch==2.8.0")

    generated = consolidate_cluster_requirements(graph, "1790")
    content = graph.files[
        generated["job:1790/job_cluster:gpu"]
    ].content.decode()

    assert "replaced:" in content
    assert "flash-attn==2.8.3" in content.split("replaced:")[1].splitlines()[0]
    assert "torch==2.8.0" in content.split("replaced:")[1].splitlines()[0]


def test_loader_rejects_url_in_protected_packages(tmp_path):
    """Round-2 review A4: pin the URL-scoping decision at the LOADER, not just
    _policy_requirement's default (a loader mutant passing allow_url=True for
    protected_packages previously survived the suite)."""
    import pytest
    from dbx_export.requirements import (
        RequirementConsolidationError, load_compatibility_policy,
    )
    policy = tmp_path / "policy.yaml"
    # schema_version is required: without it the loader fails at schema
    # validation and this test would pass for the wrong reason (round-3
    # review: the allow_url mutant survived the schema-error version).
    policy.write_text(
        "schema_version: 1\n"
        "protected_packages:\n"
        f"  flash-attn: \"@ {FLASH_ATTN_WHEEL_URL}\"\n",
        encoding="utf-8",
    )
    with pytest.raises(RequirementConsolidationError, match="direct URL"):
        load_compatibility_policy(policy)


def test_no_flash_attn_source_gets_no_injection():
    """Negative: a workload without flash-attn gains neither the wheel nor a
    torch pin from this rule."""
    graph = _flash_attn_graph("%pip install xformers torch==2.3.1")

    generated = consolidate_cluster_requirements(graph, "1790")
    content = graph.files[
        generated["job:1790/job_cluster:gpu"]
    ].content.decode()

    assert "flash-attn" not in content.replace(
        "flash-attn-prebuilt-wheel-torch-2.5-cu12-cp311-glibc214", "")
    assert "torch==2.5.1" not in content
    assert "torch==2.3.1" in content
