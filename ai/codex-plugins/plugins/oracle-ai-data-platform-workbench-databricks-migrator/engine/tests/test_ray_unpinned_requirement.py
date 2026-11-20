"""Discovered Ray requirements use the protected AIDP version, not every cluster."""

import pytest

from dbx_export.__main__ import (
    consolidate_cluster_requirements,
    discover_notebook_pip_deps,
)
from dbx_export.dependency_resolver import Graph, JobNode, NotebookNode
from dbx_export.requirements import (
    RequirementSource,
    consolidate_requirement_sources,
    load_compatibility_policy,
)


def _graph(ray_source: str) -> Graph:
    graph = Graph()
    graph.jobs["42"] = JobNode(
        dbx_job_id="42",
        depth=0,
        job_def={"settings": {
            "job_clusters": [
                {"job_cluster_key": "ray", "new_cluster": {}},
                {"job_cluster_key": "other", "new_cluster": {}},
            ],
            "tasks": [
                {"task_key": "ray_task", "job_cluster_key": "ray",
                 "notebook_task": {"notebook_path": "/ray"}},
                {"task_key": "other_task", "job_cluster_key": "other",
                 "notebook_task": {"notebook_path": "/other"}},
            ],
        }},
    )
    for path, source in (("/ray", ray_source), ("/other", "!pip install requests")):
        graph.notebooks[path] = NotebookNode(
            dbx_path=path, role="task", referenced_by=[],
            nb_dict={"cells": [{"cell_type": "code", "source": source}]},
        )
    return graph


def _lines(content: bytes) -> list[str]:
    return [line for line in content.decode().splitlines()
            if line and not line.startswith("#")]


@pytest.mark.parametrize("command", ["!pip install ray", "%pip install ray"])
def test_bare_ray_is_pinned_in_both_generated_requirements_files(command):
    graph = _graph(command)
    aggregate = discover_notebook_pip_deps(graph, "/export", "42")
    generated = consolidate_cluster_requirements(graph, "42")

    assert "ray==2.55.0" in _lines(graph.files[aggregate].content)
    assert "ray==2.55.0" in _lines(
        graph.files[generated["job:42/job_cluster:ray"]].content)
    assert not any(line.startswith("ray") for line in _lines(
        graph.files[generated["job:42/job_cluster:other"]].content))
    assert graph.notebooks["/ray"].nb_dict["cells"][0]["source"] == command


@pytest.mark.parametrize(
    "source_requirement",
    [
        "ray==2.4.0",
        "ray>=2.50.0",
        "ray[dashboard,debug,rllib,serve,tune]==2.4.0",
    ],
)
def test_explicit_ray_requirement_is_replaced_by_protected_pin(source_requirement):
    graph = _graph(f"!pip install {source_requirement}")
    aggregate = discover_notebook_pip_deps(graph, "/export", "42")
    generated = consolidate_cluster_requirements(graph, "42")

    aggregate_ray = [
        line for line in _lines(graph.files[aggregate].content)
        if line.startswith("ray")
    ]
    cluster_ray = [
        line for line in _lines(
            graph.files[generated["job:42/job_cluster:ray"]].content
        )
        if line.startswith("ray")
    ]
    expected = (
        "ray[dashboard,debug,rllib,serve,tune]==2.55.0"
        if "[" in source_requirement else "ray==2.55.0"
    )
    assert aggregate_ray == [expected]
    assert cluster_ray == [expected]
    assert graph.notebooks["/ray"].nb_dict["cells"][0]["source"] == (
        f"!pip install {source_requirement}"
    )


def test_protected_ray_pin_beats_explicit_constraint_on_same_cluster():
    graph = _graph("!pip install ray\n!pip install ray>=2.50.0")
    generated = consolidate_cluster_requirements(graph, "42")
    lines = _lines(graph.files[generated["job:42/job_cluster:ray"]].content)

    assert [line for line in lines if line.startswith("ray")] == ["ray==2.55.0"]


def test_protected_ray_pin_replaces_explicit_cluster_library_pin():
    graph = _graph("!pip install ray")
    graph.jobs["42"].job_def["settings"]["tasks"][0]["libraries"] = [
        {"pypi": {"package": "ray==2.50.0"}}
    ]
    generated = consolidate_cluster_requirements(graph, "42")
    lines = _lines(graph.files[generated["job:42/job_cluster:ray"]].content)

    assert [line for line in lines if line.startswith("ray")] == ["ray==2.55.0"]

def test_bare_ray_extras_and_marker_survive_protected_pin():
    policy = load_compatibility_policy()
    content = consolidate_requirement_sources(
        [RequirementSource("notebook", 'ray[default]; python_version >= "3.11"')],
        compatibility_policy=policy,
    )
    assert "ray[default]==2.55.0; python_version >= \"3.11\"" in content
    assert "protected:ray===2.55.0" in content


def test_non_ray_is_unchanged_and_explicit_ray_version_is_replaced():
    policy = load_compatibility_policy()
    content = consolidate_requirement_sources(
        [RequirementSource("notebook", "requests\nray==2.50.0")],
        compatibility_policy=policy,
    )
    assert "requests" in _lines(content.encode())
    assert "ray==2.55.0" in _lines(content.encode())
    assert "ray==2.50.0" in content


def test_import_only_ray_is_pinned_on_owning_cluster():
    graph = _graph("import ray\nray.init(address='auto', ignore_reinit_error=True)\n")
    generated = consolidate_cluster_requirements(graph, "42")

    assert "ray==2.55.0" in _lines(
        graph.files[generated["job:42/job_cluster:ray"]].content)
    assert not any(line.startswith("ray") for line in _lines(
        graph.files[generated["job:42/job_cluster:other"]].content))


def test_protected_ray_pin_replaces_explicit_pin_with_import_inference():
    graph = _graph("import ray\n")
    graph.jobs["42"].job_def["settings"]["tasks"][0]["libraries"] = [
        {"pypi": {"package": "ray==2.50.0"}}
    ]
    generated = consolidate_cluster_requirements(graph, "42")
    lines = _lines(graph.files[generated["job:42/job_cluster:ray"]].content)

    assert [line for line in lines if line.startswith("ray")] == ["ray==2.55.0"]
