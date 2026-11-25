"""Exercise the real exporter and migrator consumers of pip arguments."""

import pytest
from packaging.requirements import Requirement

from dbx_export import __main__ as exporter
from dbx_export.dependency_resolver import FileNode, Graph, JobNode, NotebookNode
from scripts import job_migrate as migrator
from scripts.pip_commands import PIP_VALUE_FLAGS, parse_pip_arguments


_NEW_VALUE_OPTIONS = [
    ("--root-user-action", "ignore"), ("--use-deprecated", "legacy-resolver"),
    ("--use-feature", "fast-deps"), ("--keyring-provider", "auto"),
    ("--resume-retries", "5"),
]


@pytest.mark.parametrize("flag,value", _NEW_VALUE_OPTIONS)
@pytest.mark.parametrize("separator", [" ", "="])
def test_reviewed_value_options_never_become_dependencies(flag, value, separator):
    args = f"{flag}{separator}{value} requests"
    assert exporter._parse_pip_install_line(args) == ["requests"]
    assert migrator._parse_pip_args(args) == (["requests"], [])


@pytest.mark.parametrize("tokens", [
    ["-r", "requirements.txt", "requests"],
    ["--group", "dev", "requests"],
    ["--require-hashes", "opaque-artifact.whl", "requests"],
])
def test_unresolved_provider_forms_raise_structured_inference_barrier(tokens):
    parsed = parse_pip_arguments(" ".join(tokens))
    assert parsed.inference_barrier is True


@pytest.mark.parametrize("args", [
    "-i https://packages.example.invalid/simple requests",
    "-f /tmp/wheels requests",
    "--use-feature fast-deps requests",
])
def test_non_provider_option_forms_do_not_raise_inference_barrier(args):
    parsed = parse_pip_arguments(args)
    assert parsed.inference_barrier is False


_REDIRECTS = [
    ">/tmp/pip.log", "> /tmp/pip.log", "2>/dev/null", "2> /dev/null",
    "> /dev/null 2>&1", "2>&1", ">>/tmp/pip.log", "2>> /tmp/pip.log",
    "&>/tmp/pip.log", "&>> /tmp/pip.log", ">|/tmp/pip.log", "2>&-",
    '>"/tmp/pip log"', r">/tmp/pip\ log", ">/tmp/a> /tmp/b",
]


@pytest.mark.parametrize("redirect", _REDIRECTS)
def test_redirection_operands_are_not_requirements(redirect):
    from scripts.pip_commands import parse_pip_arguments

    args = f"requests {redirect} pandas"
    parsed = parse_pip_arguments(args)
    assert parsed.requirements == ["requests", "pandas"]
    assert parsed.unresolved == []
    assert parsed.notices and all("not reproduced" in note for note in parsed.notices)
    with pytest.warns(UserWarning, match="redirection.*not reproduced"):
        assert exporter._parse_pip_install_line(args) == ["requests", "pandas"]
    assert migrator._parse_pip_args(args) == (["requests", "pandas"], [])


@pytest.mark.parametrize("args,specs", [
    ('"pydantic<2" "requests>=2"', ["pydantic<2", "requests>=2"]),
    ("pydantic<2 requests>=2", ["pydantic<2", "requests>=2"]),
    ('"sample_pkg; python_version < \'3.12\'"', ["sample_pkg; python_version < '3.12'"]),
    ('custom_lib @ "https://example.com/a>part.whl"',
     ["custom_lib @ https://example.com/a>part.whl"]),
    ('"/tmp/2>custom.whl"', ["/tmp/2>custom.whl"]),
    ('"2>custom.whl"', ["2>custom.whl"]),
    (r"2\>custom.whl", ["2>custom.whl"]),
    ('--log ">not-a-redirect" requests', ["requests"]),
])
def test_quoted_or_escaped_data_is_not_a_redirect(args, specs):
    from scripts.pip_commands import parse_pip_arguments

    parsed = parse_pip_arguments(args)
    assert parsed.requirements == specs
    assert parsed.notices == []
    assert exporter._parse_pip_install_line(args) == specs


@pytest.mark.parametrize("flag", ["--index-url", "--log", "-r", "--root-user-action"])
@pytest.mark.parametrize("separator", [";", "&&", "||", "|", "&"])
def test_separator_cannot_be_an_option_value(flag, separator):
    args = f"{flag}{separator} import x"
    names, unresolved = migrator._parse_pip_args(args)
    assert names == []
    assert any("missing option value" in item for item in unresolved)
    with pytest.warns(UserWarning, match="missing option value"):
        assert exporter._parse_pip_install_line(args) == []


@pytest.mark.parametrize("args", [
    "requests >", "requests 2>&", "requests >; echo ignored", 'requests > ""',
])
def test_missing_redirect_target_is_unresolved(args):
    names, unresolved = migrator._parse_pip_args(args)
    assert names == ["requests"]
    assert any("missing redirection target" in item for item in unresolved)


@pytest.mark.parametrize("value", [";", "&&", "||", "|", "&", ">/tmp/output", "#value"])
def test_quoted_option_values_are_data_not_shell_syntax(value):
    args = f"--config-settings '{value}' requests"
    assert exporter._parse_pip_install_line(args) == ["requests"]
    assert migrator._parse_pip_args(args) == (["requests"], [])


def test_argv_arguments_are_not_interpreted_as_shell_redirects():
    from scripts.pip_commands import parse_pip_tokens

    parsed = parse_pip_tokens(["--log", ">/tmp/output", "requests", "2>custom.whl"])
    assert parsed.requirements == ["requests", "2>custom.whl"]
    assert parsed.notices == []


@pytest.mark.parametrize("redirect", ["< /tmp/input", "<<EOF", "<<<text"])
def test_input_redirects_remain_explicitly_unresolved(redirect):
    names, unresolved = migrator._parse_pip_args(f"requests {redirect}")
    assert names == ["requests"]
    assert any("input redirection is unsupported" in item for item in unresolved)


@pytest.mark.parametrize("args,specs,names", [
    ("--log /tmp/pip.log requests", ["requests"], ["requests"]),
    ("--target /tmp/site requests", ["requests"], ["requests"]),
    ("--proxy http://user@example.com:8080 requests", ["requests"], ["requests"]),
    ("--timeout 30 requests", ["requests"], ["requests"]),
    ("--no-binary :all: requests", ["requests"], ["requests"]),
    ("--log=/tmp/pip.log requests", ["requests"], ["requests"]),
    ("-t/tmp/site requests", ["requests"], ["requests"]),
    ("'pydantic<2' requests[security]>=2.0",
     ["pydantic<2", "requests[security]>=2.0"], ["pydantic", "requests"]),
    ('"sample_pkg[fast]==1.2; python_version < \'3.12\'"',
     ["sample_pkg[fast]==1.2; python_version < '3.12'"], ["sample_pkg"]),
    ("requests; echo ignored", ["requests"], ["requests"]),
    ("requests # don't install this", ["requests"], ["requests"]),
    ("--upgrade pip requests", ["requests"], ["requests"]),
])
def test_real_parsers_agree_without_losing_specs(args, specs, names):
    assert exporter._parse_pip_install_line(args) == specs
    actual_names, unsupported = migrator._parse_pip_args(args)
    assert actual_names == names
    assert unsupported == []
    assert [Requirement(spec).name for spec in specs] == names


@pytest.mark.parametrize("reference", [
    "custom_lib @ https://example.com/custom.whl",
    "custom_lib[fast]@git+https://example.com/tools.git@v1",
    "custom_lib @ file:///tmp/custom.whl",
    "https://example.com/custom.whl",
    "/tmp/custom.whl",
    "./local_package",
    "/tmp/local_package",
])
def test_exporter_preserves_references_that_migrator_cannot_install_by_name(reference):
    assert exporter._parse_pip_install_line(f"requests {reference} pandas") == [
        "requests", reference, "pandas"]
    names, unsupported = migrator._parse_pip_args(f"requests {reference} pandas")
    assert names == ["requests", "pandas"]
    assert len(unsupported) == 1


def test_exporter_warns_about_unresolved_arguments():
    with pytest.warns(UserWarning, match="pip UNRESOLVED.*-r"):
        specs = exporter._parse_pip_install_line("-rrequirements.txt requests")
    assert specs == ["requests"]


@pytest.mark.parametrize("args,specs", [
    ('custom_lib @ "https://example.com/a;param.whl#sha256=abc" pandas',
     ["custom_lib @ https://example.com/a;param.whl#sha256=abc", "pandas"]),
    ('"custom_lib @ https://example.com/a.whl ; python_version < \'3.12\'" pandas',
     ["custom_lib @ https://example.com/a.whl ; python_version < '3.12'", "pandas"]),
    ("custom_lib @ https://example.com/a.whl; echo ignored",
     ["custom_lib @ https://example.com/a.whl"]),
    ("--log /tmp/pip.log; echo ignored", []),
])
def test_shell_separators_do_not_corrupt_quoted_requirements(args, specs):
    assert exporter._parse_pip_install_line(args) == specs
    names, unsupported = migrator._parse_pip_args(args)
    assert names == (["pandas"] if "pandas" in specs else [])
    assert bool(unsupported) == bool(specs)


@pytest.mark.parametrize("flag", sorted(PIP_VALUE_FLAGS - {
    "-r", "--requirement", "-e", "--editable", "-c", "--constraint",
    "--hash", "--group",
}))
def test_every_known_value_option_has_the_same_consumer_behavior(flag):
    args = f"{flag} option_value sample_pkg[extra]==1.2"
    assert exporter._parse_pip_install_line(args) == ["sample_pkg[extra]==1.2"]
    assert migrator._parse_pip_args(args) == (["sample_pkg"], [])


@pytest.mark.parametrize("separator", [" ", "="])
@pytest.mark.parametrize("group", ["dev", "./project/pyproject.toml:dev"])
def test_group_selectors_are_not_packages(separator, group):
    args = f"foo==1.0 --group{separator}{group} bar"
    with pytest.warns(UserWarning, match="dependency group not resolved"):
        assert exporter._parse_pip_install_line(args) == ["foo==1.0", "bar"]
    names, findings = migrator._parse_pip_args(args)
    assert names == ["foo", "bar"]
    assert len(findings) == 1 and f"--group {group}" in findings[0]


@pytest.mark.parametrize("args", [
    "foo==1.0 --hash sha256:abc", "foo==1.0 --hash=sha256:abc",
    "--hash sha256:abc foo==1.0", "foo --hash sha256:abc --hash sha256:def bar",
    "foo --hash", "foo --hash=", "foo --hash --group dev bar",
])
def test_hash_commands_never_project_to_unhashed_installs(args):
    from scripts.pip_commands import parse_pip_arguments

    parsed = parse_pip_arguments(args)
    assert parsed.installation_blocked
    assert parsed.names == [] and parsed.requirements == []
    assert any("unhashed installation not attempted" in f for f in parsed.unresolved)
    assert not any("not a valid package" in f for f in parsed.unresolved)
    with pytest.warns(UserWarning, match="pip UNRESOLVED"):
        assert exporter._parse_pip_install_line(args) == []
    assert migrator._parse_pip_args(args)[0] == []


def test_hash_blocked_command_does_not_expose_an_uninstalled_identity():
    parsed = parse_pip_arguments("pytz==2024.1 --hash=sha256:abc")

    assert parsed.requirements == []
    assert parsed.names == []
    assert parsed.managed_names == []


def test_identified_wheel_is_a_provider_without_a_global_barrier():
    parsed = parse_pip_arguments(
        "/dbfs/wheels/seaborn-0.12.2-py3-none-any.whl"
    )

    assert parsed.managed_names == ["seaborn"]
    assert parsed.inference_barrier is False


def test_hash_blocked_opaque_artifact_reports_missing_identity():
    from scripts.pip_commands import parse_pip_arguments

    parsed = parse_pip_arguments(
        "https://example.invalid/private-artifact.bin --hash=sha256:abc"
    )

    assert parsed.installation_blocked
    assert parsed.managed_names == []
    assert any(
        "hash-protected dependency has no parseable distribution identity"
        in finding
        for finding in parsed.unresolved
    )


@pytest.mark.parametrize("args", [
    "--require-hashes foo==1.0 bar==2.0",
    "foo==1.0 --require-hashes bar==2.0",
    "foo==1.0 bar==2.0 --require-hashes",
    "--require-hashes foo==1.0 --quiet bar==2.0",
    "foo==1.0 --require-hashes --require-hashes bar==2.0",
])
def test_require_hashes_blocks_both_consumers_without_consuming_a_package(args):
    import shlex
    from scripts.pip_commands import parse_pip_arguments, parse_pip_tokens

    for parsed in (parse_pip_arguments(args), parse_pip_tokens(shlex.split(args))):
        assert parsed.installation_blocked
        assert parsed.names == [] and parsed.requirements == []
        assert [d.spec for d in parsed.dependencies] == ["foo==1.0", "bar==2.0"]
        assert all("--require-hashes" in reason and "unhashed installation not attempted" in reason
                   for reason in parsed.unresolved)
        assert parsed.unresolved
        assert not any("missing option value" in reason for reason in parsed.unresolved)
    with pytest.warns(UserWarning, match="--require-hashes"):
        assert exporter._parse_pip_install_line(args) == []
    names, findings = migrator._parse_pip_args(args)
    assert names == [] and any("--require-hashes" in reason for reason in findings)


@pytest.mark.parametrize("args", [
    "--require-hashes", "foo --require-hashes=true", "foo --require-hashes=false",
    "foo --require-hashes --no-require-hashes",
    "foo --no-require-hashes --require-hashes",
])
def test_require_hashes_is_not_silently_bypassed_by_unsupported_flag_forms(args):
    from scripts.pip_commands import parse_pip_arguments

    parsed = parse_pip_arguments(args)
    assert parsed.installation_blocked
    assert parsed.names == [] and parsed.requirements == []
    assert any("--require-hashes" in reason for reason in parsed.unresolved)


@pytest.mark.parametrize("args", [
    "foo==1.0 --log=--require-hashes bar==2.0",
    "--log --require-hashes foo==1.0 bar==2.0",
    "foo==1.0 bar==2.0 > --require-hashes",
    "foo==1.0 bar==2.0 && echo --require-hashes",
    "foo==1.0 bar==2.0 # --require-hashes",
])
def test_require_hashes_text_outside_flag_position_does_not_block(args):
    from scripts.pip_commands import parse_pip_arguments

    parsed = parse_pip_arguments(args)
    assert not parsed.installation_blocked
    assert parsed.requirements == ["foo==1.0", "bar==2.0"]
    assert parsed.unresolved == []


@pytest.mark.parametrize("flag", ["--hash", "--group"])
@pytest.mark.parametrize("suffix", ["", "=", " --quiet", " && echo ignored"])
def test_reviewed_flags_report_missing_values(flag, suffix):
    from scripts.pip_commands import parse_pip_arguments

    parsed = parse_pip_arguments(f"foo {flag}{suffix}")
    assert any(f"{flag} — missing option value" == f for f in parsed.unresolved)
    assert "ignored" not in parsed.names and "--quiet" not in parsed.names


@pytest.mark.parametrize("tokens,expected", [
    (["foo", "--group", "dev"], ["foo"]),
    (["--group=dev"], []),
    (["foo", "--hash", "sha256:abc"], []),
    (["foo", "--hash=sha256:abc"], []),
    (["--require-hashes", "foo", "bar"], []),
])
def test_argv_consumers_share_hash_and_group_policy(tokens, expected):
    from scripts.pip_commands import parse_pip_tokens

    parsed = parse_pip_tokens(tokens)
    assert parsed.names == expected
    assert parsed.unresolved
    with pytest.warns(UserWarning, match="pip UNRESOLVED"):
        assert exporter._requirements_from_pip_parse(parsed) == expected


@pytest.mark.parametrize("command", [
    "!pip install --log /tmp/pip.log 'sample_pkg[fast]<2'",
    "!pip install --log=/tmp/pip.log 'sample_pkg[fast]<2'",
    "!pip install --root-user-action ignore 'sample_pkg[fast]<2' > /tmp/pip.log 2>&1",
])
def test_exported_requirements_artifacts_do_not_contain_option_values(command):
    graph = Graph()
    graph.jobs["42"] = JobNode(dbx_job_id="42", depth=0, job_def={"settings": {
        "job_clusters": [{"job_cluster_key": "cpu", "new_cluster": {}}],
        "tasks": [{"task_key": "task", "job_cluster_key": "cpu",
                   "notebook_task": {"notebook_path": "/task"}}],
    }})
    graph.notebooks["/task"] = NotebookNode(
        dbx_path="/task", role="task", referenced_by=["job:42/task:task"],
        nb_dict={"cells": [{"cell_type": "code", "source": command}]})
    graph.files["/init.sh"] = FileNode(
        dbx_source="/init.sh", kind="init_script", referenced_by=["job:42/task:task"],
        content=b"pip install --target /tmp/site other_pkg==1.2\n")
    generated = exporter.discover_notebook_pip_deps(graph, "/export", "42")
    harvested = graph.files[generated].content.decode()
    assert "sample_pkg[fast]<2" in harvested
    assert "/tmp/pip.log" not in harvested
    assert "\nignore\n" not in harvested
    consolidated = exporter.consolidate_cluster_requirements(graph, "42")
    content = graph.files[consolidated["job:42/job_cluster:cpu"]].content.decode()
    # Provenance comments may include a checkout under /tmp; only executable
    # requirement entries must exclude pip option values.
    content = "\n".join(line for line in content.splitlines()
                        if not line.lstrip().startswith("#"))
    # Consolidation canonicalizes distribution names, but keeps pins/extras.
    assert "sample-pkg[fast]<2" in content
    assert "other-pkg==1.2" in content
    assert "/tmp/" not in content
    assert "/dev/null" not in content
    assert "\nignore\n" not in content


@pytest.mark.parametrize("source", [
    'subprocess.check_call([sys.executable, "-m", "pip", "install", "--log", "/tmp/pip.log", "requests"])',
    'pip.main(["install", "--log", "/tmp/pip.log", "requests"])',
    'os.system("pip install --log /tmp/pip.log requests")',
])
def test_other_exporter_entry_points_share_argument_rules(source):
    extractors = (exporter._extract_subprocess_pip_specs,
                  exporter._extract_pip_main_specs, exporter._extract_os_system_pip_specs)
    specs = [spec for extract in extractors for spec in extract(source)]
    assert specs == ["requests"]


@pytest.mark.parametrize("args,expected", [
    ("--group dev", None),
    ("real_pkg --hash=sha256:abc", None),
    ("real_pkg --require-hashes", None),
    ("real_pkg --group=dev", "real_pkg"),
])
def test_hash_group_notebook_harvest_does_not_emit_option_values(args, expected):
    graph = Graph()
    graph.notebooks["/task"] = NotebookNode(
        dbx_path="/task", role="task", referenced_by=["job:42/task:task"],
        nb_dict={"cells": [{"cell_type": "code", "source": f"%pip install {args}"}]})
    with pytest.warns(UserWarning, match="pip UNRESOLVED"):
        generated = exporter.discover_notebook_pip_deps(graph, "/export", "42")
    if expected is None:
        assert generated is None
        assert not graph.files
    else:
        content = graph.files[generated].content.decode()
        assert expected in content
        assert 'dev' not in content and 'sha256:' not in content


@pytest.mark.parametrize("args,expected", [("--group dev", []),
                                         ("real_pkg --hash sha256:abc", []),
                                         ("--require-hashes real_pkg", []),
                                         ("real_pkg --group=dev", ["real_pkg"])])
@pytest.mark.parametrize("kind", ["subprocess", "pip_main", "os_system"])
def test_hash_group_other_exporter_entry_points(args, expected, kind):
    import shlex

    argv = ["install", *shlex.split(args)]
    if kind == "subprocess":
        source = f"subprocess.check_call({['python', '-m', 'pip', *argv]!r})"
        extract = exporter._extract_subprocess_pip_specs
    elif kind == "pip_main":
        source = f"pip.main({argv!r})"
        extract = exporter._extract_pip_main_specs
    else:
        source = f"os.system({'pip install ' + args!r})"
        extract = exporter._extract_os_system_pip_specs
    with pytest.warns(UserWarning, match="pip UNRESOLVED"):
        assert extract(source) == expected
