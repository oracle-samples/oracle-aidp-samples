"""`provision --stage-param` has to reach the notebook, or say it cannot.

Found on review, reproduced with the real provision() and a fake transport.
Three ways a stage parameter the operator typed vanished while every step
reported verified:

* A NAME NO STAGE DECLARED. `_params_cell` wrote an override only when the
  StageSpec already listed the key, and no StageSpec listed `tables`,
  `dry-run`, `force` or `target-schema` -- real argparse flags of 01 and 02.
  `provision --stage-param tables=ORDERS --stage-param dry-run=True
  --stage-param mode=overwrite` exited 0: the 02 notebook got
  `mode=overwrite` and neither of the others, so running it would OVERWRITE
  every table in the schema where the operator asked for a dry run of one.
  `run --param`'s own refusal told the operator to use --stage-param for
  exactly those names.
* A KEPT NOTEBOOK. `--reuse-existing` (the documented resume) keeps a stage
  notebook already on the workspace, so `--stage-param schema=CORE` left
  `'schema': None` and the run still reported every step verified.
* A BOOLEAN AS A STRING. `counts=true` rendered `--counts true`, and
  03_reconcile's argparse failed with `unrecognized arguments: true`.

The contract now: each StageSpec declares exactly the flags its script's
argparse accepts (derived from the real parser here, so the two cannot
drift), an undeclared name is refused before anything is called, a switch
takes true/false, a list flag takes a comma-separated value, and
--stage-param with a notebook that would be kept is refused with the flag
that applies it.
"""
import argparse
import importlib.util
import json
import pathlib
import sys

import pytest

import snowmig
from target.provisioning import JOB_SPECS, SCRIPTS_FOLDER, provision
from target.stage_notebooks import STAGES, build_stage_notebook
from test_provisioning import Fake

DATAPLANE = pathlib.Path(__file__).resolve().parents[1] / "dataplane"
_BY_KEY = {s.key: s for s in STAGES}


class _Parsed(Exception):
    pass


def _parser(stage):
    """The stage script's REAL ArgumentParser, captured as main() builds it
    (main() is stopped at parse_args, before it touches Spark)."""
    sys.path.insert(0, str(DATAPLANE))
    spec = importlib.util.spec_from_file_location(
        f"snowmig_params_{stage.key}", DATAPLANE / stage.source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    captured = {}
    original = argparse.ArgumentParser.parse_args

    def capture(self, args=None, namespace=None):
        captured["parser"] = self
        raise _Parsed

    argparse.ArgumentParser.parse_args = capture
    try:
        with pytest.raises(_Parsed):
            module.main([])
    finally:
        argparse.ArgumentParser.parse_args = original
    return captured["parser"]


def _flags(parser):
    return {opt[2:]: action for action in parser._actions
            for opt in action.option_strings
            if opt.startswith("--") and opt != "--help"}


def _argv(nb):
    """Execute the notebook's PARAMS cell exactly as the cluster would."""
    cell = next(c for c in nb["cells"]
                if "PARAMS = {" in "".join(c["source"]))
    scope: dict = {}
    exec("".join(cell["source"]), scope)
    return scope["ARGV"]


_REQUIRED = {"target-catalog": "lake", "schema": "SALES"}


@pytest.mark.parametrize("key", [s.key for s in STAGES])
def test_every_stage_declares_exactly_the_flags_its_script_accepts(key):
    stage = _BY_KEY[key]
    flags = _flags(_parser(stage))
    assert set(stage.params) == set(flags), (
        f"{stage.source}: argparse accepts {sorted(flags)}, the StageSpec "
        f"declares {sorted(stage.params)}")
    for name, action in flags.items():
        if isinstance(action, argparse._StoreTrueAction):
            assert stage.params[name] is False, (name, "is a switch")
        if action.nargs == "*":
            assert name in stage.lists, (name, "takes several values")
        if isinstance(action, argparse._AppendAction):
            assert name in stage.repeated, (name, "is repeatable")
    # The CHOICES too, not just the names: `mode` is declared by 01 and 02
    # with disjoint choices, and a value checked only against the name
    # passed provision and failed argparse on the cluster.
    parsed = {name: tuple(action.choices) for name, action in flags.items()
              if action.choices is not None}
    assert stage.choices == parsed, (
        f"{stage.source}: argparse choices {parsed}, the StageSpec "
        f"records {stage.choices}")


def test_an_undeclared_stage_param_is_refused_before_anything_is_called(
        tmp_path):
    fake = Fake()
    with pytest.raises(ValueError) as exc:
        provision(call=fake, workspace_name="acme", scripts=[],
                  stage_params={"tabels": "ORDERS"}, execute=True, delays=())
    message = str(exc.value)
    assert "tabels" in message
    assert "tables" in message, "the refusal lists the names that exist"
    assert fake.ops == [], "a refused parameter reaches nothing"


def test_the_copy_scope_flags_reach_the_notebook_and_its_parser(tmp_path):
    """The review's scenario: a dry run of ONE table must not become an
    overwrite of the whole schema."""
    fake = Fake()
    provision(call=fake, workspace_name="acme", scripts=[], execute=True,
              delays=(), target_catalog="lake",
              stage_params={"schema": "SALES", "tables": "ORDERS",
                            "dry-run": "true",
                            "copy_schema.mode": "overwrite"})
    body = fake.contents[f"{SCRIPTS_FOLDER}/02_copy_schema.ipynb"]["body"]
    argv = _argv(json.loads(body))
    ns = _parser(_BY_KEY["copy_schema"]).parse_args(argv)
    assert ns.tables == ["ORDERS"]
    assert ns.dry_run is True
    assert ns.mode == "overwrite"


def test_a_boolean_stage_param_is_a_switch_not_a_string():
    stage = _BY_KEY["reconcile"]
    argv = _argv(build_stage_notebook(
        stage, overrides={"counts": "true", **_REQUIRED}))
    assert argv[-1] == "--counts", argv
    assert _parser(stage).parse_args(argv).counts is True
    argv = _argv(build_stage_notebook(
        stage, overrides={"counts": "false", **_REQUIRED}))
    assert "--counts" not in argv


def test_a_switch_given_something_other_than_true_or_false_is_refused():
    from target.stage_notebooks import check_stage_params
    with pytest.raises(ValueError) as exc:
        check_stage_params({"counts": "maybe"})
    assert "counts" in str(exc.value) and "true" in str(exc.value)


def test_list_flags_render_in_the_shape_their_argparse_expects():
    """`--tables` is nargs='*' (one flag, several values); 01's `--schema`
    is action='append' (the flag once per value)."""
    copy = _BY_KEY["copy_schema"]
    argv = _argv(build_stage_notebook(
        copy, overrides={"tables": "ORDERS, LINES", **_REQUIRED}))
    assert _parser(copy).parse_args(argv).tables == ["ORDERS", "LINES"]
    structure = _BY_KEY["structure"]
    argv = _argv(build_stage_notebook(
        structure, overrides={"schema": "A,B", "target-catalog": "lake"}))
    assert _parser(structure).parse_args(argv).schema == ["A", "B"]


def _seeded():
    fake = Fake(workspaces=("acme",), clusters=("migration_assets",),
                jobs=tuple(s["name"] for s in JOB_SPECS))
    for spec in JOB_SPECS:
        fake.contents[f'{SCRIPTS_FOLDER}/{spec["notebook"]}'] = {
            "type": "NOTEBOOK", "body": "console-edited"}
    return fake


def test_a_stage_param_on_a_kept_notebook_is_refused_not_dropped():
    fake = _seeded()
    with pytest.raises(ValueError) as exc:
        provision(call=fake, workspace_name="acme", scripts=[],
                  execute=True, delays=(), reuse_existing=True,
                  stage_params={"schema": "CORE"})
    assert "--refresh-notebooks" in str(exc.value)
    assert fake.ops == []


def test_with_refresh_notebooks_the_stage_param_is_written():
    fake = _seeded()
    res = provision(call=fake, workspace_name="acme", scripts=[],
                    execute=True, delays=(), reuse_existing=True,
                    refresh_notebooks=True, target_catalog="lake",
                    stage_params={"schema": "CORE"})
    body = fake.contents[f"{SCRIPTS_FOLDER}/02_copy_schema.ipynb"]["body"]
    assert "'schema': 'CORE'" in "".join(json.loads(body)["cells"][1]["source"])
    assert res["stage_params"] == {"schema": "CORE"}


def test_run_param_refusal_suggests_stage_param_only_for_declared_names(
        tmp_path):
    args = argparse.Namespace(
        out_dir=str(tmp_path), datalake_ocid="ocid1.aidataplatform.oc1.iad.x",
        workspace="ws", cluster_id=None, catalog=None, backend=None,
        config=None, job="snowmig_02_copy_schema", job_key="k",
        param=["tables=ORDERS", "bogus=1"], poll_seconds=0, max_polls=1,
        cold_start_seconds=60, cold_start_restarts=1)
    with pytest.raises(snowmig.MissingTarget) as exc:
        snowmig.cmd_run(args)
    message = str(exc.value)
    # Qualified with the job's own stage: an unqualified name goes to every
    # stage that declares it, which is not what a --param on ONE job meant.
    assert "--stage-param copy_schema.tables=<value>" in message
    assert "--stage-param bogus" not in message
    assert "--stage-param copy_schema.bogus" not in message
    assert "bogus" in message, "the undeclared name is still named"


def test_the_docs_document_stage_param_and_no_driver_notebook():
    """No README, skill or command named --stage-param, and the provision
    skill still described a 'generated driver notebook' that no longer
    exists -- each stage is one self-contained notebook."""
    root = pathlib.Path(__file__).resolve().parents[2]
    for rel in ("README.md", "skills/snowflake-migrator-overview/SKILL.md",
                "skills/snowflake-provision-environment/SKILL.md"):
        text = (root / rel).read_text(encoding="utf-8")
        assert "--stage-param" in text, rel
    provision_skill = (root / "skills/snowflake-provision-environment/SKILL.md"
                       ).read_text(encoding="utf-8")
    assert "driver notebook" not in provision_skill


# ---------------- a value one declaring stage would reject
#
# Found on review of the fix above. A --stage-param goes into every stage
# that declares the name, but `mode` is declared by 01_create_structure
# (choices ddl-plan / ctas / manifest) AND 02_copy_schema (skip-existing /
# append / overwrite). `provision --execute --stage-param schema=SALES
# --stage-param mode=overwrite` -- the example the help, the README, the
# overview and the NAME=VALUE refusal all gave -- reported every step
# verified and wrote `--mode overwrite` into 01's PARAMS, which 01's
# argparse rejects ("invalid choice: 'overwrite'", exit 2) when its job
# runs. The same argparse-fails-minutes-into-a-run class as `counts=true`.
# Likewise `schema=A,B` is two schemas to 01 (append) but the single literal
# 'A,B' to 02, a schema that does not exist.
#
# Now each StageSpec records its flags' choices (checked against argparse
# above), an unqualified value a declaring stage would reject is refused
# with the stage-qualified form that says which stage it is for, and
# `<stage>.<name>=VALUE` writes into that stage only.


def _written_argvs(fake):
    """ARGV of every stage notebook provision uploaded, by stage key."""
    return {s.key: _argv(json.loads(
                fake.contents[f"{SCRIPTS_FOLDER}/{s.notebook_name}"]["body"]))
            for s in STAGES}


def test_a_value_one_declaring_stage_rejects_is_refused_before_any_call():
    fake = Fake()
    with pytest.raises(ValueError) as exc:
        provision(call=fake, workspace_name="acme", scripts=[], execute=True,
                  delays=(), target_catalog="lake",
                  stage_params={"schema": "SALES", "mode": "overwrite"})
    message = str(exc.value)
    assert "01_create_structure" in message, message
    assert "ddl-plan" in message, "the refusal names what 01 accepts"
    assert "copy_schema.mode=overwrite" in message, (
        "the refusal names the qualified form that works")
    assert fake.ops == [], "a refused parameter reaches nothing"


def test_a_stage_qualified_name_reaches_only_that_stage():
    fake = Fake()
    res = provision(call=fake, workspace_name="acme", scripts=[],
                    execute=True, delays=(), target_catalog="lake",
                    stage_params={"schema": "SALES",
                                  "copy_schema.mode": "overwrite"})
    argvs = _written_argvs(fake)
    # Every ARGV provision wrote parses with its stage's REAL parser; an
    # invalid choice would raise SystemExit(2) here, as it did on the cluster.
    parsed = {key: _parser(_BY_KEY[key]).parse_args(argv)
              for key, argv in argvs.items()}
    assert parsed["copy_schema"].mode == "overwrite"
    assert parsed["structure"].mode == "ddl-plan", "01 keeps its own default"
    assert parsed["copy_schema"].schema == "SALES"
    assert parsed["structure"].schema == ["SALES"], (
        "the unqualified name still reaches every stage declaring it")
    assert not [s for s in res["steps"] if s["verified"] is False]


def test_a_qualified_value_wins_over_the_unqualified_one_for_its_stage():
    """`mode=overwrite` alone is refused because 01 rejects it; with
    `structure.mode=ctas` beside it, 01 is covered and the unqualified value
    lands only on 02."""
    fake = Fake()
    provision(call=fake, workspace_name="acme", scripts=[], execute=True,
              delays=(), target_catalog="lake",
              stage_params={"schema": "SALES", "mode": "overwrite",
                            "structure.mode": "ctas"})
    argvs = _written_argvs(fake)
    assert _parser(_BY_KEY["structure"]).parse_args(
        argvs["structure"]).mode == "ctas"
    assert _parser(_BY_KEY["copy_schema"]).parse_args(
        argvs["copy_schema"]).mode == "overwrite"


def test_several_values_for_a_name_one_declaring_stage_takes_once():
    from target.stage_notebooks import check_stage_params
    with pytest.raises(ValueError) as exc:
        check_stage_params({"schema": "A,B"})
    message = str(exc.value)
    assert "02_copy_schema" in message and "structure.schema=A,B" in message
    # Qualified, each stage gets the shape it takes.
    check_stage_params({"structure.schema": "A,B", "copy_schema.schema": "A"})
    argv = _argv(build_stage_notebook(
        _BY_KEY["structure"],
        overrides={"structure.schema": "A,B", "copy_schema.schema": "A",
                   "target-catalog": "lake"}))
    assert _parser(_BY_KEY["structure"]).parse_args(argv).schema == ["A", "B"]


@pytest.mark.parametrize("params, needle", [
    ({"copy_schema.verify": "sums"}, "counts+sums"),
    ({"verify": "sums"}, "counts+sums"),
    ({"nosuch.mode": "x"}, "copy_schema"),
    ({"reconcile.schema": "X"}, "counts"),
])
def test_a_value_is_checked_against_the_stage_it_would_reach(params, needle):
    """A choice outside the stage's list, an unknown stage, or a name the
    named stage does not declare: each refused, naming what does exist."""
    from target.stage_notebooks import check_stage_params
    with pytest.raises(ValueError) as exc:
        check_stage_params(params)
    assert needle in str(exc.value), str(exc.value)


def test_no_doc_advertises_an_unqualified_mode_value():
    """The help, the NAME=VALUE refusal and the docs gave `mode=overwrite`
    as the example -- the one value that fails 01."""
    import re
    root = pathlib.Path(__file__).resolve().parents[2]
    unqualified = re.compile(r"(?<![\w.])mode=(?:overwrite|append|"
                             r"skip-existing|ddl-plan|ctas|manifest)\b")
    for rel in ("README.md", "engine/snowmig.py",
                "skills/snowflake-migrator-overview/SKILL.md",
                "skills/snowflake-provision-environment/SKILL.md"):
        text = (root / rel).read_text(encoding="utf-8")
        assert not unqualified.search(text), (rel, unqualified.search(text))
