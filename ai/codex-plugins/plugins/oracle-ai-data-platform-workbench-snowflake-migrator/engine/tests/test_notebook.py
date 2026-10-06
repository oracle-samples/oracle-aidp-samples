"""Shallow-clone notebook generation. Pure: builds JSON, runs nothing."""
import json

import pytest

from target.notebook import (
    NOTEBOOK_NAME, build_notebook, notebook_workspace_path,
)

DDL = {"statements": [
    {"source_identifier": "D.S.T", "object_type": "TABLE", "target_fqn": "D.S.T",
     "sql": "CREATE TABLE IF NOT EXISTS `D`.`S`.`T` (`A` STRING)\nUSING DELTA"},
    {"source_identifier": "D.S.V", "object_type": "VIEW", "target_fqn": "D.S.V",
     "sql": "CREATE VIEW IF NOT EXISTS `D`.`S`.`V` AS SELECT A FROM D.S.T"}],
    "blocked": [{"source_identifier": "D.S.J", "object_type": "TABLE",
                 "reason": "PAYLOAD: VARIANT"}]}
PLAN = {"bronze_mapping": "database -> Standard Catalog",
        "catalogs_to_create": ["D"], "schemas_to_create": [["D", "S"]],
        "waves": [["D.S.T"], ["D.S.V"]],
        "summary": {"can_migrate": 2, "cannot_migrate": 1}}


def nb():
    return build_notebook(DDL, PLAN, catalog="D",
                          source={"account": "TESTACCT01", "region": "AWS_US_EAST_2"})


# --- structure ------------------------------------------------------------

def test_is_valid_ipynb_json():
    doc = nb()
    assert doc["nbformat"] == 4
    assert doc["cells"]
    json.dumps(doc)          # must be serialisable


def test_first_cell_is_a_markdown_header_stating_no_data_moves():
    first = nb()["cells"][0]
    assert first["cell_type"] == "markdown"
    text = "".join(first["source"]).lower()
    assert "no data" in text
    assert "empty" in text or "structure only" in text


def test_header_states_source_and_destination():
    text = "".join(nb()["cells"][0]["source"])
    assert "TESTACCT01" in text and "AWS_US_EAST_2" in text
    assert "D" in text


def test_only_python_and_markdown_cells():
    for c in nb()["cells"]:
        assert c["cell_type"] in ("code", "markdown")


# --- content --------------------------------------------------------------

def test_schemas_are_created_before_objects():
    code = "\n".join("".join(c["source"]) for c in nb()["cells"]
                     if c["cell_type"] == "code")
    assert code.index("CREATE SCHEMA") < code.index("CREATE TABLE")


def test_every_statement_is_embedded():
    code = "\n".join("".join(c["source"]) for c in nb()["cells"]
                     if c["cell_type"] == "code")
    assert "CREATE TABLE IF NOT EXISTS" in code
    assert "CREATE VIEW IF NOT EXISTS" in code


def test_view_comes_after_its_table():
    code = "\n".join("".join(c["source"]) for c in nb()["cells"]
                     if c["cell_type"] == "code")
    assert code.index("`D`.`S`.`T`") < code.index("`D`.`S`.`V`")


def test_notebook_contains_no_data_movement_statement():
    code = "\n".join("".join(c["source"]) for c in nb()["cells"]
                     if c["cell_type"] == "code").upper()
    for verb in ("INSERT ", "COPY INTO", "MERGE ", "UPDATE ", "DELETE ",
                 "TRUNCATE", "DROP ", "AS SELECT *"):
        assert verb not in code, verb


def test_progress_is_printed_per_object_for_monitoring():
    code = "\n".join("".join(c["source"]) for c in nb()["cells"]
                     if c["cell_type"] == "code")
    assert "print(" in code
    assert "time" in code.lower(), "elapsed time per object enables monitoring"


def test_a_verification_cell_probes_each_object():
    code = "\n".join("".join(c["source"]) for c in nb()["cells"]
                     if c["cell_type"] == "code")
    assert "SHOW TABLES" in code or "SHOW VIEWS" in code


def test_blocked_objects_are_listed_but_not_attempted():
    doc = nb()
    md = "\n".join("".join(c["source"]) for c in doc["cells"]
                   if c["cell_type"] == "markdown")
    code = "\n".join("".join(c["source"]) for c in doc["cells"]
                     if c["cell_type"] == "code")
    assert "D.S.J" in md and "VARIANT" in md
    assert "D.S.J" not in code


def test_only_the_requested_catalog_is_in_the_notebook():
    ddl = json.loads(json.dumps(DDL))
    ddl["statements"].append(
        {"source_identifier": "OTHER.S.X", "object_type": "TABLE",
         "target_fqn": "OTHER.S.X",
         "sql": "CREATE TABLE IF NOT EXISTS `OTHER`.`S`.`X` (`A` STRING) USING DELTA"})
    doc = build_notebook(ddl, PLAN, catalog="D", source={})
    code = "\n".join("".join(c["source"]) for c in doc["cells"]
                     if c["cell_type"] == "code")
    assert "OTHER" not in code


def test_empty_plan_for_a_catalog_raises_rather_than_writing_a_no_op():
    with pytest.raises(ValueError, match="no statements"):
        build_notebook(DDL, PLAN, catalog="NOPE", source={})


# --- placement ------------------------------------------------------------

def test_workspace_path_is_under_shared_and_names_the_catalog():
    path = notebook_workspace_path("D")
    assert path.startswith("/Workspace/Shared/")
    assert "D" in path and path.endswith(".ipynb")


def test_notebook_name_is_recognisable():
    assert "snowmig" in NOTEBOOK_NAME and "shallow" in NOTEBOOK_NAME


# --------------------------------------------------------------------------
# The notebook's verify cell has the same obligation as deploy.
# --------------------------------------------------------------------------

def _verify_source(nb):
    for cell in nb["cells"]:
        src = "".join(cell.get("source", []))
        if "_expected = [" in src:
            return src
    raise AssertionError("no verify cell in the notebook")


def test_verify_cell_escapes_like_wildcards():
    nb = build_notebook(
        {"statements": [{"source_identifier": "D.S.ORDER_ITEMS",
                         "object_type": "TABLE",
                         "target_fqn": "CAT.S.ORDER_ITEMS",
                         "expected_columns": [{"name": "A", "type": "STRING"}],
                         "sql": "CREATE TABLE IF NOT EXISTS `CAT`.`S`.`ORDER_ITEMS` (`A` STRING)"}],
         "blocked": []},
        {"waves": [["D.S.ORDER_ITEMS"]]}, catalog="CAT", source={})
    src = _verify_source(nb)
    assert "_like" in src, "the probe must escape `_` and `%` before LIKE"


def test_verify_cell_compares_the_returned_name_exactly():
    nb = build_notebook(
        {"statements": [{"source_identifier": "D.S.T", "object_type": "TABLE",
                         "target_fqn": "CAT.S.T",
                         "expected_columns": [{"name": "A", "type": "STRING"}],
                         "sql": "CREATE TABLE IF NOT EXISTS `CAT`.`S`.`T` (`A` STRING)"}],
         "blocked": []},
        {"waves": [["D.S.T"]]}, catalog="CAT", source={})
    src = _verify_source(nb)
    # `if _rows:` is not enough -- the row's name must equal the target's.
    assert "_rows else" not in src
    assert ".upper() ==" in src or "_name.upper()" in src


def test_verify_cell_compares_columns_not_only_existence():
    nb = build_notebook(
        {"statements": [{"source_identifier": "D.S.T", "object_type": "TABLE",
                         "target_fqn": "CAT.S.T",
                         "expected_columns": [{"name": "A", "type": "STRING"}],
                         "sql": "CREATE TABLE IF NOT EXISTS `CAT`.`S`.`T` (`A` STRING)"}],
         "blocked": []},
        {"waves": [["D.S.T"]]}, catalog="CAT", source={})
    src = _verify_source(nb)
    assert "DESCRIBE" in src
    assert "_mismatched" in src


def test_every_generated_code_cell_is_valid_python():
    # 650 unit tests passed while the verify cell contained a broken escape,
    # because nothing ever compiled the thing we ship. The notebook is the
    # deliverable the user executes, so its cells must at least parse.
    nb = build_notebook(
        {"statements": [
            {"source_identifier": "D.S.ORDER_ITEMS", "object_type": "TABLE",
             "target_fqn": "CAT.S.ORDER_ITEMS",
             "expected_columns": [{"name": "A", "type": "STRING"}],
             "sql": "CREATE TABLE IF NOT EXISTS `CAT`.`S`.`ORDER_ITEMS` (`A` STRING)"},
            {"source_identifier": "D.S.V", "object_type": "VIEW",
             "target_fqn": "CAT.S.V",
             "expected_columns": [{"name": "A", "type": "STRING"}],
             "sql": "CREATE VIEW IF NOT EXISTS `CAT`.`S`.`V` AS SELECT 1"}],
         "blocked": [{"source_identifier": "D.S.B", "object_type": "TABLE",
                      "reason": "VARIANT column"}]},
        {"waves": [["D.S.ORDER_ITEMS"], ["D.S.V"]]}, catalog="CAT",
        source={"account": "ACC"})
    code_cells = [c for c in nb["cells"] if c["cell_type"] == "code"]
    assert code_cells
    for i, cell in enumerate(code_cells):
        src = "".join(cell["source"])
        compile(src, f"<cell {i}>", "exec")
