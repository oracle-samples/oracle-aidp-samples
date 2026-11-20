"""Fix-7h NameError triage: fail-fast ONLY on genuinely-missing symbols.

Incident (gi_claim_catboost cell 9, 2026-09-08): the notebook source carries
`# import trino` (commented in the CUSTOMER's own code) while `get_sql_df()`
still calls `trino.dbapi.connect`. The triage fast-skipped the NameError as
PARAMETER_MISSING, blocking the Opus fix loop from applying the documented
trino → spark.sql conversion — even though the cell demonstrably ran on DBX
before the imports were commented (display_data outputs in the export).

Contract: a NameError on a name whose import exists in source but is
COMMENTED OUT goes to the Opus fix loop (verdict None). Genuinely-undefined
names still fail fast as PARAMETER_MISSING.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import job_migrate as jm

NE = "NameError: name '{}' is not defined"


def _scope(cells):
    return jm._resolve_symbol_scope("/Workspace/test/nb.ipynb", cells)


TRINO_CELLS = [
    "import pandas as pd\n# import trino\n# import trino.auth \n\n"
    "def get_sql_df(q):\n    connection = trino.dbapi.connect(host='h')\n"
    "    return pd.DataFrame()",
    "df = get_sql_df('select 1')",
]


def test_commented_import_name_goes_to_fix_loop():
    """The incident shape: `# import trino` + body usage → verdict None (Opus)."""
    verdict = jm._check_name_error_can_recover(NE.format("trino"), _scope(TRINO_CELLS))
    assert verdict is None


def test_commented_from_import_also_exempt():
    cells = ["# from pyathena import connect\nx = connect()"]
    assert jm._check_name_error_can_recover(NE.format("pyathena"), _scope(cells)) is None


def test_genuinely_undefined_still_fails_fast():
    verdict = jm._check_name_error_can_recover(
        NE.format("totally_unknown_sym"), _scope(TRINO_CELLS))
    assert verdict is not None
    assert verdict["kind"] == "PARAMETER_MISSING"
    assert verdict["symbol"] == "totally_unknown_sym"


def test_scan_collects_commented_import_names():
    scan = jm._scan_cells_for_defined_symbols(TRINO_CELLS)
    assert "trino" in scan["commented_import_names"]
    # dotted module collapses to the top-level importable name
    cells = ["# import trino.auth"]
    assert "trino" in jm._scan_cells_for_defined_symbols(cells)["commented_import_names"]


def test_plain_comment_is_not_an_import_exemption():
    """A comment merely MENTIONING a module must not exempt it."""
    cells = ["# trino is not used here anymore\nx = 1"]
    scan = jm._scan_cells_for_defined_symbols(cells)
    assert "trino" not in scan["commented_import_names"]
    verdict = jm._check_name_error_can_recover(NE.format("trino"), _scope(cells))
    assert verdict is not None and verdict["kind"] == "PARAMETER_MISSING"


def test_defined_symbol_still_recoverable():
    """Symbol assigned in an earlier cell → verdict None (upstream fix path)."""
    cells = ["value = compute()", "print(value)"]
    assert jm._check_name_error_can_recover(NE.format("value"), _scope(cells)) is None
