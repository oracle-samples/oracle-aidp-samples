"""An ADJACENT-dot attribute access is not an unbound name.

The `datetime` / `timedelta` / `date` rules in `_IMPLICIT_IMPORT_RULES` matched
on `\\b`, which also matches AFTER A DOT, so attribute access was read as a
missing name and an unused import was injected:

    x = datetime.timedelta(days=1)   ->  from datetime import timedelta
    x = datetime.date(2026, 1, 1)    ->  from datetime import date
    x = ts.date()                    ->  from datetime import date
    x = row.timedelta                ->  from datetime import timedelta

The PySpark rules in the same table already carry `(?<![\\w.])to_date\\b`; these
three were simply inconsistent with them.

SCOPE. The import-statement masker (the module/class shadow) is PR #153 and is
covered by `tests/test_spark_implicit_imports.py` — not duplicated here. This
file covers only the lookbehind: the adjacent-dot negatives it fixes, the bare
names that must keep injecting, the trailing guards it leaves alone, and the
spaced-dot residual it does NOT cover.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import job_migrate as jm  # noqa: E402

TAG = "Oracle tool modification"


def injected(cell: str) -> list:
    """Names the injector would import for this cell."""
    return sorted(
        line.split("  #")[0].strip().split()[-1]
        for line in jm._add_missing_imports(cell).split("\n")
        if TAG in line
    )


# ── what the lookbehind fixes ──────────────────────────────────────────────

@pytest.mark.parametrize("cell", [
    "x = datetime.timedelta(days=1)\n",
    "x = datetime.date(2026, 1, 1)\n",
    "x = ts.date()\n",
    "x = row.timedelta\n",
    # A chained access broken over lines still puts the dot ADJACENT to the
    # name, so this is covered too.
    "x = (ts\n     .date())\n",
])
def test_adjacent_dot_access_is_not_treated_as_an_unbound_name(cell):
    assert injected(cell) == []


# ── what it must NOT cost: genuine unbound names still inject ─────────────

@pytest.mark.parametrize("cell,needed", [
    ("x = timedelta(days=1)\n", "timedelta"),
    ("x = date(2026, 1, 1)\n", "date"),
    ("x = datetime(2026, 1, 1)\n", "datetime"),
    ("if isinstance(v, datetime):\n    pass\n", "datetime"),
])
def test_bare_names_still_get_their_import(cell, needed):
    """A genuine unbound name is never `.`- or `\\w`-preceded, so tightening the
    left side cannot drop an import the cell needs."""
    assert needed in injected(cell)


# ── the trailing guards are left untouched; pin them so that stays true ───

@pytest.mark.parametrize("cell", [
    "x = date.today()\n",
    "x = date.fromtimestamp(0)\n",
])
def test_trailing_guard_behaviour_is_unchanged(cell):
    """Pins CURRENT behaviour, not a desired contract.

    `date`'s pre-existing `(?!\\.)` suppresses these. Whether that is right is
    arguable — if `date` is unbound then `date.today()` DOES need the import,
    exactly as `date(...)` does. This change deliberately does not revisit the
    trailing lookaheads, so the test exists only to catch an accidental change
    to them, not to assert they are correct.
    """
    assert injected(cell) == []


# ── the residual this change does NOT cover ───────────────────────────────

@pytest.mark.xfail(
    strict=True,
    reason="accepted residual, PRE-EXISTING and not worsened here: the "
           "lookbehind inspects ONE character, so it only covers a dot "
           "ADJACENT to the name. With a spaced dot the preceding character is "
           "a space, the guard does not fire, and both `datetime` and "
           "`timedelta` inject — a module->class rebind, not just an unused "
           "import. It cannot be widened: Python requires a fixed-width "
           "lookbehind, so `(?<!\\.\\s*)` raises re.error; covering arbitrary "
           "spacing needs token-aware DOT/NAME detection. PEP 8 forbids the "
           "spaced form so it is vanishingly rare. Recorded rather than "
           "silently asserted covered. If this starts passing, the residual "
           "has been fixed — update it.",
)
@pytest.mark.parametrize("cell", [
    "x = datetime . timedelta(days=1)\n",
    "x = ts . date()\n",
])
def test_spaced_dot_access_is_not_guarded(cell):
    assert injected(cell) == []
