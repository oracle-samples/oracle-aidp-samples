"""Generated notebooks must obey AIDP cluster constraints, not just be
valid Spark. A notebook can be flawless PySpark and still fail on an AIDP
cluster -- nothing else in the test suite checks that.

See references/aidp-runtime-constraints.md for the full, sourced list of
constraints; each check below corresponds to one marked [enforced] there.

Parametrised over all twelve committed fixtures (10 PowerCenter XML +
2 IDMC JSON) rather than a single hand-authored one -- a validator this
cheap is more valuable applied broadly than proven once. If a check fails
on a *current* fixture, the generator is at fault, not the test: fix the
generator, or (if genuinely out of scope, as with the DeltaTable.forName
catalog-type branch -- see the reference doc) tighten the check to what
is actually verifiable rather than weakening it.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from infa2aidp.converters.transformation_converter import TransformationConverter
from infa2aidp.generators.code_validation import unresolved_names
from infa2aidp.generators.notebook_generator import NotebookGenerator
from infa2aidp.models import Session
from infa2aidp.parsers.format_detector import detect_and_parse_file

FIXTURES_DIR = Path(__file__).parent / "fixtures"

# flattened_no_instances.xml is excluded deliberately: it is a stripped
# transformation-only mapping that exists to pin the *degraded* path (no
# source/target wiring -> placeholder read, skipped write). Sweeping it
# through the "every fixture produces complete, valid output" checks below
# would assert the opposite of what it is for. Its own behaviour is pinned
# in tests/test_missing_source_target_metadata.py.
_DELIBERATELY_INCOMPLETE = {"flattened_no_instances.xml"}

POWERCENTER_FIXTURES = sorted(
    p for p in (FIXTURES_DIR / "powercenter").glob("*.xml")
    if p.name not in _DELIBERATELY_INCOMPLETE
)
IDMC_FIXTURES = sorted((FIXTURES_DIR / "idmc").glob("*.json"))
ALL_FIXTURES = POWERCENTER_FIXTURES + IDMC_FIXTURES

assert len(POWERCENTER_FIXTURES) == 14, (
    f"expected 14 PowerCenter fixtures, found {len(POWERCENTER_FIXTURES)}: "
    f"{[f.name for f in POWERCENTER_FIXTURES]}"
)
assert len(IDMC_FIXTURES) == 2, (
    f"expected 2 IDMC fixtures, found {len(IDMC_FIXTURES)}: "
    f"{[f.name for f in IDMC_FIXTURES]}"
)

# Imports that do not exist on the AIDP cluster image.
UNAVAILABLE_IMPORTS = ("matplotlib", "seaborn")

FIXTURE_IDS = [f.name for f in ALL_FIXTURES]


def _generate_notebook_code(fixture_path: Path) -> str:
    """Parse a fixture end-to-end and return the concatenated source of
    every code cell in the generated notebook.

    Mirrors the rule-based fallback path in batch.py/migrator.py -- parse
    via the same format-detecting dispatcher used for both PowerCenter
    XML and IICS/IDMC JSON, run every transformation through the real
    converter, then hand it to NotebookGenerator exactly as the CLI does.
    """
    parsed = detect_and_parse_file(str(fixture_path))
    assert parsed.mappings, f"{fixture_path.name}: no mappings parsed"
    mapping = parsed.mappings[0]
    session = Session(name=f"s_{mapping.name}")

    converter = TransformationConverter()
    conversion_result = {"transformations": {}, "source_reads": [], "target_write": ""}
    for tx in mapping.transformations:
        conversion_result["transformations"][tx.name] = "\n".join(converter.convert(tx))

    notebook_json = NotebookGenerator().generate(
        mapping, session, conversion_result, output_format="ipynb"
    )
    data = json.loads(notebook_json)
    return "\n".join(
        "".join(c["source"]) if isinstance(c["source"], list) else c["source"]
        for c in data["cells"]
        if c["cell_type"] == "code"
    )


@pytest.fixture(scope="module")
def notebook_code_by_fixture() -> dict[str, str]:
    """Generate every fixture's notebook exactly once per test run."""
    return {f.name: _generate_notebook_code(f) for f in ALL_FIXTURES}


@pytest.mark.parametrize("fixture", ALL_FIXTURES, ids=FIXTURE_IDS)
def test_no_unavailable_imports(fixture, notebook_code_by_fixture):
    code = notebook_code_by_fixture[fixture.name]
    found = [m for m in UNAVAILABLE_IMPORTS if f"import {m}" in code]
    assert not found, (
        f"{fixture.name}: AIDP cluster image lacks these, importing them "
        f"fails at import time: {found}"
    )


@pytest.mark.parametrize("fixture", ALL_FIXTURES, ids=FIXTURE_IDS)
def test_no_agg_backend(fixture, notebook_code_by_fixture):
    code = notebook_code_by_fixture[fixture.name]
    assert 'use("Agg")' not in code and "use('Agg')" not in code, (
        f"{fixture.name}: matplotlib.use('Agg') poisons the AIDP kernel "
        f"for the rest of the session"
    )


@pytest.mark.parametrize("fixture", ALL_FIXTURES, ids=FIXTURE_IDS)
def test_tables_addressed_by_catalog_name_not_jdbc(fixture, notebook_code_by_fixture):
    code = notebook_code_by_fixture[fixture.name]
    assert "jdbc:" not in code.lower(), (
        f"{fixture.name}: tables must be addressed via spark.table() / "
        f"saveAsTable(), never a raw JDBC URL"
    )


@pytest.mark.parametrize("fixture", ALL_FIXTURES, ids=FIXTURE_IDS)
def test_no_hardcoded_credentials(fixture, notebook_code_by_fixture):
    code = notebook_code_by_fixture[fixture.name].lower()
    for token in ("password=", "pwd=", "secret="):
        assert token not in code, (
            f"{fixture.name}: generated notebook contains a credential "
            f"literal ({token!r})"
        )


@pytest.mark.parametrize("fixture", ALL_FIXTURES, ids=FIXTURE_IDS)
def test_unpinned_pandas_install_is_not_emitted(fixture, notebook_code_by_fixture):
    code = notebook_code_by_fixture[fixture.name]
    if "pip install" in code and "pandas" in code:
        assert "pandas==2.2.3" in code, (
            f"{fixture.name}: the AIDP installer shadows image packages "
            f"-- an installed pandas must be pinned to the image version"
        )


@pytest.mark.parametrize("fixture", ALL_FIXTURES, ids=FIXTURE_IDS)
def test_no_unresolved_names(fixture, notebook_code_by_fixture):
    """Reuses code_validation.unresolved_names -- do not re-implement a
    second name-resolution checker (see module docstring there and
    demo.sh's verify step, which already runs this over the full corpus).
    """
    code = notebook_code_by_fixture[fixture.name]
    unresolved = unresolved_names(code)
    assert not unresolved, (
        f"{fixture.name}: name(s) {unresolved} are read before ever "
        f"being assigned -- guaranteed NameError at runtime"
    )


@pytest.mark.parametrize("fixture", ALL_FIXTURES, ids=FIXTURE_IDS)
def test_delta_table_forname_records_catalog_constraint(fixture, notebook_code_by_fixture):
    """DeltaTable.forName() is Delta-only (see
    references/aidp-runtime-constraints.md) and does not apply against an
    external (ADW-backed) catalog. The generator cannot yet branch on
    target catalog type -- that's later work -- but every call site must
    at least carry the constraint as a visible comment in the generated
    notebook, not silently assume Delta.
    """
    code = notebook_code_by_fixture[fixture.name]
    if "DeltaTable.forName" in code:
        assert "aidp-runtime-constraints" in code, (
            f"{fixture.name}: emits DeltaTable.forName() (Delta-only) "
            f"with no comment noting the external-catalog constraint"
        )


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
