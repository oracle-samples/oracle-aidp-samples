"""A Sequence Generator key must not be re-written by a matched update.

``_merge_write`` deliberately prefers a natural key over a surrogate one in
the MERGE condition, because a Sequence Generator hands out a fresh value on
every run. That reasoning is only half the problem: matching on the natural
key and then calling ``whenMatchedUpdateAll()`` writes the fresh surrogate
value over the stored one, so every re-run re-keys the rows that already
exist. Anything holding a foreign key to that dimension then points at the
wrong row -- silently, because the load succeeds.

Informatica does not behave this way: NEXTVAL is consumed by the insert
path, and an update leaves the key column alone.

These tests pin the fix: a sequence-fed column that is not part of the match
condition is excluded from the matched-update set.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from infa2aidp.migrator import run_migration

FIXTURE = Path(__file__).parent / "fixtures" / "powercenter" / "seq_into_router.xml"


@pytest.fixture(scope="module")
def generated(tmp_path_factory) -> str:
    """The notebook generated for a Sequence Generator feeding a Router."""
    src = tmp_path_factory.mktemp("in")
    (src / FIXTURE.name).write_bytes(FIXTURE.read_bytes())
    out = tmp_path_factory.mktemp("out")

    run_migration([str(src / FIXTURE.name)], str(out), use_llm=False)

    books = list(Path(out).rglob("*.ipynb"))
    assert books, "no notebook generated for the sequence-into-router mapping"
    cells = json.loads(books[0].read_text())["cells"]
    return "\n".join(
        "".join(c["source"]) for c in cells if c["cell_type"] == "code"
    )


def test_merge_does_not_match_on_the_sequence_key(generated: str):
    """The surrogate key is non-deterministic, so it cannot be the join key."""
    assert ".merge(" in generated, "expected a Delta MERGE write"
    merge_line = next(l for l in generated.splitlines() if ".merge(" in l)
    assert "SURROGATE_KEY" not in merge_line, (
        f"MERGE matches on the sequence-fed key: {merge_line.strip()}"
    )


def test_sequence_key_is_frozen_on_matched_update(generated: str):
    """The defect: whenMatchedUpdateAll() would re-key existing rows."""
    assert "whenMatchedUpdateAll()" not in generated, (
        "whenMatchedUpdateAll() rewrites the sequence-fed surrogate key on "
        "every run, re-keying rows that already exist"
    )
    assert "_FROZEN_ON_UPDATE = {'SURROGATE_KEY'}" in generated
    assert "whenMatchedUpdate(set=" in generated


def test_sequence_key_is_still_written_on_insert(generated: str):
    """Freezing the key on UPDATE must not stop it being set on INSERT.

    A fix that dropped the column from the insert path too would leave the
    surrogate key NULL on every new row -- worse than the bug.
    """
    assert "whenNotMatchedInsertAll()" in generated, (
        "new rows must still receive their sequence value"
    )


def test_generated_notebook_is_valid_python(generated: str):
    """The frozen-set line is emitted inside a try: block, so its
    indentation has to match the lines around it."""
    for cell in generated.split("\n\n\n"):
        if cell.strip():
            ast.parse(cell)


def test_sequence_column_reaches_the_target_through_the_router(generated: str):
    """The wiring half: a Router between the sequence and the target must
    not drop the key."""
    assert "SURROGATE_KEY" in generated
    select = next(
        (l for l in generated.splitlines() if ".select(" in l and "SURROGATE_KEY" in l),
        None,
    )
    assert select, "target select does not carry the sequence-fed key"
