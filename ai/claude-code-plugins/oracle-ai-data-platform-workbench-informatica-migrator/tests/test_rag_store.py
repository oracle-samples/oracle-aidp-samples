"""The RAG store's two fingerprints, and why they must stay separate.

A stored conversion can be reused verbatim only if nothing that reaches the
generated code differs. Field names reach it -- as ``F.col('AMT')`` -- so a
fingerprint that deliberately omits them cannot decide reuse.

``find_exact`` used to key on that shape fingerprint, which made
``IIF(AMT > 100, 'BIG', 'SMALL')`` and ``IIF(QTY < 5, 'LOW', 'HIGH')`` hash
identically: same type, one input, one output, one function named IIF. The
first one's PySpark was returned for the second as an exact match --
different column, different operator, different literals.
"""
from __future__ import annotations

import types

import pytest

from infa2aidp.agents.rag_store import RAGStore


def _spec(expr: str, inputs=("A",), outputs=("RESULT",), ttype="Expression"):
    s = types.SimpleNamespace()
    s.transformation_type = ttype
    s.inputs = list(inputs)
    s.outputs = list(outputs)
    s.logic = {"expressions": [{"expression": expr}]}
    return s


@pytest.fixture
def store(tmp_path):
    return RAGStore(store_path=str(tmp_path / "rag.json"))


# ── The collision that started this ──────────────────────────────────

def test_two_different_expressions_do_not_share_a_content_hash(store):
    a = _spec("IIF(AMT > 100, 'BIG', 'SMALL')")
    b = _spec("IIF(QTY < 5, 'LOW', 'HIGH')")
    assert store._hash(store._content_fingerprint(a)) != \
           store._hash(store._content_fingerprint(b))


def test_the_shape_fingerprint_separates_different_operators_and_literals(store):
    """The shape fingerprint generalises over port *names*, not over the
    expression. It used to key on function names alone, so these two shared
    a fingerprint; the identifier-blanked skeleton keeps the operator and the
    literals, so ``IIF(_>100,'~','~')`` and ``IIF(_<5,'~','~')`` differ."""
    a = _spec("IIF(AMT > 100, 'BIG', 'SMALL')")
    b = _spec("IIF(QTY < 5, 'LOW', 'HIGH')")
    assert store._fingerprint(a) != store._fingerprint(b)


def test_the_shape_fingerprint_still_generalises_over_port_names(store):
    """What it is for: the same expression on renamed ports is one shape."""
    a = _spec("IIF(AMT > 100, 'BIG', 'SMALL')")
    b = _spec("IIF(TOTAL > 100, 'BIG', 'SMALL')")
    assert store._fingerprint(a) == store._fingerprint(b)


def test_find_exact_does_not_return_a_different_transformations_code(store):
    """The defect, asserted directly."""
    a = _spec("IIF(AMT > 100, 'BIG', 'SMALL')")
    store.store(spec=a, pyspark_code="CODE_FOR_A", confidence=95.0)

    b = _spec("IIF(QTY < 5, 'LOW', 'HIGH')")
    assert store.find_exact(b) is None, (
        "a different expression must not match exactly, however similar its shape"
    )


def test_find_exact_still_returns_a_genuine_repeat(store):
    """The feature has to keep working: an identical transformation is
    exactly what this store is for."""
    a = _spec("IIF(AMT > 100, 'BIG', 'SMALL')")
    store.store(spec=a, pyspark_code="CODE_FOR_A", confidence=95.0)
    again = _spec("IIF(AMT > 100, 'BIG', 'SMALL')")
    hit = store.find_exact(again)
    assert hit is not None and hit.pyspark_code == "CODE_FOR_A"


# ── Everything that reaches the generated code counts ────────────────

@pytest.mark.parametrize("changed", [
    pytest.param(_spec("IIF(AMT > 100, 'BIG', 'SMALL')", inputs=("B",)), id="input-name"),
    pytest.param(_spec("IIF(AMT > 100, 'BIG', 'SMALL')", outputs=("BAND",)), id="output-name"),
    pytest.param(_spec("IIF(AMT > 200, 'BIG', 'SMALL')"), id="numeric-literal"),
    pytest.param(_spec("IIF(AMT > 100, 'HUGE', 'SMALL')"), id="string-literal"),
    pytest.param(_spec("IIF(AMT >= 100, 'BIG', 'SMALL')"), id="operator"),
])
def test_any_difference_that_reaches_the_code_breaks_the_exact_match(store, changed):
    base = _spec("IIF(AMT > 100, 'BIG', 'SMALL')")
    store.store(spec=base, pyspark_code="CODE_FOR_BASE", confidence=95.0)
    assert store.find_exact(changed) is None


def test_a_different_transformation_type_never_matches(store):
    base = _spec("IIF(AMT > 100, 'BIG', 'SMALL')")
    store.store(spec=base, pyspark_code="CODE", confidence=95.0)
    other = _spec("IIF(AMT > 100, 'BIG', 'SMALL')", ttype="Filter")
    assert store.find_exact(other) is None


# ── Storage keys, and entries written before the fix ─────────────────

def test_two_similar_transformations_get_separate_entries(store):
    """Keyed on shape, the second store() overwrote the first."""
    store.store(spec=_spec("IIF(AMT > 100, 'BIG', 'SMALL')"),
                pyspark_code="CODE_A", confidence=90.0)
    store.store(spec=_spec("IIF(QTY < 5, 'LOW', 'HIGH')"),
                pyspark_code="CODE_B", confidence=90.0)
    assert len(store.entries) == 2
    codes = {e.pyspark_code for e in store.entries.values()}
    assert codes == {"CODE_A", "CODE_B"}


def test_a_legacy_entry_without_a_content_hash_is_not_returned_as_exact(store):
    """An entry written before content_hash existed is fuzzy-only.

    Unreachable via find_exact is the safe direction: the alternative is
    treating a shape-keyed entry as an exact match, which is the defect.
    """
    spec = _spec("IIF(AMT > 100, 'BIG', 'SMALL')")
    store.store(spec=spec, pyspark_code="LEGACY", confidence=90.0)
    for entry in store.entries.values():
        entry.content_hash = ""
    assert store.find_exact(spec) is None


def test_fuzzy_matching_does_not_cross_between_different_expressions(store):
    """This assertion used to be inverted -- it required CODE_A to come back
    for a different expression, calling that "a pattern to adapt". Nothing
    adapts it: ``ConversionPipeline._check_rag`` assigns the hit straight to
    ``final_code``, so a fuzzy hit was emitted verbatim. The shape gate in
    ``find_similar`` now refuses a skeleton that shares nothing."""
    store.store(spec=_spec("IIF(AMT > 100, 'BIG', 'SMALL')"),
                pyspark_code="CODE_A", confidence=90.0)
    assert store.find_similar(_spec("IIF(QTY < 5, 'LOW', 'HIGH')")) is None


def test_the_pipeline_reuses_only_an_exact_match(tmp_path):
    """The wiring, not just the fingerprint.

    ``_check_rag``'s return value becomes ``final_code`` with
    ``final_status="success"``, ``confidence=0.95`` and the validation loop
    skipped entirely. So the lookup it performs decides what gets emitted
    unchecked. It must be ``find_exact``: a similar entry carries another
    mapping's column names. Both PRs tightened the fingerprints; neither
    changed this call, and the fingerprints are what made it survivable.
    """
    import ast
    import inspect
    import textwrap
    from infa2aidp.agents import pipeline as pipeline_mod

    src = textwrap.dedent(inspect.getsource(pipeline_mod.ConversionPipeline._check_rag))
    tree = ast.parse(src)
    # The attribute names actually *called*, so the prose in the docstring
    # explaining why find_similar is wrong cannot satisfy or break this.
    called = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert "find_exact" in called, "_check_rag must key on the content hash"
    assert "find_similar" not in called, (
        "a fuzzy hit is emitted verbatim as final_code -- it must not feed "
        "_check_rag"
    )
