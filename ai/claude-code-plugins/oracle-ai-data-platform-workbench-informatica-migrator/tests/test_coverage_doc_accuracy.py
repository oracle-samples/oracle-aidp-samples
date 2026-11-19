"""The coverage doc claims to be read off the code. Keep it that way.

``references/conversion-coverage.md`` is the document a user reads to decide
whether this tool covers their estate. Its counts are derived from the
dispatch table and the function registry, so a type added to the converter
without a doc update makes the document quietly understate the tool -- and a
type removed makes it overstate it, which is worse.

These tests fail on either drift. They assert the documented numbers against
the code, not against each other.
"""

import inspect
import re
from pathlib import Path

from infa2aidp.converters.expression_converter import ExpressionConverter
from infa2aidp.converters.transformation_converter import TransformationConverter

DOC = Path(__file__).resolve().parents[1] / "references" / "conversion-coverage.md"

# Parser-level sentinels, not Informatica transformation types: they name the
# source and target ends of a mapping and the unrecognised case. They have no
# handler by design, so they are not "unsupported coverage".
_NON_TRANSFORMATION = {"SOURCE", "TARGET", "UNKNOWN"}

# Tokens in the expression registry that are format masks, literals or
# internal node kinds rather than Informatica functions.
_NOT_FUNCTIONS = {
    "BUILTIN", "PARAM", "SYSVAR", "IDENT", "NUMBER", "STRING",
    "BAD", "NULL", "TRUE", "FALSE",
}


def _converted_types() -> set[str]:
    src = inspect.getsource(TransformationConverter.convert)
    return set(re.findall(r"TransformationType\.(\w+):\s*self\.", src))


def _documented_count(label: str) -> int:
    text = DOC.read_text()
    m = re.search(rf"\*\*(\d+)\s+(?:types?\s+)?{label}", text)
    assert m, f"coverage doc has no '{label}' count to check"
    return int(m.group(1))


def test_documented_converted_count_matches_dispatch_table():
    assert _documented_count("types converted") == len(_converted_types())


def test_documented_reported_count_matches_no_equivalent_table():
    # the doc writes this one as "<n> reported with a reason" inside the same
    # bolded line as the converted count, so read it directly
    text = DOC.read_text()
    m = re.search(r"(\d+)\s+reported with a reason", text)
    assert m, "coverage doc has no reported count"
    assert int(m.group(1)) == len(TransformationConverter._NO_EQUIVALENT)


def test_documented_function_count_matches_registry():
    src = inspect.getsource(ExpressionConverter)
    names = set(ExpressionConverter._SIMPLE_FUNCS)
    for m in re.finditer(r'name\s*(?:==|in)\s*\(?\s*((?:"[A-Z_0-9]+"\s*,?\s*)+)\)?', src):
        names |= set(re.findall(r'"([A-Z_0-9]+)"', m.group(1)))
    names -= _NOT_FUNCTIONS

    text = DOC.read_text()
    m = re.search(r"\*\*(\d+)\s+Informatica functions\*\*", text)
    assert m, "coverage doc has no function count"
    assert int(m.group(1)) == len(names)


def test_every_transformation_type_is_converted_or_reported():
    """No type may fall through to ``_unsupported`` unnoticed.

    ``_unsupported`` is the honest fallback for a type the parser invents or
    a future Informatica release adds -- but every type the models already
    name should have a decided outcome. A type sitting in neither table is
    one the doc does not describe.
    """
    from infa2aidp.models import TransformationType

    decided = _converted_types() | {t.name for t in TransformationConverter._NO_EQUIVALENT}
    undecided = {
        t.name for t in TransformationType
        if t.name not in decided and t.name not in _NON_TRANSFORMATION
    }
    assert not undecided, (
        f"transformation types with no decided outcome: {sorted(undecided)}. "
        f"Give each a handler or a _NO_EQUIVALENT reason, and update "
        f"references/conversion-coverage.md."
    )


def test_converted_and_reported_are_disjoint():
    overlap = _converted_types() & {
        t.name for t in TransformationConverter._NO_EQUIVALENT
    }
    assert not overlap, f"type both converted and reported: {sorted(overlap)}"
