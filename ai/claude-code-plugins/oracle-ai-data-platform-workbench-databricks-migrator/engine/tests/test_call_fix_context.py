"""call_fix context: earlier cells are a pointer by default, full source only on request.

The parent fix loop passes every executed cell; inlining them grew with the cell
index and was the largest uncached block of each fix round, so the default is a
count plus the tools that fetch a cell. The child-repair path (#201) passes this
task's successful sources with inline_context=True and needs them verbatim.
"""
import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import agent_migrate as am


UPSTREAM = "upstream_value = 41\n" + "# padding\n" * 50 + "UPSTREAM_TAIL = True"


def _fix_prompt(monkeypatch, *, code="print(0)", error="NameError: x", **kwargs):
    prompts = []

    async def model(system, prompt, **kw):
        prompts.append(prompt)
        return "print(1)", {}

    monkeypatch.setattr(am, "call_opus_with_tools", model)
    asyncio.run(am.call_fix(code, error, [UPSTREAM, "b = 2"], 2,
                            session=object(), **kwargs))
    assert len(prompts) == 1
    return prompts[0]


def test_default_points_at_tools_instead_of_inlining(monkeypatch):
    prompt = _fix_prompt(monkeypatch)
    assert UPSTREAM not in prompt
    assert "UPSTREAM_TAIL" not in prompt
    assert "2 earlier cell(s) in this notebook executed OK" in prompt
    assert "get_history_entry(index)" in prompt and "read_notebook_source" in prompt


def test_inline_context_keeps_full_source_in_order(monkeypatch):
    prompt = _fix_prompt(monkeypatch, inline_context=True)
    assert f"### Cell 0 (executed OK):\n```python\n{UPSTREAM}\n```" in prompt
    assert prompt.index(UPSTREAM) < prompt.index("b = 2")
    assert "earlier cell(s) in this notebook executed OK" not in prompt


@pytest.mark.parametrize("inline_context", [False, True])
@pytest.mark.parametrize("code,error", [
    ("df = df.withColumn(name, F.to_json(F.col(name)))",
     "[DATATYPE_MISMATCH.INVALID_JSON_SCHEMA] input has type STRING"),
    ('df = spark.table("required_input")',
     "[TABLE_OR_VIEW_NOT_FOUND] required_input cannot be found"),
])
def test_repair_diagnoses_operation_before_declaring_data_unavailable(
        monkeypatch, inline_context, code, error):
    # Run cda6a48a repaired cell 9 after diagnosing its input types; cba8c418
    # investigated corrupt records, returned the same caller and declared a gap.
    # Both parent and child repair inputs must keep the evidence and ask the
    # model to distinguish that failure from a genuinely unavailable resource.
    prompt = _fix_prompt(monkeypatch, code=code, error=error,
                         inline_context=inline_context)
    assert code in prompt and error in prompt
    assert "Diagnose the failing operation from its exception and runtime inputs first." in prompt
    assert "For type errors, inspect relevant schema/types before scanning data." in prompt
    assert "Preserve the intended calculation; do not hide\nerrors or discard data to make execution pass." in prompt
    assert "(explore_path, suggest_oci_path, search_catalog, run_on_cluster) to verify\nrelevant data/resource availability BEFORE changing data-access code; do not guess paths." in prompt
    assert "Use DATA_UNAVAIL only\nwhen evidence shows a required resource prevents validation." in prompt
    assert "verify data availability BEFORE rewriting" not in prompt
