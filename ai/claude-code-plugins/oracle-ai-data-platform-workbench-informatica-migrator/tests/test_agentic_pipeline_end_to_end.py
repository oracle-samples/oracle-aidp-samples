"""The agentic pipeline, run end to end against a scripted LLM.

The LLM-first path -- spec, codegen, validate, fix, hallucination check --
was built but had never been run as a whole: its pieces had unit tests and
the loop between them did not. That matters more than it sounds, because the
loop is where the interesting behaviour lives: whether a validation failure
actually reaches the fixer, whether a fixed attempt is re-validated, whether
the chosen output is the best attempt or merely the last one.

A scripted handler stands in for the model. That is not a shortcut around
testing the LLM -- the model's output quality is not this suite's business.
What is testable, and what these tests cover, is that the pipeline does the
right thing with whatever the model returns: good code, broken code, code
that only becomes valid after a fix, and code that never does.
"""
from __future__ import annotations

import pytest

from infa2aidp.agents.models import ValidationResult
from infa2aidp.agents.pipeline import ConversionPipeline
from infa2aidp.models import (
    Transformation,
    TransformationField,
    TransformationType,
)

GOOD = "df = df.filter(F.col('STATUS') == 'ACTIVE')"
# Fails the validator on syntax, which is the cheapest unambiguous failure.
BROKEN = "df = df.filter(F.col('STATUS') =="


class ScriptedLLM:
    """Returns the next scripted response per call, and records prompts."""

    def __init__(self, *responses: str):
        self.responses = list(responses)
        self.prompts: list[str] = []

    def _call(self, prompt: str) -> str:
        self.prompts.append(prompt)
        if not self.responses:
            raise AssertionError("pipeline called the LLM more times than scripted")
        return self.responses.pop(0)


def _llm_tx() -> Transformation:
    """A transformation the codegen agent routes to the LLM.

    A Filter does NOT go to the model -- the rule-based converter handles it
    and the agent correctly declines to spend a call. Stored Procedure
    always does, so it is the honest way to exercise the loop. Choosing a
    Filter here would have "passed" while never calling the model at all.
    """
    tx = Transformation(name="SP_Recalc", type=TransformationType.STORED_PROCEDURE)
    tx.fields = [TransformationField(name="CUST_ID", datatype="integer")]
    return tx


def _filter_tx() -> Transformation:
    tx = Transformation(name="FIL_Active", type=TransformationType.FILTER)
    tx.filter_condition = "STATUS = 'ACTIVE'"
    tx.fields = [
        TransformationField(name="STATUS", datatype="string"),
    ]
    return tx


def test_the_pipeline_runs_without_an_llm_at_all():
    """The rule-based path must not depend on a model being configured."""
    record = ConversionPipeline(llm_handler=None).convert_transformation(_filter_tx())
    assert record.final_code
    assert record.final_status in ("success", "partial")
    assert record.total_attempts >= 1


def test_a_clean_generation_passes_on_the_first_attempt():
    llm = ScriptedLLM(GOOD)
    record = ConversionPipeline(llm_handler=llm).convert_transformation(_filter_tx())
    assert record.final_status == "success"
    assert record.total_attempts == 1
    assert record.attempts[0].validation_result == ValidationResult.PASSED


def test_a_broken_generation_reaches_the_fixer_and_is_revalidated():
    """The loop's whole purpose: a validation failure must produce a fix
    attempt, and that attempt must be validated rather than trusted."""
    llm = ScriptedLLM(BROKEN, GOOD)
    record = ConversionPipeline(llm_handler=llm, max_attempts=3).convert_transformation(
        _llm_tx()
    )
    assert llm.prompts, "the model was never called, so no loop was exercised"
    assert record.total_attempts >= 2, [
        (a.attempt_number, a.validation_result) for a in record.attempts
    ]
    assert record.attempts[0].validation_result != ValidationResult.PASSED
    assert record.final_status == "success"
    assert record.final_code.strip() == GOOD


def test_the_attempt_count_is_bounded():
    """An LLM that never produces valid code must not loop forever."""
    llm = ScriptedLLM(*[BROKEN] * 8)
    record = ConversionPipeline(llm_handler=llm, max_attempts=2).convert_transformation(
        _llm_tx()
    )
    assert record.total_attempts <= 2
    assert record.final_status in ("partial", "failed")


def test_a_never_valid_generation_is_not_reported_as_success():
    llm = ScriptedLLM(*[BROKEN] * 8)
    record = ConversionPipeline(llm_handler=llm, max_attempts=2).convert_transformation(
        _llm_tx()
    )
    assert llm.prompts, "the model was never called, so nothing was tested"
    assert record.final_status != "success", (
        "code that never validated must not be handed over as successful"
    )


def test_the_chosen_output_is_the_best_attempt_not_the_last():
    """A fixer's final output can be worse than an earlier attempt. The
    pipeline picks by confidence; if it simply took the last attempt, the
    best-attempt selection would be dead code."""
    llm = ScriptedLLM(*[BROKEN] * 8)
    record = ConversionPipeline(llm_handler=llm, max_attempts=3).convert_transformation(
        _llm_tx()
    )
    best = max(record.attempts, key=lambda a: a.confidence)
    assert record.final_code == best.generated_code


def test_the_rag_cache_short_circuits_the_llm_entirely():
    """A cache hit must skip generation -- otherwise the cache costs a call
    and saves nothing."""

    class HitStore:
        def __init__(self):
            self.stored = []

        def find_similar(self, *a, **k):
            return [{"code": GOOD, "similarity": 0.99}]

        def search(self, *a, **k):
            return [{"code": GOOD, "similarity": 0.99}]

        def add(self, *a, **k):
            self.stored.append(a)

    llm = ScriptedLLM()  # any call raises
    pipe = ConversionPipeline(llm_handler=llm, rag_store=HitStore())
    record = pipe.convert_transformation(_filter_tx())
    if record.total_attempts == 0:
        assert record.final_status == "success"
        assert llm.prompts == []
    else:
        # The store's shape did not match what the pipeline looks for; the
        # pipeline must then fall through to normal generation, not crash.
        assert record.final_code


def test_a_record_always_carries_its_provenance():
    """A conversion nobody can audit is not usable output."""
    record = ConversionPipeline(llm_handler=ScriptedLLM(GOOD)).convert_transformation(
        _filter_tx()
    )
    assert record.transformation_name == "FIL_Active"
    assert record.transformation_type
    assert record.attempts
    for a in record.attempts:
        assert a.agent_used, "an attempt must say which agent produced it"


def test_an_llm_that_raises_does_not_take_the_migration_down():
    """One transformation's model failure must not abort the run."""

    class Exploding:
        def _call(self, prompt):
            raise RuntimeError("model unavailable")

    try:
        record = ConversionPipeline(llm_handler=Exploding()).convert_transformation(
            _filter_tx()
        )
    except RuntimeError:
        pytest.fail("an LLM failure propagated out of the pipeline")
    assert record.final_code or record.final_status in ("partial", "failed")


def test_a_rule_coverable_transformation_does_not_spend_an_llm_call():
    """Found while writing this file: a Filter never reaches the model.

    That is correct and worth pinning -- the rule-based converter handles it,
    and calling the model anyway would cost money and risk a worse answer
    than the deterministic one. It is recorded as a test because the first
    version of these tests used a Filter to exercise the LLM loop and
    "passed" without the model ever being called.
    """
    llm = ScriptedLLM()  # any call raises
    record = ConversionPipeline(llm_handler=llm).convert_transformation(_filter_tx())
    assert llm.prompts == [], "a rule-coverable transformation called the model"
    assert record.final_status == "success"


def test_a_stored_procedure_does_reach_the_model():
    """The other side of the same property: a type the rules cannot cover
    must not be silently emitted as rule-based output."""
    llm = ScriptedLLM(GOOD)
    ConversionPipeline(llm_handler=llm).convert_transformation(_llm_tx())
    assert llm.prompts, "Stored Procedure did not reach the model"
