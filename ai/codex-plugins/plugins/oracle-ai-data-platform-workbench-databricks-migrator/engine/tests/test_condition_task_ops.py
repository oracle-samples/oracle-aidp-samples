"""Pinning tests for dbx_export/job_converter.py condition_task op mapping
(PR #68 review round 2).

An unknown/missing DBX condition op used to pass through RAW into the AIDP
boolean expression (e.g. '... STARTS_WITH "True"'), reproducing the
JOB_VALIDATE_0018 failure the mapping was added to fix — silently, at
workflow-validation time. It must fail loud at conversion time instead,
naming the op and the task. The 6 known DBX ops keep mapping.
"""
import pytest

from dbx_export.job_converter import _DBX_CONDITION_OP, convert_dbx_task


def _cond_task(op):
    return {
        "task_key": "gate",
        "condition_task": {
            "op": op,
            "left": "{{job.parameters.run_ok}}",
            "right": "True",
        },
    }


def test_exactly_six_known_ops():
    assert set(_DBX_CONDITION_OP) == {
        "EQUAL_TO", "NOT_EQUAL", "GREATER_THAN", "GREATER_THAN_OR_EQUAL",
        "LESS_THAN", "LESS_THAN_OR_EQUAL",
    }


@pytest.mark.parametrize("dbx_op,operator", sorted(_DBX_CONDITION_OP.items()))
def test_known_ops_map_to_aidp_operators(dbx_op, operator):
    out = convert_dbx_task(_cond_task(dbx_op), "/Workspace/exp", {})
    assert out["type"] == "IF_ELSE_TASK"
    expr = out["expressions"][0]["value"]
    assert expr == '{{job.parameters.run_ok}} ' + operator + ' "True"'


def test_unknown_op_fails_loud_naming_op_and_task():
    with pytest.raises(ValueError) as caught:
        convert_dbx_task(_cond_task("STARTS_WITH"), "/Workspace/exp", {})
    msg = str(caught.value)
    assert "STARTS_WITH" in msg
    assert "gate" in msg


def test_missing_op_fails_loud():
    with pytest.raises(ValueError) as caught:
        convert_dbx_task(_cond_task(None), "/Workspace/exp", {})
    assert "None" in str(caught.value)
