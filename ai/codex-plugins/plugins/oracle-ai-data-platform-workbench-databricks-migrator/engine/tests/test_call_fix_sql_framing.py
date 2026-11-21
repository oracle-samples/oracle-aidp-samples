"""call_fix tells the model a %sql cell is Spark SQL (not Python), and FIX_PROMPT
carries the verified AIDP partition-registry fact. Scala retains its language;
Python framing is unchanged."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import agent_migrate as am  # noqa: E402

PY_RULE = ("You MUST call the submit_code tool to return your fixed Python code. "
           "Do NOT return code as text.\n"
           "The code field must contain ONLY valid executable Python.")


def _fix_input(monkeypatch, code):
    seen = {}

    async def fake_tools(system, user_content, *args, **kwargs):
        seen["system"], seen["user"] = system, user_content
        return code, {}

    monkeypatch.setattr(am, "call_opus_with_tools", fake_tools)
    asyncio.run(am.call_fix(code, "boom", [], 1, session=object()))
    return seen["user"]


def test_sql_cell_is_framed_as_spark_sql(monkeypatch):
    msg = _fix_input(monkeypatch, "%sql\nMSCK REPAIR TABLE db.t;")
    assert "```sql\n%sql\nMSCK REPAIR TABLE db.t;\n```" in msg
    assert "Spark SQL" in msg and "starting with %sql" in msg
    # The disable-and-comment recipe is limited to statements FIX_PROMPT documents
    # as unsupported AND unnecessary on AIDP; anything else would be masking.
    assert "Never comment out or remove a statement to make the cell pass" in msg
    assert "never add a statement that writes data" in msg
    assert "unsupported AND unnecessary on AIDP" in msg
    assert "comment-only %sql cell fails on AIDP" in msg
    assert "ONLY valid executable Python" not in msg


def test_python_cell_framing_is_unchanged(monkeypatch):
    msg = _fix_input(monkeypatch, "x = 1")
    assert "```python\nx = 1\n```" in msg
    assert msg.rstrip().endswith(PY_RULE)


def test_scala_cell_framing_preserves_scala(monkeypatch):
    msg = _fix_input(monkeypatch, "%scala\nval x = 1")
    assert "```scala\n%scala\nval x = 1\n```" in msg
    assert "starting with %scala" in msg
    assert "ONLY valid executable Python" not in msg


def test_fix_prompt_states_the_aidp_partition_registry_fact():
    prompt = am.FIX_PROMPT
    assert "[verified: 2026-10-13]" in prompt
    assert "MSCK REPAIR TABLE" in prompt and "RECOVER PARTITIONS" in prompt
    assert "NOT_SUPPORTED_COMMAND_FOR_V2_TABLE" in prompt
    assert "REFRESH TABLE <same table>" in prompt
