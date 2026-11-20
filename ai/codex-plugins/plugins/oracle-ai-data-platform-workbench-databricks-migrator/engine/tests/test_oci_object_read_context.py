"""Prompt delivery regression for child run c8fa8eb8 (2026-10-15).

Cell 16 dropped doesObjectExist during AWS SDK replacement; the missing object
surfaced lazily in cell 20. These tests pin model context, not model behavior or
live SDK availability. Run ccc4e16d (2026-10-22) also stopped after inconclusive
JAR directory scans; SDK availability must be checked through Scala execution.
No deterministic customer-code rewrite is introduced.
"""
import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import agent_migrate as am
import job_migrate as jm


def test_cell_migration_preserves_object_read_contract():
    prompt = jm.CELL_MIGRATE_PROMPT
    assert prompt.count("Preserve existence-filter-then-fetch") == 1
    for required in (
        "existence-filter-then-fetch",
        "doesObjectExist/getObjectAsString",
        "head_object/get_object",
        "headObject/getObject",
        "ambiguous not-found/authorization failures",
        "originally strict reads",
        "Keep this check before fetching",
    ):
        assert required in prompt


def test_job_analysis_does_not_receive_cell_execution_guidance():
    assert "Preserve existence-filter-then-fetch" not in am.MIGRATION_PROMPT
    assert "SCALA OCI BRIDGE" not in am.MIGRATION_PROMPT
    assert "SCALA OCI SDK" not in am.MIGRATION_PROMPT


@pytest.mark.parametrize("scala", [False, True])
@pytest.mark.parametrize("inline_context", [False, True])
def test_call_fix_keeps_existing_oci_guidance_for_parent_and_child_repairs(
        monkeypatch, scala, inline_context):
    captured = []
    code = "%scala\nval parsed = 1" if scala else "parsed = 1"

    async def model(system, prompt, **kwargs):
        captured.append((system, prompt))
        return code, {}

    monkeypatch.setattr(am, "call_opus_with_tools", model)
    monkeypatch.setattr(am, "_cell_history", [])
    error = "[PATH_NOT_FOUND] Path does not exist: oci://fixture@mapped/device/SMS/UNION/key"
    result = asyncio.run(am.call_fix(
        code, error, ["// guarded upstream producer" if scala else "# guarded upstream producer"],
        1, session=object(), notebook_path="/Workspace/fixture.ipynb",
        inline_context=inline_context,
    ))
    assert result == code
    assert len(captured) == 1
    system, prompt = captured[0]
    assert system == am.FIX_PROMPT
    assert "OCI PATH / FILESYSTEM OPERATIONS" in system
    assert "SCALA OCI BRIDGE" in system
    assert "SCALA OCI SDK" in system
    assert "Preserve existence-filter-then-fetch" in system
    assert error in prompt
    assert "/Workspace/fixture.ipynb" in prompt
    if scala:
        assert "Preserve Scala; do NOT port to Python" in prompt


def test_existing_oci_guidance_survives_fix_budget_rebake(monkeypatch):
    monkeypatch.setattr(am, "FIX_PROMPT", am.FIX_PROMPT)
    monkeypatch.setattr(am, "MAX_FIX_ATTEMPTS", 3)
    am._rebake_fix_prompt()
    assert "OCI PATH / FILESYSTEM OPERATIONS" in am.FIX_PROMPT
    assert "Preserve existence-filter-then-fetch" in am.FIX_PROMPT
    assert "SCALA OCI BRIDGE" in am.FIX_PROMPT
    assert "SCALA OCI SDK" in am.FIX_PROMPT


def test_context_is_concise_and_inside_existing_oci_sections():
    start = jm.CELL_MIGRATE_PROMPT.index("  Preserve existence-filter-then-fetch")
    end = jm.CELL_MIGRATE_PROMPT.index("  SCALA OCI BRIDGE")
    assert len(jm.CELL_MIGRATE_PROMPT[start:end].split()) < 85
    assert (jm.CELL_MIGRATE_PROMPT.index("DATA SOURCE REPLACEMENTS")
            < start < end)


@pytest.mark.parametrize("prompt", [jm.CELL_MIGRATE_PROMPT, am.FIX_PROMPT])
def test_executor_credential_exception_retains_lifetime_and_failure_contract(prompt):
    """Pin instructions, not a claim that unsetting conf erases executor copies.

    aidputils rejects serialization on the live compute. Scala lazy actions need
    the fetched fields until consumption, including failed executor actions.
    """
    assert prompt.count("SCALA OCI BRIDGE") == 1
    for required in (
        "ONLY for executor-side Object Storage SDK calls",
        "Credential Store API-key auth",
        "temporary spark.conf keys",
        "read them into Scala driver variables",
        "construct SDK\n  clients within executor partitions",
        "non-secret probes before transferring credentials",
        "Scope secret variables to prevent\n  REPL echo",
        "Remove temporary conf keys in finally after the actual Spark actions finish",
        "including failures; defining a lazy DataFrame is not completion",
        "does not erase task/history copies",
        "Otherwise bridge non-secret results only",
        "never collect object payloads to the driver",
        "if unsupported, make_note the blocker and fail visibly",
    ):
        assert required in prompt
    assert "Never bridge credentials" not in prompt
    assert "the OCI SDK is Python" not in prompt
    assert "The OCI SDK works everywhere" not in prompt
    assert "ALWAYS use the OCI Python SDK" not in prompt


@pytest.mark.parametrize("prompt", [jm.CELL_MIGRATE_PROMPT, am.FIX_PROMPT])
def test_scala_sdk_context_uses_execution_instead_of_jar_discovery(prompt):
    """Pin the delivered guidance; filesystem scans cannot establish class access."""
    assert prompt.count("SCALA OCI SDK:") == 1
    start = prompt.index("SCALA OCI SDK:")
    end = prompt.index("Preserve existence-filter-then-fetch", start)
    context = prompt[start:end]
    assert len(context.split()) < 60
    for required in (
        "AIDP supplies com.oracle.bmc.objectstorage.ObjectStorageClient",
        "internally on the Scala classpath",
        "/aidp/libraries/java/jars lists external",
        "libraries, not all platform classes",
        "Verify driver/executor class access",
        "run_on_cluster Scala probes",
        "do not declare the SDK missing from JAR listings",
    ):
        assert required in context
    assert end < prompt.index("SCALA OCI BRIDGE:", end)
