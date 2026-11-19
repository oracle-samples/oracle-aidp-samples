import re
import string

import pytest

from dbx_export.cluster_converter import convert_dbx_cluster
from dbx_export.job_converter import convert_dbx_job
from dbx_export.sanitizer import sanitize_cluster_name, sanitize_workflow_name, sanitize_segment


def test_workflow_name_replaces_aidp_unsupported_brackets():
    result = sanitize_workflow_name(
        "[Collections Orchestration] State Based Allocation Workflow",
        "ai_tool",
    )

    assert result.startswith(
        "ai_tool_Collections_Orchestration_State_Based_Allocation_Workflow_"
    )
    assert "[" not in result and "]" not in result


def test_workflow_name_replaces_dots_and_ampersands():
    result = sanitize_workflow_name(
        "top5_v5.inference & Risk",
        "ai_tool",
    )
    assert result.startswith("ai_tool_top5_v5_inference_Risk_")
    assert re.fullmatch(r"[A-Za-z][A-Za-z0-9_/]*", result)


@pytest.mark.parametrize("character", sorted(set(string.punctuation + string.whitespace) - set("_/")) + ["é", "中", "😀"])
def test_workflow_converter_enforces_resource_charset(character):
    name = f"source{character}job"
    payload = convert_dbx_job(
        {"job_id": "1", "settings": {"name": name}},
        "/Workspace/ai",
        "ai_tool",
        {},
    )
    assert re.fullmatch(r"[A-Za-z][A-Za-z0-9_/]*", payload["name"])
    assert payload["name"] == sanitize_workflow_name(name, "ai_tool")
    cluster = convert_dbx_cluster({"node_type_id": "m5.xlarge"}, name, prefix="ai_tool")
    assert cluster.aidp_payload["displayName"] == sanitize_cluster_name(name, "ai_tool")
    assert re.fullmatch(r"[A-Za-z][A-Za-z0-9_/]*", cluster.aidp_payload["displayName"])


def test_workflow_name_replaces_incident_asterisk_without_aliasing():
    name = "*test_HELPER_dataGenPipeline_prodLedgerApi"
    result = sanitize_workflow_name(name, "UserM_Test")
    assert result.startswith("UserM_Test_test_HELPER_dataGenPipeline_prodLedgerApi_")
    assert result != sanitize_workflow_name(name.replace("*", ""), "UserM_Test")
    assert sanitize_workflow_name(result, "UserM_Test") == result


@pytest.mark.parametrize("name", ["job_123/subjob", "ai_tool_job_123/subjob", "123job", "_job"])
def test_workflow_name_preserves_allowed_characters_with_letter_led_prefix(name):
    expected = name if name.startswith("ai_tool_") else f"ai_tool_{name.lstrip('_')}"
    assert sanitize_workflow_name(name, "ai_tool") == expected


@pytest.mark.parametrize("prefix", ["", "123", "_tool", "/tool", "ai-tool", "ai.tool", "ai*tool", "ai tool", "étool"])
def test_workflow_converter_rejects_invalid_prefix(prefix):
    with pytest.raises(ValueError, match="Resource prefix must start"):
        convert_dbx_job(
            {"job_id": "1", "settings": {"name": "job"}},
            "/Workspace/ai", prefix, {},
        )


def test_cluster_name_uses_live_verified_resource_charset():
    name = "top5_v5.inference & Risk"
    assert sanitize_cluster_name(name, "ai_tool") == sanitize_workflow_name(name, "ai_tool")
    # Notebook/file path segments retain their distinct contract.
    assert sanitize_segment("source.name* & Risk") == "source.name_Risk"


def test_workflow_name_without_ampersands_is_unchanged():
    assert sanitize_workflow_name(
        "top5_v5_inference Risk", "ai_tool",
    ) == "ai_tool_top5_v5_inference_Risk"


def test_workflow_and_cluster_converters_use_shared_ampersand_mapping():
    payload = convert_dbx_job(
        {"job_id": "1", "settings": {"name": "Daily&Risk"}},
        "/Workspace/export", "ai_tool", {},
    )
    conversion = convert_dbx_cluster(
        {"node_type_id": "m5.xlarge"}, "Daily&Risk", prefix="ai_tool",
    )

    assert payload["name"] == "ai_tool_Daily_Risk"
    assert conversion.aidp_payload["displayName"] == "ai_tool_Daily_Risk"


def test_workflow_name_replaces_apostrophe_with_underscore():
    result = sanitize_workflow_name(
        "Pre_Aug'23 FL Addition",
        "ai_tool_dan_phase1",
    )

    assert result == "ai_tool_dan_phase1_Pre_Aug_23_FL_Addition_ffea616c"
    assert "'" not in result
    assert result != sanitize_workflow_name(
        "Pre_Aug_23 FL Addition",
        "ai_tool_dan_phase1",
    )


def test_cluster_name_replaces_apostrophe_with_underscore():
    result = sanitize_cluster_name(
        "Collections' Shared Cluster",
        "ai_tool",
    )

    assert result.startswith("ai_tool_Collections_Shared_Cluster_")
    assert "'" not in result
    assert result != sanitize_cluster_name(
        "Collections_ Shared Cluster",
        "ai_tool",
    )


def test_job_converter_uses_exact_apostrophe_safe_workflow_name():
    payload = convert_dbx_job(
        {"job_id": "1", "settings": {"name": "Pre_Aug'23 FL Addition"}},
        "/Workspace/ai",
        "ai_tool_dan_phase1",
        {},
    )

    assert payload["name"] == "ai_tool_dan_phase1_Pre_Aug_23_FL_Addition_ffea616c"


def test_cluster_converter_uses_exact_apostrophe_safe_display_name():
    conversion = convert_dbx_cluster(
        {"node_type_id": "m5.xlarge"},
        "Collections' Shared Cluster",
        prefix="ai_tool",
    )

    assert conversion.aidp_payload["displayName"] == (
        "ai_tool_Collections_Shared_Cluster_4d0a2767"
    )


def test_cluster_name_replaces_aidp_unsupported_brackets():
    result = sanitize_cluster_name(
        "[Collections] Allocation Cluster",
        "ai_tool",
    )

    assert result.startswith("ai_tool_Collections_Allocation_Cluster_")
    assert "[" not in result and "]" not in result


def test_reserved_workspace_sentinel_is_preserved_for_bracket_free_names():
    assert sanitize_workflow_name("Workspace", "ai_tool") == "ai_tool_Workspace_"
    assert sanitize_cluster_name("Workspace", "ai_tool") == "ai_tool_Workspace_"


def test_bracketed_names_do_not_alias_bracket_free_resources():
    workflow_name = sanitize_workflow_name("[prod] sync", "ai_tool")
    cluster_name = sanitize_cluster_name("[prod] sync", "ai_tool")

    assert workflow_name != sanitize_workflow_name("prod sync", "ai_tool")
    assert cluster_name != sanitize_cluster_name("prod sync", "ai_tool")
    assert sanitize_workflow_name("[Workspace]", "ai_tool") != (
        sanitize_workflow_name("Workspace", "ai_tool")
    )


def test_bracket_disambiguation_is_stable():
    workflow_name = sanitize_workflow_name("[prod] sync", "ai_tool")
    cluster_name = sanitize_cluster_name("[prod] sync", "ai_tool")

    assert workflow_name == sanitize_workflow_name("[prod] sync", "ai_tool")
    assert cluster_name == sanitize_cluster_name("[prod] sync", "ai_tool")


def test_degenerate_resource_names_use_stable_distinct_fallbacks():
    bracket_workflow = sanitize_workflow_name("[]", "ai_tool")
    dash_workflow = sanitize_workflow_name("---", "ai_tool")

    assert bracket_workflow.startswith("ai_tool_workflow_")
    assert bracket_workflow == sanitize_workflow_name("[]", "ai_tool")
    assert bracket_workflow != dash_workflow
    assert sanitize_cluster_name("[]", "ai_tool").startswith("ai_tool_cluster_")
