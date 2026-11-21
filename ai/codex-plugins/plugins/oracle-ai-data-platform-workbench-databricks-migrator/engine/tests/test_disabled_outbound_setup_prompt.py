"""Prompt contract for disabling outbound operations and their exclusive setup.

Evidence: cell 32 in runs aaaaaaaa-0000-4000-8000-000000000203 (19 October)
and aaaaaaaa-0000-4000-8000-000000000204 (20 October 2026). Both failed on
an unset widget; the second repair disabled the upload but retained its setup,
then classified the cell DATA_UNAVAILABLE. These tests pin instructions, not
model compliance; live model replay is a separate validation requirement.
"""
import ast
from pathlib import Path


def _outbound_setup_guidance():
    source = Path(__file__).resolve().parents[1] / "scripts" / "agent_migrate.py"
    module = ast.parse(source.read_text(encoding="utf-8"))
    prompt = next(
        ast.literal_eval(node.value)
        for node in module.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "FIX_PROMPT"
                for target in node.targets)
    )
    start = prompt.index("- When disabling an outbound operation that is side-effect only:")
    end = prompt.index("- ConnectTimeout / ConnectionError", start)
    return " ".join(prompt[start:end].split())


def test_exclusive_setup_is_disabled_even_when_it_fails_before_network_call():
    guidance = _outbound_setup_guidance()
    assert "widget reads, URL construction, and headers" in guidance
    assert "execution failed in that setup before reaching the network call" in guidance
    assert "Comment out setup used exclusively by the disabled operation" in guidance
    assert "preserve the original code with Oracle tool modification tags" in guidance
    assert "report what was skipped" in guidance


def test_shared_or_uncertain_setup_requires_dependency_inspection():
    guidance = _outbound_setup_guidance()
    assert "Keep any setup needed by remaining computation or downstream cells" in guidance
    assert "read_notebook_source or get_history_entry if usage is uncertain" in guidance
    assert "do NOT assume the whole cell is disposable" in guidance
    assert "or disable data-producing computation" in guidance


def test_missing_parameter_classification_distinguishes_retained_and_disabled_work():
    guidance = _outbound_setup_guidance()
    assert "DATA_UNAVAIL only when the retained computation requires it" in guidance
    assert "Do NOT fabricate a parameter value" in guidance
    assert "or emit DATA_UNAVAIL solely for unused setup" in guidance
