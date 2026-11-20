import pytest

from scripts.pandas_compat_rewriter import (
    preserve_valid_python_candidate,
    rewrite_removed_dataframe_append,
)


def test_rewrites_dataframe_append_without_changing_default_index_semantics():
    source = """train_pred_prob = X_train.reset_index().merge(other)\nmodeling = train_pred_prob.append(val_pred_prob)\n"""
    rewritten, report = rewrite_removed_dataframe_append(source)
    assert report.replacements == 1
    assert "modeling = pd.concat([train_pred_prob, val_pred_prob])" in rewritten
    assert "ignore_index=True" not in rewritten


def test_preserves_supported_dataframe_append_keywords():
    source = """left = pd.DataFrame(values)\nout = left.append(right, ignore_index=True, sort=True)\n"""
    rewritten, report = rewrite_removed_dataframe_append(source)
    assert report.replacements == 1
    assert "pd.concat([left, right], ignore_index=True, sort=True)" in rewritten


def test_does_not_rewrite_list_append():
    source = "items = []\nitems.append(value)\n"
    rewritten, report = rewrite_removed_dataframe_append(source)
    assert rewritten == source
    assert report.replacements == 0


def test_rejects_plain_english_model_response():
    previous = "result = execute_func()"
    candidate = "Confirmed: DataFrame.append is unavailable in pandas 2.x"
    kept, reason = preserve_valid_python_candidate(previous, candidate)
    assert kept == previous
    assert reason and "invalid Python" in reason


def test_accepts_valid_python_model_response():
    candidate = "result = pd.concat([left, right])"
    kept, reason = preserve_valid_python_candidate("old = True", candidate)
    assert kept == candidate
    assert reason is None


@pytest.mark.parametrize("candidate", [
    'message = "```"',
    "message = '```python'",
    'message = f"path={path}```"',
    r'message = r"```\nraw"',
    'message = b"```"',
    'message = """\n```python\nexample()\n```\n"""',
    '"""Example: ```python```"""\nvalue = 1',
    '# ```python\nvalue = 1\n# ```',
    'value = 1  # ```',
])
def test_backticks_in_python_data_or_comments_do_not_reject_candidate(candidate):
    # Run c8fc04e0: a message f-string ending in ``` discarded S3->OCI edits.
    kept, reason = preserve_valid_python_candidate("old = True", candidate)
    assert kept == candidate
    assert reason is None


@pytest.mark.parametrize("candidate", [
    '```python\nvalue = 1\n```',
    'value = 1\n```',
    '```\nvalue = 1',
    'message = "```"\nvalue = (',
    'message = "```"\nreturn 1',
    'message = "```"\n\x00',
])
def test_actual_fences_and_invalid_python_still_keep_previous_code(candidate):
    previous = "old = True"
    kept, reason = preserve_valid_python_candidate(previous, candidate)
    assert kept == previous
    assert reason


@pytest.mark.parametrize("candidate", ['', '  ', '%sql SELECT 1', '!echo example'])
def test_empty_and_native_candidate_contract_is_unchanged(candidate):
    previous = 'old = True'
    kept, reason = preserve_valid_python_candidate(previous, candidate)
    assert kept == (candidate if candidate.strip() else previous)
    assert (reason is None) == bool(candidate.strip())


@pytest.mark.parametrize("candidate", [
    '"""\n```python\nbad\n```\n"""',
    "# I couldn't migrate this\n# ```python\n# explanation\n# ```",
    '# ```\n"explanation"\n"more explanation"',
])
def test_fenced_inert_response_preserves_executable_code(candidate):
    previous = 'df.write.mode("append").save(destination)'
    kept, reason = preserve_valid_python_candidate(previous, candidate)
    assert kept == previous
    assert reason and "no executable statements" in reason


@pytest.mark.parametrize("candidate", ['"ordinary docstring"', '# ordinary comment'])
def test_non_fence_inert_response_keeps_existing_contract(candidate):
    assert preserve_valid_python_candidate('old = True', candidate) == (candidate, None)
