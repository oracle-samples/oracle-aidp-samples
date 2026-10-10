"""SEC-AIDP-SAMPLES-DBX-NEW-01: get_tool_output reads only its own saved files.

- ``ContextCompactor.get_saved_output`` accepts only the bare
  ``tool_<NNN>_<tool>.txt`` names that ``save_and_truncate`` produces.
- Relative traversal, absolute paths and mixed separators are refused before
  any filesystem access; the refusal never echoes a host path.
- "File not found" lists saved names only, never the host directory.
- A planted symlink inside the output directory cannot lead outside (POSIX).
- ``agent_migrate._handle_get_tool_output`` (and the ``get_tool_output``
  dispatch) refuses before any compactor is consulted and stops walking the
  compactor history on a refusal instead of treating it as "not found".
"""
import asyncio
import os
import sys

import pytest

ENGINE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(ENGINE_DIR, "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

import context_compactor  # noqa: E402
from context_compactor import ContextCompactor  # noqa: E402

MARKER = "-----BEGIN PRIVATE KEY----- do-not-leak-this-marker"
LONG_RESULT = "x" * (ContextCompactor.TRUNCATE_AT + 100)
REFUSED = getattr(context_compactor, "REFUSED_PREFIX", "[context_compactor] Refused")
NOT_FOUND = getattr(context_compactor, "NOT_FOUND_PREFIX", "[context_compactor] File not found")


@pytest.fixture
def compactor(monkeypatch, tmp_path):
    """Compactor rooted at ``tmp_path/tmp/aidp_context/call_001`` with a secret
    file three directories above its base dir (``tmp_path/oci_api_key.pem``)."""
    monkeypatch.setenv("AIDP_TMP_DIR", str(tmp_path / "tmp"))
    secret = tmp_path / "oci_api_key.pem"
    secret.write_text(MARKER, encoding="utf-8")
    logs = []
    c = ContextCompactor(call_id="call_001", log_fn=logs.append)
    c.test_logs = logs
    c.test_secret = str(secret)
    c.test_root = str(tmp_path)
    return c


def _assert_refused(result, compactor):
    assert result.startswith(REFUSED), result
    assert MARKER not in result
    assert compactor.test_root not in result, "refusal must not echo a host path"
    assert any("REFUSED" in m for m in compactor.test_logs), "refusal must be logged"


# ---------------------------------------------------------------------------
# ContextCompactor.get_saved_output
# ---------------------------------------------------------------------------

def test_happy_path_roundtrip(compactor):
    notice = compactor.save_and_truncate("run_on_cluster", LONG_RESULT)
    assert "tool_000_run_on_cluster.txt" in notice
    assert compactor.get_saved_output("tool_000_run_on_cluster.txt") == LONG_RESULT
    assert not compactor.test_logs, "a legitimate read is not a refusal"


def test_relative_traversal_refused(compactor):
    rel = "../../../oci_api_key.pem"
    # The traversal really targets the secret from the compactor's base dir.
    assert os.path.exists(os.path.join(compactor._base_dir, rel))
    _assert_refused(compactor.get_saved_output(rel), compactor)


def test_absolute_path_refused(compactor):
    _assert_refused(compactor.get_saved_output(compactor.test_secret), compactor)


@pytest.mark.parametrize("name", [
    "..\\..\\..\\oci_api_key.pem",                         # backslash traversal
    "../..\\../oci_api_key.pem",                           # mixed separators
    "tool_000_x.txt/../../../../oci_api_key.pem",          # traversal behind a valid prefix
    "sub/tool_000_x.txt",                                  # sub-directory
    "/tool_000_x.txt",                                     # rooted
    "C:\\tool_000_x.txt",                                  # drive-rooted
    "tool_000_x.txt\\",                                    # trailing separator
    ".", "..", "",                                         # degenerate names
])
def test_path_like_names_refused(compactor, name):
    _assert_refused(compactor.get_saved_output(name), compactor)


@pytest.mark.parametrize("name", [
    "notes.txt",                 # a real file in the dir, but not a saved output
    "tool_000_run.txt.bak",      # wrong extension
    "tool_00_x.txt",             # too-short counter
    "tool_000_x.log",            # not .txt
    "tool_000_x y.txt",          # tool names are identifiers
    "tool_000_.txt",             # empty tool name
    "tool_000_x.txt\n",          # trailing newline ('$' would accept it)
    "tool_000_x.txt\r\n",        # CRLF-terminated
    "\ntool_000_x.txt",          # leading newline
    "tool_٠٠١_x.txt",  # Arabic-Indic digits ('\\d' would accept them)
    "tool_０００_x.txt",  # fullwidth digits
    "tool_000_é.txt",       # non-ASCII letter in the tool name
])
def test_names_outside_the_saved_output_pattern_refused(compactor, name):
    with open(os.path.join(compactor._base_dir, "notes.txt"), "w", encoding="utf-8") as fh:
        fh.write(MARKER)
    _assert_refused(compactor.get_saved_output(name), compactor)


def test_not_found_lists_saved_names_only(compactor):
    compactor.save_and_truncate("explore_path", LONG_RESULT)
    with open(os.path.join(compactor._base_dir, "notes.txt"), "w", encoding="utf-8") as fh:
        fh.write("stray")
    result = compactor.get_saved_output("tool_999_missing.txt")
    assert result.startswith(NOT_FOUND)
    assert "tool_000_explore_path.txt" in result
    assert "notes.txt" not in result
    assert compactor.test_root not in result, "not-found must not echo the host directory"
    assert "aidp_context" not in result


@pytest.mark.skipif(os.name == "nt", reason="symlink creation needs a privilege on Windows")
def test_symlink_inside_output_dir_cannot_escape(compactor):
    link = os.path.join(compactor._base_dir, "tool_000_link.txt")
    os.symlink(compactor.test_secret, link)
    _assert_refused(compactor.get_saved_output("tool_000_link.txt"), compactor)


def test_counter_beyond_three_digits_still_retrievable(compactor):
    compactor._tool_call_count = 1000
    notice = compactor.save_and_truncate("describe_table", LONG_RESULT)
    assert "tool_1000_describe_table.txt" in notice
    assert compactor.get_saved_output("tool_1000_describe_table.txt") == LONG_RESULT


def test_validate_output_filename():
    validate = context_compactor.validate_output_filename
    assert validate("tool_003_run_on_cluster.txt") is None
    assert validate("tool_1234_x.txt") is None
    for bad in ("../tool_003_x.txt", "/etc/passwd", "tool_003_x.txt/..", None, 3, b"tool_003_x.txt",
                "tool_003_x.txt\n", "tool_٠٠٣_x.txt"):
        assert isinstance(validate(bad), str), bad


def test_newline_terminated_name_refused_before_filesystem_access(compactor, monkeypatch):
    compactor.save_and_truncate("run_on_cluster", LONG_RESULT)
    opened = []
    real_open = open

    def recording_open(path, *args, **kwargs):
        opened.append(path)
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr("builtins.open", recording_open)
    _assert_refused(compactor.get_saved_output("tool_000_run_on_cluster.txt\n"), compactor)
    assert opened == [], "a refused name must never reach open()"


# ---------------------------------------------------------------------------
# agent_migrate._handle_get_tool_output / get_tool_output dispatch
# ---------------------------------------------------------------------------

@pytest.fixture
def agent_module(monkeypatch):
    mod = pytest.importorskip("agent_migrate")  # needs the engine's runtime deps
    monkeypatch.setattr(mod, "_compactor_history", [])
    return mod


def _spy(monkeypatch, compactor):
    calls = []
    original = compactor.get_saved_output

    def recording(filename):
        calls.append(filename)
        return original(filename)

    monkeypatch.setattr(compactor, "get_saved_output", recording)
    return calls


def test_handler_refuses_traversal_before_consulting_compactors(agent_module, compactor, monkeypatch):
    calls = _spy(monkeypatch, compactor)
    agent_module._compactor_history.append(compactor)
    logs = []
    result = agent_module._handle_get_tool_output("../../../oci_api_key.pem", log_fn=logs.append)
    assert result.startswith(REFUSED)
    assert MARKER not in result and compactor.test_root not in result
    assert calls == [], "no compactor may be asked to read a path-like name"
    assert any("REFUSED" in m for m in logs)


def test_handler_refuses_absolute_path(agent_module, compactor):
    agent_module._compactor_history.append(compactor)
    result = agent_module._handle_get_tool_output(compactor.test_secret, log_fn=None)
    assert result.startswith(REFUSED)
    assert MARKER not in result


def test_handler_stops_history_walk_on_refusal(agent_module, compactor, monkeypatch, tmp_path):
    older = ContextCompactor(call_id="call_000", log_fn=lambda m: None)
    older_calls = _spy(monkeypatch, older)
    monkeypatch.setattr(compactor, "get_saved_output",
                        lambda name: f"{REFUSED}: {name} resolves outside the tool output directory")
    agent_module._compactor_history.extend([older, compactor])  # compactor is newest
    result = agent_module._handle_get_tool_output("tool_000_x.txt", log_fn=None)
    assert result.startswith(REFUSED)
    assert older_calls == [], "a refusal must not fall through to older compactors"


def test_handler_finds_file_saved_by_an_older_compactor(agent_module, compactor):
    older = ContextCompactor(call_id="call_000", log_fn=lambda m: None)
    notice = older.save_and_truncate("run_on_cluster", LONG_RESULT)
    assert "tool_000_run_on_cluster.txt" in notice
    agent_module._compactor_history.extend([older, compactor])
    assert agent_module._handle_get_tool_output("tool_000_run_on_cluster.txt", log_fn=None) == LONG_RESULT


def test_dispatch_get_tool_output_refuses_path(agent_module, compactor):
    agent_module._compactor_history.append(compactor)
    result = asyncio.run(agent_module._handle_tool_call(
        "get_tool_output", {"filename": compactor.test_secret}, session=None, log_fn=None))
    assert result.startswith(REFUSED)
    assert MARKER not in result


def test_dispatch_get_tool_output_refuses_newline_terminated_name(agent_module, compactor, monkeypatch):
    calls = _spy(monkeypatch, compactor)
    compactor.save_and_truncate("run_on_cluster", LONG_RESULT)
    agent_module._compactor_history.append(compactor)
    result = asyncio.run(agent_module._handle_tool_call(
        "get_tool_output", {"filename": "tool_000_run_on_cluster.txt\n"}, session=None, log_fn=None))
    assert result.startswith(REFUSED)
    assert calls == [], "the handler must refuse before consulting any compactor"
