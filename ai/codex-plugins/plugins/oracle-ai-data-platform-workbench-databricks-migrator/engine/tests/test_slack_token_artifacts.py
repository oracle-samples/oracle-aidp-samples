"""Exercise real migration orchestration through uploaded artifact payloads."""
import ast
import asyncio
import base64
import contextlib
import io
import json
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import agent_migrate as am
import job_migrate as jm
import cell_analyzer
from slack_token_redaction import NULL_REDACTOR


@pytest.fixture
def runtime(monkeypatch, tmp_path):
    class WebClient:
        def __init__(self, token):
            self.token = token

        def conversations_history(self, **kwargs):
            assert self.token == "fixture-authentication"
            return {"messages": []}

        def files_upload_v2(self, **kwargs):
            raise AssertionError("Slack upload must never execute")

    class Kernel:
        cluster_id = "test-cluster"

        def __init__(self):
            self.files = {}
            self.namespace = {"WebClient": WebClient}
            self.executed = []

        async def execute(self, code, **kwargs):
            self.executed.append(code)
            # The artifact transport fake acknowledges task setup; widget
            # mutation/readback is exercised by test_task_widget_isolation.
            if "_task_expected =" in code:
                return {"status": "ok", "outputs": [
                    {"type": "stream", "name": "stdout", "text": "__AIDP_ISO_OK__\n"}]}
            if ("__AIDP_CHILD_COUNT__" in code or "__AIDP_CHILD_NB__" in code
                    or "all_cells = payload.get('cells'" in code):
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    exec(code, {})  # real child disk-check/save path, inside tmp_path
                return {"status": "ok", "outputs": [{"type": "stream", "text": output.getvalue()}]}
            match = re.search(r"with builtins.open\('([^']+)', 'w[ba]'\) as f:", code)
            encoded = re.search(r"base64.b64decode\('([^']+)'\)", code)
            if match and encoded:
                self.files[match.group(1)] = base64.b64decode(encoded.group(1)).decode()
            if "# REDACTION_" in code and not encoded:
                exec(code, self.namespace)
            return {"status": "ok", "outputs": []}

    async def analyze(cells, *args, **kwargs):
        return [{"cell_index": i, "action": "execute", "description": "plain Python",
                 "risks": [], "changes_needed": [], "storage_paths": [],
                 "table_refs": [], "fuse_risks": []} for i in range(len(cells))]

    async def noop(*args, **kwargs):
        return None

    async def verified(*args, **kwargs):
        return True

    monkeypatch.setattr(cell_analyzer, "analyze_notebook_cells", analyze)
    monkeypatch.setattr(jm, "load_catalog_snapshot", lambda: "")
    monkeypatch.setattr(jm, "get_bucket_mapping_context", lambda: "")
    monkeypatch.setattr(jm, "_inject_write_guard", lambda code: code)
    monkeypatch.setattr(jm, "registry_update", noop)
    monkeypatch.setattr(jm, "verify_upload", verified)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", False)
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", "/Workspace/redaction_fixture")
    monkeypatch.setattr(jm, "MIGRATED_BASE", "/Workspace/redaction_fixture/notebooks")
    monkeypatch.setattr(jm.tempfile, "mkdtemp", lambda **kwargs: str(tmp_path))
    monkeypatch.setattr(jm.shutil, "rmtree", lambda *args, **kwargs: None)
    monkeypatch.setattr(jm.asyncio, "sleep", noop)
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    monkeypatch.setattr(am, "_cell_history", [])
    monkeypatch.setattr(jm, "_cell_history", am._cell_history)
    monkeypatch.setattr(am, "_fixup_persistence_failures", [])
    monkeypatch.setattr(am, "_replay_cell_fn", jm._replay_cell_entry)
    return Kernel()


@pytest.mark.parametrize("run_all", [True, False])
@pytest.mark.parametrize("has_upload", [True, False])
@pytest.mark.parametrize("comment_tail", ["# team-label", "team-label"])
def test_saved_artifacts_redacted_but_validation_and_replay_authenticate(
        monkeypatch, runtime, run_all, has_upload, comment_tail):
    source = ("# REDACTION_CONSTRUCTOR\n"
              f"# client = WebClient(token='fixture-old-secret') {comment_tail}\n"
              f"# hook = WebhookClient('https://example.invalid/hook') {comment_tail}\n"
              "client = WebClient(token='fixture-authentication')\n")
    read = "# REDACTION_READ\nmessages = client.conversations_history(channel='C1')\n"
    if has_upload:
        read += "client.files_upload_v2(file='unused')\n"
    notebook = {"cells": [
        {"cell_type": "code", "source": [src], "metadata": {}, "outputs": [],
         "execution_count": None} for src in (source, read)
    ], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    monkeypatch.setattr(jm, "download_notebook", lambda *a, **kw: json.dumps(notebook).encode())
    result = asyncio.run(jm.process_notebook(
        "/Workspace/redaction_fixture/source.ipynb", runtime, "fixture", "task_current", {},
        run_all=run_all,
    ))
    assert result["status"] == ("PASS" if run_all else "CONVERTED_NOT_VALIDATED"), result
    finals = [json.loads(text) for path, text in runtime.files.items() if path.endswith(".ipynb")]
    assert len(finals) == 2
    assert all("fixture-authentication" not in text for text in runtime.files.values())
    assert all("fixture-old-secret" not in text for text in runtime.files.values())
    for saved in finals:
        sources = "\n".join("".join(c["source"]) for c in saved["cells"])
        # Credential removed to the inert placeholder in the SAVED artifact
        # (owner decision 2026-09-22; no eager env lookup). Same-run execution
        # below still authenticates because it runs from the un-redacted cache.
        assert "token='<redacted>'" in sources and "environ[" not in sources
        assert "team-label" in sources
        if has_upload:
            assert "# client.files_upload_v2" in sources
    if run_all:
        assert runtime.namespace["messages"] == {"messages": []}
        assert runtime.namespace["client"].token == "fixture-authentication"
    # Dependencies are consumed from this cache by %run. Sanitizing it would
    # break same-run reads even though every cell passed before save-back.
    assert any("fixture-authentication" in text for text in jm._notebook_content_cache.values())


@pytest.mark.parametrize("exception_name", ["SlackApiError", "UploadError"])
def test_exception_fstring_allows_delivery_without_exposing_real_token(
        monkeypatch, runtime, exception_name):
    sources = [
        "from slack_sdk import WebClient\n"
        f"from slack_sdk.errors import SlackApiError as {exception_name}\n",
        "client = WebClient(token='fixture-authentication')\n",
        "def upload_pdf_to_slack(error, response):\n"
        f"    raise {exception_name}(f'Slack upload failed: {{error}}', response)\n",
    ]
    notebook = {"cells": [
        {"cell_type": "code", "source": [source], "metadata": {}, "outputs": []}
        for source in sources
    ], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    monkeypatch.setattr(jm, "download_notebook", lambda *a, **kw: json.dumps(notebook).encode())
    result = asyncio.run(jm.process_notebook(
        "/Workspace/redaction_fixture/source.ipynb", runtime, "fixture", "task_current", {},
        run_all=False,
    ))
    assert result["status"] == "CONVERTED_NOT_VALIDATED", result
    finals = [json.loads(text) for path, text in runtime.files.items() if path.endswith(".ipynb")]
    assert len(finals) == 2, "both final and delivered notebooks must be saved"
    assert all("fixture-authentication" not in text for text in runtime.files.values())
    for saved in finals:
        source = "\n".join("".join(cell["source"]) for cell in saved["cells"])
        tree = ast.parse(source)
        raises = [node for node in ast.walk(tree) if isinstance(node, ast.Raise)]
        assert len(raises) == 1
        assert raises[0].exc.func.id == exception_name
        assert isinstance(raises[0].exc.args[0], ast.JoinedStr)
        assert "<redacted>" in source


@pytest.mark.parametrize("has_upload", [False, True])
@pytest.mark.parametrize("comment_tail", ["# team-label", "team-label"])
def test_child_save_back_and_repeat_inline(monkeypatch, tmp_path, runtime, has_upload, comment_tail):
    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path))
    source = ("# REDACTION_CHILD\n"
              f"# client = WebClient(token='fixture-old-secret') {comment_tail}\n"
              "client = WebClient(token='fixture-authentication')\n"
              "messages = client.conversations_history(channel='C1')\n")
    if has_upload:
        source += "client.files_upload_v2(file='unused')\n"
    notebook = {"cells": [
        {"cell_type": "code", "source": [source], "outputs": [], "metadata": {}},
        {"cell_type": "markdown", "source": ["example token: fixture-authentication"]},
    ], "metadata": {"debug": "fixture-authentication"}}
    path = tmp_path / "child.ipynb"
    path.write_text(json.dumps(notebook))
    jm._notebook_content_cache[str(path)] = json.dumps(notebook)
    log = jm.NotebookLog(str(path), "fixture", "task_child")
    for _ in range(2):
        result = asyncio.run(jm._inline_child_notebook(
            str(path), runtime, log, "fixture", "", "", "", {},
        ))
        assert result["status"] == "ok", result
        saved = path.read_text()
        assert "fixture-authentication" not in saved
        assert "fixture-old-secret" not in saved
        assert "team-label" in saved
        assert "<redacted>" in saved and "environ[" not in saved
        assert runtime.namespace["messages"] == {"messages": []}
        assert runtime.namespace["client"].token == "fixture-authentication"
    assert "Slack token redaction" in log.text(), (
        "a redaction-only save must name the actual reason, not 'no changes'")


@pytest.mark.parametrize("source, expected_error", [
    ("client = WebClient(token='fixture-' + suffix)\n", "UNRESOLVED computed Slack token"),
    ("# client = WebClient(token='fixture-' + suffix) team-label\n",
     "UNRESOLVED computed Slack token"),
    ("# WebClient(token='fixture-first') label WebClient(token='fixture-second'\n",
     "cannot parse commented Slack token"),
])
def test_unresolved_token_withholds_the_notebook_but_keeps_the_reports(
        monkeypatch, runtime, source, expected_error):
    """A refusal must not throw away the evidence for the whole migration.

    Supersedes the earlier contract that nothing at all was uploaded: that
    discarded every report for a run that may have executed for hours, and
    recorded nothing, so the next run redid the work and refused identically.
    The notebook is still withheld -- it is the one artifact that cannot be
    shown to be free of the credential -- and the task still FAILs.
    """
    notebook = {"cells": [{"cell_type": "code", "source": [source], "outputs": [], "metadata": {}}]}
    monkeypatch.setattr(jm, "download_notebook", lambda *a, **kw: json.dumps(notebook).encode())
    registered = []

    async def registry(*args, **kwargs):
        registered.append(args)

    monkeypatch.setattr(jm, "registry_update", registry)
    result = asyncio.run(jm.process_notebook(
        "/Workspace/redaction_fixture/source.ipynb", runtime, "fixture", "task_current", {},
        run_all=False,
    ))
    assert result["status"] == "FAIL"
    assert expected_error in result["error"]
    assert "fixture-" not in result["error"]
    assert registered == [], "a withheld notebook must never register as migrated"
    # Only the notes-only migration report is published: the notebook, the
    # test report and the execution log all embed cell source, which on a
    # refusal may still carry the literal fragment that caused it.
    assert sorted(p.rsplit("/", 1)[-1] for p in runtime.files) == ["migration_report.md"]
    assert any("SLACK REDACTION REFUSED" in text for text in runtime.files.values())
    assert all("fixture-" not in text for text in runtime.files.values()), \
        "a published report must not carry the unresolved literal"


_REVIEW_TOKEN = "xox" + "b-1234567890-fixture"
_REVIEW_COMMENTS = [
    ("residual_prefix_assignment", "# backup='prefix-secret'; WebClient(token='short-auth') team-label\n",
     "prefix-secret", True),
    ("residual_prefix_keyword", "# helper(key='prefix-secret', client=WebClient(token='short-auth')) team-label\n",
     "prefix-secret", True),
    ("residual_prefix_literal", "# 'prefix-secret' WebClient(token='short-auth') team-label\n",
     "prefix-secret", True),
    ("residual_prefix_dict_key", "# backup={'prefix-secret':None}; WebClient(token='wordlike') team-label\n",
     "prefix-secret", True),
    ("residual_qualified_wrapper", "# slack_sdk.helper(key='prefix-secret', client=WebClient(token='wordlike')) team-label\n",
     "prefix-secret", True),
    ("residual_adjacent_quote", "# WebClient(token='wordlike') prose'prefix-secret'\n",
     "prefix-secret", True),
    ("residual_rotated", "# client = WebClient(token='" + "xox" + "b-fake-1') rotated: use token='new-secret-2026-01' now\n",
     "new-secret-2026-01", True),
    ("residual_outer", "# helper(WebClient(token='old-secret-99'), key='second-secret-123') label\n",
     "second-secret-123", True),
    ("residual_backup", "# WebClient(token='abc-secret-11') backup token='xyz-secret-22' here\n",
     "xyz-secret-22", True),
    ("quoted_token", f'# example = "WebClient(token=\'{_REVIEW_TOKEN}\')" team-label\n',
     _REVIEW_TOKEN, True),
    ("quoted_compound", f'# example = "WebClient(token=\'{_REVIEW_TOKEN}\')"; WebClient(token="wordlike") team-label\n',
     _REVIEW_TOKEN, True),
    ("comment_shaped_docstring", '"""\n# WebClient(token=\'fixture-secret\') team-label\n"""\n',
     "fixture-secret", True),
    ("dictionary", "# cfg={'token':'wordlike'}; WebClient(**cfg) team-label\n", "wordlike", False),
    ("literal_dictionary", "# WebClient(**{'token':'wordlike'}) team-label\n", "wordlike", False),
    ("environment_default", "# WebClient(token=os.environ.get('SLACK_TOKEN', 'fixture-secret')) team-label\n",
     "fixture-secret", True),
    ("multiple_calls", "# WebClient(token='a'); tok='wordlike'; WebClient(token=tok) team-label\n",
     "wordlike", False),
    ("quoted_example", '# example = "WebClient(token=\'test\')" team-label\n',
     None, False),
    ("alias", "from slack_sdk import WebClient as WC\n# WC(token='wordlike') team-label\n",
     "wordlike", False),
    ("inline", "value = 1  # old = WebClient(token='short-auth') team-label\n",
     "short-auth", False),
    ("live_fallback", "WebClient(token=os.environ.get('SLACK_TOKEN', 'fixture-secret'))\n",
     "fixture-secret", True),
    ("plain_fallback", "# WebClient(token=os.environ.get('SLACK_TOKEN', 'fixture-secret'))\n",
     "fixture-secret", True),
    ("docstring", '\"\"\"\nWebClient(token=\'fixture-secret\')\n\"\"\"\n', "fixture-secret", True),
    ("later_alias", "# WC(token='wordlike'\nfrom slack_sdk import WebClient as WC\n",
     "wordlike", True),
    ("live_bytes", "WebClient(token=b'fixture-secret')\n", "fixture-secret", True),
    ("quoted_container", '# example = {"doc": "WebClient(token=\'test\')"} team-label\n',
     None, False),
    ("docstring_marker", '\"\"\"WebClient(token=\'<redacted>\'); WebClient(token=\'fixture-secret\')\"\"\"\n',
     "fixture-secret", True),
]
_REVIEW_COMMENTS += [
    (case + suffix, prefix + source + "\n", credential, refused)
    for suffix, prefix in [("_line", "# "), ("_inline", "value = 1  # ")]
    for case, source, credential, refused in [
        ("apostrophe", "don't: WebClient(token='fixture-secret') team-label", "fixture-secret", False),
        ("paired_apostrophes", "client's WebClient(token='fixture-secret') isn't used", "fixture-secret", False),
        ("qualified", "slack_sdk.WebClient(token='fixture-secret') team-label", "fixture-secret", False),
        ("unknown_qualified", "sdk.WebClient(token='fixture-secret') team-label", "fixture-secret", True),
        ("unknown_qualified_name", "don't: sdk.WebClient(token='WebClient-fixture') label", "WebClient-fixture", True),
        ("quoted_prose", "The client's token isn't stored here; WebClient(token=tok) is built in utils", None, False),
        ("ambiguous_quote", 'For example "WebClient(token=\'test\')" team-label', None, True),
        ("hidden_assignment", "old: tok = 'fixture-secret'; WebClient(token=tok) team-label", "fixture-secret", True),
        ("bytes", "WebClient(token=b'fixture-secret') team-label", "fixture-secret", True),
    ]
]


# These pre-existing paths are recorded, not fixed by full-line prose recovery.
# Do not treat their unchanged save behavior as evidence of credential safety.
_LEGACY_RESIDUALS = {"inline", "live_fallback", "plain_fallback", "docstring",
                     "live_bytes", "docstring_marker"}


def _legacy_residual(case):
    return case in _LEGACY_RESIDUALS or case.endswith("_inline")


def _review_refusal(case):
    if case.startswith(("unknown_qualified", "ambiguous_quote", "quoted_", "residual_")) or case in {"later_alias", "comment_shaped_docstring", "quoted_example", "quoted_container"}:
        return "cannot parse commented Slack token"
    return "UNRESOLVED"


@pytest.mark.parametrize("run_all", [False, True])
@pytest.mark.parametrize("case, source, credential, refused", _REVIEW_COMMENTS)
def test_review_cases_reach_main_save_back(monkeypatch, runtime, run_all, case, source, credential, refused):
    legacy = _legacy_residual(case)
    refused = (refused or case in {"quoted_example", "quoted_container"}) and not legacy
    if case == "alias":
        source += "from databricks.sdk import WorkspaceClient\n"
        async def model_response(*args, **kwargs):
            return "value = 1\n# WC(token='wordlike') team-label\n", {"input": 0, "output": 0}
        monkeypatch.setattr(jm, "call_opus_with_tools", model_response)
    notebook = {"cells": [{"cell_type": "code", "source": [source], "outputs": [], "metadata": {}}]}
    monkeypatch.setattr(jm, "download_notebook", lambda *a, **kw: json.dumps(notebook).encode())
    registered = []
    async def registry(*args, **kwargs):
        registered.append(args)
    monkeypatch.setattr(jm, "registry_update", registry)
    result = asyncio.run(jm.process_notebook(
        "/Workspace/redaction_fixture/review.ipynb", runtime, "review", case, {}, run_all=run_all))
    saved = [json.loads(text) for path, text in runtime.files.items() if path.endswith(".ipynb")]
    if refused:
        assert result["status"] == "FAIL"
        assert _review_refusal(case) in result["error"]
        assert not saved and not registered
        assert any("SLACK REDACTION REFUSED" in text for text in runtime.files.values())
    else:
        assert result["status"] == ("PASS" if run_all else "CONVERTED_NOT_VALIDATED"), result
        assert len(saved) == 2
        for artifact in saved:
            text = "\n".join("".join(cell["source"]) for cell in artifact["cells"])
            if legacy:
                assert credential is None or credential in text
            elif credential:
                assert credential not in text
                assert "<redacted>" in text
            else:
                assert source.strip() in text
            if case == "alias":
                assert "from slack_sdk import WebClient as WC" not in text
    if credential and not legacy:
        assert all(credential not in text for text in runtime.files.values())


@pytest.mark.parametrize("case, source, credential, refused", _REVIEW_COMMENTS)
def test_review_cases_reach_child_save_back(monkeypatch, tmp_path, runtime, case, source, credential, refused):
    legacy = _legacy_residual(case)
    refused = (refused or case in {"quoted_example", "quoted_container"}) and not legacy
    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path))
    notebook = {"cells": [{"cell_type": "code", "source": [source], "outputs": [], "metadata": {}}]}
    path = tmp_path / (case + ".ipynb")
    original = json.dumps(notebook)
    path.write_text(original)
    jm._notebook_content_cache[str(path)] = original
    result = asyncio.run(jm._inline_child_notebook(
        str(path), runtime, jm.NotebookLog(str(path), "review", case), "review", "", "", "", {}))
    saved = path.read_text()
    if refused:
        assert result["status"] == "error"
        assert _review_refusal(case) in result["error"]
        assert saved == original
    else:
        assert result["status"] == "ok", result
        if legacy:
            assert credential is None or credential in saved
        elif credential:
            assert credential not in saved
            assert "<redacted>" in saved
        else:
            assert saved == original


@pytest.mark.parametrize("run_all", [False, True])
@pytest.mark.parametrize("magic", ["%md It's a title", "%sql -- don't drop"])
def test_magic_prose_and_credential_comment_reach_main_save_back(monkeypatch, runtime, run_all, magic):
    notebook = {"cells": [{"cell_type": "code", "source": [source], "outputs": [], "metadata": {}}
                          for source in (magic, "# WebClient(token='wordlike') team-label\n")]}
    monkeypatch.setattr(jm, "download_notebook", lambda *a, **kw: json.dumps(notebook).encode())
    result = asyncio.run(jm.process_notebook(
        "/Workspace/redaction_fixture/magic.ipynb", runtime, "review", "magic", {}, run_all=run_all))
    assert result["status"] == ("PASS" if run_all else "CONVERTED_NOT_VALIDATED"), result
    saved = [json.loads(text) for path, text in runtime.files.items() if path.endswith(".ipynb")]
    assert len(saved) == 2
    for artifact in saved:
        text = "\n".join("".join(cell["source"]) for cell in artifact["cells"])
        assert "wordlike" not in text
        assert "<redacted>" in text
        assert magic.split(" ", 1)[1] in text


def test_redacted_upload_failure_is_not_registered_as_pass(monkeypatch, runtime):
    notebook = {"cells": [{"cell_type": "code", "source": [
        "client = WebClient(token='fixture-authentication')\n"], "outputs": [], "metadata": {}}]}
    monkeypatch.setattr(jm, "download_notebook", lambda *a, **kw: json.dumps(notebook).encode())
    registered = []

    async def registry(*args, **kwargs):
        registered.append(args)

    execute = runtime.execute
    async def failed(code, **kwargs):
        if "base64.b64decode(" in code:
            raise OSError("synthetic upload failure")
        return await execute(code, **kwargs)

    monkeypatch.setattr(jm, "registry_update", registry)
    monkeypatch.setattr(runtime, "execute", failed)
    result = asyncio.run(jm.process_notebook(
        "/Workspace/redaction_fixture/source.ipynb", runtime, "fixture", "task_current", {},
        run_all=False,
    ))
    assert result["status"] == "FAIL"
    assert "Failed to upload redacted" in result["error"]
    assert registered == []


def test_code_only_malformed_percent_run_is_not_registered_as_migrated(
    monkeypatch, runtime
):
    notebook = {"cells": [{"cell_type": "code", "source": [
        '%run "unterminated'
    ], "outputs": [], "metadata": {}}]}
    monkeypatch.setattr(
        jm, "download_notebook", lambda *a, **kw: json.dumps(notebook).encode()
    )
    registered = []

    async def registry(*args, **kwargs):
        registered.append(args)

    monkeypatch.setattr(jm, "registry_update", registry)
    result = asyncio.run(jm.process_notebook(
        "/Workspace/redaction_fixture/malformed.ipynb", runtime,
        "fixture", "dep_malformed", {}, run_all=False,
    ))

    assert result["status"] == "FAIL"
    assert registered and registered[-1][-1] == "FAIL"


@pytest.mark.parametrize("failure", ["save_result", "save_exception"])
def test_child_redaction_failure_cannot_report_success(monkeypatch, tmp_path, runtime, failure):
    # NB: a READ failure is no longer in this list — it is non-fatal by owner
    # decision (2026-09-22); see test_persistent_child_read_failure_* below. A
    # SAVE failure still must never report success with a partial artifact.
    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path))
    notebook = {"cells": [{"cell_type": "code", "source": [
        "# REDACTION_CHILD\nclient = WebClient(token='fixture-authentication')\n"
    ], "outputs": [], "metadata": {}}]}
    path = tmp_path / "child.ipynb"
    original = json.dumps(notebook)
    path.write_text(original)
    jm._notebook_content_cache[str(path)] = original
    execute = runtime.execute

    async def failed(code, **kwargs):
        if "all_cells = payload.get('cells'" in code:
            if failure == "save_result":
                return {"status": "error", "outputs": []}
            if failure == "save_exception":
                raise OSError("write failed")
        return await execute(code, **kwargs)

    monkeypatch.setattr(runtime, "execute", failed)
    result = asyncio.run(jm._inline_child_notebook(
        str(path), runtime, jm.NotebookLog(str(path), "fixture", "child"),
        "fixture", "", "", "", {},
    ))
    assert result["status"] == "error"
    assert path.read_text() == original  # never silently substitute a partial artifact


# ── Blast radius on the real save-back path ──────────────────────────────
# Artifact redaction is optional; the save-back path is not. These pin the
# approval bar: a migration with no Slack credential must produce byte-
# identical artifacts, an unchanged cache, and no new way to fail.

# Wall-clock stamps are the ONLY non-determinism between two runs of the same
# notebook: `[HH:MM:SS]` in the execution log and an ISO datetime in the test
# report. Everything else must match to the byte.
_LOG_STAMP = re.compile(r"\[\d\d:\d\d:\d\d\]")
_ISO_STAMP = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?")


def _comparable(files):
    """Uploaded artifacts with wall-clock stamps normalised away."""
    return {path: _ISO_STAMP.sub("<when>", _LOG_STAMP.sub("[--:--:--]", text))
            for path, text in files.items()}


def test_non_slack_notebook_is_byte_identical_with_and_without_redaction(
        monkeypatch, runtime):
    # A hyphenated name merely STARTING with a Slack prefix matched the
    # original catch-all regex and was silently rewritten to `<redacted>` in
    # notebooks that contain no Slack code at all.
    benign = "xo" + "xp-legacy-name-here"
    notebook = {"cells": [
        {"cell_type": "code", "source": [f"label = '{benign}'\nvalue = label.upper()\n"],
         "outputs": [{"output_type": "stream", "text": [f"wrote {benign}\n"]}],
         "metadata": {}, "execution_count": None},
        {"cell_type": "markdown", "source": [f"documents {benign}"]},
    ], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    monkeypatch.setattr(jm, "download_notebook", lambda *a, **kw: json.dumps(notebook).encode())

    def run(kernel):
        jm._notebook_content_cache.clear()
        result = asyncio.run(jm.process_notebook(
            "/Workspace/redaction_fixture/source.ipynb", kernel, "fixture",
            "task_current", {}, run_all=False))
        return result["status"], _comparable(kernel.files), dict(jm._notebook_content_cache)

    with_feature = run(runtime)
    # Exactly the pre-feature pipeline: every redaction call site is inert.
    monkeypatch.setattr(jm, "build_redactor", lambda *a, **kw: NULL_REDACTOR)
    without_feature = run(type(runtime)())

    assert with_feature == without_feature
    assert benign in json.dumps(with_feature[1]), "a non-credential must survive verbatim"


def test_cache_holds_the_written_bytes_when_there_is_no_credential(monkeypatch, runtime):
    """The Slack path deliberately caches the UN-redacted notebook so same-run
    `%run` reads keep authenticating. That divergence must not leak onto the
    common path, where the cache has always been the bytes written to disk."""
    notebook = {"cells": [{"cell_type": "code", "source": ["x = 1\n"], "outputs": [],
                           "metadata": {}, "execution_count": None}],
                "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    monkeypatch.setattr(jm, "download_notebook", lambda *a, **kw: json.dumps(notebook).encode())
    asyncio.run(jm.process_notebook(
        "/Workspace/redaction_fixture/source.ipynb", runtime, "fixture",
        "task_current", {}, run_all=False))
    migrated = "/Workspace/redaction_fixture/notebooks/redaction_fixture/source.ipynb"
    assert jm._notebook_content_cache[migrated] == runtime.files[migrated]


def test_no_persist_path_reads_the_unredacted_cache():
    """Guard for the one invariant that keeps the Slack-path cache safe.

    On the Slack path `_notebook_content_cache` holds the notebook WITH its
    credentials, while disk and every upload get the redacted copy. Nothing
    may persist straight from the cache without re-redacting. Both readers
    today are safe -- `_inline_child_notebook` re-redacts through its own
    save-back, and `_resolve_symbol_scope` is a read-only symbol scan that
    never writes -- so this fails loudly the day a third reader appears.
    """
    source = (Path(__file__).resolve().parents[1] / "scripts" / "job_migrate.py").read_text()
    tree = ast.parse(source)
    enclosing = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            enclosing[child] = node

    def top_level_function(node):
        """Attribute a nested helper to the function a reviewer would open."""
        name = None
        while node is not None:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                name = node.name
            node = enclosing.get(node)
        return name

    readers = set()
    for node in ast.walk(tree):
        reads_subscript = (isinstance(node, ast.Subscript)
                           and isinstance(node.value, ast.Name)
                           and node.value.id == "_notebook_content_cache"
                           and isinstance(node.ctx, ast.Load))
        reads_get = (isinstance(node, ast.Call)
                     and isinstance(node.func, ast.Attribute)
                     and node.func.attr == "get"
                     and isinstance(node.func.value, ast.Name)
                     and node.func.value.id == "_notebook_content_cache")
        if reads_subscript or reads_get:
            readers.add(top_level_function(node))
    assert readers == {"_inline_child_notebook", "_resolve_symbol_scope"}, (
        "a new reader of the un-redacted cache must be reviewed for re-redaction "
        f"before it can persist or upload: {sorted(readers)}")


def test_child_path_with_backslashes_survives_the_redacted_save(
        monkeypatch, tmp_path, runtime):
    """A manifest-supplied child path can carry backslashes. The disk-check leg
    already used json.dumps; the save leg's quote-only escape left `\\b` live
    inside the generated literal, and the redaction gate turns that into a
    hard child failure instead of a warning."""
    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path))
    notebook = {"cells": [{"cell_type": "code", "source": [
        "# REDACTION_CHILD\nclient = WebClient(token='fixture-authentication')\n"
    ], "outputs": [], "metadata": {}}], "metadata": {}}
    path = tmp_path / "a\\b child.ipynb"
    path.write_text(json.dumps(notebook))
    jm._notebook_content_cache[str(path)] = json.dumps(notebook)
    result = asyncio.run(jm._inline_child_notebook(
        str(path), runtime, jm.NotebookLog(str(path), "fixture", "task_child"),
        "fixture", "", "", "", {},
    ))
    assert result["status"] == "ok", result
    saved = path.read_text()
    assert "fixture-authentication" not in saved
    assert "<redacted>" in saved


def test_transient_child_read_failure_is_retried_not_fatal(monkeypatch, tmp_path, runtime):
    """/Workspace FUSE reads are documented as failing spuriously seconds after
    a verified write. Redaction made that read load-bearing and a miss a
    job-stopping failure, where the non-Slack path tolerates it — so one blip
    would halt every remaining task."""
    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path))
    notebook = {"cells": [{"cell_type": "code", "source": [
        "# REDACTION_CHILD\nclient = WebClient(token='fixture-authentication')\n"
    ], "outputs": [], "metadata": {}}], "metadata": {}}
    path = tmp_path / "child.ipynb"
    path.write_text(json.dumps(notebook))
    jm._notebook_content_cache[str(path)] = json.dumps(notebook)

    execute, attempts = runtime.execute, []

    async def flaky(code, **kwargs):
        if "__AIDP_CHILD_COUNT__" in code:
            attempts.append(1)
            if len(attempts) == 1:
                return {"status": "error", "outputs": []}   # one transient miss
        return await execute(code, **kwargs)

    monkeypatch.setattr(runtime, "execute", flaky)
    result = asyncio.run(jm._inline_child_notebook(
        str(path), runtime, jm.NotebookLog(str(path), "fixture", "child"),
        "fixture", "", "", "", {},
    ))
    assert result["status"] == "ok", result
    assert len(attempts) > 1, "the read was never retried"
    assert "fixture-authentication" not in path.read_text()


def test_child_read_that_cannot_verify_alignment_blocks_save(
        monkeypatch, tmp_path, runtime):
    """If not even the tiny cell COUNT comes back (dead kernel / unreadable or
    corrupt file), redaction alignment cannot be verified, so the by-index
    save-back is BLOCKED — never overlaid blind (review 2026-09-22: a mis-indexed
    overlay could keep an un-redacted code cell or write sources into the wrong
    cell). Distinct from the FL-Queries large-payload case, where the count
    returns fine and the run proceeds (next test). The cause is surfaced."""
    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path))
    notebook = {"cells": [{"cell_type": "code", "source": [
        "# REDACTION_CHILD\nclient = WebClient(token='fixture-authentication')\n"
    ], "outputs": [], "metadata": {}}], "metadata": {}}
    path = tmp_path / "child.ipynb"
    original = json.dumps(notebook)
    path.write_text(original)
    jm._notebook_content_cache[str(path)] = original
    execute = runtime.execute

    async def count_unreadable(code, **kwargs):
        if "__AIDP_CHILD_COUNT__" in code:
            return {"status": "error", "outputs": []}
        return await execute(code, **kwargs)

    monkeypatch.setattr(runtime, "execute", count_unreadable)
    log = jm.NotebookLog(str(path), "fixture", "child")
    result = asyncio.run(jm._inline_child_notebook(
        str(path), runtime, log, "fixture", "", "", "", {},
    ))
    assert result["status"] == "error"
    assert path.read_text() == original                    # never overlaid blind
    assert "verify redaction alignment" in log.text()      # cause surfaced


def test_child_notebook_payload_unreadable_is_non_fatal_with_alignment_verified(
        monkeypatch, tmp_path, runtime):
    """The real FL-Queries incident (2026-09-22): the tiny COUNT returns fine
    (alignment verified) but the large NOTEBOOK payload — an executed child
    carries all its cell outputs — cannot be parsed off kernel stdout. The task
    must PROCEED: code-cell sources and outputs are still redacted by index; only
    the on-disk MARKDOWN/METADATA scrub is skipped (owner-accepted residual). The
    cause is surfaced so the gap is never silent."""
    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path))
    notebook = {"cells": [
        {"cell_type": "code", "source": [
            "# REDACTION_CHILD\nclient = WebClient(token='fixture-authentication')\n"
        ], "outputs": [], "metadata": {}},
        {"cell_type": "markdown", "source": ["example: fixture-authentication"]},
    ], "metadata": {}}
    path = tmp_path / "child.ipynb"
    path.write_text(json.dumps(notebook))
    jm._notebook_content_cache[str(path)] = json.dumps(notebook)
    execute = runtime.execute

    async def corrupt_notebook_payload(code, **kwargs):
        # The COUNT execute (separate call) succeeds → alignment verified. Only
        # the large NOTEBOOK-payload execute fails to json-parse — the exact
        # large-payload failure this decoupling protects against.
        if "__AIDP_CHILD_NB__" in code:
            return {"status": "ok", "outputs": [{"type": "stream", "name": "stdout",
                    "text": "__AIDP_CHILD_NB__{ truncated not valid json\n"}]}
        return await execute(code, **kwargs)

    monkeypatch.setattr(runtime, "execute", corrupt_notebook_payload)
    log = jm.NotebookLog(str(path), "fixture", "child")
    result = asyncio.run(jm._inline_child_notebook(
        str(path), runtime, log, "fixture", "", "", "", {},
    ))
    assert result["status"] == "ok", result                        # non-fatal
    saved_nb = json.loads(path.read_text())
    code_src = "\n".join("".join(c["source"]) for c in saved_nb["cells"]
                         if c["cell_type"] == "code")
    assert "fixture-authentication" not in code_src and "<redacted>" in code_src
    # The owner-accepted residual: with the payload unread, on-disk MARKDOWN is
    # NOT scrubbed, so the token can remain there (rare; acceptable, not blocking).
    md_src = "\n".join("".join(c["source"]) for c in saved_nb["cells"]
                       if c["cell_type"] == "markdown")
    assert "fixture-authentication" in md_src
    assert "could not read the saved artifact" in log.text()       # residual surfaced


def test_child_notebook_execute_failure_does_not_lose_count(monkeypatch, tmp_path, runtime):
    """Param's #144 finding (2026-09-23): the count and the large notebook payload
    are read in SEPARATE execute() calls, so a notebook execute() that TIMES OUT
    or RAISES (large-payload failure, not just corrupt stdout) must NOT take the
    count with it. Alignment stays verified via the tiny count call and the run
    proceeds non-fatally — instead of the count-unreadable path blocking a large
    child (the class this PR fixes)."""
    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path))
    notebook = {"cells": [{"cell_type": "code", "source": [
        "# REDACTION_CHILD\nclient = WebClient(token='fixture-authentication')\n"
    ], "outputs": [], "metadata": {}}], "metadata": {}}
    path = tmp_path / "child.ipynb"
    path.write_text(json.dumps(notebook))
    jm._notebook_content_cache[str(path)] = json.dumps(notebook)
    execute = runtime.execute

    async def nb_execute_times_out(code, **kwargs):
        if "__AIDP_CHILD_NB__" in code:
            raise TimeoutError("synthetic large-payload notebook read timeout")
        return await execute(code, **kwargs)      # the tiny COUNT call still succeeds

    monkeypatch.setattr(runtime, "execute", nb_execute_times_out)
    log = jm.NotebookLog(str(path), "fixture", "child")
    result = asyncio.run(jm._inline_child_notebook(
        str(path), runtime, log, "fixture", "", "", "", {},
    ))
    assert result["status"] == "ok", result        # NOT blocked — count is decoupled
    saved_nb = json.loads(path.read_text())
    code_src = "\n".join("".join(c["source"]) for c in saved_nb["cells"]
                         if c["cell_type"] == "code")
    assert "fixture-authentication" not in code_src and "<redacted>" in code_src
    assert "could not read the saved artifact" in log.text()


def test_resume_over_a_redacted_child_says_so_before_executing(monkeypatch, tmp_path, runtime):
    """On a resume the child is read from the cluster — the already-redacted
    artifact — so its Slack cells raise KeyError on the tool's OWN injected
    lookup. Handing that to the fix loop invites a hardcoded workaround."""
    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path))
    redacted = {"cells": [{"cell_type": "code", "source": [
        "# REDACTION_CHILD\nclient = WebClient("
        "token=__import__('os').environ['SLACK_TOKEN_ABCDEF123456'])\n"
    ], "outputs": [], "metadata": {}}], "metadata": {}}
    path = tmp_path / "child.ipynb"
    path.write_text(json.dumps(redacted))
    jm._notebook_content_cache[str(path)] = json.dumps(redacted)
    log = jm.NotebookLog(str(path), "fixture", "child")
    asyncio.run(jm._inline_child_notebook(
        str(path), runtime, log, "fixture", "", "", "", {},
    ))
    text = log.text()
    assert "already-redacted artifact" in text
    assert "SLACK_TOKEN_ABCDEF123456" in text
    assert "do not hardcode" in text


@pytest.mark.parametrize("path, limit", [("pip", 120), ("scala_child", 120), ("refusal", 1000), ("code_only", 2000), ("execute", 2000)])
@pytest.mark.parametrize("cut_token", [False, True])
@pytest.mark.parametrize("leading", ["", "\n  "])
def test_report_source_is_redacted_before_preview_truncation(monkeypatch, runtime, path, limit, cut_token, leading):
    token = "wordlike"
    start = ("%pip install fixture_package\n" if path == "pip" else
             "%scala\n" if path == "scala_child" else "")
    prefix = "# WebClient(token="
    if cut_token:
        offset = len(leading) if path != "pip" else 0
        prefix = '# ' + ' ' * (limit - len(start) - len(prefix) - offset - 3) + prefix[2:]
    source = leading + start + prefix + repr(token) + ") team-label\n" + '# padding\n' * 250
    if path == "scala_child":
        source += 'oidlUtils.notebook.run("/Workspace/redaction_fixture/child", 60, Map())\n'
        async def child_failed(*args, **kwargs):
            return {"status": "error", "error": "synthetic child failure"}
        monkeypatch.setattr(jm, "_inline_child_notebook", child_failed)
    notebook = {"cells": [{"cell_type": "code", "source": [source], "outputs": [], "metadata": {}}]}
    monkeypatch.setattr(jm, "download_notebook", lambda *a, **kw: json.dumps(notebook).encode())
    async def installed(*args, **kwargs):
        return []
    monkeypatch.setattr(jm, "_resolve_pip_cell_packages", installed)
    if path == "refusal":
        monkeypatch.setattr(jm, "_redirect_refusal", lambda result: "synthetic redirect refusal")
    result = asyncio.run(jm.process_notebook(
        "/Workspace/redaction_fixture/preview.ipynb", runtime, "preview", path, {},
        run_all=path != "code_only"))
    assert result["status"] == ("FAIL" if path in {"refusal", "scala_child"} else
                                 "CONVERTED_NOT_VALIDATED" if path == "code_only" else "PASS"), result
    report = next(text for name, text in runtime.files.items() if name.endswith("test_report.md"))
    assert ("token='<" if cut_token else "<redacted>") in report
    assert "token='word" not in report
    assert all(token not in text for text in runtime.files.values())
    assert notebook["cells"][0]["source"] == [source]


def test_intermediate_source_preview_is_redacted_in_uploaded_report(monkeypatch, runtime):
    source = "# PREVIEW_INTERMEDIATE\n# WebClient(token='initial') team-label\nvalue = 1\n" + '# padding\n' * 250
    notebook = {"cells": [{"cell_type": "code", "source": [source], "outputs": [], "metadata": {}}]}
    monkeypatch.setattr(jm, "download_notebook", lambda *a, **kw: json.dumps(notebook).encode())
    monkeypatch.setattr(jm, "_apply_s3_translations", lambda text: text.replace("'initial'", "'wordlike'"))
    build = jm.build_redactor
    learned_views = []
    def record_views(*groups, **kwargs):
        learned_views.extend(''.join(group[0]['source']) for group in groups[:2])
        return build(*groups, **kwargs)
    monkeypatch.setattr(jm, "build_redactor", record_views)
    execute = jm._safe_execute
    observed = []
    async def execute_with_note(*args, **kwargs):
        result = await execute(*args, **kwargs)
        observed.append(kwargs.get("tag"))
        if kwargs.get("tag") == "cell":
            jm._current_cell_notes.append("synthetic final-view note")
        return result
    monkeypatch.setattr(jm, "_safe_execute", execute_with_note)
    result = asyncio.run(jm.process_notebook(
        "/Workspace/redaction_fixture/intermediate.ipynb", runtime, "preview", "intermediate", {}, run_all=True))
    assert result["status"] == "PASS", result
    assert 'cell' in observed, observed
    report = next(text for name, text in runtime.files.items() if name.endswith("test_report.md"))
    assert "<redacted>" in report
    assert all('wordlike' not in text and "token='initial'" not in text for text in runtime.files.values())
    assert 'synthetic final-view note' in learned_views[1]
    preview = source.replace("'initial'", "'wordlike'")
    assert all(view.rstrip() not in preview for view in learned_views)
    assert notebook["cells"][0]["source"] == [source]


# Review of 7e38bc1: exercise the artifact boundary, not just the resolver.
_BINDING_PROTECTION = [
    "Ctor = WebClient if condition else Ctor\n",
    "Ctor = [WebClient][0]\n",
    "choices = {'client': WebClient}\nCtor = choices['client']\n",
    "def choose():\n    return WebClient\nCtor = choose()\n",
    "def choose():\n    Local = WebClient\n    return Local\nCtor = choose()\n",
    "Ctor = WebClient\nif condition:\n    Ctor = dict\n",
    "Ctor = dict\nif condition:\n    Ctor = WebClient\n",
]
_BINDING_PRESERVATION = [
    "Ctor = WebClient\nCtor = dict\nobj = Ctor(token='application-value')\nobj.update(other)\n",
    "Ctor = WebClient\ndef use(Ctor):\n    obj = Ctor(token='application-value')\n    obj.update(other)\nuse(dict)\n",
    "Ctor = WebClient\ndef use():\n    Ctor = dict\n    obj = Ctor(token='application-value')\n    obj.update(other)\nuse()\n",
    "Ctor = WebClient\nclass Ctor:\n    pass\nobj = Ctor(token='application-value')\nobj.update(other)\n",
    "Error = WebClient\nError = ValueError\nraise Error(f'application error: {error}')\n",
    "Ctor = WebClient\nfrom internal_http import Client as Ctor\nobj = Ctor(token='application-value')\nobj.update(other)\n",
    "import slack_sdk as sdk\nsdk = application\nobj = sdk.WebClient(token='application-value')\nobj.update(other)\n",
]


@pytest.mark.parametrize('split_cells', [False, True])
@pytest.mark.parametrize('assignment', _BINDING_PROTECTION)
def test_constructor_lifecycle_protects_both_delivered_notebooks(
        monkeypatch, runtime, assignment, split_cells):
    setup = ('from slack_sdk.errors import SlackApiError as Ctor\n'
             'from slack_sdk import WebClient\n' + assignment)
    use = "client = Ctor(token='fixture-authentication')\nclient.files_upload_v2(file=path)\n"
    sources = [setup, use] if split_cells else [setup + use]
    notebook = {'cells': [{'cell_type': 'code', 'source': [src], 'metadata': {}, 'outputs': []}
                          for src in sources], 'metadata': {}, 'nbformat': 4, 'nbformat_minor': 5}
    monkeypatch.setattr(jm, 'download_notebook', lambda *a, **kw: json.dumps(notebook).encode())
    result = asyncio.run(jm.process_notebook(
        '/Workspace/redaction_fixture/source.ipynb', runtime, 'fixture', 'task_current', {},
        run_all=False))
    if 'condition' in assignment:
        assert result['status'] == 'FAIL', result
        assert 'UNRESOLVED Slack constructor identity' in result['error']
        assert not any(path.endswith('.ipynb') for path in runtime.files)
        assert all('fixture-authentication' not in text for text in runtime.files.values())
        return
    assert result['status'] == 'CONVERTED_NOT_VALIDATED', result
    finals = [json.loads(text) for path, text in runtime.files.items() if path.endswith('.ipynb')]
    assert len(finals) == 2
    assert all('fixture-authentication' not in text for text in runtime.files.values())
    for saved in finals:
        source = '\n'.join(''.join(cell['source']) for cell in saved['cells'])
        assert '<redacted>' in source
        assert not any(isinstance(node, ast.Call)
                       and getattr(node.func, 'attr', '') == 'files_upload_v2'
                       for node in ast.walk(ast.parse(source)))


@pytest.mark.parametrize('body', _BINDING_PRESERVATION)
def test_constructor_lifecycle_preserves_application_code_in_both_copies(
        monkeypatch, runtime, body):
    source = 'from slack_sdk import WebClient\n' + body
    notebook = {'cells': [{'cell_type': 'code', 'source': [source], 'metadata': {}, 'outputs': []}],
                'metadata': {}, 'nbformat': 4, 'nbformat_minor': 5}
    monkeypatch.setattr(jm, 'download_notebook', lambda *a, **kw: json.dumps(notebook).encode())
    result = asyncio.run(jm.process_notebook(
        '/Workspace/redaction_fixture/source.ipynb', runtime, 'fixture', 'task_current', {},
        run_all=False))
    assert result['status'] == 'CONVERTED_NOT_VALIDATED', result
    finals = [json.loads(text) for path, text in runtime.files.items() if path.endswith('.ipynb')]
    assert len(finals) == 2
    expected = ast.dump(ast.parse(source), include_attributes=False)
    for saved in finals:
        actual = '\n'.join(''.join(cell['source']) for cell in saved['cells'])
        assert ast.dump(ast.parse(actual), include_attributes=False) == expected


@pytest.mark.parametrize('assignment', [
    'Ctor = choose()\n',
    'Ctor = WebClient if condition else choose()\n',
    'from slack_sdk.webhook import WebhookClient\nCtor = WebClient if condition else WebhookClient\n',
])
def test_unresolved_constructor_identity_withholds_notebooks(monkeypatch, runtime, assignment):
    source = ('from slack_sdk.errors import SlackApiError as Ctor\n'
              'from slack_sdk import WebClient\n' + assignment
              + "client = Ctor(token='fixture-authentication')\nclient.files_upload_v2(file=path)\n")
    notebook = {'cells': [{'cell_type': 'code', 'source': [source], 'metadata': {}, 'outputs': []}],
                'metadata': {}, 'nbformat': 4, 'nbformat_minor': 5}
    monkeypatch.setattr(jm, 'download_notebook', lambda *a, **kw: json.dumps(notebook).encode())
    result = asyncio.run(jm.process_notebook(
        '/Workspace/redaction_fixture/source.ipynb', runtime, 'fixture', 'task_current', {},
        run_all=False))
    assert result['status'] == 'FAIL', result
    assert 'UNRESOLVED Slack constructor identity' in result['error']
    assert not any(path.endswith('.ipynb') for path in runtime.files)
    assert all('fixture-authentication' not in text for text in runtime.files.values())
