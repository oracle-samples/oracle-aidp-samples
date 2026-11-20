"""Artifact confidentiality and runtime authentication are separate contracts."""
import ast
import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from slack_token_redaction import SlackArtifactRedactor, SlackRedactionError
from slack_ast_neutralize import neutralize_cell

_REVIEW_TOKEN = "xox" + "b-1234567890-fixture"


def nb(*sources):
    return {"cells": [{"cell_type": "code", "source": [s], "outputs": [],
                       "metadata": {}} for s in sources], "metadata": {}}


def code(notebook):
    return "\n".join("".join(c["source"]) for c in notebook["cells"])


@pytest.mark.parametrize("ctor", ["WebClient", "AsyncWebClient", "SlackClient"])
@pytest.mark.parametrize("arg", ["token = 'fixture-secret'", "'fixture-secret'",
                                "timeout=30, token='fixture-secret'",
                                "**{'token': 'fixture-secret'}"])
def test_constructor_token_removed_without_mutating_runtime(ctor, arg, monkeypatch):
    original = nb(f"from slack_sdk import {ctor}\nclient = {ctor}({arg})\n")
    before = copy.deepcopy(original)
    redactor = SlackArtifactRedactor(original["cells"])
    saved = redactor.notebook(original)
    assert original == before
    assert "fixture-secret" not in str(saved)
    # Removed to the inert placeholder, not an env lookup (owner decision
    # 2026-09-22); the saved cell runs with no env configured -- no KeyError.
    assert "<redacted>" in code(saved) and "environ[" not in code(saved)
    ast.parse(code(saved))
    monkeypatch.delenv("SLACK_TOKEN", raising=False)
    namespace = {ctor: lambda *a, **kw: a[0] if a else kw.get("token")}
    exec(code(saved).replace(f"from slack_sdk import {ctor}\n", ""), namespace)
    assert namespace["client"] == "<redacted>"


@pytest.mark.parametrize("setup, call", [
    ("from slack_sdk import WebClient as WC\n", "WC"),
    ("import slack_sdk as sdk\n", "sdk.WebClient"),
    ("from slack_sdk.web import WebClient\n", "WebClient"),
    ("", "WebClient"),
])
def test_import_alias_across_cells(setup, call):
    original = nb(setup, f"client = {call}(token='fixture-secret')\n")
    assert "fixture-secret" not in code(SlackArtifactRedactor(original["cells"]).notebook(original))


@pytest.mark.parametrize("module", ["slack_sdk.errors", "slack.errors"])
@pytest.mark.parametrize("name", ["SlackApiError", "UploadError"])
@pytest.mark.parametrize("split_cells", [False, True])
def test_slack_exception_message_is_not_a_token(module, name, split_cells):
    """Data_charter_cost_report_job refused an f-string error, not a token."""
    setup = f"from {module} import SlackApiError as {name}\n"
    body = ("def upload_pdf_to_slack(error, response):\n"
            f"    raise {name}(f'Slack upload failed: {{error}}', response)\n")
    original = nb(setup, body) if split_cells else nb(setup + body)
    redactor = SlackArtifactRedactor(original["cells"])
    assert not redactor.active
    assert redactor.notebook(original) == original


@pytest.mark.parametrize("name", ["WebClient", "SlackApiError"])
def test_real_client_import_after_exception_still_redacts(name):
    original = nb(
        f"from slack_sdk.errors import SlackApiError as {name}\n",
        f"from slack_sdk import WebClient as {name}\n"
        f"client = {name}(token='fixture-secret')\n",
    )
    saved = code(SlackArtifactRedactor(original["cells"]).notebook(original))
    assert "fixture-secret" not in saved
    assert "<redacted>" in saved


@pytest.mark.parametrize("name", ["WebClient", "W"])
def test_exception_import_does_not_exempt_computed_client_token(name):
    original = nb(
        "from slack_sdk.errors import SlackApiError\n"
        f"from slack_sdk import WebClient as {name}\n"
        f"client = {name}(token='fixture-' + suffix)\n"
    )
    with pytest.raises(SlackRedactionError, match="UNRESOLVED computed Slack token"):
        SlackArtifactRedactor(original["cells"])


@pytest.mark.parametrize("name", ["WebClient", "W"])
@pytest.mark.parametrize("scenario", ["conditional", "multiple_callers"])
@pytest.mark.parametrize("module", ["slack_sdk.errors", "slack.errors"])
def test_exception_rebinding_cannot_hide_a_possible_client_token(name, scenario, module):
    setup = f"from slack_sdk import WebClient as {name}\n"
    error_import = f"from {module} import SlackApiError as {name}\n"
    if scenario == "conditional":
        source = (setup + "if condition:\n    " + error_import
                  + f"client = {name}(token='fixture-secret')\n")
    else:
        source = (setup + "def make_client():\n"
                  + f"    return {name}(token='fixture-secret')\n"
                  + "first = make_client()\n" + error_import
                  + "second = make_client()\n")
    original = nb(source)
    # One source call has both client and exception meanings. Refuse rather
    # than redact an application argument or silently expose a credential.
    with pytest.raises(SlackRedactionError, match="UNRESOLVED Slack constructor identity"):
        SlackArtifactRedactor(original["cells"])
    if scenario == "conditional":
        guarded = neutralize_cell(source + "client.files_upload_v2(file=path)\n").source
        assert not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                       and node.func.attr == "files_upload_v2"
                       for node in ast.walk(ast.parse(guarded)))


@pytest.mark.parametrize("module", ["slack_sdk.errors", "slack.errors"])
def test_conditional_exception_import_preserves_implicit_client_token(module):
    original = nb(
        "if condition:\n"
        f"    from {module} import SlackApiError as WebClient\n"
        "client = WebClient(token='fixture-secret')\n"
    )
    with pytest.raises(SlackRedactionError, match="UNRESOLVED Slack constructor identity"):
        SlackArtifactRedactor(original["cells"])


@pytest.mark.parametrize("module", ["slack_sdk.errors", "slack.errors"])
def test_proven_exception_alias_overrides_constructor_name_fallback(module):
    original = nb(
        f"from {module} import SlackApiError as WebClient\n"
        "raise WebClient(f'Slack upload failed: {error}', response)\n"
    )
    assert SlackArtifactRedactor(original["cells"]).notebook(original) == original


@pytest.mark.parametrize("name", ["SlackApiError", "W"])
@pytest.mark.parametrize("ctor", ["WebClient", "WC", "sdk.WebClient"])
@pytest.mark.parametrize("assignment", ["{name} = {ctor}\n", "{name}: object = {ctor}\n",
                                      "{name}, other = {ctor}, None\n", "({name} := {ctor})\n"])
def test_exception_alias_rebound_to_client_class_still_redacts(name, ctor, assignment):
    original = nb(
        "from slack_sdk import WebClient as WC\nimport slack_sdk as sdk\n"
        f"from slack_sdk.errors import SlackApiError as {name}\n"
        + assignment.format(name=name, ctor=ctor)
        + f"client = {name}(token='fixture-secret')\n"
    )
    saved = code(SlackArtifactRedactor(original["cells"]).notebook(original))
    assert "fixture-secret" not in saved
    assert "<redacted>" in saved


def test_non_slack_constructor_alias_does_not_redact_application_values():
    original = nb("from internal_http import WebClient\n"
                  "W = WebClient\nclient = W(token='application-value')\n")
    assert SlackArtifactRedactor(original["cells"]).notebook(original) == original


@pytest.mark.parametrize("sources", [
    ("tok = 'fixture-secret'\n", "client = WebClient(token=tok)\n"),
    ("tok = 'fixture-secret'\nalias = tok\nclient = WebClient(token=alias)\n",),
    ("def make():\n    tok = 'fixture-secret'\n    return WebClient(token=tok)\n",),
    ("client = WebClient(token=('fixture-' + 'secret'))\n",),
    ("client = WebClient(token=('fixture-' 'secret'))\n",),
    ("client = WebClient(\n token = r'fixture-secret',\n timeout=30,\n)\n",),
    ("client = WebClient(token='fixture-\\x73ecret')\n",),
    ("def make(token='fixture-secret'):\n    return WebClient(token=token)\n",),
    ("client = WebClient(token=('fixture-'\n                         + 'secret'))\n",),
    ("client = WebClient(token='''fixture-\nsecret''')\n",),
])
def test_literal_forms_and_variables(sources):
    original = nb(*sources)
    redactor = SlackArtifactRedactor(original["cells"])
    saved = code(redactor.notebook(original))
    assert "<redacted>" in saved      # recognised and removed (in place, no env)
    ast.parse(saved)
    assert "fixture-secret" not in redactor.text(str(original))


def test_preserved_comment_outputs_metadata_reports():
    original = nb("client = WebClient(token='fixture-secret')\n"
                  "client.files_upload_v2(file=f)\n")
    redactor = SlackArtifactRedactor(original["cells"])
    migrated = nb(neutralize_cell(code(original)).source + "# token='fixture-secret'\n")
    migrated["metadata"]["debug"] = "fixture-secret"
    migrated["cells"][0]["outputs"] = [{"text": ["fixture-secret"]}]
    migrated["cells"].append({"cell_type": "raw", "source": "token='fixture-secret'"})
    saved = redactor.notebook(migrated)
    assert "fixture-secret" not in str(saved)
    assert "files_upload_v2" in code(saved)
    assert not any(isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "files_upload_v2"
                   for n in ast.walk(ast.parse("".join(saved["cells"][0]["source"]))))
    assert "fixture-secret" not in redactor.text("debug: token='fixture-secret'")


@pytest.mark.parametrize("source", [
    "from internal_http import WebClient\nclient = WebClient(token='app-token')\n",
    "import internal_http as sdk\nclient = sdk.WebClient(token='app-token')\n",
    "client = WebClient(token=os.environ['SLACK_TOKEN'])\n",
    "client = WebClient(token=dbutils.secrets.get('scope', 'key'))\n",
    "client = WebClient(token=aidputils.secrets.get('credential', key='token'))\n",
    "client = WebClient(token=external_token)\n",
    "producer.send(topic, 'app-token')\n",
])
def test_non_secrets_and_non_slack_are_byte_identical(source):
    original = nb(source)
    assert SlackArtifactRedactor(original["cells"]).notebook(original) == original


def test_distinct_credentials_do_not_collapse():
    original = nb("a = WebClient(token='fixture-one')\nb = WebClient(token='fixture-two')\n")
    redactor = SlackArtifactRedactor(original["cells"])
    # Discovery still records the two distinct credentials separately...
    assert redactor.required_env == ("SLACK_TOKEN", "SLACK_TOKEN_2")
    out = code(redactor.notebook(original))
    # ...and both are removed to the inert placeholder (no env is emitted now).
    assert "fixture-one" not in out and "fixture-two" not in out
    assert out.count("token='<redacted>'") == 2


def test_two_artifacts_redact_independently_and_run_without_env(monkeypatch):
    """Contract reversed by owner decision (2026-09-22): each child artifact has
    its credential removed to the inert placeholder and RUNS with no env
    configured. The old design made each authenticate via a distinct env var;
    that eager lookup raised KeyError on an unconfigured clone (AutoRefit
    derived_raw) and is gone."""
    namespace = {"WebClient": lambda token: token}
    for path in ("/Workspace/one.ipynb", "/Workspace/two.ipynb"):
        original = nb("client = WebClient(token='fixture-secret')\n")
        redactor = SlackArtifactRedactor(original["cells"], artifact_path=path)
        namespace[path] = code(redactor.notebook(original))
        assert "fixture-secret" not in namespace[path]
        assert "environ[" not in namespace[path]
    monkeypatch.delenv("SLACK_TOKEN", raising=False)
    for path in ("/Workspace/one.ipynb", "/Workspace/two.ipynb"):
        exec(namespace[path], namespace)       # must NOT raise
        assert namespace["client"] == "<redacted>"


def test_idempotent_and_crlf_preserved():
    original = nb("client = WebClient(token='fixture-secret')\r\n")
    redactor = SlackArtifactRedactor(original["cells"])
    saved = redactor.notebook(original)
    assert redactor.notebook(saved) == saved
    assert "\n" not in code(saved).replace("\r\n", "")


def test_redacted_ctor_is_inert_not_a_runtime_raise(monkeypatch):
    """Contract reversed by owner decision (2026-09-22). The saved credential is
    replaced with an inert placeholder, NOT an eager `os.environ[...]` lookup, so
    an unconfigured clone no longer raises KeyError at import/def time -- the
    AutoRefit derived_raw failure. The Slack call is separately commented out, so
    the placeholder is never used as a real token."""
    original = nb("client = WebClient(token='fixture-secret')\n")
    saved = code(SlackArtifactRedactor(original["cells"]).notebook(original))
    assert "token='<redacted>'" in saved and "environ[" not in saved
    monkeypatch.delenv("SLACK_TOKEN", raising=False)
    ns = {"WebClient": lambda **kw: kw}
    exec(saved, ns)      # must NOT raise
    assert ns["client"] == {"token": "<redacted>"}


def test_invalid_slack_source_refuses_without_echoing_token():
    with pytest.raises(SlackRedactionError) as exc:
        SlackArtifactRedactor(nb("client = WebClient(token='fixture-secret'\n")["cells"])
    assert "fixture-secret" not in str(exc.value)


def test_already_commented_constructor_still_redacted():
    original = nb("# client = WebClient(token='fixture-secret')\n")
    assert "fixture-secret" not in code(SlackArtifactRedactor(original["cells"]).notebook(original))


@pytest.mark.parametrize("tail", [
    "label = 'fixture-secret' ' suffix'\n",
    "label = 'prefix ' 'fixture-secret'\n",
    "label = 'fixture-secret' + ' suffix'\n",
    "# old credential:\n# client = WebClient(token='fixture-secret')\n",
])
def test_matching_fragment_never_breaks_expression_syntax(tail):
    """Whatever the spelling, the artifact still parses and the auth site is
    rewritten. Whether the same string is ALSO replaced away from the
    constructor depends on its tier -- see the two tests below."""
    original = nb("client = WebClient(token='fixture-secret')\n" + tail)
    saved = code(SlackArtifactRedactor(original["cells"]).notebook(original))
    ast.parse(saved, feature_version=(3, 11))
    assert "token='<redacted>'" in saved


def _shaped_token():
    # Assembled at runtime: CI's added-line gate rejects the literal prefix.
    return ("xo" + "xb-") + "1234567890-9876543210987-AbCdEfGhIjKlMnOpQrStUvWx"


@pytest.mark.parametrize("tail", [
    "label = 'prefix ' {tok!r}\n",
    "label = {tok!r} + ' suffix'\n",
    "lookup = os.environ[{tok!r}]\n",
])
def test_a_shape_proven_token_is_removed_from_every_position(tail):
    """Matching Slack's documented token shape proves the STRING is a secret,
    so no position is safe to keep it in -- unlike a constructor literal,
    which only proves the value authenticates where it was found."""
    token = _shaped_token()
    original = nb(f"client = WebClient(token={token!r})\n" + tail.format(tok=token))
    saved = code(SlackArtifactRedactor(original["cells"]).notebook(original))
    ast.parse(saved, feature_version=(3, 11))
    assert token not in saved


@pytest.mark.parametrize("tail, survives", [
    ("lookup = os.environ['fixture-secret']\n", "os.environ['fixture-secret']"),
    ("mapping = {'fixture-secret': 1}\n", "{'fixture-secret': 1}"),
    ("if mode == 'fixture-secret': pass\n", "== 'fixture-secret'"),
])
def test_a_constructor_literal_is_not_rewritten_away_from_its_auth_site(
        tail, survives):
    """The same string as a lookup name, a dict key or a comparison operand is
    not an authentication. Redacting it there would silently change code that
    has nothing to do with Slack (e.g. turn a dict key into `{'<redacted>': 1}`),
    so away from the auth site the literal is left in place and reported."""
    original = nb("client = WebClient(token='fixture-secret')\n" + tail)
    redactor = SlackArtifactRedactor(original["cells"])
    saved = code(redactor.notebook(original))
    ast.parse(saved, feature_version=(3, 11))
    assert "token='<redacted>'" in saved, "the auth site must still be redacted"
    assert survives in saved, "unrelated code was altered"
    assert redactor.manual_action, "leaving it in place must be reported"
    assert "fixture-secret" not in " ".join(redactor.manual_action)


@pytest.mark.parametrize("source", [
    "%time client = WebClient(token='fixture-secret')\n",
    "label = 'é'; client = WebClient(token='fixture-secret')\n",
    "client = WebClient(token='fixture-\\nsecret')\n",
])
def test_offsets_preserve_unicode_magics_and_escaped_values(source):
    original = nb(source)
    saved = code(SlackArtifactRedactor(original["cells"]).notebook(original))
    assert "token='<redacted>'" in saved
    assert "fixture-" not in saved
    if "%time" in source:
        assert "%time client =" in saved


def test_cell_magic_keeps_first_line():
    original = nb("%%time\nclient = WebClient(token='fixture-secret')\n")
    saved = code(SlackArtifactRedactor(original["cells"]).notebook(original))
    assert saved.startswith("%%time\n")
    assert "fixture-secret" not in saved


def test_malformed_commented_constructor_cannot_silently_leak():
    with pytest.raises(SlackRedactionError, match="commented Slack token"):
        SlackArtifactRedactor(nb("# client = WebClient(token='fixture-secret'\n")["cells"])


@pytest.mark.parametrize("setup, ctor", [
    ("", "WebClient"),
    ("", "AsyncWebClient"),
    ("from slack_sdk import WebClient as WC\n", "WC"),
    ("import slack_sdk as sdk\n", "sdk.WebClient"),
])
@pytest.mark.parametrize("template", [
    "# client = {ctor}(token='fixture-secret') team-label\n",
    "# old client: {ctor}(token='fixture-secret') team-label\n",
    "# client = {ctor}(\n#     token='fixture-secret',\n#     timeout=30,\n# ) team-label\n",
    "# client = {ctor}(token=('fixture-' + 'secret')) team-label\n",
    "# tok = 'fixture-secret'; client = {ctor}(token=tok) team-label\n",
])
def test_commented_calls_with_prose_preserve_customer_text(setup, ctor, template):
    from slack_token_redaction import build_redactor
    comment = template.format(ctor=ctor)
    original = nb(setup, comment)
    before = copy.deepcopy(original)
    redactor = build_redactor(original["cells"])
    saved = redactor.notebook(original)
    assert original == before, "discovery/save must not modify runtime input"
    assert "fixture-secret" not in str(saved)
    assert "<redacted>" in code(saved)
    assert "team-label" in code(saved)
    assert code(saved).count("#") == code(original).count("#")
    ast.parse(code(saved))
    # Cold-cache resume must tolerate the exact artifact we just delivered.
    assert build_redactor(saved["cells"]).notebook(saved) == saved


@pytest.mark.parametrize("literal", [
    "'fixture-)#-secret'", "'fixture-\\\"-secret'", "'fixture-\\x73ecret'",
    "'''fixture-secret'''", "('fixture-' 'secret')",
])
def test_comment_call_boundaries_respect_python_strings(literal):
    original = nb(f"# ancien: WebClient(token={literal}, timeout=(1 + 2)) team-label\n")
    redactor = SlackArtifactRedactor(original["cells"])
    saved = code(redactor.notebook(original))
    assert "<redacted>" in saved
    assert "timeout=(1 + 2)) team-label" in saved
    assert "fixture-" not in saved


def test_multiple_commented_calls_and_nested_calls_are_all_inspected():
    original = nb("# old: WebClient(token='fixture-first', other=WebClient(token='fixture-second')) "
                  "team-one; WebClient(token='fixture-third') team-two\n")
    redactor = SlackArtifactRedactor(original["cells"])
    saved = code(redactor.notebook(original))
    assert set(redactor.tokens) == {"fixture-first", "fixture-second", "fixture-third"}
    assert "fixture-" not in saved
    assert "team-one" in saved and "team-two" in saved


@pytest.mark.parametrize("tail", [
    "WebClient(token='fixture-second'",  # missing close
    "WebClient(token='fixture-second', bad=])",  # mismatched brackets
    "WebClient(token='fixture-second', =1)",  # invalid call grammar
    "WebClient(token='fixture-' + suffix)",  # unresolved computed token
])
def test_recovered_comment_does_not_hide_a_second_unsafe_call(tail):
    with pytest.raises(SlackRedactionError) as exc:
        SlackArtifactRedactor(nb(
            "# WebClient(token='fixture-first') team-one " + tail + " team-two\n")["cells"])
    assert "fixture-" not in str(exc.value)


def test_legacy_redacted_comment_cannot_hide_a_second_malformed_call():
    with pytest.raises(SlackRedactionError, match="commented Slack token"):
        SlackArtifactRedactor(nb(
            "# WebClient(token=<redacted>) old WebClient(token='fixture-secret'\n")["cells"])


def test_prose_and_webhook_comments_keep_existing_token_only_contract():
    from slack_token_redaction import build_redactor
    original = nb(
        "# Import WebClient from Python SDK (github.com/slackapi/python-slack-sdk)\n",
        "# WebhookClient('https://example.invalid/hook') team-label\n")
    redactor = build_redactor(original["cells"])
    assert not redactor.active
    assert redactor.notebook(original) == original


@pytest.mark.parametrize("token", ["test", "a", "wordlike", "fixture-secret"])
@pytest.mark.parametrize("newline", ["\n", "\r\n"])
@pytest.mark.parametrize("template", [
    "# ancien: WebClient(token={token!r}) {token} account\n",
    "# client = WebClient(token={token!r}) # {token} account\n",
    "# client = WebClient(\n#     token={token!r},\n# ) {token} account\n",
])
def test_comment_redaction_handles_short_literals_without_scrubbing_prose(token, newline, template):
    original = nb(template.format(token=token).replace("\n", newline))
    redactor = SlackArtifactRedactor(original["cells"])
    saved = code(redactor.notebook(original))
    assert "token='<redacted>'" in saved or "token=<redacted>" in saved
    if token != "fixture-secret":
        assert f"{token} account" in saved
    assert saved.count(newline) == code(original).count(newline)


def test_recovered_comment_assignments_do_not_rebind_runtime_credentials():
    original = nb(
        "from slack_sdk import WebClient as WC\ntok = 'fixture-live'\n",
        "# tok = 'fixture-comment'; client = WC(token=tok) team-label\n",
        "client = WC(token=tok)\n")
    before = copy.deepcopy(original)
    redactor = SlackArtifactRedactor(original["cells"])
    assert set(redactor.tokens) == {"fixture-live", "fixture-comment"}
    assert "fixture-" not in code(redactor.notebook(original))
    assert original == before


@pytest.mark.parametrize("prefix", ["# ", "value = 1  # "])
@pytest.mark.parametrize("expression", [
    "os.environ.get('SLACK_TOKEN', 'fixture-secret')",
    "load_token('fixture-secret')",
    "['fixture-secret'][0]",
])
def test_comment_recovery_refuses_unresolved_authentication(prefix, expression):
    if prefix != "# ":
        # Inline-comment coverage is a pre-existing gap, outside prose recovery.
        original = nb(f"{prefix}WebClient(token={expression}) team-label\n")
        assert SlackArtifactRedactor(original["cells"]).notebook(original) == original
        return
    from slack_token_redaction import build_redactor
    with pytest.raises(SlackRedactionError, match="UNRESOLVED") as exc:
        build_redactor(nb(f"{prefix}WebClient(token={expression}) team-label\n")["cells"])
    assert "fixture-secret" not in str(exc.value)


@pytest.mark.parametrize("prefix", ["# ", "value = 1  # "])
def test_comment_recovery_keeps_assignments_between_calls(prefix):
    if prefix != "# ":
        # Inline-comment coverage is a pre-existing gap, outside prose recovery.
        original = nb(prefix + "WebClient(token='a'); tok='wordlike'; WebClient(token=tok) team-label\n")
        assert SlackArtifactRedactor(original["cells"]).notebook(original) == original
        return
    original = nb(prefix + "WebClient(token='a'); tok='wordlike'; "
                  "WebClient(token=tok) team-label\n")
    redactor = SlackArtifactRedactor(original["cells"])
    saved = code(redactor.notebook(original))
    assert "wordlike" not in saved
    assert saved.count("<redacted>") == 2
    assert "team-label" in saved
    assert code(original).count("wordlike") == 1


@pytest.mark.parametrize("prefix", ["# ", "value = 1  # "])
@pytest.mark.parametrize("quote", ['"', "'''", 'r"', 'f"'])
def test_comment_recovery_does_not_parse_calls_inside_strings(prefix, quote):
    from slack_token_redaction import build_redactor
    closing = '"' if quote in {'r"', 'f"'} else quote
    source = prefix + f"example = {quote}WebClient(token='test'){closing} team-label\n"
    original = nb(source)
    if prefix == "# ":
        with pytest.raises(SlackRedactionError, match="cannot parse commented Slack token"):
            build_redactor(original["cells"])
    else:
        assert build_redactor(original["cells"]).notebook(original) == original



@pytest.mark.parametrize("setup, ctor", [
    ("from slack_sdk import WebClient as WC", "WC"),
    ("import slack_sdk as sdk", "sdk.WebClient"),
])
@pytest.mark.parametrize("prefix", ["# ", "value = 1  # "])
@pytest.mark.parametrize("separate_cells", [False, True])
def test_comment_alias_survives_import_removed_in_migrated_view(setup, ctor, prefix, separate_cells):
    if prefix != "# ":
        # Inline-comment coverage is a pre-existing gap, outside prose recovery.
        original = nb(setup + "\n" + f"{prefix}{ctor}(token='wordlike') team-label\n")
        assert SlackArtifactRedactor(original["cells"]).notebook(original) == original
        return
    from slack_token_redaction import build_redactor
    comment = f"{prefix}{ctor}(token='wordlike') team-label\n"
    original = nb(setup + "\n", comment) if separate_cells else nb(setup + "\n" + comment)
    migrated = nb(comment)
    before = copy.deepcopy(migrated)
    redactor = build_redactor(original["cells"], migrated["cells"])
    saved = code(redactor.notebook(migrated))
    assert "wordlike" not in saved
    assert "token='<redacted>'" in saved
    assert migrated == before


@pytest.mark.parametrize("token", ["short-auth", "wordlike"])
def test_inline_comment_credentials_retain_staging_behavior(token):
    original = nb(f"value = 1  # old = WebClient(token={token!r}) team-label\n")
    redactor = SlackArtifactRedactor(original["cells"])
    # Preserve the documented pre-existing inline credential gap in this PR.
    assert redactor.notebook(original) == original
    assert token in code(original)



@pytest.mark.parametrize("declaration", ["# tok='wordlike'; ", "tok='wordlike'\n# "])
def test_recovered_call_cannot_hide_a_literal_through_an_unknown_function(declaration):
    from slack_token_redaction import build_redactor
    with pytest.raises(SlackRedactionError, match="UNRESOLVED"):
        build_redactor(nb(declaration + "WebClient(token=identity(tok)) label\n")["cells"])


def test_quoted_constructor_beside_real_call_retains_base_refusal():
    original = nb('# example = "WebClient(token=\'test\')"; '
                  'WebClient(token="wordlike") team-label\n')
    with pytest.raises(SlackRedactionError, match="cannot parse commented Slack token"):
        SlackArtifactRedactor(original["cells"])



@pytest.mark.parametrize("prefix", ["# ", "value = 1  # "])
@pytest.mark.parametrize("comment", [
    "don't: WebClient(token='fixture-secret') team-label",
    "client's WebClient(token='fixture-secret') isn't used",
    "x = slack_sdk.WebClient(token='fixture-secret') team-label",
    "x = slack.WebClient(token='fixture-secret') team-label",
])
def test_prose_quotes_and_builtin_qualified_constructors_cannot_hide_credentials(prefix, comment):
    if prefix != "# ":
        # Inline-comment coverage is a pre-existing gap, outside prose recovery.
        original = nb(prefix + comment + "\n")
        assert SlackArtifactRedactor(original["cells"]).notebook(original) == original
        return
    original = nb(prefix + comment + "\n")
    saved = code(SlackArtifactRedactor(original["cells"]).notebook(original))
    assert "fixture-secret" not in saved
    assert "<redacted>" in saved
    assert ast.dump(ast.parse(saved)) == ast.dump(ast.parse(code(original)))


@pytest.mark.parametrize("prefix", ["# ", "value = 1  # "])
@pytest.mark.parametrize("comment", [
    "x = sdk.WebClient(token='fixture-secret') team-label",
    "old: tok = 'fixture-secret'; client = WebClient(token=tok) team-label",
    "WebClient(token=b'fixture-secret') team-label",
])
def test_unproven_comment_authentication_refuses_without_echoing_source(prefix, comment):
    if prefix != "# ":
        # Inline-comment coverage is a pre-existing gap, outside prose recovery.
        original = nb(prefix + comment + "\n")
        assert SlackArtifactRedactor(original["cells"]).notebook(original) == original
        return
    with pytest.raises(SlackRedactionError) as exc:
        SlackArtifactRedactor(nb(prefix + comment + "\n")["cells"])
    assert "fixture-secret" not in str(exc.value)


@pytest.mark.parametrize("prefix, tail", [("", ""), ("# ", ""), ("# ", " team-label")])
def test_unresolved_literal_authentication_is_scoped_to_recovered_comments(prefix, tail):
    source = prefix + "WebClient(token=os.environ.get('SLACK_TOKEN', 'fixture-secret'))" + tail
    if tail:
        with pytest.raises(SlackRedactionError, match="UNRESOLVED"):
            SlackArtifactRedactor(nb(source)["cells"])
    else:
        # Live/plain-comment fallback handling is unchanged from staging.
        original = nb(source)
        assert SlackArtifactRedactor(original["cells"]).notebook(original) == original



@pytest.mark.parametrize("comment", [
    "The client's token isn't stored here; WebClient(token=tok) is built in utils",
    "The token is 'external'; WebClient(token=tok) is built in utils",
])
def test_quoted_prose_is_not_evidence_of_a_token_assignment(comment):
    original = nb("# " + comment + "\n")
    assert SlackArtifactRedactor(original["cells"]).notebook(original) == original


@pytest.mark.parametrize("magic", ["%md It's a title", "%sql -- don't drop", "%run /a/client's_notebook"])
def test_magic_apostrophe_does_not_break_comment_discovery(magic):
    original = nb(magic, "# WebClient(token='wordlike') team-label\n")
    saved = SlackArtifactRedactor(original["cells"]).notebook(original)
    assert saved["cells"][0] == original["cells"][0]
    assert "wordlike" not in code(saved)


def test_later_alias_malformed_comment_refuses_during_discovery():
    original = nb("# WC(token='wordlike'\n", "from slack_sdk import WebClient as WC\n")
    with pytest.raises(SlackRedactionError, match="cannot parse commented"):
        SlackArtifactRedactor(original["cells"])


@pytest.mark.parametrize("scope", ["", "def example():\n", "class Example:\n"])
def test_plain_docstring_retains_staging_behavior(scope):
    import textwrap
    doc = '"""\nWebClient(token=\'fixture-secret\')\n"""\n'
    source = scope + (textwrap.indent(doc, "    ") if scope else doc)
    original = nb(source)
    assert SlackArtifactRedactor(original["cells"]).notebook(original) == original



def test_documentation_without_credentials_is_preserved():
    original = nb('"""Use WebClient for Slack reads."""\n',
                  'example = "WebClient(token=\'test\')"\n')
    assert SlackArtifactRedactor(original["cells"]).notebook(original) == original


def test_plain_docstring_placeholder_retains_staging_behavior():
    source = ('"""WebClient(token=\'<redacted>\'); '
              'WebClient(token=\'fixture-secret\')"""\n')
    original = nb(source)
    assert SlackArtifactRedactor(original["cells"]).notebook(original) == original



@pytest.mark.parametrize("expression", [
    '("WebClient(token=\'test\')")',
    '["WebClient(token=\'test\')"]',
    '{"doc": "WebClient(token=\'test\')"}',
    '(f"WebClient(token=\'test\')")',
])
def test_quoted_documentation_with_prose_retains_base_refusal(expression):
    original = nb(f"# example = {expression} team-label\n")
    with pytest.raises(SlackRedactionError, match="cannot parse commented Slack token"):
        SlackArtifactRedactor(original["cells"])



@pytest.mark.parametrize("prefix", ["# ", "value = 1  # "])
@pytest.mark.parametrize("quote", ['"', "'''", 'f"'])
def test_ambiguous_quoted_example_after_prose_refuses_instead_of_mutating(prefix, quote):
    if prefix != "# ":
        # Inline-comment coverage is a pre-existing gap, outside prose recovery.
        original = nb(prefix + f"For example {quote}WebClient(token='test'){chr(34) if quote == 'f'+chr(34) else quote} team-label\n")
        assert SlackArtifactRedactor(original["cells"]).notebook(original) == original
        return
    closing = '"' if quote == 'f"' else quote
    source = prefix + f"For example {quote}WebClient(token='test'){closing} team-label\n"
    with pytest.raises(SlackRedactionError, match="cannot parse commented Slack token"):
        SlackArtifactRedactor(nb(source)["cells"])


def test_reused_token_variable_does_not_redact_unrelated_credentials():
    original = nb("token = 'unrelated-service'\nother = OtherClient(token=token)\n",
                  "token = 'fixture-secret'\nclient = WebClient(token=token)\n",
                  "token = 'later-service'\n")
    redactor = SlackArtifactRedactor(original["cells"])
    assert tuple(redactor.tokens) == ("fixture-secret",)
    saved = code(redactor.notebook(original))
    assert "token = 'unrelated-service'" in saved
    assert "token = 'later-service'" in saved


def test_token_alias_resolves_before_later_rebind():
    original = nb("token = 'fixture-secret'\nalias = token\ntoken = 'unrelated-service'\n"
                  "client = WebClient(token=alias)\n")
    redactor = SlackArtifactRedactor(original["cells"])
    assert tuple(redactor.tokens) == ("fixture-secret",)


def test_conditional_token_values_both_redacted():
    original = nb("token = 'fixture-first'\nif flag:\n    token = 'fixture-second'\n"
                  "client = WebClient(token=token)\n")
    redactor = SlackArtifactRedactor(original["cells"])
    assert set(redactor.tokens) == {"fixture-first", "fixture-second"}


@pytest.mark.parametrize("scope", ["", "def example():\n", "class Example:\n"])
def test_comment_shaped_docstring_keeps_legacy_refusal(scope):
    import textwrap
    doc = '"""\n# WebClient(token=\'fixture-secret\') team-label\n"""\n'
    source = scope + (textwrap.indent(doc, "    ") if scope else doc)
    with pytest.raises(SlackRedactionError, match="cannot parse commented Slack token"):
        SlackArtifactRedactor(nb(source)["cells"])


@pytest.mark.parametrize('quote', ['"', "'''", 'r"'])
def test_quoted_constructor_with_prose_is_not_hidden_from_refusal(quote):
    closing = '"' if quote == 'r"' else quote
    source = f"# example = {quote}WebClient(token='{_REVIEW_TOKEN}'){closing} team-label\n"
    with pytest.raises(SlackRedactionError, match="cannot parse commented Slack token"):
        SlackArtifactRedactor(nb(source)["cells"])


@pytest.mark.parametrize('limit', [120, 1000, 2000])
@pytest.mark.parametrize('token', ['a', 'test', 'wordlike'])
@pytest.mark.parametrize('cut_token', [False, True])
def test_source_preview_redacts_before_truncation(limit, token, cut_token):
    from slack_token_redaction import SourcePreview
    prefix = "# WebClient(token="
    if cut_token:
        prefix = '# ' + ' ' * (limit - len(prefix) - 1) + prefix[2:]
    source = prefix + repr(token) + ") team-label\n# latest test data\n" + '# padding\n' * 250
    original = nb(source)
    redactor = SlackArtifactRedactor(original['cells'])
    preview = SourcePreview(source, limit)
    assert preview == source[:limit]
    assert preview.redacted(redactor) == redactor.text(source)[:limit]
    assert 'latest test data' in redactor.text(source)
    assert original == nb(source)
    assert copy.deepcopy(preview).redacted(redactor) == preview.redacted(redactor)
    import json
    assert json.loads(json.dumps({'code': preview})) == {'code': source[:limit]}


@pytest.mark.parametrize("tail", ["", "; WebClient(token='wordlike')"])
def test_quoted_credential_remains_in_residual_even_in_recovered_assignment(tail):
    source = f'# example = "WebClient(token=\'{_REVIEW_TOKEN}\')"' + tail + ' team-label\n'
    with pytest.raises(SlackRedactionError, match="cannot parse commented Slack token"):
        SlackArtifactRedactor(nb(source)["cells"])


@pytest.mark.parametrize("comment", [
    "# client = WebClient(token='old-secret') rotated: use token='new-secret-2026-01' now",
    "# helper(WebClient(token='old-secret-99'), key='second-secret-123') label",
    "# WebClient(token='abc-secret-11') backup token='xyz-secret-22' here",
    "# WebClient(token='<redacted>') backup token='xyz-secret-22' here",
    "# WebClient(token='test') note r'backup-secret' here",
])
def test_unrecovered_literals_after_slack_calls_refuse(comment):
    with pytest.raises(SlackRedactionError, match="cannot parse commented Slack token"):
        SlackArtifactRedactor(nb(comment)["cells"])


@pytest.mark.parametrize("token", ["short-auth", "wordlike"])
@pytest.mark.parametrize("setup, ctor", [("", "WebClient"),
                                       ("from slack_sdk import WebClient as WC\n", "WC")])
@pytest.mark.parametrize("template", [
    "backup='prefix-secret'; {ctor}(token={token!r}) team-label",
    "helper(key='prefix-secret', client={ctor}(token={token!r})) team-label",
    "'prefix-secret' {ctor}(token={token!r}) team-label",
    "backup={{'prefix-secret':None}}; {ctor}(token={token!r}) team-label",
    "backup={{'field':'prefix-secret'}}; {ctor}(token={token!r}) team-label",
    "slack_sdk.helper(key='prefix-secret', client={ctor}(token={token!r})) team-label",
    "{ctor}(token={token!r}) prose'prefix-secret'",
    "préfixe='prefix-' + 'secret'; {ctor}(token={token!r}) team-label",
    "{ctor}(token={token!r}); backup='prefix-secret'; {ctor}(token={token!r}) team-label",
])
def test_recovery_checks_all_unhandled_literals(setup, ctor, token, template):
    from slack_token_redaction import build_redactor
    original = nb(setup + '# ' + template.format(ctor=ctor, token=token) + '\n')
    before = copy.deepcopy(original)
    with pytest.raises(SlackRedactionError, match="cannot parse commented Slack token") as exc:
        build_redactor(original["cells"])
    assert "prefix-secret" not in str(exc.value)
    assert original == before


@pytest.mark.parametrize("token", ["wordlike", "<redacted>"])
@pytest.mark.parametrize("newline", ["\n", "\r\n"])
@pytest.mark.parametrize("assignment, call", [
    ("tok={token!r}", "WebClient(token=tok)"),
    ("cfg={{'token':{token!r}}}", "WebClient(**cfg)"),
])
def test_proven_prefix_authentication_survives_residual_check_and_resume(token, newline, assignment, call):
    from slack_token_redaction import build_redactor
    source = '# ' + assignment.format(token=token) + '; ' + call + "; WebClient(token='a') team-label" + newline
    original = nb(source)
    redactor = build_redactor(original["cells"])
    saved = redactor.notebook(original)
    assert "wordlike" not in code(saved) and "token='a'" not in code(saved)
    assert 'team-label' in code(saved)
    assert build_redactor(saved["cells"]).notebook(saved) == saved
    assert original == nb(source)


@pytest.mark.parametrize("limit", [120, 1000, 2000])
@pytest.mark.parametrize("change", ["strip", "transform"])
def test_preview_redacts_source_different_from_learned_views(limit, change):
    from slack_token_redaction import SourcePreview, build_redactor
    source = "\n # WebClient(token='wordlike') team-label\n" + '# padding\n' * 250
    preview = source.strip() if change == "strip" else source.replace('padding', 'changed')
    redactor = build_redactor(nb(source)["cells"], nb(source + '# final note\n')["cells"])
    saved = SourcePreview(preview, limit).redacted(redactor)
    assert 'wordlike' not in saved
    assert '<redacted>' in saved and 'team-label' in saved


@pytest.mark.parametrize('sources', [
    ('from slack_sdk import WebClient\nCtor = WebClient\n',
     "client = Ctor(token='fixture-secret')\n", 'Ctor = dict\n'),
    ('from slack_sdk import WebClient\nCtor = WebClient\nCtor = dict\n',
     "obj = Ctor(token='fixture-secret')\nobj.update(other)\n",
     "client = WebClient(token='fixture-secret')\n"),
])
def test_authentication_rewrite_uses_statement_and_cell_binding(sources):
    original = nb(*sources)
    saved = SlackArtifactRedactor(original['cells']).notebook(original)
    if 'obj = Ctor' in sources[1]:
        assert ''.join(saved['cells'][1]['source']) == sources[1]
    else:
        assert 'fixture-secret' not in code(saved)
    assert '<redacted>' in code(saved)


@pytest.mark.parametrize('kind', ['WebClient', 'WebhookClient'])
def test_constructor_alias_retains_authentication_signature(kind):
    module = 'slack_sdk.webhook' if kind == 'WebhookClient' else 'slack_sdk'
    original = nb(f'from {module} import {kind}\nCtor = {kind}\n'
                  "client = Ctor('application-url-or-token')\n")
    saved = SlackArtifactRedactor(original['cells']).notebook(original)
    if kind == 'WebhookClient':
        assert saved == original
    else:
        assert 'application-url-or-token' not in code(saved)


@pytest.mark.parametrize('actual', ['WebClient', 'dict'])
def test_constructor_argument_obeys_actual_caller(actual):
    original = nb('from slack_sdk import WebClient\nCtor = WebClient\n'
                  'def use(Ctor):\n'
                  "    obj = Ctor(token='fixture-value')\n"
                  '    obj.update(other)\n'
                  f'use({actual})\n')
    redactor = SlackArtifactRedactor(original['cells'])
    saved = redactor.notebook(original)
    guarded = neutralize_cell(code(original)).source
    if actual == 'dict':
        assert saved == original
        assert ast.dump(ast.parse(guarded)) == ast.dump(ast.parse(code(original)))
    else:
        assert 'fixture-value' not in code(saved)
        assert not any(isinstance(node, ast.Call) and getattr(node.func, 'attr', '') == 'update'
                       for node in ast.walk(ast.parse(guarded)))


def test_constructor_argument_multiple_callers_refuses_ambiguous_rewrite():
    original = nb('from slack_sdk import WebClient\n'
                  'def use(Ctor):\n'
                  "    obj = Ctor(token='fixture-value')\n"
                  'use(WebClient)\nuse(dict)\n')
    with pytest.raises(SlackRedactionError, match='UNRESOLVED Slack constructor identity'):
        SlackArtifactRedactor(original['cells'])


@pytest.mark.parametrize('replacement', [
    'for Ctor in [dict]:\n    pass\n',
    'try:\n    pass\nexcept Exception as Ctor:\n    pass\nCtor = dict\n',
    'del Ctor\nCtor = dict\n',
])
def test_constructor_other_binding_forms_do_not_leave_stale_identity(replacement):
    original = nb('from slack_sdk import WebClient\nCtor = WebClient\n'
                  + replacement + "obj = Ctor(token='application-value')\nobj.update(other)\n")
    assert SlackArtifactRedactor(original['cells']).notebook(original) == original
    assert ast.dump(ast.parse(neutralize_cell(code(original)).source)) == ast.dump(ast.parse(code(original)))


@pytest.mark.parametrize('invocation', ['use()', ''])
def test_constructor_default_is_resolved_in_definition_scope(invocation):
    original = nb('from slack_sdk import WebClient\n'
                  'def use(Ctor=WebClient):\n'
                  "    obj = Ctor(token='fixture-value')\n"
                  f'{invocation}\n')
    saved = SlackArtifactRedactor(original['cells']).notebook(original)
    assert 'fixture-value' not in code(saved)


def test_replacement_function_returning_client_still_neutralizes_upload():
    original = nb('from slack_sdk import WebClient\nCtor = WebClient\n'
                  'def Ctor(token):\n    return WebClient(token=token)\n'
                  "client = Ctor(token='fixture-value')\nclient.files_upload_v2(file=path)\n")
    guarded = neutralize_cell(code(original)).source
    assert not any(isinstance(node, ast.Call) and getattr(node.func, 'attr', '') == 'files_upload_v2'
                   for node in ast.walk(ast.parse(guarded)))


@pytest.mark.parametrize('replacement', ['WebClient', 'dict'])
@pytest.mark.parametrize('wrapper', [False, True])
def test_global_constructor_rebinding_uses_call_time_identity(replacement, wrapper):
    original = nb('from slack_sdk.errors import SlackApiError as Ctor\n'
                  'from slack_sdk import WebClient\n'
                  'def initialize():\n    global Ctor\n'
                  f'    Ctor = {replacement}\n'
                  + ('def wrapper():\n    initialize()\nwrapper()\n' if wrapper else 'initialize()\n')
                  + "obj = Ctor(token='fixture-value')\nobj.update(other)\n")
    saved = SlackArtifactRedactor(original['cells']).notebook(original)
    guarded = neutralize_cell(code(original)).source
    if replacement == 'dict':
        assert saved == original
        assert ast.dump(ast.parse(guarded)) == ast.dump(ast.parse(code(original)))
    else:
        assert 'fixture-value' not in code(saved)
        assert not any(isinstance(node, ast.Call) and getattr(node.func, 'attr', '') == 'update'
                       for node in ast.walk(ast.parse(guarded)))


@pytest.mark.parametrize('expr', ['getattr(sdk, "WebClient")', 'type(obj)', '(lambda: WebClient)()'])
def test_opaque_constructor_reassignment_cannot_disable_protection(expr):
    original = nb('from slack_sdk.errors import SlackApiError as Ctor\n'
                  'import slack_sdk as sdk\nfrom slack_sdk import WebClient\n'
                  f'Ctor = {expr}\n'
                  "obj = Ctor(token='fixture-value')\nobj.files_upload_v2(file=path)\n")
    with pytest.raises(SlackRedactionError, match='UNRESOLVED Slack constructor identity'):
        SlackArtifactRedactor(original['cells'])


def test_conditional_import_is_proven_within_its_own_branch():
    original = nb('from slack_sdk.errors import SlackApiError\n'
                  'class SlackNotifier:\n'
                  '    def send(self):\n'
                  '        if self.bot_token:\n'
                  '            from slack_sdk import WebClient\n'
                  '            client = WebClient(token=self.bot_token)\n'
                  '            client.files_upload_v2(file=path)\n')
    assert SlackArtifactRedactor(original['cells']).notebook(original) == original
    assert not any(isinstance(node, ast.Call) and getattr(node.func, 'attr', '') == 'files_upload_v2'
                   for node in ast.walk(ast.parse(neutralize_cell(code(original)).source)))


def test_non_slack_constructor_is_proven_within_reassignment_branch():
    original = nb('from slack_sdk import WebClient\nCtor = WebClient\n'
                  'if condition:\n    Ctor = dict\n'
                  "    obj = Ctor(token='application-value')\n    obj.update(other)\n")
    assert SlackArtifactRedactor(original['cells']).notebook(original) == original
    assert ast.dump(ast.parse(neutralize_cell(code(original)).source)) == ast.dump(ast.parse(code(original)))


def test_conditional_factory_definitions_cannot_hide_slack():
    original = nb('from slack_sdk.errors import SlackApiError as Ctor\n'
                  'from slack_sdk import WebClient\n'
                  'def choose():\n    return WebClient\n'
                  'if condition:\n    def choose():\n        return dict\n'
                  "Ctor = choose()\nobj = Ctor(token='fixture-value')\nobj.files_upload_v2(file=path)\n")
    with pytest.raises(SlackRedactionError, match='UNRESOLVED Slack constructor identity'):
        SlackArtifactRedactor(original['cells'])


@pytest.mark.parametrize('mutation', [
    'choices[0] = WebClient\n',
    'alias = choices\nalias[0] = WebClient\n',
    'choices.append(WebClient)\n',
])
def test_mutated_constructor_containers_cannot_supply_stale_non_slack_identity(mutation):
    original = nb('from slack_sdk.errors import SlackApiError as Ctor\n'
                  'from slack_sdk import WebClient\nchoices = [dict]\n'
                  + mutation + "Ctor = choices[-1]\nobj = Ctor(token='fixture-value')\n")
    with pytest.raises(SlackRedactionError, match='UNRESOLVED Slack constructor identity'):
        SlackArtifactRedactor(original['cells'])


def test_client_subclass_replaces_exception_alias_without_losing_protection():
    original = nb('from slack_sdk.errors import SlackApiError as Ctor\n'
                  'from slack_sdk import WebClient\nclass Ctor(WebClient):\n    pass\n'
                  "obj = Ctor(token='fixture-value')\nobj.files_upload_v2(file=path)\n")
    assert 'fixture-value' not in code(SlackArtifactRedactor(original['cells']).notebook(original))
    assert not any(isinstance(node, ast.Call) and getattr(node.func, 'attr', '') == 'files_upload_v2'
                   for node in ast.walk(ast.parse(neutralize_cell(code(original)).source)))


def test_conditional_sdk_module_replacement_refuses_instead_of_redacting_application():
    original = nb('import slack_sdk as sdk\nif condition:\n    import internal_http as sdk\n'
                  "obj = sdk.WebClient(token='application-value')\nobj.update(other)\n")
    with pytest.raises(SlackRedactionError, match='UNRESOLVED Slack constructor identity'):
        SlackArtifactRedactor(original['cells'])


@pytest.mark.parametrize('setup, call', [
    ('import slack_sdk.errors as errors\n', 'errors.SlackApiError'),
    ('import slack_sdk as sdk\n', 'sdk.errors.SlackApiError'),
    ('from slack_sdk import errors\n', 'errors.SlackApiError'),
])
def test_qualified_exception_import_has_no_authentication_signature(setup, call):
    original = nb(setup + f"raise {call}(f'application error: {{error}}', response)\n")
    assert SlackArtifactRedactor(original['cells']).notebook(original) == original


def test_class_methods_resolve_free_constructor_names_in_enclosing_scope():
    original = nb('from slack_sdk.errors import SlackApiError as Ctor\n'
                  'from slack_sdk import WebClient\nCtor = WebClient\n'
                  'class Application:\n    Ctor = dict\n'
                  '    def use(self):\n'
                  "        obj = Ctor(token='fixture-value')\n"
                  '        obj.files_upload_v2(file=path)\n')
    assert 'fixture-value' not in code(SlackArtifactRedactor(original['cells']).notebook(original))
    assert not any(isinstance(node, ast.Call) and getattr(node.func, 'attr', '') == 'files_upload_v2'
                   for node in ast.walk(ast.parse(neutralize_cell(code(original)).source)))


@pytest.mark.parametrize('actual', ['WebClient', 'dict'])
@pytest.mark.parametrize('invocation', ['use(*[{actual}])', "use(**{{'Ctor': {actual}}})"])
def test_constructor_parameter_expansion_preserves_argument_identity(actual, invocation):
    original = nb('from slack_sdk import WebClient\nCtor = WebClient\n'
                  'def use(Ctor):\n'
                  "    obj = Ctor(token='fixture-value')\n"
                  '    obj.update(other)\n'
                  + invocation.format(actual=actual) + '\n')
    saved = SlackArtifactRedactor(original['cells']).notebook(original)
    if actual == 'dict':
        assert saved == original
    else:
        assert 'fixture-value' not in code(saved)


def test_reassigned_function_cannot_apply_obsolete_constructor_effects():
    original = nb('from slack_sdk.errors import SlackApiError as Ctor\n'
                  'from slack_sdk import WebClient\n'
                  'def initialize():\n    global Ctor\n    Ctor = WebClient\n'
                  'initialize = dict\ninitialize()\n'
                  "raise Ctor(f'application error: {error}', response)\n")
    assert SlackArtifactRedactor(original['cells']).notebook(original) == original


def test_function_alias_keeps_effects_after_original_name_is_replaced():
    original = nb('from slack_sdk.errors import SlackApiError as Ctor\n'
                  'from slack_sdk import WebClient\n'
                  'def initialize():\n    global Ctor\n    Ctor = WebClient\n'
                  'alias = initialize\ninitialize = dict\nalias()\n'
                  "obj = Ctor(token='fixture-value')\nobj.files_upload_v2(file=path)\n")
    assert 'fixture-value' not in code(SlackArtifactRedactor(original['cells']).notebook(original))


@pytest.mark.parametrize('setup, call', [
    ('import slack_sdk.errors as errors\n', 'errors.SlackApiError'),
    ('import slack_sdk as sdk\n', 'sdk.errors.SlackApiError'),
    ('from slack_sdk import errors\n', 'errors.SlackApiError'),
])
def test_commented_exception_keeps_its_module_origin(setup, call):
    original = nb(setup + f"# raise {call}(f'application error: {{error}}', response)\n")
    assert SlackArtifactRedactor(original['cells']).notebook(original) == original
