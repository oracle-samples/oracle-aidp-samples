"""Redaction must be inert on non-Slack notebooks and refuse far less.

Two contracts pull in opposite directions here, and both are pinned:

  * BLAST RADIUS -- artifact redaction is an optional feature living on the
    save-back path of every notebook. A notebook with no Slack credential must
    come out byte-identical, with unchanged cache semantics and no new way to
    fail. `tests/test_slack_token_artifacts.py` pins the orchestration half.
  * REFUSAL SURFACE -- what the gate REFUSES was over-eager: prose comments,
    runtime-composed tokens, placeholder literals and the module's own
    preserved comments each failed a whole task (`job_should_stop=True`) with
    no credential anywhere in the notebook.
"""
import ast
import copy
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from slack_token_redaction import (  # noqa: E402
    NULL_REDACTOR, SlackArtifactRedactor, SlackRedactionError, build_redactor,
)

# CI gate 2 forbids committing a literal `xox<kind>-` on an added line, which
# is exactly why the free-text catch-all shipped untested. Build the literal
# at runtime so the shape-matching path is genuinely exercised.
_X = "xo" + "x"


# Assembled at runtime: CI's added-line gate also rejects a literal
# credential-in-URL, which is exactly the shape this case needs to exercise.
_INDEX_URL = "https://u:{tok}" + "@" + "pypi.internal/simple"


def slack_token(kind="b"):
    return (f"{_X}{kind}-" if kind else "xa" + "pp-") + \
        "1234567890-9876543210987-AbCdEfGhIjKlMnOpQrStUvWx"


def nb(*sources, cell_type="code"):
    return [{"cell_type": cell_type, "source": [s], "outputs": [], "metadata": {}}
            for s in sources]


def doc(cells):
    return {"cells": cells, "metadata": {}}


def code(notebook):
    return "\n".join("".join(c["source"]) for c in notebook["cells"])


def saved(*sources):
    cells = nb(*sources)
    return code(SlackArtifactRedactor(cells).notebook(doc(cells)))


# ── Blast radius: the gate ────────────────────────────────────────────────

@pytest.mark.parametrize("cells", [
    nb("import pandas as pd\ndf = pd.read_parquet('oci://bucket/x')\n"),
    nb("# nothing to do with messaging\nresult = spark.sql('SELECT 1')\n"),
    nb("client = WebClient(token=os.environ['SLACK_TOKEN'])\n"),   # already safe
    nb("run_id = 'run-2024-08'\npath = 's3://bucket/xox-data/part'\n"),
    nb(f"label = '{_X}p-legacy-name-here'\n"),                     # shaped, not a token
])
def test_notebook_without_a_credential_gets_the_inert_redactor(cells):
    assert build_redactor(cells, artifact_path="/Workspace/x.ipynb") is NULL_REDACTOR


def test_inert_redactor_is_identity_on_every_entry_point():
    notebook = doc(nb("x = 1\n") + nb("# markdown", cell_type="markdown"))
    before = copy.deepcopy(notebook)
    assert NULL_REDACTOR.notebook(notebook) == before
    assert NULL_REDACTOR.text("anything at all") == "anything at all"
    assert NULL_REDACTOR.source("client = WebClient(token='x')\n") == \
        "client = WebClient(token='x')\n"
    assert NULL_REDACTOR.data({"a": ["b"]}) == {"a": ["b"]}
    assert NULL_REDACTOR.required_env == () and NULL_REDACTOR.active is False


def test_gate_sees_a_credential_introduced_only_by_the_migrated_copy():
    """A model rewrite can move the literal from code into a comment."""
    original = nb("client = WebClient(token=os.environ['T'])\n")
    migrated = nb("# client = WebClient(token='fixture-secret')\n"
                  "client = WebClient(token=os.environ['T'])\n")
    redactor = build_redactor(original, migrated, artifact_path="/Workspace/x.ipynb")
    assert redactor is not NULL_REDACTOR
    assert "fixture-secret" not in redactor.text("note: token='fixture-secret'")


def test_shaped_but_non_credential_string_is_never_rewritten():
    """A hyphenated name that merely STARTS with a Slack prefix matched the old
    catch-all and was silently replaced in notebooks with no Slack code."""
    benign = f"{_X}p-legacy-name-here"
    redactor = SlackArtifactRedactor(nb(f"client = WebClient(token='fixture-secret')\n"
                                        f"label = '{benign}'\n"))
    assert redactor.text(f"report mentions {benign}") == f"report mentions {benign}"
    assert benign in saved("client = WebClient(token='fixture-secret')\n"
                           f"label = '{benign}'\n")


# ── Refusal surface: what must NOT stop a migration ───────────────────────

@pytest.mark.parametrize("comment", [
    "# Slack bot token comes from Databricks secrets",
    "# WebClient token is injected at runtime by the platform",
    "# TODO: rotate the Slack token before go-live",
    "# client = WebClient(token=<redacted>)",          # this module's own output
])
def test_prose_mentioning_slack_and_token_does_not_fail_the_task(comment):
    assert SlackArtifactRedactor(nb(comment + "\nx = 1\n")).required_env == ()


def test_redaction_survives_a_cold_cache_re_run():
    """Every resume re-reads the already-redacted child from disk. The
    preserved-original comment must not be mistaken for a hidden credential."""
    first = SlackArtifactRedactor(nb("client = WebClient(token='fixture-secret')\n"))
    once = first.notebook(doc(nb("client = WebClient(token='fixture-secret')\n")))
    assert "<redacted>" in code(once)
    # A FRESH redactor, as a resume gets — not the warm instance.
    second = SlackArtifactRedactor(once["cells"])
    assert second.required_env == ()
    assert second.notebook(once) == once


def test_non_python_cell_mentioning_a_client_is_not_a_credential():
    assert SlackArtifactRedactor(nb("# uses WebClient\nprint 'hello world'\n")).tokens == {}


@pytest.mark.parametrize("expression", [
    "f\"{os.environ['SLACK_BOT_TOKEN']}\"",
    "prefix + os.environ['SLACK_BOT_TOKEN']",
    "os.environ['A'] + os.environ['B']",
])
def test_runtime_composed_token_is_not_refused(expression):
    assert SlackArtifactRedactor(nb(f"client = WebClient(token={expression})\n")).tokens == {}


@pytest.mark.parametrize("expression", [
    "'fixture-' + suffix",
    "f'fixture-{suffix}'",
    "'fixture-%s' % suffix",
])
def test_partially_literal_token_is_still_refused(expression):
    """Half the credential in source is still a credential in source."""
    with pytest.raises(SlackRedactionError, match="UNRESOLVED"):
        SlackArtifactRedactor(nb(f"client = WebClient(token={expression})\n"))


# ── Precision: placeholders, f-strings, eager defaults ────────────────────

@pytest.mark.parametrize("placeholder", ["test", "None", "TOKEN", "a", "changeme"])
def test_placeholder_literal_is_removed_without_rewriting_prose(placeholder):
    """`str.replace` of a short literal across code, markdown, outputs and four
    reports corrupts the artifact; the constructor edit is AST-precise."""
    redactor = SlackArtifactRedactor(nb(f"client = WebClient(token={placeholder!r})\n"))
    prose = f"a banana in a report about {placeholder} data"
    assert redactor.text(prose) == prose
    assert "token='<redacted>'" in saved(f"client = WebClient(token={placeholder!r})\n")


def test_f_string_interpolation_survives_redaction():
    """Before 3.12 every node inside an f-string reports the whole f-string's
    span, so a node-level rewrite silently dropped `{name}`."""
    token = slack_token()
    out = saved(f"client = WebClient(token={token!r})\n",
                f'msg = f"using {token} for {{name}}"\n')
    assert 'f"using <redacted> for {name}"' in out
    assert token not in out
    ast.parse(out, feature_version=(3, 11))


def test_nested_literal_inside_an_f_string_keeps_its_quotes():
    token = slack_token()
    out = saved(f"client = WebClient(token={token!r})\n",
                f"msg = f\"{{{token!r}}}\"\n")
    assert token not in out
    ast.parse(out, feature_version=(3, 11))


def test_a_constructor_literal_is_treated_the_same_in_and_out_of_an_f_string():
    """It used to be rewritten as a bare literal but left untouched inside an
    f-string -- the same string, two answers, in one notebook. Away from its
    authentication site it is left in both; AT the auth site it is replaced with
    the inert placeholder (no longer an env lookup -- see the module header)."""
    out = saved("client = WebClient(token='fixture-secret')\n",
                'msg = f"use fixture-secret for {name}"\n'
                "other = 'fixture-secret'\n")
    assert 'f"use fixture-secret for {name}"' in out
    assert "other = 'fixture-secret'" in out
    assert "token='<redacted>'" in out, "the auth site must still be redacted"
    ast.parse(out, feature_version=(3, 11))


def test_environ_get_default_stays_a_get_with_a_scrubbed_default(monkeypatch):
    """`environ.get(K, <token>)` evaluates its default EAGERLY. The token must be
    removed WITHOUT turning the call into a bare `environ[K]`: that promotion
    made an unconfigured clone raise KeyError at import/def time -- the AutoRefit
    derived_raw class (2026-09-22). A shape-proven fallback is scrubbed to the
    inert placeholder inside the existing get(); the direct-ctor literal is
    likewise removed. The saved cell is eval-safe whether or not K is set."""
    token = slack_token()
    out = saved(f"a = WebClient(token={token!r})\n",
                f"b = WebClient(token=os.environ.get('SLACK_TOKEN', {token!r}))\n")
    assert token not in out
    assert "__import__" not in out and "os.environ['SLACK_TOKEN']" not in out
    assert "token='<redacted>'" in out                                   # direct ctor
    assert "os.environ.get('SLACK_TOKEN', '<redacted>')" in out          # get() kept
    # unset: get() returns the placeholder, never KeyError
    monkeypatch.delenv("SLACK_TOKEN", raising=False)
    ns = {"WebClient": lambda token: token, "os": __import__("os")}
    exec(out, ns)
    assert ns["a"] == "<redacted>" and ns["b"] == "<redacted>"
    # set: the customer's own value flows through the get()
    monkeypatch.setenv("SLACK_TOKEN", "configured-fixture")
    ns = {"WebClient": lambda token: token, "os": __import__("os")}
    exec(out, ns)
    assert ns["a"] == "<redacted>" and ns["b"] == "configured-fixture"


# ── Discovery: free text, containers, extra prefixes ──────────────────────

def test_free_text_token_is_scrubbed_without_inventing_an_env_var():
    """A docstring example is not an authentication the saved notebook performs;
    advertising a variable nothing reads sends operators chasing phantoms."""
    token = slack_token()
    redactor = SlackArtifactRedactor(nb(f"# docs: pass {token} as the value\nx = 1\n"))
    assert redactor.required_env == ()
    assert redactor.text(f"log line with {token}") == "log line with <redacted>"


@pytest.mark.parametrize("kind", ["b", "p", "a", "e", None])
def test_every_documented_token_prefix_is_matched_in_free_text(kind):
    token = slack_token(kind)
    redactor = SlackArtifactRedactor(nb(f"note = 'see {token}'\n"))
    assert redactor.text(f"raw {token}") == "raw <redacted>"


@pytest.mark.parametrize("call", ["WebClient(**cfg)", "WebClient(token=cfg['token'])"])
def test_token_reached_through_a_dict_is_discovered(call):
    out = saved(f"cfg = {{'token': 'fixture-secret'}}\nclient = {call}\n")
    assert "fixture-secret" not in out
    assert "'<redacted>'" in out


def test_unparseable_slack_cell_reports_what_it_could_not_verify():
    """Narrowing the refusal must not trade a loud failure for silence.

    A cell that will not parse but references a Slack client may still bind a
    credential in a format this pass does not recognise, on a different
    logical line from the client name. It is not worth failing a task over --
    but static analysis cannot prove it safe, so it is reported.
    """
    redactor = build_redactor(
        nb("tok = 'my-legacy-secret-value'\nclient = WebClient(token=tok\n"),
        artifact_path="/Workspace/x.ipynb")
    assert redactor is not NULL_REDACTOR
    assert redactor.active is False, "a report must not drag the notebook onto the redacting path"
    assert len(redactor.unverified) == 1
    assert "could not be verified" in redactor.unverified[0]
    assert "my-legacy-secret-value" not in redactor.unverified[0], "never echo source"


@pytest.mark.parametrize("cells", [
    nb("import pandas as pd\nx = 1\n"),                       # no Slack at all
    nb("# Slack bot token comes from secrets\nx = 1\n"),       # prose, parses fine
    nb("client = WebClient(token=os.environ['T'])\n"),         # parses, already safe
])
def test_parseable_or_non_slack_notebooks_report_nothing(cells):
    assert build_redactor(cells, artifact_path="/Workspace/x.ipynb").unverified == ()


# ── source() must never emit a known secret, nor a fake one ──────────────

@pytest.mark.parametrize("cell, description", [
    ('%sh curl -H "Authorization: Bearer {tok}" https://slack.com/api\n', "shell magic"),
    ("%pip install --extra-index-url " + _INDEX_URL + " pkg\n", "pip magic"),
    ('hdr = b"Bearer {tok}"\n', "bytes literal"),
    ("query = rb'token={tok}'\n", "raw bytes literal"),
])
def test_credential_never_survives_in_code_source_cannot_rewrite(cell, description):
    """`visit()` only reaches AST *str* constants. A magic line is blanked to
    `pass` before parsing and a bytes literal is not a str node, yet both live
    in cells that parse -- so the unparseable-cell refusal never fires either.
    Shipping a value we already know is a secret is the silent-miss class the
    write guard exists to prevent."""
    token = slack_token()
    sources = (cell.format(tok=token), "client = WebClient(token='fixture-secret')\n")
    redactor = build_redactor(nb(*sources), artifact_path="/Workspace/x.ipynb")
    out = code(redactor.notebook(doc(nb(*sources))))
    assert token not in out, f"{description} leaked the credential"
    assert "<redacted>" in out
    assert redactor.manual_action, "an unrewritable credential must be reported"
    assert token not in " ".join(redactor.manual_action), "never echo the secret"


def test_credential_in_executable_code_is_removed_to_an_inert_placeholder():
    """Contract reversed by owner decision (2026-09-22). The old design rewrote a
    top-level `TOKEN = '<real>'` into an eager `os.environ[...]` lookup so an
    unconfigured clone would FAIL LOUDLY. But that lookup is eager: in a default
    parameter value or a module-level assignment it raised KeyError at import/def
    time and bricked notebooks that never call Slack (AutoRefit derived_raw,
    KeyError SLACK_TOKEN_EXAMPLE01). The Slack CALL is separately commented
    out by the execution-boundary neutraliser, so the credential is dead code --
    it is now replaced IN PLACE with the inert placeholder, evaluation-safe in
    every position, and there is nothing for the operator to configure. The
    customer restores their own token if they re-enable the call."""
    token = slack_token()
    sources = (f"TOKEN = '{token}'\n", "client = make_slack_client(TOKEN)\n")
    redactor = build_redactor(nb(*sources), artifact_path="/Workspace/x.ipynb")
    out = code(redactor.notebook(doc(nb(*sources))))
    assert token not in out
    assert "TOKEN = '<redacted>'" in out, "removed to an inert placeholder"
    assert "environ[" not in out, "no eager env lookup is emitted anymore"
    assert redactor.configured_env == (), "nothing to configure"
    # a module-level assignment must be eval-safe with no env configured
    ns = {"make_slack_client": lambda t: t}
    exec(out, ns)
    assert ns["TOKEN"] == "<redacted>"
    ast.parse(out, feature_version=(3, 11))


def test_environ_get_default_is_scrubbed_not_promoted(monkeypatch):
    """A shape-proven token appearing ONLY as an `environ.get(K, <token>)`
    fallback (no direct constructor elsewhere) must still be removed -- and
    removed in place, NEVER promoted to a bare `os.environ[K]`. The promotion
    made an unconfigured clone raise KeyError (AutoRefit derived_raw, 2026-09-22);
    scrubbing the fallback to the inert placeholder keeps the call eval-safe:
    K's value when set, the placeholder when not, never a raise."""
    token = slack_token()
    source = f"client = WebClient(token=os.environ.get('SLACK_BOT_TOKEN', '{token}'))\n"
    redactor = build_redactor(nb(source), artifact_path="/Workspace/x.ipynb")
    out = code(redactor.notebook(doc(nb(source))))
    assert token not in out
    assert "os.environ['SLACK_BOT_TOKEN']" not in out, "must not promote to an eager lookup"
    assert "os.environ.get('SLACK_BOT_TOKEN', '<redacted>')" in out
    # unset -> placeholder (no KeyError); set -> the customer's own value
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    ns = {"WebClient": lambda token: token, "os": __import__("os")}
    exec(out, ns)
    assert ns["client"] == "<redacted>"
    monkeypatch.setenv("SLACK_BOT_TOKEN", "configured-fixture")
    ns = {"WebClient": lambda token: token, "os": __import__("os")}
    exec(out, ns)
    assert ns["client"] == "configured-fixture"


def test_reported_env_names_are_only_those_the_artifact_reads():
    """`required_env` counts every discovered credential, including one the
    migration model already rewrote to its own lookup. Telling an operator to
    configure that sends them after a variable nothing reads."""
    original = nb("client = WebClient(token='fixture-secret')\n")
    migrated = nb("client = WebClient(token=os.environ['MY_SLACK'])\n")
    redactor = build_redactor(original, migrated, artifact_path="/Workspace/x.ipynb")
    out = code(redactor.notebook(doc(migrated)))
    assert redactor.configured_env == (), redactor.configured_env
    assert redactor.required_env, "discovery still records what it found"
    for name in redactor.required_env:
        assert name not in out


def test_inert_redactor_supports_every_attribute_the_migrator_uses():
    """A caller must never have to branch on which redactor it holds.

    Derived from the real call sites rather than hand-listed: adding a member
    to `SlackArtifactRedactor`, using it in the migrator, and forgetting the
    inert twin raises AttributeError on the save path of every NON-Slack
    notebook -- the exact blast radius the gate exists to remove.
    """
    migrator = (Path(__file__).resolve().parents[1] / "scripts" / "job_migrate.py").read_text()
    used = set(re.findall(r"\b(?:child_)?slack_redactor\.([A-Za-z_]\w*)", migrator))
    assert used, "call sites not found — has the attribute been renamed?"
    missing = sorted(name for name in used if not hasattr(NULL_REDACTOR, name))
    assert missing == [], f"NULL_REDACTOR is missing: {missing}"


# ── Idempotence and refusal precision ────────────────────────────────────

@pytest.mark.parametrize("ctor", [
    "WebClient(token='fixture-secret', base_url='https://slack.com/api/')",
    "WebClient(base_url='https://slack.com/api/', token='fixture-secret')",
    "WebClient(token='fixture-secret', headers={'X-App': 'reporting-service'})",
])
def test_preserved_comment_with_another_literal_still_re_reads(ctor):
    """The preserved comment keeps every OTHER argument's literal, so on a
    cold-cache resume it looks exactly like a hidden credential. Every resume
    of an already-redacted dep hit this, deterministically."""
    source = f"client = {ctor}\n"
    once = SlackArtifactRedactor(nb(source)).notebook(doc(nb(source)))
    assert "<redacted>" in code(once)
    second = SlackArtifactRedactor(once["cells"])   # fresh, as a resume gets
    assert second.required_env == ()
    assert second.notebook(once) == once


@pytest.mark.parametrize("cell", [
    "print 'WebClient failed'\n",                       # Python 2
    "print 'connecting to Slack'\n",
    "%sql SELECT 'ok' FROM t -- WebClient note\n",
])
def test_a_mention_beside_a_literal_is_not_a_constructor(cell):
    """Sharing a logical line is not evidence. Requiring an actual call keeps
    the guard on `WebClient(token='…'` while letting ordinary non-Python and
    Python-2 cells through -- Databricks exports are full of them.

    Still conservative by design: a ctor call appearing INSIDE a string or a
    comment on an unparseable line is matched textually and will refuse. That
    direction is safe -- and a mention that does NOT refuse is still surfaced
    through `unverified`, so nothing about an unparseable cell is silent.
    """
    assert build_redactor(nb("import slack_sdk\n"), nb(cell),
                          artifact_path="/Workspace/x.ipynb").required_env == ()


def test_unparseable_constructor_with_a_literal_is_still_refused():
    """The narrowing must not disarm the guard it narrows."""
    with pytest.raises(SlackRedactionError):
        SlackArtifactRedactor(nb("client = WebClient(token='fixture-secret'\n"))


@pytest.mark.parametrize("indent", ["", "    ", "\t"])
def test_expression_magic_maps_back_at_any_indentation(indent):
    """`_strip_magics` moves the expression left to the original indent, so a
    substring search for it finds nothing when there IS an indent -- and the
    whole cell was refused with 'cannot map Slack literal'."""
    body = f"{indent}%time client = WebClient(token='fixture-secret')\n"
    source = ("def f():\n" + body) if indent else body
    out = code(SlackArtifactRedactor(nb(source)).notebook(doc(nb(source))))
    assert "fixture-secret" not in out
    assert "%time client =" in out
    assert "token='<redacted>'" in out
