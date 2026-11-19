"""Tests for scripts/slack_ast_neutralize.py.

Two contracts, both regressions from real incidents:

  FALSE NEGATIVES — the 2026-09-03 post. `files_upload_v2` was not in the
  regex allowlist, so it was never commented at any indentation. Measured on
  the live code: files_upload_v2 -> commented=0.

  FALSE POSITIVES — fixes 7o' and 7o'' exist because tier-2 `.send(` matched
  Kafka / socket / SMTP. That protection was hard-won and must not regress.
  Measured on the live code: pure Kafka, mixed slack+Kafka, sock.send and
  smtp.send are all correctly left alone — EXCEPT `webhook_producer.send()`
  on a Kafka producer, which the name-hint fallback wrongly comments.
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from slack_ast_neutralize import (  # noqa: E402
    neutralize_cell, new_bindings, update_bindings, _strip_magics,
)


def _run(cell: str, bindings=None, include_generic: bool = False):
    """Drive one cell the way job_migrate.py does.

    ORDER MATTERS and is part of the contract: neutralise FIRST, then fold the
    cell into the notebook-wide bindings. See
    `test_wiring_order_is_neutralise_then_update`.
    """
    b = bindings if bindings is not None else new_bindings()
    res = neutralize_cell(cell, b, include_generic=include_generic)
    update_bindings(cell, b)
    return res


def _live_calls(source: str, methods):
    """Uncommented calls remaining, via AST — comments are invisible to it."""
    tree = ast.parse(source)
    return [n.func.attr for n in ast.walk(tree)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
            and n.func.attr in methods]


# ─── False negatives: the incident ───────────────────────────────────────────
ALL_METHODS = [
    "files_upload", "files_upload_v2", "chat_postMessage", "chat_update",
    "chat_delete", "chat_postEphemeral", "chat_scheduleMessage",
    "views_open", "views_publish", "files_remote_add", "api_call",
    "conversations_create", "reactions_add",
]

NESTINGS = {
    "module":  "{call}\n",
    "try":     "try:\n    {call}\nexcept Exception:\n    pass\n",
    "if":      "if flag:\n    {call}\n",
    "def":     "def post():\n    {call}\n",
    "def_for": "def post():\n    for ch in chans:\n        {call}\n    return 1\n",
    "while":   "while go:\n    {call}\n    break\n",
    "with":    "with open('f') as fh:\n    {call}\n",
}


@pytest.mark.parametrize("method", ALL_METHODS)
@pytest.mark.parametrize("nesting", sorted(NESTINGS))
def test_no_slack_call_survives_at_any_nesting(method, nesting):
    """Every method, every nesting depth. The regex path covered 1 of 13."""
    call = f"client.{method}(channel='C1', text='t')"
    cell = ("from slack_sdk import WebClient\n"
            "client = WebClient(token=tok)\n" + NESTINGS[nesting].format(call=call))

    res = _run(cell)

    assert res.commented > 0, f"{method} @ {nesting} was NOT neutralised"
    assert not _live_calls(res.source, {method}), \
        f"{method} still live after neutralisation"
    ast.parse(res.source)                      # must stay valid Python


def test_files_upload_v2_in_try_the_exact_incident():
    """Verbatim shape from Monitoring_and_Posting cell 42."""
    cell = (
        "from slack_sdk import WebClient\n"
        "from slack_sdk.errors import SlackApiError\n"
        "client = WebClient(token=slack_webclient_token)\n"
        "try:\n"
        "    result = client.files_upload_v2(\n"
        "        channel=channel_id,\n"
        "        initial_comment=initial_comment,\n"
        "    )\n"
        "except SlackApiError as e:\n"
        "    logger.error(e)\n"
    )
    res = _run(cell)

    assert not _live_calls(res.source, {"files_upload_v2"})
    assert "except SlackApiError" in res.source     # handler preserved
    assert "# result = client.files_upload_v2(" in res.source  # original kept
    ast.parse(res.source)


def test_client_bound_in_an_earlier_cell():
    """c41 binds the client, c42 calls it — per-cell analysis cannot see this."""
    b = new_bindings()
    c41 = "from slack_sdk import WebClient\nclient = WebClient(token=t)\n"
    c42 = "result = client.files_upload_v2(channel='C1', file=f)\n"

    update_bindings(c41, b)
    update_bindings(c42, b)
    res = neutralize_cell(c42, b)

    assert res.commented > 0
    assert not _live_calls(res.source, {"files_upload_v2"})


def test_inline_construction():
    cell = "WebClient(token=t).chat_postMessage(channel='C1', text='x')\n"
    res = _run(cell)
    assert res.commented > 0


# ─── False positives: must NOT regress ───────────────────────────────────────
def test_pure_kafka_untouched():
    cell = "producer = KafkaProducer(bootstrap_servers=b)\nproducer.send(topic, msg)\n"
    res = _run(cell)
    assert res.commented == 0
    assert res.source == cell


def test_mixed_slack_import_and_kafka():
    """7o'' Defect 3: slack import gives the cell 'evidence'; Kafka must survive."""
    cell = ("from slack_sdk import WebClient\n"
            "producer = KafkaProducer(b)\n"
            "producer.send(topic, msg)\n")
    res = _run(cell)
    assert res.commented == 0


def test_kafka_and_a_real_slack_post_together():
    cell = ("from slack_sdk import WebClient\n"
            "c = WebClient(token=t)\n"
            "producer = KafkaProducer(b)\n"
            "producer.send(topic, m)\n"
            "c.chat_postMessage(channel='C1', text='x')\n")
    res = _run(cell)

    assert res.commented > 0
    assert "producer.send(topic, m)" in res.source          # untouched
    assert not res.source.count("# producer.send")          # not commented
    assert not _live_calls(res.source, {"chat_postMessage"})


@pytest.mark.parametrize("recv", ["sock", "smtp", "conn", "session"])
def test_generic_send_receivers_untouched(recv):
    cell = f"from slack_sdk import WebClient\n{recv}.send(payload)\n"
    res = _run(cell)
    assert res.commented == 0


def test_kafka_named_webhook_is_untouched():
    """The current implementation gets this WRONG (measured commented=1).

    `_SLACK_RECEIVER_HINT_RE` treats any receiver containing 'webhook' as
    Slack. Binding must beat naming.
    """
    cell = ("from slack_sdk import WebClient\n"
            "webhook_producer = KafkaProducer(b)\n"
            "webhook_producer.send(topic, msg)\n")
    res = _run(cell)
    assert res.commented == 0, "a Kafka producer named *webhook* must not be commented"


def test_rebinding_to_non_slack_releases_the_name():
    """Rebinding marks the name KNOWN-NON-SLACK, which is not the same as
    forgetting it.

    The binding state is deliberately three-way — True / False / absent.
    `False` (assigned something with no Slack evidence) blocks the
    receiver-name hint tier, which is what keeps
    `webhook_producer = KafkaProducer(...)` from being commented. `absent`
    (never assigned anywhere visible) still allows that tier, which is how a
    client arriving from a %run child is caught. Collapsing the two into a
    plain set is what dropped the `slack_client = get_slack_client()` class of
    coverage.
    """
    b = new_bindings()
    update_bindings("client = WebClient(token=t)\n", b)
    assert b.get("client") is True
    update_bindings("client = KafkaProducer(b)\n", b)
    assert b.get("client") is False, "rebind must record known-non-Slack"
    assert "never_seen" not in b, "an unassigned name stays UNKNOWN"

    # The behaviour is what matters: the Kafka send stays live.
    res = neutralize_cell("client.send(topic, msg)\n", b)
    assert res.commented == 0


# ─── Structural bugs the regex path had ──────────────────────────────────────
def test_sole_statement_in_block_gets_pass():
    """Regex path produced an empty `if`, then Opus patched `pass` afterwards."""
    cell = ("client = WebClient(token=t)\n"
            "if original_message_ts:\n"
            "    client.chat_postMessage(channel='C1', text='x')\n")
    res = _run(cell)

    ast.parse(res.source)                     # would be IndentationError before
    assert "pass" in res.source


def test_multiline_call_commented_as_a_whole():
    cell = ("client = WebClient(token=t)\n"
            "client.chat_postMessage(\n"
            "    channel='C1',\n"
            "    text='hello',\n"
            ")\n")
    res = _run(cell)
    ast.parse(res.source)                     # dangling args would break this
    assert res.commented >= 4


def test_match_inside_a_comment_is_ignored():
    """`# replaced slack_sdk with SNS` previously triggered evidence."""
    cell = ("producer = KafkaProducer(b)\n"
            "producer.send(topic, msg)  # replaced slack_sdk chat_postMessage with SNS\n")
    res = _run(cell)
    assert res.commented == 0


def test_slack_id_lookalike_string_is_ignored():
    """`channel="CAMPAIGNS"` needed a digit-count hack in the regex path."""
    cell = ("cfg = {'channel': 'CAMPAIGNS'}\n"
            "tracker.record(channel='COFFEE_ORDER')\n")
    res = _run(cell)
    assert res.commented == 0


def test_idempotent_across_reruns():
    cell = ("client = WebClient(token=t)\n"
            "client.files_upload_v2(channel='C1', file=f)\n")
    first = _run(cell)
    second = neutralize_cell(first.source, {"client"})

    assert second.commented == 0, "second pass must not re-comment"
    assert first.source.count("neutralised") == second.source.count("neutralised")
    assert "# # " not in second.source


def test_imports_and_defs_are_preserved():
    """AutoRefit cascade: an earlier revision deleted a load-bearing import."""
    cell = ("from HelperFunctions.constants import *\n"
            "import json\n"
            "from slack_sdk import WebClient\n"
            "client = WebClient(token=t)\n"
            "client.chat_postMessage(channel='C1', text='x')\n"
            "APP = APP_SOURCE_NAME\n")
    res = _run(cell)

    assert "from HelperFunctions.constants import *" in res.source
    assert "# from HelperFunctions" not in res.source
    assert "APP = APP_SOURCE_NAME" in res.source
    assert "client = WebClient(token=t)" in res.source     # ctor is inert


# ─── Magics and parse failures ───────────────────────────────────────────────
def test_cell_with_magics_still_analysed():
    """`%matplotlib inline` / `!pip` are not Python; ast.parse would raise.

    A cell that fails to parse goes unanalysed, which is exactly how a post
    slips through.
    """
    cell = ("%matplotlib inline\n"
            "client = WebClient(token=t)\n"
            "!echo hi\n"
            "client.files_upload_v2(channel='C1', file=f)\n")
    res = _run(cell)

    assert res.parse_error is None, f"should have parsed: {res.parse_error}"
    assert res.commented > 0
    assert "%matplotlib inline" in res.source        # magic left alone
    assert "!echo hi" in res.source


def test_unparseable_cell_reports_instead_of_silently_skipping():
    cell = "def broken(:\n    pass\n"
    res = neutralize_cell(cell, new_bindings())

    assert res.parse_error is not None
    assert res.source == cell            # unchanged, caller falls back
    assert res.commented == 0


# ─── Reporting the unprovable ────────────────────────────────────────────────
def test_dynamic_dispatch_is_reported():
    cell = ("client = WebClient(token=t)\n"
            "getattr(client, method_name)(channel='C1')\n")
    res = _run(cell)
    kinds = [k for k, _, _ in res.unprovable]
    assert "dynamic-dispatch" in kinds


def test_decorator_is_reported():
    cell = ("from knockknock import slack_sender\n"
            "@slack_sender(webhook_url=u, channel='#a')\n"
            "def train():\n"
            "    return 1\n")
    res = _run(cell)
    kinds = [k for k, _, _ in res.unprovable]
    assert "decorator" in kinds


def test_indirect_receiver_is_reported():
    """Must exercise the REPORT gate, not tier-1.

    The previous version used `chat_postMessage`, which tier-1 matches by text,
    so `res.commented > 0` satisfied the assertion and the report gate was
    never tested at all. It passed while the gate silently dropped real leaks
    (`bot.client.files_upload_v2(...)` reported nothing). Use a method tier-1
    does NOT cover so the only way to pass is a genuine finding.
    """
    cell = ("from slack_sdk import WebClient\n"
            "clients = {'a': WebClient(token=t)}\n"
            "clients['a'].files_upload_v2(channel='C1', file=f)\n")
    res = _run(cell, include_generic=True)
    kinds = [k for k, _, _ in res.unprovable]
    assert res.commented == 0, "tier-1 must not cover this — otherwise tautological"
    assert "indirect-receiver" in kinds


# ─── Tier-1 supplements ──────────────────────────────────────────────────────
def test_customer_wrapper_call_still_caught():
    cell = "send_data_to_slack(msg='done')\n"
    res = _run(cell)
    assert res.commented > 0


def test_raw_webhook_url_still_caught():
    cell = "requests.post('https://hooks.slack.com/services/T/B/x', json=p)\n"
    res = _run(cell)
    assert res.commented > 0


# ─── Reads must keep working ─────────────────────────────────────────────────
# Neutralisation stops side effects. A read has none, and the regex path never
# touched reads — commenting one would be a NEW regression:
#     resp = client.conversations_history(...)   # if commented ->
#     msgs = resp['messages']                    # NameError
READ_METHODS = [
    "conversations_history", "conversations_list", "conversations_info",
    "conversations_replies", "conversations_members",
    "conversations_listConnectInvites",
    "users_list", "users_info", "users_lookupByEmail", "users_getPresence",
    "users_conversations", "users_profile_get",
    "files_list", "files_info", "team_info", "emoji_list", "pins_list",
    "reactions_list", "reactions_get", "usergroups_list", "search_messages",
    "api_test", "auth_test", "chat_getPermalink", "team_accessLogs",
    "team_billableInfo", "admin_conversations_restrictAccess_listGroups",
    "bots_info", "dnd_info",
]


@pytest.mark.parametrize("method", READ_METHODS)
def test_read_methods_are_left_running(method):
    cell = ("from slack_sdk import WebClient\n"
            "client = WebClient(token=tok)\n"
            f"resp = client.{method}(channel='C1')\n"
            "n = len(resp)\n")
    res = _run(cell)

    assert res.commented == 0, f"{method} is a read and must not be commented"
    assert _live_calls(res.source, {method}), f"{method} must stay live"
    assert res.source == cell
    ast.parse(res.source)


def test_read_is_reported_as_left_running():
    cell = ("client = WebClient(token=t)\n"
            "resp = client.conversations_history(channel='C1')\n")
    res = _run(cell)
    kinds = [k for k, _, _ in res.unprovable]
    assert "read-left-running" in kinds


def test_read_and_write_in_the_same_cell():
    """The read survives, the write is neutralised."""
    cell = ("client = WebClient(token=t)\n"
            "hist = client.conversations_history(channel='C1')\n"
            "client.chat_postMessage(channel='C1', text=str(len(hist)))\n")
    res = _run(cell)

    assert _live_calls(res.source, {"conversations_history"}), "read must survive"
    assert not _live_calls(res.source, {"chat_postMessage"}), "write must go"
    ast.parse(res.source)


@pytest.mark.parametrize("method", [
    "files_upload_v2", "chat_postMessage", "chat_update", "chat_delete",
    "files_upload", "views_open", "views_publish", "conversations_create",
    "conversations_invite", "conversations_setTopic", "users_setPhoto",
    "users_profile_set", "reactions_add", "pins_add", "chat_scheduleMessage",
    "admin_conversations_restrictAccess_addGroup",
    "files_completeUploadExternal", "api_call",
])
def test_write_methods_are_still_neutralised(method):
    """Read exemption must not let a write through — that is the incident."""
    cell = ("client = WebClient(token=t)\n"
            f"client.{method}(channel='C1')\n")
    res = _run(cell)
    assert res.commented > 0, f"{method} is a WRITE and must be neutralised"
    assert not _live_calls(res.source, {method})


def test_unclassified_method_is_commented_and_reported():
    """Fail safe: unknown verb -> comment (not leak) and surface it."""
    cell = ("client = WebClient(token=t)\n"
            "client.files_frobnicate(channel='C1')\n")
    res = _run(cell)

    assert res.commented > 0, "unknown methods must default to WRITE"
    kinds = [k for k, _, _ in res.unprovable]
    assert "unclassified-method" in kinds


# ─── Regressions found by probing before wiring ──────────────────────────────
def test_post_whose_text_contains_the_word_neutralised():
    """Idempotency must key on the ANNOTATION, not the bare word.

    Found while probing: matching the substring 'neutralised' anywhere in the
    statement skipped a real post whose message text happened to contain it
    (measured: old commented=1, new commented=0 — a leak I introduced).
    """
    cell = ("client = WebClient(token=t)\n"
            "client.chat_postMessage(channel='C1', text='neutralised run')\n")
    res = _run(cell)

    assert res.commented > 0, "a real post must not be skipped by a word match"
    assert not _live_calls(res.source, {"chat_postMessage"})


@pytest.mark.parametrize("header,body", [
    ("while client.chat_postMessage(channel='C1', text='x'):", "    break"),
    ("if client.chat_delete(channel='C1', ts=x):", "    print('gone')"),
    ("with client.files_upload_v2(channel='C1', file=f) as r:", "    pass"),
    ("for x in client.chat_scheduleMessage(channel='C1'):", "    use(x)"),
])
def test_write_in_a_control_flow_header_is_neutralised(header, body):
    """A header call cannot be commented alone without orphaning the block, so
    the whole container goes. Leaving it live would be a leak."""
    cell = f"client = WebClient(token=t)\n{header}\n{body}\n"
    res = _run(cell)

    assert res.commented > 0, f"write in header not neutralised: {header}"
    ast.parse(res.source)
    kinds = [k for k, _, _ in res.unprovable]
    assert "container-header-write" in kinds, "must be reported for review"


def test_read_in_a_for_header_survives():
    """`for m in client.conversations_history(...)` is normal code."""
    cell = ("client = WebClient(token=t)\n"
            "for m in client.conversations_history(channel='C1')['messages']:\n"
            "    use(m)\n")
    res = _run(cell)

    assert res.commented == 0
    assert res.source == cell


def test_def_returning_a_post_is_neutralised():
    """The regex path skipped def bodies entirely, so this posted."""
    cell = ("client = WebClient(token=t)\n"
            "def notify(m):\n"
            "    return client.chat_postMessage(channel='C1', text=m)\n"
            "r = notify('hi')\n")
    res = _run(cell)

    assert res.commented > 0
    assert not _live_calls(res.source, {"chat_postMessage"})
    ast.parse(res.source)


def test_docstring_containing_a_magic_does_not_break_analysis():
    cell = ('client = WebClient(token=t)\n'
            'def f():\n'
            '    """\n'
            '    !pip install foo\n'
            '    """\n'
            "    client.chat_postMessage(channel='C1', text='x')\n")
    res = _run(cell)
    assert res.commented > 0 or res.parse_error is not None
    if res.parse_error is None:
        ast.parse(res.source)


# ─── Second probe round: shapes found before wiring ──────────────────────────
def test_two_statements_on_one_line_commented_once():
    """Shared line span must not double-prefix ('# # ') or duplicate the note."""
    cell = ("client = WebClient(token=t)\n"
            "client.chat_postMessage(channel='C1', text='a'); "
            "client.chat_update(channel='C1', text='b')\n")
    res = _run(cell)

    assert "# # " not in res.source
    assert res.source.count("Oracle tool modification") == 1
    assert not _live_calls(res.source, {"chat_postMessage", "chat_update"})
    ast.parse(res.source)


def test_shared_line_with_unrelated_code_is_reported():
    """`x = 1; post(...)` loses `x = 1`. Same as the regex path, but surfaced."""
    cell = ("client = WebClient(token=t)\n"
            "x = 1; client.chat_postMessage(channel='C1', text='a')\n")
    res = _run(cell)

    assert "shared-line" in [k for k, _, _ in res.unprovable]


def test_crlf_line_endings_preserved():
    """splitlines() drops endings; joining with \\n would rewrite the whole cell."""
    cell = ("client = WebClient(token=t)\r\n"
            "client.chat_postMessage(channel='C1', text='a')\r\n")
    res = _run(cell)

    assert "\r\n" in res.source, "CRLF must survive a one-line edit"
    assert not _live_calls(res.source.replace("\r\n", "\n"), {"chat_postMessage"})


def test_with_as_binding_is_tracked():
    """`with WebClient(...) as client:` is not an Assign — needs its own pass."""
    cell = ("from slack_sdk import WebClient\n"
            "with WebClient(token=t) as client:\n"
            "    client.files_upload_v2(channel='C1', file=f)\n")
    res = _run(cell)

    assert res.commented > 0
    assert not _live_calls(res.source, {"files_upload_v2"})
    ast.parse(res.source)


def test_walrus_in_header():
    cell = ("client = WebClient(token=t)\n"
            "if (r := client.chat_postMessage(channel='C1', text='x')):\n"
            "    pass\n")
    res = _run(cell)
    assert not _live_calls(res.source, {"chat_postMessage"})
    ast.parse(res.source)


def test_async_await_client():
    cell = ("from slack_sdk import AsyncWebClient\n"
            "client = AsyncWebClient(token=t)\n"
            "async def f():\n"
            "    await client.chat_postMessage(channel='C1', text='x')\n")
    res = _run(cell)
    assert res.commented > 0
    assert not _live_calls(res.source, {"chat_postMessage"})
    ast.parse(res.source)


@pytest.mark.parametrize("shape", [
    "try:\n    go()\nexcept Exception:\n    {call}\n",
    "for i in r:\n    pass\nelse:\n    {call}\n",
    "try:\n    go()\nfinally:\n    {call}\n",
])
def test_sole_statement_in_handler_else_finally(shape):
    cell = ("client = WebClient(token=t)\n"
            + shape.format(call="client.chat_postMessage(channel='C1', text='x')"))
    res = _run(cell)
    assert res.commented > 0
    ast.parse(res.source)               # `pass` must be inserted where needed
    assert not _live_calls(res.source, {"chat_postMessage"})


def test_annotated_binding_is_tracked():
    cell = ("from slack_sdk import WebClient\n"
            "client: WebClient = WebClient(token=t)\n"
            "client.files_upload_v2(channel='C1', file=f)\n")
    res = _run(cell)
    assert res.commented > 0


def test_deeply_nested_write():
    cell = ("client = WebClient(token=t)\n"
            "def f():\n"
            "    for c in cs:\n"
            "        try:\n"
            "            client.chat_postMessage(channel=c, text='x')\n"
            "        except Exception:\n"
            "            pass\n")
    res = _run(cell)
    assert res.commented > 0
    assert not _live_calls(res.source, {"chat_postMessage"})
    ast.parse(res.source)


# ═══════════════════════════════════════════════════════════════════════════
# PR #82 review round: every case below was measured FAILING before the fix.
# Base numbers are the merge-base regex path, measured with the same harness.
# ═══════════════════════════════════════════════════════════════════════════

# ─── A1: bindings must evolve in STATEMENT ORDER ─────────────────────────────
def test_same_cell_rebind_after_the_post_does_not_unbind_it():
    """`client = None` cleanup after the post. base=0, pre-fix head=0."""
    cell = ("client = WebClient(token=t)\n"
            "client.files_upload_v2(channel='C1', file=f)\n"
            "client = None\n")
    res = _run(cell)
    assert res.commented > 0
    assert not _live_calls(res.source, {"files_upload_v2"})


def test_cross_cell_rebind_after_the_post():
    """Bind in cell 41, post + clean up in cell 42. base=0, pre-fix head=0."""
    b = new_bindings()
    _run("from slack_sdk import WebClient\nclient = WebClient(token=t)\n", b)
    res = _run("client.files_upload_v2(file=f)\nclient = None\n", b)
    assert res.commented > 0
    assert not _live_calls(res.source, {"files_upload_v2"})


def test_wiring_order_is_neutralise_then_update():
    """Pins the job_migrate.py call order.

    `update_bindings` BEFORE `neutralize_cell` collapses the cell to its END
    state, so a post followed by a cleanup rebind is judged against the
    rebound value and leaks. This is the order the integration relies on.
    """
    cell = "client.files_upload_v2(file=f)\nclient = None\n"

    good = new_bindings()
    update_bindings("client = WebClient(token=t)\n", good)
    assert neutralize_cell(cell, good).commented > 0, "production order leaks"

    bad = new_bindings()
    update_bindings("client = WebClient(token=t)\n", bad)
    update_bindings(cell, bad)                      # the WRONG order
    assert neutralize_cell(cell, bad).commented == 0, (
        "if this now passes, the ordering foot-gun is gone and this test "
        "can be deleted — until then it documents why the call order matters")


def test_try_guarded_construction_survives_the_except_rebind():
    """Found by the review matrix, not by either reviewer. base=0, pre-fix 0.

    A guarded construction is normal code. Letting the `except` branch's
    rebind erase the binding makes every post below it leak.
    """
    cell = ("from slack_sdk import WebClient\n"
            "try:\n"
            "    client = WebClient(token=t)\n"
            "except Exception:\n"
            "    client = None\n"
            "client.files_upload_v2(file=f)\n")
    res = _run(cell)
    assert res.commented > 0
    assert not _live_calls(res.source, {"files_upload_v2"})


# ─── A2: helper-constructed clients (regression vs merge-base) ───────────────
@pytest.mark.parametrize("factory,method", [
    ("get_slack_client()", "send"),
    ("make_slack()", "api_call"),
    ("build_hook()", "post_message"),
])
def test_factory_constructed_client_is_neutralised(factory, method):
    """base=1 for all three; head regressed to 0 when the name-hint tier was
    dropped without replacement (`include_generic` was threaded but never
    read). The hint now anchors on the FACTORY name, which is evidence about
    the object, rather than the receiver name, which is not.
    """
    cell = (f"from slack_sdk import WebClient\nc = {factory}\n"
            f"c.{method}(payload)\n")
    res = _run(cell, include_generic=True)
    assert res.commented > 0, f"{factory}.{method} posts silently"


def test_slack_named_factory_returning_a_dict_is_left_alone():
    """The guard on the fix above: a slack-named factory is WEAK evidence.

    `get_slack_config()` returns a dict, and commenting `cfg.get("channel")`
    broke working code the regex path never touched (measured: base=0,
    intermediate head=1). Only recognised write verbs are commented on a
    weak binding.
    """
    cell = ("from slack_sdk import WebClient\n"
            "cfg = get_slack_config()\n"
            "ch = cfg.get('channel')\n")
    res = _run(cell, include_generic=True)
    assert res.commented == 0
    assert "weak-binding-nonwrite" in [k for k, _, _ in res.unprovable]


def test_hint_tier_only_fires_on_unknown_receivers():
    """Binding beats naming: a name KNOWN to hold a Kafka producer is skipped,
    while a slack-hinted name with no visible binding still fires (the regex
    path's behaviour, for clients arriving via %run or a parameter).
    """
    known = _run("from slack_sdk import WebClient\n"
                 "webhook_producer = KafkaProducer(b)\n"
                 "webhook_producer.send(topic, msg)\n", include_generic=True)
    assert known.commented == 0

    unknown = _run("from slack_sdk import WebClient\n"
                   "slack_hook.send(payload)\n", include_generic=True)
    assert unknown.commented > 0


# ─── A3: binding shapes that were untracked ──────────────────────────────────
@pytest.mark.parametrize("cell,label", [
    ("from slack_sdk import WebClient as W\nc = W(token=t)\n"
     "c.reactions_add(name='x')\n", "aliased ctor import"),
    ("from slack_sdk import WebClient\nc, d = WebClient(token=t), 1\n"
     "c.reactions_add(name='x')\n", "tuple assignment"),
    ("from slack_sdk import WebClient\nif (c := WebClient(token=t)):\n"
     "    c.reactions_add(name='x')\n", "walrus"),
    ("from slack_sdk import WebClient\nc = WebClient(token=t) if p else None\n"
     "c.reactions_add(name='x')\n", "conditional expression"),
    ("from slack_sdk import WebClient\nclient = WebClient(token=t)\n"
     "c2 = client\nc2.files_upload_v2(file=f)\n", "instance alias"),
])
def test_untracked_binding_shapes(cell, label):
    """All five measured base=0, head=0 — silent, and not reported either.

    Deliberately asserted with `reactions_add`, not `chat_postMessage`: tier-1
    matches the latter by text, which MASKS a broken binding layer. That
    masking is why the alias-import gap survived review.
    """
    res = _run(cell)
    assert res.commented > 0, f"{label} posts silently"


# ─── A4: a bound write method passed as a value ──────────────────────────────
def test_bound_write_method_passed_as_a_value():
    """`_classify` inspected only ast.Call, so the reference escaped and the
    post fired when the callee invoked it."""
    cell = ("from slack_sdk import WebClient\nclient = WebClient(token=t)\n"
            "retry(client.files_upload_v2, f)\n")
    res = _run(cell)
    assert res.commented > 0
    assert "method-as-value" in [k for k, _, _ in res.unprovable]


def test_attribute_read_on_a_client_is_not_commented():
    """The guard on the fix above — `client.token` is not a write."""
    cell = ("from slack_sdk import WebClient\nclient = WebClient(token=t)\n"
            "hdr = {'Authorization': client.token}\n")
    assert _run(cell).commented == 0


# ─── B6: the report gate must not hide true positives ───────────────────────
@pytest.mark.parametrize("stmt", [
    "clients['s'].files_upload_v2(file=f)",
    "d['c'].post(text='x')",
])
def test_unresolvable_receivers_are_reported(stmt):
    """All four reported nothing on the head commit: the gate required a
    slack-ish word in the SAME statement plus a `resource_action` method name,
    and `post` fails the shape test outright. Evidence now comes from the
    cell.
    """
    NOTE = """`bot.client.files_upload_v2(...)` also left this list: an
    UNRESOLVED receiver calling an unmistakable Slack Web API write is now
    neutralised rather than reported. `cfg.slack_webhook.send(payload)` used to be in this list. It is
    now NEUTRALISED rather than reported, because dotted receivers are resolved
    as binding keys — a strictly better outcome, so it moved to
    test_dotted_receiver_bound_to_a_client_is_neutralised. The three left here
    are genuinely unresolvable: a subscript in the path, or a receiver whose
    own binding is invisible."""
    res = _run(f"from slack_sdk import WebClient\n{stmt}\n", include_generic=True)
    assert "indirect-receiver" in [k for k, _, _ in res.unprovable], stmt


def test_report_gate_does_not_flood_a_pandas_heavy_slack_cell():
    """The flood the gate exists to stop was real: 39 findings on the live
    Monitoring_and_Posting and 98 on Reporting_Excel, none of them Slack.

    Method SHAPE alone still admitted every underscored pandas/Spark call in a
    cell that merely imported slack (to_dict, sort_values, reset_index). The
    gate is now Slack's RESOURCE namespace, which is a closed set.
    """
    cell = ("from slack_sdk import WebClient\n"
            "client = WebClient(token=tok)\n"
            "rows = df.toPandas().to_dict('records')\n"
            "s = pd.DataFrame(rows).sort_values('k').reset_index(drop=True)\n"
            "out = s['k'].str.strip().tolist()\n"
            "spark.sql('select 1').write.mode('overwrite').saveAsTable('t')\n"
            "client.chat_postMessage(channel='C1', text='done')\n")
    res = _run(cell, include_generic=True)
    assert res.commented > 0, "the real post must still be neutralised"
    assert [k for k, _, _ in res.unprovable] == [], "report flooded"


# ─── B7: an unparseable cell with Slack evidence must be loud ───────────────
def test_unparseable_cell_with_slack_evidence_is_reported():
    """The regex fallback does not cover files_upload_v2, and the downstream
    repair loop can revalidate the cell with the post still live."""
    cell = ("from slack_sdk import WebClient\n"
            "print 'py2 syntax'\n"
            "client.files_upload_v2(file=f)\n")
    res = neutralize_cell(cell, new_bindings(), include_generic=True)
    assert res.parse_error is not None
    assert "unparseable-with-slack-evidence" in [k for k, _, _ in res.unprovable]


# ─── C8: scope- and order-insensitive bindings resurrected the Kafka FP ─────
def test_def_local_binding_does_not_feed_the_notebook_bindings():
    """A never-called def-local `client = WebClient(...)` re-added the name
    after the module-level Kafka rebind, so the production Kafka send was
    commented out of the delivered notebook. Measured head=1 — wrong.
    """
    cell = ("def helper():\n"
            "    client = WebClient(token=t)\n"
            "client = KafkaProducer(b)\n"
            "client.send(topic, msg)\n")
    res = _run(cell, include_generic=True)
    assert res.commented == 0
    assert "client.send(topic, msg)" in res.source


def test_use_before_slack_bind_is_left_running_but_reported():
    """`producer.send(...)` above `producer = WebClient(...)`.

    Commenting it is the Kafka false positive (measured base=1). On a fresh
    kernel the call raises NameError rather than posting, so leaving it live
    is correct — but it CAN post on a re-run in a live kernel, so it is
    reported rather than silently allowed.
    """
    cell = ("from slack_sdk import WebClient\n"
            "producer.send(topic, msg)\n"
            "producer = WebClient(token=t)\n")
    res = _run(cell, include_generic=True)
    assert res.commented == 0
    assert "use-before-slack-bind" in [k for k, _, _ in res.unprovable]


def test_a_client_bound_inside_a_function_still_posts_when_called():
    """The scope barrier must not become a hole: a post INSIDE a def is still
    neutralised, judged against the end-of-cell state (a function can be
    called at any later point)."""
    for cell in (
        "from slack_sdk import WebClient\nclient = WebClient(token=t)\n"
        "def post():\n    client.files_upload_v2(file=f)\n",
        # bind AFTER the def -- the function body must still be covered
        "from slack_sdk import WebClient\n"
        "def post():\n    client.files_upload_v2(file=f)\n"
        "client = WebClient(token=t)\n",
    ):
        assert _run(cell).commented > 0, cell


# ─── C9: import origin ───────────────────────────────────────────────────────
def test_same_named_class_from_a_non_slack_module_is_untouched():
    """`SLACK_CTORS` matched bare class names with no origin check, so a
    non-Slack `WebClient` had ALL its instance calls commented (measured
    head=2; base touched neither), breaking working code."""
    cell = ("from internal_http import WebClient\n"
            "c = WebClient(url=u)\n"
            "c.fetch_report()\n"
            "c.upload_metrics(m)\n")
    res = _run(cell)
    assert res.commented == 0
    assert "c.fetch_report()" in res.source and "c.upload_metrics(m)" in res.source


def test_slack_import_of_the_same_name_still_wins():
    """Fail-safe direction: if both a Slack and a non-Slack import of the same
    name are seen, treat it as Slack."""
    cell = ("from slack_sdk import WebClient\n"
            "c = WebClient(token=t)\n"
            "c.files_upload_v2(file=f)\n")
    assert _run(cell).commented > 0


def test_no_import_at_all_still_defaults_to_slack():
    """A bare `WebClient(...)` with no visible import keeps the old fail-safe
    behaviour — absence of an import is not evidence of innocence."""
    assert _run("c = WebClient(token=t)\nc.files_upload_v2(file=f)\n").commented > 0


# ─── C10: comments and string literals pull in OPPOSITE directions ──────────
def test_tier1_ignores_a_trailing_comment():
    """`# results used to go to hooks.slack.com` commented out a real line of
    working code, producing a downstream NameError. Measured base=1."""
    cell = "rows = df.count()  # results used to go to hooks.slack.com\n"
    res = _run(cell)
    assert res.commented == 0
    assert "rows = df.count()" in res.source


def test_tier1_still_matches_a_webhook_url_inside_a_string():
    """The other half: `hooks.slack.com` lives inside a string in real code.
    Masking string literals here would turn a false positive into a false
    negative, which is the wrong direction — so literals are NOT masked for
    this pattern.
    """
    cell = "requests.post('https://hooks.slack.com/services/X', json=p)\n"
    assert _run(cell).commented > 0


@pytest.mark.parametrize("cell", [
    'def f():\n    """call client.chat_postMessage() to post"""\n    return 1\n',
    "log.info('use client.chat_postMessage() instead')\n",
])
def test_a_code_pattern_only_inside_a_string_is_not_commented(cell):
    """`.chat_postMessage(` cannot execute from inside a docstring or a log
    message. The docstring case was a NEW false positive (base skipped
    function bodies entirely); the log case was parity with base.
    """
    res = _run(cell)
    assert res.commented == 0
    assert "tier1-in-string-literal" in [k for k, _, _ in res.unprovable]


def test_a_real_call_on_a_line_that_also_has_strings_is_still_commented():
    """Regression guard for the masking itself: an early version blanked whole
    ROWS containing any string token, so `client.chat_postMessage(text='x')`
    stopped matching — a false negative introduced by the fix.
    """
    for cell in (
        "client.chat_postMessage(channel='C1', text='x')\n",
        "send_slack('hello')\n",
        "client.chat_postMessage(\n    channel='C1',\n    text='hi',\n)\n",
        "try:\n    client.chat_postMessage(channel='C1', text='x')\nexcept Exception:\n    pass\n",
    ):
        assert _run(cell).commented > 0, cell


# ─── D12: nested control-flow headers ───────────────────────────────────────
def test_nested_header_writes_are_edited_once():
    """Both containers landed in `header_targets` with overlapping spans, and
    the bottom-up edit double-commented the inserted `pass`. Fail-safe before
    (still parsed, still neutralised) but cosmetically broken.
    """
    cell = ("client = WebClient(token=t)\n"
            "while client.chat_update(x):\n"
            "    if client.chat_delete(y):\n"
            "        print(1)\n")
    res = _run(cell)
    ast.parse(res.source)
    assert "# #" not in res.source
    assert res.source.count("slack execution neutralised") == 1
    assert not _live_calls(res.source, {"chat_update", "chat_delete"})


# ─── Binding state: the three-way split is the contract ─────────────────────
def test_binding_state_is_three_way():
    b = new_bindings()
    update_bindings("from slack_sdk import WebClient\n"
                    "a = WebClient(token=t)\n"
                    "kafka = KafkaProducer(x)\n", b)
    assert b.get("a") is True            # Slack
    assert b.get("kafka") is False       # assigned, no Slack evidence
    assert "ghost" not in b              # never assigned -> UNKNOWN


def test_weak_bindings_are_tracked_separately():
    b = new_bindings()
    update_bindings("real = WebClient(token=t)\nweak = get_slack_client()\n", b)
    assert b.get("real") is True and not b.is_weak("real")
    assert b.get("weak") is True and b.is_weak("weak")


def test_weakness_survives_the_cell_boundary():
    """Regression guard: `update_bindings` originally copied names, ctors and
    module aliases back to the caller's state but NOT the weak set, so a weak
    binding was promoted to strong in the next cell and the false positive
    reappeared one cell later.
    """
    b = new_bindings()
    _run("from slack_sdk import WebClient\ncfg = get_slack_config()\n", b,
         include_generic=True)
    assert b.is_weak("cfg"), "weakness lost across cells"
    res = _run("ch = cfg.get('channel')\n", b, include_generic=True)
    assert res.commented == 0


# ═══════════════════════════════════════════════════════════════════════════
# PR #82 re-review, residual R1: laundering a Slack binding through a call
# that cannot be identified. Measured base=1, head-at-1055b0f=0, ZERO findings
# — a silent coverage regression against the merge-base regex path.
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("cell,label", [
    ("from slack_sdk import WebClient\nslack_hook = WebClient(token=t)\n"
     "slack_hook = normalize(slack_hook)\nslack_hook.send('hello')\n",
     "call form"),
    ("from slack_sdk import WebClient\nclient = WebClient(token=t)\n"
     "client = client.copy()\nclient.send('x')\n",
     "method form"),
    ("from slack_sdk import WebClient\nhook = WebClient(token=t)\n"
     "hook = hook.with_token(x)\nhook.chat_postMessage(text='y')\n",
     "method form with args"),
])
def test_laundered_binding_is_still_neutralised(cell, label):
    """The client goes IN, so whatever comes OUT is probably still a client.

    Marking it known-non-Slack did two things at once: it unbound the client
    AND blocked the receiver-hint tier that would otherwise have caught it —
    one state doing two jobs that disagree on this shape.
    """
    res = _run(cell, include_generic=True)
    assert res.commented > 0, f"{label}: laundered client posts silently"


def test_laundering_that_does_not_name_the_target_is_reported():
    """`slack_hook = normalize(cfg)` cannot be proven to still hold a client,
    so it is not commented — but it must not be silent either."""
    cell = ("from slack_sdk import WebClient\nslack_hook = WebClient(token=t)\n"
            "slack_hook = normalize(cfg)\nslack_hook.send('x')\n")
    res = _run(cell, include_generic=True)
    assert res.commented == 0
    assert "slack-binding-laundered" in [k for k, _, _ in res.unprovable]


@pytest.mark.parametrize("method", [
    "close()", "update(d)", "add(v)", "remove(k)",
    "open()", "create()", "push(x)", "send(m)", "post(d)",
])
def test_objects_merely_derived_from_a_client_are_untouched(method):
    """The guard on the fix above, and the reason it tests the TARGET name
    rather than any Slack name in the call.

    A looser rule -- degrade whenever an unknown call receives a Slack client
    -- introduced 9 false positives in one go: every object built FROM a
    client had its write-verb methods commented, none of which the regex path
    touched.
    """
    cell = ("from slack_sdk import WebClient\nclient = WebClient(token=t)\n"
            f"x = make_thing(client)\nr = x.{method}\n")
    assert _run(cell, include_generic=True).commented == 0


def test_a_genuine_rebind_to_kafka_is_still_not_commented():
    """The reviewer's literal suggestion — degrade EVERY unknown-call rebind of
    a Slack-bound name to WEAK — was not taken, because it comments this: the
    `.send` here is a real Kafka send. It is reported instead.
    """
    cell = ("client = WebClient(token=t)\nclient = KafkaProducer(b)\n"
            "client.send(topic, msg)\n")
    res = _run(cell, include_generic=True)
    assert res.commented == 0, "a real Kafka send must not be commented"
    assert "client.send(topic, msg)" in res.source
    assert "slack-binding-laundered" in [k for k, _, _ in res.unprovable]


def test_laundering_report_cannot_fire_without_a_live_slack_binding():
    """Bounds the noise: the report keys on a name that is CURRENTLY bound to
    a Slack client, so ordinary non-Slack rebinding never triggers it.
    Measured 0 across boto3/connection/model-retuning cells.
    """
    for cell in (
        "producer = KafkaProducer(bootstrap_servers=b)\nproducer.send(t, m)\n",
        "client = boto3.client('s3')\nclient = boto3.client('sts')\n"
        "client.get_caller_identity()\n",
        "conn = get_conn()\nconn = reconnect()\nconn.execute(q)\n",
        "model = load_model(p)\nmodel = tune(model)\nmodel.predict(X)\n",
    ):
        res = _run(cell, include_generic=True)
        assert res.commented == 0
        assert "slack-binding-laundered" not in [k for k, _, _ in res.unprovable]


# ═══════════════════════════════════════════════════════════════════════════
# PR #82 re-review, residual R2: a client parked on a config object.
# `cfg.slack_webhook.send(...)` — base commented it, head only REPORTED it.
# Dotted receivers are now resolved as binding keys, symmetrically with plain
# names: same three-state model, same "binding beats naming" rule.
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("cell,label", [
    ("from slack_sdk import WebhookClient\ncfg.slack_webhook = WebhookClient(url=u)\n"
     "cfg.slack_webhook.send(text='x')\n", "bound then tier-2 method"),
    ("from slack_sdk import WebhookClient\ncfg.slack_webhook.send(text='x')\n",
     "unbound, slack-hinted last segment"),
    ("from slack_sdk import WebhookClient\na.b.slack_hook.api_call('chat.postMessage', d)\n",
     "three-deep chain"),
])
def test_dotted_receiver_bound_to_a_client_is_neutralised(cell, label):
    assert _run(cell, include_generic=True).commented > 0, label


def test_dotted_binding_covers_methods_the_regex_path_never_reached():
    """Beyond base: base only ever matched `send`/`api_call`/`post_message` on
    a dotted receiver, so the incident method itself (`files_upload_v2`) was
    missed entirely (measured base=0)."""
    cell = ("from slack_sdk import WebhookClient\n"
            "cfg.notifier = WebhookClient(url=u)\n"
            "cfg.notifier.files_upload_v2(file=f)\n")
    assert _run(cell, include_generic=True).commented > 0


@pytest.mark.parametrize("cell,label", [
    ("from slack_sdk import WebhookClient\ncfg.webhook_producer = KafkaProducer(b)\n"
     "cfg.webhook_producer.send(t, m)\n", "slack-hinted name, Kafka binding"),
    ("from slack_sdk import WebhookClient\ncfg.kafka = KafkaProducer(b)\n"
     "cfg.kafka.send(t, m)\n", "plainly non-slack"),
])
def test_dotted_kafka_producers_are_not_commented(cell, label):
    """Base commented the first of these (measured base=1) — its dotted
    receiver check was pure name-hint, so it had the SAME Kafka false positive
    on attribute paths that it had on plain names. Binding beats naming here
    too, so restoring dotted coverage must not restore that.
    """
    res = _run(cell, include_generic=True)
    assert res.commented == 0, label
    assert "indirect-receiver" not in [k for k, _, _ in res.unprovable], (
        "a receiver resolved as NON-Slack is not 'unresolvable' — reporting it "
        "is noise")


def test_rebinding_the_root_invalidates_its_dotted_children():
    """`cfg.notifier` described the OLD `cfg`. Once `cfg` is replaced the key
    is stale, and a stale binding that keeps claiming Slack is how working code
    gets commented."""
    live = ("from slack_sdk import WebhookClient\n"
            "cfg.notifier = WebhookClient(url=u)\ncfg.notifier.send(x)\n")
    assert _run(live, include_generic=True).commented > 0

    stale = ("from slack_sdk import WebhookClient\n"
             "cfg.notifier = WebhookClient(url=u)\ncfg = load_other()\n"
             "cfg.notifier.send(x)\n")
    res = _run(stale, include_generic=True)
    assert res.commented == 0
    assert "indirect-receiver" in [k for k, _, _ in res.unprovable]

    b = new_bindings()
    update_bindings("from slack_sdk import WebhookClient\n"
                    "cfg.notifier = WebhookClient(url=u)\n", b)
    assert b.get("cfg.notifier") is True
    update_bindings("cfg = load_other()\n", b)
    assert "cfg.notifier" not in b, "stale dotted key survived a root rebind"


@pytest.mark.parametrize("stmt", [
    "spark.conf.set('k', 'v')",
    "spark.sparkContext.setLogLevel('WARN')",
    "df.write.mode('overwrite').saveAsTable('t')",
    "self.logger.info('x')",
    "cfg.db.execute(q)",
    "boto.s3.upload_file(a, b)",
    "self.producer.send(topic, msg)",
    "conn.cursor.close()",
    "app.config.update(d)",
    "mlflow.tracking.MlflowClient().create_run(x)",
])
def test_ordinary_dotted_receivers_are_untouched(stmt):
    """Resolving dotted receivers must not start commenting ordinary chained
    code in any cell that happens to import slack."""
    cell = f"from slack_sdk import WebClient\nclient = WebClient(token=t)\n{stmt}\n"
    assert _run(cell, include_generic=True).commented == 0, stmt


def test_self_attribute_with_an_unmistakable_slack_method_is_neutralised():
    """`self.client` cannot be resolved — `self` denotes a different object per
    class — but `files_upload_v2` is a Slack Web API write by name alone, and
    no plausible non-Slack object exposes it. Fail safe: comment it."""
    cell = ("from slack_sdk import WebClient\n"
            "class N:\n"
            "    def __init__(self):\n"
            "        self.client = WebClient(token=t)\n"
            "    def post(self):\n"
            "        self.client.files_upload_v2(file=f)\n")
    res = _run(cell, include_generic=True)
    assert res.commented > 0
    assert "unresolved-slack-method" in [k for k, _, _ in res.unprovable]


def test_self_attributes_with_an_ambiguous_method_stay_report_only():
    """The other half, and the reason `self.*` is not tracked as a binding key.

    `.send()` is ambiguous, and one `self.client` key would let class A's Slack
    binding comment class B's Kafka send. Not a regression — base measured 0
    for this shape too.
    """
    cell = ("from slack_sdk import WebClient\n"
            "class N:\n"
            "    def __init__(self):\n"
            "        self.client = WebClient(token=t)\n"
            "    def post(self):\n"
            "        self.client.send('x')\n")
    assert _run(cell, include_generic=True).commented == 0


# ═══════════════════════════════════════════════════════════════════════════
# PR #82 re-review, residuals R3 + R4: loop semantics and untracked targets.
# ═══════════════════════════════════════════════════════════════════════════

# ─── R3: a loop body runs more than once ────────────────────────────────────
@pytest.mark.parametrize("cell,label", [
    ("from slack_sdk import WebClient\nfor i in range(2):\n    if i:\n"
     "        client.files_upload_v2(file=f)\n    client = WebClient(token=t)\n",
     "for"),
    ("from slack_sdk import WebClient\nwhile go:\n"
     "    client.files_upload_v2(file=f)\n    client = WebClient(token=t)\n",
     "while"),
    ("from slack_sdk import WebClient\nfor a in x:\n    for b in y:\n"
     "        client.files_upload_v2(file=f)\n    client = WebClient(token=t)\n",
     "nested"),
])
def test_use_before_bind_inside_a_loop_is_neutralised(cell, label):
    """Iteration 1 binds, iteration 2 posts — on a FRESH kernel.

    The use-before-bind heuristic left this live on the rationale that it would
    raise NameError before it could post. That rationale is true at module
    scope and false inside a loop, so the loop's fixed point is applied: every
    statement in the body also sees what the body binds.
    """
    res = _run(cell, include_generic=True)
    assert res.commented > 0, f"{label}: posts on the second iteration"


def test_use_before_bind_outside_a_loop_is_still_only_reported():
    """The non-loop rationale still holds for an AMBIGUOUS method: NameError on
    a fresh kernel, so commenting it would be the Kafka false positive.

    Uses `.send()` deliberately. An unmistakable Slack method on the same shape
    IS commented — see the assertion below — so this test would pass
    vacuously if it used `files_upload_v2`.
    """
    cell = ("from slack_sdk import WebClient\nproducer.send(topic, msg)\n"
            "producer = WebClient(token=t)\n")
    res = _run(cell, include_generic=True)
    assert res.commented == 0
    assert "use-before-slack-bind" in [k for k, _, _ in res.unprovable]

    unmistakable = ("from slack_sdk import WebClient\nclient.files_upload_v2(file=f)\n"
                    "client = WebClient(token=t)\n")
    assert _run(unmistakable, include_generic=True).commented > 0


def test_loop_fixed_point_only_promotes_slack_bindings():
    """The guard on the fix above — a Kafka rebind inside a loop must not make
    the loop body treat the name as a client."""
    cell = ("from slack_sdk import WebClient\nfor x in y:\n"
            "    producer.send(topic, msg)\n    producer = KafkaProducer(b)\n")
    res = _run(cell, include_generic=True)
    assert res.commented == 0
    assert "producer.send(topic, msg)" in res.source


# ─── R4: loop, comprehension and except-as targets ──────────────────────────
@pytest.mark.parametrize("cell,label", [
    ("from slack_sdk import WebClient\nfor client in [WebClient(token=t)]:\n"
     "    client.files_upload_v2(file=f)\n", "for over a list"),
    ("from slack_sdk import WebClient\nfor c in (WebClient(token=t),):\n"
     "    c.files_upload_v2(file=f)\n", "for over a tuple"),
    ("from slack_sdk import WebClient\n"
     "rs = [c.files_upload_v2(file=f) for c in [WebClient(token=t)]]\n",
     "list comprehension"),
    ("from slack_sdk import WebClient\n"
     "rs = list(c.files_upload_v2(file=f) for c in [WebClient(token=t)])\n",
     "generator expression"),
])
def test_iteration_targets_are_tracked(cell, label):
    """`_apply_stmt` handled Assign/AnnAssign/walrus/with-as but not iteration
    targets, so these leaked with ZERO findings — a plain-Name receiver is
    excluded from indirect reporting, so nothing surfaced them.

    A comprehension binds and uses its target inside ONE statement, which no
    per-statement snapshot can see; those are resolved as a statement-local
    overlay.
    """
    assert _run(cell, include_generic=True).commented > 0, label


def test_except_as_releases_a_stale_slack_binding():
    """`except Exception as client:` rebinds the name to an EXCEPTION.

    Untracked, the earlier Slack binding survived into the handler and
    commented `client.files_upload_v2(...)` there — measured head=1, base=0.
    That call is an AttributeError at runtime, not a post.
    """
    cell = ("from slack_sdk import WebClient\nclient = WebClient(token=t)\n"
            "try:\n    pass\nexcept Exception as client:\n"
            "    client.files_upload_v2(file=f)\n")
    res = _run(cell, include_generic=True)
    assert res.commented == 0
    assert "client.files_upload_v2(file=f)" in res.source


@pytest.mark.parametrize("cell,label", [
    ("for i in range(10):\n    i.close()\n", "range"),
    ("for c in kafka_clients:\n    c.send(t, m)\n", "unknown iterable"),
    ("for row in df.collect():\n    row.update(d)\n", "spark collect"),
    ("for f in files:\n    f.close()\n", "file handles"),
    ("xs = [r.asDict() for r in df.collect()]\n", "comprehension over spark"),
    ("try:\n    pass\nexcept Exception as e:\n    e.add_note('x')\n", "plain except-as"),
])
def test_iteration_targets_over_unknown_iterables_change_nothing(cell, label):
    """Only a literal sequence carries evidence. Everything else is UNKNOWN,
    not non-Slack, so ordinary loops cannot produce a false positive."""
    full = f"from slack_sdk import WebClient\n{cell}"
    assert _run(full, include_generic=True).commented == 0, label


# ═══════════════════════════════════════════════════════════════════════════
# Blocking findings F1-F3. Every case below executed a real Slack call against
# a recording fake before the fix.
# ═══════════════════════════════════════════════════════════════════════════

# ─── F1: function-local clients and client parameters ───────────────────────
@pytest.mark.parametrize("cell,label", [
    ("from slack_sdk import WebClient\ndef post():\n"
     "    client = WebClient(token=t)\n    client.files_upload_v2(file=f)\npost()\n",
     "local constructor"),
    ("from slack_sdk import WebClient\ndef post():\n"
     "    client = WebClient(token=t)\n    client.send('x')\n",
     "local constructor, ambiguous method"),
    ("from slack_sdk import WebClient\ndef post(client):\n"
     "    client.files_upload_v2(file=f)\npost(WebClient(token=t))\n",
     "client as a parameter"),
    ("from slack_sdk import WebClient\ndef outer():\n    def inner():\n"
     "        c = WebClient(token=t)\n        c.send('x')\n    inner()\n",
     "nested function"),
])
def test_function_local_clients_are_neutralised(cell, label):
    """Function bodies were skipped by the binding walk (correctly — a
    def-local binding must not feed notebook-wide state) and then classified
    against the module's FINAL state, so a locally constructed client was
    invisible. Each body now evolves its own lexical, source-ordered scope.
    """
    assert _run(cell, include_generic=True).commented > 0, label


@pytest.mark.parametrize("cell,label", [
    ("from slack_sdk import WebClient\nclient = WebClient(token=t)\n"
     "def go():\n    client = KafkaProducer(b)\n    client.send(topic, m)\n",
     "local non-Slack shadows a module Slack name"),
    ("from slack_sdk import WebClient\nclient = WebClient(token=t)\n"
     "def go(client):\n    client.send(topic, m)\n",
     "parameter shadows a module Slack name"),
    ("from slack_sdk import WebClient\ndef go():\n"
     "    p = KafkaProducer(b)\n    p.send(topic, m)\n",
     "plain Kafka inside a function"),
])
def test_function_scope_shadowing_does_not_produce_false_positives(cell, label):
    """The same defect ran in the other direction too: a module-level Slack
    name shadowed by a local non-Slack one was treated as Slack, so a
    function-local Kafka `send` was commented out of the delivered notebook.

    Parameters are removed from the seed rather than inherited — a client
    passed in is not the module-level one.
    """
    res = _run(cell, include_generic=True)
    assert res.commented == 0, label


def test_unresolved_receiver_with_an_unmistakable_slack_method():
    """Where the receiver genuinely cannot be resolved, the METHOD decides.

    `files_upload_v2` is `resource_action` with the resource in Slack's closed
    namespace and a recognised write verb. No plausible non-Slack object
    exposes it, and the alternative is letting the post through.
    """
    plain = ("from slack_sdk import WebClient\n"
             "cfg.slack_webhook.files_upload_v2(file=f)\n")
    res = _run(plain, include_generic=True)
    assert res.commented > 0
    assert "unresolved-slack-method" in [k for k, _, _ in res.unprovable]

    # Same call in a control-flow header: the whole container is commented, so
    # it is reported as container-header-write rather than per-statement.
    header = ("from slack_sdk import WebClient\n"
              "if cfg.slack_webhook.files_upload_v2(file=f):\n    pass\n")
    res_h = _run(header, include_generic=True)
    assert res_h.commented > 0
    assert "container-header-write" in [k for k, _, _ in res_h.unprovable]
    ast.parse(res_h.source)


def test_a_known_non_slack_receiver_beats_the_method_name():
    """The guard on the rule above: it applies to UNKNOWN receivers only.

    `except Exception as client:` binds an exception. Letting the method name
    override that commented an AttributeError-to-be, which base left alone.
    """
    cell = ("from slack_sdk import WebClient\nclient = WebClient(token=t)\n"
            "try:\n    pass\nexcept Exception as client:\n"
            "    client.files_upload_v2(file=f)\n")
    assert _run(cell, include_generic=True).commented == 0


def test_reads_are_still_exempt_on_unresolved_receivers():
    cell = "from slack_sdk import WebClient\ndef f(c):\n    return c.users_list()\n"
    assert _run(cell, include_generic=True).commented == 0


# ─── F2: special execution forms ────────────────────────────────────────────
def test_tier1_applies_to_control_flow_headers():
    """`if send_slack(msg):` executed. This is a safety REGRESSION the AST
    rewrite introduced: the regex path commented the header (base=1, head=0),
    because it matched on line text and never had to understand headers.
    """
    cell = "from slack_sdk import WebClient\nif send_slack(msg):\n    pass\n"
    res = _run(cell, include_generic=True)
    assert res.commented > 0
    ast.parse(res.source)


def test_bound_method_as_value_accepts_a_dotted_receiver():
    """`executor.submit(cfg.slack.files_upload_v2, f)` — the check accepted
    only a plain Name, so a dotted receiver escaped."""
    cell = ("from slack_sdk import WebClient\ncfg.slack = WebClient(token=t)\n"
            "executor.submit(cfg.slack.files_upload_v2, f)\n")
    assert _run(cell, include_generic=True).commented > 0


def test_line_magic_with_an_executable_tail_is_analysed():
    """`%time client.files_upload_v2(file=f)` was replaced by `pass` before any
    receiver analysis ran, so the call it executes was never examined."""
    cell = ("from slack_sdk import WebClient\nclient = WebClient(token=t)\n"
            "%time client.files_upload_v2(file=f)\n")
    res = _run(cell, include_generic=True)
    assert res.commented > 0
    assert "files_upload_v2" in res.source          # preserved as a comment


@pytest.mark.parametrize("cell,label", [
    ("%timeit -n 5 sum(range(10))\n", "flag form falls back to pass"),
    ("%pip install slack_sdk\n", "install magic"),
    ("%%time\nx = 1 + 1\n", "cell magic then code"),
    ("%time df.count()\n", "timed non-Slack call"),
])
def test_magic_handling_does_not_break_or_over_fire(cell, label):
    """Blanking the magic token must keep the ORIGINAL indent — moving the
    expression right turns `%time expr` into an IndentationError at module
    scope. Flag forms that cannot parse fall back to `pass`.
    """
    full = f"from slack_sdk import WebClient\n{cell}"
    res = _run(full, include_generic=True)
    assert res.commented == 0, label
    ast.parse(_strip_magics(res.source))


# ─── F3: only this module's own markers mean "already neutralised" ──────────
def test_an_unrelated_tool_annotation_does_not_suppress_neutralisation():
    """`_already_done` matched the whole "Oracle tool modification" family, and
    the tool writes those for DBFS path rewrites, stripped pip installs and
    more. One inside a multi-line upload suppressed Slack handling entirely
    (measured: commented=0, no finding, upload executed).
    """
    cell = ("from slack_sdk import WebClient\nclient = WebClient(token=t)\n"
            "client.files_upload_v2(\n"
            "    # Oracle tool modification: rewrote DBFS path\n"
            "    file=f,\n"
            ")\n")
    res = _run(cell, include_generic=True)
    assert res.commented > 0
    assert not _live_calls(res.source, {"files_upload_v2"})


def test_re_migration_of_our_own_output_is_still_a_no_op():
    """The narrowed marker must still make the pass idempotent."""
    cell = ("from slack_sdk import WebClient\nclient = WebClient(token=t)\n"
            "client.files_upload_v2(file=f)\n")
    first = _run(cell, include_generic=True)
    second = _run(first.source, include_generic=True)
    assert first.commented > 0
    assert second.commented == 0
    assert second.source == first.source


# ═══════════════════════════════════════════════════════════════════════════
# Blocking findings N1, N3, N5. Each case executed a live Slack call, or
# silently destroyed customer code, before the fix.
# ═══════════════════════════════════════════════════════════════════════════

# ─── N1: the analyser must fail CLOSED ──────────────────────────────────────
@pytest.mark.parametrize("cell,label", [
    ("%timeit -n 1 client.files_upload_v2(file=f)\n", "timeit with options"),
    ("%timeit -r3 -n1 client.files_upload_v2(file=f)\n", "multiple options"),
    ("%time client.files_upload_v2(file=f)\n", "no options"),
])
def test_magic_options_do_not_hide_the_executable_tail(cell, label):
    """`%timeit -n 1 <expr>` fell back to `pass` because the flags made the
    blanked line unparseable — and IPython runs the tail anyway, repeatedly.
    Options are consumed so the expression is still analysed.
    """
    full = f"from slack_sdk import WebClient\nclient = WebClient(token=t)\n{cell}"
    res = _run(full, include_generic=True)
    assert res.commented > 0, label
    assert "files_upload_v2" in res.source          # preserved as a comment


@pytest.mark.parametrize("cell,label", [
    ("from slack_sdk import WebClient\nclient = WebClient(token=t)\n"
     "files = !echo x\nclient.files_upload_v2(file=f)\n", "shell-assign magic"),
    ("from slack_sdk import WebClient\nclient = WebClient(token=t)\n"
     "print 'py2'\nclient.files_upload_v2(file=f)\n", "py2 syntax"),
])
def test_unparseable_cells_fail_closed(cell, label):
    """An AST failure used to hand the ORIGINAL source back, so an upload in an
    IPython-valid-but-Python-invalid cell reached the kernel live. Line-level
    fail-closed neutralisation is applied instead.
    """
    res = _run(cell, include_generic=True)
    assert res.parse_error is not None
    assert res.commented > 0, label
    assert "unparseable-with-slack-evidence" in [k for k, _, _ in res.unprovable]
    assert not res.unsafe, "line-level pass should have cleared the evidence"


@pytest.mark.parametrize("cell", [
    "%timeit -n 1 sum(range(10))\n",
    "%pip install slack_sdk\n",
    "%%time\nx = 1\n",
    "%time df.count()\n",
])
def test_fail_closed_handling_does_not_over_fire_on_magics(cell):
    full = f"from slack_sdk import WebClient\n{cell}"
    assert _run(full, include_generic=True).commented == 0


def test_unparseable_without_slack_evidence_is_left_alone():
    res = _run("print 'x'\ndf.count()\n", include_generic=False)
    assert res.commented == 0 and not res.unsafe


# ─── N3: recursive lexical scopes ───────────────────────────────────────────
@pytest.mark.parametrize("cell,label", [
    ("from slack_sdk.webhook import WebhookClient\nclass A:\n"
     "    client = WebhookClient(url=u)\n    client.send(text='x')\n",
     "class body constructs and posts at definition"),
    ("from slack_sdk import WebClient\ndef outer():\n"
     "    client = WebClient(token=t)\n    def inner():\n"
     "        client.send('x')\n    inner()\n",
     "closure over the outer function's client"),
    ("from slack_sdk import WebClient\ndef a():\n    c = WebClient(token=t)\n"
     "    def b():\n        def d():\n            c.send('x')\n        d()\n    b()\n",
     "three-deep closure"),
])
def test_nested_and_class_scopes_are_evolved(cell, label):
    """Every function used to be seeded independently from module-final state,
    so a nested function never saw its enclosing function's bindings and class
    bodies were never evolved at all.

    Asserted with `.send()` on purpose: `files_upload_v2` is caught by the
    unmistakable-method rule regardless of scope, which would make these pass
    vacuously.
    """
    assert _run(cell, include_generic=True).commented > 0, label


@pytest.mark.parametrize("cell,label", [
    ("from slack_sdk import WebClient\ndef outer():\n"
     "    client = WebClient(token=t)\n    def inner(client):\n"
     "        client.send('x')\n    inner(kafka)\n", "parameter shadows the closure"),
    ("from slack_sdk import WebClient\ndef outer():\n"
     "    client = WebClient(token=t)\n    def inner():\n"
     "        client = KafkaProducer(b)\n        client.send(topic, m)\n    inner()\n",
     "local rebind shadows the closure"),
    ("class A:\n    p = KafkaProducer(b)\n    p.send(t, m)\n", "class body Kafka"),
])
def test_nested_scope_shadowing_is_respected(cell, label):
    assert _run(cell, include_generic=True).commented == 0, label


# ─── N5: a Slack call sharing a line with unrelated code ────────────────────
def test_unrelated_code_on_a_shared_line_stays_live():
    """`x = compute(); client.files_upload_v2(...)` commented the WHOLE line, so
    `x` was never assigned and `consume(x)` raised NameError — and the repair
    model's obvious fix is to restore the line, upload included.

    Shared-line detection counted only Slack TARGETS, so with one call and one
    assignment there was a single target and no finding fired at all. Uses
    `files_upload_v2` deliberately: the old test used tier-1
    `chat_postMessage`, which classifies BOTH statements and hides the defect.
    """
    cell = ("from slack_sdk import WebClient\nclient = WebClient(token=t)\n"
            "x = compute_required_value(); client.files_upload_v2(file=f)\n"
            "consume(x)\n")
    res = _run(cell, include_generic=True)

    assert not _live_calls(res.source, {"files_upload_v2"})
    assert "shared-line" in [k for k, _, _ in res.unprovable]
    tree = ast.parse(res.source)
    assigned = {t.id for n in ast.walk(tree) if isinstance(n, ast.Assign)
                for t in n.targets if isinstance(t, ast.Name)}
    assert "x" in assigned, "unrelated assignment was destroyed"


def test_shared_line_preservation_keeps_a_block_valid():
    """The preserved statement also satisfies the block, so no stray `pass`."""
    cell = ("from slack_sdk import WebClient\nclient = WebClient(token=t)\n"
            "if flag:\n    y = 2; client.files_upload_v2(file=f)\n")
    res = _run(cell, include_generic=True)
    ast.parse(res.source)
    assert not _live_calls(res.source, {"files_upload_v2"})
    assert "y = 2" in res.source
    assert res.source.count("pass") == 0


def test_two_slack_calls_on_one_line_are_commented_once():
    cell = ("from slack_sdk import WebClient\nclient = WebClient(token=t)\n"
            "client.chat_postMessage(text='a'); client.chat_update(text='b')\n")
    res = _run(cell, include_generic=True)
    assert "# #" not in res.source
    assert not _live_calls(res.source, {"chat_postMessage", "chat_update"})


# ─── N4 semantics: bindings must not run ahead of the executing cell ────────
def test_binding_state_is_not_advanced_past_the_executing_cell():
    """The %run child pre-scan walked bindings through the child's LAST cell
    before the FIRST one ran, so an earlier nested %run saw future state.

    Concretely: the parent holds a real Slack client; a later child cell
    rebinds the name to Kafka. Folding that future rebind in first marks the
    name non-Slack and the earlier `client.send(...)` — which at runtime still
    holds the Slack client — is left live.
    """
    parent = "from slack_sdk.webhook import WebhookClient\nclient = WebhookClient(url=u)\n"
    early_use = "client.send('x')\n"
    later_rebind = "client = KafkaProducer(b)\n"

    just_in_time = new_bindings()
    update_bindings(parent, just_in_time)
    assert neutralize_cell(early_use, just_in_time,
                           include_generic=True).commented > 0

    pre_scanned = new_bindings()
    update_bindings(parent, pre_scanned)
    update_bindings(early_use, pre_scanned)
    update_bindings(later_rebind, pre_scanned)          # the future rebind
    assert neutralize_cell(early_use, pre_scanned,
                           include_generic=True).commented == 0, (
        "if this now passes, pre-scanning is safe and this test can go — "
        "until then it documents why neutralisation is just-in-time")


def test_no_spurious_module_level_pass():
    """A MODULE body may legally be empty, so `pass` at top level is noise —
    and it was emitted mid-cell, between two commented calls, in a
    customer-facing artifact. Blocks still get one where they need it.
    """
    marker = "`pass` keeps"
    for cell in ("client.chat_postMessage(text='x')\n",
                 "client.chat_postMessage(text='a')\nclient.chat_update(text='b')\n"):
        res = _run(cell, include_generic=True)
        assert marker not in res.source
        ast.parse(res.source)
    for cell in ("if f:\n    client.chat_postMessage(text='x')\n",
                 "def g():\n    client.chat_postMessage(text='x')\n"):
        res = _run(cell, include_generic=True)
        assert res.source.count(marker) == 1
        ast.parse(res.source)


# ═══════════════════════════════════════════════════════════════════════════
# Blocking findings F1 (logical statements) and F3 (closure call-time binding).
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("cell,label", [
    ('files = !printf shell-ok\nclient = WebhookClient()\n'
     'client.\\\n    send(text="POSTED")\n', "backslash continuation"),
    ('files = !echo x\nclient = WebhookClient()\n'
     'client.send(\n    text="POSTED"\n)\n', "bracket continuation"),
])
def test_unparseable_cell_with_a_split_call_is_neutralised(cell, label):
    """The shell assignment makes the cell invalid for `ast.parse` but valid
    IPython, so the fail-closed line pass is the only thing standing between
    the call and the kernel — and it examined PHYSICAL lines, so neither
    `client.\\` nor `    send(...)` contained `.send(`. The guard returned the
    source byte-identical and IPython ran the post.

    Statements are grouped into LOGICAL lines (backslash and bracket
    continuations) before detection.
    """
    res = _run(cell, include_generic=True)
    assert res.parse_error is not None
    assert res.commented > 0, label
    assert res.source != cell


def test_failsafe_detection_uses_cell_level_evidence():
    """The client is constructed on one line and used on another, so requiring
    the Slack evidence on the SAME statement missed it."""
    cell = 'x = !echo hi\nclient = WebhookClient()\nclient.send(text="p")\n'
    assert _run(cell, include_generic=True).commented > 0


@pytest.mark.parametrize("cell,label", [
    ("print 'py2'\ndf.count()\nfoo.\\\n    bar()\n", "no Slack anywhere"),
    ("print 'py2'\nproducer.send(topic, m)\n", "Kafka, no Slack evidence"),
])
def test_failsafe_pass_does_not_fire_without_evidence(cell, label):
    assert _run(cell, include_generic=True).commented == 0, label


# ─── F3: closures resolve free variables at CALL time ───────────────────────
def test_closure_over_a_client_bound_after_the_def():
    """Python resolves a closure's free variables when the function RUNS.

        def outer():
            def inner():
                client.send(message)     # resolved at call time
            client = WebhookClient(...)  # ...bound AFTER the def
            inner()

    Seeding `inner` from the state at its `def` missed this entirely.
    """
    cell = ("from slack_sdk.webhook import WebhookClient\n"
            "def outer():\n"
            "    def inner():\n"
            "        client.send(message)\n"
            "    client = WebhookClient(url=url)\n"
            "    inner()\nouter()\n")
    assert _run(cell, include_generic=True).commented > 0


def test_closure_rebound_to_non_slack_before_the_call_is_left_alone():
    """The inverse of the same defect: a client bound BEFORE the def and
    rebound to Kafka after it had the harmless Kafka send commented."""
    cell = ("from slack_sdk.webhook import WebhookClient\n"
            "def outer():\n"
            "    client = WebhookClient(url=url)\n"
            "    def inner():\n"
            "        client.send(m)\n"
            "    client = KafkaProducer(b)\n"
            "    inner()\nouter()\n")
    res = _run(cell, include_generic=True)
    assert res.commented == 0
    assert "client.send(m)" in res.source


def test_deep_closure_with_late_binding():
    cell = ("from slack_sdk.webhook import WebhookClient\n"
            "def a():\n    def b():\n        def d():\n"
            "            c.send('x')\n        d()\n"
            "    c = WebhookClient(url=u)\n    b()\n")
    assert _run(cell, include_generic=True).commented > 0


# ═══════════════════════════════════════════════════════════════════════════
# Blocking findings G2 (call-site binding states) and G3 (global effects).
# ═══════════════════════════════════════════════════════════════════════════

def test_closure_called_at_two_different_binding_states():
    """A function runs at EVERY caller's state, not just the last one.

        client = WebhookClient(...)
        def inner(): client.send(...)
        inner()                       # <- Slack here
        client = KafkaProducer(b)
        inner()                       # the later rebind hid the call above

    Judged against a single enclosing-final state, the trailing Kafka
    assignment concealed the first invocation entirely.
    """
    cell = ("from slack_sdk.webhook import WebhookClient\n"
            "def outer():\n"
            "    client = WebhookClient(url=u)\n"
            "    def inner():\n"
            "        client.send(text='POSTED')\n"
            "    inner()\n"
            "    client = KafkaProducer(b)\n"
            "    inner()\nouter()\n")
    assert _run(cell, include_generic=True).commented > 0


def test_closure_called_before_and_after_a_slack_bind():
    """The inverse ordering must also be caught."""
    cell = ("from slack_sdk.webhook import WebhookClient\n"
            "def outer():\n"
            "    client = KafkaProducer(b)\n"
            "    def inner():\n"
            "        client.send(m)\n"
            "    inner()\n"
            "    client = WebhookClient(url=u)\n"
            "    inner()\nouter()\n")
    assert _run(cell, include_generic=True).commented > 0


def test_closure_only_ever_called_with_kafka_is_left_alone():
    """The guard on the union: if no call site is Slack-bound, nothing fires."""
    cell = ("from slack_sdk.webhook import WebhookClient\n"
            "def outer():\n"
            "    client = KafkaProducer(b)\n"
            "    def inner():\n"
            "        client.send(m)\n"
            "    inner()\nouter()\n")
    res = _run(cell, include_generic=True)
    assert res.commented == 0
    assert "client.send(m)" in res.source


def test_closure_called_with_kafka_before_later_slack_rebind_is_left_alone():
    """A known call site must not inherit state from after that call.

    The enclosing final-state union made the later WebClient assignment travel
    backwards in time and removed a Kafka send that had already run.
    """
    cell = ("from slack_sdk.webhook import WebhookClient\n"
            "def outer():\n"
            "    client = KafkaProducer(b)\n"
            "    def inner():\n"
            "        client.send(topic, message)\n"
            "    inner()\n"
            "    client = WebhookClient(url=url)\n"
            "outer()\n")
    res = _run(cell, include_generic=True)
    assert res.commented == 0
    assert "client.send(topic, message)" in res.source


def test_module_function_called_with_kafka_before_later_slack_rebind_is_left_alone():
    """The same call-time rule applies to module-level function bodies."""
    cell = ("from slack_sdk.webhook import WebhookClient\n"
            "client = KafkaProducer(b)\n"
            "def send_now():\n"
            "    client.send(topic, message)\n"
            "send_now()\n"
            "client = WebhookClient(url=url)\n")
    res = _run(cell, include_generic=True)
    assert res.commented == 0
    assert "client.send(topic, message)" in res.source


def test_global_binding_established_by_a_called_function():
    """Function bodies are isolated from module state by design, but a direct
    call still applies the function's declared `global` effects."""
    cell = ("from slack_sdk.webhook import WebhookClient\n"
            "def init():\n    global client\n    client = WebhookClient(url=u)\n"
            "init()\nclient.send(text='POSTED')\n")
    assert _run(cell, include_generic=True).commented > 0


@pytest.mark.parametrize("cell,label", [
    ("def init():\n    global client\n    client = KafkaProducer(b)\n"
     "init()\nclient.send(t, m)\n", "global bind to Kafka"),
    ("from slack_sdk.webhook import WebhookClient\n"
     "def init():\n    global client\n    client = WebhookClient(url=u)\n"
     "client.send(text='x')\n", "init() never called"),
])
def test_global_effects_do_not_over_fire(cell, label):
    assert _run(cell, include_generic=True).commented == 0, label


# ═══════════════════════════════════════════════════════════════════════════
# Blocking findings H1 (global-effect precision) and H2 (closure aliases).
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("cell,label", [
    ("from slack_sdk.webhook import WebhookClient\n"
     "def init(flag):\n    global client\n"
     "    if flag:\n        client = WebhookClient(url=url)\n"
     "    else:\n        client = None\n"
     "init(True)\nclient.send(text='POSTED')\n",
     "branch — collapsing to the else arm hid the Slack arm"),
    ("from slack_sdk import WebClient as W\n"
     "def init():\n    global client\n    client = W(token=t)\n"
     "init()\nclient.send(text='POSTED')\n",
     "constructor alias must resolve in the call-site environment"),
    ("from slack_sdk.webhook import WebhookClient\n"
     "def init():\n    global client\n    client = WebhookClient(url=url)\n"
     "    def _reset():\n        global client\n        client = None\n"
     "init()\nclient.send(text='POSTED')\n",
     "a never-called NESTED def must not cancel the parent's effect"),
    ("from slack_sdk.webhook import WebhookClient\n"
     "def init():\n    global client\n    client = WebhookClient(url=url)\n"
     "init()\nclient.send(text='POSTED')\n"
     "def init():\n    global client\n    client = None\n",
     "a LATER redefinition must not change what an earlier call did"),
])
def test_global_effects_are_definition_and_branch_precise(cell, label):
    """`ast.walk` collapsed every assignment a function makes into one
    name-indexed summary, losing branch selection, nesting, definition identity
    and source order. Each case above executed a real post."""
    assert _run(cell, include_generic=True).commented > 0, label


@pytest.mark.parametrize("cell,label", [
    ("from slack_sdk.webhook import WebhookClient\ndef outer():\n"
     "    client = WebhookClient(url=url)\n"
     "    def inner():\n        client.send(text='POSTED')\n"
     "    f = inner\n    f()\n    client = KafkaProducer(b)\nouter()\n",
     "invoked through an alias"),
    ("from slack_sdk.webhook import WebhookClient\ndef outer():\n"
     "    client = WebhookClient(url=url)\n"
     "    def inner():\n        client.send(text='POSTED')\n"
     "    executor.submit(inner)\n    client = KafkaProducer(b)\nouter()\n",
     "handed to a callback"),
    ("from slack_sdk.webhook import WebhookClient\ndef outer():\n"
     "    client = WebhookClient(url=url)\n"
     "    def inner():\n        client.send(text='POSTED')\n"
     "    cbs = [inner]\n    client = KafkaProducer(b)\nouter()\n",
     "stored in a container"),
])
def test_closure_invoked_indirectly_is_still_analysed(cell, label):
    """`_called_names` recorded `f`, while the call-state lookup expected
    `inner`, so no call site was visible and the closure was judged against the
    scope's final Kafka state. Exact aliases are resolved; a function that
    escapes to a callback or container is judged against every state the scope
    passed through.
    """
    assert _run(cell, include_generic=True).commented > 0, label


@pytest.mark.parametrize("cell,label", [
    ("def init():\n    global client\n    client = KafkaProducer(b)\n"
     "init()\nclient.send(t, m)\n", "global bind to Kafka"),
    ("from slack_sdk.webhook import WebhookClient\n"
     "def init():\n    global client\n    client = WebhookClient(url=u)\n"
     "client.send(text='x')\n", "initializer never called"),
    ("from slack_sdk.webhook import WebhookClient\ndef outer():\n"
     "    client = KafkaProducer(b)\n    def inner():\n        client.send(m)\n"
     "    f = inner\n    f()\nouter()\n", "alias to a Kafka-only closure"),
    ("from slack_sdk.webhook import WebhookClient\ndef outer():\n"
     "    client = KafkaProducer(b)\n    def inner():\n        client.send(m)\n"
     "    executor.submit(inner)\nouter()\n", "escape of a Kafka-only closure"),
])
def test_effect_and_alias_analysis_do_not_over_fire(cell, label):
    assert _run(cell, include_generic=True).commented == 0, label


# ═══════════════════════════════════════════════════════════════════════════
# Blocking findings I1 (nested-scope re-entry, transitive effects) and
# I2 (alias chains, aliased escapes).
# ═══════════════════════════════════════════════════════════════════════════

def test_nested_def_under_control_flow_does_not_leak_a_global_declaration():
    """`_own_statements` skips nested definitions, but the `global` scan then
    ran `ast.walk` over each statement, re-entering a definition nested under
    an `if`. A never-called inner function's `global client` made the enclosing
    function's ordinary LOCAL `client = None` look like a module rebind, so the
    module client was marked non-Slack and the post went out.
    """
    cell = ("from slack_sdk.webhook import WebhookClient\n"
            "client = WebhookClient(url=url)\n"
            "def outer():\n"
            "    if True:\n"
            "        def never_called():\n            global client\n"
            "    client = None\n"
            "outer()\nclient.send(text='POSTED')\n")
    assert _run(cell, include_generic=True).commented > 0


@pytest.mark.parametrize("cell,label", [
    ("from slack_sdk.webhook import WebhookClient\n"
     "def init():\n    global client\n    client = WebhookClient(url=url)\n"
     "def wrapper():\n    init()\nwrapper()\nclient.send(text='POSTED')\n",
     "one hop"),
    ("from slack_sdk.webhook import WebhookClient\n"
     "def init():\n    global client\n    client = WebhookClient(url=url)\n"
     "def mid():\n    init()\ndef top():\n    mid()\n"
     "top()\nclient.send(text='POSTED')\n", "three hops"),
    ("from slack_sdk.webhook import WebhookClient\n"
     "def a():\n    b()\ndef b():\n    a()\n    global client\n"
     "    client = WebhookClient(url=url)\n"
     "a()\nclient.send(text='POSTED')\n", "mutual recursion terminates"),
])
def test_global_effects_propagate_through_callees(cell, label):
    """Only assignments syntactically present in the CALLED function were
    summarised, so one level of indirection hid the initializer completely."""
    assert _run(cell, include_generic=True).commented > 0, label


@pytest.mark.parametrize("cell,label", [
    ("from slack_sdk.webhook import WebhookClient\ndef outer():\n"
     "    client = WebhookClient(url=url)\n"
     "    def inner():\n        client.send(text='POSTED')\n"
     "    f = inner\n    g = f\n    g()\n    client = KafkaProducer(b)\nouter()\n",
     "alias of an alias"),
    ("from slack_sdk.webhook import WebhookClient\ndef outer():\n"
     "    client = WebhookClient(url=url)\n"
     "    def inner():\n        client.send(text='POSTED')\n"
     "    f = inner\n    executor.submit(f)\n"
     "    client = KafkaProducer(b)\nouter()\n",
     "aliased callback escape"),
])
def test_alias_chains_and_aliased_escapes(cell, label):
    """An alias was recorded only when its right-hand name was a definition, so
    `g = f` was lost; and escape detection checked canonical names only, so
    `executor.submit(f)` never marked `inner` as escaping."""
    assert _run(cell, include_generic=True).commented > 0, label


@pytest.mark.parametrize("cell,label", [
    ("def init():\n    global client\n    client = KafkaProducer(b)\n"
     "def wrapper():\n    init()\nwrapper()\nclient.send(t, m)\n",
     "transitive bind to Kafka"),
    ("from slack_sdk.webhook import WebhookClient\n"
     "def init():\n    global client\n    client = WebhookClient(url=u)\n"
     "def wrapper():\n    init()\nclient.send(text='x')\n",
     "wrapper never called"),
    ("from slack_sdk.webhook import WebhookClient\ndef outer():\n"
     "    client = KafkaProducer(b)\n    def inner():\n        client.send(m)\n"
     "    f = inner\n    f = other\n    f()\nouter()\n",
     "alias reassigned away"),
    ("from slack_sdk.webhook import WebhookClient\ndef outer():\n"
     "    client = KafkaProducer(b)\n    def inner():\n        client.send(m)\n"
     "    f = inner\n    g = f\n    g()\nouter()\n",
     "chained alias, Kafka only"),
])
def test_transitive_and_alias_analysis_do_not_over_fire(cell, label):
    assert _run(cell, include_generic=True).commented == 0, label


# ═══════════════════════════════════════════════════════════════════════════
# Blocking findings J1 (callee-local environment) and J2 (container storage).
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("cell,label", [
    ("from slack_sdk.webhook import WebhookClient\n"
     "def wrapper():\n"
     "    def init():\n        global client\n"
     "        client = WebhookClient(url=url)\n"
     "    init()\nwrapper()\nclient.send(text='POSTED')\n",
     "initializer defined INSIDE the wrapper"),
    ("from slack_sdk.webhook import WebhookClient\n"
     "def init():\n    global client\n    client = WebhookClient(url=url)\n"
     "def wrapper():\n    f = init\n    f()\n"
     "wrapper()\nclient.send(text='POSTED')\n",
     "initializer aliased INSIDE the wrapper"),
])
def test_transitive_effects_use_the_callee_s_own_environment(cell, label):
    """`defs`/`aliases` describe the CALLER's scope, so a helper defined or
    aliased inside the wrapper was invisible — and `_own_statements` excludes
    the nested definition, so the call could not be resolved at all. A
    function-local definition/alias environment is now evolved in source order
    while recursing, with enclosing names still visible underneath.
    """
    assert _run(cell, include_generic=True).commented > 0, label


@pytest.mark.parametrize("cell,label", [
    ("    box.callback = inner\n    box.callback()\n", "attribute storage"),
    ("    callbacks[0] = inner\n    callbacks[0]()\n", "subscript storage"),
    ("    a, b2 = inner, other\n    a()\n", "tuple destructuring"),
])
def test_container_storage_of_a_closure_counts_as_escaping(cell, label):
    """Escape detection returned early for ANY assignment whose right-hand side
    was a name, but alias tracking only records plain-NAME targets. So
    `box.callback = inner` was treated as a tracked alias while being recorded
    nowhere: neither aliased nor escaped, and the Slack call — invoked before
    the final Kafka rebind — stayed live.
    """
    full = ("from slack_sdk.webhook import WebhookClient\n"
            "def outer():\n"
            "    client = WebhookClient(url=url)\n"
            "    def inner():\n        client.send(text='POSTED')\n"
            + cell +
            "    client = KafkaProducer(b)\nouter()\n")
    assert _run(full, include_generic=True).commented > 0, label


@pytest.mark.parametrize("cell,label", [
    ("from slack_sdk.webhook import WebhookClient\ndef outer():\n"
     "    client = KafkaProducer(b)\n    def inner():\n        client.send(m)\n"
     "    box.callback = inner\n    box.callback()\nouter()\n",
     "attribute storage, Kafka only"),
    ("def wrapper():\n    def init():\n        global client\n"
     "        client = KafkaProducer(b)\n    init()\n"
     "wrapper()\nclient.send(t, m)\n", "nested initializer binds Kafka"),
    ("from slack_sdk.webhook import WebhookClient\ndef outer():\n"
     "    client = KafkaProducer(b)\n    def inner():\n        client.send(m)\n"
     "    f = inner\n    f()\nouter()\n",
     "a plain alias still suppresses escape"),
])
def test_callee_env_and_container_escape_do_not_over_fire(cell, label):
    assert _run(cell, include_generic=True).commented == 0, label


# ═══════════════════════════════════════════════════════════════════════════
# Blocking findings K1 (source-order callee environment) and K2 (nonlocal).
# ═══════════════════════════════════════════════════════════════════════════

_INITIALIZERS = ("def init():\n    global client\n"
                 "    client = WebhookClient(url=url)\n"
                 "def clear():\n    global client\n    client = None\n")


@pytest.mark.parametrize("cell,label", [
    ("from slack_sdk.webhook import WebhookClient\n" + _INITIALIZERS +
     "def wrapper():\n    f = init\n    f()\n    f = clear\n"
     "wrapper()\nclient.send(text='POSTED')\n",
     "a later alias reassignment must not change an earlier call"),
    ("from slack_sdk.webhook import WebhookClient\n"
     "def wrapper():\n"
     "    def set_client():\n        global client\n"
     "        client = WebhookClient(url=url)\n"
     "    set_client()\n"
     "    def set_client():\n        global client\n        client = None\n"
     "wrapper()\nclient.send(text='POSTED')\n",
     "a later redefinition must not change an earlier call"),
    ("from slack_sdk.webhook import WebhookClient\n"
     "def wrapper():\n    if True:\n        def init():\n"
     "            global client\n            client = WebhookClient(url=url)\n"
     "    init()\nwrapper()\nclient.send(text='POSTED')\n",
     "a definition nested under `if` must be visible"),
])
def test_callee_environment_is_evaluated_in_source_order(cell, label):
    """The callee's environment was built by scanning the whole body first and
    then resolving calls against that FINAL state — not source order — and the
    scan never descended into control flow."""
    assert _run(cell, include_generic=True).commented > 0, label


def test_latest_local_definition_wins_when_call_follows_both_definitions():
    """Python replaces a local function name when the second def executes.

    Keeping the first definition made the analyser apply the clearing effect,
    infer ``client=False`` and let the real WebhookClient send through both the
    cell pass and execution-boundary pass.
    """
    cell = ("from slack_sdk.webhook import WebhookClient\n"
            "def wrapper():\n"
            "    def set_client():\n"
            "        global client\n"
            "        client = None\n"
            "    def set_client():\n"
            "        global client\n"
            "        client = WebhookClient(url=url)\n"
            "    set_client()\n"
            "wrapper()\n"
            "client.send(text='POSTED')\n")
    bindings = new_bindings()
    res = _run(cell, bindings=bindings, include_generic=True)
    assert res.commented > 0
    assert bindings.get("client") is True
    guarded = neutralize_cell(res.source, bindings, include_generic=True)
    assert not _live_calls(guarded.source, {"send"})


def test_latest_local_non_slack_definition_releases_the_name():
    """Fixing latest-definition semantics must work in both directions."""
    cell = ("from slack_sdk.webhook import WebhookClient\n"
            "def wrapper():\n"
            "    def set_client():\n"
            "        global client\n"
            "        client = WebhookClient(url=url)\n"
            "    def set_client():\n"
            "        global client\n"
            "        client = KafkaProducer(b)\n"
            "    set_client()\n"
            "wrapper()\n"
            "client.send(topic, message)\n")
    res = _run(cell, include_generic=True)
    assert res.commented == 0
    assert "client.send(topic, message)" in res.source


@pytest.mark.parametrize("first_branch", ["slack", "kafka"])
def test_conditional_local_redefinitions_union_possible_effects(first_branch):
    """Unknown branches retain every possible active definition; Slack wins."""
    slack_def = ("        def set_client():\n"
                 "            global client\n"
                 "            client = WebhookClient(url=url)\n")
    kafka_def = ("        def set_client():\n"
                 "            global client\n"
                 "            client = KafkaProducer(b)\n")
    left, right = ((slack_def, kafka_def) if first_branch == "slack"
                   else (kafka_def, slack_def))
    cell = ("from slack_sdk.webhook import WebhookClient\n"
            "def wrapper(flag):\n"
            "    if flag:\n" + left +
            "    else:\n" + right +
            "    set_client()\n"
            "wrapper(flag)\n"
            "client.send(text='POSSIBLE_POST')\n")
    assert _run(cell, include_generic=True).commented > 0


def test_nonlocal_rebind_to_a_slack_client_is_seen_by_the_caller():
    """Only `global` was recognised. A nested `bind()` doing
    `nonlocal client; client = WebhookClient(...)` was analysed inside its own
    scope and never propagated back, so the enclosing `client.send(...)` ran
    against a stale Kafka binding and posted for real.
    """
    cell = ("from slack_sdk.webhook import WebhookClient\n"
            "def outer():\n    client = KafkaProducer(b)\n"
            "    def bind():\n        nonlocal client\n"
            "        client = WebhookClient(url=url)\n"
            "    bind()\n    client.send(text='POSTED')\nouter()\n")
    assert _run(cell, include_generic=True).commented > 0


@pytest.mark.parametrize("cell,label", [
    ("from slack_sdk.webhook import WebhookClient\n"
     "def outer():\n    client = WebhookClient(url=url)\n"
     "    def bind():\n        nonlocal client\n        client = KafkaProducer(b)\n"
     "    bind()\n    client.send(m)\nouter()\n",
     "nonlocal rebind AWAY from Slack — the send is Kafka's"),
    ("from slack_sdk.webhook import WebhookClient\n"
     "def outer():\n    client = KafkaProducer(b)\n"
     "    def bind():\n        nonlocal client\n"
     "        client = WebhookClient(url=url)\n"
     "    client.send(m)\nouter()\n",
     "binder never called"),
])
def test_nonlocal_effects_do_not_over_fire(cell, label):
    """The inverse direction was wrong too: without nonlocal tracking the
    rebind away from Slack was invisible, so a harmless Kafka send was
    commented out of the delivered notebook."""
    assert _run(cell, include_generic=True).commented == 0, label
