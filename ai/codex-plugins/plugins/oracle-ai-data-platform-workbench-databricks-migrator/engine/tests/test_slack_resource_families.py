"""_SLACK_RESOURCES is safety-critical for an UNCERTAIN receiver, so pin it.

DEFECT (found in review of #174, reproduced 2026-10-06 against both
origin/the dev branch and this branch's head):

Requiring method evidence for a non-conclusive receiver -- the fix for the
cell-66 false positive -- made the completeness of `_SLACK_RESOURCES` the only
thing between a real Slack write and the customer's workspace. The set was
hand-written and missing ELEVEN of the SDK's forty families, so every method in
those families executed for real:

    if prod:                                # both branches conditional, so
        client = WebClient(token=t)         # _bind's fail-safe keeps the Slack
    else:                                   # verdict and marks `client` stale
        client = Mock()
    client.canvases_create(title="x")       # -> SEND EXECUTED

    the dev branch : COMMENTED
    this head: left running, and executing it delivers

Measured escapes, 12 of 15 probed methods, execution-detected (a client whose
every method raises if reached): canvases_create / canvases_edit /
canvases_delete / canvases_access_set / assistant_threads_setStatus /
slackLists_create / slackLists_items_create / entity_presentDetails /
channels_create / groups_create / im_open / mpim_open. `canvases_delete`
destroys customer content; `im_open` and `*_create` are writes to the
customer's workspace.

TWO reachable binding shapes produce the uncertain state, not one. The review
reported if/else; `try: client = WebClient(...) / except: client = None` is the
commoner shape in real notebooks and escapes identically. A DEFINITE binding
(`client = WebClient(token=t)`) is conclusive and was never affected -- which
is why earlier probes of `canvases_delete` showed it correctly blocked and the
gap stayed hidden.

WHY A TEST AND NOT JUST A LONGER LIST

The set is a snapshot of a vendor namespace that grows -- canvases, assistant
and slackLists are all recent additions, and a stale snapshot fails OPEN on
this path. The list is now generated from `dir(WebClient)` rather than written
by hand, and these tests pin it two ways: one real method per family is
asserted literally (so CI covers it with no SDK installed), and every method
the installed SDK exposes is run through the gate when importable (so a
version bump that adds a family fails here rather than in a customer's
workspace).

NOT EVERY FAMILY IS PREFIX-GATED, AND THAT IS THE POINT

Prefix-gating all forty families overshoots: `validate`, `blocks`, `entity`
and `agents` are words customer code uses for its own helpers, and gating them
by prefix neutralised `validate_schema(...)` on an uncertain receiver -- a new
false positive, i.e. exactly the class of break the method gate exists to
prevent. Those four are matched by full method name (or by the longer
`agents_conversations_` prefix, which covers all 11 of their methods), so both
directions are pinned here: every real Slack method is claimed, and a list of
plausible lookalikes is not.
"""
import os
import sys

import pytest

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_DIR, "scripts"))

from slack_ast_neutralize import (  # noqa: E402
    _SLACK_METHOD_SHAPE_RE,
    _is_slack_namespace_method,
    neutralize_cell,
    new_bindings,
    update_bindings,
)

# The contract is per METHOD, not per family: four of the forty families are
# deliberately matched by full name instead of prefix, because `validate_*`,
# `blocks_*`, `entity_*` and `agents_*` are words customer code uses for its
# own helpers. Pinning families would therefore assert the wrong thing.
#
# One real method per family, alphabetically first, generated from
# slack_sdk 3.45.0 (334 methods, 40 families):
#   sorted({sorted(g)[0] for g in <methods grouped by m.split("_", 1)[0]>})
# over `dir(WebClient)` filtered to `"_" in m`, `not m.startswith("_")` and
# `callable(getattr(WebClient, m))`. The callable filter matters: WebClient
# carries a BASE_URL class attribute, which otherwise appears as a "BASE"
# family.
SDK_METHOD_PER_FAMILY_3_45_0 = (
    "admin_analytics_getFile", "agents_conversations_archive", "api_call",
    "apps_connections_open", "assistant_threads_setStatus", "auth_revoke",
    "blocks_validate", "bookmarks_add", "bots_info", "calls_add",
    "canvases_access_delete", "channels_archive", "chat_appendStream",
    "conversations_acceptSharedInvite", "dialog_open", "dnd_endDnd",
    "emoji_list", "entity_presentDetails", "files_comments_delete",
    "functions_completeError", "groups_archive", "im_close",
    "migration_exchange", "mpim_close", "oauth_access",
    "openid_connect_token", "pins_add", "reactions_add", "reminders_add",
    "rtm_connect", "search_all", "slackLists_access_delete", "stars_add",
    "team_accessLogs", "tooling_tokens_rotate", "usergroups_create",
    "users_conversations", "validate_slack_signature", "views_open",
    "workflows_featured_add",
)

# Non-Slack methods whose names brush against a Slack family. These are the
# cost side of the fix: every one of them would be neutralised on an uncertain
# receiver if the corresponding family were prefix-gated, breaking a real
# migration. `validate_schema` is not hypothetical -- it is what caught the
# first version of this change.
FOREIGN_LOOKALIKES = (
    "validate_schema", "validate_data", "validate_input",
    "blocks_count", "blocks_list",
    "entity_resolution", "entity_map",
    "agents_run", "agents_list", "agents_invoke",
)

# Methods from the families absent from the hand-written set. Every name here
# is a REAL slack_sdk 3.45.0 method -- an invented one would pin a contract
# that does not exist. Every one of these DELIVERED before this fix.
# `validate_slack_signature` is the one method of a missing family that is NOT
# listed: it is a local signature check with no side effect, so it belongs in
# the recognition pin but not in an escape test.
PREVIOUSLY_ESCAPING = [
    ("canvases", 'canvases_create(title="x")'),
    ("canvases", 'canvases_delete(canvas_id="c")'),
    ("canvases", 'canvases_edit(canvas_id="c", changes=[])'),
    ("canvases", 'canvases_access_set(canvas_id="c", access_level="read")'),
    ("assistant", 'assistant_threads_setStatus(status="typing")'),
    ("assistant", 'assistant_threads_setTitle(title="t")'),
    ("slackLists", 'slackLists_create(name="n")'),
    ("slackLists", 'slackLists_items_create(list_id="i")'),
    ("entity", 'entity_presentDetails(trigger_id="x")'),
    ("agents", 'agents_conversations_create(name="n")'),
    ("agents", 'agents_conversations_archive(channel="C1")'),
    ("agents", 'agents_sessions_setStatus(status="x")'),
    ("agents", 'agents_sessions_rename(name="n")'),
    ("blocks", 'blocks_validate(blocks=[])'),
    ("channels", 'channels_create(name="n")'),
    ("groups", 'groups_create(name="n")'),
    ("im", 'im_open(user="U1")'),
    ("mpim", 'mpim_open(users="U1,U2")'),
]

# Both shapes that leave a Slack binding believed-but-contradicted. Each
# assigns in every branch, so BOTH assignments are conditional: the Slack
# verdict survives via _bind's fail-safe and the name is marked stale.
UNCERTAIN_BINDERS = {
    "if/else": (
        "if prod:\n"
        "    client = WebClient(token=t)\n"
        "else:\n"
        "    client = Mock()\n"
    ),
    "try/except": (
        "try:\n"
        "    client = WebClient(token=t)\n"
        "except Exception:\n"
        "    client = None\n"
    ),
}

# A client that cannot be called without us noticing. Mirrors the real failure
# mode: the question is never "was a line commented" but "did a message go".
EXEC_PRELUDE = """
class _Sent(Exception):
    pass


class _TripwireClient:
    def __getattr__(self, name):
        def _call(*a, **k):
            raise _Sent(name)
        return _call


def WebClient(**kwargs):
    return _TripwireClient()


class Mock:
    def __getattr__(self, name):
        return lambda *a, **k: None


prod = True
t = "TOKEN_PLACEHOLDER"
"""


def _uncertain_bindings(binder):
    bindings = new_bindings()
    update_bindings(binder, bindings)
    return bindings


def _sends(binder, call):
    """Execute binder + call through the pipeline. True iff a send happened.

    The binder cell is run through the pipeline too, exactly as the migrator
    does -- running only the call cell leaves `client` unbound, which masks an
    escape as a NameError. That harness error hid these twelve escapes twice.
    """
    bindings = new_bindings()
    bound = neutralize_cell(binder, bindings, include_generic=True)
    update_bindings(binder, bindings)
    sent = neutralize_cell("r = client.%s\n" % call, bindings,
                           include_generic=True)
    namespace = {}
    exec(compile(EXEC_PRELUDE, "<prelude>", "exec"), namespace)
    try:
        exec(compile(bound.source, "<binder>", "exec"), namespace)
        exec(compile(sent.source, "<call>", "exec"), namespace)
    except namespace["_Sent"]:
        return True
    except Exception:
        return False
    return False


# ─── the snapshot ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("method", SDK_METHOD_PER_FAMILY_3_45_0)
def test_every_sdk_family_is_recognised(method):
    """The literal pin: holds with no SDK installed, so CI always runs it.

    Parametrised per method so a regression names the family that came back.
    """
    assert _is_slack_namespace_method(method), method


# The four non-prefix-gated families, in FULL. One-method-per-family coverage
# is not enough here: `agents` splits across two sub-resources, and a first
# version of this fix gated only `agents_conversations_`, leaving
# `agents_sessions_rename` and `agents_sessions_setStatus` executable. Only the
# SDK drift test caught it, so the full list is pinned literally too.
NON_PREFIX_GATED_METHODS = (
    "agents_conversations_archive", "agents_conversations_create",
    "agents_conversations_getCanvas", "agents_conversations_listViews",
    "agents_conversations_removeView",
    "agents_conversations_setCanvasContent",
    "agents_conversations_setCommands",
    "agents_conversations_setProperties", "agents_conversations_setView",
    "agents_sessions_rename", "agents_sessions_setStatus",
    "blocks_validate", "entity_presentDetails", "validate_slack_signature",
)


@pytest.mark.parametrize("method", NON_PREFIX_GATED_METHODS)
def test_every_non_prefix_gated_method_is_recognised(method):
    assert _is_slack_namespace_method(method), method


@pytest.mark.parametrize("method", FOREIGN_LOOKALIKES)
def test_foreign_lookalikes_are_not_claimed(method):
    """The other half of the contract: closing the escape must not start
    neutralising customer helpers that merely share a prefix."""
    assert not _is_slack_namespace_method(method), method


@pytest.mark.parametrize("family,call", PREVIOUSLY_ESCAPING)
def test_previously_escaping_methods_are_in_slacks_namespace(family, call):
    method = call.split("(")[0]
    assert _is_slack_namespace_method(method), method


# ─── drift against the installed SDK ────────────────────────────────────────

def test_installed_sdk_exposes_no_unrecognised_method():
    """The part that catches a vendor addition after an SDK bump.

    Stronger than the literal pin: it asserts the gate over ALL 334 methods,
    not one per family. Env-gated because slack_sdk is deliberately NOT a
    dependency of this toolkit -- the migrator analyses notebooks that import
    it, it never calls Slack itself. The literal pin above is what gives
    unconditional coverage.
    """
    web = pytest.importorskip(
        "slack_sdk.web.client",
        reason="slack_sdk is not a toolkit dependency; the literal snapshot "
               "in SDK_METHOD_PER_FAMILY_3_45_0 pins the same contract "
               "without it",
    )
    methods = [name for name in dir(web.WebClient)
               if "_" in name and not name.startswith("_")
               and callable(getattr(web.WebClient, name))]
    assert methods, "introspection found no methods — filter is wrong"
    unrecognised = sorted(m for m in methods
                          if not _is_slack_namespace_method(m))
    assert not unrecognised, (
        "the installed slack_sdk exposes %d method(s) the guard does not "
        "recognise: %s. On an uncertain receiver these execute for real -- "
        "regenerate _SLACK_RESOURCES from dir(WebClient) and decide whether "
        "any new family needs full-name matching instead of a prefix."
        % (len(unrecognised), unrecognised))


# ─── the escapes themselves ─────────────────────────────────────────────────

@pytest.mark.parametrize("shape", sorted(UNCERTAIN_BINDERS))
@pytest.mark.parametrize("family,call", PREVIOUSLY_ESCAPING)
def test_no_previously_escaping_method_delivers(shape, family, call):
    """The regression this file exists for, asserted by EXECUTION."""
    assert not _sends(UNCERTAIN_BINDERS[shape], call), (
        "%s on a %s-bound client delivered" % (call.split("(")[0], shape))


@pytest.mark.parametrize("shape", sorted(UNCERTAIN_BINDERS))
@pytest.mark.parametrize("call", [
    'chat_postMessage(channel="#c", text="x")',
    'files_upload_v2(channel="C1", content="x")',
    'reactions_add(channel="C1", name="x", timestamp="1")',
])
def test_families_that_already_worked_still_do_not_deliver(shape, call):
    """Controls: these were blocked before and must stay blocked."""
    assert not _sends(UNCERTAIN_BINDERS[shape], call)


@pytest.mark.parametrize("shape", sorted(UNCERTAIN_BINDERS))
def test_both_shapes_really_do_produce_an_uncertain_binding(shape):
    """If this stops holding, the tests above pass for the wrong reason."""
    bindings = _uncertain_bindings(UNCERTAIN_BINDERS[shape])
    assert bindings.is_slack("client")
    assert not bindings.is_weak("client")
    assert bindings.is_uncertain("client")


# ─── what must NOT regress: the cell-66 false positive ──────────────────────

@pytest.mark.parametrize("shape", sorted(UNCERTAIN_BINDERS))
@pytest.mark.parametrize("method", [
    "download_artifacts", "get_model_version_by_alias", "get_latest_versions",
])
def test_non_slack_methods_still_run_on_an_uncertain_receiver(shape, method):
    """Completing the family set must not re-break the incident it fixed.

    None of these is in any Slack family, so an uncertain receiver still
    executes them -- which is the whole point of the method gate.
    """
    assert not _is_slack_namespace_method(method)
    bindings = _uncertain_bindings(UNCERTAIN_BINDERS[shape])
    result = neutralize_cell("v = client.%s('m')\n" % method, bindings,
                             include_generic=True)
    assert not result.commented, "%s was re-neutralised" % method


# ─── the shape regex, same contract ─────────────────────────────────────────

def test_camelcase_family_matches_the_method_shape():
    """`slackLists_*` failed the old initial-lowercase-only resource pattern,
    so the family was unreportable on an unresolvable receiver even once it
    was added to _SLACK_RESOURCES."""
    for method in ("slackLists_create", "slackLists_items_list",
                   "chat_postMessage", "files_upload_v2"):
        assert _SLACK_METHOD_SHAPE_RE.match(method), method


def test_shape_regex_still_rejects_plain_and_dunder_names():
    for method in ("get", "strip", "now", "cast", "createOrReplaceTempView"):
        assert not _SLACK_METHOD_SHAPE_RE.match(method), method


def test_indirect_receiver_reports_a_camelcase_family():
    bindings = new_bindings()
    result = neutralize_cell(
        'clients["slack"].slackLists_create(name="n")\n',
        bindings, include_generic=True)
    kinds = {kind for kind, _msg, _line in (result.unprovable or [])}
    assert "indirect-receiver" in kinds
