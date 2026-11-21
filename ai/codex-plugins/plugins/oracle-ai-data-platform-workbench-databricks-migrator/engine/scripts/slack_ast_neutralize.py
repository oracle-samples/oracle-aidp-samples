"""AST-based neutralisation of Slack / external-post statements in notebook cells.

WHY THIS REPLACES THE REGEX PATH
--------------------------------
`_neutralize_slack_execution` in job_migrate.py comments out lines matched by an
ALLOWLIST OF ~8 METHOD-NAME PATTERNS. Measured against the live code on
2026-09-07:

    files_upload_v2   -> commented=0    <-- posted for real on 2026-09-03
    files_upload      -> commented=0
    chat_update       -> commented=0
    chat_postMessage  -> commented=1    (the only covered one)

Slack's Web API has ~200 methods. An allowlist cannot close that, which is why
the fix history reads 7o, 7o', 7o'', 7o''' — four rounds, each after an
incident, and it still let the file upload through.

Its FALSE-POSITIVE protection, by contrast, works and was hard-won (verified:
pure Kafka, mixed slack+Kafka, `sock.send`, `smtp.send` are all correctly left
alone). This module keeps that property and closes the false negatives.

WHAT CHANGES
------------
Detection moves from *method name* to *receiver identity*:

    if a name is bound to a Slack client constructor, then ANY method call on
    it is a post — whatever the method is called.

Bindings are tracked THREE ways, and the distinction is load-bearing:

    True    bound to a Slack client
    False   assigned something visible, with no Slack evidence
    absent  never assigned anywhere visible

`False` blocks the receiver-name hint tier, which is what keeps
`webhook_producer = KafkaProducer(b); webhook_producer.send(topic, msg)` from
being commented (the regex path commented it — measured). `absent` still
allows that tier, which is how a client arriving from a `%run` child or a
function parameter is caught. Collapsing the two into a plain set is what
dropped the `slack_client = get_slack_client()` class of coverage in the first
version of this module.

Where a name hint IS used, it is anchored on the FACTORY name rather than the
receiver name, because the factory is evidence about the object:

    slack_client = get_slack_client()   -> Slack (weak)
    webhook_producer = KafkaProducer(b) -> not Slack

A "weak" binding (name hint, not a real constructor) only has recognised WRITE
verbs commented on it. Treating it as a full client commented `cfg.get(...)`
on `cfg = get_slack_config()`, which returns a dict.

Bindings are applied in SOURCE ORDER and only from MODULE SCOPE. Both matter:
`ast.walk` order is not source order, so applying an add and a discard
order-insensitively let `client = WebClient(t); client.files_upload_v2(...);
client = None` through; and a never-called def-local binding re-adding a name
after a module-level Kafka rebind commented the Kafka send.

Editing moves from *lines* to *statements*, which structurally removes three
historical bugs: dangling multi-line args (SyntaxError), empty blocks
(IndentationError, previously repaired by Opus), and `# # #` accumulation
across runs.

COMMENTS AND STRING LITERALS PULL IN OPPOSITE DIRECTIONS
--------------------------------------------------------
An earlier version of this docstring claimed matches inside comments and string
literals were "structurally removed". That was false as written, and the two
cases need opposite treatment:

  * Comments are masked for every tier-1 pattern. A comment cannot execute, so
    `rows = df.count()  # used to post to hooks.slack.com` must stay live.
  * String literals are masked for CODE patterns only (`.chat_postMessage(`,
    `send_slack(`) — those cannot execute from inside a string, so matching a
    docstring or a log message breaks working code.
  * String literals are NOT masked for `hooks.slack.com`, because
    `requests.post("https://hooks.slack.com/...")` is a real post. Masking
    there would convert a false positive into a false negative.

A code pattern found only inside a string literal is REPORTED, not commented
(`eval()` of that string would still post).

WHAT IT STILL CANNOT PROVE
--------------------------
Static analysis has limits. These are DETECTED AND REPORTED rather than
silently missed — the difference between a known gap and the last incident:

  * getattr(client, name)()            — dynamic dispatch
  * @slack_sender                      — decorator, posts on entry/exit
  * self.client.post(), d["c"].post()  — receiver is not a plain Name
  * a client arriving as a function parameter
  * URLs read from config/secrets at runtime
  * a call used BEFORE the cell binds the name (NameError on a fresh kernel,
    but it can post on a re-run in a live kernel)
  * a cell that does not parse — the caller falls back to the regex path,
    which does not cover the files_upload_v2 family

Reporting is gated on the CELL's Slack evidence and on Slack's resource
namespace (`files_*`, `chat_*`, `conversations_*`, ...), which is a closed set.
Gating on the call statement's own text instead hid real leaks —
`bot.client.files_upload_v2(...)` reported nothing at all. Gating on method
SHAPE alone flooded the report with pandas calls (`to_dict`, `sort_values`,
`reset_index`) in any cell that merely imported slack.

Nothing is deleted. The original statement stays in the cell as a comment.
"""
from __future__ import annotations

import ast
import re
import textwrap
from typing import Dict, List, Optional, Set, Tuple

__all__ = [
    "NeutralizeResult", "neutralize_cell", "update_bindings", "new_bindings",
    "SLACK_CTORS",
]

# Constructors whose instances hit the Slack network. A name bound to one of
# these is a Slack client regardless of what it is called.
SLACK_CTORS: Set[str] = {
    "WebClient", "AsyncWebClient",
    "WebhookClient", "AsyncWebhookClient",
    "SlackClient", "Slack", "SlackWebClient",
}

ANNOTATION = "  # Oracle tool modification: slack execution neutralised (statement)"
# Matching the whole "Oracle tool modification" family was too broad: the tool
# writes those annotations for DBFS path rewrites, stripped pip installs and
# more. One of them landed INSIDE a multi-line upload call --
#     client.files_upload_v2(
#         # Oracle tool modification: rewrote DBFS path
#         file=f,
#     )
# -- and the whole statement was treated as already neutralised, so the upload
# executed (measured: commented=0, no finding). Only the neutralisation markers
# this module writes count.
_NEUTRALISED_MARKER_RE = re.compile(
    r"Oracle tool modification: (?:slack execution neutralised"
    r"|block body neutralised)")
PRESERVED_ANNOTATION = ("  # Oracle tool modification: preserved from a line "
                        "whose Slack call was neutralised")
PASS_ANNOTATION = ("  # Oracle tool modification: block body neutralised; "
                   "`pass` keeps the block valid")
BINDING_ANNOTATION = ("  # Oracle tool modification: Slack send neutralised; "
                      "NO message was delivered. This stand-in reports "
                      "success so the notebook follows its normal path — see "
                      "the migration report")

# ─── Preserving the binding of a neutralised assignment ─────────────────────
# Commenting `result = client.chat_postMessage(...)` removes the STATEMENT but
# leaves the DATA DEPENDENCY live, so `logger.info(result)` below it raises
# `NameError: name 'result' is not defined`. The classification is correct --
# it IS a Slack send -- so the defect is the edit, not the decision, and no
# amount of classifier accuracy fixes it.
#
# The same hazard is already handled one line over: a semicolon-shared
# `x = compute(); client.files_upload_v2(...)` re-emits `x = compute()` above
# the comment (see PRESERVED_ANNOTATION). This applies that idea to the
# assignment's own target.
#
# The stub is a SELF-SIMILAR NULL OBJECT rather than None or {}: a Slack
# response is consumed as `r["ts"]`, `r.get("ok")`, `r.data`, and nested
# `r["file"]["id"]`, so None raises TypeError and {} raises KeyError -- moving
# the crash rather than removing it. Returning itself for subscript, attribute
# and call makes every one of those shapes work.
#
# Its PAYLOAD is empty, so `r["ts"]`, `r.json()` and `str(r)` are '' -- the
# object never invents content it does not have. Its SUCCESS SIGNALS are
# fabricated, so `if r.get("ok"):` takes the SENT branch; see _STUB_EXPR for
# why and for the three field groups. An earlier version was falsy throughout
# and took the not-sent branch; that diverted notebooks onto a path production
# never executes and blocked retry/alert conversions outright.
#
# Either way the "no message was delivered" signal lives in
# BINDING_ANNOTATION and in the report, never in the value -- and with success
# fabricated that disclosure is load-bearing rather than merely informative.
_STUB_TEMP_NAME = "_aidp_slack_neutralised"
# `type` and `str` are reached through BUILTINS, not by bare name. They are
# ordinary names, resolved local -> global -> builtins at runtime, so a
# notebook variable called `type` (an everyday name in data code, and a
# plausible widget name) was found first and the stub line raised:
#
#     type = 'daily'   ->  TypeError: 'str' object is not callable
#     str  = 'x'       ->  TypeError: metaclass conflict
#
# That broke cells which ran fine before, including cells whose Slack result
# is never read, and the kernel namespace persists -- so one such assignment
# in any earlier cell or %run child poisoned every later neutralised
# assignment in the notebook.
#
# `_b` is bound once by a lambda rather than assigned, so the fix adds no
# second name to the customer's namespace and imports only once.
#
# __getattr__ returns self only for ORDINARY names. For a __dunder__ it defers
# to __getattribute__, which raises AttributeError properly -- otherwise the
# stub claimed to have every dunder, so `hasattr(r, '__UDT__')` was True
# (PySpark probes that during schema inference) and pickling failed. There is
# no recursion: the implicit __getattr__ fallback lives in the attribute
# protocol, not inside __getattribute__.
#
# __reduce__ degrades the stub to a plain '' across a process boundary, so it
# survives being pickled into a Spark worker.
# The stub REPORTS SUCCESS. ACCEPTED, with residuals named, on PR #174 by
# the repository owner (review comment, 2026-10-06) -- the
# decision record for this design, required because a cell can pass without
# its notification path having run and an author's own deferral is not an
# owner decision. The accepted residuals are listed at the end of this block.
# Basis given in that ruling: nothing is sent under either design, so the only
# question is which downstream branch gets validated, and the repository's
# conditional-exit doctrine (never validate a branch production does not take)
# cuts for the truthy design. The notebook should
# follow the same path production follows, so a conversion can complete
# instead of being diverted onto a not-sent branch that production never
# executes.
#
# What that buys and what it costs, stated plainly because the cost is real:
#
#   buys  the realistic downstream path runs, so it gets exercised and the
#         cell can complete. A falsy stub sent `if r["ok"]:` down the else
#         branch, and `assert r["ok"]` / `resp.status_code >= 400` raised
#         outright -- validating a branch production never takes.
#
#   costs the cell can PASS without its notification path having been
#         exercised, and any action the success path performs (marking records
#         notified, writing an audit row, dropping a queue entry) runs on a
#         send that never happened. Table writes are redirected to the
#         migration sandbox, so those do not reach production; non-table side
#         effects are not covered by that.
#
# Every emission therefore reports the fabrication (see the binding-preserved
# finding) so a green cell can never be read as "alerting validated". That
# disclosure is the condition that makes this acceptable rather than a silent
# false PASS.
#
# Only SUCCESS-signalling fields are fabricated. Payload fields stay empty and
# self-similar, which keeps the object honest about having no content and
# keeps `r["channel"]["id"]`-style chains working:
#
#   r["ok"] / r.ok / r.data["ok"] -> True       r.status_code -> 200
#   bool(r) -> True                             r["ts"], r.json(), str(r) -> ''
#
# `status_code` is an int so `>= 400` evaluates instead of raising TypeError.
# 200 rather than a 5xx: a 5xx is also a fabrication, and it would divert onto
# the error branch -- the thing this decision is meant to avoid.
#
# Three field groups, because "reports success" is not the same as "answers
# True to everything":
#
#   success flags  ok / success            -> True
#   status codes   status_code / status     -> 200   (ints: `status` is
#                                             aiohttp's field and is compared
#                                             numerically, so True made
#                                             `resp.status != 200` raise)
#   error fields   error / errors / warning -> None  (a real SUCCESSFUL Slack
#                                             response has no error key, so
#                                             returning the truthy self made
#                                             `if r.get("error"): raise` fire
#                                             on a send that "succeeded")
#
# Everything else stays empty and self-similar.
_STUB_EXPR = (
    "(lambda _b: _b.type('AidpNeutralisedSlackResponse', (_b.str,), "
    "{'__getitem__': lambda _s, _k: True if _k in ('ok', 'success') "
    "else (200 if _k in ('status_code', 'status') "
    "else (None if _k in ('error', 'errors', 'warning', 'warnings') "
    "else _s)), "
    "'__getattr__': lambda _s, _n: True if _n in ('ok', 'success') "
    "else (200 if _n in ('status_code', 'status') "
    "else (None if _n in ('error', 'errors', 'warning', 'warnings') "
    "else (_s if not _n.startswith('__') "
    "else _s.__getattribute__(_n)))), "
    "'get': lambda _s, _k, _d=None: _s[_k], "
    "'__bool__': lambda _s: True, "
    "'__call__': lambda _s, *_a, **_k: _s, "
    "'__reduce__': lambda _s: (_b.str, ('',))})())"
    "(__import__('builtins'))"
)
# `get` is a REAL method rather than the self-similar callable, because
# `r.get("error")` otherwise never consults the key at all: `.get` returned
# the stand-in, calling it returned the stand-in, and the stand-in is truthy --
# so `if r.get("error"): raise` fired on a send the object claims succeeded.
# Routing it through __getitem__ gives `get("ok")` -> True, `get("error")` ->
# None and `get("ts")` -> '' consistently with subscripting.


# A retry loop and a success-reporting stand-in.
#
# The stand-in was originally FALSY, and that made a retry loop
# non-terminating:
#
#     ok = False
#     while not ok:
#         r = client.chat_postMessage(...)   # commented; no-op
#         ok = r["ok"]                        # falsy stub -> never satisfied
#
# Six shapes hung. The response then was to withhold the stand-in from any
# cell containing a `while`, so those cells failed loudly with NameError.
#
# Reporting success removes the mechanism rather than the symptom: anything
# derived from the stand-in is truthy, so eight of the nine measured retry
# shapes now terminate on the first iteration exactly as production does --
# `r["ok"]`, `r.get("ok")`, `resp.ok`, `status_code == 200`, `while True` with
# `if r["ok"]: break`, an unknown key, a payload field, and the poll-outside
# form. Withholding the stand-in would block all of them for a reason that no
# longer applies, which is why the per-cell guard was removed
# (completion-first). The guard's removal is covered by the same decision
# record as _STUB_EXPR -- source-toolkit review, 2026-10-06 -- which accepts
# the literal-comparison hang as a DOCUMENTED-ONLY residual for the reasons
# set out in _RETRY_LOOP_RESIDUAL.
#
# ACCEPTED RESIDUAL, reported per cell rather than guarded against:
# a loop whose exit test compares a derived value against a literal the
# stand-in does not fabricate can still never be satisfied --
#
#     sent = resp.status_code == 201        # 200 == 201 is really False
#     ok   = r["ts"] == "1700000000.1"      # '' == ... is really False
#     ok   = len(resp.text) > 0             # 0 > 0 is really False
#
# and that cell will not terminate: it burns the 14,400 s execution timeout in
# SILENCE, which this project's runbook warns is indistinguishable from
# healthy progress, and the WebSocket dies after ~90 s of quiet so it surfaces
# as ConnectionDied and sends the next investigator into the session layer
# after a platform bug that does not exist.
#
# It is REPORTED, not guarded, because the two available guards are both worse
# than the residual. Withholding the stand-in from every `while` cell blocks
# the eight shapes that work. Detecting "compares a stub-derived value against
# an unsatisfiable literal" needs value tracking through copies, containers
# and attributes, and it can fail in BOTH directions -- miss a flow and the
# silent hang returns anyway; over-trigger on a satisfiable literal
# (`== 200`) and a working conversion is blocked. It would also duplicate the
# stand-in's fabricated values into the rule that reasons about them, so the
# two can drift.
#
# Two further residuals that are DOCUMENTED ONLY -- deliberately not reported,
# because neither has been observed and the per-cell `while` test cannot see
# either one, so there is nothing to fire on. Do not describe them as reported:
#   * an unbounded `for` -- `for _ in itertools.count(): if r["ok"]: break`.
#   * a `while` in a LATER cell: cells share a namespace, and this check is
#     per-cell.
_RETRY_LOOP_RESIDUAL = (
    "this cell contains a while loop and a neutralised send. The stand-in "
    "reports success, so a loop that waits for the send to succeed "
    "terminates on its first iteration as it would in production. But if the "
    "exit test compares a derived value against a literal the stand-in does "
    "NOT fabricate — status_code == 201 (it is 200), r['ts'] == '<a ts>' or "
    "len(resp.text) > 0 (both empty) — the condition can never be satisfied "
    "and this cell will NOT TERMINATE: it burns the full execution timeout in "
    "silence and surfaces as a WebSocket ConnectionDied, not as a guard "
    "problem. CHECK THIS LOOP'S EXIT CONDITION before running the task"
)


def _walrus_targets(stmt: ast.stmt,
                    method: str) -> Tuple[List[Tuple[int, str]], List[str]]:
    """(re-bindable, declined) walrus targets inside `stmt`.

    `(r := expr)` assigns expr's value to `r` and also yields it, so a walrus
    target holds the call's RESULT exactly as a plain `r = <call>` does -- it
    is precisely the binding the stand-in exists for. Nothing re-bound these
    before, because the emit loop only inspected STATEMENT-level assignment
    targets, and `print((r := <send>))` has none. So the send was commented,
    `r` was left dangling, the next reader raised NameError, and no finding
    was emitted at all.

    A walrus is only re-bound when its own value is the FLAGGED SEND's call.
    An earlier version asked merely `isinstance(node.value, ast.Call)`, which
    re-opened the exact data loss _value_is_the_flagged_send exists to close,
    one commit after closing it on the statement-level path:

        client.chat_postMessage(channel="#c", text=f"rows={(n := len(rows))}")
        print(n)        base NameError  ->  head '' (silently wrong)

    `n` holds `len(rows)`, not a response. Declined targets are returned
    separately so they are REPORTED rather than silently dropped.

    Also restricted to a Name target: a non-Name cannot be re-bound safely,
    the same reasoning _preservable_binding_text applies to subscript and
    attribute targets.
    """
    rebindable: List[Tuple[int, str]] = []
    declined: List[str] = []
    for node in ast.walk(stmt):
        if not (isinstance(node, ast.NamedExpr)
                and isinstance(node.target, ast.Name)):
            continue
        if _value_is_the_flagged_send(node.value, method):
            rebindable.append((getattr(node, "col_offset",
                                       getattr(stmt, "col_offset", 0)),
                               node.target.id))
        else:
            declined.append(node.target.id)
    return rebindable, declined


_WALRUS_NOT_A_RESPONSE_REASON = (
    "`%s` is assigned by a walrus (`:=`) inside a neutralised statement, but "
    "its value is a %s rather than the send's own result, so no stand-in is "
    "substituted for it and a reader below raises NameError. Re-run that "
    "sub-expression on its own line if the value is still needed"
)


# Plain English for an AST node kind. The audience is a migration operator
# reading a report, not someone reading the grammar, so "a list comprehension"
# rather than "a listcomp".
_VALUE_DESCRIPTIONS = {
    "Dict": "dict", "List": "list", "Tuple": "tuple", "Set": "set",
    "ListComp": "list comprehension", "DictComp": "dict comprehension",
    "SetComp": "set comprehension", "GeneratorExp": "generator expression",
    "BoolOp": "boolean expression", "BinOp": "computed value",
    "Compare": "comparison result", "JoinedStr": "f-string",
    "Subscript": "indexed value", "Attribute": "attribute value",
    "Name": "copy of another variable", "IfExp": "conditional expression",
    "Lambda": "lambda",
}


def _called_name(node) -> Optional[str]:
    """The name being called by a Call node, or None if it is not resolvable."""
    if not isinstance(node, ast.Call):
        return None
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr
    return getattr(func, "id", None)


def _describe_value(node) -> str:
    if node is None:
        return "value"
    if isinstance(node, ast.Constant):
        return ("string" if isinstance(node.value, str)
                else "%s literal" % type(node.value).__name__)
    if isinstance(node, ast.Call):
        name = _called_name(node)
        return "value returned by %s()" % name if name else "returned value"
    return _VALUE_DESCRIPTIONS.get(type(node).__name__,
                                   type(node).__name__.lower())


def _value_is_the_flagged_send(value, method: str) -> bool:
    """Is this assigned value the SEND's own result?

    "The outermost expression is a call" is necessary but NOT sufficient, and
    treating it as sufficient re-opened the data loss it was meant to close.
    A statement commented purely on TEXT evidence very often assigns the
    result of some ORDINARY call, and every one of those got the response
    stand-in (base raises NameError in all of them):

        cfg = dict(webhook="<hook>", table="prod.sales")  -> cfg["table"] ''
        url = os.getenv("SLACK_URL", "<hook>")            -> url '' and the
                                                             POST went to ''
        cfg = load_cfg(webhook="<hook>", ...)             -> cfg["table"] ''
        msg = "... <hook> ...".format(1)                  -> msg ''
        links = sorted(["<hook>", ...])                   -> len(links) 0
        payload = json.dumps({"webhook": "<hook>"})       -> payload ''

    The `os.getenv` shape is the worst: losing the URL turned a suppressed
    send into an outbound request to a new target.

    So judge the OUTERMOST call's own identity. Deliberately not
    `_owner_proves_send_by_call`, which walks the whole statement and would
    therefore accept `x = wrap(client.chat_postMessage(...))` -- there `x` is
    wrap's return value, not the response, which is the
    _VALUE_NOT_THE_RESPONSE_REASON case.
    """
    name = _called_name(value)
    if not name:
        return False
    return name == method or _is_slack_namespace_method(name)


# The statement DID call Slack, but the value it assigns is an expression built
# AROUND the response rather than the response itself. Distinct from
# _NOT_A_RESPONSE_REASON, which is for a statement commented purely on TEXT
# evidence and never called Slack at all. One message served both, so
#     ok = (r := <send>) and True
# was reported as "commented because a Slack address appears in its TEXT, not
# because it calls Slack" -- plainly false, the statement is a send.
_VALUE_NOT_THE_RESPONSE_REASON = (
    "this statement DOES call Slack, but the value assigned is a %s built "
    "around the response rather than the response itself, so no stand-in is "
    "substituted for it and a reader below raises NameError. Any walrus "
    "target inside the statement IS re-bound"
)


def _assigned_value_expr(stmt: ast.stmt):
    """The expression whose value the assignment produces, or None.

    `await` is unwrapped so an AsyncWebClient send still reads as a call.
    """
    value = getattr(stmt, "value", None)
    while isinstance(value, ast.Await):
        value = value.value
    return value


# The stub stands in for a Slack RESPONSE, and the tool comments a statement
# for TWO different reasons -- only one of which produces a response:
#
#   call evidence  `r = client.chat_postMessage(...)`
#                  the value lost IS Slack's reply. An inert reply is a
#                  coherent stand-in. This is what the stub was built for.
#
#   TEXT evidence  `cfg = {"webhook": "...hooks.slack.com...",
#                          "threshold": 0.5, "table": "prod.sales"}`
#                  the statement never calls Slack; it is commented because a
#                  Slack address appears in a string literal (which is how
#                  webhook sends are caught at all). The value lost is the
#                  CUSTOMER'S OWN data -- a config dict, a message, a list of
#                  links. There is no "inert response" that stands in for a
#                  config dict.
#
# Stubbing the second class replaced real data with an empty value and let the
# notebook continue:
#     cfg  -> stub, so spark.read.table(cfg["table"]) reads table ''
#     links -> stub, so len(links) == 0 and a loop over them does nothing
# base the dev branch raised NameError for all of these. Turning that loud failure
# into silent wrong data is the one outcome this repository forbids outright,
# and it was introduced by the stub, not inherited.
_NOT_A_RESPONSE_REASON = (
    "the statement was commented because a Slack address appears in its TEXT, "
    "not because it calls Slack — the value it produced was the customer's "
    "own %s, never a Slack response, and an inert response stub is not a "
    "substitute for it. The binding is deliberately left unset so the cell "
    "fails loudly instead of continuing with empty values"
)


_NOT_THE_SENDER_REASON = (
    "the statement shares a physical line with a Slack call and spans several "
    "lines, so its own column slice could not be read; it was kept as a "
    "target fail-closed (never drop a genuine send) but it does not contain "
    "the call it was flagged for, so its value was never a Slack response. "
    "The binding is deliberately left unset so the cell fails loudly instead "
    "of continuing with an empty value"
)


def _preservable_binding_text(stmt: ast.stmt):
    """Assignment target text that can be re-bound to the stub, or a reason.

    Returns (list_of_target_texts, None) or (None, reason_string).

    Only plain names and flat name tuples/lists are handled. A subscript or
    attribute target (`d["k"] = ...`, `self.r = ...`) is NOT re-bound: the
    container may itself be gone or unset, so writing into it could raise or
    mutate unrelated state. Augmented assignment is excluded because the name
    must already exist and overwriting it with an empty stub would discard a
    real accumulated value. Both are reported instead.

    Each reason states the CONSEQUENCE for that target kind, because they
    differ and a single wording was wrong for half of them. Measured:

        augmented assignment, name already set -> nothing raises; the reader
                                                  silently sees a STALE value
        augmented assignment, name never set   -> NameError
        subscript, container exists            -> KeyError / IndexError
        subscript, container unset             -> NameError
        attribute target                       -> AttributeError
        starred unpacking                      -> NameError

    The augmented-assignment case matters most: the old wording promised a
    loud NameError where the real outcome is a quiet wrong value.
    """
    # Augmented assignment is checked FIRST so it keeps its own consequence
    # wording (a stale value, with nothing raising) rather than the generic
    # not-a-response one.
    if isinstance(stmt, ast.AugAssign):
        return None, ("augmented assignment — the name already holds a real "
                      "value that a stub would discard, so a reader below "
                      "sees the STALE pre-existing value and nothing raises "
                      "(NameError only if the name was never set)")

    # The "value must be a call" rule lives in the CALLER, not here: choosing
    # between the two possible explanations for declining a stand-in needs to
    # know whether the statement actually called Slack, which needs the
    # flagged method and the source lines. This function only sees the
    # statement. See _NOT_A_RESPONSE_REASON / _VALUE_NOT_THE_RESPONSE_REASON.
    if isinstance(stmt, ast.AnnAssign):
        targets = [stmt.target]
    elif isinstance(stmt, ast.Assign):
        targets = list(stmt.targets)
    else:
        return None, None            # not an assignment; nothing to preserve

    texts = []
    for target in targets:
        if isinstance(target, ast.Name):
            texts.append(target.id)
        elif isinstance(target, (ast.Tuple, ast.List)):
            # Arity is statically known from the target, which is what makes
            # `ok, info = send_slack(...)` preservable at all.
            if not all(isinstance(e, ast.Name) for e in target.elts):
                return None, ("unpacking target contains a starred or "
                              "non-name element, so a reader below raises "
                              "NameError")
            if not target.elts:
                return None, ("empty unpacking target, so a reader below "
                              "raises NameError")
            names = [e.id for e in target.elts]
            texts.append(", ".join(names))
        elif isinstance(target, ast.Attribute):
            return None, ("attribute target — the attribute is not set, so a "
                          "reader below raises AttributeError (NameError if "
                          "the object itself is unset)")
        elif isinstance(target, ast.Subscript):
            return None, ("subscript target — the container is not written "
                          "to, so a reader below raises KeyError or "
                          "IndexError (NameError if the container itself is "
                          "unset)")
        else:
            return None, (f"{type(target).__name__.lower()} target — not "
                          "re-bound, so a reader below raises NameError")
    return (texts, None) if texts else (None, None)


def _names_read_after(tree: ast.AST, lineno: int) -> Set[str]:
    """Names LOADed after `lineno` in this cell.

    Diagnostics only. It deliberately does NOT gate whether the stub is
    emitted: neutralize_cell sees ONE cell, so a consumer in a later cell is
    invisible, and gating on this would miss exactly the cross-cell case that
    makes the bug hard to spot. An unused stub is a harmless dead assignment,
    so the stub is unconditional and this only enriches the finding.
    """
    return {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name)
        and isinstance(node.ctx, ast.Load)
        and getattr(node, "lineno", 0) > lineno
    }

# ─── Read vs write ───────────────────────────────────────────────────────────
# Neutralisation exists to stop outbound SIDE EFFECTS. A read has none, and
# commenting one turns working code into a NameError:
#
#     resp = client.conversations_history(channel='C1')   # commented
#     msgs = resp['messages']                             # NameError
#
# The regex path never touched reads (they were not in its allowlist), so
# commenting them would be a NEW regression. Reads are therefore left alone.
#
# Slack method names are `resource_action`. Classify on the action.
_READ_ONLY_RE = re.compile(
    r"""
    _(?:list|info|history|replies|members|identity|permalink|prefs|counts)$
    |_get$                          # reactions_get, users_profile_get
    |_(?:get|lookup|list)[A-Z]     # chat_getPermalink, listConnectInvites
    |^search_
    |^(?:api_test|auth_test)$
    |(?:Info|Logs)$                 # team_billableInfo, team_accessLogs
    |^(?:users_conversations|users_getPresence|dnd_info|bots_info)$
    """,
    re.VERBOSE,
)

# Actions that unambiguously write. Used only to decide whether a commented
# call is CONFIDENTLY a write or merely UNCLASSIFIED — an unclassified one is
# still commented (fail safe) but is surfaced in the report so a
# misclassified read shows up as a finding rather than a mystery NameError.
_KNOWN_WRITE_RE = re.compile(
    r"""
    post|upload|update|delete|create|invite|join|leave|kick|archive
    |unarchive|rename|set[A-Z]|add|remove|revoke|schedule|open|publish
    |push|close|mark|complete|approve|restrict|send|meMessage|api_call
    """,
    re.VERBOSE | re.IGNORECASE,
)


def _is_read_only(method: str) -> bool:
    """True for Slack API methods that only fetch data.

    Fail-safe direction: anything NOT matching is treated as a write. A
    misclassified read breaks loudly (NameError, one-line fix); a
    misclassified write posts silently — which is the incident we are fixing.
    """
    return bool(method and _READ_ONLY_RE.search(method))

# Unambiguously Slack even without a visible binding. Retained from the current
# implementation (tier-1) because it covers cases receiver-tracking cannot see:
# customer wrapper CALLS, raw webhook URLs, and chained construction.
# Tier-1 splits in two, because the two halves behave OPPOSITELY inside a
# string literal:
#
#   CODE patterns (`.chat_postMessage(`, `send_slack(`) cannot execute from
#   inside a string, so matching one there comments working code. A docstring
#   reading "call client.chat_postMessage() to post" was commented out.
#
#   TEXT patterns (`hooks.slack.com`) are usually ONLY ever inside a string --
#   `requests.post("https://hooks.slack.com/...")` is a real post. Masking
#   literals here would turn a false positive into a false negative.
#
# So: mask comments for both, mask string literals for CODE patterns only.
_TIER1_CODE_RE = re.compile(
    r"""
    \.chat_postMessage\s*\(
    |\.chat_postmessage\s*\(
    |WebhookClient\s*\([^)]*\)\s*\.
    |\bsend_slack\s*\(
    |\bslack_alert\s*\(
    |\b(?:send|post|push|publish|notify|alert|deliver|dispatch|broadcast
        |message|msg|write|ping|report|chat)\w*slack\w*\s*\(
    |\bslack_(?:send|post|push|publish|notify|alert|deliver|dispatch|broadcast
        |notification|message|msg|write|ping|report|chat|call|api)\s*\(
    """,
    re.VERBOSE,
)
_TIER1_TEXT_RE = re.compile(r"hooks\.slack\.com")

# Union, kept for the report gate (a statement already commented by tier-1
# must not also be reported as an unresolvable receiver).
_TIER1_RE = re.compile(
    _TIER1_CODE_RE.pattern + "|" + _TIER1_TEXT_RE.pattern, re.VERBOSE)

# Slack's Web API resource namespace. ~200 methods, but the RESOURCE prefix is
# a closed set, which makes it a far tighter report gate than method shape
# alone: `files_upload_v2` matches, while `to_dict` / `sort_values` /
# `reset_index` (real pandas calls in real notebooks) do not.
#
# PROVENANCE: generated from the installed SDK, not written by hand --
#   sorted({m.split("_", 1)[0] for m in dir(slack_sdk.web.client.WebClient)
#           if "_" in m and not m.startswith("_")
#           and callable(getattr(WebClient, m))})
# Keep the callable filter -- WebClient has a BASE_URL class attribute, which
# otherwise lands here as a bogus "BASE" family.
# slack_sdk 3.45.0 -> 334 methods, 40 families; 36 are prefix-gated here and
# the other 4 are handled by name just below. Regenerate with that one-liner
# when bumping the SDK. tests/test_slack_resource_families.py asserts that
# EVERY method the installed SDK exposes is recognised, so drift in either
# direction is caught rather than guessed at.
#
# This set is a VERSIONED SNAPSHOT of a vendor namespace that grows, and for an
# UNCERTAIN receiver it is the only thing standing between a real Slack write
# and the customer's workspace (see _is_slack_namespace_method). Eleven
# families were missing from the hand-written original -- canvases, assistant,
# slackLists, entity, agents, blocks, validate and the legacy
# channels/groups/im/mpim -- and all eleven DELIVERED a real message on an
# if/else- or try/except-bound client. Do not edit this set by hand.
_SLACK_RESOURCES = frozenset({
    "admin", "api", "apps", "assistant", "auth", "bookmarks", "bots", "calls",
    "canvases", "channels", "chat", "conversations", "dialog", "dnd", "emoji",
    "files", "functions", "groups", "im", "migration", "mpim", "oauth",
    "openid", "pins", "reactions", "reminders", "rtm", "search", "slackLists",
    "stars", "team", "tooling", "usergroups", "users", "views", "workflows",
})

# Four of the forty families are NOT prefix-gated, because their prefix is a
# word customer code uses for its own helpers. Gating them by prefix flags a
# real non-Slack call on an uncertain receiver and breaks a live migration --
# `validate_schema(...)` on a stale-bound name was caught doing exactly that by
# tests/test_slack_namespace_gate.py. Nothing is lost by matching the full
# name: `validate`, `blocks` and `entity` have exactly ONE Slack method each.
_SLACK_EXACT_METHODS = frozenset({
    "validate_slack_signature",   # local signature check, not even an API call
    "blocks_validate",
    "entity_presentDetails",
})

# `agents` is the same problem with 11 methods rather than one, but they fall
# under just two sub-resources, so the longer prefixes are both safe and
# complete. Derive these, never eyeball them -- a first pass here listed only
# `agents_conversations_` because the enumeration output was truncated, and the
# SDK drift test is what caught the two `agents_sessions_*` methods that
# therefore stayed executable:
#   sorted({"_".join(m.split("_")[:2]) + "_"
#           for m in dir(WebClient) if m.startswith("agents_")})
_SLACK_SUBRESOURCE_PREFIXES = ("agents_conversations_", "agents_sessions_")

# Gate for reporting an unresolvable receiver. Without it the check fires on
# every chained call in the notebook (spark.read.parquet, df.cast, dt.now, ...)
# and floods the report — measured 39 findings on Monitoring_and_Posting and 98
# on Reporting_Excel, none of them Slack.
_SLACK_SUSPICION_RE = re.compile(
    r"slack|webhook|WebClient|WebhookClient|SlackClient", re.IGNORECASE)
# Slack Web API methods are `resource_action`: chat_postMessage, files_upload_v2,
# conversations_history. Plain names (get, strip, now, cast) never match.
# The resource may be camelCase -- `slackLists_create`, `slackLists_items_list`
# are real methods. An initial-lowercase-only resource pattern silently dropped
# that whole family from the indirect-receiver report. Widening is safe because
# the only caller also requires the resource to be in _SLACK_RESOURCES, which
# is what actually excludes pandas/Spark names.
_SLACK_METHOD_SHAPE_RE = re.compile(r"^[a-z][A-Za-z0-9]*_[a-zA-Z][A-Za-z0-9_]*$")

# IPython magics / shell escapes are NOT valid Python. They must be neutralised
# before parsing or ast.parse raises and the whole cell goes unanalysed — which
# is exactly how a post slips through. Replaced 1:1 so line numbers still map to
# the original source.
_MAGIC_RE = re.compile(r"^\s*[%!]")


class NeutralizeResult:
    """Outcome for one cell."""

    def __init__(
        self,
        source: str,
        commented: int = 0,
        neutralised: Optional[List[Tuple[str, int]]] = None,
        unprovable: Optional[List[Tuple[str, str, int]]] = None,
        parse_error: Optional[str] = None,
        unsafe: bool = False,
    ) -> None:
        # `unsafe` means: this cell could NOT be certified free of external
        # posts. The caller must refuse to execute it rather than fall back to
        # the original source -- falling back is what let an unparseable cell
        # carrying `client.files_upload_v2(...)` reach the kernel live.
        self.unsafe = unsafe
        self.source = source
        self.commented = commented
        self.neutralised = neutralised or []      # [(method, lineno)]
        self.unprovable = unprovable or []        # [(kind, detail, lineno)]
        self.parse_error = parse_error            # set -> caller must fall back

    @property
    def changed(self) -> bool:
        return self.commented > 0

    def __repr__(self) -> str:  # pragma: no cover
        return (f"NeutralizeResult(commented={self.commented}, "
                f"neutralised={self.neutralised}, "
                f"unprovable={len(self.unprovable)}, "
                f"parse_error={self.parse_error!r})")


# ─── What a constructor NAME means ───────────────────────────────────────────
# Modules whose exports are Slack clients. Used to decide IMPORT ORIGIN: a bare
# class name is not enough, because `from internal_http import WebClient` is a
# different class that happens to share the name.
_SLACK_MODULE_RE = re.compile(r"(?:^|\.)slack(?:_sdk|client)?(?:\.|$)", re.I)

# A FACTORY whose name is slack-ish is evidence about the object it returns.
#
# This is the anchor the regex path got wrong. It applied the same hint to the
# RECEIVER name, which is why `webhook_producer.send(topic, msg)` on a Kafka
# producer was commented out. The factory name is evidence about what was
# constructed; the receiver name is just a name. Moving the hint here recovers
# the coverage (`slack_client = get_slack_client()`) without the false positive.
_SLACK_HINT_RE = re.compile(r"slack|webhook|hook", re.I)

# The regex path's tier-2 method set, kept EXACTLY as it was. Widening it would
# add false positives that the old code did not have.
_TIER2_METHODS = frozenset({"send", "api_call", "post_message"})

# Extra verbs used for REPORTING ONLY, never for commenting. Slack's
# `resource_action` shape misses plain verbs, so `d["c"].post()` would go
# unreported without this.
_REPORT_VERBS = frozenset({"post", "send", "notify", "alert", "upload", "message"})

# Send verbs, for deciding whether a method belongs to Slack's namespace at
# all. Same list tier-1 already curates in _TIER1_CODE_RE — kept in sync so a
# customer wrapper like `notify_team()` / `dispatch_alert()` on a Slack client
# is still recognised as a send.
_SEND_VERBS = frozenset({
    "send", "post", "push", "publish", "notify", "alert", "deliver",
    "dispatch", "broadcast", "message", "msg", "write", "ping", "report",
    "chat", "upload",
})


def _receiver_evidence_is_conclusive(recv: Optional[str],
                                     bindings: "Bindings") -> bool:
    """Is the receiver PROVEN Slack, as opposed to merely believed to be?

    The single place this question is answered, so the statement, header and
    method-as-value paths cannot drift apart again. They previously each
    implemented their own variant, and adding a method gate to only one made
    the same line behave differently in `x.m()` and `if x.m():`.

    Conclusive  -> the method is irrelevant; every method on a real Slack
                   client is a Slack action, so stay fail-closed. Nothing about
                   the method name can reopen this path.
    Uncertain   -> the method is the ONLY tie-breaker, so _SLACK_RESOURCES
                   becomes safety-critical rather than merely a report filter;
                   see _is_slack_namespace_method.

    An earlier version of this docstring claimed conclusiveness was what kept
    `client.canvases_delete(...)` blocked "even though `canvases` is absent
    from _SLACK_RESOURCES". That was only true of a DEFINITE binding. With
    `if prod: client = WebClient(...)` / `else: client = Mock()` -- or the
    commoner `try: client = WebClient(...) / except: client = None` -- both
    assignments are conditional, _bind's fail-safe keeps the Slack verdict and
    marks the name stale, and this function correctly returns False. The
    missing family then decided it, and the write ran.
    """
    return bindings.is_slack(recv) and not bindings.is_uncertain(recv)


def _acts_on_slack(recv: Optional[str], method: str,
                   bindings: "Bindings") -> bool:
    """Should a call of `recv.method(...)` be treated as a Slack operation?

    Two situations needing OPPOSITE evidence, which is the whole lesson of the
    cell-66 incident and of the fail-open regression that the first attempt at
    fixing it introduced:

      * receiver proven Slack      -> yes, whatever the method is
      * receiver only believed     -> only if the method is in Slack's
                                      namespace
    """
    if not bindings.is_slack(recv):
        return False
    if _receiver_evidence_is_conclusive(recv, bindings):
        return True
    return _is_slack_namespace_method(method)


def _is_slack_namespace_method(method: str) -> bool:
    """Could `method` plausibly be a Slack operation at all?

    Slack's Web API namespace is ENUMERABLE per SDK version: every method is
    `resource_action` with the resource drawn from _SLACK_RESOURCES. It is a
    snapshot, not a closed set -- Slack ships new families (canvases, assistant
    and slackLists are all recent), so a stale snapshot fails OPEN here. That
    residual is real and deliberate: tests/test_slack_resource_families.py
    pins the snapshot and fails when the installed SDK knows a family this set
    does not, so the gap surfaces in CI instead of in a customer's workspace.
    Only this uncertain path depends on it; a proven receiver never reaches
    here. The set of
    NON-Slack methods that can appear on a Slack-bound name is OPEN (any
    library, any user class), so it can never be enumerated -- which is why
    keying the decision off the receiver alone produces unbounded false
    positives. Gate on the closed set instead.

    Incident (2026-09-25, Example_Models_Monitoring cell 66 /
    Example_Monitoring_Base_Prep.ipynb): `%run` children bound `client` to a real
    WebClient; the parent rebound it to `MlflowClient()` INSIDE an `if`, so
    _bind's conditional fail-safe kept the Slack verdict; branch A then
    commented `client.get_latest_versions(...)` and
    `client.download_artifacts(...)`, and the next line's `model_version.run_id`
    raised NameError. Neither method is in Slack's namespace.
    """
    if method in _TIER2_METHODS or method in _SLACK_EXACT_METHODS:
        return True
    if method.startswith(_SLACK_SUBRESOURCE_PREFIXES):
        return True
    if "_" not in method:
        # Whole-name match, never a substring: `createOrReplaceTempView`
        # contains "create" but is a Spark call.
        return method.lower() in _SEND_VERBS
    prefix = method.split("_", 1)[0]
    return prefix in _SLACK_RESOURCES or prefix in _SEND_VERBS

# Binding barriers. A function-local `client = WebClient(...)` must NOT feed the
# notebook-wide binding set: a never-called def-local binding re-adding the name
# after a module-level `client = KafkaProducer(...)` is what resurrected the
# Kafka false positive.
# Sentinel for "probably a Slack client, but only a name hint says so".
WEAK = "weak"

_SCOPE_BARRIERS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)

_CONDITIONAL_STMTS = tuple(
    t for t in (ast.If, ast.Try, ast.For, ast.AsyncFor, ast.While,
                getattr(ast, "TryStar", None))
    if t is not None
)


class Bindings(dict):
    """Notebook-wide name -> binding verdict.

        True    bound to a Slack client
        False   assigned something we could see, with no Slack evidence
        absent  UNKNOWN -- never assigned anywhere visible

    The three-way split is load-bearing. `False` and `absent` behave
    differently on purpose:

      * `absent` still allows the receiver-name hint tier, which is how a
        client arriving from a `%run` child or a function parameter is caught
        (the regex path's behaviour, preserved).
      * `False` BLOCKS that tier. That is what keeps
        `webhook_producer = KafkaProducer(...)` from being commented out.

    Collapsing them into a two-state set is precisely what lost the
    `slack_client = get_slack_client()` class of coverage.

    Subclasses `dict` so `name in bindings` keeps working for existing callers.
    """

    def __init__(self) -> None:
        super().__init__()
        # Local ctor name -> is-slack, from import origin. `WebClient` means
        # different things depending on where it was imported from.
        self.ctors: Dict[str, bool] = {}
        # Constructor values have assignment/scope lifetimes, unlike the old
        # accumulating import flag. Keep the constructor kind: aliases must
        # not turn a webhook URL into a WebClient token.
        self.ctor_values = {}
        self.ctor_functions = {}
        self.ctor_resolving = set()
        # Module aliases bound to a Slack module: `import slack_sdk as s`.
        self.modules: Set[str] = set()
        # Names whose True came from a slack-ish FACTORY NAME rather than a
        # real constructor. Weaker evidence, so only recognised write verbs are
        # commented on them -- treating them as full clients commented working
        # code (`cfg = get_slack_config()` then `cfg.get("channel")`).
        self.weak: Set[str] = set()
        # Names still marked True ONLY because _bind's conditional fail-safe
        # discarded a contrary verdict. The state previously had no way to say
        # "I derived that this is NOT Slack, but threw that answer away because
        # the reassignment sat inside an `if`" -- so a child-bound name the
        # parent rebinds to `MlflowClient()` looked IDENTICAL to a client
        # constructed on the line above. Both read
        # is_slack=True / is_weak=False, which is why the guard could not pick
        # the right evidence and commented MLflow calls (cell 66 of
        # Example_Monitoring_Base_Prep.ipynb).
        #
        # The distinction matters because the two cases need OPPOSITE rules:
        # when the receiver is proven, the method is irrelevant (every method
        # on a real client is a Slack action); when it is doubtful, the method
        # is the only thing that can settle it.
        self.stale: Set[str] = set()

    def _child(self) -> "Bindings":
        """A copy that shares nothing mutable with the parent."""
        clone = Bindings()
        clone.update(self)
        clone.ctors.update(self.ctors)
        clone.ctor_values.update(self.ctor_values)
        clone.ctor_functions.update(self.ctor_functions)
        clone.ctor_resolving.update(self.ctor_resolving)
        clone.modules.update(self.modules)
        clone.weak.update(self.weak)
        clone.stale.update(self.stale)
        return clone

    def is_slack(self, name: Optional[str]) -> bool:
        return name is not None and self.get(name) is True

    def is_known_other(self, name: Optional[str]) -> bool:
        return name is not None and self.get(name) is False

    def is_weak(self, name: Optional[str]) -> bool:
        return name is not None and name in self.weak

    def is_uncertain(self, name: Optional[str]) -> bool:
        """True when the Slack verdict is NOT conclusive.

        Either the evidence was never strong (a slack-ish factory name), or it
        was contradicted and the contradiction was discarded by the
        conditional fail-safe. In both cases the method has to decide.
        """
        return self.is_weak(name) or (name is not None and name in self.stale)


def new_bindings() -> Bindings:
    """Fresh per-notebook binding state. Reset when a new notebook starts."""
    return Bindings()


def _as_bindings(obj: Optional[object]) -> Bindings:
    """Accept a Bindings, a plain set (legacy callers), or None."""
    if isinstance(obj, Bindings):
        return obj
    out = Bindings()
    if isinstance(obj, dict):
        out.update(obj)
        if isinstance(obj, Bindings):
            out.ctors.update(obj.ctors)
            out.modules.update(obj.modules)
            out.weak.update(obj.weak)
    elif isinstance(obj, (set, frozenset)):
        out.update({name: True for name in obj})
    return out


# Line magics that TAKE A PYTHON EXPRESSION and execute it. Blanking the whole
# line hides that expression from every analysis below, so
# `%time client.files_upload_v2(file=f)` was left completely unexamined.
_MAGIC_WITH_CODE_RE = re.compile(
    r"^(\s*)(%{1,2}(?:time|timeit|prun|capture)\b)"
    r"((?:\s+-{1,2}\w+(?:\s+[^\s-]\S*)?)*\s+)(?=\S)")


def _strip_magics(source: str) -> str:
    """Neutralise magic/shell lines for parsing, preserving line count + columns.

    For a magic that wraps a Python expression the MAGIC TOKEN is blanked and
    the expression is kept, so the call it runs is still analysed. Column
    positions are preserved (spaces, not deletion) so `_stmt_text` line/col
    mapping back to the ORIGINAL source stays exact.

    If blanking produces something unparseable (`%timeit -n 5 expr` and other
    flag forms), the line falls back to `pass` -- the old behaviour.
    """
    lines = source.splitlines()
    out, softened = [], []
    for line in lines:
        m = _MAGIC_WITH_CODE_RE.match(line) if line.strip() else None
        if m:
            # Blanking the token IN PLACE would indent the expression
            # (`%time expr` -> `      expr`, an IndentationError at module
            # scope). Keep the original indent, move the expression up to it,
            # and pad at the END so the line count and length are unchanged.
            token = m.group(2) + m.group(3)
            out.append(m.group(1) + line[m.end(3):] + (" " * len(token)))
            softened.append(len(out) - 1)
        elif _MAGIC_RE.match(line) and line.strip():
            indent = line[: len(line) - len(line.lstrip())]
            out.append(f"{indent}pass  # __AIDP_MAGIC__")
        else:
            out.append(line)
    if softened:
        try:
            ast.parse("\n".join(out))
        except SyntaxError:
            for row in softened:          # flag forms etc. -- revert to `pass`
                original = lines[row]
                indent = original[: len(original) - len(original.lstrip())]
                out[row] = f"{indent}pass  # __AIDP_MAGIC__"
    return "\n".join(out)


def _ctor_name(call: ast.Call) -> Optional[str]:
    """Constructor being called: `WebClient(...)` or `slack_sdk.WebClient(...)`."""
    fn = call.func
    if isinstance(fn, ast.Name):
        return fn.id
    if isinstance(fn, ast.Attribute):
        return fn.attr
    return None


_TOKEN_CTOR = frozenset({"token"})
_WEBHOOK_CTOR = frozenset({"webhook"})
_OTHER_CTOR = frozenset({"other"})
_UNKNOWN_CTOR = frozenset({"unknown"})
_CLIENT_INSTANCE = frozenset({"instance"})


def _constructor_roles(value, bindings: Bindings):
    """Possible callable roles, without executing customer code.

    A union is intentional: if one branch can construct Slack, neither a
    later branch nor a later caller can erase that safety evidence. Literal
    containers retain their elements so a proven selection stays precise.
    Unsupported computation stays unknown, never proven non-Slack.
    """
    result = _constructor_value(value, bindings)
    return result if isinstance(result, frozenset) else _OTHER_CTOR


def _constructor_value(value, bindings: Bindings):
    if isinstance(value, ast.Name):
        if value.id in bindings.ctor_values:
            return bindings.ctor_values[value.id]
        if value.id in bindings.ctors:
            return (_WEBHOOK_CTOR if "Webhook" in value.id else _TOKEN_CTOR
                    ) if bindings.ctors[value.id] else _OTHER_CTOR
        if value.id in SLACK_CTORS:
            return _WEBHOOK_CTOR if "Webhook" in value.id else _TOKEN_CTOR
        # Builtin replacements (dict, ValueError, etc.) are exact evidence.
        import builtins
        if hasattr(builtins, value.id):
            return _OTHER_CTOR
        return _UNKNOWN_CTOR
    if isinstance(value, ast.Attribute):
        dotted = _dotted(value)
        if dotted in bindings.ctor_values:
            return bindings.ctor_values[dotted]
        root = value.value
        while isinstance(root, ast.Attribute):
            root = root.value
        if isinstance(root, ast.Name):
            origin = _constructor_roles(root, bindings)
        else:
            origin = _UNKNOWN_CTOR
        if isinstance(root, ast.Name) and (root.id in bindings.modules
                or origin & {'slack-module', 'slack-errors-module'} or (
                    root.id in {"slack", "slack_sdk"} and root.id not in bindings.ctor_values)):
            if origin == frozenset({'slack-errors-module'}) or '.errors.' in (dotted or ''):
                return _OTHER_CTOR
            if value.attr in SLACK_CTORS:
                roles = _WEBHOOK_CTOR if "Webhook" in value.attr else _TOKEN_CTOR
            else:
                # Preserve the old outbound guard for unknown SDK attributes,
                # without guessing their authentication signature.
                roles = _TOKEN_CTOR | _UNKNOWN_CTOR
            if origin & {'other', 'unknown', 'slack-errors-module'} and root.id in bindings.ctor_values:
                roles |= _UNKNOWN_CTOR
            return roles
        return _UNKNOWN_CTOR
    if isinstance(value, ast.IfExp):
        return (_constructor_roles(value.body, bindings)
                | _constructor_roles(value.orelse, bindings))
    if isinstance(value, (ast.Tuple, ast.List)):
        return tuple(_constructor_value(item, bindings) for item in value.elts)
    if isinstance(value, ast.Dict):
        if all(isinstance(key, ast.Constant) for key in value.keys):
            return {key.value: _constructor_value(item, bindings)
                    for key, item in zip(value.keys, value.values)}
        return _UNKNOWN_CTOR
    if isinstance(value, ast.Subscript):
        container = _constructor_value(value.value, bindings)
        if isinstance(container, (tuple, dict)):
            if isinstance(value.slice, ast.Constant):
                try:
                    return container[value.slice.value]
                except (KeyError, IndexError, TypeError):
                    return _UNKNOWN_CTOR
            elements = container.values() if isinstance(container, dict) else container
            return frozenset().union(*(item if isinstance(item, frozenset)
                                       else _OTHER_CTOR for item in elements)) or _UNKNOWN_CTOR
        return _UNKNOWN_CTOR
    if isinstance(value, ast.Call):
        fn = (bindings.ctor_functions.get(value.func.id)
              if isinstance(value.func, ast.Name) else None)
        if isinstance(fn, tuple):
            results = frozenset()
            for candidate in fn:
                if candidate is None:
                    results |= _UNKNOWN_CTOR
                    continue
                local = bindings._child()
                local.ctor_functions[value.func.id] = candidate
                results |= _constructor_roles(value, local)
            return results
        if fn is not None and (id(fn) in bindings.ctor_resolving
                               or len(bindings.ctor_resolving) >= 8):
            return _UNKNOWN_CTOR
        if fn is not None:
            if fn.decorator_list:
                return _UNKNOWN_CTOR
            local = bindings._child()
            local.ctor_resolving.add(id(fn))
            _shadow_constructor_locals(fn, local)
            for name in _arg_names(fn):
                _shadow(local, name)
            for name, default in _constructor_defaults(fn).items():
                _set_constructor(local, name, _constructor_value(default, bindings))
            for name, actual in _constructor_actuals(fn, value, bindings).items():
                _set_constructor(local, name, actual)
            roles = frozenset()
            for stmt, conditional in _walk_module_scope(fn.body, yield_barriers=True):
                if isinstance(stmt, ast.Return):
                    roles |= _constructor_roles(stmt.value, local)
                elif not isinstance(stmt, _SCOPE_BARRIERS):
                    _apply_stmt(stmt, local, conditional)
            return roles or _UNKNOWN_CTOR
        # Calling a known class produces an instance, not another class.
        roles = _constructor_roles(value.func, bindings)
        if roles & {"token", "webhook"}:
            return _CLIENT_INSTANCE
        if isinstance(value.func, ast.Name) and value.func.id not in bindings.ctor_values:
            import builtins
            builtin = getattr(builtins, value.func.id, None)
            if isinstance(builtin, type) and builtin is not type:
                return _OTHER_CTOR
        return _UNKNOWN_CTOR
    if isinstance(value, (ast.Constant, ast.Lambda)) or value is None:
        return _OTHER_CTOR
    return _UNKNOWN_CTOR


def _set_constructor(bindings, name, value, conditional=False):
    previous = bindings.ctor_values.get(name)
    if conditional:
        old = _constructor_roles(ast.Name(id=name), bindings)
        new = value if isinstance(value, frozenset) else _OTHER_CTOR
        value = old | new
    # Unknown reassignment cannot silently erase a previously tracked
    # constructor. Refuse at its next use instead of guessing its arguments.
    if value == _UNKNOWN_CTOR and (name in bindings.ctors or
            _constructor_roles(ast.Name(id=name), bindings) & {'token', 'webhook'}):
        old = _constructor_roles(ast.Name(id=name), bindings)
        value |= old | _TOKEN_CTOR
    bindings.ctor_values[name] = value
    roles = value if isinstance(value, frozenset) else _OTHER_CTOR
    if roles & {"token", "webhook"} or name in bindings.ctors or name in SLACK_CTORS:
        bindings.ctors[name] = bool(roles & {"token", "webhook"})
    bindings.ctor_functions.pop(name, None)
    bindings.modules.discard(name)


def _bind_constructor_definition(bindings, stmt):
    roles = _OTHER_CTOR
    if stmt.decorator_list:
        roles = _UNKNOWN_CTOR
    elif isinstance(stmt, ast.ClassDef) and stmt.bases:
        roles = frozenset().union(*(_constructor_roles(base, bindings) for base in stmt.bases))
        if roles & {"token", "webhook"} and any(
                isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef))
                and member.name in {'__init__', '__new__'} for member in stmt.body):
            roles |= _UNKNOWN_CTOR
    _set_constructor(bindings, stmt.name, roles)
    if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
        bindings.ctor_functions[stmt.name] = stmt


def _invalidate_constructor_object(bindings, expression):
    value = _constructor_value(expression, bindings)
    if not isinstance(value, (tuple, dict)):
        return

    def contains(candidate):
        if candidate is value:
            return True
        if isinstance(candidate, (tuple, dict)):
            items = candidate.values() if isinstance(candidate, dict) else candidate
            return any(contains(item) for item in items)
        return False

    for name, candidate in list(bindings.ctor_values.items()):
        if contains(candidate):
            bindings.ctor_values[name] = _UNKNOWN_CTOR


def _tracked_constructor_function(fn, bindings):
    """A function (or its alias) replacing a tracked constructor name."""
    return isinstance(fn, ast.Name) and (
        fn.id in bindings.ctors or
        getattr(bindings.ctor_functions.get(fn.id), 'name', None) in bindings.ctors)


def _ctor_verdict(call: ast.Call, bindings: Bindings):
    """Does this Call construct a Slack client?

    True   -- yes, a real constructor (strong)
    WEAK   -- a factory whose NAME is slack-ish; likely, but not proof
    False  -- provably not (import origin says the name is someone else's class)
    None   -- no evidence either way
    """
    fn = call.func
    roles = _constructor_roles(fn, bindings)
    if roles & {"token", "webhook"}:
        return True
    if roles == _OTHER_CTOR:
        if not (isinstance(fn, ast.Name) and fn.id in bindings.ctor_functions):
            return False
        result = _constructor_roles(call, bindings)
        if "instance" in result:
            return True if result == _CLIENT_INSTANCE else WEAK
        if "unknown" in result and _tracked_constructor_function(fn, bindings):
            return WEAK  # retain the write guard until artifact delivery refuses
    name = fn.id if isinstance(fn, ast.Name) else fn.attr if isinstance(fn, ast.Attribute) else ""
    if _SLACK_HINT_RE.search(name):
        # get_slack_client(), make_slack(), build_hook(). Evidence about the
        # object, but not proof it is a Slack client -- `get_slack_config()`
        # returns a dict. Recorded WEAK so only write verbs are commented.
        return WEAK
    return None


def _shadow_constructor_locals(fn, state):
    """Python locals hide outer constructors throughout the function body."""
    local, outer = set(), set()

    class Locals(ast.NodeVisitor):
        def visit_Name(self, node):
            if isinstance(node.ctx, (ast.Store, ast.Del)):
                local.add(node.id)

        def visit_Global(self, node):
            outer.update(node.names)

        visit_Nonlocal = visit_Global

        def visit_Import(self, node):
            local.update(alias.asname or alias.name.split('.')[0] for alias in node.names)

        def visit_ImportFrom(self, node):
            local.update(alias.asname or alias.name for alias in node.names)

        def visit_FunctionDef(self, node):
            local.add(node.name)

        visit_AsyncFunctionDef = visit_FunctionDef
        visit_ClassDef = visit_FunctionDef

        def visit_Lambda(self, node):
            pass

        def visit_ListComp(self, node):
            pass  # comprehensions own their iteration variables

        visit_SetComp = visit_ListComp
        visit_DictComp = visit_ListComp
        visit_GeneratorExp = visit_ListComp

    visitor = Locals()
    for stmt in fn.body:
        visitor.visit(stmt)
    for name in local - outer:
        _shadow_constructor(state, name)


def _constructor_defaults(fn):
    positional = list(fn.args.posonlyargs) + list(fn.args.args)
    defaults = dict((arg.arg, default) for arg, default in zip(
        positional[len(positional) - len(fn.args.defaults):], fn.args.defaults))
    defaults.update((arg.arg, default) for arg, default in zip(
        fn.args.kwonlyargs, fn.args.kw_defaults) if default is not None)
    return defaults


def _constructor_actuals(fn, call, bindings):
    """Bind ordinary and statically expanded arguments using one contract."""
    positional = list(fn.args.posonlyargs) + list(fn.args.args)
    values, unknown = [], False
    for arg in call.args:
        if isinstance(arg, ast.Starred):
            expanded = _constructor_value(arg.value, bindings)
            if isinstance(expanded, tuple):
                values.extend(expanded)
            else:
                unknown = True
        else:
            values.append(_constructor_value(arg, bindings))
    actuals = ({arg.arg: _UNKNOWN_CTOR for arg in positional} if unknown else
               {arg.arg: value for arg, value in zip(positional, values)})
    for kw in call.keywords:
        value = _constructor_value(kw.value, bindings)
        if kw.arg is not None:
            actuals[kw.arg] = value
        elif isinstance(value, dict):
            actuals.update(value)
        else:
            for name in _arg_names(fn):
                actuals.setdefault(name, _UNKNOWN_CTOR)
    return actuals


def _seed_constructor_parameters(fn, body, snapshots, seed, escaped=False):
    """Constructor arguments belong to the callee, not its enclosing scope.

    Visible calls through tracked function aliases also supply evidence.
    Multiple callers are joined;
    an application constructor at one caller cannot erase Slack at another.
    """
    samples = {name: [] for name in _arg_names(fn)}
    positional = list(fn.args.posonlyargs) + list(fn.args.args)
    defaults = _constructor_defaults(fn)
    at_def = snapshots.get(id(fn), seed)

    class Calls(ast.NodeVisitor):
        state = None

        def visit_FunctionDef(self, node):
            pass

        visit_AsyncFunctionDef = visit_FunctionDef
        visit_ClassDef = visit_FunctionDef
        visit_Lambda = visit_FunctionDef

        def visit_Call(self, node):
            if (isinstance(node.func, ast.Name)
                    and self.state.ctor_functions.get(node.func.id) is fn):
                actuals = _constructor_actuals(fn, node, self.state)
                for name in samples:
                    samples[name].append(
                        actuals[name] if name in actuals else
                        _constructor_value(defaults[name], at_def) if name in defaults else
                        _UNKNOWN_CTOR)
            self.generic_visit(node)

    visitor = Calls()
    for stmt, _ in _walk_module_scope(body, yield_barriers=True):
        if isinstance(stmt, _SCOPE_BARRIERS):
            continue
        visitor.state = snapshots.get(id(stmt), seed)
        # Inspect expressions belonging to this statement; child statements
        # have their own snapshots and are visited by the outer walk.
        for child in ast.iter_child_nodes(stmt):
            if not isinstance(child, ast.stmt):
                visitor.visit(child)
    for name, values in samples.items():
        if escaped:
            # A callback can have callers whose arguments are not visible here.
            # One known caller cannot prove all callers use an ordinary class.
            values.append(_constructor_roles(ast.Name(id=name), seed) | _UNKNOWN_CTOR)
        if not values:
            if name in defaults:
                _set_constructor(seed, name, _constructor_value(defaults[name], at_def))
            continue
        value = values[0]
        for other in values[1:]:
            if value != other:
                value = ((value if isinstance(value, frozenset) else _OTHER_CTOR)
                         | (other if isinstance(other, frozenset) else _OTHER_CTOR))
        _set_constructor(seed, name, value)


def _launders(call: ast.Call, bindings: Bindings, target: Optional[str]) -> bool:
    """Is this call a TRANSFORM of the very name being assigned?

        slack_hook = normalize(slack_hook)   -> yes, still a client
        client     = client.copy()           -> yes (method form)
        client     = KafkaProducer(b)        -> no, a genuine rebind
        x          = make_thing(client)      -> no, a DERIVED object

    Requiring the TARGET ITSELF to appear inside the call is what separates
    laundering from a rebind. A looser test -- any Slack-bound name anywhere in
    the arguments -- commented every object merely derived from a client:
    measured 9 new false positives (x.close(), x.update(), x.add(), x.remove(),
    x.open(), x.create(), x.push(), x.send(), x.post()), none of which the
    regex path touched.
    """
    if target is None or bindings.get(target) is not True:
        return False
    return any(isinstance(node, ast.Name) and node.id == target
               for node in ast.walk(call))


def _value_verdict(value: ast.expr, bindings: Bindings,
                   target: Optional[str] = None) -> Optional[bool]:
    """Tri-state verdict for the right-hand side of an assignment."""
    if isinstance(value, ast.Call):
        # An assignment we CAN see. A factory with no Slack evidence makes the
        # name known-non-Slack, which is what blocks the hint tier for Kafka.
        verdict = _ctor_verdict(value, bindings)
        if verdict is not None:
            return verdict
        if _launders(value, bindings, target):
            # `slack_hook = normalize(slack_hook)` -- the client went IN, so
            # whatever came out is probably still a client. Marking it
            # known-non-Slack both unbinds it AND blocks the receiver-hint
            # tier, so the post below went out silently (base commented it;
            # measured head=0 with zero findings). WEAK, so only recognised
            # write verbs are commented.
            return WEAK
        return False
    if isinstance(value, ast.Name):
        # instance alias: `c2 = client`
        if bindings.get(value.id) is True:
            return WEAK if bindings.is_weak(value.id) else True
        return bindings.get(value.id)
    if isinstance(value, ast.Await):
        return _value_verdict(value.value, bindings)
    if isinstance(value, ast.IfExp):
        left = _value_verdict(value.body, bindings)
        right = _value_verdict(value.orelse, bindings)
        if left is True or right is True:
            return True                          # fail safe: one branch posts
        if left is WEAK or right is WEAK:
            return WEAK
        if left is False and right is False:
            return False
        return None
    if isinstance(value, ast.Attribute):
        # `hook = cfg.slack_hook` / `url = cfg.webhook_url`.
        #
        # Deliberately UNKNOWN, not Slack. Reading the hint as a binding
        # commented working code (`url = cfg.webhook_url` then `url.strip()`).
        # Unknown is the right answer: it leaves the receiver-name hint tier
        # free to fire on `hook.send(...)`, which is the regex path's
        # behaviour, without turning the name into a full client.
        return None
    if isinstance(value, ast.Subscript):
        return None                              # no evidence; keep hint tier alive
    return False                                 # literal, comprehension, ...


def _bind_target(bindings: Bindings, name: str, value: ast.expr,
                 conditional: bool, events: Optional[List[Tuple[str, int]]],
                 lineno: int, ctor_conditional=None) -> None:
    """Bind one name, recording a downgrade of a live Slack binding.

    A currently-Slack-bound name reassigned from a call we cannot identify is
    reported even when `_launders` says no, because the laundering may not
    mention the target:

        slack_hook = WebClient(token=t)
        slack_hook = normalize(cfg)      # target absent -> not provably a client
        slack_hook.send('x')             # ...but it may well still be one

    Measured cost of always reporting this: 1 finding across the 37-case review
    matrix, and 0 across realistic non-Slack cells (boto3 client rebinds,
    connection reconnects, model re-tuning). It can only fire in a notebook
    that already holds a live Slack binding.
    """
    # An exception alias can later be rebound to a genuine client CLASS.
    # Carry that constructor identity separately from instance bindings so
    # `Error = WebClient; Error(token=...)` still has its credential redacted.
    constructor = _constructor_value(value, bindings)
    factory = bindings.ctor_functions.get(value.id) if isinstance(value, ast.Name) else None
    previous = bindings.get(name)
    verdict = _value_verdict(value, bindings, name)
    if (previous is True and verdict is False and isinstance(value, ast.Call)
            and events is not None):
        events.append((name, lineno))
    _bind(bindings, name, verdict, conditional)
    _set_constructor(bindings, name, constructor,
                     conditional if ctor_conditional is None else ctor_conditional)
    if factory is not None:
        bindings.ctor_functions[name] = factory


def _invalidate_prefix(bindings: Bindings, name: str) -> None:
    """Rebinding `cfg` invalidates every `cfg.*` key.

    Those keys described the OLD object. Without this a dotted binding goes
    stale and keeps claiming Slack after its root has been replaced:

        cfg.notifier = WebClient(token=t)
        cfg = load_other()            # cfg.notifier now describes nothing
        cfg.notifier.send(x)          # must NOT be treated as a Slack client

    Only plain roots invalidate; binding `cfg.notifier` itself must not wipe
    its own siblings.
    """
    if "." in name:
        return
    prefix = name + "."
    for key in [k for k in bindings.ctor_values if k.startswith(prefix)]:
        bindings.ctor_values.pop(key, None)
        bindings.ctors.pop(key, None)
    for key in [k for k in bindings if k.startswith(prefix)]:
        del bindings[key]
        bindings.weak.discard(key)


def _iter_verdict(node: ast.expr, bindings: Bindings):
    """Verdict for what a `for` target will be bound to, one element at a time.

    Only a literal sequence carries evidence. Everything else returns None --
    UNKNOWN, not False -- so `for i in range(10)` and `for c in kafka_clients`
    change nothing and cannot produce a false positive.
    """
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        verdicts = [_value_verdict(element, bindings) for element in node.elts]
        if any(v is True for v in verdicts):
            return True
        if any(v is WEAK for v in verdicts):
            return WEAK
        return None
    return None


def _bind_iter_target(target: ast.expr, verdict, bindings: Bindings,
                      conditional: bool) -> None:
    """Bind a `for` target, which may be a name or a tuple of names."""
    if isinstance(target, ast.Name):
        _bind(bindings, target.id, verdict, conditional)
    elif isinstance(target, (ast.Tuple, ast.List)):
        for element in target.elts:
            if isinstance(element, ast.Name):
                _bind(bindings, element.id, verdict, conditional)


def _bind(bindings: Bindings, name: str, verdict, conditional: bool) -> None:
    """Apply one assignment to the binding state."""
    if verdict is None:
        return          # no evidence -- never clear what we already know
    if verdict is WEAK:
        bindings[name] = True
        bindings.weak.add(name)
        _invalidate_prefix(bindings, name)
        return
    if verdict is True:
        bindings[name] = True
        bindings.weak.discard(name)
        # Conclusive again: a real constructor overrides any earlier doubt.
        bindings.stale.discard(name)
        _invalidate_prefix(bindings, name)
        return
    # verdict is False
    if conditional and bindings.get(name) is True:
        # `try: client = WebClient(...)` / `except: client = None` is a normal
        # guarded construction. Letting the guarded rebind erase the binding
        # makes the post below it leak (measured: base 0, pre-fix head 0).
        # The binding survives, so its dotted children must survive too.
        #
        # But it survives on WEAKER evidence than before: we saw a contrary
        # verdict and discarded it only because the branch might not execute.
        # Record that, so the guard can require method evidence here without
        # loosening anything for a receiver that was never contradicted.
        bindings.stale.add(name)
        return
    bindings[name] = False
    bindings.weak.discard(name)
    bindings.stale.discard(name)
    _invalidate_prefix(bindings, name)


def _walk_module_scope(body: List[ast.stmt], conditional: bool = False,
                       yield_barriers: bool = False):
    """Yield (stmt, conditional) in SOURCE order for module-scope statements.

    Recurses into control flow, which executes at module scope, but stops at
    def/class. Source order matters: `ast.walk` order is not source order, and
    applying an add and a discard order-insensitively is what let
    `client = WebClient(t); client.files_upload_v2(...); client = None`
    through.
    """
    for stmt in body:
        if isinstance(stmt, _SCOPE_BARRIERS):
            if yield_barriers:
                # Surfaced (never descended into) so the caller can open a
                # nested lexical scope at exactly this point.
                yield stmt, conditional
            continue
        yield stmt, conditional
        sub = conditional or isinstance(stmt, _CONDITIONAL_STMTS)
        for field in ("body", "orelse", "finalbody"):
            inner = getattr(stmt, field, None)
            if isinstance(inner, list):
                yield from _walk_module_scope(
                    [s for s in inner if isinstance(s, ast.stmt)], sub)
        for handler in getattr(stmt, "handlers", None) or []:
            # The handler itself is yielded so `except ... as name` can rebind
            # before its body is judged.
            yield handler, True
            yield from _walk_module_scope(handler.body, True)


def _copy_constructor_state(destination, source):
    for attr in ('ctors', 'ctor_values', 'ctor_functions', 'modules'):
        target = getattr(destination, attr)
        target.clear()
        target.update(getattr(source, attr))


def _walk_constructor_paths(body, state, conditional=False):
    """Use definite bindings inside each path, join only at its exit.

    `if flag: import WebClient; WebClient(...)` has a proven import at the
    call. Joining with the unbound path BEFORE the call creates a false
    refusal. Existing instance conditional/stale rules remain independent.
    """
    for stmt in body:
        before = state._child()
        yield stmt, conditional
        if isinstance(stmt, _SCOPE_BARRIERS):
            continue
        start = state._child()
        if isinstance(stmt, ast.If):
            paths = (stmt.body, stmt.orelse)
        elif isinstance(stmt, (ast.For, ast.AsyncFor, ast.While)):
            nonempty = (isinstance(stmt, ast.For)
                        and isinstance(_constructor_value(stmt.iter, state), tuple)
                        and bool(_constructor_value(stmt.iter, state)))
            paths = (stmt.body,) if nonempty else (stmt.body, [])
        elif isinstance(stmt, ast.Try):
            paths = (list(stmt.body) + list(stmt.orelse),) + tuple(
                [handler] for handler in stmt.handlers)
        else:
            for field in ('body', 'orelse', 'finalbody'):
                child = getattr(stmt, field, None)
                if isinstance(child, list):
                    yield from _walk_constructor_paths(child, state, conditional)
            continue
        exits = []
        for index, path in enumerate(paths):
            seed = before if isinstance(stmt, ast.Try) and index else start
            if isinstance(stmt, (ast.For, ast.AsyncFor, ast.While)) and index:
                seed = before
            _copy_constructor_state(state, seed)
            yield from _walk_constructor_paths(path, state, True)
            exits.append(state._child())
        _copy_constructor_state(state, _union_states(exits))
        if isinstance(stmt, (ast.For, ast.AsyncFor, ast.While)):
            yield from _walk_constructor_paths(stmt.orelse, state, True)
        if isinstance(stmt, ast.Try):
            yield from _walk_constructor_paths(stmt.finalbody, state, conditional)


def _global_effects_for(body: List[ast.stmt]) -> Dict[str, Dict[str, object]]:
    """`_global_effects` over a bare statement list."""
    holder = ast.Module(body=list(body), type_ignores=[])
    return _global_effects(holder, Bindings())


def _constructor_effects(fn, bindings, defs=None, seen=frozenset(), declaration=ast.Global):
    """Evolve constructor rebindings exported by a visible function call.

    Instance summaries alone cannot represent `global Ctor; Ctor = dict`.
    Reuse the same assignment transfer used at module scope, and propagate
    only declared outer names (plus outer effects of visible nested calls).
    """
    candidates = _definition_candidates(fn)
    if len(candidates) != 1:
        effects = {}
        for candidate in candidates:
            for name, value in _constructor_effects(
                    candidate, bindings, defs, seen, declaration).items():
                effects[name] = effects.get(name, frozenset()) | value
        return effects
    fn = candidates[0]
    if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return {}
    if id(fn) in seen or len(seen) >= 8:
        return {}
    seen = seen | {id(fn)}
    local = bindings._child()
    _shadow_constructor_locals(fn, local)
    for name in _arg_names(fn):
        _shadow(local, name)
    declared = {name for stmt in _own_statements(fn)
                if isinstance(stmt, declaration) for name in stmt.names}
    exported = set(declared)
    local_defs = dict(defs or {})
    for stmt, conditional in _walk_constructor_paths(fn.body, local):
        if isinstance(stmt, _SCOPE_BARRIERS):
            _register_local_definition(local_defs, stmt, conditional)
            _bind_constructor_definition(local, stmt)
            continue
        for called in _called_names(stmt):
            callee = local.ctor_functions.get(called)
            if callee is not None:
                for name, value in _constructor_effects(
                        callee, local, local_defs, seen, declaration).items():
                    _set_constructor(local, name, value)
                    exported.add(name)
        _apply_stmt(stmt, local, conditional, ctor_conditional=False)
    return {name: _constructor_roles(ast.Name(id=name), local) for name in exported}


def _apply_stmt(stmt: ast.stmt, bindings: Bindings, conditional: bool,
                events: Optional[List[Tuple[str, int]]] = None,
                global_effects: Optional[Dict[str, object]] = None,
                aliases: Optional[Dict[str, str]] = None,
                ctor_conditional=None) -> None:
    """Apply one module-scope statement's effect on the binding state."""
    _ln = getattr(stmt, "lineno", 0)
    cc = conditional if ctor_conditional is None else ctor_conditional

    # A direct call to a function that assigns a `global` applies that effect
    # here, at the call site. `global_effects` maps a name to the definition
    # ACTIVE AT THIS POINT, so a later redefinition cannot retroactively change
    # what an earlier call did. Effects are computed with the bindings visible
    # here, so constructor aliases resolve.
    if global_effects:
        for called in _called_names(stmt, aliases):
            fn = global_effects.get(called)
            if fn is None:
                continue
            for name, verdict in _transitive_effects(
                    fn, bindings, global_effects, aliases).items():
                _bind(bindings, name, verdict, conditional)
    if global_effects is not None:
        for called in _called_names(stmt):
            fn = bindings.ctor_functions.get(called)
            if fn is not None:
                for name, value in _constructor_effects(fn, bindings, global_effects).items():
                    _set_constructor(bindings, name, value, cc)
    if isinstance(stmt, ast.Import):
        for alias in stmt.names:
            local = alias.asname or alias.name.split(".")[0]
            roles = _OTHER_CTOR
            if _SLACK_MODULE_RE.search(alias.name):
                roles = frozenset({'slack-errors-module' if alias.asname and alias.name
                                   in {'slack_sdk.errors', 'slack.errors'} else 'slack-module'})
            _set_constructor(bindings, local, roles, cc)
            if _SLACK_MODULE_RE.search(alias.name):
                bindings.modules.add(local)
        return

    if isinstance(stmt, ast.ImportFrom):
        from_slack = bool(stmt.module and _SLACK_MODULE_RE.search(stmt.module))
        for alias in stmt.names:
            local = alias.asname or alias.name
            # Import origin alone does not establish a factory's return type.
            # Unknown retains the existing weak factory-name outbound guard;
            # known constructor-name replacements and Slack exceptions stay
            # ordinary, preserving application code and exception messages.
            roles = _OTHER_CTOR
            if (not from_slack and alias.name not in SLACK_CTORS
                    and _SLACK_HINT_RE.search(alias.name)):
                roles = _UNKNOWN_CTOR
            if stmt.module in {'slack_sdk', 'slack'} and alias.name == 'errors':
                roles = frozenset({'slack-errors-module'})
            if (from_slack and stmt.module not in {"slack_sdk.errors", "slack.errors"}
                    and (alias.name in SLACK_CTORS or _SLACK_HINT_RE.search(alias.name))):
                roles = _WEBHOOK_CTOR if "Webhook" in alias.name else _TOKEN_CTOR
            _set_constructor(bindings, local, roles, cc)
            if stmt.module in {"slack_sdk.errors", "slack.errors"}:
                bindings.ctors.setdefault(local, False)
        return

    # Walrus can bind anywhere in the statement, including a container header.
    expressions = [node for child in ast.iter_child_nodes(stmt)
                   if not isinstance(child, (ast.stmt, ast.ExceptHandler))
                   for node in ast.walk(child)]
    for node in expressions:
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                _invalidate_constructor_object(bindings, node.func.value)
            for arg in list(node.args) + [kw.value for kw in node.keywords]:
                _invalidate_constructor_object(bindings, arg)
    for node in expressions:
        if isinstance(node, ast.NamedExpr) and isinstance(node.target, ast.Name):
            _bind_target(bindings, node.target.id, node.value, conditional,
                         events, _ln, cc)

    if isinstance(stmt, (ast.With, ast.AsyncWith)):
        for item in stmt.items:
            if isinstance(item.optional_vars, ast.Name):
                _bind_target(bindings, item.optional_vars.id,
                             item.context_expr, conditional, events, _ln, cc)
        return

    if isinstance(stmt, (ast.For, ast.AsyncFor)):
        # `for client in [WebClient(token=t)]:` -- the loop target holds the
        # client for the whole body. Untracked, this leaked with no finding at
        # all, because a plain-Name receiver is excluded from indirect
        # reporting.
        _bind_iter_target(stmt.target, _iter_verdict(stmt.iter, bindings),
                          bindings, conditional)
        if isinstance(stmt.target, ast.Name):
            values = _constructor_value(stmt.iter, bindings)
            if isinstance(values, tuple):
                if values:
                    roles = frozenset().union(*(v if isinstance(v, frozenset)
                                                else _OTHER_CTOR for v in values))
                    _set_constructor(bindings, stmt.target.id, roles)
            else:
                _set_constructor(bindings, stmt.target.id, _UNKNOWN_CTOR, True)
        return

    if isinstance(stmt, ast.Delete):
        for target in stmt.targets:
            if isinstance(target, ast.Name):
                _shadow(bindings, target.id)
        return

    if isinstance(stmt, ast.AugAssign) and isinstance(stmt.target, ast.Name):
        _set_constructor(bindings, stmt.target.id, _UNKNOWN_CTOR, cc)
        return

    targets = stmt.targets if isinstance(stmt, ast.Assign) else [getattr(stmt, 'target', None)]
    for target in targets:
        if isinstance(target, ast.Subscript):
            _invalidate_constructor_object(bindings, target.value)

    if isinstance(stmt, ast.ExceptHandler):
        # `except Exception as client:` rebinds the name to an EXCEPTION. Left
        # untracked, a stale Slack binding survived into the handler and
        # commented `client.files_upload_v2(...)` there (measured head=1,
        # base=0) -- an AttributeError at runtime, not a post. Forced to False
        # rather than routed through the conditional guard, because when the
        # handler runs the rebind definitely happened.
        if stmt.name:
            _set_constructor(bindings, stmt.name, _OTHER_CTOR)
            bindings[stmt.name] = False
            bindings.weak.discard(stmt.name)
            _invalidate_prefix(bindings, stmt.name)
        return

    if isinstance(stmt, ast.AnnAssign):
        if isinstance(stmt.target, ast.Name) and stmt.value is not None:
            _bind_target(bindings, stmt.target.id, stmt.value, conditional,
                         events, _ln, cc)
        return

    if not isinstance(stmt, ast.Assign):
        return

    value = stmt.value
    for target in stmt.targets:
        if isinstance(target, ast.Name):
            _bind_target(bindings, target.id, value, conditional, events, _ln, cc)
        elif isinstance(target, ast.Attribute):
            # `cfg.slack_webhook = WebhookClient(url=u)` -- a client parked on a
            # config object. Only Name-rooted chains are tracked; anything with
            # a subscript or call in the path stays unresolved.
            dotted = _dotted(target)
            if dotted:
                _bind_target(bindings, dotted, value, conditional, events, _ln, cc)
        elif isinstance(target, (ast.Tuple, ast.List)):
            # `c, d = WebClient(token=t), 1` -- zip positionally.
            elts = (value.elts if isinstance(value, (ast.Tuple, ast.List))
                    else None)
            for idx, element in enumerate(target.elts):
                if not isinstance(element, ast.Name):
                    continue
                if elts is not None and idx < len(elts):
                    _bind_target(bindings, element.id, elts[idx], conditional,
                                 events, _ln, cc)
                else:
                    _bind(bindings, element.id, None, conditional)


def _arg_names(fn) -> List[str]:
    a = fn.args
    names = [x.arg for x in list(a.posonlyargs) + list(a.args) + list(a.kwonlyargs)]
    for extra in (a.vararg, a.kwarg):
        if extra is not None:
            names.append(extra.arg)
    return names


def _shadow_constructor(state: Bindings, name: str) -> None:
    """Hide a constructor while retaining uncertainty about tracked names."""
    prior = _constructor_roles(ast.Name(id=name), state)
    tracked = name in state.ctors
    state.ctors.pop(name, None)
    # Shadowing is uncertainty, not evidence that a tracked constructor is
    # ordinary. A visible caller/default replaces this state; callers outside
    # this cell must not silently disable credential/outbound protection.
    roles = _UNKNOWN_CTOR
    if prior & {'token', 'webhook'}:
        roles |= prior
    elif tracked:
        roles |= _TOKEN_CTOR
    state.ctor_values[name] = roles
    state.ctor_functions.pop(name, None)
    state.modules.discard(name)


def _shadow(state: Bindings, name: str) -> None:
    """A local/parameter name hides whatever the enclosing scope bound."""
    state.pop(name, None)
    state.weak.discard(name)
    state.stale.discard(name)
    _shadow_constructor(state, name)
    for dotted in [k for k in state if k.startswith(name + ".")]:
        del state[dotted]


def _union_states(states: List[Bindings]) -> Bindings:
    """Fold several binding states together, Slack winning.

    A function called more than once runs at EVERY caller's state, not just the
    last one. Judging it against a single final state let this post:

        client = WebhookClient(...)
        def inner(): client.send(...)
        inner()                       # <- Slack here
        client = KafkaProducer(b)
        inner()                       # the later rebind hid the call above
    """
    merged = Bindings()
    constructor_names = set().union(*(set(st.ctors) | set(st.ctor_values) for st in states))
    for st in states:
        for name in constructor_names:
            value = _constructor_value(ast.Name(id=name), st)
            old = merged.ctor_values.get(name)
            if old is None or old == value:
                merged.ctor_values[name] = value
            else:
                merged.ctor_values[name] = (
                    (old if isinstance(old, frozenset) else _OTHER_CTOR)
                    | (value if isinstance(value, frozenset) else _OTHER_CTOR))
            roles = merged.ctor_values[name]
            if name in st.ctors or name in merged.ctors or (
                    isinstance(roles, frozenset) and roles & {"token", "webhook"}):
                merged.ctors[name] = (isinstance(roles, frozenset)
                                      and bool(roles & {"token", "webhook"}))
        merged.modules.update(st.modules)
        for name, verdict in st.items():
            if verdict is True:
                merged[name] = True
                if st.is_weak(name):
                    merged.weak.add(name)
                # Doubt is contagious on purpose: if ANY caller's state had the
                # verdict only via the fail-safe, the union is not conclusive
                # either, so the method must still decide.
                if name in st.stale:
                    merged.stale.add(name)
            elif name not in merged:
                merged[name] = verdict
    for name in set().union(*(set(st.ctor_functions) for st in states)):
        candidates = []
        for st in states:
            value = st.ctor_functions.get(name)
            for candidate in value if isinstance(value, tuple) else (value,):
                if not any(candidate is old for old in candidates):
                    candidates.append(candidate)
        merged.ctor_functions[name] = (candidates[0] if len(candidates) == 1
                                       else tuple(candidates))
    return merged


def _own_statements(fn) -> List[ast.stmt]:
    """Statements belonging to THIS function, excluding nested definitions.

    `ast.walk` swept nested defs into the parent's summary, so a never-called
    inner `_reset()` doing `global client; client = None` cancelled the
    initializer's real effect.
    """
    out: List[ast.stmt] = []

    def descend(body):
        for stmt in body:
            if isinstance(stmt, _SCOPE_BARRIERS):
                continue                      # a nested scope owns its effects
            out.append(stmt)
            for field in ("body", "orelse", "finalbody"):
                inner = getattr(stmt, field, None)
                if isinstance(inner, list):
                    descend([x for x in inner if isinstance(x, ast.stmt)])
            for handler in getattr(stmt, "handlers", None) or []:
                descend(handler.body)

    descend([x for x in fn.body if isinstance(x, ast.stmt)])
    return out


def _walk_in_source_order(body: List[ast.stmt], conditional: bool = False):
    """Yield ``(statement, conditional)`` in source order.

    Scope barriers are yielded but not descended into, so a caller can register
    a definition at exactly the point it appears — including one nested under
    an `if`, which a direct `fn.body` scan never saw.  The conditional flag is
    retained so a branch-local definition augments the possible environment
    instead of incorrectly replacing the definition from another path.
    """
    for stmt in body:
        yield stmt, conditional
        if isinstance(stmt, _SCOPE_BARRIERS):
            continue
        nested_conditional = conditional or isinstance(stmt, _CONDITIONAL_STMTS)
        for field in ("body", "orelse", "finalbody"):
            inner = getattr(stmt, field, None)
            if isinstance(inner, list):
                yield from _walk_in_source_order(
                    [x for x in inner if isinstance(x, ast.stmt)],
                    nested_conditional)
        for handler in getattr(stmt, "handlers", None) or []:
            yield from _walk_in_source_order(handler.body, True)


def _register_local_definition(defs: Dict[str, object], stmt: ast.stmt,
                               conditional: bool) -> None:
    """Register a local definition with Python-compatible overwrite rules.

    A later unconditional ``def`` replaces an earlier definition of the same
    name.  A definition under control flow is different: static analysis does
    not know whether that branch ran, so both the prior value and the branch's
    definition remain possible and their effects must be unioned at a call.
    """
    name = getattr(stmt, "name", "")
    if not name:
        return
    if not conditional:
        defs[name] = stmt
        return

    previous = defs.get(name)
    if previous is None:
        defs[name] = (stmt,)
        return
    candidates = previous if isinstance(previous, tuple) else (previous,)
    if all(candidate is not stmt for candidate in candidates):
        defs[name] = candidates + (stmt,)


def _definition_candidates(value: object) -> Tuple[object, ...]:
    """Return every definition that may be active for a local name."""
    return value if isinstance(value, tuple) else (value,)


def _nonlocal_effects_of_def(fn, bindings: Bindings) -> Dict[str, object]:
    """Names this function rebinds in its ENCLOSING scope via `nonlocal`.

    Only `global` was recognised, so a nested `bind()` doing
    `nonlocal client; client = WebhookClient(...)` was analysed inside its own
    scope and never propagated back to the caller — missing the post in one
    direction and commenting a harmless Kafka send in the other.
    """
    own = _own_statements(fn)
    declared = {name for stmt in own
                if isinstance(stmt, ast.Nonlocal) for name in stmt.names}
    if not declared:
        return {}
    seen: Dict[str, List[object]] = {}
    for stmt in own:
        pairs = []
        if isinstance(stmt, ast.Assign):
            pairs = [(t, stmt.value) for t in stmt.targets]
        elif isinstance(stmt, ast.AnnAssign) and stmt.value is not None:
            pairs = [(stmt.target, stmt.value)]
        for target, value in pairs:
            if isinstance(target, ast.Name) and target.id in declared:
                seen.setdefault(target.id, []).append(
                    _value_verdict(value, bindings, target.id))
    out: Dict[str, object] = {}
    for name, verdicts in seen.items():
        if any(v is True for v in verdicts):
            out[name] = True
        elif any(v is WEAK for v in verdicts):
            out[name] = WEAK
        elif all(v is False for v in verdicts):
            out[name] = False
        else:
            out[name] = None
    return out


def _effects_of_def(fn, bindings: Bindings,
                    defs: Optional[Dict[str, object]] = None,
                    aliases: Optional[Dict[str, str]] = None,
                    _seen: Optional[Set[int]] = None) -> Dict[str, object]:
    """Module-level names this ONE definition assigns via `global`.

    Branches are UNIONED with Slack winning — collapsing to the last assignment
    let `if flag: client = WebhookClient(...) else: client = None` be summarised
    as `None`, and `init(True)` then posted for real.

    `bindings` is the environment visible at the CALL SITE, so constructor
    aliases resolve (`from slack_sdk import WebClient as W`).
    """
    own = _own_statements(fn)
    # Read `global` declarations from the OWN statements themselves. Walking
    # each statement re-entered definitions nested under an `if`/`try`/loop, so
    # a never-called inner function's `global client` made the enclosing
    # function's ordinary local `client = None` look like a module rebind.
    declared = {name for stmt in own
                if isinstance(stmt, ast.Global) for name in stmt.names}
    if not declared:
        return {}
    seen: Dict[str, List[object]] = {}
    for stmt in own:
        pairs = []
        if isinstance(stmt, ast.Assign):
            pairs = [(t, stmt.value) for t in stmt.targets]
        elif isinstance(stmt, ast.AnnAssign) and stmt.value is not None:
            pairs = [(stmt.target, stmt.value)]
        for target, value in pairs:
            if isinstance(target, ast.Name) and target.id in declared:
                seen.setdefault(target.id, []).append(
                    _value_verdict(value, bindings, target.id))
    effects: Dict[str, object] = {}
    for name, verdicts in seen.items():
        if any(v is True for v in verdicts):
            effects[name] = True          # Slack wins across branches
        elif any(v is WEAK for v in verdicts):
            effects[name] = WEAK
        elif all(v is False for v in verdicts):
            effects[name] = False
        else:
            effects[name] = None
    return effects


def _transitive_effects(fn, bindings: Bindings,
                        defs: Optional[Dict[str, object]] = None,
                        aliases: Optional[Dict[str, str]] = None,
                        _seen: Optional[Set[int]] = None) -> Dict[str, object]:
    """`fn`'s own global effects, plus those of the functions it calls.

    Summarising only the assignments syntactically present in the callee missed
    one level of indirection entirely:

        def init():   global client; client = WebhookClient(...)
        def wrapper(): init()
        wrapper()
        client.send(...)          # executed for real

    Recursion is bounded by `_seen`, so mutual recursion terminates.
    """
    _seen = _seen or set()
    if id(fn) in _seen:
        return {}
    _seen.add(id(fn))
    merged = dict(_effects_of_def(fn, bindings))

    # The callee's OWN environment. `defs`/`aliases` describe the CALLER's
    # scope, so a helper defined or aliased inside the wrapper was invisible:
    #     def wrapper():
    #         def init(): global client; client = WebhookClient(...)
    #         init()
    # `_own_statements` also excludes that nested definition, so the call could
    # not be resolved at all. Enclosing names remain visible underneath.
    # ONE source-order traversal: the environment is built and consulted as we
    # go, so a call resolves against the definitions and aliases active AT THAT
    # STATEMENT. Scanning the whole body first and then resolving used the
    # FINAL environment, which let a later `f = clear` or a later redefinition
    # of `set_client` retroactively change what an earlier call meant — and
    # skipped definitions nested under an `if` entirely.
    local_defs: Dict[str, object] = dict(defs or {})
    local_aliases: Dict[str, str] = dict(aliases or {})
    for stmt, stmt_conditional in _walk_in_source_order(
            [x for x in fn.body if isinstance(x, ast.stmt)]):
        if isinstance(stmt, _SCOPE_BARRIERS):
            _register_local_definition(local_defs, stmt, stmt_conditional)
            continue
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if not isinstance(target, ast.Name):
                    continue
                if (isinstance(stmt.value, ast.Name)
                        and (stmt.value.id in local_defs
                             or stmt.value.id in local_aliases)):
                    local_aliases[target.id] = local_aliases.get(
                        stmt.value.id, stmt.value.id)
                else:
                    local_aliases.pop(target.id, None)
        for called in _called_names(stmt, local_aliases):
            callee = local_defs.get(called)
            if callee is None:
                continue
            for candidate in _definition_candidates(callee):
                if candidate is fn:
                    continue
                for name, verdict in _transitive_effects(
                        candidate, bindings, local_defs, local_aliases,
                        _seen).items():
                    # Slack wins: an inner initializer that installs a client
                    # is not cancelled by another possible branch.
                    if merged.get(name) is True:
                        continue
                    if verdict is True or name not in merged:
                        merged[name] = verdict
    return merged


def _called_names(stmt: ast.stmt,
                  aliases: Optional[Dict[str, str]] = None) -> Set[str]:
    """Names invoked by this statement, resolved through function aliases.

    `f = inner` then `f()` recorded `f`, while the call-state lookup expected
    `inner` — so the closure saw no call site at all and was judged against the
    scope's final state.
    """
    aliases = aliases or {}
    out = set()
    for node in ast.walk(stmt):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            name = node.func.id
            out.add(aliases.get(name, name))
    return out


def _escaping_function_names(stmt: ast.stmt, known: Set[str],
                             aliases: Optional[Dict[str, str]] = None) -> Set[str]:
    """Function names used as VALUES here rather than called directly.

    `executor.submit(inner)`, `[inner]`, `cb = inner` handed onwards — the
    invocation state is unknowable, so the callee must be judged conservatively.
    A plain `alias = inner` is excluded; that is tracked precisely instead.
    """
    aliases = aliases or {}
    if isinstance(stmt, ast.Assign) and isinstance(stmt.value, ast.Name):
        # Suppress ONLY when every target is a plain name, because that is the
        # only shape alias tracking actually records. `box.callback = inner`
        # and `callbacks[0] = inner` were treated as tracked aliases while
        # being recorded nowhere, so the function was neither aliased nor
        # escaped and its Slack call stayed live.
        if all(isinstance(t, ast.Name) for t in stmt.targets):
            return set()
    called_directly = {node.func.id for node in ast.walk(stmt)
                       if isinstance(node, ast.Call)
                       and isinstance(node.func, ast.Name)}
    out = set()
    # A function stored into an attribute or subscript can be invoked through
    # that container later; treat it as escaping.
    if isinstance(stmt, ast.Assign):
        for target in stmt.targets:
            if isinstance(target, (ast.Attribute, ast.Subscript, ast.Tuple,
                                   ast.List)):
                for node in ast.walk(stmt.value):
                    if isinstance(node, ast.Name):
                        canonical = aliases.get(node.id, node.id)
                        if canonical in known:
                            out.add(canonical)
    for node in ast.walk(stmt):
        if not (isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)):
            continue
        if node.id in called_directly:
            continue
        # Canonicalise first: `executor.submit(f)` where `f = inner` escapes
        # `inner`, but the raw name `f` is not a known definition.
        canonical = aliases.get(node.id, node.id)
        if canonical in known:
            out.add(canonical)
    return out


def _evolve_scope(body: List[ast.stmt], state: Bindings,
                  snapshots: Dict[int, Bindings],
                  conditional: bool = False, class_enclosing=None) -> None:
    """Evolve one lexical scope in source order, then recurse into nested ones.

    TWO PHASES, and the order is the whole point:

      1. walk this scope's own statements, snapshotting each against the state
         as of that statement;
      2. only then descend into nested functions, seeding them with this
         scope's FINAL state.

    Phase 2 must not use the state at the `def`, because Python resolves a
    closure's free variables when the function RUNS, not when it is defined:

        def outer():
            def inner():
                client.send(message)     # resolved at call time
            client = WebhookClient(...)  # ...bound after the def
            inner()

    Seeding `inner` at its `def` missed that entirely. The inverse misfired too:
    a client bound BEFORE the def and rebound to Kafka after it had the
    harmless Kafka send commented. Using the enclosing scope's final state is
    the same rule module-level functions already use, and it is conservative in
    both directions.

    A CLASS body is different: it executes AT the `class` statement, so it is
    seeded with the state right there and evolved in source order.
    """
    deferred: List[Tuple[ast.stmt, Bindings]] = []
    call_states: Dict[str, List[Bindings]] = {}
    defs_by_name: Dict[str, object] = {}
    nonlocal_effects: Dict[str, object] = {}
    aliases: Dict[str, str] = {}
    escaped: Set[str] = set()
    every_state: List[Bindings] = []
    for stmt, cond in _walk_constructor_paths(body, state, conditional):
        snapshots[id(stmt)] = state._child()
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            defs_by_name[stmt.name] = stmt      # source order: latest wins here
            nonlocal_effects[stmt.name] = stmt
            deferred.append((stmt, None))
            _bind_constructor_definition(state, stmt)
            continue
        if isinstance(stmt, ast.ClassDef):
            defs_by_name[stmt.name] = stmt
            deferred.append((stmt, state._child()))   # immediate execution
            _bind_constructor_definition(state, stmt)
            continue
        every_state.append(state._child())
        # `f = inner`, and `g = f` — aliases chain, so resolve transitively.
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if not isinstance(target, ast.Name):
                    continue
                if (isinstance(stmt.value, ast.Name)
                        and (stmt.value.id in defs_by_name
                             or stmt.value.id in aliases)):
                    aliases[target.id] = aliases.get(stmt.value.id,
                                                     stmt.value.id)
                else:
                    aliases.pop(target.id, None)   # stale alias, reassigned
        escaped |= _escaping_function_names(stmt, set(defs_by_name), aliases)
        for called in _called_names(stmt, aliases):
            call_states.setdefault(called, []).append(state._child())
        _apply_stmt(stmt, state, cond, None, defs_by_name, aliases, ctor_conditional=False)
        # A nested function invoked here can rebind one of THIS scope's names
        # through `nonlocal`.
        for called in _called_names(stmt, aliases):
            nested = nonlocal_effects.get(called)
            if nested is None:
                continue
            for name, verdict in _nonlocal_effects_of_def(nested, state).items():
                _bind(state, name, verdict, cond)
            for name, value in _constructor_effects(
                    nested, state, nonlocal_effects, declaration=ast.Nonlocal).items():
                _set_constructor(state, name, value, cond)

    for stmt, at_def in deferred:
        if isinstance(stmt, ast.ClassDef):
            seed = at_def if at_def is not None else state._child()
        else:
            sites = call_states.get(stmt.name) or []
            if stmt.name in escaped:
                # Handed to a callback/container: the invocation state is
                # unknowable, so union every state this scope passed through.
                seed = _union_states(every_state + sites + [state])
            elif sites:
                # Direct call sites are exact execution-time evidence.  Do not
                # mix in the enclosing scope's later/final state: that made a
                # Kafka-only call look Slack merely because the same variable
                # was rebound to WebClient after the call returned.
                seed = _union_states(sites)
            else:
                seed = state._child()
            if class_enclosing is not None:
                _copy_constructor_state(seed, class_enclosing)
            for name in _arg_names(stmt):
                _shadow(seed, name)
            _shadow_constructor_locals(stmt, seed)
            _seed_constructor_parameters(stmt, body, snapshots, seed,
                                         escaped=stmt.name in escaped)
        _evolve_scope([x for x in stmt.body if isinstance(x, ast.stmt)],
                      seed, snapshots, class_enclosing=state if isinstance(stmt, ast.ClassDef) else None)


def _apply_lexical_scopes(tree: ast.AST, snapshots: Dict[int, Bindings],
                          module_final: Bindings) -> None:
    """Open a lexical scope for every function and class reachable at module
    scope, then recurse."""
    body = [x for x in getattr(tree, "body", []) if isinstance(x, ast.stmt)]
    call_states: Dict[str, List[Bindings]] = {}
    known: Set[str] = {getattr(n, "name", "") for n, _ in
                       _walk_module_scope(body, yield_barriers=True)
                       if isinstance(n, _SCOPE_BARRIERS)}
    aliases: Dict[str, str] = {}
    escaped: Set[str] = set()
    all_states: List[Bindings] = []
    for stmt, _c in _walk_module_scope(body, yield_barriers=True):
        snap = snapshots.get(id(stmt))
        if snap is None or isinstance(stmt, _SCOPE_BARRIERS):
            continue
        all_states.append(snap)
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if not isinstance(target, ast.Name):
                    continue
                if (isinstance(stmt.value, ast.Name)
                        and (stmt.value.id in known or stmt.value.id in aliases)):
                    aliases[target.id] = aliases.get(stmt.value.id, stmt.value.id)
                else:
                    aliases.pop(target.id, None)
        escaped |= _escaping_function_names(stmt, known, aliases)
        for called in _called_names(stmt, aliases):
            call_states.setdefault(called, []).append(snap)

    for stmt, _cond in _walk_module_scope(body, yield_barriers=True):
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            sites = call_states.get(stmt.name) or []
            if stmt.name in escaped:
                seed = _union_states(all_states + sites + [module_final])
            elif sites:
                seed = _union_states(sites)
            else:
                seed = module_final._child()
            for name in _arg_names(stmt):
                _shadow(seed, name)
            _shadow_constructor_locals(stmt, seed)
            _seed_constructor_parameters(stmt, body, snapshots, seed,
                                         escaped=stmt.name in escaped)
            _evolve_scope([x for x in stmt.body if isinstance(x, ast.stmt)],
                          seed, snapshots)
        elif isinstance(stmt, ast.ClassDef):
            at_def = snapshots.get(id(stmt))
            seed = at_def._child() if at_def is not None else module_final._child()
            _evolve_scope([x for x in stmt.body if isinstance(x, ast.stmt)],
                          seed, snapshots, class_enclosing=module_final)


def _apply_loop_fixed_point(tree: ast.AST, snapshots: Dict[int, Bindings]) -> None:
    """A loop body runs more than once, so it must not be judged as if it ran once.

        for i in range(2):
            if i:
                client.files_upload_v2(file=f)   # iteration 2: bound already
            client = WebClient(token=t)

    Judged in pure source order, the post sees `client` unbound and is only
    reported -- on the rationale that it would raise NameError on a fresh
    kernel. Inside a loop that rationale is simply false: iteration 1 binds and
    iteration 2 posts, on a completely fresh kernel.

    The fix is the loop's fixed point: every statement in the body also sees
    whatever the body itself binds. Only names the body binds to a SLACK client
    are added, so a Kafka rebind inside a loop still changes nothing.
    """
    for node in ast.walk(tree):
        if not isinstance(node, (ast.For, ast.AsyncFor, ast.While)):
            continue
        at_loop = snapshots.get(id(node))
        if at_loop is None:
            continue                    # inside a def -- uses the final state
        body = [x for x in list(node.body) + list(node.orelse)
                if isinstance(x, ast.stmt)]
        inner = [st for st, _ in _walk_module_scope(body, True)]
        if not inner:
            continue
        probe = at_loop._child()
        for st in inner:
            _apply_stmt(st, probe, True, None)
        gained = {n for n, v in probe.items()
                  if v is True and at_loop.get(n) is not True}
        gained_ctors = {name for name in probe.ctors
                        if _constructor_roles(ast.Name(id=name), probe) & {'token', 'webhook'}
                        and not _constructor_roles(ast.Name(id=name), at_loop) & {'token', 'webhook'}}
        if not gained and not gained_ctors:
            continue
        for st in inner:
            snap = snapshots.get(id(st))
            if snap is None:
                continue
            for name in gained_ctors:
                roles = (_constructor_roles(ast.Name(id=name), snap)
                         | _constructor_roles(ast.Name(id=name), probe))
                _set_constructor(snap, name, roles)
            for name in gained:
                snap[name] = True
                if probe.is_weak(name):
                    snap.weak.add(name)


def _evolve(tree: ast.AST, bindings: Bindings
            ) -> Tuple[Dict[int, Bindings], Bindings,
                       List[Tuple[str, int]], Set[int]]:
    """Walk one cell in source order, returning per-statement snapshots.

    Returns (snapshots, final_state, laundering_events, module_scope_ids).

    `snapshots[id(stmt)]` is the state AS OF that statement, before its own
    effect -- which is what the interpreter would see. Statements inside
    def/class bodies get no snapshot; the caller uses the end-of-cell state for
    those, because a function can be called at any later point.

    Takes a TREE, not source: the snapshot keys are `id(stmt)`, so they must
    come from the very tree the caller classifies against.
    """
    snapshots: Dict[int, Bindings] = {}
    events: List[Tuple[str, int]] = []
    state = bindings._child()
    body = [s for s in getattr(tree, "body", []) if isinstance(s, ast.stmt)]
    _defs: Dict[str, object] = {}
    _aliases: Dict[str, str] = {}
    for stmt, conditional in _walk_constructor_paths(body, state):
        snapshots[id(stmt)] = state._child()
        if isinstance(stmt, _SCOPE_BARRIERS):
            _defs[getattr(stmt, "name", "")] = stmt
            _bind_constructor_definition(state, stmt)
            continue          # snapshot only, so nested scopes can seed here
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if not isinstance(target, ast.Name):
                    continue
                if (isinstance(stmt.value, ast.Name)
                        and (stmt.value.id in _defs or stmt.value.id in _aliases)):
                    _aliases[target.id] = _aliases.get(stmt.value.id,
                                                       stmt.value.id)
                else:
                    _aliases.pop(target.id, None)
        _apply_stmt(stmt, state, conditional, events, _defs, _aliases, ctor_conditional=False)
    module_ids = {k for k in snapshots}
    # Function bodies AFTER the module pass, so they can be seeded with the
    # module's final state. Loop fixed points last, so they see both.
    _apply_lexical_scopes(tree, snapshots, state)
    _apply_loop_fixed_point(tree, snapshots)
    return snapshots, state, events, module_ids


def update_bindings(source: str, bindings: Bindings) -> None:
    """Fold one cell into the notebook-wide binding state, in source order.

    CALL ORDER MATTERS. This must run AFTER `neutralize_cell` for the same
    cell. Running it first collapses the cell to its END state, so

        cell 41:  client = WebClient(token=t)
        cell 42:  client.files_upload_v2(...)   # <- must be neutralised
                  client = None                 # routine cleanup

    would see `client` already rebound and let the upload through.
    `neutralize_cell` re-derives in-cell bindings itself, so it does not need
    this to have run.
    """
    holder = _as_bindings(bindings)
    try:
        tree = ast.parse(_strip_magics(source))
    except SyntaxError:
        return                      # unparseable cell cannot inform bindings
    _, final, _, _ = _evolve(tree, holder)
    if holder is bindings:
        bindings.clear()
        bindings.update(final)
        bindings.ctors.clear()
        bindings.ctors.update(final.ctors)
        bindings.ctor_values.clear()
        bindings.ctor_values.update(final.ctor_values)
        bindings.ctor_functions.clear()
        bindings.ctor_functions.update(final.ctor_functions)
        bindings.modules.clear()
        bindings.modules.update(final.modules)
        # Weakness MUST cross the cell boundary too. Without this a binding
        # from a slack-named factory becomes STRONG in the next cell, and
        # `cfg = get_slack_config()` in cell 3 gets `cfg.get("channel")`
        # commented in cell 4 -- the exact false positive the weak tier exists
        # to prevent, just one cell later.
        bindings.weak.clear()
        bindings.weak.update(final.weak)
        # Uncertainty crosses the cell boundary for the same reason weakness
        # does, and the incident depends on it: the `%run` child binds `client`
        # in one cell, the conditional rebind happens in another, and the
        # MLflow calls run in a third. Dropping `stale` here would make the
        # binding look conclusive again in every later cell and re-comment
        # them -- the sticky misclassification measured on cells #58 and #60.
        bindings.stale.clear()
        bindings.stale.update(final.stale)


def _dotted(node: ast.expr) -> Optional[str]:
    """`cfg.slack_webhook` -> 'cfg.slack_webhook'.

    None unless the whole chain is rooted at a plain Name, so a subscript or a
    call anywhere in the path (`d["k"].x`, `f().x`) stays unresolved and is
    reported rather than guessed at.
    """
    parts: List[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if not isinstance(node, ast.Name):
        return None
    parts.append(node.id)
    return ".".join(reversed(parts))


def _receiver_name(call: ast.Call) -> Optional[str]:
    """Receiver of a method call, as a binding key.

    `client.foo(...)`            -> 'client'
    `cfg.slack_webhook.foo(...)` -> 'cfg.slack_webhook'

    Dotted receivers are resolved because a client is routinely parked on a
    config object. Leaving them unresolved meant `cfg.slack_webhook.send(...)`
    was only ever REPORTED, while the regex path commented it.
    """
    fn = call.func
    if isinstance(fn, ast.Attribute):
        return _dotted(fn.value)
    return None


def _is_inline_slack_ctor(call: ast.Call, bindings: "Bindings") -> bool:
    """`WebClient(token=t).chat_postMessage(...)` — construction then call."""
    fn = call.func
    return (isinstance(fn, ast.Attribute) and isinstance(fn.value, ast.Call)
            and _ctor_verdict(fn.value, bindings) is True)


_CONTAINERS = (ast.Module, ast.If, ast.For, ast.AsyncFor, ast.While, ast.With,
               ast.AsyncWith, ast.Try, ast.FunctionDef, ast.AsyncFunctionDef,
               ast.ClassDef, ast.ExceptHandler)


def _index_bodies(tree: ast.AST) -> Tuple[Dict[int, int], Dict[int, List[ast.stmt]]]:
    """Map each statement to the body it belongs to.

    Needed so that commenting out the ONLY statement in a block can insert
    `pass` in the same edit. The current implementation emits an empty `if`
    and leaves the model to repair it with `pass` afterwards.
    """
    stmt_to_body: Dict[int, int] = {}
    bodies: Dict[int, List[ast.stmt]] = {}
    for node in ast.walk(tree):
        for field in ("body", "orelse", "finalbody"):
            body = getattr(node, field, None)
            if not isinstance(body, list):
                continue
            stmts = [s for s in body if isinstance(s, ast.stmt)]
            if not stmts:
                continue
            bodies[id(body)] = stmts
            for s in stmts:
                stmt_to_body[id(s)] = id(body)
    return stmt_to_body, bodies


def _innermost_statements(tree: ast.AST) -> List[ast.stmt]:
    """Every statement that is not itself a container.

    Containers are skipped so that a whole `try:` block is never commented out
    along with its handler — only the offending statement inside it.
    """
    return [n for n in ast.walk(tree)
            if isinstance(n, ast.stmt) and not isinstance(n, _CONTAINERS)]


def _mask_comments(text: str) -> str:
    """Blank out `#` comments, keeping string literals intact.

    Tier-1 matches on statement TEXT, so a trailing comment could comment out
    working code:

        rows = df.count()   # results used to go to hooks.slack.com

    String literals are deliberately NOT masked. `hooks.slack.com` inside a
    string is a real post target --
    `requests.post("https://hooks.slack.com/...")` -- so masking literals would
    turn a false positive into a false negative, which is the wrong direction.
    """
    try:
        import io
        import tokenize
        out = text
        spans = []
        for tok in tokenize.generate_tokens(io.StringIO(
                textwrap.dedent(text)).readline):
            if tok.type == tokenize.COMMENT:
                spans.append(tok.start[0] - 1)
        if not spans:
            return text
        lines = text.splitlines()
        for row in spans:
            if row < len(lines):
                line = lines[row]
                # Strip from the LAST `#` that is not inside a quote. The
                # tokenizer already told us there is a comment on this row.
                depth_s = depth_d = False
                cut = None
                for pos, ch in enumerate(line):
                    if ch == "'" and not depth_d:
                        depth_s = not depth_s
                    elif ch == '"' and not depth_s:
                        depth_d = not depth_d
                    elif ch == "#" and not depth_s and not depth_d:
                        cut = pos
                        break
                if cut is not None:
                    lines[row] = line[:cut]
        return "\n".join(lines)
    except Exception:
        return text          # unparseable fragment -- fail safe to raw text


def _mask_strings(text: str) -> str:
    """Blank out string-literal CONTENT, keeping the quotes and line count.

    Used only for tier-1 CODE patterns: `.chat_postMessage(` inside a docstring
    or a log message is not an executable call.
    """
    try:
        import io
        import tokenize
        # Work entirely in dedented space: tokenize needs it, and the result is
        # only ever regex-searched, never used to edit the cell. Masking must be
        # COLUMN-accurate -- blanking whole rows that merely contain a string
        # silently swallowed real calls (`client.chat_postMessage(text='x')`
        # stopped matching, a false negative).
        dedented = textwrap.dedent(text)
        rows = dedented.splitlines()
        spans: List[Tuple[int, int, int]] = []       # (row, start_col, end_col)
        for tok in tokenize.generate_tokens(io.StringIO(dedented).readline):
            if tok.type != tokenize.STRING:
                continue
            (r0, c0), (r1, c1) = tok.start, tok.end
            for row in range(r0 - 1, r1):
                if row >= len(rows):
                    break
                start = c0 if row == r0 - 1 else 0
                end = c1 if row == r1 - 1 else len(rows[row])
                spans.append((row, start, end))
        for row, start, end in spans:
            line = rows[row]
            start, end = max(0, start), min(len(line), end)
            if end > start:
                rows[row] = line[:start] + (" " * (end - start)) + line[end:]
        return "\n".join(rows)
    except Exception:
        return text          # unparseable fragment -- fail safe to raw text


def _is_slack_api_method(method: str) -> bool:
    """An unmistakable Slack Web API write, judged by name alone.

    `resource_action` where the RESOURCE is in Slack's closed namespace and the
    action is a recognised write: `files_upload_v2`, `chat_postMessage`,
    `conversations_create`. Used only where the receiver cannot be resolved --
    a function parameter, a local client, an unbound config attribute -- where
    the alternative is letting the post through. No non-Slack object plausibly
    exposes `files_upload_v2`.
    """
    if not method or "_" not in method:
        return False
    if _is_read_only(method):
        return False
    return (method.split("_", 1)[0] in _SLACK_RESOURCES
            and bool(_KNOWN_WRITE_RE.search(method)))


# Line-level last resort for cells the AST cannot parse. Deliberately broader
# than the old regex allowlist, which knew ~8 method names and missed
# files_upload_v2 entirely.
_FAILSAFE_CALL_RE = re.compile(r"\.\s*([A-Za-z_]\w*)\s*\(")


# `<receiver>.<method>(` and a bare `<receiver>.<method>` reference. The bare
# form matters: `post = client.send` hands the bound method somewhere else, and
# `(client.send)(...)` puts the parenthesis in the wrong place for a naive
# `.method(` search.
_FAILSAFE_ATTR_CALL_RE = re.compile(
    r"([A-Za-z_][\w.]*)\s*\.\s*([A-Za-z_]\w*)\s*\(")
_FAILSAFE_ATTR_REF_RE = re.compile(
    r"([A-Za-z_][\w.]*)\s*\.\s*([A-Za-z_]\w*)")
_FAILSAFE_NAME_CALL_RE = re.compile(r"\b([A-Za-z_]\w*)\s*\(")


def _line_has_external_post(line: str, cell_evidence: bool = False,
                            bound: Optional[Set[str]] = None,
                            tainted: Optional[Set[str]] = None) -> bool:
    """Does this logical statement look like it performs an external post?

    `cell_evidence` is Slack evidence found ANYWHERE in the cell. It has to be
    cell-level: the client is routinely constructed on one line and used on
    another, so requiring the evidence on the same statement missed
    `client = WebhookClient()` / `client.send(...)` entirely.
    """
    active = _mask_strings(_mask_comments(line))
    if not active.strip():
        return False
    if _TIER1_RE.search(_mask_comments(line)):
        return True
    bound = bound or set()
    tainted = tainted or set()
    suspicious = cell_evidence or bool(_SLACK_SUSPICION_RE.search(active))

    # A call on a receiver the NOTEBOOK knows is a Slack client. Cross-cell
    # bindings were ignored here, so a client bound in an earlier cell and used
    # in an unparseable one was certified safe.
    for receiver, method in _FAILSAFE_ATTR_CALL_RE.findall(active):
        if _is_slack_api_method(method):
            return True
        if receiver in bound and not _is_read_only(method):
            return True
        if method in _TIER2_METHODS and suspicious:
            return True

    # A bound method handed around as a VALUE: `post = client.send`,
    # `(client.send)(...)`, `submit(client.send, ...)`.
    for receiver, method in _FAILSAFE_ATTR_REF_RE.findall(active):
        if receiver in bound and not _is_read_only(method):
            return True

    # Calling something previously assigned from a bound method.
    for name in _FAILSAFE_NAME_CALL_RE.findall(active):
        if name in tainted:
            return True
    return False


def _logical_lines(source: str) -> List[List[int]]:
    """Group physical line indices into LOGICAL lines.

    Joins backslash continuations and open brackets. Physical-line analysis
    missed a call split across lines:

        client.\\
            send(text="POSTED")

    -- neither physical line contains `.send(`, so the fail-closed pass saw
    nothing and the cell went to the kernel unchanged.
    """
    groups: List[List[int]] = []
    current: List[int] = []
    depth = 0
    continued = False
    for idx, line in enumerate(source.splitlines()):
        current.append(idx)
        code = _mask_comments(line)
        # Bracket depth, ignoring anything inside string literals.
        masked = _mask_strings(code)
        depth += masked.count("(") + masked.count("[") + masked.count("{")
        depth -= masked.count(")") + masked.count("]") + masked.count("}")
        depth = max(depth, 0)
        continued = code.rstrip().endswith("\\")
        if depth == 0 and not continued:
            groups.append(current)
            current = []
    if current:
        groups.append(current)
    return groups


def _failsafe_line_neutralise(source: str, bindings: Optional[Bindings] = None
                              ) -> Tuple[str, int, bool]:
    """Comment every LOGICAL statement that looks like an external post.

    Used only when the AST cannot parse the cell. Returns
    (new_source, commented, residual_evidence) -- residual_evidence True means
    Slack evidence survived even this pass, so the cell must not be executed.
    """
    lines = source.splitlines()
    out = list(lines)
    commented = 0
    cell_evidence = bool(_SLACK_SUSPICION_RE.search(_mask_comments(source)))
    bound = {n for n, v in (bindings or {}).items() if v is True}
    if bound:
        cell_evidence = True          # the notebook knows a client is live
    tainted: Set[str] = set()
    for group in _logical_lines(source):
        joined = " ".join(lines[i].strip().rstrip("\\") for i in group)
        if not joined.strip() or joined.strip().startswith("#"):
            continue
        # `post = client.send` taints `post` for the lines that follow.
        _act = _mask_strings(_mask_comments(joined))
        _asg = re.match(r"\s*([A-Za-z_]\w*)\s*=\s*(.+)$", _act)
        if _asg:
            for receiver, method in _FAILSAFE_ATTR_REF_RE.findall(_asg.group(2)):
                if receiver in bound and not _is_read_only(method):
                    tainted.add(_asg.group(1))
        if not _line_has_external_post(joined, cell_evidence, bound, tainted):
            continue
        for i in group:
            raw = lines[i]
            if raw.strip().startswith("#"):
                continue
            indent = raw[: len(raw) - len(raw.lstrip())]
            suffix = ANNOTATION if i == group[0] else ""
            out[i] = f"{indent}# {raw.strip()}{suffix}"
            commented += 1
    new_source = "\n".join(out)
    if source.endswith(("\n", "\r")):
        new_source += "\n"
    residual = False
    for group in _logical_lines(new_source):
        rows = new_source.splitlines()
        if any(rows[i].strip().startswith("#") for i in group):
            continue
        joined = " ".join(rows[i].strip().rstrip("\\") for i in group)
        if _line_has_external_post(joined, cell_evidence, bound, tainted):
            residual = True
            break
    return new_source, commented, residual


def _tier1_match(text: str) -> Tuple[Optional[str], bool]:
    """Shared tier-1 test. Returns (matched_label, code_pattern_in_string_only).

    Used by regular statements, control-flow HEADERS and magic tails alike.
    Headers previously had no tier-1 test at all, so `if send_slack(msg):`
    executed -- a safety regression against the regex path, which commented it.
    """
    hit = _TIER1_TEXT_RE.search(text)
    if hit:
        return hit.group(0)[:40].strip(), False
    hit = _TIER1_CODE_RE.search(_mask_strings(text))
    if hit:
        return hit.group(0)[:40].strip(), False
    return None, bool(_TIER1_CODE_RE.search(text))


def _classify(
    stmt: ast.stmt,
    bindings: Bindings,
    lines: List[str],
    include_generic: bool,
    at_module_scope: bool = True,
) -> Tuple[Optional[str], List[Tuple[str, str, int]]]:
    """Decide whether `stmt` posts externally.

    Returns (method_or_None, unprovable_findings).

    Reasons to comment, in order:
      A.  the receiver is a name bound to a Slack client   (ANY method)
      A'. a bound WRITE method passed as a value, never called here
      B.  the client is constructed inline in the same expression
      B'. tier-2: a slack-hinted receiver with NO known binding (regex-path
          parity -- covers a client from a %run child or a parameter)
      C.  a tier-1 text pattern matches -- unambiguous Slack

    Deliberately NOT a reason: a slack-ish receiver name whose binding is
    KNOWN to be something else. That is what commented out
    `webhook_producer.send(topic, msg)` on a Kafka producer.
    """
    unprovable: List[Tuple[str, str, int]] = []
    lineno = getattr(stmt, "lineno", 0)

    # A comprehension binds AND uses its target inside ONE statement, so the
    # per-statement snapshot cannot contain it:
    #     rows = [c.files_upload_v2(file=f) for c in [WebClient(token=t)]]
    generators = [gen
                  for node in ast.walk(stmt)
                  if isinstance(node, (ast.ListComp, ast.SetComp,
                                       ast.DictComp, ast.GeneratorExp))
                  for gen in node.generators]
    if generators:
        bindings = bindings._child()
        for gen in generators:
            _bind_iter_target(gen.target, _iter_verdict(gen.iter, bindings),
                              bindings, False)

    calls = [n for n in ast.walk(stmt) if isinstance(n, ast.Call)]
    call_func_ids = {id(c.func) for c in calls}

    for call in calls:
        # A — receiver bound to a Slack client
        recv = _receiver_name(call)
        if recv and bindings.is_slack(recv) and isinstance(call.func, ast.Attribute):
            method = call.func.attr
            if _is_read_only(method):
                # Reads have no side effect. Leave them running — the regex
                # path never touched them, and commenting one would turn
                # working code into a NameError.
                unprovable.append(
                    ("read-left-running",
                     f"{method}() is a read — not neutralised", lineno))
                continue
            if bindings.is_weak(recv) and not _KNOWN_WRITE_RE.search(method):
                # The binding came from a slack-ish FACTORY NAME, not a
                # constructor. `cfg = get_slack_config()` then
                # `cfg.get("channel")` is a dict read, and commenting it
                # broke working code the regex path left alone. Kept ahead of
                # the namespace gate below so this more specific diagnosis
                # (and its message) survives unchanged.
                unprovable.append(
                    ("weak-binding-nonwrite",
                     f"{recv}.{method}() — {recv} came from a slack-named "
                     "factory, not a constructor, and this is not a write "
                     "verb; left running, review manually", lineno))
                continue
            if not _acts_on_slack(recv, method, bindings):
                # The receiver is only BELIEVED Slack -- a contrary verdict was
                # discarded by the conditional fail-safe -- and the method is
                # not in Slack's namespace. That combination is a variable-name
                # collision, so report it and leave it RUNNING. Deleting it is a
                # guaranteed NameError on the next line, and the re-neutralise
                # pass after each LLM repair makes that unrecoverable.
                #
                # A receiver that was never contradicted stays fail-closed and
                # never reaches here, which is what keeps an unlisted family
                # like `canvases_delete` blocked.
                #
                # Deliberately BEFORE the _KNOWN_WRITE_RE check: that regex
                # substring-matches verbs, so MLflow's `update_run` hits
                # "update" and would be commented without ever reaching here.
                unprovable.append(
                    ("nonslack-method-on-slack-bound-name",
                     f"{recv}.{method}() — {recv} is not provably a Slack "
                     "client here and this is not a Slack method; left running "
                     "(probable variable-name collision)",
                     lineno))
                continue
            if not _KNOWN_WRITE_RE.search(method):
                unprovable.append(
                    ("unclassified-method",
                     f"{method}() neutralised but is not a recognised write "
                     "verb — confirm it is not a read", lineno))
            return method, unprovable

        # B — inline construction
        if (_is_inline_slack_ctor(call, bindings)
                and isinstance(call.func, ast.Attribute)):
            method = call.func.attr
            if _is_read_only(method):
                continue
            return method, unprovable

        # B' — tier-2 parity: slack-hinted receiver with NO binding evidence.
        #
        # Restricted to UNKNOWN receivers on purpose. `recv in bindings` means
        # we saw an assignment and found no Slack evidence, which is exactly
        # the `webhook_producer = KafkaProducer(...)` case. Method set and the
        # module-scope restriction are the regex path's, unchanged — widening
        # either would add false positives the old code did not have.
        if (include_generic and recv and recv not in bindings
                and isinstance(call.func, ast.Attribute)
                and call.func.attr in _TIER2_METHODS
                and _SLACK_HINT_RE.search(recv.rsplit(".", 1)[-1])):
            if at_module_scope:
                return call.func.attr, unprovable
            unprovable.append(
                ("hinted-receiver-in-function",
                 f"{recv}.{call.func.attr}() inside a function — slack-hinted "
                 "name with no visible binding; review manually", lineno))

        # B'' — receiver unresolvable, but the METHOD is unmistakably Slack.
        # Covers a client that is a function parameter, one constructed inside
        # a function, and an unbound `cfg.slack_webhook`. All three executed
        # the upload with no finding at all before this.
        # UNKNOWN receivers only. When the receiver is KNOWN to hold something
        # else the method name must not override that -- `except Exception as
        # client: client.files_upload_v2(...)` is an AttributeError, not a post.
        if (recv and recv not in bindings
                and isinstance(call.func, ast.Attribute)
                and _is_slack_api_method(call.func.attr)):
            unprovable.append(
                ("unresolved-slack-method",
                 f"{recv}.{call.func.attr}() — receiver could not be resolved, "
                 "but the method is a Slack Web API write; neutralised",
                 lineno))
            return call.func.attr, unprovable

        # D — record what cannot be proven, so it lands in the report
        fn = call.func
        if isinstance(fn, ast.Name) and fn.id == "getattr" and call.args:
            first = call.args[0]
            if isinstance(first, ast.Name) and bindings.is_slack(first.id):
                unprovable.append(
                    ("dynamic-dispatch",
                     f"getattr({first.id}, ...) on a Slack client", lineno))
        if isinstance(fn, ast.Attribute) and not isinstance(fn.value, ast.Name):
            # Report only when the call is PLAUSIBLY Slack. Reporting every
            # chained call floods the report: measured on the real
            # Monitoring_and_Posting, an unfiltered check produced 39 findings
            # — get(), strip(), mkdirs(), now(), cast(), sort() — and 98 on
            # Reporting_Excel, burying the findings that matter.
            #
            # The METHOD SHAPE test is what kills that flood (plain names never
            # match `resource_action`). Evidence is taken from the CELL, not
            # just this statement: keying it to the statement's own text hid
            # real leaks — `bot.client.files_upload_v2(...)` and
            # `clients["s"].files_upload_v2(...)` reported nothing at all.
            if bindings.is_known_other(_dotted(fn.value)):
                # Resolved, and resolved as NOT Slack (`cfg.kafka.send(...)`).
                # Reporting it would be noise -- there is nothing to review.
                continue
            text = _stmt_text(stmt, lines)
            # Resource prefix, not just `resource_action` shape. Shape alone
            # still admitted every underscored pandas/Spark method in a cell
            # that happened to import slack (to_dict, sort_values,
            # reset_index); the resource namespace is closed, so it does not.
            shaped = bool(
                (_SLACK_METHOD_SHAPE_RE.match(fn.attr)
                 and fn.attr.split("_", 1)[0] in _SLACK_RESOURCES)
                or fn.attr in _REPORT_VERBS)
            suspicious = bool(_SLACK_SUSPICION_RE.search(text)) or include_generic
            if shaped and suspicious and _TIER1_RE.search(_mask_comments(text)) is None:
                kind = type(fn.value).__name__
                unprovable.append(
                    ("indirect-receiver",
                     f"{fn.attr}() on a {kind} receiver — cannot resolve, "
                     "review manually", lineno))

    # A' — a bound WRITE method passed as a VALUE, not called here.
    #
    #   retry(client.files_upload_v2, f) / executor.submit(...) / partial(...)
    #
    # `_classify` used to inspect only ast.Call nodes, so the reference escaped
    # and the post fired when the helper invoked it. Gated on a recognised
    # write verb so attribute READS (`client.token`, `client.base_url`) are
    # left alone.
    for node in ast.walk(stmt):
        if not isinstance(node, ast.Attribute) or id(node) in call_func_ids:
            continue
        # Dotted receivers too: `executor.submit(cfg.slack.files_upload_v2, f)`
        # escaped while only plain Names were accepted.
        recv = _dotted(node.value)
        # A WEAK receiver is NOT skipped here. It used to be, and that bailed
        # out before any method check, so a namespace Slack method on a
        # slack-named-factory binding escaped with commented=0 and no finding
        # at all -- the send fired when the callee invoked it:
        #
        #     cfg = get_slack_client()                     # weak binding
        #     executor.submit(cfg.chat_postMessage, ...)   # SENT
        #     cbs.append(cfg.chat_postMessage)             # SENT
        #     fn = cfg.files_upload_v2                     # SENT
        #
        # Pre-existing (base skips weak receivers here too) but a send escape,
        # which is the 2026-09-03 class. `_acts_on_slack` below is the right
        # gate: it treats a weak receiver as UNCERTAIN and lets the method name
        # decide, so `cfg.chat_postMessage` is caught while `cfg.get`,
        # `cfg.token` and MLflow's `cfg.update_run` are left alone -- each
        # verified. Earlier rounds reported this as "does not reproduce"
        # because they probed a PROVEN binding, which takes the conclusive
        # path; the weak state is a different cell of the grid.
        #
        # B'' MIRRORED INTO VALUE POSITION: receiver unresolvable, but the
        # METHOD is unmistakably a Slack Web API write.
        #
        # The direct-call path has had this fallback for a while; this path did
        # not, and the asymmetry was a send escape with commented=0 and no
        # finding -- the same grid as the weak-receiver case above, one cell
        # over. Measured: a direct `bot.client.files_upload_v2(...)` was
        # blocked while passing the same bound method as a VALUE sent:
        #
        #     executor.submit(bot.client.files_upload_v2, file="f")   # SENT
        #     executor.submit(s.client.files_upload_v2, file="f")     # SENT
        #     def go(c): executor.submit(c.files_upload_v2, file="f") # SENT
        #
        # covering an unbound config attribute, an attribute of a weak
        # binding, and a client arriving as a function parameter.
        #
        # UNKNOWN receivers only, exactly as in B''. When the receiver is KNOWN
        # to hold something else the method name must not override that --
        # `kafka = KafkaProducer()` then `submit(kafka.files_upload_v2, ...)`
        # is resolved as non-Slack and stays running (verified, along with
        # pandas `to_dict`/`sort_values`, `spark.read.parquet`, MLflow
        # `update_run`, a Kafka `send`, and the Slack READ
        # `conversations_history`).
        #
        # Pre-existing: base escapes identically. Fixed here because a send
        # escape is the 2026-09-03 class and this repository gates those on the
        # owner regardless of which diff introduced them.
        if (recv and recv not in bindings
                and _is_slack_api_method(node.attr)):
            unprovable.append(
                ("method-as-value",
                 f"{recv}.{node.attr} passed as a value — receiver could not "
                 "be resolved, but the method is a Slack Web API write; "
                 "neutralised", lineno))
            return node.attr, unprovable

        # Receiver CERTAINTY, same helper the statement and header paths use.
        # Without it this path kept the pre-#170 rule, so a receiver whose
        # Slack verdict survived only via the conditional fail-safe still had
        # any write-verb-ish method neutralised: MLflow's `update_run` hits
        # "update" in _KNOWN_WRITE_RE and `create_experiment` hits "create".
        if not _acts_on_slack(recv, node.attr, bindings):
            continue
        # The write-verb gate STAYS, in addition. It is what keeps attribute
        # READS (`client.token`, `client.base_url`) alone -- dropping it in
        # favour of the certainty check alone would neutralise those on any
        # proven client.
        if _is_read_only(node.attr) or not _KNOWN_WRITE_RE.search(node.attr):
            continue
        unprovable.append(
            ("method-as-value",
             f"{recv}.{node.attr} passed as a value — the post fires "
             "when the callee invokes it", lineno))
        return node.attr, unprovable

    # C — tier-1 patterns (wrappers, raw webhook URLs)
    text = _mask_comments(_stmt_text(stmt, lines))
    hit, in_string_only = _tier1_match(text)
    if hit:
        return hit, unprovable
    if in_string_only:
        # Present, but only inside a string literal. Not executable as written
        # (a docstring or a log message), so commenting it would break working
        # code — but `eval()`/`exec()` of that string would post, so report it.
        unprovable.append(
            ("tier1-in-string-literal",
             "a Slack call pattern appears only inside a string literal — not "
             "executable as written; check for eval/exec", lineno))

    return None, unprovable


def _header_write_method(expr: ast.expr, bindings: Bindings) -> Optional[str]:
    """Slack WRITE inside a control-flow header expression, if any.

    Reads are ignored here for the same reason as elsewhere — they have no side
    effect, and `for msg in client.conversations_history(...)` is normal code.
    """
    for call in [n for n in ast.walk(expr) if isinstance(n, ast.Call)]:
        if not isinstance(call.func, ast.Attribute):
            continue
        recv_node = call.func.value
        # Dotted receivers as well as plain names:
        #   `if cfg.slack_webhook.files_upload_v2(file=f):`
        name = _dotted(recv_node)
        # Same decision as the statement path, via the same helper. These two
        # were independent implementations, so gating only the statement path
        # made `x.m()` and `if x.m():` disagree about identical code: the
        # cell-66 MLflow call stayed commented in an `if` header long after the
        # statement form was fixed.
        bound = _acts_on_slack(name, call.func.attr, bindings)
        if (bound and bindings.is_weak(name)
                and not _KNOWN_WRITE_RE.search(call.func.attr)):
            bound = False            # weak evidence, non-write verb — leave it
        inline = (isinstance(recv_node, ast.Call)
                  and _ctor_verdict(recv_node, bindings) is True)
        if (bound or inline) and not _is_read_only(call.func.attr):
            return call.func.attr
        if not bound and _is_slack_api_method(call.func.attr):
            return call.func.attr      # `if cfg.slack_webhook.files_upload_v2(...)`
    return None


def _stmt_owns_evidence(stmt: ast.stmt, method: str,
                        lines: List[str]) -> bool:
    """Does `stmt` itself carry the evidence it was flagged for?

    `_stmt_text()` slices by LINE, not by column, so on a semicolon-shared
    line `a; b` BOTH statements are shown the whole line and both get flagged
    for a call only one of them contains. Two consequences followed:

      * `by_span` kept whichever came first in walk order, so an unrelated
        statement could win the span. Binding preservation then read ITS
        assignment target and bound the stub to it -- `x = 1; <slack send>`
        silently made `x` an empty stub instead of 1, where the base branch
        at least failed loudly with NameError.
      * `preserved` re-emits span members that are NOT targets, so with every
        member flagged its `others` list was always empty. The mechanism
        written precisely to stop `consume(x)` raising NameError on a shared
        line had therefore never fired.

    Judging ownership on the statement's OWN column slice fixes both.

    Fails CLOSED when that slice is unavailable (a statement spanning several
    lines): answering True keeps it a target, so a genuine send is never
    dropped. Verified over the shapes in test_slack_shared_line.py that no
    input ends up with zero owners -- `client.canvases_delete(...)` has no
    tier-1 pattern, and a tier-1-text-only test would have dropped every
    candidate and let the send execute.
    """
    own = _exact_source(stmt, lines)
    if own is None:
        return True
    for node in ast.walk(stmt):
        if isinstance(node, ast.Call):
            fn = node.func
            name = (fn.attr if isinstance(fn, ast.Attribute)
                    else getattr(fn, "id", None))
            if name == method:
                return True
        # method-as-value: the flagged name appears as a bare attribute.
        if isinstance(node, ast.Attribute) and node.attr == method:
            return True
    # Tier-1 TEXT evidence (a raw webhook URL) has no method to point at.
    return bool(_TIER1_RE.search(_mask_comments(own)))


def _owner_proves_send_by_call(stmt: ast.stmt, method: str,
                               lines: List[str]) -> bool:
    """Does the owning statement contain the CALL it was flagged for?

    The call half of `_stmt_owns_evidence`, without its tier-1 TEXT fallback,
    and the discriminator for whether a span's sender is identified at all:

      * owner holds the call (`client.files_upload_v2(file=f)`,
        `send_slack("done")`) -> the sender IS the owner, so an unrelated
        sibling may be re-emitted.
      * owner holds only tier-1 TEXT (`note = '...hooks.slack.com...'`) -> the
        text proves SOMETHING on this line sends, but not WHICH statement, and
        the real sender carries no evidence of its own. Re-emitting a sibling
        can therefore put the send back as live code.
    """
    for node in ast.walk(stmt):
        if isinstance(node, ast.Call):
            fn = node.func
            name = (fn.attr if isinstance(fn, ast.Attribute)
                    else getattr(fn, "id", None))
            if name == method:
                return True
        if isinstance(node, ast.Attribute) and node.attr == method:
            return True
    # The tier-1 path records a sentinel method ("statement"), not an attribute
    # name, so a customer wrapper CALL -- `send_slack("done")` -- matches
    # nothing above. Its own slice matching tier-1 CODE is still call-level
    # proof. _TIER1_CODE_RE, never _TIER1_RE: the latter also matches tier-1
    # TEXT (a raw `hooks.slack.com` URL in a literal), which is exactly the
    # evidence that does NOT identify which statement sends.
    own = _exact_source(stmt, lines)
    if own is None:
        return False                     # unknown -> treat as unproven
    return bool(_TIER1_CODE_RE.search(_mask_comments(own)))


def _sibling_may_transmit(stmt: ast.stmt, lines: List[str]) -> bool:
    """Could this NON-target sibling itself be performing the send?

    Second, narrower fail-closed gate, needed even when the owner's call is
    proven: `requests.post(WEBHOOK_CONST); client.chat_postMessage(...)` has a
    proven owner, yet the sibling posts to a webhook held in a plain name, so
    it carries neither receiver nor tier-1-text evidence of its own and would
    otherwise be re-emitted live. The base implementation commented the whole
    line, so re-emission would be a regression, not an inherited gap.
    """
    for node in ast.walk(stmt):
        if isinstance(node, ast.Call):
            fn = node.func
            name = (fn.attr if isinstance(fn, ast.Attribute)
                    else getattr(fn, "id", None))
            if not name:
                continue
            if (name.lower() in _SEND_VERBS
                    or name.split("_", 1)[0] in _SEND_VERBS
                    or _is_slack_namespace_method(name)):
                return True
    text = _exact_source(stmt, lines)
    if text is None:
        return True                      # unreadable slice -> fail closed
    masked = _mask_comments(text)
    return bool(_SLACK_SUSPICION_RE.search(text)
                or _TIER1_RE.search(masked))


def _exact_source(stmt: ast.stmt, lines: List[str]) -> Optional[str]:
    """Exact source of a SINGLE-LINE statement, by column offsets.

    None when the statement spans several lines, where re-emitting it on one
    line is not safe.
    """
    lineno = getattr(stmt, "lineno", 0)
    if lineno != getattr(stmt, "end_lineno", lineno) or not lineno:
        return None
    col = getattr(stmt, "col_offset", None)
    end = getattr(stmt, "end_col_offset", None)
    if col is None or end is None or lineno - 1 >= len(lines):
        return None
    return lines[lineno - 1][col:end].strip() or None


def _stmt_text(stmt: ast.stmt, lines: List[str]) -> str:
    start = getattr(stmt, "lineno", 1) - 1
    end = getattr(stmt, "end_lineno", getattr(stmt, "lineno", 1))
    return "\n".join(lines[start:end])


def _decorator_findings(tree: ast.AST) -> List[Tuple[str, str, int]]:
    """knockknock senders are decorators — they post on function entry/exit.

    Not a call site, so no call-site analysis can see them. Reported, never
    silently ignored.
    """
    out = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for dec in node.decorator_list:
            target = dec.func if isinstance(dec, ast.Call) else dec
            name = (target.id if isinstance(target, ast.Name)
                    else getattr(target, "attr", ""))
            if name and ("slack" in name.lower() or name.endswith("_sender")):
                out.append(("decorator",
                            f"@{name} on {node.name}() — posts on entry/exit",
                            getattr(dec, "lineno", node.lineno)))
    return out


def neutralize_cell(
    source: str,
    bindings: Optional[Set[str]] = None,
    include_generic: bool = False,
) -> NeutralizeResult:
    """Comment out statements that post to Slack. Returns a NeutralizeResult.

    `bindings` is the notebook-wide Slack-client name set; pass the same set
    across cells and call `update_bindings()` for each cell in execution order.

    On a parse failure the source is returned UNCHANGED with `parse_error` set,
    so the caller can fall back to the regex path. A cell must never be silently
    skipped — that is how the 2026-09-03 post got out.
    """
    if not source or not source.strip():
        return NeutralizeResult(source)

    bindings = _as_bindings(bindings)
    stripped = _strip_magics(source)
    try:
        tree = ast.parse(stripped)
    except SyntaxError as exc:
        # The caller falls back to the regex path, which misses whole method
        # families (files_upload_v2 among them). A migration note alone is too
        # quiet for that, because the downstream repair loop can revalidate the
        # cell with the post still live.
        findings: List[Tuple[str, str, int]] = []
        _bound_now = any(v is True for v in bindings.values())
        if (not include_generic and not _bound_now
                and not _line_has_external_post(source)):
            # No Slack evidence at all -- nothing to certify, caller may fall
            # back to its own handling.
            return NeutralizeResult(
                source, parse_error=f"{type(exc).__name__}: {exc}")
        # Evidence present and the AST is unavailable. FAIL CLOSED: comment
        # every line that looks like an external post, and if anything still
        # looks live afterwards refuse to certify the cell at all.
        safe_source, commented, residual = _failsafe_line_neutralise(
            source, bindings)
        findings.append((
            "unparseable-with-slack-evidence",
            f"cell has Slack evidence but does not parse; applied line-level "
            f"fail-closed neutralisation ({commented} line(s))"
            + (" — RESIDUAL evidence remains, cell must not execute"
               if residual else ""),
            getattr(exc, "lineno", 0) or 0))
        return NeutralizeResult(
            safe_source, commented, [], findings,
            parse_error=f"{type(exc).__name__}: {exc}",
            unsafe=residual)

    lines = source.splitlines()

    # A magic whose tail could not be reduced to parseable code became `pass`,
    # hiding whatever it executes. IPython still runs it, so
    # `%timeit -n 1 client.files_upload_v2(file=f)` posted -- repeatedly.
    # Anything still hidden AND carrying evidence is commented outright.
    _hidden: List[int] = []
    _stripped_lines = stripped.splitlines()
    for _idx, _raw in enumerate(lines):
        if _idx >= len(_stripped_lines):
            break
        if not _MAGIC_RE.match(_raw) or not _raw.strip():
            continue
        if "__AIDP_MAGIC__" not in _stripped_lines[_idx]:
            continue                      # tail was preserved and is analysed
        if _line_has_external_post(_raw):
            _hidden.append(_idx)

    stmt_to_body, bodies = _index_bodies(tree)

    # Per-statement binding snapshots, in source order. A statement is judged
    # against the bindings AS OF that statement — what the interpreter sees.
    # Statements inside def/class bodies are absent from `snapshots` and fall
    # back to the end-of-cell state, because a function may be called later.
    snapshots, final_state, _laundered, _module_ids = _evolve(tree, bindings)

    targets: List[Tuple[ast.stmt, str]] = []
    unprovable: List[Tuple[str, str, int]] = list(_decorator_findings(tree))
    for _name, _ln in _laundered:
        unprovable.append((
            "slack-binding-laundered",
            f"{_name} held a Slack client and was reassigned from a call that "
            "cannot be identified — if it is still a client, calls on it below "
            "are NOT neutralised", _ln))
    for stmt in _innermost_statements(tree):
        # Distinct from "has a snapshot": function statements have snapshots
        # now too, but the tier-2 hint layer stays module-scope-only, matching
        # the regex path.
        at_module_scope = id(stmt) in _module_ids
        state = snapshots.get(id(stmt), final_state)
        method, findings = _classify(stmt, state, lines, include_generic,
                                     at_module_scope)
        unprovable.extend(findings)
        if method:
            # Only the statement that actually carries the evidence becomes a
            # target. On a semicolon-shared line the line-based text flags
            # every member, which let an unrelated statement win the span and
            # kept `preserved` permanently empty.
            if _stmt_owns_evidence(stmt, method, lines):
                targets.append((stmt, method))
            continue
        # Used BEFORE the cell binds it to a Slack client. On a fresh kernel
        # this raises NameError rather than posting, so commenting it would be
        # a false positive (it is how `producer.send(...)` above
        # `producer = WebClient(...)` got the Kafka send commented). It can
        # still post on a re-run in a live kernel, so it is reported.
        for call in [n for n in ast.walk(stmt) if isinstance(n, ast.Call)]:
            recv = _receiver_name(call)
            if (recv and not state.is_slack(recv)
                    and not state.is_known_other(recv)
                    and final_state.is_slack(recv)
                    and isinstance(call.func, ast.Attribute)
                    and not _is_read_only(call.func.attr)):
                unprovable.append((
                    "use-before-slack-bind",
                    f"{recv}.{call.func.attr}() runs before {recv} is bound to "
                    "a Slack client in this cell — NameError on a fresh "
                    "kernel, but posts on a re-run",
                    getattr(stmt, "lineno", 0)))
                break

    # ── Writes in a CONTROL-FLOW HEADER ──────────────────────────────────────
    #   while client.chat_postMessage(...):     if client.chat_delete(...):
    #   for x in client.something(...):         with client.upload(...) as r:
    #
    # The header cannot be commented on its own without orphaning the block, so
    # the whole container statement is commented instead — its body is
    # preserved as comments, nothing is lost, and the result still parses.
    # Rare in practice, but leaving it live would be a leak: the regex path
    # caught the `while` case (by line text) and emitted broken syntax, so a
    # silent miss here would be a regression.
    header_targets: List[Tuple[ast.stmt, str]] = []
    for node in ast.walk(tree):
        headers = []
        if isinstance(node, (ast.If, ast.While)):
            headers = [node.test]
        elif isinstance(node, (ast.For, ast.AsyncFor)):
            headers = [node.iter]
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            headers = [it.context_expr for it in node.items]
        if not headers:
            continue
        for expr in headers:
            state_here = snapshots.get(id(node), final_state)
            method = _header_write_method(expr, state_here)
            if method is None:
                # Tier-1 on the HEADER's own source text. Without this
                # `if send_slack(msg):` executed — a safety regression against
                # the regex path, which commented the header (base=1, head=0).
                # Restricted to the header's own lines so the block body is not
                # scanned twice.
                _h0 = getattr(node, "lineno", 1) - 1
                _h1 = getattr(expr, "end_lineno", getattr(node, "lineno", 1))
                _htext = _mask_comments("\n".join(lines[_h0:_h1]))
                _hit, _ = _tier1_match(_htext)
                if _hit:
                    method = _hit
            if method:
                header_targets.append((node, method))
                unprovable.append(
                    ("container-header-write",
                     f"{method}() in a {type(node).__name__.lower()} header — "
                     "whole block commented; review manually",
                     getattr(node, "lineno", 0)))
                break

    if header_targets:
        # Nested headers overlap: `while client.chat_update(x):` containing
        # `if client.chat_delete(y):` puts BOTH containers in the list, and the
        # bottom-up edit then double-comments the inserted `pass` and leaves a
        # stray comment line. Keep only the outermost — commenting it already
        # covers every header nested inside it.
        header_targets.sort(key=lambda t: (getattr(t[0], "lineno", 0),
                                           -getattr(t[0], "end_lineno", 0)))
        outermost: List[Tuple[ast.stmt, str]] = []
        for node, method in header_targets:
            lo, hi = getattr(node, "lineno", 0), getattr(node, "end_lineno", 0)
            if any(a <= lo and hi <= b
                   for a, b in ((getattr(n, "lineno", 0),
                                 getattr(n, "end_lineno", 0))
                                for n, _ in outermost)):
                continue
            outermost.append((node, method))
        header_targets = outermost

        # A commented container already covers everything inside it, so drop
        # inner targets that fall within its line span (avoids double-editing).
        spans = [(getattr(n, "lineno", 0), getattr(n, "end_lineno", 0))
                 for n, _ in header_targets]
        targets = [(s, m) for s, m in targets
                   if not any(a <= getattr(s, "lineno", 0) <= b for a, b in spans)]
        targets.extend(header_targets)

    if not targets and not _hidden:
        return NeutralizeResult(source, 0, [], unprovable)

    # Already-neutralised statements (re-migration) must not be re-commented.
    #
    # The test is the ANNOTATION MARKER on a COMMENT line — not the bare word
    # "neutralised" anywhere in the statement. Matching the word leaks:
    #     client.chat_postMessage(channel='C1', text='neutralised run')
    # is a real post whose message text happens to contain it, and a substring
    # test silently skips it (measured: old commented=1, this commented=0).
    def _already_done(stmt: ast.stmt) -> bool:
        for line in _stmt_text(stmt, lines).splitlines():
            s = line.strip()
            if s.startswith("#") and _NEUTRALISED_MARKER_RE.search(s):
                return True
        return False

    targets = [(s, m) for s, m in targets if not _already_done(s)]
    if not targets and not _hidden:
        return NeutralizeResult(source, 0, [], unprovable)

    target_ids = {id(s) for s, _ in targets}
    # Blocks that will be left with nothing executable need a `pass`.
    needs_pass: Set[int] = set()
    _module_body_id = id(getattr(tree, "body", None))
    for body_id, stmts in bodies.items():
        if body_id == _module_body_id:
            # A MODULE body may legally be empty. Emitting `pass` at top level
            # when every statement happens to be a Slack call is pure noise in
            # a customer-facing artifact -- and it landed mid-cell, between two
            # commented calls.
            continue
        if stmts and all(id(s) in target_ids for s in stmts):
            needs_pass.add(body_id)

    # ── Collapse targets that share a line span ──────────────────────────────
    # Two statements can live on one physical line:
    #     client.chat_postMessage(...); client.chat_update(...)
    # Commenting each separately double-prefixes the line ('# # ') and appends
    # the annotation twice. Comment the span ONCE.
    #
    # A shared span can also carry UNRELATED code:
    #     x = compute_required_value(); client.files_upload_v2(...)
    # Commenting the whole line silently drops `x`, so `consume(x)` below then
    # raises NameError -- and the repair model's obvious fix is to restore the
    # whole line, upload included. The unrelated statements are therefore
    # re-emitted as live code above the commented original.
    #
    # Counting only TARGETS here was the defect: with one Slack call and one
    # ordinary assignment there is a single target, so no finding fired at all.
    # Membership is computed over EVERY statement on the span.
    def _span_of(node: ast.stmt) -> Tuple[int, int]:
        return (getattr(node, "lineno", 0),
                getattr(node, "end_lineno", getattr(node, "lineno", 0)))

    span_members: Dict[Tuple[int, int], List[ast.stmt]] = {}
    for stmt in _innermost_statements(tree):
        span_members.setdefault(_span_of(stmt), []).append(stmt)

    by_span: Dict[Tuple[int, int], Tuple[ast.stmt, str]] = {}
    # EVERY target on the span, not just the representative. The span is
    # commented once, so only one statement drives the edit -- but each one may
    # bind a name that readers below depend on:
    #     a = client.chat_postMessage(...); b = client.chat_postMessage(...)
    # collapsing to the representative stubbed `a` and lost `b` entirely, with
    # no finding. Keeping the full list lets the emit loop re-bind all of them.
    targets_by_span: Dict[Tuple[int, int], List[Tuple[ast.stmt, str]]] = {}
    for stmt, method in targets:
        by_span.setdefault(_span_of(stmt), (stmt, method))
        targets_by_span.setdefault(_span_of(stmt), []).append((stmt, method))

    # Preserved siblings keep their COLUMN, so the emit loop can interleave
    # them with the stub assignments in the original left-to-right order. A
    # sibling must observe exactly the state its position implies:
    #     old = r; r = <slack>        -> `old = r` reads the REAL prior `r`
    #     r = <slack>; ts = r["ts"]   -> `ts = ...` reads the stub
    #     a = <s1>; old = b; b = <s2> -> `old = b` reads the real prior `b`,
    #                                    because it runs BEFORE s2's stub
    # An earlier version split siblings into "before"/"after" at the LEFTMOST
    # send, which has no answer for the third shape: with two sends there are
    # two boundaries, and a sibling between them was emitted after BOTH stubs,
    # so `old` silently became '' where base raised NameError. Ordering by
    # column needs no boundary at all and is correct for any number of sends.
    preserved: Dict[Tuple[int, int], List[Tuple[int, str]]] = {}
    target_id_set = {id(s) for s, _ in targets}
    for span in by_span:
        members = span_members.get(span, [])
        others = [m for m in members if id(m) not in target_id_set]
        kept: List[Tuple[int, str]] = []
        # Is the sender on this span identified by a call, or only by text?
        _owner_call_proven = any(
            _owner_proves_send_by_call(s, m, lines)
            for s, m in targets_by_span.get(span, []))
        for other in sorted(others, key=lambda n: getattr(n, "col_offset", 0)):
            text = _exact_source(other, lines)
            if text is None:
                unprovable.append(
                    ("shared-line-unpreservable",
                     f"a statement on line {span[0]} shares the span with a "
                     "Slack call and spans multiple lines; it could not be "
                     "re-emitted and is commented with the rest — review",
                     span[0]))
                continue
            # Ownership decides which statement carries the stub, but it
            # cannot by itself decide which statement does the SENDING. Two
            # fail-closed gates, because re-emitting the actual sender put a
            # live webhook POST back into customer code:
            #     note = 'see https://hooks.slack.com docs'; requests.post(url)
            # commented the harmless string and re-emitted the POST, which
            # DELIVERED. The base implementation commented the whole line, so
            # re-emission created that escape rather than inheriting it.
            #
            # Deliberately NOT "any call fails closed": that destroys the case
            # this mechanism exists for -- `x = compute_required_value();
            # client.files_upload_v2(...)` must keep `x`, or `consume(x)` below
            # raises NameError and the repair model's obvious fix is to restore
            # the line, upload included.
            if not _owner_call_proven or _sibling_may_transmit(other, lines):
                unprovable.append(
                    ("shared-line-call-not-preserved",
                     f"a statement on line {span[0]} shares the span with a "
                     "Slack call and could not be proven NOT to be the sender "
                     + ("(the line's only Slack evidence is TEXT, so which "
                        "statement sends is unknown)" if not _owner_call_proven
                        else "(it calls a send-shaped name or carries Slack "
                             "text of its own)")
                     + "; it is commented with the rest rather than "
                       "re-emitted. Re-run it by hand on its own line if it "
                       "is unrelated — review",
                     span[0]))
                continue
            kept.append((getattr(other, "col_offset", 0), text))
        if len(members) > 1:
            unprovable.append(
                ("shared-line",
                 f"{len(members)} statements share line {span[0]} "
                 f"({len(others)} unrelated); the Slack call is commented, "
                 f"{len(kept)} of the unrelated statement(s) re-emitted in "
                 f"their original order and {len(others) - len(kept)} "
                 "commented with it",
                 span[0]))
        if kept:
            preserved[span] = kept

    targets = list(by_span.values())
    target_ids = {id(s) for s, _ in targets}

    # A loop that waits for the send to succeed can fail to terminate when the
    # stand-in cannot satisfy its exit test.
    # Judged per CELL, not per loop body: half the hanging shapes assign
    # OUTSIDE the loop and only READ the value inside it, so a loop-body-scoped
    # test would miss the poll-until-ok idiom. Used only to REPORT the residual
    # now -- see _RETRY_LOOP_RESIDUAL.
    _cell_has_while = any(isinstance(n, ast.While) for n in ast.walk(tree))
    _loop_risk_lines: Set[int] = set()

    # Edit bottom-up so earlier line numbers stay valid.
    ordered = sorted(targets, key=lambda t: -getattr(t[0], "lineno", 0))
    out_lines = list(lines)
    commented = 0
    neutralised: List[Tuple[str, int]] = []

    for stmt, method in ordered:
        start = getattr(stmt, "lineno", 1) - 1
        end = getattr(stmt, "end_lineno", getattr(stmt, "lineno", 1))
        block = out_lines[start:end]
        if not block:
            continue
        indent = block[0][: len(block[0]) - len(block[0].lstrip())]
        replacement = []
        # Re-bind the assignment's target so consumers below keep working.
        # Without this the statement's binding disappears and the next reader
        # raises NameError on code the guard was right to neutralise.
        #
        # Stub assignments and preserved siblings are emitted in the ORIGINAL
        # left-to-right order, each at the column where it stood, so every
        # statement observes exactly the state its position implies:
        #     r = <slack>; ts = r["ts"]      ts reads the stub
        #     old = r; r = <slack>           old reads the real prior r
        #     a = <s1>; old = b; b = <s2>    old reads the real prior b
        # The last shape is why there is no "before/after the send" boundary:
        # with two sends a sibling can sit BETWEEN them, and any single
        # boundary puts it on the wrong side of one of the two stubs.
        # Every target on the span contributes its bindings, not just the
        # representative: `a = <send>; b = <send>` has two, and collapsing to
        # the representative stubbed `a` and lost `b` with no finding at all.
        _stmt_line = getattr(stmt, "lineno", 0)
        _span_here = (getattr(stmt, "lineno", 0),
                      getattr(stmt, "end_lineno", getattr(stmt, "lineno", 0)))
        _bind_items: List[Tuple[int, str]] = []
        _bind_texts: List[str] = []
        _bind_reasons: List[Tuple[str, int]] = []
        for _tstmt, _m in targets_by_span.get(_span_here, [(stmt, method)]):
            _tline = getattr(_tstmt, "lineno", _stmt_line)

            # Walrus targets first, and independently of the statement's own
            # target: a walrus holds the call's result wherever it appears, so
            # `print((r := <send>))` -- which has no statement-level target at
            # all -- must still re-bind `r`. See _walrus_targets.
            _wok, _wdeclined = _walrus_targets(_tstmt, _m)
            for _wcol, _wname in _wok:
                _bind_texts.append(_wname)
                _bind_items.append(
                    (_wcol,
                     f"{indent}{_wname} = "
                     f"{_STUB_TEMP_NAME}{BINDING_ANNOTATION}"))
            for _wname in _wdeclined:
                _wvalue = next(
                    (n.value for n in ast.walk(_tstmt)
                     if isinstance(n, ast.NamedExpr)
                     and isinstance(n.target, ast.Name)
                     and n.target.id == _wname), None)
                _bind_reasons.append(
                    (_WALRUS_NOT_A_RESPONSE_REASON
                     % (_wname, _describe_value(_wvalue)), _tline))

            # No withholding here any more -- see _RETRY_LOOP_RESIDUAL. The
            # stand-in reports success, so a retry loop terminates as it does
            # in production; the narrow unsatisfiable-literal case is reported
            # per cell instead of blocking every `while` cell.
            # Ownership fails CLOSED for a statement whose own column slice is
            # unavailable (it spans several lines), which is right for
            # deciding whether to COMMENT it -- never drop a genuine send. It
            # is not proof that the statement's value was a Slack response:
            #     x = len(
            #         "abc"); client.chat_postMessage(...)
            # made `x` a target, so `x` was stubbed to '' where it should be 3,
            # and base raised NameError. Requiring the statement to actually
            # contain the call it was flagged for separates that from a
            # genuine multi-line send, whose AST does contain it.
            #
            # Scoped to exactly that fail-closed branch. Applying it to every
            # target instead would drop the stub for a single-line webhook
            # post (`resp = requests.post("<hook>", ...)`, flagged from the
            # URL in its own arguments rather than from a method name) and
            # would also replace the accurate text-evidence diagnosis on
            # `WEBHOOK = "<hook>"` with a multi-line explanation that does not
            # apply to it. Single-line statements had their ownership properly
            # proven, so _preservable_binding_text decides those.
            if (_exact_source(_tstmt, lines) is None
                    and not _owner_proves_send_by_call(_tstmt, _m, lines)):
                _bind_reasons.append((_NOT_THE_SENDER_REASON, _tline))
                continue

            # Substitute a response stand-in only where a response was
            # actually lost: the OUTERMOST expression must be the flagged
            # send's own call. Never "contains a call somewhere"
            # (`cfg = {"webhook": get_url(), ...}` assigns a DICT) and never
            # merely "is a call" (`cfg = dict(...)`, `url = os.getenv(...)`
            # assign ordinary results) -- see _value_is_the_flagged_send.
            # Erring this way can only DECLINE a stand-in, which fails loudly;
            # it can never invent one for a value that was never a response.
            #
            # Which explanation applies depends on whether the statement sent
            # at all, which is why this decision lives here rather than in
            # _preservable_binding_text: one message for both read
            # "commented because a Slack address appears in its TEXT, not
            # because it calls Slack" on `ok = (r := <send>) and True`, which
            # is a send.
            _value = _assigned_value_expr(_tstmt)
            if _value is not None and not _value_is_the_flagged_send(
                    _value, _m):
                _template = (_VALUE_NOT_THE_RESPONSE_REASON
                             if _owner_proves_send_by_call(_tstmt, _m, lines)
                             else _NOT_A_RESPONSE_REASON)
                _bind_reasons.append(
                    (_template % _describe_value(_value), _tline))
                continue

            _texts, _reason = _preservable_binding_text(_tstmt)
            if _texts:
                _bind_texts.extend(_texts)
                _tcol = getattr(_tstmt, "col_offset", 0)
                for _text in _texts:
                    if "," in _text:        # flat unpacking: arity from target
                        _n = len(_text.split(","))
                        _rhs = "(" + ", ".join([_STUB_TEMP_NAME] * _n) + ")"
                    else:
                        _rhs = _STUB_TEMP_NAME
                    _bind_items.append(
                        (_tcol,
                         f"{indent}{_text} = {_rhs}{BINDING_ANNOTATION}"))
            elif _reason:
                _bind_reasons.append((_reason, _tline))

        if _bind_items:
            # The temp is a pure definition under a reserved name, so placing
            # it ahead of everything is order-independent.
            replacement.append(
                f"{indent}{_STUB_TEMP_NAME} = {_STUB_EXPR}{BINDING_ANNOTATION}")

        _emit: List[Tuple[int, str]] = list(_bind_items) + [
            (_col, f"{indent}{_text}{PRESERVED_ANNOTATION}")
            for _col, _text in preserved.get(_span_here, [])]
        for _col, _text in sorted(_emit, key=lambda item: item[0]):
            replacement.append(_text)

        if _bind_texts:
            _read_later = _names_read_after(tree, _stmt_line)
            _consumed = sorted(
                {n for _t in _bind_texts
                 for n in (x.strip() for x in _t.split(","))} & _read_later)
            unprovable.append(
                ("binding-preserved",
                 f"{', '.join(_bind_texts)} re-bound to a stand-in response because "
                 f"its Slack call was neutralised"
                 + (f"; read below in this cell by {', '.join(_consumed)}"
                    if _consumed else "")
                 + ". NO message was delivered, but the stand-in REPORTS "
                   "SUCCESS so the notebook follows the same path production "
                   "follows: r['ok'], r.ok and r.data['ok'] are True, "
                   "status_code is 200, and assert / >=400 checks behave as "
                   "on a real send. Payload fields are empty, so r['ts'] is "
                   "'' and an artifact can receive {\"ts\": \"\"}. Anything "
                   "the success path then did — marking records notified, "
                   "writing an audit row, dropping a queue entry — ran on a "
                   "send that never happened; table writes are redirected to "
                   "the migration sandbox, other side effects are not. THIS "
                   "CELL CAN PASS WITHOUT ITS NOTIFICATION PATH HAVING BEEN "
                   "EXERCISED — validate that path by re-running against a "
                   "throwaway workspace or test channel",
                 _stmt_line))
        for _reason, _rline in _bind_reasons:
            # The consequence lives in `_reason`: it differs per target kind
            # (stale value / KeyError / AttributeError / NameError), and a
            # single hardcoded "will raise NameError" was wrong for half of
            # them -- worst for augmented assignment, where nothing raises at
            # all and the reader quietly sees the old value.
            unprovable.append(
                ("binding-not-preserved",
                 f"a neutralised Slack call's assignment target was NOT "
                 f"re-bound — {_reason}. Review", _rline))

        if stmt_to_body.get(id(stmt)) in needs_pass:
            if not replacement:      # preserved code already keeps the block valid
                replacement.append(f"{indent}pass{PASS_ANNOTATION}")
            needs_pass.discard(stmt_to_body.get(id(stmt)))
        for idx, line in enumerate(block):
            suffix = ANNOTATION if idx == 0 else ""
            replacement.append(f"{indent}# {line.lstrip()}{suffix}"
                               if line.strip() else f"{indent}#")
        out_lines[start:end] = replacement
        commented += len(block)
        neutralised.append((method, getattr(stmt, "lineno", 0)))
        if _cell_has_while and _bind_items:
            _loop_risk_lines.add(_stmt_line)

    # ONE finding per cell, not per target: the risk is the loop's exit
    # condition, which an operator inspects once. See _RETRY_LOOP_RESIDUAL --
    # this disclosure is what makes reporting the residual acceptable instead
    # of withholding the stand-in from every `while` cell.
    for _line in sorted(_loop_risk_lines)[:1]:
        unprovable.append(("retry-loop-may-not-terminate",
                           _RETRY_LOOP_RESIDUAL, _line))

    # Preserve the original line ending. splitlines() drops them, so joining
    # with "\n" would silently convert a CRLF cell to LF -- a whole-cell diff
    # for a one-line edit.
    for _idx in _hidden:
        _raw = out_lines[_idx]
        _ind = _raw[: len(_raw) - len(_raw.lstrip())]
        if not _raw.strip().startswith("#"):
            out_lines[_idx] = f"{_ind}# {_raw.strip()}{ANNOTATION}"
            commented += 1
            neutralised.append(("magic-tail", _idx + 1))

    newline = "\r\n" if "\r\n" in source else "\n"
    new_source = newline.join(out_lines)
    if source.endswith(("\n", "\r")):
        new_source += newline

    # Never emit something that will not parse. If we somehow would, keep the
    # original and report it rather than shipping a broken cell.
    try:
        ast.parse(_strip_magics(new_source))
    except SyntaxError as exc:
        return NeutralizeResult(
            source, parse_error=f"edit would break the cell: {exc}")

    neutralised.reverse()
    return NeutralizeResult(new_source, commented, neutralised, unprovable)
